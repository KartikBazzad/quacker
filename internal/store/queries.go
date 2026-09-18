package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// CreateRun inserts a run and its initial steps in one transaction.
func (s *Store) CreateRun(ctx context.Context, run *Run, steps []*Step) error {
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, `INSERT INTO runs
		(id, workflow, kind, status, queue, priority, input, output, error, attempts, max_attempts, run_at, created_at, started_at, completed_at)
		VALUES (?,?,?,?,?,?,?,NULL,'',0,?,?,?,0,0)`,
		run.ID, run.Workflow, run.Kind, run.Status, run.Queue, run.Priority,
		run.Input, run.MaxAttempts, run.RunAt, run.CreatedAt)
	if err != nil {
		return fmt.Errorf("quacker: insert run: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("quacker: insert run: %d rows affected", n)
	}
	for _, st := range steps {
		_, err := tx.ExecContext(ctx, `INSERT INTO steps
			(id, run_id, name, task, ord, status, depends_on, queue, priority, input, output, error, attempts, max_attempts, timeout_ns, run_at, created_at, started_at, completed_at)
			VALUES (?,?,?,?,?,?,?,?,?,?,NULL,'',0,?,?,?,?,0,0)`,
			st.ID, st.RunID, st.Name, st.Task, st.Ord, st.Status, joinDeps(st.DependsOn), st.Queue, st.Priority,
			st.Input, st.MaxAttempts, st.Timeout, st.RunAt, st.CreatedAt)
		if err != nil {
			return fmt.Errorf("quacker: insert step %q: %w", st.Name, err)
		}
	}
	return tx.Commit()
}

// Claim is a step atomically moved to RUNNING, with its parent run.
type Claim struct {
	Step *Step
	Run  *Run
}

// ClaimDue claims up to limit due steps for the given queue, moving them and
// their parent runs to RUNNING. Safe under concurrency: executed on the single
// writer connection inside one immediate transaction, and each UPDATE is
// guarded on the step still being QUEUED.
func (s *Store) ClaimDue(ctx context.Context, queue string, limit int, now int64) ([]*Claim, error) {
	if limit <= 0 {
		return nil, nil
	}
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `SELECT `+stepCols+` FROM steps
		WHERE status = ? AND run_at <= ? AND queue = ?
		ORDER BY priority DESC, run_at ASC, ord ASC LIMIT ?`,
		StatusQueued, now, queue, limit)
	if err != nil {
		return nil, err
	}
	var cands []*Step
	for rows.Next() {
		st, err := scanStep(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		cands = append(cands, st)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	var claims []*Claim
	for _, st := range cands {
		res, err := tx.ExecContext(ctx, `UPDATE steps SET
			status = ?, attempts = attempts + 1, started_at = CASE WHEN started_at = 0 THEN ? ELSE started_at END
			WHERE id = ? AND status = ?`, StatusRunning, now, st.ID, StatusQueued)
		if err != nil {
			return nil, err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			continue // someone else moved it; skip
		}
		if _, err := tx.ExecContext(ctx, `UPDATE runs SET
			status = ?, attempts = attempts + 1, started_at = CASE WHEN started_at = 0 THEN ? ELSE started_at END
			WHERE id = ? AND status = ?`, StatusRunning, now, st.RunID, StatusQueued); err != nil {
			return nil, err
		}
		st.Status = StatusRunning
		st.Attempts++
		if st.StartedAt == 0 {
			st.StartedAt = now
		}
		claims = append(claims, &Claim{Step: st})
	}
	if len(claims) == 0 {
		return nil, tx.Commit()
	}
	// Attach run rows.
	ids := make([]string, len(claims))
	for i, c := range claims {
		ids[i] = c.Step.RunID
	}
	q := `SELECT ` + runCols + ` FROM runs WHERE id IN (` + placeholders(len(ids)) + `)`
	rrows, err := tx.QueryContext(ctx, q, argsAny(ids)...)
	if err != nil {
		return nil, err
	}
	byRun := map[string]*Run{}
	for rrows.Next() {
		r, err := scanRun(rrows)
		if err != nil {
			rrows.Close()
			return nil, err
		}
		byRun[r.ID] = r
	}
	rrows.Close()
	if err := rrows.Err(); err != nil {
		return nil, err
	}
	for _, c := range claims {
		if r := byRun[c.Step.RunID]; r != nil {
			r.Status = StatusRunning
			if r.StartedAt == 0 {
				r.StartedAt = now
			}
			c.Run = r
		}
	}
	return claims, tx.Commit()
}

// CompleteStep marks a step SUCCEEDED and advances its DAG: unblocks dependents
// whose dependencies are all satisfied and, when no steps remain active, moves
// the run to SUCCEEDED (or FAILED if any step failed). The run output becomes
// CompleteResult describes what a CompleteStep transaction changed.
type CompleteResult struct {
	// StepDone is false when the step was already terminal (cancelled or
	// failed by another path) and the success write was refused.
	StepDone bool
	// ReadySteps lists DAG steps unblocked by this success.
	ReadySteps []string
	// CancelledSteps lists sibling steps cancelled by a run failure.
	CancelledSteps []string
	// CancelledStepIDs are the step IDs of CancelledSteps, for context
	// cancellation by the engine.
	CancelledStepIDs []string
	// RunTerminal is true when the run reached a final status in this call.
	RunTerminal bool
	RunStatus   string
	RunError    string
	RunOutput   []byte
}

// CompleteStep marks a step SUCCEEDED and advances its DAG: unblocks dependents
// whose dependencies are all satisfied and, when no steps remain active, moves
// the run to SUCCEEDED (or FAILED if any step failed). The run output becomes
// the output of the last step (highest ord). Guards prevent overwriting a run
// that is already terminal (e.g. cancelled mid-flight) and refuse to
// resurrect a step that was cancelled or failed by another path.
func (s *Store) CompleteStep(ctx context.Context, stepID, runID string, output []byte, now int64) (CompleteResult, error) {
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return CompleteResult{}, err
	}
	defer tx.Rollback()

	// A run that is already terminal (cancelled/failed/interrupted) must not
	// gain SUCCEEDED steps from executors that raced the transition.
	var runStatus string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM runs WHERE id=?`, runID).Scan(&runStatus); err != nil {
		return CompleteResult{}, err
	}
	if IsTerminal(runStatus) {
		// Converge the step to CANCELLED so it doesn't sit RUNNING forever;
		// the run's outcome was decided by Cancel/failure, not this task.
		_, _ = tx.ExecContext(ctx, `UPDATE steps SET status=?, completed_at=? WHERE id=? AND status IN ('QUEUED','RUNNING')`,
			StatusCancelled, now, stepID)
		return CompleteResult{StepDone: false}, tx.Commit()
	}

	upd, err := tx.ExecContext(ctx, `UPDATE steps SET status=?, output=?, error='', completed_at=? WHERE id=? AND status IN ('QUEUED','RUNNING')`,
		StatusSucceeded, output, now, stepID)
	if err != nil {
		return CompleteResult{}, err
	}
	if n, _ := upd.RowsAffected(); n == 0 {
		// Already terminal (cancelled, failed, interrupted): keep it that way.
		return CompleteResult{StepDone: false}, tx.Commit()
	}

	steps, err := loadStepsTx(ctx, tx, runID)
	if err != nil {
		return CompleteResult{}, err
	}
	res := CompleteResult{StepDone: true, RunStatus: StatusRunning}

	// Unblock newly-ready dependents.
	for _, st := range steps {
		if st.Status != StatusBlocked {
			continue
		}
		ready := true
		for _, dep := range st.DependsOn {
			if statusOf(steps, dep) != StatusSucceeded {
				ready = false
				break
			}
		}
		if !ready {
			continue
		}
		if _, err := tx.ExecContext(ctx, `UPDATE steps SET status=?, run_at=? WHERE id=?`,
			StatusQueued, now, st.ID); err != nil {
			return CompleteResult{}, err
		}
		st.Status = StatusQueued
		res.ReadySteps = append(res.ReadySteps, st.Name)
	}

	// Terminal check.
	if anyActive(steps) {
		return res, tx.Commit()
	}
	runStatus, runErr := StatusSucceeded, ""
	for _, st := range steps {
		if st.Status == StatusFailed {
			runStatus, runErr = StatusFailed, st.Error
			break
		}
	}
	last := steps[0]
	for _, st := range steps[1:] {
		if st.Ord > last.Ord {
			last = st
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET status=?, output=?, error=?, completed_at=? WHERE id=? AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED','INTERRUPTED')`,
		runStatus, last.Output, runErr, now, runID); err != nil {
		return CompleteResult{}, err
	}
	res.RunTerminal, res.RunStatus, res.RunError, res.RunOutput = true, runStatus, runErr, last.Output
	return res, tx.Commit()
}

// RetryStep returns a failed attempt to the queue with a future run_at.
func (s *Store) RetryStep(ctx context.Context, stepID string, errMsg string, nextRunAt, now int64) error {
	_, err := s.write.ExecContext(ctx, `UPDATE steps SET status=?, error=?, run_at=? WHERE id=? AND status=?`,
		StatusQueued, errMsg, nextRunAt, stepID, StatusRunning)
	return err
}

// ParkStep requeues a step that was claimed but never really executed (task
// not registered in this process). Unlike RetryStep it hands back the
// attempt ClaimDue took: parking is not a real execution attempt and must
// not consume the task's retry budget.
func (s *Store) ParkStep(ctx context.Context, stepID string, errMsg string, nextRunAt, now int64) error {
	_, err := s.write.ExecContext(ctx, `UPDATE steps SET
		status=?, error=?, run_at=?, attempts = CASE WHEN attempts > 0 THEN attempts - 1 ELSE attempts END
		WHERE id=? AND status=?`,
		StatusQueued, errMsg, nextRunAt, stepID, StatusRunning)
	return err
}

// FinalFailStep marks a step FAILED, cancels all remaining steps of the run,
// and fails the run (v1 policy: a failed step fails the whole run).
func (s *Store) FinalFailStep(ctx context.Context, stepID, runID string, errMsg string, now int64) (CompleteResult, error) {
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return CompleteResult{}, err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `UPDATE steps SET status=?, error=?, completed_at=? WHERE id=?`,
		StatusFailed, errMsg, now, stepID); err != nil {
		return CompleteResult{}, err
	}
	res := CompleteResult{StepDone: true}
	rows, err := tx.QueryContext(ctx, `UPDATE steps SET status=?, completed_at=? WHERE run_id=? AND status IN ('QUEUED','BLOCKED','RUNNING') AND id != ? RETURNING id, name`,
		StatusCancelled, now, runID, stepID)
	if err != nil {
		return CompleteResult{}, err
	}
	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			rows.Close()
			return CompleteResult{}, err
		}
		res.ReadySteps = append(res.ReadySteps, name) // reuse field: cancelled siblings
		res.CancelledStepIDs = append(res.CancelledStepIDs, id)
	}
	rows.Close()

	if _, err := tx.ExecContext(ctx, `UPDATE runs SET status=?, error=?, completed_at=? WHERE id=? AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED','INTERRUPTED')`,
		StatusFailed, errMsg, now, runID); err != nil {
		return CompleteResult{}, err
	}
	res.RunTerminal, res.RunStatus, res.RunError = true, StatusFailed, errMsg
	return res, tx.Commit()
}

// StepCancelled marks a single step CANCELLED (used when a run is cancelled
// while the step is executing; the run row is already terminal).
func (s *Store) StepCancelled(ctx context.Context, stepID string, now int64) error {
	_, err := s.write.ExecContext(ctx, `UPDATE steps SET status=?, completed_at=? WHERE id=? AND status IN ('QUEUED','RUNNING')`,
		StatusCancelled, now, stepID)
	return err
}

// CancelRun cancels a run: queued/blocked steps become CANCELLED immediately
// and the IDs of still-running steps are returned so the caller can interrupt
// their contexts. Returns false if the run was already terminal.
func (s *Store) CancelRun(ctx context.Context, runID string, now int64) (runningSteps []string, ok bool, err error) {
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()

	var status string
	err = tx.QueryRowContext(ctx, `SELECT status FROM runs WHERE id=?`, runID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, ErrNotFound
	}
	if err != nil {
		return nil, false, err
	}
	if IsTerminal(status) {
		return nil, false, nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET status=?, completed_at=? WHERE id=?`,
		StatusCancelled, now, runID); err != nil {
		return nil, false, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE steps SET status=?, completed_at=? WHERE run_id=? AND status IN ('QUEUED','BLOCKED')`,
		StatusCancelled, now, runID); err != nil {
		return nil, false, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT id FROM steps WHERE run_id=? AND status=?`, runID, StatusRunning)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, false, err
		}
		runningSteps = append(runningSteps, id)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	return runningSteps, true, tx.Commit()
}

// ---------------------------------------------------------------------------
// Introspection (read pool; never blocks the writer in WAL mode).

func (s *Store) GetRun(ctx context.Context, id string) (*Run, error) {
	r, err := scanRun(s.read.QueryRowContext(ctx, `SELECT `+runCols+` FROM runs WHERE id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return r, err
}

// GetRunWithSteps returns a run and its steps. The run row is read FIRST and
// steps second (both autocommit on the read pool): step statuses only ever
// advance forward, so a terminal run read at time T can never be paired with
// steps read at T+ε that are less terminal — the invariant "terminal run ⇒
// terminal steps" holds without holding a transaction, which matters because
// in shared-cache memory mode a multi-statement read transaction can
// deadlock the single writer immediately (SQLITE_LOCKED bypasses
// busy_timeout).
func (s *Store) GetRunWithSteps(ctx context.Context, runID string) (*Run, []*Step, error) {
	run, err := s.GetRun(ctx, runID)
	if err != nil {
		return nil, nil, err
	}
	steps, err := s.GetSteps(ctx, runID)
	if err != nil {
		return nil, nil, err
	}
	return run, steps, nil
}

func (s *Store) GetSteps(ctx context.Context, runID string) ([]*Step, error) {
	return loadStepsDB(ctx, s.read, runID)
}

// Filter selects runs for ListRuns. Zero fields are ignored.
type Filter struct {
	Status   string
	Queue    string
	Workflow string
	Limit    int
	Offset   int
}

func (s *Store) ListRuns(ctx context.Context, f Filter) ([]*Run, error) {
	where, args := []string{"1=1"}, []any{}
	if f.Status != "" {
		where, args = append(where, "status=?"), append(args, f.Status)
	}
	if f.Queue != "" {
		where, args = append(where, "queue=?"), append(args, f.Queue)
	}
	if f.Workflow != "" {
		where, args = append(where, "workflow=?"), append(args, f.Workflow)
	}
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	args = append(args, limit, f.Offset)
	rows, err := s.read.QueryContext(ctx, `SELECT `+runCols+` FROM runs WHERE `+strings.Join(where, " AND ")+
		` ORDER BY created_at DESC LIMIT ? OFFSET ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// QueueStats is a per-queue depth snapshot.
type QueueStats struct {
	Queued  int64
	Running int64
	Blocked int64
}

// Metrics returns run counts by status and per-queue depths.
func (s *Store) Metrics(ctx context.Context) (map[string]int64, map[string]*QueueStats, error) {
	byStatus := map[string]int64{}
	rows, err := s.read.QueryContext(ctx, `SELECT status, COUNT(*) FROM runs GROUP BY status`)
	if err != nil {
		return nil, nil, err
	}
	for rows.Next() {
		var st string
		var n int64
		if err := rows.Scan(&st, &n); err != nil {
			rows.Close()
			return nil, nil, err
		}
		byStatus[st] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	queues := map[string]*QueueStats{}
	rows, err = s.read.QueryContext(ctx, `SELECT queue, status, COUNT(*) FROM steps
		WHERE status IN ('QUEUED','RUNNING','BLOCKED') GROUP BY queue, status`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var q, st string
		var n int64
		if err := rows.Scan(&q, &st, &n); err != nil {
			return nil, nil, err
		}
		qs := queues[q]
		if qs == nil {
			qs = &QueueStats{}
			queues[q] = qs
		}
		switch st {
		case StatusQueued:
			qs.Queued += n
		case StatusRunning:
			qs.Running += n
		case StatusBlocked:
			qs.Blocked += n
		}
	}
	return byStatus, queues, rows.Err()
}

// AppendLogs persists task log lines (batched by the engine).
func (s *Store) AppendLogs(ctx context.Context, entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, e := range entries {
		if _, err := tx.ExecContext(ctx, `INSERT INTO logs (run_id, step, at, level, message) VALUES (?,?,?,?,?)`,
			e.RunID, e.Step, e.At, e.Level, e.Message); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// GetLogs returns up to limit most recent log lines for a run, oldest first.
func (s *Store) GetLogs(ctx context.Context, runID string, limit int) ([]LogEntry, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.read.QueryContext(ctx, `SELECT run_id, step, at, level, message FROM logs
		WHERE run_id=? ORDER BY seq DESC LIMIT ?`, runID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LogEntry
	for rows.Next() {
		var e LogEntry
		if err := rows.Scan(&e.RunID, &e.Step, &e.At, &e.Level, &e.Message); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Reverse into chronological order.
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}

// KnownQueues returns every queue that has at least one step, so a restarted
// engine can recreate its queue registry before any new enqueue arrives.
func (s *Store) KnownQueues(ctx context.Context) ([]string, error) {
	rows, err := s.read.QueryContext(ctx, `SELECT DISTINCT queue FROM steps`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var q string
		if err := rows.Scan(&q); err != nil {
			return nil, err
		}
		out = append(out, q)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// Cron rows.

func (s *Store) UpsertCron(ctx context.Context, c *Cron) error {
	_, err := s.write.ExecContext(ctx, `INSERT INTO crons (id, name, spec, task, input, next_at, created_at)
		VALUES (?,?,?,?,?,?,?)
		ON CONFLICT(name) DO UPDATE SET spec=excluded.spec, task=excluded.task, input=excluded.input, next_at=excluded.next_at`,
		c.Name, c.Name, c.Spec, c.Task, c.Input, c.NextAt, c.CreatedAt)
	return err
}

func (s *Store) DeleteCron(ctx context.Context, name string) error {
	_, err := s.write.ExecContext(ctx, `DELETE FROM crons WHERE name=?`, name)
	return err
}

// ---------------------------------------------------------------------------

// GetStepOutputs returns the outputs of the named succeeded steps of a run.
// Missing names are simply absent from the result.
func (s *Store) GetStepOutputs(ctx context.Context, runID string, names []string) (map[string][]byte, error) {
	q := `SELECT name, output FROM steps WHERE run_id=? AND status='SUCCEEDED' AND name IN (` + placeholders(len(names)) + `)`
	args := make([]any, 0, len(names)+1)
	args = append(args, runID)
	for _, n := range names {
		args = append(args, n)
	}
	rows, err := s.read.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]byte{}
	for rows.Next() {
		var name string
		var output []byte
		if err := rows.Scan(&name, &output); err != nil {
			return nil, err
		}
		out[name] = output
	}
	return out, rows.Err()
}

func loadStepsTx(ctx context.Context, tx *sql.Tx, runID string) ([]*Step, error) {
	rows, err := tx.QueryContext(ctx, `SELECT `+stepCols+` FROM steps WHERE run_id=? ORDER BY ord`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Step
	for rows.Next() {
		st, err := scanStep(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

func loadStepsDB(ctx context.Context, db *sql.DB, runID string) ([]*Step, error) {
	rows, err := db.QueryContext(ctx, `SELECT `+stepCols+` FROM steps WHERE run_id=? ORDER BY ord`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Step
	for rows.Next() {
		st, err := scanStep(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

func statusOf(steps []*Step, name string) string {
	for _, s := range steps {
		if s.Name == name {
			return s.Status
		}
	}
	return StatusFailed // missing dependency fails the gate (validated at enqueue anyway)
}

func anyActive(steps []*Step) bool {
	for _, s := range steps {
		switch s.Status {
		case StatusQueued, StatusRunning, StatusBlocked:
			return true
		}
	}
	return false
}

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

func argsAny[T any](xs []T) []any {
	out := make([]any, len(xs))
	for i, x := range xs {
		out[i] = x
	}
	return out
}
