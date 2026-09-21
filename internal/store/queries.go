package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/kartikbazzad/quacker/driver"
)

// maxIntrospectionRows bounds run-scoped introspection collections (a run's
// steps, a run's children) so a pathological run cannot exhaust memory through
// the read pool. The bound is generous — real DAGs and fan-out counts sit far
// below it — and only truncates the snapshot, never execution.
const maxIntrospectionRows = 10000

// CreateRun inserts a run and its initial steps in one transaction.
func (s *Store) CreateRun(ctx context.Context, run *Run, steps []*Step) error {
	return s.CreateRuns(ctx, []*Run{run}, [][]*Step{steps})
}

// CreateRuns inserts several runs and their steps in one transaction — one
// transaction for a batch enqueue instead of one per run.
func (s *Store) CreateRuns(ctx context.Context, runs []*Run, steps [][]*Step) error {
	if len(runs) != len(steps) {
		return fmt.Errorf("quacker: CreateRuns: %d runs, %d step sets", len(runs), len(steps))
	}
	tx, err := s.beginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := insertRunsTx(ctx, tx, runs, steps); err != nil {
		return err
	}
	return tx.Commit()
}

// Sentinel errors for unique jobs.
var (
	// ErrUniqueViolation reports that an insert collided with the unique-job
	// index — normally a concurrent enqueue race; the caller re-resolves.
	ErrUniqueViolation = errors.New("quacker: unique constraint violation")
	// ErrDuplicateJob is returned under UniqueError when a live run already
	// holds the run's unique key.
	ErrDuplicateJob = errors.New("quacker: a run with this unique key already exists")
)

// UniqueConflict selects what a unique run does when a live run already holds
// its (workflow, unique_key).
type UniqueConflict int

const (
	// UniqueReuse returns the live run's id and inserts nothing.
	UniqueReuse UniqueConflict = iota
	// UniqueError fails the enqueue with ErrDuplicateJob.
	UniqueError
	// UniqueReplace cancels the live run and inserts the new one.
	UniqueReplace
)

// ConcurrencyStrategy selects what an enqueue does when a key is at capacity.
type ConcurrencyStrategy int

const (
	// ConcurrencyHold queues the run and waits for a slot (default).
	ConcurrencyHold ConcurrencyStrategy = iota
	// ConcurrencyCancelInProgress cancels running/oldest runs to make room.
	ConcurrencyCancelInProgress
	// ConcurrencyCancelNewest cancels the incoming run.
	ConcurrencyCancelNewest
	// ConcurrencyCancelQueuedExceptNewest keeps the newest Limit queued runs
	// and cancels older queued ones.
	ConcurrencyCancelQueuedExceptNewest
	// ConcurrencyCancelQueuedExceptOldest keeps the oldest Limit queued runs
	// and cancels newer queued ones.
	ConcurrencyCancelQueuedExceptOldest
)

// KeyPolicy is a run's concurrency-key policy, applied at enqueue.
type KeyPolicy struct {
	Key      string
	Limit    int64
	Strategy ConcurrencyStrategy
}

// ReplacedRun is a run cancelled by UniqueReplace. The engine cancels the
// contexts of RunningSteps and releases the run's local waiter.
type ReplacedRun struct {
	RunID        string
	RunningSteps []string
}

// CreateRunsUnique inserts runs and steps, resolving unique keys against live
// runs in the same transaction. For each run with a non-empty UniqueKey it
// looks up a non-terminal run with the same (workflow, key): UniqueReuse skips
// the insert and returns the existing id, UniqueError fails with
// ErrDuplicateJob, UniqueReplace cancels the existing run and inserts. It
// returns the effective run id per input (existing for reuse, new otherwise)
// and any runs replaced (for the engine to interrupt locally). A concurrent
// enqueue race surfaces as a wrapped ErrUniqueViolation; the caller retries.
func (s *Store) CreateRunsUnique(ctx context.Context, runs []*Run, steps [][]*Step, conflicts []UniqueConflict, policies []KeyPolicy) ([]string, []ReplacedRun, error) {
	tx, err := s.beginTx(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()
	ids, replaced, err := s.createRunsUniqueTx(ctx, tx, runs, steps, conflicts, policies)
	if err != nil {
		return nil, nil, err
	}
	return ids, replaced, tx.Commit()
}

// CreateRunsTx is CreateRunsUnique on a caller-owned transaction: it performs
// the same conflict resolution and inserts but never commits or rolls back,
// so the caller's business writes and the enqueued runs are atomic. Used by
// Quacker.EnqueueTx (Postgres/MySQL only); no waits are registered.
func (s *Store) CreateRunsTx(ctx context.Context, tx *sql.Tx, runs []*Run, steps [][]*Step, conflicts []UniqueConflict, policies []KeyPolicy) ([]string, error) {
	ids, _, err := s.createRunsUniqueTx(ctx, &txn{Tx: tx, be: s.be}, runs, steps, conflicts, policies)
	return ids, err
}

// createRunsUniqueTx is the shared body of CreateRunsUnique and CreateRunsTx.
func (s *Store) createRunsUniqueTx(ctx context.Context, tx *txn, runs []*Run, steps [][]*Step, conflicts []UniqueConflict, policies []KeyPolicy) ([]string, []ReplacedRun, error) {
	if len(runs) != len(steps) || len(runs) != len(conflicts) {
		return nil, nil, fmt.Errorf("quacker: CreateRunsUnique: %d runs, %d step sets, %d conflicts", len(runs), len(steps), len(conflicts))
	}
	if len(policies) > 0 && len(policies) != len(runs) {
		return nil, nil, fmt.Errorf("quacker: CreateRunsUnique: %d policies for %d runs", len(policies), len(runs))
	}
	policy := func(i int) KeyPolicy {
		if len(policies) == 0 {
			return KeyPolicy{}
		}
		return policies[i]
	}
	ids := make([]string, len(runs))
	var replaced []ReplacedRun
	// Reserve insertion-order values for sequenced runs and keyed runs that may
	// need cancel-strategy ordering. Runs without either never touch the counter.
	nSeq := 0
	for i, run := range runs {
		if run.SequenceKey != "" || policy(i).Key != "" {
			nSeq++
		}
	}
	var seqNext int64
	if nSeq > 0 {
		base, err := s.allocSeq(ctx, tx, int64(nSeq))
		if err != nil {
			return nil, nil, err
		}
		seqNext = base
	}
	// Plain runs (no unique key, no cancel strategy) batch into multi-row
	// inserts. A unique run is resolved and inserted on its own so its
	// liveRunByUnique sees every previously inserted row, and a run with a
	// cancel strategy is inserted on its own so the strategy sees exactly the
	// runs inserted before it — batching either could change the outcome.
	var batchRuns []*Run
	var batchSteps [][]*Step
	flush := func() error {
		if len(batchRuns) == 0 {
			return nil
		}
		err := insertRunsTx(ctx, tx, batchRuns, batchSteps)
		batchRuns, batchSteps = nil, nil
		return err
	}
	insertErr := func(err error) error {
		if driver.IsUniqueViolation(s.be, err) {
			return fmt.Errorf("%w: %w", ErrUniqueViolation, err)
		}
		return err
	}
	for i, run := range runs {
		ids[i] = run.ID
		if run.SequenceKey != "" || policy(i).Key != "" {
			seqNext++
			run.Seq = seqNext
			for _, st := range steps[i] {
				st.SequenceKey = run.SequenceKey
				st.Seq = run.Seq
			}
		}
		p := policy(i)
		useStrategy := p.Strategy != ConcurrencyHold && p.Key != "" && p.Limit > 0
		immediate := run.UniqueKey != "" || useStrategy
		if immediate {
			if err := flush(); err != nil {
				return nil, nil, insertErr(err)
			}
		}
		if run.UniqueKey != "" {
			existing, err := liveRunByUnique(ctx, tx, run.Workflow, run.UniqueKey)
			if err != nil {
				return nil, nil, err
			}
			if existing != "" {
				switch conflicts[i] {
				case UniqueReuse:
					ids[i] = existing
					continue
				case UniqueError:
					return nil, nil, ErrDuplicateJob
				case UniqueReplace:
					running, err := replaceRunTx(ctx, tx, existing, nowUnix())
					if err != nil {
						return nil, nil, err
					}
					replaced = append(replaced, ReplacedRun{RunID: existing, RunningSteps: running})
				}
			}
		}
		if immediate {
			if err := insertRunsTx(ctx, tx, []*Run{run}, [][]*Step{steps[i]}); err != nil {
				return nil, nil, insertErr(err)
			}
		} else {
			batchRuns = append(batchRuns, run)
			batchSteps = append(batchSteps, steps[i])
		}
		if useStrategy {
			victims, err := s.applyConcurrencyStrategy(ctx, tx, run, p.Limit, p.Strategy, nowUnix())
			if err != nil {
				return nil, nil, err
			}
			replaced = append(replaced, victims...)
		}
	}
	if err := flush(); err != nil {
		return nil, nil, insertErr(err)
	}
	return ids, replaced, nil
}

// allocSeq reserves n consecutive sequence values, returning the base: the
// first reserved value is base+1. The UPDATE row-locks the counter for the
// transaction on Postgres/MySQL, and SQLite is single-writer, so concurrent
// enqueues allocate distinct, ordered values.
func (s *Store) allocSeq(ctx context.Context, tx *txn, n int64) (int64, error) {
	if _, err := tx.exec(ctx, `UPDATE counters SET next = next + ? WHERE name=?`, n, "run"); err != nil {
		return 0, err
	}
	var next int64
	if err := tx.queryRow(ctx, `SELECT next FROM counters WHERE name=?`, "run").Scan(&next); err != nil {
		return 0, err
	}
	return next - n, nil
}

// liveRunByUnique returns the id of a non-terminal run holding (workflow, key),
// or "" when none exists.
func liveRunByUnique(ctx context.Context, tx *txn, workflow, key string) (string, error) {
	var id string
	err := tx.queryRow(ctx, `SELECT id FROM runs
		WHERE workflow=? AND unique_key=? AND status NOT IN (?,?,?,?)
		ORDER BY created_at LIMIT 1`,
		workflow, key, StatusSucceeded, StatusFailed, StatusCancelled, StatusInterrupted).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return id, err
}

// replaceRunTx cancels a run and every non-terminal step. The replacement
// insert needs the unique key freed and cannot coexist with the old run. It
// returns the ids that were RUNNING so the engine can cancel their contexts.
func replaceRunTx(ctx context.Context, tx *txn, runID string, now int64) ([]string, error) {
	rows, err := tx.query(ctx, `SELECT id FROM steps WHERE run_id=? AND status=?`, runID, StatusRunning)
	if err != nil {
		return nil, err
	}
	var running []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		running = append(running, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if _, err := tx.exec(ctx, `UPDATE runs SET status=?, completed_at=?, unique_key=NULL WHERE id=?`,
		StatusCancelled, now, runID); err != nil {
		return nil, err
	}
	if _, err := tx.exec(ctx, `UPDATE steps SET status=?, completed_at=? WHERE run_id=? AND status IN ('QUEUED','BLOCKED','RUNNING','SUSPENDED')`,
		StatusCancelled, now, runID); err != nil {
		return nil, err
	}
	return running, nil
}

// cancelRunTx marks a run and its non-running steps CANCELLED inside an
// existing transaction.
func cancelRunTx(ctx context.Context, tx *txn, runID string, now int64) error {
	if _, err := tx.exec(ctx, `UPDATE runs SET status=?, completed_at=?, unique_key=NULL WHERE id=?`,
		StatusCancelled, now, runID); err != nil {
		return err
	}
	_, err := tx.exec(ctx, `UPDATE steps SET status=?, completed_at=? WHERE run_id=? AND status IN ('QUEUED','BLOCKED','SUSPENDED')`,
		StatusCancelled, now, runID)
	return err
}

// cancelOrDeleteRunTx cancels a run — or deletes it when ephemeral — inside an
// existing transaction, returning the ids of steps that were RUNNING so the
// caller can interrupt their contexts.
func cancelOrDeleteRunTx(ctx context.Context, tx *txn, runID string, now int64) ([]string, error) {
	var ephemeral int64
	err := tx.queryRow(ctx, `SELECT ephemeral FROM runs WHERE id=?`, runID).Scan(&ephemeral)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	rows, err := tx.query(ctx, `SELECT id FROM steps WHERE run_id=? AND status=?`, runID, StatusRunning)
	if err != nil {
		return nil, err
	}
	var running []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		running = append(running, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if ephemeral != 0 {
		return running, deleteEphemeralRunTx(ctx, tx, runID)
	}
	return running, cancelRunTx(ctx, tx, runID, now)
}

// applyConcurrencyStrategy enforces a run's key policy: when the key is over
// its limit, it cancels the victims the strategy selects (which may include
// run itself) and returns them so the engine can interrupt local contexts. It
// runs inside the enqueue transaction, after run is inserted, so ordering is
// the persisted seq.
func (s *Store) applyConcurrencyStrategy(ctx context.Context, tx *txn, run *Run, limit int64, strategy ConcurrencyStrategy, now int64) ([]ReplacedRun, error) {
	rows, err := tx.query(ctx, `SELECT id, status, seq FROM runs
		WHERE concurrency_key=? AND status NOT IN (?,?,?,?)
		ORDER BY seq ASC, created_at ASC`,
		run.ConcurrencyKey, StatusSucceeded, StatusFailed, StatusCancelled, StatusInterrupted)
	if err != nil {
		return nil, err
	}
	type cand struct {
		id     string
		status string
	}
	var cands []cand
	for rows.Next() {
		var c cand
		var seq int64
		if err := rows.Scan(&c.id, &c.status, &seq); err != nil {
			rows.Close()
			return nil, err
		}
		cands = append(cands, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if int64(len(cands)) <= limit {
		return nil, nil
	}
	var victims []string
	switch strategy {
	case ConcurrencyCancelNewest:
		victims = []string{cands[len(cands)-1].id} // the newest, i.e. run
	case ConcurrencyCancelInProgress:
		// Cancel running instances first, then oldest, until under the limit.
		var running, others []cand
		for _, c := range cands {
			if c.status == StatusRunning {
				running = append(running, c)
			} else {
				others = append(others, c)
			}
		}
		ordered := append(running, others...)
		for i := 0; i < len(cands)-int(limit); i++ {
			victims = append(victims, ordered[i].id)
		}
	case ConcurrencyCancelQueuedExceptNewest, ConcurrencyCancelQueuedExceptOldest:
		var queued []cand
		for _, c := range cands {
			if c.status == StatusQueued {
				queued = append(queued, c)
			}
		}
		if int64(len(queued)) <= limit {
			break
		}
		if strategy == ConcurrencyCancelQueuedExceptNewest {
			// Keep the newest limit queued; cancel the older overflow.
			for i := 0; i < len(queued)-int(limit); i++ {
				victims = append(victims, queued[i].id)
			}
		} else {
			// Keep the oldest limit queued; cancel the newer overflow.
			for i := int(limit); i < len(queued); i++ {
				victims = append(victims, queued[i].id)
			}
		}
	}
	var out []ReplacedRun
	for _, id := range victims {
		running, err := cancelOrDeleteRunTx(ctx, tx, id, now)
		if err != nil {
			return nil, err
		}
		out = append(out, ReplacedRun{RunID: id, RunningSteps: running})
	}
	return out, nil
}

// The multi-row INSERT column list and one row's value tuple. output and
// started_at/completed_at/error/attempts use literals so only the real columns
// travel as bound parameters. unique_key is NULL for non-unique runs, which
// every backend's unique index ignores.
const (
	runInsertCols = `id, workflow, kind, status, queue, priority, input, output, error, attempts, max_attempts, run_at, created_at, started_at, completed_at, concurrency_key, parent_id, parent_step, trace_parent, unique_key, sequence_key, seq, ephemeral, groups_json`
	runInsertRow  = `(?,?,?,?,?,?,?,NULL,'',0,?,?,?,0,0,?,?,?,?,?,?,?,?,?)`

	stepInsertCols = `id, run_id, name, task, ord, status, depends_on, queue, priority, input, output, error, attempts, max_attempts, timeout_ns, run_at, created_at, started_at, completed_at, concurrency_key, key_limit, labels, sequence_key, seq`
	stepInsertRow  = `(?,?,?,?,?,?,?,?,?,?,NULL,'',0,?,?,?,?,0,0,?,?,?,?,?)`
)

func runInsertArgs(run *Run) []any {
	return []any{
		run.ID, run.Workflow, run.Kind, run.Status, run.Queue, run.Priority,
		run.Input, run.MaxAttempts, run.RunAt, run.CreatedAt, run.ConcurrencyKey,
		run.ParentID, run.ParentStep, run.TraceParent, nullableString(run.UniqueKey),
		run.SequenceKey, run.Seq, boolInt(run.Ephemeral), string(run.Groups),
	}
}

func stepInsertArgs(st *Step) []any {
	return []any{
		st.ID, st.RunID, st.Name, st.Task, st.Ord, st.Status, encodeList(st.DependsOn),
		st.Queue, st.Priority, st.Input, st.MaxAttempts, st.Timeout, st.RunAt, st.CreatedAt,
		st.ConcurrencyKey, st.KeyLimit, encodeList(st.Labels), st.SequenceKey, st.Seq,
	}
}

// insertMaxParams bounds the bound parameters in a single INSERT statement so
// a large workflow cannot exceed a backend's placeholder limit. SQLite's
// default SQLITE_MAX_VARIABLE_NUMBER is 32,766; Postgres and MySQL allow
// 65,535. 30,000 leaves margin under all three.
const insertMaxParams = 30000

// Bound parameters per inserted row, derived from the arg builders so the
// count cannot drift from the column lists. step_keys has no builder, so its
// four columns are counted directly.
var (
	runInsertArgc  = len(runInsertArgs(&Run{}))
	stepInsertArgc = len(stepInsertArgs(&Step{}))
	keyInsertArgc  = 4
)

// insertRunsTx inserts runs and their steps into an existing transaction using
// multi-row INSERTs, chunked by the backend's InsertBatchRows (large for
// networked databases, small for modernc SQLite). Shared by CreateRuns and
// FireCron (which fuses the insert with a cron CAS). All runs in a chunk are
// inserted before their steps, so the steps' foreign key always resolves.
func insertRunsTx(ctx context.Context, tx *txn, runs []*Run, steps [][]*Step) error {
	batch := driver.DefaultInsertBatchRows
	if b, ok := tx.be.(driver.InsertBatcher); ok {
		batch = b.InsertBatchRows()
	}
	if batch < 1 {
		batch = 1
	}
	// A backend could return a large batch; the runs statement is one INSERT,
	// so cap it by the parameter budget too.
	if maxRuns := insertMaxParams / runInsertArgc; batch > maxRuns {
		batch = maxRuns
	}
	for start := 0; start < len(runs); start += batch {
		end := start + batch
		if end > len(runs) {
			end = len(runs)
		}
		if err := insertRunsChunk(ctx, tx, runs[start:end], steps[start:end]); err != nil {
			return err
		}
	}
	return nil
}

// insertRunsChunk writes one chunk of runs, all of their steps, and all of
// their step keys. The runs are one statement (the chunk is already bounded),
// but steps and keys are split across as many statements as the parameter
// budget requires — a single run can carry thousands of steps. unique_key is
// NULL when the run is not unique so the unique index ignores it.
func insertRunsChunk(ctx context.Context, tx *txn, runs []*Run, steps [][]*Step) error {
	var rb strings.Builder
	rb.WriteString(`INSERT INTO runs (` + runInsertCols + `) VALUES `)
	rargs := make([]any, 0, len(runs)*runInsertArgc)
	for i, run := range runs {
		if i > 0 {
			rb.WriteByte(',')
		}
		rb.WriteString(runInsertRow)
		rargs = append(rargs, runInsertArgs(run)...)
	}
	res, err := tx.exec(ctx, rb.String(), rargs...)
	if err != nil {
		return fmt.Errorf("quacker: insert run: %w", err)
	}
	if n, _ := res.RowsAffected(); n != int64(len(runs)) {
		return fmt.Errorf("quacker: insert run: %d rows affected for %d runs", n, len(runs))
	}

	stepPrefix := `INSERT INTO steps (` + stepInsertCols + `) VALUES `
	maxStepRows := insertMaxParams / stepInsertArgc
	var sb strings.Builder
	sargs := make([]any, 0, maxStepRows*stepInsertArgc)
	rows := 0
	flushSteps := func() error {
		if rows == 0 {
			return nil
		}
		if _, err := tx.exec(ctx, sb.String(), sargs...); err != nil {
			return fmt.Errorf("quacker: insert steps: %w", err)
		}
		sb.Reset()
		sb.WriteString(stepPrefix)
		sargs = sargs[:0]
		rows = 0
		return nil
	}
	sb.WriteString(stepPrefix)
	for _, sts := range steps {
		for _, st := range sts {
			if rows == maxStepRows {
				if err := flushSteps(); err != nil {
					return err
				}
			}
			if rows > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(stepInsertRow)
			sargs = append(sargs, stepInsertArgs(st)...)
			rows++
		}
	}
	if rows > 0 {
		if _, err := tx.exec(ctx, sb.String(), sargs...); err != nil {
			return fmt.Errorf("quacker: insert steps: %w", err)
		}
	}

	const keyPrefix = `INSERT INTO step_keys (step_id, name, value, key_limit) VALUES `
	maxKeyRows := insertMaxParams / keyInsertArgc
	var kb strings.Builder
	kargs := make([]any, 0, maxKeyRows*keyInsertArgc)
	krows := 0
	flushKeys := func() error {
		if krows == 0 {
			return nil
		}
		if _, err := tx.exec(ctx, kb.String(), kargs...); err != nil {
			return fmt.Errorf("quacker: insert step keys: %w", err)
		}
		kb.Reset()
		kb.WriteString(keyPrefix)
		kargs = kargs[:0]
		krows = 0
		return nil
	}
	kb.WriteString(keyPrefix)
	for _, sts := range steps {
		for _, st := range sts {
			for _, k := range st.Keys {
				if krows == maxKeyRows {
					if err := flushKeys(); err != nil {
						return err
					}
				}
				if krows > 0 {
					kb.WriteByte(',')
				}
				kb.WriteString(`(?,?,?,?)`)
				kargs = append(kargs, st.ID, k.Name, k.Value, k.Limit)
				krows++
			}
		}
	}
	if krows > 0 {
		if _, err := tx.exec(ctx, kb.String(), kargs...); err != nil {
			return fmt.Errorf("quacker: insert step keys: %w", err)
		}
	}
	return nil
}

func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func boolInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// deleteEphemeralRunTx removes an ephemeral run and its steps (the journal
// cascades) and logs. Callers invoke it in the terminal transition's
// transaction.
func deleteEphemeralRunTx(ctx context.Context, tx *txn, runID string) error {
	if _, err := tx.exec(ctx, `DELETE FROM logs WHERE run_id=?`, runID); err != nil {
		return err
	}
	if _, err := tx.exec(ctx, `DELETE FROM steps WHERE run_id=?`, runID); err != nil {
		return err
	}
	_, err := tx.exec(ctx, `DELETE FROM runs WHERE id=?`, runID)
	return err
}

// ErrNonTerminalPurge is returned when a purge is asked to delete a
// non-terminal status. Only terminal runs may ever be purged.
var ErrNonTerminalPurge = errors.New("quacker: purge statuses must be terminal")

// terminalStatuses is the default purge set when Statuses is empty.
var terminalStatuses = []string{StatusSucceeded, StatusFailed, StatusCancelled, StatusInterrupted}

// PurgeOptions selects terminal runs for deletion.
type PurgeOptions struct {
	// Before is the exclusive completion-time cutoff in unix nanoseconds:
	// runs with completed_at == 0 or completed_at >= Before are never
	// eligible. Required (> 0).
	Before int64
	// Statuses restricts deletion to these terminal statuses; empty means
	// every terminal status. A non-terminal status is an error.
	Statuses []string
	// Queue, when non-empty, restricts deletion to runs in that queue.
	Queue string
	// ExcludeDeadLettered, when true, never deletes dead-lettered runs.
	ExcludeDeadLettered bool
	// KeepLogs retains the logs of purged runs and skips the orphan sweep.
	KeepLogs bool
	// BatchSize bounds rows deleted per transaction. <= 0 uses 500.
	BatchSize int
}

// PurgeResult reports how many rows a purge removed. Events are aged with
// the same cutoff so the events table cannot grow without bound; event
// subscriptions are never purged.
type PurgeResult struct {
	Runs   int64
	Steps  int64
	Logs   int64
	Events int64
}

// PurgeRuns deletes terminal runs older than opts.Before, in batches, along
// with their steps and (unless KeepLogs) their logs. A run is never eligible
// while any of its steps is RUNNING, which keeps a purge from racing a step
// that is still recording its own cancellation. When logs are purged, an
// orphan sweep then removes logs whose run no longer exists (e.g. left by an
// earlier KeepLogs purge).
func (s *Store) PurgeRuns(ctx context.Context, opts PurgeOptions) (PurgeResult, error) {
	if opts.Before <= 0 {
		return PurgeResult{}, errors.New("quacker: purge requires a positive Before cutoff")
	}
	statuses := opts.Statuses
	if len(statuses) == 0 {
		statuses = terminalStatuses
	}
	for _, st := range statuses {
		if !IsTerminal(st) {
			return PurgeResult{}, fmt.Errorf("%w: %s", ErrNonTerminalPurge, st)
		}
	}
	batch := opts.BatchSize
	if batch <= 0 {
		batch = 500
	}
	var res PurgeResult
	for {
		n, r, err := s.purgeRunBatch(ctx, statuses, opts.Queue, opts.ExcludeDeadLettered, opts.Before, batch, opts.KeepLogs)
		if err != nil {
			return res, err
		}
		res.Runs += r.Runs
		res.Steps += r.Steps
		res.Logs += r.Logs
		if n < batch {
			break
		}
	}
	if !opts.KeepLogs {
		for {
			n, err := s.purgeOrphanLogs(ctx, batch)
			if err != nil {
				return res, err
			}
			res.Logs += n
			if n < int64(batch) {
				break
			}
		}
	}
	// Events age out with the same cutoff: they are best-effort audit, so
	// leaving them behind would recreate the unbounded-growth problem that
	// retention exists to solve.
	for {
		n, err := s.purgeEvents(ctx, opts.Before, batch)
		if err != nil {
			return res, err
		}
		res.Events += n
		if n < int64(batch) {
			break
		}
	}
	return res, nil
}

// DeadLetters lists dead-lettered runs (dead_lettered_at > 0), newest first,
// optionally filtered by workflow and/or queue.
func (s *Store) DeadLetters(ctx context.Context, workflow, queue string, limit, offset int) ([]*Run, error) {
	if limit <= 0 {
		limit = 100
	}
	where := []string{"dead_lettered_at > 0"}
	var args []any
	if workflow != "" {
		where = append(where, "workflow=?")
		args = append(args, workflow)
	}
	if queue != "" {
		where = append(where, "queue=?")
		args = append(args, queue)
	}
	args = append(args, limit, offset)
	rows, err := s.read.QueryContext(ctx, `SELECT `+runCols+` FROM runs WHERE `+
		strings.Join(where, " AND ")+` ORDER BY dead_lettered_at DESC LIMIT ? OFFSET ?`, args...)
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

// RetryDeadLetter reopens a dead-lettered run in place: its steps return to
// QUEUED (BLOCKED when they still have dependencies), attempts and errors are
// cleared, and the dead-letter marker is removed, so the run is claimed again.
func (s *Store) RetryDeadLetter(ctx context.Context, runID string, now int64) error {
	tx, err := s.beginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := s.be.RunLock(ctx, tx.Tx, runID); err != nil {
		return err
	}
	var status string
	var dead int64
	err = tx.queryRow(ctx, `SELECT status, dead_lettered_at FROM runs WHERE id=?`, runID).Scan(&status, &dead)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if status != StatusFailed || dead == 0 {
		return ErrNotDeadLetter
	}
	// Reset steps, restoring BLOCKED for those with dependencies.
	rows, err := tx.query(ctx, `SELECT id, depends_on FROM steps WHERE run_id=?`, runID)
	if err != nil {
		return err
	}
	type stepRow struct {
		id   string
		deps string
	}
	var stepRows []stepRow
	for rows.Next() {
		var sr stepRow
		if err := rows.Scan(&sr.id, &sr.deps); err != nil {
			rows.Close()
			return err
		}
		stepRows = append(stepRows, sr)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, sr := range stepRows {
		st := StatusQueued
		if len(decodeList(sr.deps)) > 0 {
			st = StatusBlocked
		}
		if _, err := tx.exec(ctx, `UPDATE steps SET status=?, attempts=0, error='', output=NULL,
			started_at=0, completed_at=0, claimed_at=0, worker_id='', lease_expires_at=0,
			resume_at=0, wait_kind='', wait_event='', run_at=? WHERE id=?`, st, now, sr.id); err != nil {
			return err
		}
	}
	if _, err := tx.exec(ctx, `UPDATE runs SET status=?, attempts=0, error='', output=NULL,
		started_at=0, completed_at=0, run_at=?, dead_lettered_at=0 WHERE id=?`,
		StatusQueued, now, runID); err != nil {
		return err
	}
	return tx.Commit()
}

// DismissDeadLetter clears a run's dead-letter marker without retrying it.
func (s *Store) DismissDeadLetter(ctx context.Context, runID string) error {
	res, err := s.write.ExecContext(ctx, `UPDATE runs SET dead_lettered_at=0 WHERE id=? AND dead_lettered_at>0`, runID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotDeadLetter
	}
	return nil
}

// purgeEvents deletes up to batch events older than before.
func (s *Store) purgeEvents(ctx context.Context, before int64, batch int) (int64, error) {
	tx, err := s.beginTx(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	// Derived table: MySQL rejects LIMIT in a subquery of the same table being
	// modified (error 1093).
	r, err := tx.exec(ctx, `DELETE FROM events WHERE seq IN (
		SELECT seq FROM (SELECT seq FROM events WHERE created_at > 0 AND created_at < ? LIMIT ?) AS e)`,
		before, batch)
	if err != nil {
		return 0, err
	}
	n, _ := r.RowsAffected()
	return n, tx.Commit()
}

// purgeRunBatch deletes one batch of eligible runs (and their steps/logs) in
// a single transaction, returning how many runs were selected.
func (s *Store) purgeRunBatch(ctx context.Context, statuses []string, queue string, excludeDead bool, before int64, batch int, keepLogs bool) (int, PurgeResult, error) {
	var res PurgeResult
	tx, err := s.beginTx(ctx)
	if err != nil {
		return 0, res, err
	}
	defer tx.Rollback()

	dead := 0
	if excludeDead {
		dead = 1
	}
	q := `SELECT r.id FROM runs r
		WHERE r.status IN (` + placeholders(len(statuses)) + `)
		  AND r.completed_at > 0 AND r.completed_at < ?
		  AND (? = '' OR r.queue = ?)
		  AND (? = 0 OR r.dead_lettered_at = 0)
		  AND NOT EXISTS (SELECT 1 FROM steps s WHERE s.run_id = r.id AND s.status = ?)
		ORDER BY r.completed_at LIMIT ?`
	args := make([]any, 0, len(statuses)+6)
	for _, st := range statuses {
		args = append(args, st)
	}
	args = append(args, before, queue, queue, dead, StatusRunning, batch)
	rows, err := tx.query(ctx, q, args...)
	if err != nil {
		return 0, res, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, res, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, res, err
	}
	rows.Close()
	if len(ids) == 0 {
		return 0, res, tx.Commit()
	}
	idArgs := argsAny(ids)
	ph := placeholders(len(ids))
	if !keepLogs {
		r, err := tx.exec(ctx, `DELETE FROM logs WHERE run_id IN (`+ph+`)`, idArgs...)
		if err != nil {
			return 0, res, err
		}
		res.Logs, _ = r.RowsAffected()
	}
	if r, err := tx.exec(ctx, `DELETE FROM steps WHERE run_id IN (`+ph+`)`, idArgs...); err != nil {
		return 0, res, err
	} else {
		res.Steps, _ = r.RowsAffected()
	}
	if r, err := tx.exec(ctx, `DELETE FROM runs WHERE id IN (`+ph+`)`, idArgs...); err != nil {
		return 0, res, err
	} else {
		res.Runs, _ = r.RowsAffected()
	}
	if err := tx.Commit(); err != nil {
		return 0, res, err
	}
	return len(ids), res, nil
}

// purgeOrphanLogs deletes up to batch log rows whose run no longer exists.
func (s *Store) purgeOrphanLogs(ctx context.Context, batch int) (int64, error) {
	tx, err := s.beginTx(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	// Derived table: MySQL rejects LIMIT in a subquery of the same table being
	// modified (error 1093).
	r, err := tx.exec(ctx, `DELETE FROM logs WHERE seq IN (
		SELECT seq FROM (SELECT seq FROM logs WHERE run_id NOT IN (SELECT id FROM runs) LIMIT ?) AS l)`, batch)
	if err != nil {
		return 0, err
	}
	n, _ := r.RowsAffected()
	return n, tx.Commit()
}

// Claim is a step atomically moved to RUNNING, with its parent run. Resumed
// is true when the step was SUSPENDED (a durable resume) rather than QUEUED;
// a resume stamps claimed_at but does not increment attempts.
type Claim struct {
	Step    *Step
	Run     *Run
	Resumed bool
}

// The backend's KeyGate lets a claim proceed only while fewer than key_limit
// RUNNING steps share the step's concurrency_key. It appears in both the
// candidate SELECT (so saturated keys don't crowd the LIMIT with unclaimable
// rows) and the per-step UPDATE (re-checked there against this transaction's
// own earlier claims, which is what makes a batch of same-key steps safe).
// The count is database state, not an in-memory semaphore: parked, cancelled,
// and interrupted steps free their key automatically.
// QueueClaim is one queue's claim budget for ClaimDueMulti.
type QueueClaim struct {
	Name  string
	Limit int
	// RateLimit/RateWindow are the queue's sliding-window start cap; a
	// RateLimit <= 0 disables it.
	RateLimit  int64
	RateWindow int64
}

// ClaimDue claims up to limit due steps for one queue, moving them and their
// parent runs to RUNNING. It is ClaimDueMulti with a single queue.
func (s *Store) ClaimDue(ctx context.Context, queue string, limit int, now int64, rateLimit, rateWindow int64, workerLabels []string, workerID string, leaseUntil int64) ([]*Claim, error) {
	return s.ClaimDueMulti(ctx, []QueueClaim{{
		Name: queue, Limit: limit, RateLimit: rateLimit, RateWindow: rateWindow,
	}}, now, workerLabels, workerID, leaseUntil)
}

// queuePausedGate excludes steps whose queue is paused. It is enforced in the
// claim SQL (the candidate SELECT and both UPDATE arms) so a pause holds
// across instances and restarts without an engine sync loop.
const queuePausedGate = `NOT EXISTS (SELECT 1 FROM queue_pauses p WHERE p.queue = steps.queue)`

// runNotPausedGate excludes steps whose run is paused. Like the queue gate it
// lives in the claim SQL so a run pause holds across instances.
const runNotPausedGate = `NOT EXISTS (SELECT 1 FROM runs r WHERE r.id = steps.run_id AND r.status = 'PAUSED')`

// PauseQueue stops a queue from being claimed. Enqueues, crons, and events
// still create runs, which accumulate QUEUED until ResumeQueue.
func (s *Store) PauseQueue(ctx context.Context, name string) error {
	q := s.be.UpsertSQL("queue_pauses", []string{"queue", "paused_at"}, []string{"queue"}, []string{"paused_at"})
	_, err := s.write.ExecContext(ctx, q, name, nowUnix())
	return err
}

// ResumeQueue re-enables claims for a queue. Resuming an unpaused queue is a
// no-op.
func (s *Store) ResumeQueue(ctx context.Context, name string) error {
	_, err := s.write.ExecContext(ctx, `DELETE FROM queue_pauses WHERE queue=?`, name)
	return err
}

// PausedQueues returns the paused queue names, sorted.
func (s *Store) PausedQueues(ctx context.Context) ([]string, error) {
	rows, err := s.read.QueryContext(ctx, `SELECT queue FROM queue_pauses ORDER BY queue`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

// NextDue returns the earliest scheduled claim time (unix nanos) among QUEUED
// steps' run_at and SUSPENDED steps' resume_at, or 0 when nothing is
// scheduled. A value at or before now means work is already due — it may have
// been inserted after the last claim (e.g. EnqueueTx) or be held by a
// concurrency/sequence/label gate — so the scheduler must not sleep long.
// A future value lets the scheduler sleep until then. It ignores event-only
// waits (resume_at = 0), which never self-claim.
func (s *Store) NextDue(ctx context.Context) (int64, error) {
	var next sql.NullInt64
	err := s.read.QueryRowContext(ctx, `SELECT MIN(t) FROM (
		SELECT run_at AS t FROM steps WHERE status=?
		UNION ALL
		SELECT resume_at AS t FROM steps WHERE status=? AND resume_at > 0
	)`, StatusQueued, StatusSuspended).Scan(&next)
	if err != nil {
		return 0, err
	}
	if !next.Valid {
		return 0, nil
	}
	return next.Int64, nil
}

// claimTxs counts claim transactions opened, so tests can prove the scheduler
// stops opening empty ones when nothing is due.
func (s *Store) ClaimTransactions() int64 { return s.claimTxs.Load() }

// ClaimDueMulti claims due steps across several queues in one write
// transaction — one transaction per scheduler tick rather than one per queue.
// Each queue is capped at its own limit and rate window, and the per-key gate
// plus the guarded UPDATE are unchanged.
//
// When workerID is non-empty (leases enabled) each claim is stamped with the
// worker and a lease deadline, and the whole batch takes the backend's claim
// lock so the counting gates are evaluated cluster-wide exactly as SQLite's
// single writer does.
func (s *Store) ClaimDueMulti(ctx context.Context, queues []QueueClaim, now int64, workerLabels []string, workerID string, leaseUntil int64) ([]*Claim, error) {
	s.claimTxs.Add(1)
	tx, err := s.beginTx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := s.be.ClaimLock(ctx, tx.Tx); err != nil {
		return nil, err
	}

	workerJSON := encodeList(workerLabels)
	var claims []*Claim
	for _, q := range queues {
		cs, err := s.claimQueueTx(ctx, tx, q, now, workerJSON, workerID, leaseUntil)
		if err != nil {
			return nil, err
		}
		claims = append(claims, cs...)
	}
	if len(claims) == 0 {
		return nil, tx.Commit()
	}
	// Attach run rows for the whole batch in one query.
	ids := make([]string, len(claims))
	for i, c := range claims {
		ids[i] = c.Step.RunID
	}
	q := `SELECT ` + runCols + ` FROM runs WHERE id IN (` + placeholders(len(ids)) + `)`
	rrows, err := tx.query(ctx, q, argsAny(ids)...)
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

// claimCandidatesSQL is the scheduler's candidate read: due QUEUED steps plus
// resumable SUSPENDED steps in one queue, in claim order. Its ORDER BY must
// stay in sync with the (queue, priority DESC, run_at, ord) index from
// migration22 — the index lets the scan stop at LIMIT instead of gathering and
// sorting every due step. Extracted so BenchmarkClaimCandidates measures the
// exact query the scheduler runs.
func (s *Store) claimCandidatesSQL() string {
	return `SELECT ` + stepCols + ` FROM steps
		WHERE queue = ? AND (
			(status = ? AND run_at <= ?)
			OR (status = ? AND resume_at > 0 AND resume_at <= ?)
		) AND ` + s.keyGate + `
		AND ` + queuePausedGate + `
		AND ` + runNotPausedGate + `
		AND ` + s.seqGate + `
		AND ` + s.keysGate + `
		AND ` + s.be.LabelGate() + `
		ORDER BY priority DESC, run_at ASC, ord ASC LIMIT ?`
}

// claimQueueTx claims up to q.Limit steps for one queue inside an existing
// transaction. Returned claims have their Run unset (ClaimDueMulti attaches
// runs for the whole batch).
//
// When RateLimit > 0 the queue is rate-limited: at most RateLimit steps may
// be claimed in the trailing RateWindow nanoseconds (a sliding window over
// claimed_at), so the limit is capped at the window's remaining budget. The
// count is persisted, so the window survives a File-mode restart instead of
// allowing a fresh-process burst.
func (s *Store) claimQueueTx(ctx context.Context, tx *txn, q QueueClaim, now int64, workerLabelsJSON, workerID string, leaseUntil int64) ([]*Claim, error) {
	limit := q.Limit
	if limit <= 0 {
		return nil, nil
	}
	if q.RateLimit > 0 {
		rateWindow := q.RateWindow
		if rateWindow <= 0 {
			rateWindow = int64(time.Second)
		}
		var started int64
		if err := tx.queryRow(ctx, `SELECT COUNT(*) FROM steps WHERE queue=? AND claimed_at >= ?`,
			q.Name, now-rateWindow).Scan(&started); err != nil {
			return nil, err
		}
		if remaining := q.RateLimit - started; remaining < int64(limit) {
			limit = int(remaining)
		}
		if limit <= 0 {
			return nil, nil
		}
	}

	// Two claim arms: fresh QUEUED work, and due SUSPENDED steps resuming
	// (a sleep's wake time or a wait's timeout deadline). Event-driven wakes
	// flip the step to QUEUED and are covered by the first arm.
	rows, err := tx.query(ctx, s.claimCandidatesSQL(),
		q.Name, StatusQueued, now, StatusSuspended, now, workerLabelsJSON, limit)
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
		resumed := st.Status == StatusSuspended
		var (
			res sql.Result
			err error
		)
		if resumed {
			// A resume spends start budget (claimed_at) but is not a new
			// attempt and does not restamp started_at.
			res, err = tx.exec(ctx, `UPDATE steps SET
				status = ?, claimed_at = ?, resume_at = 0, wait_kind = '', wait_event = '',
				worker_id = ?, lease_expires_at = ?
				WHERE id = ? AND status = ? AND resume_at > 0 AND resume_at <= ? AND `+s.keyGate+` AND `+queuePausedGate+` AND `+runNotPausedGate+` AND `+s.seqGate+` AND `+s.keysGate,
				StatusRunning, now, workerID, leaseUntil, st.ID, StatusSuspended, now)
		} else {
			res, err = tx.exec(ctx, `UPDATE steps SET
				status = ?, attempts = attempts + 1, claimed_at = ?,
				started_at = CASE WHEN started_at = 0 THEN ? ELSE started_at END,
				worker_id = ?, lease_expires_at = ?
				WHERE id = ? AND status = ? AND `+s.keyGate+` AND `+queuePausedGate+` AND `+runNotPausedGate+` AND `+s.seqGate+` AND `+s.keysGate,
				StatusRunning, now, now, workerID, leaseUntil, st.ID, StatusQueued)
		}
		if err != nil {
			return nil, err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			continue // moved by someone else, or its key saturated mid-batch
		}
		if !resumed {
			if _, err := tx.exec(ctx, `UPDATE runs SET
				status = ?, attempts = attempts + 1, started_at = CASE WHEN started_at = 0 THEN ? ELSE started_at END
				WHERE id = ? AND status = ?`, StatusRunning, now, st.RunID, StatusQueued); err != nil {
				return nil, err
			}
			st.Attempts++
			if st.StartedAt == 0 {
				st.StartedAt = now
			}
		}
		st.Status = StatusRunning
		st.ClaimedAt = now
		st.ResumeAt = 0
		st.WorkerID = workerID
		st.LeaseExpiresAt = leaseUntil
		claims = append(claims, &Claim{Step: st, Resumed: resumed})
	}
	return claims, nil
}

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
// Completion is one step success to record in a batch. Name is the step's DAG
// identity (the executor already knows it), carried so completion can find its
// dependents without re-reading the row.
type Completion struct {
	StepID string
	RunID  string
	Name   string
	Output []byte
	Now    int64
}

func (s *Store) CompleteStep(ctx context.Context, stepID, runID, name string, output []byte, now int64) (CompleteResult, error) {
	tx, err := s.beginTx(ctx)
	if err != nil {
		return CompleteResult{}, err
	}
	defer tx.Rollback()
	// Serialize concurrent completions of the same run across nodes.
	if err := s.be.RunLock(ctx, tx.Tx, runID); err != nil {
		return CompleteResult{}, err
	}
	counts, err := s.stepCountsTx(ctx, tx, []string{runID})
	if err != nil {
		return CompleteResult{}, err
	}
	res, err := s.completeStepTx(ctx, tx, stepID, runID, name, counts[runID], output, now)
	if err != nil {
		return CompleteResult{}, err
	}
	return res, tx.Commit()
}

// stepCountsTx returns the total step count per run id, so completion can
// recognize a run whose only step just finished and skip the DAG queries.
func (s *Store) stepCountsTx(ctx context.Context, tx *txn, runIDs []string) (map[string]int64, error) {
	if len(runIDs) == 0 {
		return map[string]int64{}, nil
	}
	rows, err := tx.query(ctx, `SELECT run_id, COUNT(*) FROM steps WHERE run_id IN (`+placeholders(len(runIDs))+`) GROUP BY run_id`,
		argsAny(runIDs)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := make(map[string]int64, len(runIDs))
	for rows.Next() {
		var id string
		var n int64
		if err := rows.Scan(&id, &n); err != nil {
			return nil, err
		}
		counts[id] = n
	}
	return counts, rows.Err()
}

// CompleteSteps records several step completions in one transaction, in order,
// returning one result per completion. Batching amortizes the commit and the
// per-completion round trips; a batch is atomic, so on error the caller should
// retry the completions individually.
func (s *Store) CompleteSteps(ctx context.Context, comps []Completion) ([]CompleteResult, error) {
	tx, err := s.beginTx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	out := make([]CompleteResult, len(comps))
	// One query classifies every run in the batch by step count; a run with a
	// single step takes the fast path below.
	runIDs := make([]string, 0, len(comps))
	seen := make(map[string]struct{}, len(comps))
	for _, c := range comps {
		if _, ok := seen[c.RunID]; !ok {
			seen[c.RunID] = struct{}{}
			runIDs = append(runIDs, c.RunID)
		}
	}
	counts, err := s.stepCountsTx(ctx, tx, runIDs)
	if err != nil {
		return nil, err
	}
	for i, c := range comps {
		if err := s.be.RunLock(ctx, tx.Tx, c.RunID); err != nil {
			return nil, err
		}
		res, err := s.completeStepTx(ctx, tx, c.StepID, c.RunID, c.Name, counts[c.RunID], c.Output, c.Now)
		if err != nil {
			return nil, err
		}
		out[i] = res
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

// completeStepTx is the body of CompleteStep (and each entry of CompleteSteps):
// it must run inside a transaction that already holds the run lock, and it does
// not commit.
func (s *Store) completeStepTx(ctx context.Context, tx *txn, stepID, runID, stepName string, stepCount int64, output []byte, now int64) (CompleteResult, error) {
	// A run that is already terminal (cancelled/failed/interrupted) must not
	// gain SUCCEEDED steps from executors that raced the transition.
	var runStatus string
	var ephemeral int64
	if err := tx.queryRow(ctx, `SELECT status, ephemeral FROM runs WHERE id=?`, runID).Scan(&runStatus, &ephemeral); err != nil {
		return CompleteResult{}, err
	}
	if IsTerminal(runStatus) {
		// Converge the step to CANCELLED so it doesn't sit RUNNING forever;
		// the run's outcome was decided by Cancel/failure, not this task.
		_, _ = tx.exec(ctx, `UPDATE steps SET status=?, completed_at=? WHERE id=? AND status IN ('QUEUED','RUNNING')`,
			StatusCancelled, now, stepID)
		return CompleteResult{StepDone: false}, nil
	}

	upd, err := tx.exec(ctx, `UPDATE steps SET status=?, output=?, error='', completed_at=? WHERE id=? AND status IN ('QUEUED','RUNNING')`,
		StatusSucceeded, output, now, stepID)
	if err != nil {
		return CompleteResult{}, err
	}
	if n, _ := upd.RowsAffected(); n == 0 {
		// Already terminal (cancelled, failed, interrupted): keep it that way.
		return CompleteResult{StepDone: false}, nil
	}

	res := CompleteResult{StepDone: true, RunStatus: StatusRunning}

	// The run's only step just succeeded: it is terminal, has no BLOCKED
	// dependents within the run to unblock, no failed sibling to report, and
	// its own output is the run output. Skip the DAG queries — this is the
	// common one-task-per-run shape.
	if stepCount == 1 {
		if _, err := tx.exec(ctx, `UPDATE runs SET status=?, output=?, error='', completed_at=?, unique_key=NULL WHERE id=? AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED','INTERRUPTED')`,
			StatusSucceeded, output, now, runID); err != nil {
			return CompleteResult{}, err
		}
		res.RunTerminal, res.RunStatus, res.RunError, res.RunOutput = true, StatusSucceeded, "", output
		if ephemeral != 0 {
			if err := deleteEphemeralRunTx(ctx, tx, runID); err != nil {
				return CompleteResult{}, err
			}
		}
		return res, nil
	}

	// Unblock the direct dependents of the step that just finished. Only those
	// BLOCKED steps are read (indexed by run_id,status, filtered to those that
	// depend on stepName), and only their dependencies' statuses are fetched
	// (indexed by run_id,name) — never the whole run or input/output blobs.
	if err := s.unblockReadyTx(ctx, tx, runID, stepName, now, &res); err != nil {
		return CompleteResult{}, err
	}

	// Terminal check: is any step still active? EXISTS stops at the first
	// active row (indexed by run_id,status); only the final completion scans
	// the run to find none.
	var one int
	switch err := tx.queryRow(ctx, `SELECT 1 FROM steps WHERE run_id=? AND status IN ('QUEUED','RUNNING','BLOCKED','SUSPENDED') LIMIT 1`, runID).Scan(&one); {
	case err == nil:
		return res, nil // still active
	case errors.Is(err, sql.ErrNoRows):
		// terminal, fall through
	default:
		return CompleteResult{}, err
	}

	// The run's outcome and payload in one round trip: a failed step's error
	// (if any) and the last step's output (highest ord). Both are scalar
	// subqueries, so exactly one row is returned even when neither exists.
	runStatus, runErr := StatusSucceeded, ""
	var failedErr sql.NullString
	var lastOut []byte
	if err := tx.queryRow(ctx, `SELECT
		(SELECT error FROM steps WHERE run_id=? AND status=? LIMIT 1),
		(SELECT output FROM steps WHERE run_id=? ORDER BY ord DESC LIMIT 1)`,
		runID, StatusFailed, runID).Scan(&failedErr, &lastOut); err != nil {
		return CompleteResult{}, err
	}
	if failedErr.Valid {
		runStatus, runErr = StatusFailed, failedErr.String
	}
	if _, err := tx.exec(ctx, `UPDATE runs SET status=?, output=?, error=?, completed_at=?, unique_key=NULL WHERE id=? AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED','INTERRUPTED')`,
		runStatus, lastOut, runErr, now, runID); err != nil {
		return CompleteResult{}, err
	}
	res.RunTerminal, res.RunStatus, res.RunError, res.RunOutput = true, runStatus, runErr, lastOut
	if ephemeral != 0 {
		// Ephemeral runs leave no history: delete the finished run in the same
		// transaction that recorded it terminal.
		if err := deleteEphemeralRunTx(ctx, tx, runID); err != nil {
			return CompleteResult{}, err
		}
	}
	return res, nil
}

// unblockReadyTx queues the BLOCKED steps of runID whose dependencies have all
// succeeded, appending their names to res.ReadySteps. It reads only BLOCKED
// steps (indexed by run_id,status) and their dependencies' statuses (indexed
// by run_id,name) — never the whole run and never input/output blobs.
func (s *Store) unblockReadyTx(ctx context.Context, tx *txn, runID, completedName string, now int64, res *CompleteResult) error {
	rows, err := tx.query(ctx, s.be.BlockedDependentsSQL(), runID, StatusBlocked, completedName)
	if err != nil {
		return err
	}
	type blockedStep struct {
		id   string
		name string
		deps []string
	}
	var blocked []blockedStep
	depSet := map[string]struct{}{}
	for rows.Next() {
		var id, name, deps string
		if err := rows.Scan(&id, &name, &deps); err != nil {
			rows.Close()
			return err
		}
		b := blockedStep{id: id, name: name, deps: decodeList(deps)}
		blocked = append(blocked, b)
		for _, d := range b.deps {
			depSet[d] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if len(blocked) == 0 {
		return nil
	}

	status := make(map[string]string, len(depSet))
	if len(depSet) > 0 {
		names := make([]string, 0, len(depSet))
		for n := range depSet {
			names = append(names, n)
		}
		drows, err := tx.query(ctx, `SELECT name, status FROM steps WHERE run_id=? AND name IN (`+placeholders(len(names))+`)`,
			append([]any{runID}, argsAny(names)...)...)
		if err != nil {
			return err
		}
		for drows.Next() {
			var n, st string
			if err := drows.Scan(&n, &st); err != nil {
				drows.Close()
				return err
			}
			status[n] = st
		}
		if err := drows.Err(); err != nil {
			drows.Close()
			return err
		}
		drows.Close()
	}

	for _, b := range blocked {
		ready := true
		for _, d := range b.deps {
			if status[d] != StatusSucceeded {
				ready = false
				break
			}
		}
		if !ready {
			continue
		}
		if _, err := tx.exec(ctx, `UPDATE steps SET status=?, run_at=? WHERE id=? AND status=?`,
			StatusQueued, now, b.id, StatusBlocked); err != nil {
			return err
		}
		res.ReadySteps = append(res.ReadySteps, b.name)
	}
	return nil
}

// RetryStep returns a failed attempt to the queue with a future run_at.
func (s *Store) RetryStep(ctx context.Context, stepID string, errMsg string, nextRunAt, now int64) error {
	_, err := s.write.ExecContext(ctx, `UPDATE steps SET status=?, error=?, run_at=? WHERE id=? AND status=?`,
		StatusQueued, errMsg, nextRunAt, stepID, StatusRunning)
	return err
}

// ParkStep requeues a step that was claimed but never really executed (task
// not registered in this process). Unlike RetryStep it hands back the
// attempt ClaimDue took — and clears claimed_at, so a parked claim doesn't
// consume the queue's rate window either: parking is not a real execution
// attempt and must not consume the task's retry budget.
func (s *Store) ParkStep(ctx context.Context, stepID string, errMsg string, nextRunAt, now int64) error {
	_, err := s.write.ExecContext(ctx, `UPDATE steps SET
		status=?, error=?, run_at=?, claimed_at=0,
		attempts = CASE WHEN attempts > 0 THEN attempts - 1 ELSE attempts END
		WHERE id=? AND status=?`,
		StatusQueued, errMsg, nextRunAt, stepID, StatusRunning)
	return err
}

// FinalFailStep marks a step FAILED, cancels all remaining steps of the run,
// and fails the run (v1 policy: a failed step fails the whole run).
func (s *Store) FinalFailStep(ctx context.Context, stepID, runID string, errMsg string, deadLetter bool, now int64) (CompleteResult, error) {
	tx, err := s.beginTx(ctx)
	if err != nil {
		return CompleteResult{}, err
	}
	defer tx.Rollback()
	// Serialize concurrent completions of the same run across nodes.
	if err := s.be.RunLock(ctx, tx.Tx, runID); err != nil {
		return CompleteResult{}, err
	}
	var ephemeral int64
	if err := tx.queryRow(ctx, `SELECT ephemeral FROM runs WHERE id=?`, runID).Scan(&ephemeral); err != nil {
		return CompleteResult{}, err
	}

	if _, err := tx.exec(ctx, `UPDATE steps SET status=?, error=?, completed_at=? WHERE id=?`,
		StatusFailed, errMsg, now, stepID); err != nil {
		return CompleteResult{}, err
	}
	res := CompleteResult{StepDone: true}
	// SELECT then UPDATE rather than UPDATE ... RETURNING: MySQL has no
	// UPDATE ... RETURNING. RunLock serializes concurrent completions of this
	// run, so the rows cannot change between the two statements.
	rows, err := tx.query(ctx, `SELECT id, name FROM steps
		WHERE run_id=? AND status IN ('QUEUED','BLOCKED','RUNNING','SUSPENDED') AND id != ?`,
		runID, stepID)
	if err != nil {
		return CompleteResult{}, err
	}
	var cancelledIDs []string
	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			rows.Close()
			return CompleteResult{}, err
		}
		res.CancelledSteps = append(res.CancelledSteps, name)
		res.CancelledStepIDs = append(res.CancelledStepIDs, id)
		cancelledIDs = append(cancelledIDs, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return CompleteResult{}, err
	}
	rows.Close()
	if len(cancelledIDs) > 0 {
		args := append([]any{StatusCancelled, now}, argsAny(cancelledIDs)...)
		if _, err := tx.exec(ctx, `UPDATE steps SET status=?, completed_at=? WHERE id IN (`+
			placeholders(len(cancelledIDs))+`)`, args...); err != nil {
			return CompleteResult{}, err
		}
	}

	runSet := `status=?, error=?, completed_at=?, unique_key=NULL`
	runArgs := []any{StatusFailed, errMsg, now}
	if deadLetter && ephemeral == 0 { // ephemeral runs are never dead-lettered
		runSet += `, dead_lettered_at=?`
		runArgs = append(runArgs, now)
	}
	runArgs = append(runArgs, runID)
	if _, err := tx.exec(ctx, `UPDATE runs SET `+runSet+
		` WHERE id=? AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED','INTERRUPTED')`,
		runArgs...); err != nil {
		return CompleteResult{}, err
	}
	res.RunTerminal, res.RunStatus, res.RunError = true, StatusFailed, errMsg
	if ephemeral != 0 {
		if err := deleteEphemeralRunTx(ctx, tx, runID); err != nil {
			return CompleteResult{}, err
		}
	}
	return res, tx.Commit()
}

// StepCancelled marks a single step CANCELLED (used when a run is cancelled
// while the step is executing; the run row is already terminal). SUSPENDED is
// included so a step that suspended after its run went terminal (the
// cancellation race) converges to CANCELLED rather than lingering forever.
func (s *Store) StepCancelled(ctx context.Context, stepID string, now int64) error {
	_, err := s.write.ExecContext(ctx, `UPDATE steps SET status=?, completed_at=?, resume_at=0, wait_kind='', wait_event='' WHERE id=? AND status IN ('QUEUED','RUNNING','SUSPENDED')`,
		StatusCancelled, now, stepID)
	return err
}

// CancelRun cancels a run: queued/blocked steps become CANCELLED immediately
// and the IDs of still-running steps are returned so the caller can interrupt
// their contexts. prevStatus is the run's status before the transition (for
// event publishing). Returns false if the run was already terminal.
func (s *Store) CancelRun(ctx context.Context, runID string, now int64) (runningSteps []string, prevStatus string, ok bool, err error) {
	tx, err := s.beginTx(ctx)
	if err != nil {
		return nil, "", false, err
	}
	defer tx.Rollback()
	// Serialize against concurrent completion of the same run across nodes.
	if err := s.be.RunLock(ctx, tx.Tx, runID); err != nil {
		return nil, "", false, err
	}

	var status string
	var ephemeral int64
	err = tx.queryRow(ctx, `SELECT status, ephemeral FROM runs WHERE id=?`, runID).Scan(&status, &ephemeral)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, "", false, ErrNotFound
	}
	if err != nil {
		return nil, "", false, err
	}
	if IsTerminal(status) {
		return nil, status, false, nil
	}
	// Collect running steps before any deletion so their contexts are
	// interrupted.
	rows, err := tx.query(ctx, `SELECT id FROM steps WHERE run_id=? AND status=?`, runID, StatusRunning)
	if err != nil {
		return nil, "", false, err
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, "", false, err
		}
		runningSteps = append(runningSteps, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, "", false, err
	}
	rows.Close()
	if ephemeral != 0 {
		if err := deleteEphemeralRunTx(ctx, tx, runID); err != nil {
			return nil, "", false, err
		}
	} else if err := cancelRunTx(ctx, tx, runID, now); err != nil {
		return nil, "", false, err
	}
	return runningSteps, status, true, tx.Commit()
}

// Snooze moves a non-running run's start time to until (unix nanos): its
// QUEUED steps and the run row are pushed forward. A run with a RUNNING step
// returns ErrRunRunning (cancel or pause it first); a terminal run returns
// ErrRunTerminal; an unknown run returns ErrNotFound. SUSPENDED steps keep
// their resume_at — snoozing never wakes a durable wait early.
func (s *Store) Snooze(ctx context.Context, runID string, until int64) error {
	tx, err := s.beginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := s.be.RunLock(ctx, tx.Tx, runID); err != nil {
		return err
	}
	var status string
	err = tx.queryRow(ctx, `SELECT status FROM runs WHERE id=?`, runID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if IsTerminal(status) {
		return ErrRunTerminal
	}
	var running int
	if err := tx.queryRow(ctx, `SELECT COUNT(*) FROM steps WHERE run_id=? AND status=?`, runID, StatusRunning).Scan(&running); err != nil {
		return err
	}
	if running > 0 {
		return ErrRunRunning
	}
	if _, err := tx.exec(ctx, `UPDATE steps SET run_at=? WHERE run_id=? AND status=?`,
		until, runID, StatusQueued); err != nil {
		return err
	}
	if _, err := tx.exec(ctx, `UPDATE runs SET run_at=? WHERE id=?`, until, runID); err != nil {
		return err
	}
	return tx.Commit()
}

// PauseRun marks a non-terminal run PAUSED so its steps are not claimed until
// ResumeRun. It returns the ids of still-RUNNING steps (for the engine to
// interrupt) and the previous status; changed is false when the run was
// already paused. A terminal run returns ErrRunTerminal.
func (s *Store) PauseRun(ctx context.Context, runID string, now int64) (running []string, prevStatus string, changed bool, err error) {
	tx, err := s.beginTx(ctx)
	if err != nil {
		return nil, "", false, err
	}
	defer tx.Rollback()
	if err := s.be.RunLock(ctx, tx.Tx, runID); err != nil {
		return nil, "", false, err
	}
	var status string
	err = tx.queryRow(ctx, `SELECT status FROM runs WHERE id=?`, runID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, "", false, ErrNotFound
	}
	if err != nil {
		return nil, "", false, err
	}
	if IsTerminal(status) {
		return nil, status, false, ErrRunTerminal
	}
	if status == StatusPaused {
		return nil, status, false, nil
	}
	if _, err := tx.exec(ctx, `UPDATE runs SET status=?, paused_at=? WHERE id=?`,
		StatusPaused, now, runID); err != nil {
		return nil, "", false, err
	}
	rows, err := tx.query(ctx, `SELECT id FROM steps WHERE run_id=? AND status=?`, runID, StatusRunning)
	if err != nil {
		return nil, "", false, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, "", false, err
		}
		running = append(running, id)
	}
	if err := rows.Err(); err != nil {
		return nil, "", false, err
	}
	return running, status, true, tx.Commit()
}

// ResumeRun clears a run's PAUSED status, returning it to QUEUED so its steps
// are claimable again. A non-paused run is a no-op; a terminal run returns
// ErrRunTerminal.
func (s *Store) ResumeRun(ctx context.Context, runID string, now int64) error {
	tx, err := s.beginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := s.be.RunLock(ctx, tx.Tx, runID); err != nil {
		return err
	}
	var status string
	err = tx.queryRow(ctx, `SELECT status FROM runs WHERE id=?`, runID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if IsTerminal(status) {
		return ErrRunTerminal
	}
	if status != StatusPaused {
		return tx.Commit()
	}
	// Resume means "continue now": reset the run and its QUEUED steps to the
	// current time so a run paused while scheduled ahead starts immediately.
	if _, err := tx.exec(ctx, `UPDATE steps SET run_at=? WHERE run_id=? AND status=?`,
		now, runID, StatusQueued); err != nil {
		return err
	}
	if _, err := tx.exec(ctx, `UPDATE runs SET status=?, run_at=?, paused_at=0 WHERE id=?`,
		StatusQueued, now, runID); err != nil {
		return err
	}
	return tx.Commit()
}

// RequeuePausedStep returns a step whose execution was interrupted by a run
// pause to QUEUED, refunding its attempt (a pause is not a real attempt). The
// run stays PAUSED, so the step is not claimed until ResumeRun.
func (s *Store) RequeuePausedStep(ctx context.Context, stepID string, now int64) error {
	_, err := s.write.ExecContext(ctx, `UPDATE steps SET
		status=?, claimed_at=0, worker_id='', lease_expires_at=0,
		attempts = CASE WHEN attempts > 0 THEN attempts - 1 ELSE attempts END
		WHERE id=? AND status=?`,
		StatusQueued, stepID, StatusRunning)
	return err
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

// GetSteps returns a run's steps in DAG order, capped at maxIntrospectionRows.
func (s *Store) GetSteps(ctx context.Context, runID string) ([]*Step, error) {
	return loadStepsDB(ctx, s.read, runID)
}

// Filter selects runs for ListRuns. Zero fields are ignored.
type Filter struct {
	Status   string
	Queue    string
	Workflow string
	ParentID string
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
	if f.ParentID != "" {
		where, args = append(where, "parent_id=?"), append(args, f.ParentID)
	}
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	args = append(args, limit, f.Offset)
	// id is the tiebreaker: created_at can collide (batch enqueues share a
	// now), and without it OFFSET pagination can repeat or skip rows.
	rows, err := s.read.QueryContext(ctx, `SELECT `+runCols+` FROM runs WHERE `+strings.Join(where, " AND ")+
		` ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?`, args...)
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

// ListChildren returns the runs enqueued from parentID, oldest first, capped
// at maxIntrospectionRows.
func (s *Store) ListChildren(ctx context.Context, parentID string) ([]*Run, error) {
	rows, err := s.read.QueryContext(ctx,
		`SELECT `+runCols+` FROM runs WHERE parent_id=? ORDER BY created_at, id LIMIT ?`,
		parentID, maxIntrospectionRows)
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
	Queued    int64
	Running   int64
	Blocked   int64
	Suspended int64
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
		WHERE status IN ('QUEUED','RUNNING','BLOCKED','SUSPENDED') GROUP BY queue, status`)
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
		case StatusSuspended:
			qs.Suspended += n
		}
	}
	return byStatus, queues, rows.Err()
}

// AppendLogs persists task log lines (batched by the engine).
func (s *Store) AppendLogs(ctx context.Context, entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	tx, err := s.beginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, e := range entries {
		if _, err := tx.exec(ctx, `INSERT INTO logs (run_id, step, at, level, message) VALUES (?,?,?,?,?)`,
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
	q := s.be.UpsertSQL("crons",
		[]string{"id", "name", "spec", "task", "input", "next_at", "created_at"},
		[]string{"name"},
		[]string{"spec", "task", "input", "next_at"})
	_, err := s.write.ExecContext(ctx, q,
		c.Name, c.Name, c.Spec, c.Task, c.Input, c.NextAt, c.CreatedAt)
	return err
}

func (s *Store) DeleteCron(ctx context.Context, name string) error {
	_, err := s.write.ExecContext(ctx, `DELETE FROM crons WHERE name=?`, name)
	return err
}

// ListCrons returns every persisted cron trigger, for re-arming on Start.
func (s *Store) ListCrons(ctx context.Context) ([]*Cron, error) {
	rows, err := s.read.QueryContext(ctx,
		`SELECT name, spec, task, input, next_at, created_at FROM crons ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Cron
	for rows.Next() {
		var c Cron
		if err := rows.Scan(&c.Name, &c.Spec, &c.Task, &c.Input, &c.NextAt, &c.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, &c)
	}
	return out, rows.Err()
}

// FireCron atomically claims a cron occurrence and enqueues its run: it
// advances next_at only if it still equals expectNext, and inserts the run in
// the same transaction. It returns false when another node already fired this
// occurrence, so a cron fires once per occurrence across the cluster (at-most-
// once: a crash between the CAS and the insert rolls both back and retries).
func (s *Store) FireCron(ctx context.Context, name string, expectNext, newNext int64, run *Run, steps []*Step) (bool, error) {
	tx, err := s.beginTx(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	res, err := tx.exec(ctx, `UPDATE crons SET next_at=? WHERE name=? AND next_at=?`, newNext, name, expectNext)
	if err != nil {
		return false, err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return false, tx.Commit() // lost the race; nothing to enqueue
	}
	if err := insertRunsTx(ctx, tx, []*Run{run}, [][]*Step{steps}); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// ---------------------------------------------------------------------------
// Events.

// ListEvents returns up to limit most recent events, newest first.
func (s *Store) ListEvents(ctx context.Context, limit int) ([]*Event, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.read.QueryContext(ctx,
		`SELECT seq, name, payload, created_at FROM events ORDER BY seq DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.Seq, &e.Name, &e.Payload, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, &e)
	}
	return out, rows.Err()
}

// UpsertEventSub persists an event→task binding (idempotent per pair).
func (s *Store) UpsertEventSub(ctx context.Context, sub *EventSub) error {
	id := sub.Event + "/" + sub.Task
	q := s.be.UpsertSQL("event_subscriptions",
		[]string{"id", "event", "task", "created_at"},
		[]string{"event", "task"},
		[]string{"created_at"})
	_, err := s.write.ExecContext(ctx, q, id, sub.Event, sub.Task, sub.CreatedAt)
	return err
}

// DeleteEventSub removes an event→task binding.
func (s *Store) DeleteEventSub(ctx context.Context, event, task string) error {
	_, err := s.write.ExecContext(ctx,
		`DELETE FROM event_subscriptions WHERE event=? AND task=?`, event, task)
	return err
}

// ListEventSubs returns every persisted binding, for re-arming on Start.
func (s *Store) ListEventSubs(ctx context.Context) ([]*EventSub, error) {
	rows, err := s.read.QueryContext(ctx,
		`SELECT event, task, created_at FROM event_subscriptions ORDER BY event, task`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*EventSub
	for rows.Next() {
		var s EventSub
		if err := rows.Scan(&s.Event, &s.Task, &s.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, &s)
	}
	return out, rows.Err()
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

func loadStepsDB(ctx context.Context, db *dbConn, runID string) ([]*Step, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT `+stepCols+` FROM steps WHERE run_id=? ORDER BY ord LIMIT ?`,
		runID, maxIntrospectionRows)
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
