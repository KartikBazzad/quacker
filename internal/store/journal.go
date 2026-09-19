package store

import (
	"context"
	"database/sql"
	"errors"
)

// Journal entry kinds. Each durable await a task performs appends one entry,
// and every invocation replays them by index in call order.
const (
	JournalSleep = "sleep"
	JournalWait  = "wait"
	JournalOnce  = "once"
)

// JournalEntry is one durable-await record on a step.
type JournalEntry struct {
	StepID string
	Idx    int64
	Kind   string
	// Key identifies a RunOnce block (validated on replay).
	Key string
	// Event is the awaited event name for a wait entry.
	Event string
	// WakeAt is a sleep's wake time (unix nanos).
	WakeAt int64
	// Deadline is a wait's timeout (unix nanos; 0 = no timeout).
	Deadline int64
	// Payload is a delivered event's payload (wait entries).
	Payload []byte
	// Result is a RunOnce result.
	Result []byte
	// Err is a memoized RunOnce error string.
	Err string
	// Done notes the await is satisfied; for a wait it means delivered or
	// timed out.
	Done bool
	// TimedOut marks a wait that expired (done, but no payload).
	TimedOut bool
}

const journalCols = `step_id, idx, kind, key, event, wake_at, deadline, payload, result, err, done, timed_out`

func scanJournalEntry(row interface{ Scan(...any) error }) (*JournalEntry, error) {
	var e JournalEntry
	if err := row.Scan(&e.StepID, &e.Idx, &e.Kind, &e.Key, &e.Event, &e.WakeAt, &e.Deadline,
		&e.Payload, &e.Result, &e.Err, &e.Done, &e.TimedOut); err != nil {
		return nil, err
	}
	return &e, nil
}

// LoadJournal returns a step's journal entries in call order.
func (s *Store) LoadJournal(ctx context.Context, stepID string) ([]*JournalEntry, error) {
	rows, err := s.read.QueryContext(ctx,
		`SELECT `+journalCols+` FROM step_journal WHERE step_id=? ORDER BY idx`, stepID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*JournalEntry
	for rows.Next() {
		e, err := scanJournalEntry(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// AppendJournal records a new await entry.
func (s *Store) AppendJournal(ctx context.Context, e *JournalEntry) error {
	_, err := s.write.ExecContext(ctx, `INSERT INTO step_journal
		(step_id, idx, kind, key, event, wake_at, deadline, payload, result, err, done, timed_out)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
		e.StepID, e.Idx, e.Kind, e.Key, e.Event, e.WakeAt, e.Deadline,
		e.Payload, e.Result, e.Err, e.Done, e.TimedOut)
	return err
}

// CompleteJournalOnce memoizes a successful RunOnce result.
func (s *Store) CompleteJournalOnce(ctx context.Context, stepID string, idx int64, result []byte, errMsg string) error {
	_, err := s.write.ExecContext(ctx,
		`UPDATE step_journal SET done=TRUE, result=?, err=? WHERE step_id=? AND idx=?`,
		result, errMsg, stepID, idx)
	return err
}

// ErrStepNotRunning is returned when a durable helper tries to suspend a step
// that is no longer RUNNING — typically because it was cancelled concurrently.
var ErrStepNotRunning = errors.New("quacker: step is no longer running")

// SuspendStep moves a RUNNING step to SUSPENDED with its resume policy.
// resumeAt=0 means event-only (never self-claims). waitKind/waitEvent describe
// what it waits on, for introspection.
func (s *Store) SuspendStep(ctx context.Context, stepID, waitKind, waitEvent string, resumeAt, now int64) error {
	res, err := s.write.ExecContext(ctx, `UPDATE steps SET
		status=?, resume_at=?, wait_kind=?, wait_event=?
		WHERE id=? AND status=?`,
		StatusSuspended, resumeAt, waitKind, waitEvent, stepID, StatusRunning)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrStepNotRunning
	}
	return nil
}

// SuspendWithJournal records a durable await and moves the RUNNING step to
// SUSPENDED in one transaction. Doing both atomically is what makes event
// delivery race-free: a wait entry is never visible while its step is still
// RUNNING (an Emit between the two would otherwise be lost when the step then
// suspended with resume_at=0).
func (s *Store) SuspendWithJournal(ctx context.Context, e *JournalEntry, waitKind, waitEvent string, resumeAt, now int64) error {
	tx, err := s.beginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.exec(ctx, `UPDATE steps SET
		status=?, resume_at=?, wait_kind=?, wait_event=?
		WHERE id=? AND status=?`,
		StatusSuspended, resumeAt, waitKind, waitEvent, e.StepID, StatusRunning)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrStepNotRunning
	}
	if _, err := tx.exec(ctx, `INSERT INTO step_journal
		(step_id, idx, kind, key, event, wake_at, deadline, payload, result, err, done, timed_out)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
		e.StepID, e.Idx, e.Kind, e.Key, e.Event, e.WakeAt, e.Deadline,
		e.Payload, e.Result, e.Err, e.Done, e.TimedOut); err != nil {
		return err
	}
	return tx.Commit()
}

// GetJournalEntry re-reads one entry (used to resolve an Emit/timeout race).
func (s *Store) GetJournalEntry(ctx context.Context, stepID string, idx int64) (*JournalEntry, error) {
	e, err := scanJournalEntry(s.read.QueryRowContext(ctx,
		`SELECT `+journalCols+` FROM step_journal WHERE step_id=? AND idx=?`, stepID, idx))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return e, err
}

// TimeoutWait atomically marks an undone wait timed out. It returns true when
// this call won; false means an Emit delivered the wait first, so the caller
// must use the payload instead of the timeout.
func (s *Store) TimeoutWait(ctx context.Context, stepID string, idx int64) (bool, error) {
	res, err := s.write.ExecContext(ctx,
		`UPDATE step_journal SET timed_out=TRUE, done=TRUE WHERE step_id=? AND idx=? AND done=FALSE`, stepID, idx)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// DeliverEvent records an event and wakes every waiting step bound to it in
// one transaction (at-least-once per waiter), returning how many were woken.
// Only undone wait entries are considered, so a wait that already timed out is
// never resurrected, and events emitted before a wait registered do not count.
func (s *Store) DeliverEvent(ctx context.Context, name string, payload []byte, now int64) (int, error) {
	tx, err := s.beginTx(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if _, err := tx.exec(ctx,
		`INSERT INTO events (name, payload, created_at) VALUES (?,?,?)`, name, payload, now); err != nil {
		return 0, err
	}
	rows, err := tx.query(ctx, `UPDATE step_journal SET payload=?, done=TRUE
		WHERE kind=? AND done=FALSE AND event=?
		  AND step_id IN (
			SELECT st.id FROM steps st JOIN runs r ON r.id = st.run_id
			WHERE r.status NOT IN (?,?,?,?))
		RETURNING step_id`,
		payload, JournalWait, name, StatusSucceeded, StatusFailed, StatusCancelled, StatusInterrupted)
	if err != nil {
		return 0, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()
	for _, id := range ids {
		if _, err := tx.exec(ctx, `UPDATE steps SET status=?, resume_at=0, wait_kind='', wait_event='' WHERE id=? AND status=?`,
			StatusQueued, id, StatusSuspended); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(ids), nil
}
