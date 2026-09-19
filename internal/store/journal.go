package store

import (
	"context"
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
		`UPDATE step_journal SET done=1, result=?, err=? WHERE step_id=? AND idx=?`,
		result, errMsg, stepID, idx)
	return err
}

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
		return errors.New("quacker: suspend: step is not RUNNING")
	}
	return nil
}
