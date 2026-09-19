package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/kartikbazzad/quacker/internal/store"
)

// suspendSignal is the internal panic payload that unwinds a durable task so
// the executor can publish its SUSPENDED transition. The helper has already
// recorded the suspension in the store (atomically with its journal entry);
// the panic only unwinds the task. It is a panic (not an error) so a task
// cannot accidentally swallow a suspension by ignoring an error; the flip
// side is that recovering over it — in a task or in middleware — breaks
// durability.
type suspendSignal struct{}

// ErrJournalMisaligned is returned when a durable task's replayed calls do
// not match its persisted journal — the usual cause is changing task code
// (reordering, adding, or removing durable calls) while runs are suspended.
// It is a hard failure rather than silent wrong-typed data.
var ErrJournalMisaligned = errors.New("quacker: durable journal misaligned (task code changed across a suspend?)")

// ErrWaitTimeout is returned by WaitFor when its timeout elapses before the
// awaited event arrives.
var ErrWaitTimeout = errors.New("quacker: wait timed out")

func stepStateFrom(ctx context.Context, fn string) (*stepState, error) {
	ss, ok := ctx.Value(ctxKey{}).(*stepState)
	if !ok || ss == nil {
		return nil, fmt.Errorf("quacker: %s may only be called inside a task", fn)
	}
	if ss.eng == nil {
		return nil, fmt.Errorf("quacker: %s: step context has no engine", fn)
	}
	return ss, nil
}

// takeEntry consumes the next journal slot, validating its kind against the
// call. It returns the slot index and the existing entry (nil if new).
func (ss *stepState) takeEntry(fn, kind string) (int64, *store.JournalEntry, error) {
	idx := int64(ss.cursor)
	ss.cursor++
	if idx < int64(len(ss.journal)) {
		e := ss.journal[idx]
		if e.Kind != kind {
			return idx, e, fmt.Errorf("%w: %s call #%d expected kind %q, journal has %q", ErrJournalMisaligned, fn, idx, kind, e.Kind)
		}
		return idx, e, nil
	}
	return idx, nil, nil
}

// SleepDurable suspends the running step for at least d without consuming an
// attempt or holding a queue or per-key slot. On resume the task is
// re-invoked from the top and this call returns nil immediately.
//
// Code before a durable await re-executes on resume; wrap side effects in
// RunOnce. Do not recover() around a durable helper.
func SleepDurable(ctx context.Context, d time.Duration) error {
	ss, err := stepStateFrom(ctx, "SleepDurable")
	if err != nil {
		return err
	}
	return ss.sleep(d)
}

func (ss *stepState) sleep(d time.Duration) error {
	idx, e, err := ss.takeEntry("SleepDurable", store.JournalSleep)
	if err != nil {
		return err
	}
	now := ss.eng.now().UnixNano()
	if e == nil {
		wake := now + int64(d)
		entry := &store.JournalEntry{StepID: ss.stepID, Idx: idx, Kind: store.JournalSleep, WakeAt: wake, Done: d <= 0}
		if d <= 0 {
			if aerr := ss.eng.st.AppendJournal(context.Background(), entry); aerr != nil {
				return aerr
			}
			ss.journal = append(ss.journal, entry)
			return nil
		}
		if serr := ss.eng.st.SuspendWithJournal(context.Background(), entry, "sleep", "", wake, now); serr != nil {
			return serr
		}
		ss.journal = append(ss.journal, entry)
		panic(&suspendSignal{})
	}
	// Replay: the entry exists.
	if e.WakeAt > now {
		// Clock skew or a manual resume before the wake time: re-suspend for
		// the remainder rather than proceeding early.
		if serr := ss.eng.st.SuspendStep(context.Background(), ss.stepID, "sleep", "", e.WakeAt, now); serr != nil {
			return serr
		}
		panic(&suspendSignal{})
	}
	return nil
}

// RunOnce runs fn at most once per step journal slot, memoizing its result
// across suspensions and retries. It never suspends. On fn error the entry is
// left undone, so a retry re-invokes fn — exactly-once on success,
// at-least-once on failure. key is validated on replay.
func RunOnce[T any](ctx context.Context, key string, fn func() (T, error)) (T, error) {
	var zero T
	ss, err := stepStateFrom(ctx, "RunOnce")
	if err != nil {
		return zero, err
	}
	return runOnce[T](ss, key, fn)
}

func runOnce[T any](ss *stepState, key string, fn func() (T, error)) (T, error) {
	var zero T
	idx, e, err := ss.takeEntry("RunOnce", store.JournalOnce)
	if err != nil {
		return zero, err
	}
	if e != nil && e.Key != key {
		return zero, fmt.Errorf("%w: RunOnce call #%d expected key %q, journal has %q", ErrJournalMisaligned, idx, key, e.Key)
	}
	if e != nil && e.Done {
		if e.Err != "" {
			return zero, errors.New(e.Err)
		}
		if len(e.Result) > 0 {
			if uerr := json.Unmarshal(e.Result, &zero); uerr != nil {
				return zero, fmt.Errorf("quacker: RunOnce %q: decode memoized result: %w", key, uerr)
			}
		}
		return zero, nil
	}
	if e == nil {
		e = &store.JournalEntry{StepID: ss.stepID, Idx: idx, Kind: store.JournalOnce, Key: key}
		if aerr := ss.eng.st.AppendJournal(context.Background(), e); aerr != nil {
			return zero, aerr
		}
		ss.journal = append(ss.journal, e)
	}
	out, ferr := fn()
	if ferr != nil {
		return zero, ferr // leave the entry undone; a retry re-runs fn
	}
	res, merr := json.Marshal(out)
	if merr != nil {
		return zero, fmt.Errorf("quacker: RunOnce %q: encode result: %w", key, merr)
	}
	if cerr := ss.eng.st.CompleteJournalOnce(context.Background(), ss.stepID, idx, res, ""); cerr != nil {
		return zero, cerr
	}
	e.Done = true
	e.Result = res
	return out, nil
}

// WaitFor suspends the running step until an event named event is emitted,
// then decodes its payload into T. A timeout > 0 bounds the wait and yields
// ErrWaitTimeout; timeout <= 0 waits indefinitely. Only events emitted after
// the wait registers count (subscription semantics).
func WaitFor[T any](ctx context.Context, event string, timeout time.Duration) (T, error) {
	var zero T
	ss, err := stepStateFrom(ctx, "WaitFor")
	if err != nil {
		return zero, err
	}
	if event == "" {
		return zero, errors.New("quacker: WaitFor requires an event name")
	}
	return waitFor[T](ss, event, timeout)
}

func waitFor[T any](ss *stepState, event string, timeout time.Duration) (T, error) {
	var zero T
	idx, e, err := ss.takeEntry("WaitFor", store.JournalWait)
	if err != nil {
		return zero, err
	}
	if e == nil {
		now := ss.eng.now().UnixNano()
		var deadline int64
		if timeout > 0 {
			deadline = now + int64(timeout)
		}
		entry := &store.JournalEntry{StepID: ss.stepID, Idx: idx, Kind: store.JournalWait, Event: event, Deadline: deadline}
		// resumeAt is the deadline (0 = event-only, never self-claims).
		if serr := ss.eng.st.SuspendWithJournal(context.Background(), entry, "wait", event, deadline, now); serr != nil {
			return zero, serr
		}
		ss.journal = append(ss.journal, entry)
		panic(&suspendSignal{})
	}
	// Replay: kind matched; the event must too.
	if e.Event != event {
		return zero, fmt.Errorf("%w: WaitFor call #%d expected event %q, journal has %q", ErrJournalMisaligned, idx, event, e.Event)
	}
	if e.Done {
		return waitResult[T](e)
	}
	now := ss.eng.now().UnixNano()
	if e.Deadline > 0 && now >= e.Deadline {
		// Timeout. Claim it atomically; if an Emit won the race, return its
		// payload instead — never resurrect a consumed entry.
		won, terr := ss.eng.st.TimeoutWait(context.Background(), ss.stepID, idx)
		if terr != nil {
			return zero, terr
		}
		if won {
			return zero, ErrWaitTimeout
		}
		updated, lerr := ss.eng.st.GetJournalEntry(context.Background(), ss.stepID, idx)
		if lerr != nil {
			return zero, lerr
		}
		if updated != nil && updated.Done && !updated.TimedOut {
			return waitResult[T](updated)
		}
		return zero, ErrWaitTimeout
	}
	// Spurious early resume (clock skew): suspend again for the remainder.
	if serr := ss.eng.st.SuspendStep(context.Background(), ss.stepID, "wait", event, e.Deadline, now); serr != nil {
		return zero, serr
	}
	panic(&suspendSignal{})
}

func waitResult[T any](e *store.JournalEntry) (T, error) {
	var out T
	if e.TimedOut {
		return out, ErrWaitTimeout
	}
	if len(e.Payload) > 0 {
		if uerr := json.Unmarshal(e.Payload, &out); uerr != nil {
			return out, fmt.Errorf("quacker: WaitFor %q: decode payload: %w", e.Event, uerr)
		}
	}
	return out, nil
}
