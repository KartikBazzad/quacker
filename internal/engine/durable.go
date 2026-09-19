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
// the executor can record the step SUSPENDED. It is a panic (not an error)
// so a task cannot accidentally swallow a suspension by ignoring an error;
// the flip side is that recovering over it — in a task or in middleware —
// breaks durability.
type suspendSignal struct {
	waitKind string
	event    string
	resumeAt int64
}

// ErrJournalMisaligned is returned when a durable task's replayed calls do
// not match its persisted journal — the usual cause is changing task code
// (reordering, adding, or removing durable calls) while runs are suspended.
// It is a hard failure rather than silent wrong-typed data.
var ErrJournalMisaligned = errors.New("quacker: durable journal misaligned (task code changed across a suspend?)")

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
		done := d <= 0
		entry := &store.JournalEntry{StepID: ss.stepID, Idx: idx, Kind: store.JournalSleep, WakeAt: wake, Done: done}
		if err := ss.eng.st.AppendJournal(context.Background(), entry); err != nil {
			return err
		}
		ss.journal = append(ss.journal, entry)
		if done {
			return nil
		}
		panic(&suspendSignal{waitKind: "sleep", resumeAt: wake})
	}
	// Replay: the entry exists.
	if e.WakeAt > now {
		// Clock skew or a manual resume before the wake time: re-suspend for
		// the remainder rather than proceeding early.
		panic(&suspendSignal{waitKind: "sleep", resumeAt: e.WakeAt})
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
