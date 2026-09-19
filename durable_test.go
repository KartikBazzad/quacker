package quacker

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// waitSuspended polls until the run's first step is SUSPENDED.
func waitSuspended(t *testing.T, q *Quacker, runID string) {
	t.Helper()
	waitFor(t, 2*time.Second, func() bool {
		s, err := q.Execution(context.Background(), runID)
		return err == nil && len(s.Steps) == 1 && s.Steps[0].Status == StatusSuspended
	})
}

// TestSleepDurableResumes: a sleeping step suspends, frees its slot, and
// completes after the wake time; the attempt count is untouched and the
// snapshot exposes the resume time.
func TestSleepDurableResumes(t *testing.T) {
	q := newTestQ(t)
	task := NewTask("sleepy", func(ctx context.Context, in greetIn) (greetOut, error) {
		if err := SleepDurable(ctx, 40*time.Millisecond); err != nil {
			return greetOut{}, err
		}
		return greetOut{Greeting: "woke"}, nil
	})
	// Measure from enqueue: the sleep begins when the task first runs, so
	// timing from after waitSuspended would miss the part already elapsed.
	start := time.Now()
	h, err := Enqueue(context.Background(), q, task, greetIn{})
	if err != nil {
		t.Fatal(err)
	}
	waitSuspended(t, q, h.RunID())
	snap, _ := q.Execution(context.Background(), h.RunID())
	if snap.Steps[0].ResumeAt.IsZero() {
		t.Fatal("suspended step has no ResumeAt")
	}

	out, err := h.Result(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out.Greeting != "woke" {
		t.Fatalf("output = %q", out.Greeting)
	}
	if elapsed := time.Since(start); elapsed < 35*time.Millisecond {
		t.Fatalf("resumed after %v, want a real sleep", elapsed)
	}
	final, _ := q.Execution(context.Background(), h.RunID())
	if final.Steps[0].Attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (a suspend must not consume an attempt)", final.Steps[0].Attempts)
	}
	if final.Status != StatusSucceeded {
		t.Fatalf("status = %s, want SUCCEEDED", final.Status)
	}
}

// TestSleepFreesQueueSlot: a suspended step releases its queue slot, so other
// work runs while it sleeps.
func TestSleepFreesQueueSlot(t *testing.T) {
	q := newTestQ(t, WithQueue("serial", 1))
	var bRan atomic.Bool
	a := NewTask("slot-a", func(ctx context.Context, in greetIn) (greetOut, error) {
		if err := SleepDurable(ctx, 250*time.Millisecond); err != nil {
			return greetOut{}, err
		}
		return greetOut{}, nil
	}, Queue("serial"))
	b := NewTask("slot-b", func(ctx context.Context, in greetIn) (greetOut, error) {
		bRan.Store(true)
		return greetOut{}, nil
	}, Queue("serial"))

	ha, err := Enqueue(context.Background(), q, a, greetIn{})
	if err != nil {
		t.Fatal(err)
	}
	hb, err := Enqueue(context.Background(), q, b, greetIn{})
	if err != nil {
		t.Fatal(err)
	}
	waitSuspended(t, q, ha.RunID())
	// b runs while a sleeps.
	waitFor(t, 200*time.Millisecond, func() bool { return bRan.Load() })
	if _, err := hb.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := ha.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestAttemptTimeoutPerSegment: the per-attempt timeout covers active
// execution, not suspended time, so a sleep longer than the timeout succeeds.
func TestAttemptTimeoutPerSegment(t *testing.T) {
	q := newTestQ(t)
	task := NewTask("seg-timeout", func(ctx context.Context, in greetIn) (greetOut, error) {
		if err := SleepDurable(ctx, 120*time.Millisecond); err != nil {
			return greetOut{}, err
		}
		return greetOut{}, nil
	}, Timeout(50*time.Millisecond))
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatalf("sleep longer than the attempt timeout should still succeed: %v", err)
	}
}

// TestRunOnceMemoizesAcrossSleep: a side effect wrapped in RunOnce happens
// exactly once across the suspend/resume replay.
func TestRunOnceMemoizesAcrossSleep(t *testing.T) {
	q := newTestQ(t)
	var effects atomic.Int32
	task := NewTask("once-sleep", func(ctx context.Context, in greetIn) (greetOut, error) {
		v, err := RunOnce(ctx, "charge", func() (string, error) {
			effects.Add(1)
			return "receipt-1", nil
		})
		if err != nil {
			return greetOut{}, err
		}
		if err := SleepDurable(ctx, 30*time.Millisecond); err != nil {
			return greetOut{}, err
		}
		return greetOut{Greeting: v}, nil
	})
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	out, err := h.Result(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out.Greeting != "receipt-1" {
		t.Fatalf("output = %q, want the memoized value", out.Greeting)
	}
	if n := effects.Load(); n != 1 {
		t.Fatalf("side effect ran %d times, want 1", n)
	}
}

// TestReplayReExecutesPreAwaitCode documents the contract: without RunOnce,
// code before a durable await re-executes on resume.
func TestReplayReExecutesPreAwaitCode(t *testing.T) {
	q := newTestQ(t)
	var before atomic.Int32
	task := NewTask("replay-raw", func(ctx context.Context, in greetIn) (greetOut, error) {
		before.Add(1) // not memoized
		if err := SleepDurable(ctx, 30*time.Millisecond); err != nil {
			return greetOut{}, err
		}
		return greetOut{}, nil
	})
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := before.Load(); n != 2 {
		t.Fatalf("pre-await code ran %d times, want 2 (initial + replay)", n)
	}
}

// TestRunOnceRerunsOnFailure: an errored RunOnce is not memoized; a retry
// re-invokes fn, then memoizes the success.
func TestRunOnceRerunsOnFailure(t *testing.T) {
	q := newTestQ(t)
	var calls atomic.Int32
	task := NewTask("once-fail", func(ctx context.Context, in greetIn) (greetOut, error) {
		v, err := RunOnce(ctx, "flaky", func() (int, error) {
			if calls.Add(1) == 1 {
				return 0, errors.New("boom")
			}
			return 7, nil
		})
		if err != nil {
			return greetOut{}, err
		}
		return greetOut{Greeting: fmt.Sprint(v)}, nil
	}, Retries(1), BackoffPolicy(Constant(time.Millisecond)))
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	out, err := h.Result(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out.Greeting != "7" {
		t.Fatalf("output = %q, want 7", out.Greeting)
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("fn ran %d times, want 2", n)
	}
}

// TestJournalMisaligned: changing a task's durable-call shape across a
// suspend fails loudly instead of resuming with wrong data.
func TestJournalMisaligned(t *testing.T) {
	q := newTestQ(t)
	v1 := NewTask("evolve", func(ctx context.Context, in greetIn) (greetOut, error) {
		if err := SleepDurable(ctx, 300*time.Millisecond); err != nil {
			return greetOut{}, err
		}
		return greetOut{}, nil
	})
	h, _ := Enqueue(context.Background(), q, v1, greetIn{})
	waitSuspended(t, q, h.RunID())

	// "Deploy" a changed task that calls RunOnce where the journal has a sleep.
	v2 := NewTask("evolve", func(ctx context.Context, in greetIn) (greetOut, error) {
		_, err := RunOnce(ctx, "k", func() (int, error) { return 1, nil })
		return greetOut{}, err
	})
	Register(q, v2)

	if _, err := h.Result(context.Background()); err == nil {
		t.Fatal("expected a failure from the misaligned journal")
	}
	snap, _ := q.Execution(context.Background(), h.RunID())
	if snap.Status != StatusFailed || !strings.Contains(snap.Error, "journal misaligned") {
		t.Fatalf("run = %s error=%q, want FAILED with a misaligned-journal error", snap.Status, snap.Error)
	}
}

// TestCancelSuspended: a sleeping run can be cancelled.
func TestCancelSuspended(t *testing.T) {
	q := newTestQ(t)
	task := NewTask("cancel-sleep", func(ctx context.Context, in greetIn) (greetOut, error) {
		if err := SleepDurable(ctx, 10*time.Second); err != nil {
			return greetOut{}, err
		}
		return greetOut{}, nil
	})
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	waitSuspended(t, q, h.RunID())
	if err := q.Cancel(h.RunID()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool {
		s, err := q.Execution(context.Background(), h.RunID())
		return err == nil && s.Status == StatusCancelled
	})
}

// TestSleepSurvivesRestart: a suspended sleep persists and completes after a
// process restart, with the task re-registered.
func TestSleepSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	task := NewTask("restart-sleep", func(ctx context.Context, in greetIn) (greetOut, error) {
		if err := SleepDurable(ctx, 200*time.Millisecond); err != nil {
			return greetOut{}, err
		}
		return greetOut{}, nil
	})
	q1 := openFileQ(t, path)
	h, err := Enqueue(context.Background(), q1, task, greetIn{})
	if err != nil {
		t.Fatal(err)
	}
	waitSuspended(t, q1, h.RunID())
	closeQ(t, q1)

	task2 := NewTask("restart-sleep", func(ctx context.Context, in greetIn) (greetOut, error) {
		if err := SleepDurable(ctx, 200*time.Millisecond); err != nil {
			return greetOut{}, err
		}
		return greetOut{}, nil
	})
	q2 := openFileQ(t, path)
	defer closeQ(t, q2)
	Register(q2, task2)

	waitFor(t, 3*time.Second, func() bool {
		s, err := q2.Execution(context.Background(), h.RunID())
		return err == nil && s.Status == StatusSucceeded
	})
}

// TestSleepDurableOutsideTask: helper misuse is a clear error.
func TestSleepDurableOutsideTask(t *testing.T) {
	if err := SleepDurable(context.Background(), time.Millisecond); err == nil {
		t.Fatal("expected an error outside a task")
	}
}
