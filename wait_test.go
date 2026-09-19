package quacker

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// TestWaitForDeliversPayload: a waiting step resumes when the event is
// emitted, with the payload decoded.
func TestWaitForDeliversPayload(t *testing.T) {
	q := newTestQ(t)
	task := NewTask("waiter", func(ctx context.Context, in greetIn) (greetOut, error) {
		v, err := WaitFor[greetIn](ctx, "go", 5*time.Second)
		if err != nil {
			return greetOut{}, err
		}
		return greetOut{Greeting: "hi " + v.Name}, nil
	})
	h, err := Enqueue(context.Background(), q, task, greetIn{})
	if err != nil {
		t.Fatal(err)
	}
	waitSuspended(t, q, h.RunID())
	snap, _ := q.Execution(context.Background(), h.RunID())
	if snap.Steps[0].WaitEvent != "go" {
		t.Fatalf("wait event = %q, want go", snap.Steps[0].WaitEvent)
	}
	if _, err := q.Emit(context.Background(), "go", greetIn{Name: "z"}); err != nil {
		t.Fatal(err)
	}
	out, err := h.Result(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out.Greeting != "hi z" {
		t.Fatalf("output = %q, want hi z", out.Greeting)
	}
}

// TestWaitForBroadcast: one emit wakes every waiting run.
func TestWaitForBroadcast(t *testing.T) {
	q := newTestQ(t)
	task := NewTask("bcast", func(ctx context.Context, in greetIn) (greetOut, error) {
		v, err := WaitFor[greetIn](ctx, "ping", 5*time.Second)
		if err != nil {
			return greetOut{}, err
		}
		return greetOut{Greeting: v.Name}, nil
	})
	var hs []*RunHandle[greetOut]
	for i := 0; i < 2; i++ {
		h, err := Enqueue(context.Background(), q, task, greetIn{})
		if err != nil {
			t.Fatal(err)
		}
		hs = append(hs, h)
	}
	for _, h := range hs {
		waitSuspended(t, q, h.RunID())
	}
	if _, err := q.Emit(context.Background(), "ping", greetIn{Name: "all"}); err != nil {
		t.Fatal(err)
	}
	for _, h := range hs {
		out, err := h.Result(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if out.Greeting != "all" {
			t.Fatalf("output = %q, want all", out.Greeting)
		}
	}
}

// TestWaitForTimeout: a bounded wait returns ErrWaitTimeout.
func TestWaitForTimeout(t *testing.T) {
	q := newTestQ(t)
	task := NewTask("timeout-wait", func(ctx context.Context, in greetIn) (greetOut, error) {
		_, err := WaitFor[greetIn](ctx, "never", 40*time.Millisecond)
		if errors.Is(err, ErrWaitTimeout) {
			return greetOut{Greeting: "timedout"}, nil
		}
		return greetOut{}, err
	})
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	out, err := h.Result(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out.Greeting != "timedout" {
		t.Fatalf("output = %q, want timedout", out.Greeting)
	}
}

// TestWaitForSubscriptionSemantics: an event emitted before the wait
// registers does not satisfy it.
func TestWaitForSubscriptionSemantics(t *testing.T) {
	q := newTestQ(t)
	if _, err := q.Emit(context.Background(), "early", greetIn{Name: "x"}); err != nil {
		t.Fatal(err)
	}
	task := NewTask("late-wait", func(ctx context.Context, in greetIn) (greetOut, error) {
		_, err := WaitFor[greetIn](ctx, "early", 50*time.Millisecond)
		if errors.Is(err, ErrWaitTimeout) {
			return greetOut{Greeting: "missed"}, nil
		}
		return greetOut{}, err
	})
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	out, err := h.Result(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out.Greeting != "missed" {
		t.Fatalf("output = %q, want the earlier event ignored", out.Greeting)
	}
}

// TestEmitWakesWaitersAndTriggersBindings: one emit both resumes durable
// waiters and enqueues On-bound runs.
func TestEmitWakesWaitersAndTriggersBindings(t *testing.T) {
	q := newTestQ(t)
	bound := NewTask("combo-bound", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	})
	if err := On(q, "combo", bound); err != nil {
		t.Fatal(err)
	}
	waiter := NewTask("combo-wait", func(ctx context.Context, in greetIn) (greetOut, error) {
		v, err := WaitFor[greetIn](ctx, "combo", 5*time.Second)
		if err != nil {
			return greetOut{}, err
		}
		return greetOut{Greeting: "woke " + v.Name}, nil
	})
	h, _ := Enqueue(context.Background(), q, waiter, greetIn{})
	waitSuspended(t, q, h.RunID())

	n, err := q.Emit(context.Background(), "combo", greetIn{Name: "z"})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("On bindings dispatched = %d, want 1", n)
	}
	out, err := h.Result(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out.Greeting != "woke z" {
		t.Fatalf("waiter output = %q, want woke z", out.Greeting)
	}
}

// TestCancelWaitingRun: a run blocked on WaitFor is cancellable.
func TestCancelWaitingRun(t *testing.T) {
	q := newTestQ(t)
	task := NewTask("cancel-wait", func(ctx context.Context, in greetIn) (greetOut, error) {
		_, err := WaitFor[greetIn](ctx, "forever", 0)
		return greetOut{}, err
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

// TestWaitForSurvivesRestart: a durable wait persisted by one process is
// delivered by an emit in the next.
func TestWaitForSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	task := NewTask("pw", func(ctx context.Context, in greetIn) (greetOut, error) {
		v, err := WaitFor[greetIn](ctx, "pw.event", 0)
		if err != nil {
			return greetOut{}, err
		}
		return greetOut{Greeting: v.Name}, nil
	})
	q1 := openFileQ(t, path)
	h, err := Enqueue(context.Background(), q1, task, greetIn{})
	if err != nil {
		t.Fatal(err)
	}
	waitSuspended(t, q1, h.RunID())
	closeQ(t, q1)

	task2 := NewTask("pw", func(ctx context.Context, in greetIn) (greetOut, error) {
		v, err := WaitFor[greetIn](ctx, "pw.event", 0)
		if err != nil {
			return greetOut{}, err
		}
		return greetOut{Greeting: v.Name}, nil
	})
	q2 := openFileQ(t, path)
	defer closeQ(t, q2)
	Register(q2, task2)

	if _, err := q2.Emit(context.Background(), "pw.event", greetIn{Name: "done"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		s, err := q2.Execution(context.Background(), h.RunID())
		return err == nil && s.Status == StatusSucceeded
	})
	s, _ := q2.Execution(context.Background(), h.RunID())
	if string(s.Output) != `{"Greeting":"done"}` {
		t.Fatalf("output = %s", s.Output)
	}
}

// TestWaitForOutsideTask: helper misuse is a clear error.
func TestWaitForOutsideTask(t *testing.T) {
	if _, err := WaitFor[greetIn](context.Background(), "x", 0); err == nil {
		t.Fatal("expected an error outside a task")
	}
}
