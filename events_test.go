package quacker

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// TestEmitTriggersBoundTask: emitting an event enqueues its bound task with
// the payload as input.
func TestEmitTriggersBoundTask(t *testing.T) {
	q := newTestQ(t)
	task := NewTask("evt-task", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{Greeting: "hi " + in.Name}, nil
	})
	if err := On(q, "order.placed", task); err != nil {
		t.Fatal(err)
	}
	n, err := q.Emit(context.Background(), "order.placed", greetIn{Name: "a"})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("dispatched %d runs, want 1", n)
	}
	var runID string
	waitFor(t, 3*time.Second, func() bool {
		runs, _ := q.Runs(context.Background(), RunFilter{Workflow: "evt-task", Status: StatusSucceeded})
		if len(runs) == 1 {
			runID = runs[0].RunID
			return true
		}
		return false
	})
	snap, err := q.Execution(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if string(snap.Output) != `{"Greeting":"hi a"}` {
		t.Fatalf("output = %s, want hi a", snap.Output)
	}
}

// TestEmitMultipleBindings: one event with two bound tasks enqueues two runs.
func TestEmitMultipleBindings(t *testing.T) {
	q := newTestQ(t)
	body := func(name string) *Task[greetIn, greetOut] {
		return NewTask(name, func(ctx context.Context, in greetIn) (greetOut, error) {
			return greetOut{}, nil
		})
	}
	if err := On(q, "fanout", body("fan-1")); err != nil {
		t.Fatal(err)
	}
	if err := On(q, "fanout", body("fan-2")); err != nil {
		t.Fatal(err)
	}
	n, err := q.Emit(context.Background(), "fanout", greetIn{})
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("dispatched %d, want 2", n)
	}
}

// TestEmitUnboundPersists: an event with no bindings still persists.
func TestEmitUnboundPersists(t *testing.T) {
	q := newTestQ(t)
	n, err := q.Emit(context.Background(), "nobody", greetIn{Name: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("dispatched %d, want 0", n)
	}
	evs, err := q.Events(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 || evs[0].Name != "nobody" {
		t.Fatalf("events = %+v, want one 'nobody'", evs)
	}
}

// TestOffRemovesBinding: after Off, the event no longer dispatches.
func TestOffRemovesBinding(t *testing.T) {
	q := newTestQ(t)
	task := NewTask("off-task", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	})
	if err := On(q, "go", task); err != nil {
		t.Fatal(err)
	}
	if err := Off(q, "go", task); err != nil {
		t.Fatal(err)
	}
	n, err := q.Emit(context.Background(), "go", greetIn{})
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("dispatched %d after Off, want 0", n)
	}
}

// TestEventsIntrospection: Events returns recent emits newest first.
func TestEventsIntrospection(t *testing.T) {
	q := newTestQ(t)
	for _, name := range []string{"a", "b", "c"} {
		if _, err := q.Emit(context.Background(), name, greetIn{}); err != nil {
			t.Fatal(err)
		}
	}
	evs, err := q.Events(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 3 || evs[0].Name != "c" || evs[2].Name != "a" {
		t.Fatalf("events = %+v, want c,b,a", evs)
	}
}

// TestPersistentEventArming: an event→task binding persisted by one process
// re-arms in the next once the task is registered.
func TestPersistentEventArming(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	task := NewTask("pevt", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	})
	q1 := openFileQ(t, path)
	if err := On(q1, "pevt.event", task); err != nil {
		t.Fatal(err)
	}
	closeQ(t, q1)

	var seen atomic.Int32
	task2 := NewTask("pevt", func(ctx context.Context, in greetIn) (greetOut, error) {
		seen.Add(1)
		return greetOut{}, nil
	})
	q2 := openFileQ(t, path)
	defer closeQ(t, q2)
	Register(q2, task2) // arms the persisted subscription

	n, err := q2.Emit(context.Background(), "pevt.event", greetIn{})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("dispatched %d, want 1", n)
	}
	waitFor(t, 3*time.Second, func() bool { return seen.Load() >= 1 })
}

// TestRetentionPurgesEvents: q.Purge ages events out with terminal runs.
func TestRetentionPurgesEvents(t *testing.T) {
	q := newTestQ(t)
	if _, err := q.Emit(context.Background(), "old.event", greetIn{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Millisecond)
	res, err := q.Purge(context.Background(), PurgeOptions{OlderThan: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if res.Events != 1 {
		t.Fatalf("purged %d events, want 1", res.Events)
	}
	evs, err := q.Events(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 0 {
		t.Fatalf("events remain after purge: %+v", evs)
	}
}
