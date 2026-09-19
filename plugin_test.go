package quacker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type funcPlugin struct {
	name  string
	hooks Hooks
}

func (p funcPlugin) Name() string { return p.name }
func (p funcPlugin) Hooks() Hooks { return p.hooks }

// TestPluginOrdering: Before* run in registration order, After* in reverse,
// wrapped around the task body.
func TestPluginOrdering(t *testing.T) {
	var mu sync.Mutex
	var seq []string
	rec := func(s string) { mu.Lock(); seq = append(seq, s); mu.Unlock() }
	mk := func(name string) funcPlugin {
		return funcPlugin{name, Hooks{
			BeforeStep: func(ctx context.Context, s StepInfo) (context.Context, error) {
				rec(name + ".before")
				return ctx, nil
			},
			AfterStep: func(ctx context.Context, s StepInfo, err error) {
				rec(name + ".after")
			},
		}}
	}
	q := newTestQ(t, WithPlugin(mk("A")), WithPlugin(mk("B")))
	task := NewTask("order", func(ctx context.Context, in greetIn) (greetOut, error) {
		rec("body")
		return greetOut{}, nil
	})
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{"A.before", "B.before", "body", "B.after", "A.after"}
	if len(seq) != len(want) {
		t.Fatalf("order = %v, want %v", seq, want)
	}
	for i := range want {
		if seq[i] != want[i] {
			t.Fatalf("order = %v, want %v", seq, want)
		}
	}
}

// TestPluginStepVeto: a BeforeStep error fails the step immediately, without
// retries, and skips the body and AfterStep.
func TestPluginStepVeto(t *testing.T) {
	var bodyRan, afterCalled atomic.Bool
	p := funcPlugin{"veto", Hooks{
		BeforeStep: func(ctx context.Context, s StepInfo) (context.Context, error) {
			return ctx, errors.New("admission denied")
		},
		AfterStep: func(ctx context.Context, s StepInfo, err error) { afterCalled.Store(true) },
	}}
	q := newTestQ(t, WithPlugin(p))
	task := NewTask("vetoed", func(ctx context.Context, in greetIn) (greetOut, error) {
		bodyRan.Store(true)
		return greetOut{}, nil
	}, Retries(3))
	h, err := Enqueue(context.Background(), q, task, greetIn{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(context.Background()); err == nil {
		t.Fatal("expected the vetoed run to fail")
	}
	snap, _ := q.Execution(context.Background(), h.RunID())
	if snap.Status != StatusFailed || snap.Steps[0].Attempts != 1 {
		t.Fatalf("status=%s attempts=%d, want FAILED with 1 attempt (no retries)", snap.Status, snap.Steps[0].Attempts)
	}
	if bodyRan.Load() {
		t.Fatal("task body ran despite veto")
	}
	if afterCalled.Load() {
		t.Fatal("AfterStep ran for a vetoed step")
	}
}

// TestPluginEnqueueVeto: a BeforeEnqueue error aborts the enqueue; a batch is
// all-or-nothing.
func TestPluginEnqueueVeto(t *testing.T) {
	p := funcPlugin{"gate", Hooks{
		BeforeEnqueue: func(ctx context.Context, e EnqueueInfo) (context.Context, error) {
			return ctx, errors.New("closed for business")
		},
	}}
	q := newTestQ(t, WithPlugin(p))
	task := NewTask("nope", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	})
	if _, err := Enqueue(context.Background(), q, task, greetIn{}); err == nil {
		t.Fatal("expected enqueue veto")
	}
	if _, err := EnqueueBatch(context.Background(), q, task, []greetIn{{}, {}}); err == nil {
		t.Fatal("expected batch enqueue veto")
	}
	runs, _ := q.Runs(context.Background(), RunFilter{Workflow: "nope"})
	if len(runs) != 0 {
		t.Fatalf("runs = %d, want 0", len(runs))
	}
}

// TestPluginEmitVeto: a BeforeEmit error stops the event being recorded.
func TestPluginEmitVeto(t *testing.T) {
	p := funcPlugin{"gate", Hooks{
		BeforeEmit: func(ctx context.Context, e EmitInfo) (context.Context, error) {
			return ctx, errors.New("no emits today")
		},
	}}
	q := newTestQ(t, WithPlugin(p))
	if _, err := q.Emit(context.Background(), "x", greetIn{}); err == nil {
		t.Fatal("expected emit veto")
	}
	evs, _ := q.Events(context.Background(), 10)
	if len(evs) != 0 {
		t.Fatalf("events = %d, want 0 (veto must not persist)", len(evs))
	}
}

// TestPluginPanicRecovery: a panicking BeforeStep vetoes; a panicking AfterStep
// is recovered and the run still succeeds.
func TestPluginPanicRecovery(t *testing.T) {
	before := funcPlugin{"panic-before", Hooks{
		BeforeStep: func(ctx context.Context, s StepInfo) (context.Context, error) { panic("boom") },
	}}
	q := newTestQ(t, WithPlugin(before))
	task := NewTask("pb", func(ctx context.Context, in greetIn) (greetOut, error) { return greetOut{}, nil })
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	if _, err := h.Result(context.Background()); err == nil {
		t.Fatal("expected the panicking BeforeStep to fail the run")
	}

	after := funcPlugin{"panic-after", Hooks{
		AfterStep: func(ctx context.Context, s StepInfo, err error) { panic("boom") },
	}}
	q2 := newTestQ(t, WithPlugin(after))
	h2, _ := Enqueue(context.Background(), q2, task, greetIn{})
	if _, err := h2.Result(context.Background()); err != nil {
		t.Fatalf("AfterStep panic should be recovered, got %v", err)
	}
}

// TestPluginPerAttempt: hooks wrap every attempt.
func TestPluginPerAttempt(t *testing.T) {
	var before, after atomic.Int32
	p := funcPlugin{"counter", Hooks{
		BeforeStep: func(ctx context.Context, s StepInfo) (context.Context, error) {
			before.Add(1)
			return ctx, nil
		},
		AfterStep: func(ctx context.Context, s StepInfo, err error) { after.Add(1) },
	}}
	q := newTestQ(t, WithPlugin(p))
	var calls atomic.Int32
	task := NewTask("flaky", func(ctx context.Context, in greetIn) (greetOut, error) {
		if calls.Add(1) == 1 {
			return greetOut{}, errors.New("transient")
		}
		return greetOut{}, nil
	}, Retries(2), BackoffPolicy(Constant(time.Millisecond)))
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	if before.Load() != 2 || after.Load() != 2 {
		t.Fatalf("before=%d after=%d, want 2/2", before.Load(), after.Load())
	}
}

// TestPluginRunFinished: OnRunFinished fires with the terminal status.
func TestPluginRunFinished(t *testing.T) {
	got := make(chan RunInfo, 1)
	p := funcPlugin{"observer", Hooks{
		OnRunFinished: func(ctx context.Context, r RunInfo) { got <- r },
	}}
	q := newTestQ(t, WithPlugin(p))
	task := NewTask("obs", func(ctx context.Context, in greetIn) (greetOut, error) { return greetOut{}, nil })
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-got:
		if r.RunID != h.RunID() || r.Status != string(StatusSucceeded) || r.Workflow != "obs" {
			t.Fatalf("run info = %+v", r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnRunFinished never fired")
	}
}

// TestPluginConcurrent: hooks are invoked safely for parallel steps.
func TestPluginConcurrent(t *testing.T) {
	var count atomic.Int64
	p := funcPlugin{"concurrent", Hooks{
		BeforeStep: func(ctx context.Context, s StepInfo) (context.Context, error) {
			count.Add(1)
			return ctx, nil
		},
	}}
	q := newTestQ(t, WithPlugin(p))
	task := NewTask("cc", func(ctx context.Context, in greetIn) (greetOut, error) { return greetOut{}, nil })
	inputs := make([]greetIn, 50)
	hs, err := EnqueueBatch(context.Background(), q, task, inputs)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range hs {
		if _, err := h.Result(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if count.Load() != 50 {
		t.Fatalf("BeforeStep calls = %d, want 50", count.Load())
	}
}
