package quacker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

type recordingMW struct {
	mu   *sync.Mutex
	seq  *[]string
	name string
}

// namedMW records its name when entered, then calls next.
func namedMW(name string, mu *sync.Mutex, seq *[]string) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
			mu.Lock()
			*seq = append(*seq, name)
			mu.Unlock()
			return next(ctx, in)
		}
	}
}

// TestMiddlewareOrderAndContext: engine-wide middleware wraps per-task
// Wrap middleware, which wraps the body (global → task → body), and the
// step context exposes RunID/Step/Task/Attempt.
func TestMiddlewareOrderAndContext(t *testing.T) {
	var mu sync.Mutex
	var seq []string
	q := newTestQ(t,
		WithMiddleware(namedMW("g1", &mu, &seq)),
		WithMiddleware(namedMW("g2", &mu, &seq)),
	)
	task := NewTask("mw-task", func(ctx context.Context, in greetIn) (greetOut, error) {
		sc, ok := StepFromContext(ctx)
		if !ok {
			t.Error("no step context in body")
		} else if sc.RunID == "" || sc.Step != "mw-task" || sc.Task != "mw-task" || sc.Attempt != 1 {
			t.Errorf("step context = %+v, want run set, step/task mw-task, attempt 1", sc)
		}
		mu.Lock()
		seq = append(seq, "body")
		mu.Unlock()
		return greetOut{Greeting: "hi"}, nil
	}, Wrap(namedMW("t1", &mu, &seq)))

	h, err := Enqueue(context.Background(), q, task, greetIn{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"g1", "g2", "t1", "body"}
	if len(seq) != len(want) {
		t.Fatalf("order = %v, want %v", seq, want)
	}
	for i := range want {
		if seq[i] != want[i] {
			t.Fatalf("order = %v, want %v", seq, want)
		}
	}
}

// TestMiddlewareTransformsOutput: a middleware may rewrite the JSON output
// that Result and DepOutput observe.
func TestMiddlewareTransformsOutput(t *testing.T) {
	rewrite := func(next Handler) Handler {
		return func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
			out, err := next(ctx, in)
			if err != nil {
				return out, err
			}
			var g greetOut
			if err := json.Unmarshal(out, &g); err != nil {
				return nil, err
			}
			g.Greeting += "!"
			return json.Marshal(g)
		}
	}
	q := newTestQ(t, WithMiddleware(rewrite))
	task := NewTask("mw-out", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{Greeting: "hi"}, nil
	})
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	out, err := h.Result(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out.Greeting != "hi!" {
		t.Fatalf("output = %q, want hi!", out.Greeting)
	}
}

// TestMiddlewareShortCircuit: not calling next skips the task body entirely.
func TestMiddlewareShortCircuit(t *testing.T) {
	var ran bool
	q := newTestQ(t, WithMiddleware(func(next Handler) Handler {
		return func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
			return json.Marshal(greetOut{Greeting: "short"})
		}
	}))
	task := NewTask("mw-short", func(ctx context.Context, in greetIn) (greetOut, error) {
		ran = true
		return greetOut{}, nil
	})
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	out, err := h.Result(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if ran {
		t.Fatal("body ran despite short-circuit")
	}
	if out.Greeting != "short" {
		t.Fatalf("output = %q, want short", out.Greeting)
	}
}

// TestMiddlewarePanicRecovered: a panic in middleware is caught by the same
// recover that guards the task body — the step fails and the engine lives on.
func TestMiddlewarePanicRecovered(t *testing.T) {
	q := newTestQ(t, WithMiddleware(func(next Handler) Handler {
		return func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
			panic("middleware boom")
		}
	}))
	task := NewTask("mw-panic", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	})
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	if _, err := h.Result(context.Background()); err == nil {
		t.Fatal("expected failure from middleware panic")
	}
	snap, err := q.Execution(context.Background(), h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if snap.Status != StatusFailed {
		t.Fatalf("run status = %s, want FAILED", snap.Status)
	}
	// The engine must still be usable.
	h2, _ := Enqueue(context.Background(), q, task, greetIn{})
	if _, err := h2.Result(context.Background()); err == nil {
		t.Fatal("expected failure from middleware panic on second run")
	}
}

// TestMiddlewareRunsPerAttempt: middleware wraps every attempt, so it sees
// Attempt advance across retries.
func TestMiddlewareRunsPerAttempt(t *testing.T) {
	var mu sync.Mutex
	attempts := map[int]int{}
	q := newTestQ(t, WithMiddleware(func(next Handler) Handler {
		return func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
			sc, _ := StepFromContext(ctx)
			mu.Lock()
			attempts[sc.Attempt]++
			mu.Unlock()
			return next(ctx, in)
		}
	}))
	var calls int
	task := NewTask("mw-retry", func(ctx context.Context, in greetIn) (greetOut, error) {
		calls++
		if calls < 3 {
			return greetOut{}, errors.New("transient")
		}
		return greetOut{Greeting: "ok"}, nil
	}, Retries(3), BackoffPolicy(Constant(time.Millisecond)))
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if attempts[1] != 1 || attempts[2] != 1 || attempts[3] != 1 {
		t.Fatalf("middleware attempts = %v, want one each of 1,2,3", attempts)
	}
}

// TestMiddlewareWorkflowStepTaskName: for a named workflow step, Step is the
// DAG identity and Task is the registered task.
func TestMiddlewareWorkflowStepTaskName(t *testing.T) {
	var mu sync.Mutex
	var got []StepContext
	q := newTestQ(t, WithMiddleware(func(next Handler) Handler {
		return func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
			sc, _ := StepFromContext(ctx)
			mu.Lock()
			got = append(got, sc)
			mu.Unlock()
			return next(ctx, in)
		}
	}))
	task := NewTask("charge-fn", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	})
	wf := NewWorkflow[greetIn]("wf", Step("audit", task))
	h, err := EnqueueWorkflow[greetOut](context.Background(), q, wf, greetIn{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0].Step != "audit" || got[0].Task != "charge-fn" {
		t.Fatalf("step context = %+v, want Step=audit Task=charge-fn", got)
	}
}

// TestMiddlewareLogSinkRedirect: middleware can reroute TaskLogger output to
// its own sink, leaving built-in storage untouched.
func TestMiddlewareLogSinkRedirect(t *testing.T) {
	var mu sync.Mutex
	var captured []string
	q := newTestQ(t, WithMiddleware(func(next Handler) Handler {
		return func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
			ctx = WithLogSink(ctx, func(e LogEntry) {
				mu.Lock()
				captured = append(captured, e.Message)
				mu.Unlock()
			})
			return next(ctx, in)
		}
	}))
	task := NewTask("mw-log", func(ctx context.Context, in greetIn) (greetOut, error) {
		TaskLogger(ctx).Info("hello from task")
		return greetOut{}, nil
	})
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(captured) != 1 || captured[0] != "hello from task" {
		t.Fatalf("captured = %v, want the task line", captured)
	}
	// Storage was never enabled, so nothing persisted.
	logs, err := q.Logs(context.Background(), h.RunID(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 0 {
		t.Fatalf("built-in logs = %v, want none", logs)
	}
}

// TestPerTaskMiddlewareDoesNotLeak: a Wrap on one task must not affect
// another task.
func TestPerTaskMiddlewareDoesNotLeak(t *testing.T) {
	var mu sync.Mutex
	wrappedRuns := 0
	q := newTestQ(t)
	wrappedTask := NewTask("wrapped", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	}, Wrap(func(next Handler) Handler {
		return func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
			mu.Lock()
			wrappedRuns++
			mu.Unlock()
			return next(ctx, in)
		}
	}))
	plainTask := NewTask("plain", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	})
	h1, _ := Enqueue(context.Background(), q, wrappedTask, greetIn{})
	h2, _ := Enqueue(context.Background(), q, plainTask, greetIn{})
	for _, h := range []*RunHandle[greetOut]{h1, h2} {
		if _, err := h.Result(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if wrappedRuns != 1 {
		t.Fatalf("wrapped middleware ran %d times, want 1", wrappedRuns)
	}
}
