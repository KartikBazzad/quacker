package quacker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestQ(t *testing.T, opts ...Option) *Quacker {
	t.Helper()
	all := append([]Option{
		WithStorage(Memory()),
		WithPollInterval(5 * time.Millisecond),
	}, opts...)
	q, err := Open(all...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = q.Close(ctx)
	})
	return q
}

// waitFor polls cond until true or the deadline passes.
func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met within", d)
}

type greetIn struct{ Name string }
type greetOut struct{ Greeting string }

func TestTaskSuccess(t *testing.T) {
	q := newTestQ(t)
	task := NewTask("greet", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{Greeting: "hi " + in.Name}, nil
	})
	h, err := Enqueue(context.Background(), q, task, greetIn{Name: "world"})
	if err != nil {
		t.Fatal(err)
	}
	out, err := h.Result(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out.Greeting != "hi world" {
		t.Fatalf("got %q", out.Greeting)
	}
	snap, err := q.Execution(context.Background(), h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if snap.Status != StatusSucceeded {
		t.Fatalf("status = %s", snap.Status)
	}
	if snap.Kind != KindTask || snap.Queue != "default" {
		t.Fatalf("kind=%s queue=%s", snap.Kind, snap.Queue)
	}
	if len(snap.Steps) != 1 || snap.Steps[0].Attempts != 1 || snap.Steps[0].Status != StatusSucceeded {
		t.Fatalf("steps = %+v", snap.Steps)
	}
	if snap.StartedAt.IsZero() || snap.CompletedAt.IsZero() {
		t.Fatal("timestamps not recorded")
	}
}

func TestRetriesThenSuccess(t *testing.T) {
	q := newTestQ(t)
	var attempts atomic.Int32
	task := NewTask("flaky", func(ctx context.Context, in greetIn) (greetOut, error) {
		if attempts.Add(1) < 3 {
			return greetOut{}, fmt.Errorf("boom")
		}
		return greetOut{Greeting: "ok"}, nil
	}, Retries(3), BackoffPolicy(Constant(2*time.Millisecond)))

	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	out, err := h.Result(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out.Greeting != "ok" || attempts.Load() != 3 {
		t.Fatalf("out=%+v attempts=%d", out, attempts.Load())
	}
	snap, _ := q.Execution(context.Background(), h.RunID())
	if snap.Steps[0].Attempts != 3 {
		t.Fatalf("recorded attempts = %d", snap.Steps[0].Attempts)
	}
}

func TestRetriesExhausted(t *testing.T) {
	q := newTestQ(t)
	task := NewTask("always-fails", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, errors.New("kaput")
	}, Retries(2), BackoffPolicy(Constant(1*time.Millisecond)))

	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	_, err := h.Result(context.Background())
	var re *RunError
	if !errors.As(err, &re) {
		t.Fatalf("want *RunError, got %v", err)
	}
	if !strings.Contains(re.Message, "kaput") {
		t.Fatalf("message = %q", re.Message)
	}
	snap, _ := q.Execution(context.Background(), h.RunID())
	if snap.Status != StatusFailed || snap.Steps[0].Attempts != 3 {
		t.Fatalf("status=%s attempts=%d", snap.Status, snap.Steps[0].Attempts)
	}
}

func TestPanicRecovered(t *testing.T) {
	q := newTestQ(t)
	task := NewTask("panics", func(ctx context.Context, in greetIn) (greetOut, error) {
		panic("task exploded")
	})
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	_, err := h.Result(context.Background())
	var re *RunError
	if !errors.As(err, &re) {
		t.Fatalf("want *RunError, got %v", err)
	}
	if !strings.Contains(re.Message, "task exploded") || !strings.Contains(re.Message, "panic") {
		t.Fatalf("message = %q", re.Message)
	}
}

func TestTimeout(t *testing.T) {
	q := newTestQ(t)
	task := NewTask("slow", func(ctx context.Context, in greetIn) (greetOut, error) {
		select {
		case <-time.After(5 * time.Second):
			return greetOut{}, nil
		case <-ctx.Done():
			return greetOut{}, ctx.Err()
		}
	}, Timeout(30*time.Millisecond))

	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	start := time.Now()
	_, err := h.Result(context.Background())
	if err == nil {
		t.Fatal("expected timeout failure")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("timeout not enforced promptly")
	}
	snap, _ := q.Execution(context.Background(), h.RunID())
	if snap.Status != StatusFailed || snap.Steps[0].Attempts != 1 {
		t.Fatalf("status=%s attempts=%d (timeouts must not retry by surprise here: maxAttempts=1)", snap.Status, snap.Steps[0].Attempts)
	}
}

func TestConcurrencyLimit(t *testing.T) {
	q := newTestQ(t, WithQueue("limited", 2))
	var cur, max atomic.Int32
	task := NewTask("counted", func(ctx context.Context, in greetIn) (greetOut, error) {
		n := cur.Add(1)
		for {
			m := max.Load()
			if n <= m || max.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		cur.Add(-1)
		return greetOut{}, nil
	}, Queue("limited"))
	var hs []*RunHandle[greetOut]
	for i := 0; i < 8; i++ {
		h, err := Enqueue(context.Background(), q, task, greetIn{})
		if err != nil {
			t.Fatal(err)
		}
		hs = append(hs, h)
	}
	for _, h := range hs {
		if _, err := h.Result(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if got := max.Load(); got > 2 {
		t.Fatalf("max concurrency = %d, want <= 2", got)
	}
}

func TestPriorityOrdering(t *testing.T) {
	q := newTestQ(t, WithQueue("ordered", 1))
	release := make(chan struct{})
	var mu sync.Mutex
	var order []string
	record := func(name string) { mu.Lock(); order = append(order, name); mu.Unlock() }

	gate := NewTask("gate", func(ctx context.Context, in greetIn) (greetOut, error) {
		<-release
		record("gate")
		return greetOut{}, nil
	}, Queue("ordered"))
	low := NewTask("low", func(ctx context.Context, in greetIn) (greetOut, error) {
		record(in.Name)
		return greetOut{}, nil
	}, Queue("ordered"))
	high := NewTask("high", func(ctx context.Context, in greetIn) (greetOut, error) {
		record(in.Name)
		return greetOut{}, nil
	}, Queue("ordered"))

	hg, _ := Enqueue(context.Background(), q, gate, greetIn{})
	waitFor(t, 2*time.Second, func() bool {
		s, _ := q.Execution(context.Background(), hg.RunID())
		return s != nil && s.Status == StatusRunning
	})
	hl, _ := Enqueue(context.Background(), q, low, greetIn{Name: "low"}, WithPriority(1))
	hh, _ := Enqueue(context.Background(), q, high, greetIn{Name: "high"}, WithPriority(10))
	close(release)
	for _, h := range []*RunHandle[greetOut]{hg, hl, hh} {
		if _, err := h.Result(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(order, ",") != "gate,high,low" {
		t.Fatalf("order = %v", order)
	}
}

type wfIn struct{ ID string }
type receipt struct{ ChargeID string }
type shipment struct{ Tracking string }

func TestWorkflowDAG(t *testing.T) {
	q := newTestQ(t)
	var mu sync.Mutex
	var order []string

	charge := NewTask("charge", func(ctx context.Context, in wfIn) (receipt, error) {
		mu.Lock()
		order = append(order, "charge")
		mu.Unlock()
		return receipt{ChargeID: "ch-" + in.ID}, nil
	})
	ship := NewTask("ship", func(ctx context.Context, in wfIn) (shipment, error) {
		rec, err := DepOutput[receipt](ctx, "charge")
		if err != nil {
			return shipment{}, err
		}
		mu.Lock()
		order = append(order, "ship")
		mu.Unlock()
		return shipment{Tracking: rec.ChargeID + "-track"}, nil
	})

	wf := NewWorkflow[wfIn]("fulfill", Step("charge", charge), Step("ship", ship, "charge"))
	h, err := EnqueueWorkflow[shipment](context.Background(), q, wf, wfIn{ID: "o1"})
	if err != nil {
		t.Fatal(err)
	}
	out, err := h.Result(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out.Tracking != "ch-o1-track" {
		t.Fatalf("output = %+v", out)
	}
	mu.Lock()
	if strings.Join(order, ",") != "charge,ship" {
		t.Fatalf("order = %v", order)
	}
	mu.Unlock()

	snap, _ := q.Execution(context.Background(), h.RunID())
	if snap.Kind != KindWorkflow || len(snap.Steps) != 2 {
		t.Fatalf("snap = %+v", snap)
	}
	if snap.Steps[1].Status != StatusSucceeded || snap.Steps[1].Deps[0] != "charge" {
		t.Fatalf("ship step = %+v", snap.Steps[1])
	}
}

func TestWorkflowFailureCancelsRemaining(t *testing.T) {
	q := newTestQ(t)
	charge := NewTask("bad-charge", func(ctx context.Context, in wfIn) (receipt, error) {
		return receipt{}, errors.New("card declined")
	}, Retries(1), BackoffPolicy(Constant(1*time.Millisecond)))
	shipRan := false
	ship := NewTask("never-ship", func(ctx context.Context, in wfIn) (shipment, error) {
		shipRan = true
		return shipment{}, nil
	})
	wf := NewWorkflow[wfIn]("bad-flow", Step("bad-charge", charge), Step("never-ship", ship, "bad-charge"))
	h, _ := EnqueueWorkflow[shipment](context.Background(), q, wf, wfIn{})
	_, err := h.Result(context.Background())
	if err == nil || !strings.Contains(err.Error(), "card declined") {
		t.Fatalf("err = %v", err)
	}
	if shipRan {
		t.Fatal("dependent step ran after failure")
	}
	snap, _ := q.Execution(context.Background(), h.RunID())
	if snap.Status != StatusFailed {
		t.Fatalf("run status = %s", snap.Status)
	}
	if snap.Steps[1].Status != StatusCancelled {
		t.Fatalf("dependent step = %s", snap.Steps[1].Status)
	}
}

func TestWorkflowValidation(t *testing.T) {
	q := newTestQ(t)
	a := NewTask("wf-a", func(ctx context.Context, in wfIn) (receipt, error) { return receipt{}, nil })
	b := NewTask("wf-b", func(ctx context.Context, in wfIn) (receipt, error) { return receipt{}, nil })

	cycle := NewWorkflow[wfIn]("cycle", Step("a", a, "b"), Step("b", b, "a"))
	if _, err := EnqueueWorkflow[receipt](context.Background(), q, cycle, wfIn{}); err == nil {
		t.Fatal("expected cycle error")
	}
	dup := NewWorkflow[wfIn]("dup", Step("x", a), Step("x", b))
	if _, err := EnqueueWorkflow[receipt](context.Background(), q, dup, wfIn{}); err == nil {
		t.Fatal("expected duplicate name error")
	}
	unknown := NewWorkflow[wfIn]("unknown", Step("a", a, "nope"))
	if _, err := EnqueueWorkflow[receipt](context.Background(), q, unknown, wfIn{}); err == nil {
		t.Fatal("expected unknown dep error")
	}
}

func TestCancelRun(t *testing.T) {
	q := newTestQ(t, WithQueue("cancel-queue", 1))
	started := make(chan struct{})
	blocked := NewTask("blocked", func(ctx context.Context, in greetIn) (greetOut, error) {
		close(started)
		<-ctx.Done()
		return greetOut{}, ctx.Err()
	}, Queue("cancel-queue"))
	queued := NewTask("queued", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	}, Queue("cancel-queue"))

	hb, _ := Enqueue(context.Background(), q, blocked, greetIn{})
	hq, _ := Enqueue(context.Background(), q, queued, greetIn{})
	<-started
	if err := q.Cancel(hb.RunID()); err != nil {
		t.Fatal(err)
	}
	if err := q.Cancel(hq.RunID()); err != nil {
		t.Fatal(err)
	}
	if err := q.Cancel("does-not-exist"); err != nil {
		t.Fatalf("cancel unknown run should be a no-op, got %v", err)
	}
	if _, err := hb.Result(context.Background()); !errors.Is(err, ErrRunCancelled) {
		t.Fatalf("blocked result err = %v", err)
	}
	if _, err := hq.Result(context.Background()); !errors.Is(err, ErrRunCancelled) {
		t.Fatalf("queued result err = %v", err)
	}
	snap, _ := q.Execution(context.Background(), hb.RunID())
	if snap.Status != StatusCancelled {
		t.Fatalf("run status = %s", snap.Status)
	}
	// The running step converges to CANCELLED once its executor observes the
	// cancelled context; poll for it rather than racing the snapshot.
	waitFor(t, 2*time.Second, func() bool {
		s, _ := q.Execution(context.Background(), hb.RunID())
		return s != nil && s.Steps[0].Status == StatusCancelled
	})
	sq, _ := q.Execution(context.Background(), hq.RunID())
	if sq.Steps[0].Status != StatusCancelled {
		t.Fatalf("queued step = %s", sq.Steps[0].Status)
	}
}

func TestExecutionSnapshotWhileRunning(t *testing.T) {
	q := newTestQ(t)
	started := make(chan struct{})
	release := make(chan struct{})
	task := NewTask("watched", func(ctx context.Context, in greetIn) (greetOut, error) {
		close(started)
		<-release
		return greetOut{Greeting: "done"}, nil
	})
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	events, stop := q.Subscribe(h.RunID())
	defer stop()

	<-started
	snap, err := q.Execution(context.Background(), h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if snap.Status != StatusRunning || snap.Attempts != 1 || snap.StartedAt.IsZero() {
		t.Fatalf("mid-flight snapshot = %+v", snap)
	}
	if len(snap.Steps) != 1 || snap.Steps[0].Status != StatusRunning {
		t.Fatalf("steps = %+v", snap.Steps)
	}
	close(release)
	out, err := h.Result(context.Background())
	if err != nil || out.Greeting != "done" {
		t.Fatalf("result = %+v %v", out, err)
	}

	// The event stream should have observed the transitions without anyone
	// pausing execution.
	deadline := time.After(3 * time.Second)
	sawSucceeded := false
	for !sawSucceeded {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("event channel closed early")
			}
			if ev.To == StatusSucceeded {
				sawSucceeded = true
			}
		case <-deadline:
			t.Fatal("never saw SUCCEEDED event")
		}
	}
}

func TestIntrospectionUnderLoad(t *testing.T) {
	q := newTestQ(t, WithQueue("load", 16))
	task := NewTask("micro", func(ctx context.Context, in greetIn) (greetOut, error) {
		time.Sleep(time.Millisecond)
		return greetOut{Greeting: in.Name}, nil
	})

	const n = 200
	hs := make([]*RunHandle[greetOut], n)
	for i := 0; i < n; i++ {
		h, err := Enqueue(context.Background(), q, task, greetIn{Name: fmt.Sprint(i)})
		if err != nil {
			t.Fatal(err)
		}
		hs[i] = h
	}

	// Hammer introspection while workers write state.
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				ctx := context.Background()
				if _, err := q.Execution(ctx, hs[i%n].RunID()); err != nil {
					t.Errorf("Execution: %v", err)
					return
				}
				if _, err := q.Runs(ctx, RunFilter{Status: StatusRunning, Limit: 50}); err != nil {
					t.Errorf("Runs: %v", err)
					return
				}
				if _, err := q.Metrics(ctx); err != nil {
					t.Errorf("Metrics: %v", err)
					return
				}
			}
		}(i)
	}

	succeeded := 0
	for _, h := range hs {
		if _, err := h.Result(context.Background()); err == nil {
			succeeded++
		}
	}
	close(stop)
	wg.Wait()
	if succeeded != n {
		t.Fatalf("succeeded = %d/%d", succeeded, n)
	}
	m, _ := q.Metrics(context.Background())
	if m.Runs[StatusSucceeded] != n {
		t.Fatalf("metrics = %+v", m.Runs)
	}
}

func TestSubscribeAllRuns(t *testing.T) {
	q := newTestQ(t)
	events, stop := q.Subscribe("")
	defer stop()
	task := NewTask("small", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	})
	h1, _ := Enqueue(context.Background(), q, task, greetIn{})
	h2, _ := Enqueue(context.Background(), q, task, greetIn{})
	_, _ = h1.Result(context.Background())
	_, _ = h2.Result(context.Background())

	count := 0
	deadline := time.After(3 * time.Second)
	for count < 4 {
		select {
		case <-events:
			count++
		case <-deadline:
			t.Fatalf("got %d events, want >= 4", count)
		}
	}
}

func TestDelayedRun(t *testing.T) {
	q := newTestQ(t)
	task := NewTask("later", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{Greeting: "late"}, nil
	})
	h, _ := Enqueue(context.Background(), q, task, greetIn{}, WithDelay(80*time.Millisecond))
	snap, _ := q.Execution(context.Background(), h.RunID())
	if snap.Status != StatusQueued {
		t.Fatalf("status = %s, want QUEUED", snap.Status)
	}
	out, err := h.Result(context.Background())
	if err != nil || out.Greeting != "late" {
		t.Fatalf("out=%+v err=%v", out, err)
	}
}

func TestLogs(t *testing.T) {
	q := newTestQ(t, WithLogStorage(true))
	task := NewTask("logger", func(ctx context.Context, in greetIn) (greetOut, error) {
		log := TaskLogger(ctx)
		log.Info("starting work", "n", 1)
		log.Warn("almost done")
		return greetOut{}, nil
	})
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	var lines []LogEntry
	waitFor(t, 3*time.Second, func() bool {
		lines, _ = q.Logs(context.Background(), h.RunID(), 100)
		return len(lines) >= 2
	})
	if !strings.Contains(lines[0].Message, "starting work n=1") {
		t.Fatalf("line 0 = %q", lines[0].Message)
	}
	if lines[0].Step != "logger" || lines[0].Level != "INFO" {
		t.Fatalf("entry = %+v", lines[0])
	}
}

func TestRunsFilterAndMetrics(t *testing.T) {
	q := newTestQ(t)
	ok := NewTask("ok", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	})
	bad := NewTask("bad", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, errors.New("no")
	}, BackoffPolicy(Constant(1*time.Millisecond)))
	h1, _ := Enqueue(context.Background(), q, ok, greetIn{})
	h2, _ := Enqueue(context.Background(), q, bad, greetIn{})
	if _, err := h1.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := h2.Result(context.Background()); err == nil {
		t.Fatal("expected failure")
	}
	rs, err := q.Runs(context.Background(), RunFilter{Status: StatusSucceeded})
	if err != nil || len(rs) != 1 || rs[0].RunID != h1.RunID() {
		t.Fatalf("runs = %+v err=%v", rs, err)
	}
	m, _ := q.Metrics(context.Background())
	if m.Runs[StatusSucceeded] != 1 || m.Runs[StatusFailed] != 1 {
		t.Fatalf("metrics = %+v", m.Runs)
	}
}

func TestUnknownRun(t *testing.T) {
	q := newTestQ(t)
	if _, err := q.Execution(context.Background(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
}

func TestCron(t *testing.T) {
	q := newTestQ(t)
	var fires atomic.Int32
	task := NewTask("cronned", func(ctx context.Context, in greetIn) (greetOut, error) {
		fires.Add(1)
		return greetOut{}, nil
	})
	if err := Cron(q, "tick", "@every 1s", task, greetIn{}); err != nil {
		t.Fatal(err)
	}
	var cr CronInfo
	waitFor(t, 2*time.Second, func() bool {
		for _, c := range q.Crons() {
			if c.Name == "tick" {
				cr = c
				return true
			}
		}
		return false
	})
	if cr.Spec != "@every 1s" || cr.Task != "cronned" {
		t.Fatalf("cron info = %+v", cr)
	}
	waitFor(t, 5*time.Second, func() bool { return fires.Load() >= 2 })

	if err := q.RemoveCron("tick"); err != nil {
		t.Fatal(err)
	}
	n := fires.Load()
	time.Sleep(1600 * time.Millisecond)
	if fires.Load() != n {
		t.Fatalf("cron fired after removal: %d -> %d", n, fires.Load())
	}
}

func TestGracefulCloseDrains(t *testing.T) {
	q, err := Open(WithStorage(Memory()), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	task := NewTask("drain", func(ctx context.Context, in greetIn) (greetOut, error) {
		<-release
		return greetOut{Greeting: "drained"}, nil
	})
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	waitFor(t, 2*time.Second, func() bool {
		s, _ := q.Execution(context.Background(), h.RunID())
		return s != nil && s.Status == StatusRunning
	})
	close(release)
	out, err := h.Result(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := q.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	if out.Greeting != "drained" {
		t.Fatalf("out = %+v", out)
	}
}

func TestCloseInterruptsStuckTasks(t *testing.T) {
	q, err := Open(WithStorage(Memory()), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	stuck := make(chan struct{})
	task := NewTask("stuck", func(ctx context.Context, in greetIn) (greetOut, error) {
		<-stuck // never released; ignores ctx like a badly-behaved task
		return greetOut{}, nil
	})
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	waitFor(t, 2*time.Second, func() bool {
		s, _ := q.Execution(context.Background(), h.RunID())
		return s != nil && s.Status == StatusRunning
	})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := q.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close err = %v", err)
	}
	if _, err := h.Result(context.Background()); !errors.Is(err, ErrRunInterrupted) {
		t.Fatalf("result err = %v", err)
	}
	close(stuck)
}
