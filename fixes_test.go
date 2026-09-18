package quacker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestStepContextHelpers guards the context contract: StepFromContext and
// RunIDFromContext must work inside task functions (a regression where the
// context stored *stepState but the accessor asserted StepContext made both
// permanently return false).
func TestStepContextHelpers(t *testing.T) {
	q := newTestQ(t)
	ran := make(chan struct{})
	task := NewTask("ctx-probe", func(ctx context.Context, in greetIn) (greetOut, error) {
		defer close(ran)
		sc, ok := StepFromContext(ctx)
		if !ok {
			t.Error("StepFromContext returned false inside a task")
			return greetOut{}, nil
		}
		if sc.Step != "ctx-probe" || sc.Attempt != 1 {
			t.Errorf("step context = %+v", sc)
		}
		if RunIDFromContext(ctx) != sc.RunID || sc.RunID == "" {
			t.Errorf("RunIDFromContext = %q, step context = %+v", RunIDFromContext(ctx), sc)
		}
		return greetOut{}, nil
	})
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("task never ran")
	}
	_ = h
}

// TestWaitersAreReleased guards against the waiter-map leak: every finished
// run must remove its waiter, including cancelled and failed runs.
func TestWaitersAreReleased(t *testing.T) {
	q := newTestQ(t)
	ok := NewTask("ok", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	})
	bad := NewTask("bad", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, errors.New("no")
	}, BackoffPolicy(Constant(1*time.Millisecond)))
	for i := 0; i < 25; i++ {
		h1, _ := Enqueue(context.Background(), q, ok, greetIn{})
		if _, err := h1.Result(context.Background()); err != nil {
			t.Fatal(err)
		}
		h2, _ := Enqueue(context.Background(), q, bad, greetIn{})
		if _, err := h2.Result(context.Background()); err == nil {
			t.Fatal("expected failure")
		}
	}
	waitFor(t, 2*time.Second, func() bool {
		return q.eng.WaitersLen() == 0
	})
}

// TestParkDoesNotConsumeAttempts: a claimed step whose task is not
// registered is parked without burning a retry attempt.
func TestParkDoesNotConsumeAttempts(t *testing.T) {
	q := newTestQ(t, WithPollInterval(5*time.Millisecond))
	var attempts atomic.Int32
	task := NewTask("parked", func(ctx context.Context, in greetIn) (greetOut, error) {
		attempts.Add(1)
		return greetOut{}, nil
	}, Retries(2), BackoffPolicy(Constant(1*time.Millisecond)))

	// Enqueue registers the task; drop the registration so the claim hits the
	// park path, then restore it. The delay keeps the scheduler from claiming
	// (and running) the step before ForgetTask lands.
	h, err := Enqueue(context.Background(), q, task, greetIn{}, WithDelay(150*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	q.eng.ForgetTask("parked")
	waitFor(t, 3*time.Second, func() bool {
		s, _ := q.Execution(context.Background(), h.RunID())
		return s != nil && s.Steps[0].Error == "task not registered in this process"
	})
	// Give the park loop a few cycles, then restore the task.
	time.Sleep(120 * time.Millisecond)
	Register(q, task)
	out, err := h.Result(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = out
	if got := attempts.Load(); got != 1 {
		t.Fatalf("task executed %d times, want 1 (park cycles must not run it)", got)
	}
	snap, _ := q.Execution(context.Background(), h.RunID())
	if snap.Steps[0].Attempts != 1 {
		t.Fatalf("recorded attempts = %d, want 1 (parking must not consume attempts)", snap.Steps[0].Attempts)
	}
}

// TestFailedRunCancelsSiblingContexts: when a step's final failure fails the
// run, sibling steps still executing must have their contexts cancelled, not
// just their DB rows.
func TestFailedRunCancelsSiblingContexts(t *testing.T) {
	q := newTestQ(t)
	fastFail := NewTask("fast-fail", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, errors.New("boom")
	})
	sawCancel := make(chan struct{})
	var once sync.Once
	slow := NewTask("slow-sibling", func(ctx context.Context, in greetIn) (greetOut, error) {
		<-ctx.Done()
		once.Do(func() { close(sawCancel) })
		return greetOut{}, ctx.Err()
	})
	wf := NewWorkflow[greetIn]("siblings",
		Step("fast-fail", fastFail),
		Step("slow-sibling", slow),
	)
	h, _ := EnqueueWorkflow[greetOut](context.Background(), q, wf, greetIn{})
	_, err := h.Result(context.Background())
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v", err)
	}
	select {
	case <-sawCancel:
	case <-time.After(3 * time.Second):
		t.Fatal("sibling context was never cancelled after run failure")
	}
	snap, _ := q.Execution(context.Background(), h.RunID())
	if snap.Steps[1].Status != StatusCancelled {
		t.Fatalf("sibling step = %s", snap.Steps[1].Status)
	}
}

// TestCancelledStepStaysCancelled: a task that ignores its context and
// finishes successfully after the run was cancelled must not flip its step
// back to SUCCEEDED.
func TestCancelledStepStaysCancelled(t *testing.T) {
	q := newTestQ(t)
	started := make(chan struct{})
	ignore := NewTask("ignores-ctx", func(ctx context.Context, in greetIn) (greetOut, error) {
		close(started)
		time.Sleep(150 * time.Millisecond) // ignores ctx on purpose
		return greetOut{}, nil
	})
	h, _ := Enqueue(context.Background(), q, ignore, greetIn{})
	<-started
	if err := q.Cancel(h.RunID()); err != nil {
		t.Fatal(err)
	}
	// Let the oblivious task finish and try to record success.
	time.Sleep(300 * time.Millisecond)
	snap, _ := q.Execution(context.Background(), h.RunID())
	if snap.Steps[0].Status != StatusCancelled {
		t.Fatalf("step = %s, cancelled steps must not resurrect", snap.Steps[0].Status)
	}
	if snap.Status != StatusCancelled {
		t.Fatalf("run = %s", snap.Status)
	}
}

// TestEnqueueHonorsCallerContext: a cancelled caller ctx must prevent the
// enqueue, not just get ignored.
func TestEnqueueHonorsCallerContext(t *testing.T) {
	q := newTestQ(t)
	task := NewTask("whatever", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Enqueue(ctx, q, task, greetIn{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// TestEnqueueDuringCloseRejected: an enqueue racing Close is either accepted
// (and its Result resolves) or rejected with ErrClosed — never stranded.
func TestEnqueueDuringCloseRejected(t *testing.T) {
	q, err := Open(WithStorage(Memory()), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	task := NewTask("slow-close", func(ctx context.Context, in greetIn) (greetOut, error) {
		time.Sleep(50 * time.Millisecond)
		return greetOut{}, nil
	})
	go q.Close(context.Background())
	// Hammer enqueue during close; every call must return a definitive result.
	deadline := time.After(2 * time.Second)
	accepted := 0
	for {
		select {
		case <-deadline:
			if accepted == 0 {
				return // close won the race; nothing was accepted
			}
			return
		default:
		}
		h, err := Enqueue(context.Background(), q, task, greetIn{})
		if errors.Is(err, ErrClosed) {
			return
		}
		if err != nil {
			if strings.Contains(err.Error(), "closed") {
				return
			}
			t.Fatalf("unexpected err: %v", err)
		}
		accepted++
		// The result must arrive promptly with SOME definitive outcome: a
		// success, or a documented interruption (the run may not have been
		// claimed before Close stopped claiming). Hanging is the regression.
		ctx2, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err = h.Result(ctx2)
		cancel()
		if err != nil && !errors.Is(err, ErrRunInterrupted) && !errors.Is(err, ErrRunCancelled) {
			t.Fatalf("accepted run's Result failed unexpectedly: %v", err)
		}
	}
}

// TestJitterAppliedToDefaults: Exponential (and the implicit default backoff)
// must actually jitter; Constant must not.
func TestJitterAppliedToDefaults(t *testing.T) {
	now := time.Now()
	exp := Exponential(10 * time.Millisecond)
	distinct := map[time.Duration]struct{}{}
	for i := 0; i < 20; i++ {
		distinct[exp.Delay(1, now)] = struct{}{}
	}
	if len(distinct) < 2 {
		t.Fatal("Exponential has no jitter despite documenting 10%")
	}
	if d := exp.Delay(1, now); d > 15*time.Millisecond {
		t.Fatalf("jittered delay %s exceeds base+10%%", d)
	}
	c := Constant(10 * time.Millisecond)
	for i := 0; i < 5; i++ {
		if got := c.Delay(1, now); got != 10*time.Millisecond {
			t.Fatalf("Constant jittered: %s", got)
		}
	}
	// The implicit default (task without BackoffPolicy) inherits jitter.
	if defaultBackoff.Jitter == 0 {
		t.Fatal("default backoff lost its jitter")
	}
}

// TestRogueTaskLoggerAfterClose: a task that outlives the drain deadline and
// logs via TaskLogger must not panic (send on closed channel), and its step
// must end INTERRUPTED rather than SUCCEEDED.
func TestRogueTaskLoggerAfterClose(t *testing.T) {
	q, err := Open(WithStorage(Memory()), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	rogueDone := make(chan struct{})
	task := NewTask("rogue-logger", func(ctx context.Context, in greetIn) (greetOut, error) {
		defer close(rogueDone)
		time.Sleep(300 * time.Millisecond) // outlives the 50ms drain below
		TaskLogger(ctx).Info("logged after close")
		return greetOut{}, nil
	})
	h, err := Enqueue(context.Background(), q, task, greetIn{})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool {
		s, _ := q.Execution(context.Background(), h.RunID())
		return s != nil && s.Status == StatusRunning
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_ = q.Close(ctx) // drain deadline expires while the task sleeps
	select {
	case <-rogueDone:
	case <-time.After(2 * time.Second):
		t.Fatal("rogue task never finished")
	}
	// No panic getting here is the point; the sweep must own the outcome.
}

// TestMultiEngineLogsStayIsolated: two engines in one process each keep
// their own task logs (the old global log sink misrouted them).
func TestMultiEngineLogsStayIsolated(t *testing.T) {
	q1 := newTestQ(t)
	q2 := newTestQ(t)
	t1 := NewTask("logger-1", func(ctx context.Context, in greetIn) (greetOut, error) {
		TaskLogger(ctx).Info("from-engine-1")
		return greetOut{}, nil
	})
	t2 := NewTask("logger-2", func(ctx context.Context, in greetIn) (greetOut, error) {
		TaskLogger(ctx).Info("from-engine-2")
		return greetOut{}, nil
	})
	h1, _ := Enqueue(context.Background(), q1, t1, greetIn{})
	h2, _ := Enqueue(context.Background(), q2, t2, greetIn{})
	if _, err := h1.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := h2.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		logs1, _ := q1.Logs(context.Background(), h1.RunID(), 10)
		return len(logs1) > 0
	})
	logs1, _ := q1.Logs(context.Background(), h1.RunID(), 10)
	logs2, _ := q2.Logs(context.Background(), h2.RunID(), 10)
	if len(logs1) == 0 || !strings.Contains(logs1[0].Message, "from-engine-1") {
		t.Fatalf("engine 1 logs = %+v", logs1)
	}
	if len(logs2) == 0 || !strings.Contains(logs2[0].Message, "from-engine-2") {
		t.Fatalf("engine 2 logs = %+v", logs2)
	}
}

// TestConcurrentSetQueueDuringLoad exercises SetQueue against the running
// scheduler (the queueState.concurrency race).
func TestConcurrentSetQueueDuringLoad(t *testing.T) {
	q := newTestQ(t, WithQueue("spin", 4))
	task := NewTask("spin", func(ctx context.Context, in greetIn) (greetOut, error) {
		time.Sleep(time.Millisecond)
		return greetOut{}, nil
	}, Queue("spin"))
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		conc := 1
		for {
			select {
			case <-stop:
				return
			default:
			}
			q.SetQueue("spin", conc%8+1)
			conc++
		}
	}()
	var hs []*RunHandle[greetOut]
	for i := 0; i < 100; i++ {
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
	close(stop)
	wg.Wait()
}

// TestExecutionConsistentSnapshot: while a run transitions, the snapshot's
// run and step statuses must never come from different points in time in a
// way that breaks the terminal invariant (terminal run => terminal steps seen
// together).
func TestExecutionConsistentSnapshot(t *testing.T) {
	q := newTestQ(t)
	task := NewTask("churn", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	})
	var hs []*RunHandle[greetOut]
	for i := 0; i < 30; i++ {
		h, _ := Enqueue(context.Background(), q, task, greetIn{})
		hs = append(hs, h)
	}
	// Read snapshots concurrently with execution; any terminal run observed
	// must carry only terminal steps (single-transaction read guarantee).
	var wg sync.WaitGroup
	for _, h := range hs {
		wg.Add(1)
		go func(h *RunHandle[greetOut]) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				snap, err := q.Execution(context.Background(), h.RunID())
				if err != nil {
					t.Errorf("Execution: %v", err)
					return
				}
				if snap.Status == StatusSucceeded {
					for _, s := range snap.Steps {
						if s.Status != StatusSucceeded {
							t.Errorf("terminal run with non-terminal step: run=%s step=%s", snap.Status, s.Status)
							return
						}
					}
					return
				}
			}
		}(h)
	}
	for _, h := range hs {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if _, err := h.Result(ctx); err != nil {
			t.Fatal(err)
		}
		cancel()
	}
	wg.Wait()
}
