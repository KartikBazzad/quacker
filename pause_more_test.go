package quacker

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPauseQueueGatesSuspendedResume(t *testing.T) {
	q := newTestQ(t, WithQueue("dq", 5))
	ctx := context.Background()
	task := NewTask("pqm.susp", func(ctx context.Context, in string) (string, error) {
		if err := SleepDurable(ctx, 40*time.Millisecond); err != nil {
			return "", err
		}
		return in, nil
	}, Queue("dq"))

	h, err := Enqueue(ctx, q, task, "x")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		s, err := q.Execution(ctx, h.RunID())
		return err == nil && len(s.Steps) == 1 && s.Steps[0].Status == StatusSuspended
	})
	// Pause past the sleep's resume time: the resume arm must not claim it.
	if err := q.PauseQueue(ctx, "dq"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(90 * time.Millisecond)
	if s, err := q.Execution(ctx, h.RunID()); err != nil || s.Status == StatusSucceeded {
		t.Fatalf("run advanced while its queue was paused: %+v err=%v", s, err)
	}
	if err := q.ResumeQueue(ctx, "dq"); err != nil {
		t.Fatal(err)
	}
	if out, err := h.Result(ctx); err != nil || out != "x" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

func TestPauseRunRealErrorFails(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	started := make(chan struct{})
	release := make(chan struct{})
	task := NewTask("prm.err", func(ctx context.Context, in string) (string, error) {
		close(started)
		<-release // ignores ctx
		return "", errors.New("boom")
	})
	h, err := Enqueue(ctx, q, task, "x")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	})
	if err := q.PauseRun(ctx, h.RunID()); err != nil {
		t.Fatal(err)
	}
	close(release)
	if _, err := h.Result(ctx); err == nil {
		t.Fatal("a real error while paused must fail the run")
	}
	if snap, err := q.Execution(ctx, h.RunID()); err != nil || snap.Status != StatusFailed {
		t.Fatalf("status = %+v err=%v, want FAILED", snap, err)
	}
}

func TestPauseRunIgnoresCtxLastStepCompletes(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	started := make(chan struct{})
	release := make(chan struct{})
	task := NewTask("prm.ignore", func(ctx context.Context, in string) (string, error) {
		close(started)
		<-release // ignores ctx
		return "ok", nil
	})
	h, err := Enqueue(ctx, q, task, "x")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	})
	if err := q.PauseRun(ctx, h.RunID()); err != nil {
		t.Fatal(err)
	}
	close(release)
	// The only step finished, so the run completes rather than sticking PAUSED.
	if out, err := h.Result(ctx); err != nil || out != "ok" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

func TestPauseRunDAGBoundary(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	started := make(chan struct{})
	release := make(chan struct{})
	a := NewTask("prm.a", func(ctx context.Context, in string) (string, error) {
		close(started)
		<-release // ignores ctx
		return "A", nil
	})
	b := NewTask("prm.b", func(ctx context.Context, in string) (string, error) { return "B", nil })
	wf := NewWorkflow[string]("prm.wf", Step("a", a), Step("b", b, "a"))

	h, err := EnqueueWorkflow[string](ctx, q, wf, "in")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	})
	if err := q.PauseRun(ctx, h.RunID()); err != nil {
		t.Fatal(err)
	}
	close(release)
	// step a finishes; step b becomes ready but must not be claimed while paused.
	waitFor(t, 3*time.Second, func() bool {
		s, err := q.Execution(ctx, h.RunID())
		if err != nil {
			return false
		}
		for _, st := range s.Steps {
			if st.Name == "a" && st.Status == StatusSucceeded {
				return true
			}
		}
		return false
	})
	time.Sleep(40 * time.Millisecond)
	snap, err := q.Execution(ctx, h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if snap.Status != StatusPaused {
		t.Fatalf("run = %s, want PAUSED", snap.Status)
	}
	for _, st := range snap.Steps {
		if st.Name == "b" && st.Status != StatusQueued {
			t.Fatalf("step b = %s, want QUEUED (held while paused)", st.Status)
		}
	}
	if err := q.ResumeRun(ctx, h.RunID()); err != nil {
		t.Fatal(err)
	}
	if out, err := h.Result(ctx); err != nil || out != "B" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

func TestPauseRunDurable(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	task := NewTask("prm.durable", func(ctx context.Context, in string) (string, error) {
		if err := SleepDurable(ctx, 40*time.Millisecond); err != nil {
			return "", err
		}
		return in, nil
	})
	h, err := Enqueue(ctx, q, task, "x")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		s, err := q.Execution(ctx, h.RunID())
		return err == nil && len(s.Steps) == 1 && s.Steps[0].Status == StatusSuspended
	})
	if err := q.PauseRun(ctx, h.RunID()); err != nil {
		t.Fatal(err)
	}
	if snap, err := q.Execution(ctx, h.RunID()); err != nil || snap.Status != StatusPaused {
		t.Fatalf("status = %+v err=%v, want PAUSED", snap, err)
	}
	if err := q.ResumeRun(ctx, h.RunID()); err != nil {
		t.Fatal(err)
	}
	if out, err := h.Result(ctx); err != nil || out != "x" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

func TestCancelPausedRun(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	task := NewTask("prm.cancel", func(ctx context.Context, in string) (string, error) { return in, nil })
	h, err := Enqueue(ctx, q, task, "x", WithRunAt(time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	if err := q.PauseRun(ctx, h.RunID()); err != nil {
		t.Fatal(err)
	}
	if err := q.Cancel(h.RunID()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(ctx); !errors.Is(err, ErrRunCancelled) {
		t.Fatalf("want ErrRunCancelled, got %v", err)
	}
	if snap, err := q.Execution(ctx, h.RunID()); err != nil || snap.Status != StatusCancelled {
		t.Fatalf("status = %+v err=%v, want CANCELLED", snap, err)
	}
}

func TestPurgeSkipsPaused(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	task := NewTask("prm.purge", func(ctx context.Context, in string) (string, error) { return in, nil })
	h, err := Enqueue(ctx, q, task, "x", WithRunAt(time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	if err := q.PauseRun(ctx, h.RunID()); err != nil {
		t.Fatal(err)
	}
	res, err := q.Purge(ctx, PurgeOptions{OlderThan: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if res.Runs != 0 {
		t.Fatalf("purged %d paused runs, want 0", res.Runs)
	}
	if _, err := q.Execution(ctx, h.RunID()); err != nil {
		t.Fatalf("paused run gone after purge: %v", err)
	}
}
