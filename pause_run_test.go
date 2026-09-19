package quacker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestPauseRunQueued(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	task := NewTask("pr.queued", func(ctx context.Context, in string) (string, error) { return in, nil })

	h, err := Enqueue(ctx, q, task, "x", WithRunAt(time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	if err := q.PauseRun(ctx, h.RunID()); err != nil {
		t.Fatal(err)
	}
	snap, err := q.Execution(ctx, h.RunID())
	if err != nil || snap.Status != StatusPaused {
		t.Fatalf("status = %+v err=%v, want PAUSED", snap, err)
	}
	// Idempotent.
	if err := q.PauseRun(ctx, h.RunID()); err != nil {
		t.Fatal(err)
	}
	if err := q.ResumeRun(ctx, h.RunID()); err != nil {
		t.Fatal(err)
	}
	if out, err := h.Result(ctx); err != nil || out != "x" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

func TestPauseRunRunningRequeues(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	started := make(chan struct{})
	var calls atomic.Int32
	task := NewTask("pr.running", func(ctx context.Context, in string) (string, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-ctx.Done()
			return "", ctx.Err()
		}
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
	// The paused run's step is requeued with the attempt refunded.
	waitFor(t, 3*time.Second, func() bool {
		s, err := q.Execution(ctx, h.RunID())
		return err == nil && s.Status == StatusPaused && len(s.Steps) == 1 &&
			s.Steps[0].Status == StatusQueued && s.Steps[0].Attempts == 0
	})

	// While paused the step must not be re-claimed.
	time.Sleep(40 * time.Millisecond)
	if s, _ := q.Execution(ctx, h.RunID()); s.Status != StatusPaused {
		t.Fatalf("run advanced while paused: %+v", s)
	}

	if err := q.ResumeRun(ctx, h.RunID()); err != nil {
		t.Fatal(err)
	}
	if out, err := h.Result(ctx); err != nil || out != "ok" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

func TestPauseRunTerminalAndUnknown(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	task := NewTask("pr.done", func(ctx context.Context, in string) (string, error) { return in, nil })
	h, err := Enqueue(ctx, q, task, "x")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(ctx); err != nil {
		t.Fatal(err)
	}
	if err := q.PauseRun(ctx, h.RunID()); !errors.Is(err, ErrRunTerminal) {
		t.Fatalf("pause terminal: want ErrRunTerminal, got %v", err)
	}
	if err := q.ResumeRun(ctx, h.RunID()); !errors.Is(err, ErrRunTerminal) {
		t.Fatalf("resume terminal: want ErrRunTerminal, got %v", err)
	}
	if err := q.PauseRun(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("pause unknown: want ErrNotFound, got %v", err)
	}
	// Resuming a non-paused live run is a no-op.
	h2, err := Enqueue(ctx, q, task, "y", WithRunAt(time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	if err := q.ResumeRun(ctx, h2.RunID()); err != nil {
		t.Fatalf("resume non-paused: %v", err)
	}
}

func TestPauseRunPersists(t *testing.T) {
	path := t.TempDir() + "/pr.db"
	ctx := context.Background()
	q1, err := Open(WithStorage(File(path)), WithPollInterval(2*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	task := NewTask("pr.file", func(ctx context.Context, in string) (string, error) { return in, nil })
	h, err := Enqueue(ctx, q1, task, "x", WithRunAt(time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	if err := q1.PauseRun(ctx, h.RunID()); err != nil {
		t.Fatal(err)
	}
	closeQ(t, q1)

	q2, err := Open(WithStorage(File(path)), WithPollInterval(2*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer closeQ(t, q2)
	Register(q2, task)
	if snap, err := q2.Execution(ctx, h.RunID()); err != nil || snap.Status != StatusPaused {
		t.Fatalf("reopened status = %+v err=%v, want PAUSED", snap, err)
	}
	if err := q2.ResumeRun(ctx, h.RunID()); err != nil {
		t.Fatal(err)
	}
	// The original handle belonged to q1 and was released on its Close, so
	// observe the reopened engine's view instead.
	waitFor(t, 5*time.Second, func() bool {
		s, err := q2.Execution(ctx, h.RunID())
		return err == nil && s.Status == StatusSucceeded
	})
}
