package quacker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestDeadLetterOnExhaustedFailure(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	dl := NewTask("dlq.fail", func(ctx context.Context, in string) (string, error) {
		return "", errors.New("boom")
	}, WithDeadLetter())
	plain := NewTask("dlq.plain", func(ctx context.Context, in string) (string, error) {
		return "", errors.New("boom")
	})

	hd, err := Enqueue(ctx, q, dl, "x")
	if err != nil {
		t.Fatal(err)
	}
	hp, err := Enqueue(ctx, q, plain, "y")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hd.Result(ctx); err == nil {
		t.Fatal("expected failure")
	}
	if _, err := hp.Result(ctx); err == nil {
		t.Fatal("expected failure")
	}

	letters, err := q.DeadLetters(ctx, DeadLetterFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(letters) != 1 || letters[0].RunID != hd.RunID() {
		t.Fatalf("dead letters = %+v, want only %s", letters, hd.RunID())
	}
	if letters[0].DeadLetteredAt.IsZero() {
		t.Fatal("dead_lettered_at not set")
	}
	// Filter by an unrelated queue yields nothing.
	if l, err := q.DeadLetters(ctx, DeadLetterFilter{Queue: "nope"}); err != nil || len(l) != 0 {
		t.Fatalf("filtered = %+v err=%v", l, err)
	}
}

func TestDeadLetterNotSetOnCancel(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	release := make(chan struct{})
	task := NewTask("dlq.cancel", func(ctx context.Context, in string) (string, error) {
		select {
		case <-release:
			return in, nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}, WithDeadLetter())
	h, err := Enqueue(ctx, q, task, "x")
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Cancel(h.RunID()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(ctx); !errors.Is(err, ErrRunCancelled) {
		t.Fatalf("want ErrRunCancelled, got %v", err)
	}
	close(release)
	if l, err := q.DeadLetters(ctx, DeadLetterFilter{}); err != nil || len(l) != 0 {
		t.Fatalf("cancelled run dead-lettered: %+v err=%v", l, err)
	}
}

func TestRetryDeadLetter(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	var calls atomic.Int32
	task := NewTask("dlq.retry", func(ctx context.Context, in string) (string, error) {
		if calls.Add(1) == 1 {
			return "", errors.New("boom")
		}
		return "ok:" + in, nil
	}, WithDeadLetter())

	h, err := Enqueue(ctx, q, task, "x")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(ctx); err == nil {
		t.Fatal("expected first failure")
	}
	if err := q.RetryDeadLetter(ctx, h.RunID()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool {
		s, err := q.Execution(ctx, h.RunID())
		return err == nil && s.Status == StatusSucceeded
	})
	if l, err := q.DeadLetters(ctx, DeadLetterFilter{}); err != nil || len(l) != 0 {
		t.Fatalf("marker not cleared: %+v err=%v", l, err)
	}
}

func TestRetryDismissDeadLetterEdges(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	task := NewTask("dlq.edge", func(ctx context.Context, in string) (string, error) {
		return "", errors.New("boom")
	}, WithDeadLetter())
	h, err := Enqueue(ctx, q, task, "x")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(ctx); err == nil {
		t.Fatal("expected failure")
	}
	if err := q.DismissDeadLetter(ctx, h.RunID()); err != nil {
		t.Fatal(err)
	}
	// Dismissed: still FAILED, no longer retryable as a dead letter.
	if s, err := q.Execution(ctx, h.RunID()); err != nil || s.Status != StatusFailed {
		t.Fatalf("status = %+v err=%v, want FAILED", s, err)
	}
	if err := q.RetryDeadLetter(ctx, h.RunID()); !errors.Is(err, ErrNotDeadLetter) {
		t.Fatalf("retry dismissed: want ErrNotDeadLetter, got %v", err)
	}
	if err := q.DismissDeadLetter(ctx, h.RunID()); !errors.Is(err, ErrNotDeadLetter) {
		t.Fatalf("re-dismiss: want ErrNotDeadLetter, got %v", err)
	}
	if err := q.RetryDeadLetter(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("retry unknown: want ErrNotFound, got %v", err)
	}
}

func TestDeadLetterDAGRetry(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	var calls atomic.Int32
	a := NewTask("dlq.dag.a", func(ctx context.Context, in string) (string, error) {
		if calls.Add(1) == 1 {
			return "", errors.New("boom")
		}
		return "A", nil
	}, WithDeadLetter())
	b := NewTask("dlq.dag.b", func(ctx context.Context, in string) (string, error) { return "B", nil })
	wf := NewWorkflow[string]("dlq.dag.wf", Step("a", a), Step("b", b, "a"))

	h, err := EnqueueWorkflow[string](ctx, q, wf, "in")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(ctx); err == nil {
		t.Fatal("expected failure")
	}
	if err := q.RetryDeadLetter(ctx, h.RunID()); err != nil {
		t.Fatal(err)
	}
	// The original handle already resolved at the first failure, so observe
	// the recovery through the engine.
	waitFor(t, 5*time.Second, func() bool {
		s, err := q.Execution(ctx, h.RunID())
		return err == nil && s.Status == StatusSucceeded
	})
}

func TestPurgeExcludeDeadLettered(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	dl := NewTask("dlq.purge", func(ctx context.Context, in string) (string, error) {
		return "", errors.New("boom")
	}, WithDeadLetter())
	h, err := Enqueue(ctx, q, dl, "x")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(ctx); err == nil {
		t.Fatal("expected failure")
	}
	time.Sleep(5 * time.Millisecond)

	res, err := q.Purge(ctx, PurgeOptions{OlderThan: time.Millisecond, ExcludeDeadLettered: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Runs != 0 {
		t.Fatalf("dead letter purged despite exclusion: %d", res.Runs)
	}
	if l, err := q.DeadLetters(ctx, DeadLetterFilter{}); err != nil || len(l) != 1 {
		t.Fatalf("dead letter missing: %+v err=%v", l, err)
	}
	// Without the exclusion it is purged.
	res, err = q.Purge(ctx, PurgeOptions{OlderThan: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if res.Runs != 1 {
		t.Fatalf("purged %d, want 1", res.Runs)
	}
}
