package quacker

import (
	"context"
	"errors"
	"testing"
	"time"
)

func waitStarted(t *testing.T, ch <-chan string, want string) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("started %q, want %q", got, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("task %q never started", want)
	}
}

// strategyTask returns a keyed task (limit 1) whose body blocks on release and
// reports its input when it starts.
func strategyTask(name string, s ConcurrencyStrategy) (*Task[string, string], chan string, chan struct{}) {
	started := make(chan string, 8)
	release := make(chan struct{})
	task := NewTask(name, func(ctx context.Context, in string) (string, error) {
		started <- in
		select {
		case <-release:
			return in, nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}, WithKey(func(in string) string { return "k" }), WithKeyConcurrency(1), WithKeyStrategy(s))
	return task, started, release
}

func TestConcurrencyCancelNewest(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	task, started, release := strategyTask("cs.newest", ConcurrencyCancelNewest)

	h1, err := Enqueue(ctx, q, task, "a")
	if err != nil {
		t.Fatal(err)
	}
	waitStarted(t, started, "a")
	h2, err := Enqueue(ctx, q, task, "b")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h2.Result(ctx); !errors.Is(err, ErrRunCancelled) {
		t.Fatalf("incoming run: want ErrRunCancelled, got %v", err)
	}
	if snap, err := q.Execution(ctx, h1.RunID()); err != nil || snap.Status != StatusRunning {
		t.Fatalf("running run = %+v err=%v", snap, err)
	}
	close(release)
	if out, err := h1.Result(ctx); err != nil || out != "a" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

func TestConcurrencyCancelInProgress(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	task, started, release := strategyTask("cs.inprogress", ConcurrencyCancelInProgress)

	h1, err := Enqueue(ctx, q, task, "a")
	if err != nil {
		t.Fatal(err)
	}
	waitStarted(t, started, "a")
	h2, err := Enqueue(ctx, q, task, "b")
	if err != nil {
		t.Fatal(err)
	}
	// The in-progress run is cancelled to make room.
	if _, err := h1.Result(ctx); !errors.Is(err, ErrRunCancelled) {
		t.Fatalf("in-progress run: want ErrRunCancelled, got %v", err)
	}
	waitStarted(t, started, "b")
	close(release)
	if out, err := h2.Result(ctx); err != nil || out != "b" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

func TestConcurrencyCancelQueuedExceptNewest(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	task, started, release := strategyTask("cs.excnewest", ConcurrencyCancelQueuedExceptNewest)

	h1, err := Enqueue(ctx, q, task, "a")
	if err != nil {
		t.Fatal(err)
	}
	waitStarted(t, started, "a")
	h2, err := Enqueue(ctx, q, task, "b") // queued
	if err != nil {
		t.Fatal(err)
	}
	h3, err := Enqueue(ctx, q, task, "c") // keeps newest c, cancels older queued b
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h2.Result(ctx); !errors.Is(err, ErrRunCancelled) {
		t.Fatalf("older queued run: want ErrRunCancelled, got %v", err)
	}
	close(release)
	if out, err := h3.Result(ctx); err != nil || out != "c" {
		t.Fatalf("newest out=%q err=%v", out, err)
	}
	if _, err := h1.Result(ctx); err != nil {
		t.Fatalf("first run: %v", err)
	}
}

func TestConcurrencyCancelQueuedExceptOldest(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	task, started, release := strategyTask("cs.excoldest", ConcurrencyCancelQueuedExceptOldest)

	h1, err := Enqueue(ctx, q, task, "a")
	if err != nil {
		t.Fatal(err)
	}
	waitStarted(t, started, "a")
	h2, err := Enqueue(ctx, q, task, "b") // oldest queued, kept
	if err != nil {
		t.Fatal(err)
	}
	h3, err := Enqueue(ctx, q, task, "c") // newer queued, cancelled
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h3.Result(ctx); !errors.Is(err, ErrRunCancelled) {
		t.Fatalf("newer queued run: want ErrRunCancelled, got %v", err)
	}
	close(release)
	if out, err := h2.Result(ctx); err != nil || out != "b" {
		t.Fatalf("oldest queued out=%q err=%v", out, err)
	}
	if _, err := h1.Result(ctx); err != nil {
		t.Fatalf("first run: %v", err)
	}
}
