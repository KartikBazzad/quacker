package quacker

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSnoozeDelaysRun(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	task := NewTask("snz.task", func(ctx context.Context, in string) (string, error) {
		return in, nil
	})

	// Far future so it stays QUEUED, then snooze to shortly ahead.
	h, err := Enqueue(ctx, q, task, "x", WithRunAt(time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(60 * time.Millisecond)
	if err := q.Snooze(ctx, h.RunID(), until); err != nil {
		t.Fatal(err)
	}
	snap, err := q.Execution(ctx, h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if snap.Status != StatusQueued {
		t.Fatalf("status = %s, want QUEUED", snap.Status)
	}
	if !snap.RunAt.Equal(until) {
		t.Fatalf("run_at = %s, want %s", snap.RunAt, until)
	}
	if _, err := h.Result(ctx); err != nil {
		t.Fatal(err)
	}
	if snap, err := q.Execution(ctx, h.RunID()); err != nil || snap.StartedAt.Before(until.Add(-5*time.Millisecond)) {
		t.Fatalf("started before snooze elapsed: %+v err=%v", snap, err)
	}
}

func TestSnoozeRunningRejected(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	started := make(chan struct{})
	release := make(chan struct{})
	task := NewTask("snz.run", func(ctx context.Context, in string) (string, error) {
		close(started)
		select {
		case <-release:
			return in, nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
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
	if err := q.Snooze(ctx, h.RunID(), time.Now().Add(time.Second)); !errors.Is(err, ErrRunRunning) {
		t.Fatalf("want ErrRunRunning, got %v", err)
	}
	close(release)
}

func TestSnoozeTerminalRejected(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	task := NewTask("snz.done", func(ctx context.Context, in string) (string, error) { return in, nil })
	h, err := Enqueue(ctx, q, task, "x")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(ctx); err != nil {
		t.Fatal(err)
	}
	if err := q.Snooze(ctx, h.RunID(), time.Now().Add(time.Second)); !errors.Is(err, ErrRunTerminal) {
		t.Fatalf("want ErrRunTerminal, got %v", err)
	}
	if err := q.Snooze(ctx, "nope", time.Now()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}
