package quacker

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestEphemeralSuccessGone(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	task := NewTask("eph.ok", func(ctx context.Context, in string) (string, error) { return in, nil }, WithEphemeral())
	h, err := Enqueue(ctx, q, task, "x")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := h.Result(ctx); err != nil || out != "x" {
		t.Fatalf("out=%q err=%v", out, err)
	}
	if _, err := q.Execution(ctx, h.RunID()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ephemeral run still present: %v", err)
	}
}

func TestEphemeralFailureGoneNotDeadLettered(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	task := NewTask("eph.fail", func(ctx context.Context, in string) (string, error) {
		return "", errors.New("boom")
	}, WithEphemeral(), WithDeadLetter())
	h, err := Enqueue(ctx, q, task, "x")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(ctx); err == nil {
		t.Fatal("expected failure")
	}
	if _, err := q.Execution(ctx, h.RunID()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("failed ephemeral run still present: %v", err)
	}
	if l, err := q.DeadLetters(ctx, DeadLetterFilter{}); err != nil || len(l) != 0 {
		t.Fatalf("ephemeral run dead-lettered: %+v err=%v", l, err)
	}
}

func TestEphemeralCancelGone(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	release := make(chan struct{})
	task := NewTask("eph.cancel", func(ctx context.Context, in string) (string, error) {
		select {
		case <-release:
			return in, nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}, WithEphemeral())
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
	if _, err := q.Execution(ctx, h.RunID()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cancelled ephemeral run still present: %v", err)
	}
}

func TestEphemeralNoRecovery(t *testing.T) {
	path := t.TempDir() + "/eph.db"
	ctx := context.Background()
	q1, err := Open(WithStorage(File(path)), WithPollInterval(2*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	task := NewTask("eph.file", func(ctx context.Context, in string) (string, error) { return in, nil }, WithEphemeral())
	// Scheduled ahead so it is still QUEUED at close.
	h, err := Enqueue(ctx, q1, task, "x", WithRunAt(time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	closeQ(t, q1)

	q2, err := Open(WithStorage(File(path)), WithPollInterval(2*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer closeQ(t, q2)
	if _, err := q2.Execution(ctx, h.RunID()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ephemeral run recovered on reopen: %v", err)
	}
}

func TestEphemeralUniqueKeyFreed(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	task := NewTask("eph.uniq", func(ctx context.Context, in string) (string, error) { return in, nil },
		WithEphemeral(), WithUnique(func(in string) string { return in }))
	h1, err := Enqueue(ctx, q, task, "k")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h1.Result(ctx); err != nil {
		t.Fatal(err)
	}
	h2, err := Enqueue(ctx, q, task, "k")
	if err != nil {
		t.Fatal(err)
	}
	if h2.RunID() == h1.RunID() {
		t.Fatal("key not freed after ephemeral terminal")
	}
	if _, err := h2.Result(ctx); err != nil {
		t.Fatal(err)
	}
}
