package quacker

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestPurgeDeletesOldTerminalRuns: a completed run past the cutoff is removed
// with its logs, and stops being visible to Execution.
func TestPurgeDeletesOldTerminalRuns(t *testing.T) {
	q := newTestQ(t, WithLogStorage(true))
	task := NewTask("purge-me", func(ctx context.Context, in greetIn) (greetOut, error) {
		TaskLogger(ctx).Info("bye")
		return greetOut{}, nil
	})
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		logs, _ := q.Logs(context.Background(), h.RunID(), 10)
		return len(logs) >= 1
	})
	time.Sleep(3 * time.Millisecond)

	res, err := q.Purge(context.Background(), PurgeOptions{OlderThan: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if res.Runs != 1 || res.Steps != 1 || res.Logs < 1 {
		t.Fatalf("purge result = %+v, want 1 run, 1 step, >=1 log", res)
	}
	if _, err := q.Execution(context.Background(), h.RunID()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Execution after purge: err=%v, want ErrNotFound", err)
	}
}

// TestPurgeKeepsRecent: runs newer than the cutoff survive.
func TestPurgeKeepsRecent(t *testing.T) {
	q := newTestQ(t)
	task := NewTask("keep-me", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	})
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	res, err := q.Purge(context.Background(), PurgeOptions{OlderThan: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if res.Runs != 0 {
		t.Fatalf("purged %d recent runs, want 0", res.Runs)
	}
	if _, err := q.Execution(context.Background(), h.RunID()); err != nil {
		t.Fatalf("recent run gone: %v", err)
	}
}

// TestPurgeNonTerminalStatus: asking to purge a live status is rejected.
func TestPurgeNonTerminalStatus(t *testing.T) {
	q := newTestQ(t)
	_, err := q.Purge(context.Background(), PurgeOptions{
		OlderThan: time.Millisecond,
		Statuses:  []Status{StatusRunning},
	})
	if !errors.Is(err, ErrNonTerminalPurge) {
		t.Fatalf("err = %v, want ErrNonTerminalPurge", err)
	}
}

// TestPurgeRequiresOlderThan: a zero/negative window is refused.
func TestPurgeRequiresOlderThan(t *testing.T) {
	q := newTestQ(t)
	if _, err := q.Purge(context.Background(), PurgeOptions{}); err == nil {
		t.Fatal("expected error for OlderThan <= 0")
	}
}

// TestPurgeBatching: the batch loop removes more rows than one batch holds.
func TestPurgeBatching(t *testing.T) {
	q := newTestQ(t)
	task := NewTask("batch-me", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	})
	const n = 7
	for i := 0; i < n; i++ {
		h, err := Enqueue(context.Background(), q, task, greetIn{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.Result(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(3 * time.Millisecond)
	res, err := q.Purge(context.Background(), PurgeOptions{OlderThan: time.Millisecond, BatchSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	if res.Runs != n {
		t.Fatalf("purged %d runs, want %d", res.Runs, n)
	}
}

// TestPurgeKeepLogsThenOrphanSweep: KeepLogs leaves logs behind; a later
// purge cleans them via the orphan sweep even though the run is gone.
func TestPurgeKeepLogsThenOrphanSweep(t *testing.T) {
	q := newTestQ(t, WithLogStorage(true))
	task := NewTask("log-keeper", func(ctx context.Context, in greetIn) (greetOut, error) {
		TaskLogger(ctx).Info("retain me")
		return greetOut{}, nil
	})
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		logs, _ := q.Logs(context.Background(), h.RunID(), 10)
		return len(logs) >= 1
	})
	time.Sleep(3 * time.Millisecond)

	res, err := q.Purge(context.Background(), PurgeOptions{OlderThan: time.Millisecond, KeepLogs: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Runs != 1 || res.Logs != 0 {
		t.Fatalf("keep-logs purge = %+v, want 1 run and 0 logs", res)
	}
	if logs, _ := q.Logs(context.Background(), h.RunID(), 10); len(logs) == 0 {
		t.Fatal("logs deleted despite KeepLogs")
	}

	res, err = q.Purge(context.Background(), PurgeOptions{OlderThan: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if res.Runs != 0 || res.Logs < 1 {
		t.Fatalf("sweep purge = %+v, want 0 runs and >=1 log", res)
	}
	if logs, _ := q.Logs(context.Background(), h.RunID(), 10); len(logs) != 0 {
		t.Fatalf("orphan logs remain: %+v", logs)
	}
}

// TestRetentionAutoPurge: WithRetention removes old terminal runs on its own.
func TestRetentionAutoPurge(t *testing.T) {
	q := newTestQ(t, WithRetention(RetentionPolicy{
		OlderThan: time.Millisecond,
		Interval:  time.Second,
	}))
	task := NewTask("auto-purge", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	})
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 4*time.Second, func() bool {
		_, err := q.Execution(context.Background(), h.RunID())
		return errors.Is(err, ErrNotFound)
	})
}
