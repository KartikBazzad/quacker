package quacker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// blockingTask returns a task whose body blocks until release is closed, so a
// run stays non-terminal for the duration of a test.
func blockingTask(t *testing.T, name string) (*Task[string, string], chan struct{}) {
	t.Helper()
	release := make(chan struct{})
	task := NewTask(name, func(ctx context.Context, in string) (string, error) {
		select {
		case <-release:
			return "ok:" + in, nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}, WithUnique(func(in string) string { return in }))
	return task, release
}

func TestUniqueReuse(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	task, release := blockingTask(t, "uq.reuse")

	h1, err := Enqueue(ctx, q, task, "a")
	if err != nil {
		t.Fatal(err)
	}
	// The first run is still non-terminal, so the second reuses it.
	h2, err := Enqueue(ctx, q, task, "a")
	if err != nil {
		t.Fatal(err)
	}
	if h1.RunID() != h2.RunID() {
		t.Fatalf("reuse ids differ: %s vs %s", h1.RunID(), h2.RunID())
	}
	// A different key is independent.
	h3, err := Enqueue(ctx, q, task, "b")
	if err != nil {
		t.Fatal(err)
	}
	if h3.RunID() == h1.RunID() {
		t.Fatal("different keys share a run")
	}
	close(release)
	if out, err := h2.Result(ctx); err != nil || out != "ok:a" {
		t.Fatalf("reused result = %q, %v", out, err)
	}
	if _, err := h1.Result(ctx); err != nil {
		t.Fatalf("original result: %v", err)
	}
	// Once terminal, the key is free.
	h4, err := Enqueue(ctx, q, task, "a")
	if err != nil {
		t.Fatal(err)
	}
	if h4.RunID() == h1.RunID() {
		t.Fatal("unique key not freed after terminal")
	}
}

func TestUniqueError(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	release := make(chan struct{})
	task := NewTask("uq.error", func(ctx context.Context, in string) (string, error) {
		select {
		case <-release:
			return in, nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}, WithUnique(func(in string) string { return in }), WithUniqueConflict(UniqueError))

	if _, err := Enqueue(ctx, q, task, "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := Enqueue(ctx, q, task, "k"); !errors.Is(err, ErrDuplicateJob) {
		t.Fatalf("want ErrDuplicateJob, got %v", err)
	}
	close(release)
}

func TestUniqueReplace(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	release := make(chan struct{})
	task := NewTask("uq.replace", func(ctx context.Context, in string) (string, error) {
		select {
		case <-release:
			return in, nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}, WithUnique(func(in string) string { return in }), WithUniqueConflict(UniqueReplace))

	h1, err := Enqueue(ctx, q, task, "k")
	if err != nil {
		t.Fatal(err)
	}
	h2, err := Enqueue(ctx, q, task, "k")
	if err != nil {
		t.Fatal(err)
	}
	if h1.RunID() == h2.RunID() {
		t.Fatal("replace should create a new run")
	}
	if _, err := h1.Result(ctx); !errors.Is(err, ErrRunCancelled) {
		t.Fatalf("replaced run: want ErrRunCancelled, got %v", err)
	}
	close(release)
	if _, err := h2.Result(ctx); err != nil {
		t.Fatalf("replacement result: %v", err)
	}
}

func TestUniqueConflictWithBatch(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	task := NewTask("uq.batch", func(ctx context.Context, in string) (string, error) {
		return in, nil
	}, WithUnique(func(in string) string { return in }))

	hs, err := EnqueueBatch(ctx, q, task, []string{"x", "x", "y"})
	if err != nil {
		t.Fatal(err)
	}
	if hs[0].RunID() != hs[1].RunID() {
		t.Fatalf("in-batch duplicate not reused: %s vs %s", hs[0].RunID(), hs[1].RunID())
	}
	if hs[2].RunID() == hs[0].RunID() {
		t.Fatal("distinct key reused")
	}
}

func TestUniqueConcurrent(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	release := make(chan struct{})
	task := NewTask("uq.concurrent", func(ctx context.Context, in string) (string, error) {
		select {
		case <-release:
			return in, nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}, WithUnique(func(in string) string { return in }))

	const n = 16
	ids := make([]string, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			h, err := Enqueue(ctx, q, task, "same")
			if h != nil {
				ids[i] = h.RunID()
			}
			errs[i] = err
		}(i)
	}
	wg.Wait()
	close(release)
	for i, err := range errs {
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
		if ids[i] != ids[0] {
			t.Fatalf("id[%d]=%s, want %s", i, ids[i], ids[0])
		}
	}
	runs, err := q.Runs(ctx, RunFilter{Workflow: "uq.concurrent"})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 {
		t.Fatalf("persisted %d runs, want 1", len(runs))
	}
}

func TestUniquePersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/uq.db"
	ctx := context.Background()

	q1, err := Open(WithStorage(File(path)), WithPollInterval(2*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	task := NewTask("uq.file", func(ctx context.Context, in string) (string, error) {
		return in, nil
	}, WithUnique(func(in string) string { return in }))
	// Scheduled in the future so it stays QUEUED and survives a clean close.
	h1, err := Enqueue(ctx, q1, task, "k", WithRunAt(time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	closeQ(t, q1)

	q2, err := Open(WithStorage(File(path)), WithPollInterval(2*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer closeQ(t, q2)
	Register(q2, task)
	h2, err := Enqueue(ctx, q2, task, "k", WithRunAt(time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	if h1.RunID() != h2.RunID() {
		t.Fatalf("unique key not persisted: %s vs %s", h1.RunID(), h2.RunID())
	}
}
