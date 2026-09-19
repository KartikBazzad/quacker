package quacker

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestUniqueFreesKeyOnFailure(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	task := NewTask("uqm.fail", func(ctx context.Context, in string) (string, error) {
		return "", errors.New("boom")
	}, WithUnique(func(in string) string { return in }))

	h, err := Enqueue(ctx, q, task, "k")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(ctx); err == nil {
		t.Fatal("expected failure")
	}
	// Terminal (failed) freed the key.
	h2, err := Enqueue(ctx, q, task, "k")
	if err != nil {
		t.Fatal(err)
	}
	if h2.RunID() == h.RunID() {
		t.Fatal("key not freed after failure")
	}
}

func TestUniqueFreesKeyOnCancel(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	release := make(chan struct{})
	task := NewTask("uqm.cancel", func(ctx context.Context, in string) (string, error) {
		select {
		case <-release:
			return in, nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}, WithUnique(func(in string) string { return in }))

	h, err := Enqueue(ctx, q, task, "k")
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
	h2, err := Enqueue(ctx, q, task, "k")
	if err != nil {
		t.Fatal(err)
	}
	if h2.RunID() == h.RunID() {
		t.Fatal("key not freed after cancel")
	}
}

func TestUniqueScopedPerTask(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	release := make(chan struct{})
	mk := func(name string) *Task[string, string] {
		return NewTask(name, func(ctx context.Context, in string) (string, error) {
			select {
			case <-release:
				return in, nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}, WithUnique(func(in string) string { return in }))
	}
	t1, t2 := mk("uqm.t1"), mk("uqm.t2")

	h1, err := Enqueue(ctx, q, t1, "k")
	if err != nil {
		t.Fatal(err)
	}
	h2, err := Enqueue(ctx, q, t2, "k")
	if err != nil {
		t.Fatal(err)
	}
	if h1.RunID() == h2.RunID() {
		t.Fatal("same key across different tasks must be independent")
	}
	close(release)
}

func TestUniqueWithUniqueKeyOption(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	release := make(chan struct{})
	task := NewTask("uqm.keyopt", func(ctx context.Context, in string) (string, error) {
		select {
		case <-release:
			return in, nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}) // no WithUnique: key comes from the enqueue option
	h1, err := Enqueue(ctx, q, task, "a", WithUniqueKey("shared"))
	if err != nil {
		t.Fatal(err)
	}
	h2, err := Enqueue(ctx, q, task, "b", WithUniqueKey("shared"))
	if err != nil {
		t.Fatal(err)
	}
	if h1.RunID() != h2.RunID() {
		t.Fatalf("WithUniqueKey reuse: %s vs %s", h1.RunID(), h2.RunID())
	}
	close(release)
}

func TestUniqueWorkflowKey(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	release := make(chan struct{})
	step := NewTask("uqm.wfstep", func(ctx context.Context, in string) (string, error) {
		select {
		case <-release:
			return in, nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
	wf := NewWorkflow[string]("uqm.wf", Step("only", step))

	h1, err := EnqueueWorkflow[string](ctx, q, wf, "in", WithUniqueKey("wfk"))
	if err != nil {
		t.Fatal(err)
	}
	h2, err := EnqueueWorkflow[string](ctx, q, wf, "in", WithUniqueKey("wfk"))
	if err != nil {
		t.Fatal(err)
	}
	if h1.RunID() != h2.RunID() {
		t.Fatalf("workflow unique reuse: %s vs %s", h1.RunID(), h2.RunID())
	}
	close(release)
}

func TestUniqueErrorInBatchIsAtomic(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	task := NewTask("uqm.batch", func(ctx context.Context, in string) (string, error) {
		return in, nil
	}, WithUnique(func(in string) string { return in }), WithUniqueConflict(UniqueError))

	if _, err := EnqueueBatch(ctx, q, task, []string{"a", "a"}); !errors.Is(err, ErrDuplicateJob) {
		t.Fatalf("want ErrDuplicateJob, got %v", err)
	}
	// All-or-nothing: the first insert rolled back with the batch.
	runs, err := q.Runs(ctx, RunFilter{Workflow: "uqm.batch"})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 0 {
		t.Fatalf("batch not atomic: %d runs persisted", len(runs))
	}
}

func TestUniqueReuseSurvivesPause(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	task := NewTask("uqm.pausereuse", func(ctx context.Context, in string) (string, error) { return in, nil },
		WithUnique(func(in string) string { return in }))
	h, err := Enqueue(ctx, q, task, "k", WithRunAt(time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	if err := q.PauseRun(ctx, h.RunID()); err != nil {
		t.Fatal(err)
	}
	// A paused run is non-terminal, so it still holds the key.
	h2, err := Enqueue(ctx, q, task, "k", WithRunAt(time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	if h2.RunID() != h.RunID() {
		t.Fatalf("paused run lost its unique key: %s vs %s", h.RunID(), h2.RunID())
	}
}
