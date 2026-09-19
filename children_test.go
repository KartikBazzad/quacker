package quacker

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestEnqueueChildLineage: a child enqueued from inside a task records the
// parent and is exposed under Execution.Children.
func TestEnqueueChildLineage(t *testing.T) {
	q := newTestQ(t)
	child := NewTask("child", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{Greeting: "child"}, nil
	})
	childID := make(chan string, 1)
	parent := NewTask("parent", func(ctx context.Context, in greetIn) (greetOut, error) {
		h, err := EnqueueChild(ctx, q, child, greetIn{Name: "c"})
		if err != nil {
			return greetOut{}, err
		}
		childID <- h.RunID()
		return greetOut{Greeting: "parent"}, nil
	})

	h, err := Enqueue(context.Background(), q, parent, greetIn{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	var cid string
	select {
	case cid = <-childID:
	case <-time.After(2 * time.Second):
		t.Fatal("parent never enqueued a child")
	}
	waitFor(t, 3*time.Second, func() bool {
		s, err := q.Execution(context.Background(), cid)
		return err == nil && s.Status == StatusSucceeded
	})
	snap, err := q.Execution(context.Background(), h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Children) != 1 {
		t.Fatalf("children = %d, want 1", len(snap.Children))
	}
	if snap.Children[0].RunID != cid || snap.Children[0].ParentID != h.RunID() {
		t.Fatalf("child = %+v, want id=%s parent=%s", snap.Children[0], cid, h.RunID())
	}
}

// TestChildFailureDoesNotFailParent: lineage only — a failed child leaves the
// parent untouched.
func TestChildFailureDoesNotFailParent(t *testing.T) {
	q := newTestQ(t)
	child := NewTask("bad-child", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, errors.New("child boom")
	})
	childID := make(chan string, 1)
	parent := NewTask("ok-parent", func(ctx context.Context, in greetIn) (greetOut, error) {
		h, err := EnqueueChild(ctx, q, child, greetIn{})
		if err != nil {
			return greetOut{}, err
		}
		childID <- h.RunID()
		return greetOut{Greeting: "parent ok"}, nil
	})
	h, _ := Enqueue(context.Background(), q, parent, greetIn{})
	out, err := h.Result(context.Background())
	if err != nil || out.Greeting != "parent ok" {
		t.Fatalf("parent out=%q err=%v", out.Greeting, err)
	}
	cid := <-childID
	waitFor(t, 3*time.Second, func() bool {
		s, err := q.Execution(context.Background(), cid)
		return err == nil && s.Status == StatusFailed
	})
	snap, _ := q.Execution(context.Background(), h.RunID())
	if len(snap.Children) != 1 || snap.Children[0].Status != StatusFailed {
		t.Fatalf("children = %+v, want one FAILED", snap.Children)
	}
}

// TestEnqueueChildOutsideTask: the helper is rejected without a parent run.
func TestEnqueueChildOutsideTask(t *testing.T) {
	q := newTestQ(t)
	task := NewTask("orphan", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	})
	if _, err := EnqueueChild(context.Background(), q, task, greetIn{}); err == nil {
		t.Fatal("expected an error outside a task")
	}
}

// TestRunFilterParentID: Runs can list a run's children directly.
func TestRunFilterParentID(t *testing.T) {
	q := newTestQ(t)
	child := NewTask("filter-child", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	})
	parent := NewTask("filter-parent", func(ctx context.Context, in greetIn) (greetOut, error) {
		for i := 0; i < 3; i++ {
			if _, err := EnqueueChild(ctx, q, child, greetIn{}); err != nil {
				return greetOut{}, err
			}
		}
		return greetOut{}, nil
	})
	h, _ := Enqueue(context.Background(), q, parent, greetIn{})
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		kids, err := q.Runs(context.Background(), RunFilter{ParentID: h.RunID()})
		return err == nil && len(kids) == 3
	})
}

// TestPurgeParentKeepsLiveChild: children are independent, so purging a
// terminal parent does not touch a still-running child.
func TestPurgeParentKeepsLiveChild(t *testing.T) {
	q := newTestQ(t)
	child := NewTask("live-child", func(ctx context.Context, in greetIn) (greetOut, error) {
		if err := SleepDurable(ctx, 10*time.Second); err != nil {
			return greetOut{}, err
		}
		return greetOut{}, nil
	})
	parent := NewTask("purge-parent", func(ctx context.Context, in greetIn) (greetOut, error) {
		if _, err := EnqueueChild(ctx, q, child, greetIn{}); err != nil {
			return greetOut{}, err
		}
		return greetOut{}, nil
	})
	h, _ := Enqueue(context.Background(), q, parent, greetIn{})
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	kids, err := q.Runs(context.Background(), RunFilter{ParentID: h.RunID()})
	if err != nil || len(kids) != 1 {
		t.Fatalf("children = %+v err=%v", kids, err)
	}
	childRunID := kids[0].RunID
	waitSuspended(t, q, childRunID)

	time.Sleep(3 * time.Millisecond)
	res, err := q.Purge(context.Background(), PurgeOptions{OlderThan: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if res.Runs != 1 {
		t.Fatalf("purged %d runs, want just the parent", res.Runs)
	}
	if _, err := q.Execution(context.Background(), h.RunID()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("parent still present: %v", err)
	}
	if _, err := q.Execution(context.Background(), childRunID); err != nil {
		t.Fatalf("live child was purged: %v", err)
	}
	if err := q.Cancel(childRunID); err != nil {
		t.Fatal(err)
	}
}
