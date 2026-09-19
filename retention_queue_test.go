package quacker

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPurgeByQueue(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	taskA := NewTask("rq.a", func(ctx context.Context, in string) (string, error) { return in, nil }, Queue("qa"))
	taskB := NewTask("rq.b", func(ctx context.Context, in string) (string, error) { return in, nil }, Queue("qb"))

	ha, err := Enqueue(ctx, q, taskA, "x")
	if err != nil {
		t.Fatal(err)
	}
	hb, err := Enqueue(ctx, q, taskB, "y")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ha.Result(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := hb.Result(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)

	res, err := q.Purge(ctx, PurgeOptions{OlderThan: time.Millisecond, Queue: "qa"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Runs != 1 {
		t.Fatalf("purged %d runs, want 1", res.Runs)
	}
	if _, err := q.Execution(ctx, ha.RunID()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("qa run not purged: %v", err)
	}
	if _, err := q.Execution(ctx, hb.RunID()); err != nil {
		t.Fatalf("qb run wrongly purged: %v", err)
	}
	// An empty Queue still matches every queue.
	res, err = q.Purge(ctx, PurgeOptions{OlderThan: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if res.Runs != 1 {
		t.Fatalf("global purge removed %d, want 1", res.Runs)
	}
}

func TestRetentionPolicyByQueue(t *testing.T) {
	// The loop's interval is clamped to >= 1s, so this is the one slow test.
	q := newTestQ(t, WithRetention(RetentionPolicy{
		OlderThan: time.Millisecond,
		Queue:     "qa",
		Interval:  time.Second,
	}))
	ctx := context.Background()
	taskA := NewTask("rq.loop.a", func(ctx context.Context, in string) (string, error) { return in, nil }, Queue("qa"))
	taskB := NewTask("rq.loop.b", func(ctx context.Context, in string) (string, error) { return in, nil }, Queue("qb"))
	ha, err := Enqueue(ctx, q, taskA, "x")
	if err != nil {
		t.Fatal(err)
	}
	hb, err := Enqueue(ctx, q, taskB, "y")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ha.Result(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := hb.Result(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 4*time.Second, func() bool {
		_, err := q.Execution(ctx, ha.RunID())
		return errors.Is(err, ErrNotFound)
	})
	if _, err := q.Execution(ctx, hb.RunID()); err != nil {
		t.Fatalf("qb run purged by qa policy: %v", err)
	}
}
