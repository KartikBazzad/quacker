package quacker

import (
	"context"
	"testing"
	"time"
)

func TestPauseQueueHoldsClaims(t *testing.T) {
	q := newTestQ(t, WithQueue("pq", 1))
	ctx := context.Background()
	task := NewTask("pq.task", func(ctx context.Context, in int) (int, error) {
		return in, nil
	}, Queue("pq"))

	if err := q.PauseQueue(ctx, "pq"); err != nil {
		t.Fatal(err)
	}
	hs, err := EnqueueBatch(ctx, q, task, []int{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond)
	runs, err := q.Runs(ctx, RunFilter{Workflow: "pq.task"})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 3 {
		t.Fatalf("runs = %d, want 3", len(runs))
	}
	for _, r := range runs {
		if r.Status != StatusQueued {
			t.Fatalf("run %s = %s, want QUEUED while paused", r.RunID, r.Status)
		}
	}
	paused, err := q.PausedQueues(ctx)
	if err != nil || len(paused) != 1 || paused[0] != "pq" {
		t.Fatalf("paused = %v err=%v", paused, err)
	}

	if err := q.ResumeQueue(ctx, "pq"); err != nil {
		t.Fatal(err)
	}
	for _, h := range hs {
		if _, err := h.Result(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if p, err := q.PausedQueues(ctx); err != nil || len(p) != 0 {
		t.Fatalf("still paused after resume: %v err=%v", p, err)
	}
}

func TestPauseQueueIdempotent(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	if err := q.PauseQueue(ctx, "never"); err != nil {
		t.Fatal(err)
	}
	if err := q.PauseQueue(ctx, "never"); err != nil { // idempotent
		t.Fatal(err)
	}
	if err := q.ResumeQueue(ctx, "never"); err != nil {
		t.Fatal(err)
	}
	if err := q.ResumeQueue(ctx, "never"); err != nil { // no-op
		t.Fatal(err)
	}
}

func TestPauseQueuePersists(t *testing.T) {
	path := t.TempDir() + "/pq.db"
	ctx := context.Background()
	q1, err := Open(WithStorage(File(path)), WithPollInterval(2*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := q1.PauseQueue(ctx, "pq"); err != nil {
		t.Fatal(err)
	}
	closeQ(t, q1)

	q2, err := Open(WithStorage(File(path)), WithPollInterval(2*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer closeQ(t, q2)
	paused, err := q2.PausedQueues(ctx)
	if err != nil || len(paused) != 1 || paused[0] != "pq" {
		t.Fatalf("pause not persisted: %v err=%v", paused, err)
	}
	// A new enqueue on the reopened engine stays held.
	task := NewTask("pq.file", func(ctx context.Context, in string) (string, error) { return in, nil }, Queue("pq"))
	h, err := Enqueue(ctx, q2, task, "x")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if snap, err := q2.Execution(ctx, h.RunID()); err != nil || snap.Status != StatusQueued {
		t.Fatalf("run = %+v err=%v, want QUEUED", snap, err)
	}
}
