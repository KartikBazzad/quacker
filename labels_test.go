package quacker

import (
	"context"
	"testing"
	"time"
)

// TestLabelsRouteTasks: an engine only claims steps whose labels it covers.
func TestLabelsRouteTasks(t *testing.T) {
	gpu := newTestQ(t, WithWorkerLabels("gpu"))
	plain := newTestQ(t)

	labeled := NewTask("gpu-task", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{Greeting: "gpu"}, nil
	}, WithLabels("gpu"))
	any := NewTask("any-task", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{Greeting: "any"}, nil
	})

	// Unlabeled engine runs an unlabeled task.
	hu, err := Enqueue(context.Background(), plain, any, greetIn{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hu.Result(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Unlabeled engine must not claim the labeled task.
	hl, err := Enqueue(context.Background(), plain, labeled, greetIn{})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond)
	snap, err := plain.Execution(context.Background(), hl.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if snap.Steps[0].Status != StatusQueued {
		t.Fatalf("unlabeled engine status = %s, want QUEUED", snap.Steps[0].Status)
	}

	// GPU engine runs it.
	hl2, err := Enqueue(context.Background(), gpu, labeled, greetIn{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hl2.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestLabelsSubset: extra worker labels are fine; missing ones are not.
func TestLabelsSubset(t *testing.T) {
	q := newTestQ(t, WithWorkerLabels("gpu", "linux"))
	ok := NewTask("subset-ok", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	}, WithLabels("gpu"))
	missing := NewTask("subset-missing", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	}, WithLabels("gpu", "arm64"))

	ho, _ := Enqueue(context.Background(), q, ok, greetIn{})
	if _, err := ho.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	hm, _ := Enqueue(context.Background(), q, missing, greetIn{})
	time.Sleep(80 * time.Millisecond)
	snap, _ := q.Execution(context.Background(), hm.RunID())
	if snap.Steps[0].Status != StatusQueued {
		t.Fatalf("status = %s, want QUEUED (arm64 not available)", snap.Steps[0].Status)
	}
}

// TestLabelsVisibleInSnapshot: required labels show up on the step snapshot.
func TestLabelsVisibleInSnapshot(t *testing.T) {
	q := newTestQ(t, WithWorkerLabels("gpu", "linux"))
	task := NewTask("lbl", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	}, WithLabels("gpu", "linux"))
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	snap, _ := q.Execution(context.Background(), h.RunID())
	if got := snap.Steps[0].Labels; len(got) != 2 || got[0] != "gpu" || got[1] != "linux" {
		t.Fatalf("step labels = %v, want [gpu linux]", got)
	}
}
