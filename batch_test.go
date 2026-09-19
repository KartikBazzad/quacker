package quacker

import (
	"context"
	"testing"
)

// TestEnqueueBatch: many runs land in one transaction and each handle resolves.
func TestEnqueueBatch(t *testing.T) {
	q := newTestQ(t)
	task := NewTask("batch-task", func(ctx context.Context, in benchIn) (benchOut, error) {
		return benchOut{N: in.N * 2}, nil
	})
	inputs := make([]benchIn, 50)
	for i := range inputs {
		inputs[i] = benchIn{N: i}
	}
	hs, err := EnqueueBatch(context.Background(), q, task, inputs)
	if err != nil {
		t.Fatal(err)
	}
	if len(hs) != len(inputs) {
		t.Fatalf("handles = %d, want %d", len(hs), len(inputs))
	}
	for i, h := range hs {
		out, err := h.Result(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if out.N != i*2 {
			t.Fatalf("out[%d] = %d, want %d", i, out.N, i*2)
		}
	}
}

// TestEnqueueBatchAllOrNothing: a bad input rejects the whole batch.
func TestEnqueueBatchAllOrNothing(t *testing.T) {
	q := newTestQ(t)
	task := NewTask[any, greetOut]("bad-batch", func(ctx context.Context, in any) (greetOut, error) {
		return greetOut{}, nil
	})
	_, err := EnqueueBatch[any, greetOut](context.Background(), q, task, []any{1, make(chan int)})
	if err == nil {
		t.Fatal("expected a marshal error")
	}
	runs, err := q.Runs(context.Background(), RunFilter{Workflow: "bad-batch"})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 0 {
		t.Fatalf("runs = %d, want 0 (batch must be all-or-nothing)", len(runs))
	}
}

// TestEnqueueBatchEmpty: an empty batch is a no-op.
func TestEnqueueBatchEmpty(t *testing.T) {
	q := newTestQ(t)
	task := NewTask("empty-batch", func(ctx context.Context, in benchIn) (benchOut, error) {
		return benchOut{}, nil
	})
	hs, err := EnqueueBatch(context.Background(), q, task, nil)
	if err != nil || len(hs) != 0 {
		t.Fatalf("hs=%v err=%v, want empty", hs, err)
	}
}
