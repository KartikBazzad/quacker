package quacker

import (
	"context"
	"testing"
	"time"
)

// TestBatchedCompletionManyRuns enqueues enough runs that the batched completer
// has to flush repeatedly, and asserts every run finishes (no completion is
// dropped by the batching).
func TestBatchedCompletionManyRuns(t *testing.T) {
	q := newTestQ(t, WithPollInterval(time.Millisecond), WithQueue("bulk", 32))
	ctx := context.Background()
	task := NewTask("cmp.bulk", func(ctx context.Context, in int) (int, error) {
		return in * 2, nil
	}, Queue("bulk"))

	const n = 3000
	inputs := make([]int, n)
	for i := range inputs {
		inputs[i] = i
	}
	hs, err := EnqueueBatch(ctx, q, task, inputs)
	if err != nil {
		t.Fatal(err)
	}
	for i, h := range hs {
		out, err := h.Result(ctx)
		if err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
		if out != i*2 {
			t.Fatalf("run %d out = %d, want %d", i, out, i*2)
		}
	}
}

// TestBatchedCompletionDAG chains steps so each completion unblocks the next,
// exercising the batched path across many DAG advances.
func TestBatchedCompletionDAG(t *testing.T) {
	q := newTestQ(t, WithPollInterval(time.Millisecond))
	ctx := context.Background()
	steps := make([]StepDef[string], 0, 20)
	for i := 0; i < 20; i++ {
		name := string(rune('a' + i))
		task := NewTask("cmp.dag."+name, func(ctx context.Context, in string) (string, error) {
			return in + name, nil
		})
		if i == 0 {
			steps = append(steps, Step(name, task))
		} else {
			prev := string(rune('a' + i - 1))
			steps = append(steps, Step(name, task, prev))
		}
	}
	wf := NewWorkflow[string]("cmp.dag", steps...)
	h, err := EnqueueWorkflow[string](ctx, q, wf, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(ctx); err != nil {
		t.Fatal(err)
	}
}
