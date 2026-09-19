package quacker

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// recorder collects execution order across task invocations.
type recorder struct {
	mu    sync.Mutex
	order []int
}

func (r *recorder) add(v int) {
	r.mu.Lock()
	r.order = append(r.order, v)
	r.mu.Unlock()
}

func (r *recorder) snapshot() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.order...)
}

func TestSequenceStrictOrder(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	var rec recorder
	task := NewTask("seq.order", func(ctx context.Context, in int) (int, error) {
		rec.add(in)
		return in, nil
	}, WithSequence(func(in int) string { return "s" }))

	hs, err := EnqueueBatch(ctx, q, task, []int{0, 1, 2, 3, 4})
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range hs {
		if _, err := h.Result(ctx); err != nil {
			t.Fatal(err)
		}
	}
	got := rec.snapshot()
	for i, v := range got {
		if v != i {
			t.Fatalf("execution order = %v, want [0 1 2 3 4]", got)
		}
	}
	if len(got) != 5 {
		t.Fatalf("ran %d jobs, want 5", len(got))
	}
}

// TestSequenceNoOvertakeOnRetry is the point of sequences: a retrying earlier
// job must block later jobs, unlike a plain concurrency key.
func TestSequenceNoOvertakeOnRetry(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	var rec recorder
	var mu sync.Mutex
	attempts := map[string]int{}
	task := NewTask("seq.retry", func(ctx context.Context, in string) (string, error) {
		mu.Lock()
		attempts[in]++
		n := attempts[in]
		mu.Unlock()
		if in == "a" && n == 1 {
			return "", fmt.Errorf("transient")
		}
		rec.add(int(in[0]))
		return in, nil
	}, WithSequence(func(in string) string { return "s" }),
		Retries(2), BackoffPolicy(Constant(60*time.Millisecond)))

	ha, err := Enqueue(ctx, q, task, "a")
	if err != nil {
		t.Fatal(err)
	}
	hb, err := Enqueue(ctx, q, task, "b")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hb.Result(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ha.Result(ctx); err != nil {
		t.Fatal(err)
	}
	if got := rec.snapshot(); len(got) != 2 || got[0] != int('a') || got[1] != int('b') {
		t.Fatalf("order = %v, want [a b] (a must not be overtaken while retrying)", got)
	}
}

func TestSequenceParallelAcrossKeys(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	release := make(chan struct{})
	task := NewTask("seq.parallel", func(ctx context.Context, in int) (int, error) {
		<-release
		return in, nil
	}, WithSequence(func(in int) string { return fmt.Sprintf("k%d", in) }))

	h0, err := Enqueue(ctx, q, task, 0)
	if err != nil {
		t.Fatal(err)
	}
	h1, err := Enqueue(ctx, q, task, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Different keys must be able to run at the same time.
	waitFor(t, 3*time.Second, func() bool {
		s0, e0 := q.Execution(ctx, h0.RunID())
		s1, e1 := q.Execution(ctx, h1.RunID())
		return e0 == nil && e1 == nil && s0.Status == StatusRunning && s1.Status == StatusRunning
	})
	close(release)
	for _, h := range []*RunHandle[int]{h0, h1} {
		if _, err := h.Result(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSequenceBlocksLaterRun(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	task := NewTask("seq.run", func(ctx context.Context, in string) (string, error) {
		once.Do(func() { close(started) })
		<-release
		return in, nil
	}, WithSequence(func(in string) string { return "s" }))

	h1, err := Enqueue(ctx, q, task, "one")
	if err != nil {
		t.Fatal(err)
	}
	h2, err := Enqueue(ctx, q, task, "two")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("first run never started")
	}
	// The second run's step must stay queued while the first is unfinished.
	time.Sleep(60 * time.Millisecond)
	s2, err := q.Execution(ctx, h2.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if s2.Status != StatusQueued {
		t.Fatalf("second run = %s, want QUEUED behind the first", s2.Status)
	}
	close(release)
	if _, err := h1.Result(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := h2.Result(ctx); err != nil {
		t.Fatal(err)
	}
}
