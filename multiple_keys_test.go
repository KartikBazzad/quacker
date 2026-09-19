package quacker

import (
	"context"
	"sync"
	"testing"
	"time"
)

type mkIn struct{ Acct, Region string }

func TestMultipleKeyLimits(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	var mu sync.Mutex
	inFlight, maxInFlight := 0, 0
	task := NewTask("mk.multi", func(ctx context.Context, in mkIn) (string, error) {
		mu.Lock()
		inFlight++
		if inFlight > maxInFlight {
			maxInFlight = inFlight
		}
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		mu.Lock()
		inFlight--
		mu.Unlock()
		return in.Acct, nil
	},
		WithKeyLimit("acct", func(in mkIn) string { return in.Acct }, 1),
		WithKeyLimit("region", func(in mkIn) string { return in.Region }, 4),
	)

	// Same account, different regions: the account limit (1) serializes them.
	hs, err := EnqueueBatch(ctx, q, task, []mkIn{{"a", "r1"}, {"a", "r2"}, {"a", "r3"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range hs {
		if _, err := h.Result(ctx); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if maxInFlight != 1 {
		t.Fatalf("max in-flight = %d, want 1 (account key)", maxInFlight)
	}
}

func TestSecondKeyGates(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	var mu sync.Mutex
	inFlight, maxInFlight := 0, 0
	task := NewTask("mk.second", func(ctx context.Context, in mkIn) (string, error) {
		mu.Lock()
		inFlight++
		if inFlight > maxInFlight {
			maxInFlight = inFlight
		}
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		mu.Lock()
		inFlight--
		mu.Unlock()
		return in.Region, nil
	},
		WithKeyLimit("acct", func(in mkIn) string { return in.Acct }, 5),
		WithKeyLimit("region", func(in mkIn) string { return in.Region }, 1),
	)

	// Different accounts but one region: the second key serializes.
	hs, err := EnqueueBatch(ctx, q, task, []mkIn{{"a", "r"}, {"b", "r"}, {"c", "r"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range hs {
		if _, err := h.Result(ctx); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if maxInFlight != 1 {
		t.Fatalf("max in-flight = %d, want 1 (region key)", maxInFlight)
	}
}

func TestMultipleKeysParallel(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	release := make(chan struct{})
	task := NewTask("mk.par", func(ctx context.Context, in mkIn) (string, error) {
		<-release
		return in.Acct, nil
	},
		WithKeyLimit("acct", func(in mkIn) string { return in.Acct }, 1),
		WithKeyLimit("region", func(in mkIn) string { return in.Region }, 1),
	)
	h1, err := Enqueue(ctx, q, task, mkIn{"a", "r1"})
	if err != nil {
		t.Fatal(err)
	}
	h2, err := Enqueue(ctx, q, task, mkIn{"b", "r2"})
	if err != nil {
		t.Fatal(err)
	}
	// Different values for every key run in parallel.
	waitFor(t, 3*time.Second, func() bool {
		s1, e1 := q.Execution(ctx, h1.RunID())
		s2, e2 := q.Execution(ctx, h2.RunID())
		return e1 == nil && e2 == nil && s1.Status == StatusRunning && s2.Status == StatusRunning
	})
	close(release)
	if _, err := h1.Result(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := h2.Result(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSharedKeyAcrossTasks(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	var mu sync.Mutex
	inFlight, maxInFlight := 0, 0
	body := func(ctx context.Context, in string) (string, error) {
		mu.Lock()
		inFlight++
		if inFlight > maxInFlight {
			maxInFlight = inFlight
		}
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		mu.Lock()
		inFlight--
		mu.Unlock()
		return in, nil
	}
	// Two different tasks declare the same shared key name+limit.
	t1 := NewTask("mk.t1", body, WithKeyLimit("shared", func(in string) string { return in }, 1))
	t2 := NewTask("mk.t2", body, WithKeyLimit("shared", func(in string) string { return in }, 1))

	h1, err := Enqueue(ctx, q, t1, "x")
	if err != nil {
		t.Fatal(err)
	}
	h2, err := Enqueue(ctx, q, t2, "x")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h1.Result(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := h2.Result(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if maxInFlight != 1 {
		t.Fatalf("max in-flight = %d, want 1 (shared budget across tasks)", maxInFlight)
	}
}
