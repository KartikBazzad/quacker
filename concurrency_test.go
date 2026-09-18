package quacker

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type keyedIn struct{ K string }
type keyedOut struct{}

// TestPerKeySerializesRuns: WithKey and no explicit limit (default 1) —
// steps sharing a key never overlap, while different keys still run in
// parallel (the gate is per key, not a global serializer).
func TestPerKeySerializesRuns(t *testing.T) {
	q := newTestQ(t)
	var mu sync.Mutex
	inFlight := map[string]int{}
	maxPerKey := map[string]int{}
	global, maxGlobal := 0, 0
	task := NewTask("keyed", func(ctx context.Context, in keyedIn) (keyedOut, error) {
		mu.Lock()
		inFlight[in.K]++
		global++
		if inFlight[in.K] > maxPerKey[in.K] {
			maxPerKey[in.K] = inFlight[in.K]
		}
		if global > maxGlobal {
			maxGlobal = global
		}
		mu.Unlock()
		time.Sleep(30 * time.Millisecond) // hold the slot long enough to collide
		mu.Lock()
		inFlight[in.K]--
		global--
		mu.Unlock()
		return keyedOut{}, nil
	}, WithKey(func(in keyedIn) string { return in.K }))

	var hs []*RunHandle[keyedOut]
	for i := 0; i < 4; i++ {
		h, err := Enqueue(context.Background(), q, task, keyedIn{K: "a"})
		if err != nil {
			t.Fatal(err)
		}
		hs = append(hs, h)
	}
	for i := 0; i < 3; i++ {
		h, err := Enqueue(context.Background(), q, task, keyedIn{K: "b"})
		if err != nil {
			t.Fatal(err)
		}
		hs = append(hs, h)
	}
	for _, h := range hs {
		if _, err := h.Result(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if maxPerKey["a"] != 1 || maxPerKey["b"] != 1 {
		t.Fatalf("max concurrent per key = a:%d b:%d, want 1/1", maxPerKey["a"], maxPerKey["b"])
	}
	if maxGlobal < 2 {
		t.Fatalf("keys serialized globally (maxGlobal=%d); different keys should overlap", maxGlobal)
	}
}

// TestPerKeyConcurrencyN: WithKeyConcurrency(2) lets exactly two same-key
// steps run at once.
func TestPerKeyConcurrencyN(t *testing.T) {
	q := newTestQ(t)
	var mu sync.Mutex
	inFlight, maxInFlight := 0, 0
	task := NewTask("keyed2", func(ctx context.Context, in keyedIn) (keyedOut, error) {
		mu.Lock()
		inFlight++
		if inFlight > maxInFlight {
			maxInFlight = inFlight
		}
		mu.Unlock()
		time.Sleep(30 * time.Millisecond)
		mu.Lock()
		inFlight--
		mu.Unlock()
		return keyedOut{}, nil
	}, WithKey(func(in keyedIn) string { return in.K }), WithKeyConcurrency(2))

	var hs []*RunHandle[keyedOut]
	for i := 0; i < 5; i++ {
		h, err := Enqueue(context.Background(), q, task, keyedIn{K: "same"})
		if err != nil {
			t.Fatal(err)
		}
		hs = append(hs, h)
	}
	for _, h := range hs {
		if _, err := h.Result(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if maxInFlight != 2 {
		t.Fatalf("max same-key concurrency = %d, want 2", maxInFlight)
	}
}

// TestPerKeyAcrossQueues: the gate is global over the key, not the queue —
// different tasks on different queues sharing a key still serialize.
func TestPerKeyAcrossQueues(t *testing.T) {
	q := newTestQ(t)
	var mu sync.Mutex
	inFlight, maxInFlight := 0, 0
	body := func(ctx context.Context, in keyedIn) (keyedOut, error) {
		mu.Lock()
		inFlight++
		if inFlight > maxInFlight {
			maxInFlight = inFlight
		}
		mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		mu.Lock()
		inFlight--
		mu.Unlock()
		return keyedOut{}, nil
	}
	t1 := NewTask("kq1", body, Queue("q1"), WithKey(func(in keyedIn) string { return in.K }))
	t2 := NewTask("kq2", body, Queue("q2"), WithKey(func(in keyedIn) string { return in.K }))

	h1, err := Enqueue(context.Background(), q, t1, keyedIn{K: "shared"})
	if err != nil {
		t.Fatal(err)
	}
	h2, err := Enqueue(context.Background(), q, t2, keyedIn{K: "shared"})
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range []*RunHandle[keyedOut]{h1, h2} {
		if _, err := h.Result(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if maxInFlight != 1 {
		t.Fatalf("cross-queue same-key concurrency = %d, want 1", maxInFlight)
	}
}

// TestKeyGateHoldsQueued: while a same-key step is RUNNING a second step
// stays QUEUED — deterministically, not just "happens not to overlap".
func TestKeyGateHoldsQueued(t *testing.T) {
	q := newTestQ(t)
	release := make(chan struct{})
	started := make(chan string, 4)
	task := NewTask("keyed-hold", func(ctx context.Context, in keyedIn) (keyedOut, error) {
		started <- in.K
		<-release
		return keyedOut{}, nil
	}, WithKey(func(in keyedIn) string { return in.K }))

	h1, err := Enqueue(context.Background(), q, task, keyedIn{K: "k"})
	if err != nil {
		t.Fatal(err)
	}
	<-started // first run is executing and holding the key
	h2, err := Enqueue(context.Background(), q, task, keyedIn{K: "k"})
	if err != nil {
		t.Fatal(err)
	}
	// The second run must remain QUEUED while the key is held.
	time.Sleep(150 * time.Millisecond)
	snap, err := q.Execution(context.Background(), h2.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if snap.Steps[0].Status != StatusQueued {
		t.Fatalf("key-gated step status = %s, want QUEUED", snap.Steps[0].Status)
	}
	close(release)
	for _, h := range []*RunHandle[keyedOut]{h1, h2} {
		if _, err := h.Result(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

// TestKeyVisibleInSnapshot: the persisted key is observable through
// Execution on both the run and its step.
func TestKeyVisibleInSnapshot(t *testing.T) {
	q := newTestQ(t)
	task := NewTask("keyed-snap", func(ctx context.Context, in keyedIn) (keyedOut, error) {
		return keyedOut{}, nil
	}, WithKey(func(in keyedIn) string { return in.K }))
	h, err := Enqueue(context.Background(), q, task, keyedIn{K: "user-42"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	snap, err := q.Execution(context.Background(), h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if snap.Key != "user-42" || snap.Steps[0].Key != "user-42" {
		t.Fatalf("snapshot key = run:%q step:%q, want user-42", snap.Key, snap.Steps[0].Key)
	}
}

// TestRateLimitWindow: a rate-capped queue never exceeds N starts in any
// trailing window. The assertion reads claimed_at — the persisted column
// the gate counts — rather than task-side timestamps, whose goroutine
// scheduling jitter can shift an observed start a few ms across a window
// edge even when every claim was legal.
func TestRateLimitWindow(t *testing.T) {
	const limit = 3
	window := 400 * time.Millisecond
	q := newTestQ(t, WithRate("limited", limit, window))
	task := NewTask("rl", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	}, Queue("limited"))

	const n = 9
	var hs []*RunHandle[greetOut]
	for i := 0; i < n; i++ {
		h, err := Enqueue(context.Background(), q, task, greetIn{})
		if err != nil {
			t.Fatal(err)
		}
		hs = append(hs, h)
	}
	for _, h := range hs {
		if _, err := h.Result(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := q.st.Read().Query(`SELECT claimed_at FROM steps WHERE queue='limited' ORDER BY claimed_at`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var claims []int64
	for rows.Next() {
		var c int64
		if err := rows.Scan(&c); err != nil {
			t.Fatal(err)
		}
		claims = append(claims, c)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(claims) != n {
		t.Fatalf("claims = %d, want %d", len(claims), n)
	}
	// Every window of `window` length beginning at a claim holds at most
	// `limit` claims.
	wns := int64(window)
	for _, t0 := range claims {
		c := 0
		for _, s := range claims {
			if s >= t0 && s < t0+wns {
				c++
			}
		}
		if c > limit {
			t.Fatalf("%d claims inside window at %d (limit %d)", c, t0, limit)
		}
	}
	// 9 tasks at 3 per 400ms: the last claim is at least ~2 windows out.
	if spread := claims[len(claims)-1] - claims[0]; spread < 700*int64(time.Millisecond) {
		t.Fatalf("claims spread over %dns, want >= ~700ms", spread)
	}
}

// TestRateLimitSurvivesRestart: the rate window counts persisted claim
// timestamps, so a File-mode reopen inherits the pre-restart budget — an
// in-memory token bucket would allow a fresh burst.
func TestRateLimitSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	opts := []Option{
		WithStorage(File(path)),
		WithPollInterval(5 * time.Millisecond),
		WithRate("limited", 2, 3*time.Second),
	}
	open := func(t *testing.T) *Quacker {
		q, err := Open(opts...)
		if err != nil {
			t.Fatal(err)
		}
		return q
	}
	close := func(q *Quacker) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = q.Close(ctx)
	}
	task := NewTask("rl-file", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	}, Queue("limited"))

	q1 := open(t)
	for i := 0; i < 2; i++ {
		h, err := Enqueue(context.Background(), q1, task, greetIn{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.Result(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	close(q1)

	// Reopen well inside the 3s window: both prior claims still count, so a
	// third run must sit QUEUED until the window slides past them.
	q2 := open(t)
	defer close(q2)
	h, err := Enqueue(context.Background(), q2, task, greetIn{})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)
	snap, err := q2.Execution(context.Background(), h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if snap.Steps[0].Status != StatusQueued {
		t.Fatalf("step status right after reopen = %s, want QUEUED (rate window must survive restart)", snap.Steps[0].Status)
	}
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
}
