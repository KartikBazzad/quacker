package quacker

import (
	"context"
	"testing"
	"time"
)

// TestScheduledRunFiresAtDueTime: with a poll interval of an hour, a run
// scheduled 60ms out must still fire promptly — the scheduler sleeps until the
// store's next-due time rather than polling, so a due time wakes it.
func TestScheduledRunFiresAtDueTime(t *testing.T) {
	q := newTestQ(t, WithPollInterval(time.Hour))
	ctx := context.Background()
	task := NewTask("idle.sched", func(ctx context.Context, in string) (string, error) { return in, nil })

	start := time.Now()
	h, err := Enqueue(ctx, q, task, "x", WithRunAt(time.Now().Add(60*time.Millisecond)))
	if err != nil {
		t.Fatal(err)
	}
	rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	out, err := h.Result(rctx)
	if err != nil {
		t.Fatalf("scheduled run did not fire (poll is an hour): %v", err)
	}
	if out != "x" {
		t.Fatalf("out = %q", out)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("scheduled run took %v", elapsed)
	}
}

// TestIdleSchedulerStopsClaiming: once a queue has no due work, the scheduler
// must stop opening claim transactions every poll. With a 1ms poll it would
// otherwise open ~200 in 200ms.
func TestIdleSchedulerStopsClaiming(t *testing.T) {
	q := newTestQ(t, WithPollInterval(time.Millisecond), WithQueue("idle", 4))
	// Let the first (empty) tick land and the scheduler fall asleep.
	waitFor(t, 2*time.Second, func() bool { return q.st.ClaimTransactions() > 0 })
	time.Sleep(50 * time.Millisecond)

	before := q.st.ClaimTransactions()
	time.Sleep(200 * time.Millisecond)
	after := q.st.ClaimTransactions()
	if grown := after - before; grown > 3 {
		t.Fatalf("idle scheduler opened %d claim transactions in 200ms at a 1ms poll", grown)
	}
}
