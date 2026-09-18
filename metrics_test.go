package quacker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestMetricsCallback: the callback fires on the interval with counts
// reflecting completed work.
func TestMetricsCallback(t *testing.T) {
	var mu sync.Mutex
	var last *Metrics
	q := newTestQ(t,
		WithMetricsFunc(func(m *Metrics) {
			mu.Lock()
			last = m
			mu.Unlock()
		}),
		WithMetricsInterval(10*time.Millisecond),
	)
	task := NewTask("m-ok", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	})
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return last != nil && last.Runs[StatusSucceeded] >= 1
	})
	mu.Lock()
	defer mu.Unlock()
	if last.Runs[StatusSucceeded] != 1 {
		t.Fatalf("succeeded = %d, want 1", last.Runs[StatusSucceeded])
	}
}

// TestMetricsCallbackPanicRecovered: a panicking callback is recovered and
// the loop keeps firing.
func TestMetricsCallbackPanicRecovered(t *testing.T) {
	var count int32
	q := newTestQ(t,
		WithMetricsFunc(func(m *Metrics) {
			if atomic.AddInt32(&count, 1) == 1 {
				panic("metrics boom")
			}
		}),
		WithMetricsInterval(10*time.Millisecond),
	)
	task := NewTask("m-panic", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	})
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool { return atomic.LoadInt32(&count) >= 2 })
}

// TestMetricsStopsOnClose: no callbacks fire after Close.
func TestMetricsStopsOnClose(t *testing.T) {
	var count int32
	q, err := Open(WithStorage(Memory()), WithPollInterval(5*time.Millisecond),
		WithMetricsFunc(func(m *Metrics) { atomic.AddInt32(&count, 1) }),
		WithMetricsInterval(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool { return atomic.LoadInt32(&count) >= 1 })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := q.Close(ctx); err != nil {
		t.Fatal(err)
	}
	at := atomic.LoadInt32(&count)
	time.Sleep(300 * time.Millisecond)
	if now := atomic.LoadInt32(&count); now != at {
		t.Fatalf("metrics callback ran after Close (%d -> %d)", at, now)
	}
}
