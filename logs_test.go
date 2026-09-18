package quacker

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestTaskLogSinkCustom: WithTaskLogSink receives lines and built-in storage
// stays empty (opt-in).
func TestTaskLogSinkCustom(t *testing.T) {
	var mu sync.Mutex
	var got []LogEntry
	q := newTestQ(t, WithTaskLogSink(func(e LogEntry) {
		mu.Lock()
		got = append(got, e)
		mu.Unlock()
	}))
	task := NewTask("sink-task", func(ctx context.Context, in greetIn) (greetOut, error) {
		TaskLogger(ctx).Info("to the sink")
		return greetOut{}, nil
	})
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0].Message != "to the sink" || got[0].Step != "sink-task" || got[0].RunID != h.RunID() {
		t.Fatalf("sink entries = %+v", got)
	}
	logs, err := q.Logs(context.Background(), h.RunID(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 0 {
		t.Fatalf("built-in logs = %+v, want none", logs)
	}
}

// TestLogStorageAndSink: WithLogStorage(true) persists while a custom sink
// still receives the same lines.
func TestLogStorageAndSink(t *testing.T) {
	var mu sync.Mutex
	sunk := 0
	q := newTestQ(t, WithLogStorage(true), WithTaskLogSink(func(e LogEntry) {
		mu.Lock()
		sunk++
		mu.Unlock()
	}))
	task := NewTask("both-task", func(ctx context.Context, in greetIn) (greetOut, error) {
		TaskLogger(ctx).Info("both sinks")
		return greetOut{}, nil
	})
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	var logs []LogEntry
	waitFor(t, 3*time.Second, func() bool {
		logs, _ = q.Logs(context.Background(), h.RunID(), 10)
		return len(logs) >= 1
	})
	mu.Lock()
	defer mu.Unlock()
	if sunk != 1 {
		t.Fatalf("custom sink saw %d lines, want 1", sunk)
	}
}
