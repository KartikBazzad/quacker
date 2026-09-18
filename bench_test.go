package quacker

import (
	"context"
	"testing"
	"time"
)

type benchIn struct{ N int }
type benchOut struct{ N int }

// BenchmarkEnqueueRun measures enqueue latency (scheduler and workers running
// concurrently against the same SQLite database).
func BenchmarkEnqueueRun(b *testing.B) {
	q, err := Open(WithStorage(Memory()), WithPollInterval(5*time.Millisecond))
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	defer q.Close(ctx)

	task := NewTask("bench", func(ctx context.Context, in benchIn) (benchOut, error) {
		return benchOut{N: in.N}, nil
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Enqueue(ctx, q, task, benchIn{N: i}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkThroughput measures end-to-end executions per second with results
// awaited (workers, state writes, and introspection all active).
func BenchmarkThroughput(b *testing.B) {
	q, err := Open(
		WithStorage(Memory()),
		WithPollInterval(5*time.Millisecond),
		WithQueue("bench", 64),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer q.Close(context.Background())

	task := NewTask("work", func(ctx context.Context, in benchIn) (benchOut, error) {
		return benchOut{N: in.N * 2}, nil
	}, Queue("bench"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h, err := Enqueue(context.Background(), q, task, benchIn{N: i})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := h.Result(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkExecutionSnapshot measures read-side cost while the engine is
// under write load — the "introspection never pauses execution" guarantee.
func BenchmarkExecutionSnapshot(b *testing.B) {
	q, err := Open(WithStorage(Memory()), WithPollInterval(5*time.Millisecond))
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	defer q.Close(ctx)

	task := NewTask("noise", func(ctx context.Context, in benchIn) (benchOut, error) {
		return benchOut{N: in.N}, nil
	})
	h, err := Enqueue(ctx, q, task, benchIn{})
	if err != nil {
		b.Fatal(err)
	}
	// Keep thousands of writes flowing during the benchmark.
	for i := 0; i < 20000; i++ {
		if _, err := Enqueue(ctx, q, task, benchIn{N: i}); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := q.Execution(ctx, h.RunID()); err != nil {
			b.Fatal(err)
		}
	}
}
