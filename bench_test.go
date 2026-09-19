package quacker

import (
	"context"
	"os"
	"strconv"
	"sync/atomic"
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

// BenchmarkEnqueueBatch measures the cost of enqueuing 100 runs in one
// transaction (per-op = 100 runs).
func BenchmarkEnqueueBatch(b *testing.B) {
	q, err := Open(WithStorage(Memory()), WithPollInterval(5*time.Millisecond))
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	defer q.Close(ctx)

	task := NewTask("batch", func(ctx context.Context, in benchIn) (benchOut, error) {
		return benchOut{N: in.N}, nil
	})
	inputs := make([]benchIn, 100)
	for i := range inputs {
		inputs[i] = benchIn{N: i}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := EnqueueBatch(context.Background(), q, task, inputs); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEnqueueParallel measures enqueue contention from concurrent
// producers; run with -cpu 1,4,10 to see the single-writer effect.
func BenchmarkEnqueueParallel(b *testing.B) {
	q, err := Open(WithStorage(Memory()), WithPollInterval(5*time.Millisecond))
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	defer q.Close(ctx)

	task := NewTask("parallel-bench", func(ctx context.Context, in benchIn) (benchOut, error) {
		return benchOut{N: in.N}, nil
	})
	var n atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := int(n.Add(1))
			if _, err := Enqueue(context.Background(), q, task, benchIn{N: i}); err != nil {
				b.Fatal(err)
			}
		}
	})
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

// BenchmarkSaturatedThroughput measures jobs/sec at saturation: enqueue a batch
// of n runs, then wait for every run to finish with all workers and the single
// writer busy. ns/op covers insert + execution; jobs/sec = n / (ns/op) * 1e9.
// Run with -benchtime 1x; tweak saturatedN / the queue concurrency to probe the
// ceiling. Ephemeral (WAL) is used because Memory cannot use WAL and is
// markedly slower under this read/write mix.
func BenchmarkSaturatedThroughput(b *testing.B) {
	saturatedN, workers := 5000, 64
	if v := os.Getenv("QUACKER_SAT_N"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			saturatedN = n
		}
	}
	if v := os.Getenv("QUACKER_SAT_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			workers = n
		}
	}
	q, err := Open(
		WithStorage(Ephemeral()),
		WithPollInterval(time.Millisecond),
		WithQueue("bench", workers),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer q.Close(context.Background())

	task := NewTask("sat", func(ctx context.Context, in benchIn) (benchOut, error) {
		return benchOut{N: in.N}, nil
	}, Queue("bench"))
	inputs := make([]benchIn, saturatedN)
	for i := range inputs {
		inputs[i] = benchIn{N: i}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hs, err := EnqueueBatch(context.Background(), q, task, inputs)
		if err != nil {
			b.Fatal(err)
		}
		for _, h := range hs {
			if _, err := h.Result(context.Background()); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.ReportMetric(float64(saturatedN)*1e9/float64(b.Elapsed().Nanoseconds()/int64(b.N)), "runs/s")
}
