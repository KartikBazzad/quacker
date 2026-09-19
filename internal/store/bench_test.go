package store

import (
	"context"
	"fmt"
	"testing"
)

// BenchmarkCreateRunsBatch isolates the batch-insert win: one transaction for
// n runs. Divide ns/op by n for the per-run cost, and compare n=1 (the old
// one-transaction-per-run path) with n=100.
func BenchmarkCreateRunsBatch(b *testing.B) {
	for _, n := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			s, err := Open(Config{Mode: ModeEphemeral})
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			ctx := context.Background()
			now := nowUnix()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runs := make([]*Run, n)
				steps := make([][]*Step, n)
				for j := 0; j < n; j++ {
					id := fmt.Sprintf("r-%d-%d", i, j)
					runs[j] = &Run{ID: id, Workflow: "w", Kind: KindTask, Status: StatusQueued, Queue: "q", RunAt: now, CreatedAt: now, MaxAttempts: 1}
					steps[j] = []*Step{{ID: id + "/s", RunID: id, Name: "s", Task: "t", Ord: 0, Status: StatusQueued, Queue: "q", RunAt: now, CreatedAt: now, MaxAttempts: 1}}
				}
				if err := s.CreateRuns(ctx, runs, steps); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
