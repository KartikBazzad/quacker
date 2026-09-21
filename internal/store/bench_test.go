package store

import (
	"context"
	"fmt"
	"github.com/kartikbazzad/quacker/driver"
	"testing"
)

// BenchmarkWideDAGComplete measures the per-completion cost of finishing a
// chain of n steps: each CompleteStep unblocks the next and checks whether the
// run is terminal. It times the completions only (setup is outside the timer),
// so it exposes the O(n) per-completion scan.
func BenchmarkWideDAGComplete(b *testing.B) {
	for _, n := range []int{50, 200, 800} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			ctx := context.Background()
			out := []byte(`{"ok":true}`)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Fresh store per iteration so the completion loop sees only
				// this run's n rows (a shared store would accumulate rows and
				// distort the measurement). Setup is outside the timer.
				b.StopTimer()
				s, err := Open(driver.Config{Mode: driver.ModeEphemeral})
				if err != nil {
					b.Fatal(err)
				}
				run, steps := chainRun(n, nowUnix(), i)
				if err := s.CreateRun(ctx, run, steps); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				for _, st := range steps {
					if _, err := s.CompleteStep(ctx, st.ID, run.ID, st.Name, out, nowUnix()); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				s.Close()
			}
		})
	}
}

// BenchmarkClaimCandidates isolates the scheduler's candidate read: return up
// to limit due steps from a queue of n. Migration22's
// (queue, priority DESC, run_at, ord) index matches the ORDER BY, so the scan
// walks the index and stops at LIMIT. Without it the two-arm OR plus the
// priority sort gather every due step into a temp B-tree first. Keep this flat
// as n grows; a regression to ~n-proportional cost means the plan fell back to
// a sort.
func BenchmarkClaimCandidates(b *testing.B) {
	for _, n := range []int{500, 5000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			s, err := Open(driver.Config{Mode: driver.ModeEphemeral})
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			ctx := context.Background()
			now := nowUnix()
			runs := make([]*Run, n)
			steps := make([][]*Step, n)
			for i := 0; i < n; i++ {
				id := fmt.Sprintf("r-%d", i)
				p := int64(i % 5)
				runs[i] = &Run{ID: id, Workflow: "w", Kind: KindTask, Status: StatusQueued, Queue: "q", Priority: p, RunAt: now, CreatedAt: now, MaxAttempts: 1}
				steps[i] = []*Step{{ID: id + "/s", RunID: id, Name: "s", Task: "t", Ord: 0, Status: StatusQueued, Queue: "q", Priority: p, RunAt: now, CreatedAt: now, MaxAttempts: 1}}
			}
			if err := s.CreateRuns(ctx, runs, steps); err != nil {
				b.Fatal(err)
			}
			sql := s.claimCandidatesSQL()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, err := s.read.QueryContext(ctx, sql, "q", StatusQueued, now, StatusSuspended, now, "[]", 64)
				if err != nil {
					b.Fatal(err)
				}
				for rows.Next() {
					if _, err := scanStep(rows); err != nil {
						b.Fatal(err)
					}
				}
				rows.Close()
			}
		})
	}
}

func chainRun(n int, now int64, seq int) (*Run, []*Step) {
	runID := fmt.Sprintf("r%d", seq)
	run := &Run{ID: runID, Workflow: "w", Kind: KindWorkflow, Status: StatusQueued, Queue: "q", RunAt: now, CreatedAt: now, MaxAttempts: 1}
	steps := make([]*Step, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("s%d", i)
		status := StatusQueued
		var deps []string
		if i > 0 {
			status = StatusBlocked
			deps = []string{fmt.Sprintf("s%d", i-1)}
		}
		steps[i] = &Step{ID: runID + "/" + name, RunID: runID, Name: name, Task: "t", Ord: int64(i), Status: status, DependsOn: deps, Queue: "q", RunAt: now, CreatedAt: now, MaxAttempts: 1}
	}
	return run, steps
}

// BenchmarkCreateRunsBatch isolates the batch-insert win: one transaction for
// n runs. Divide ns/op by n for the per-run cost, and compare n=1 (the old
// one-transaction-per-run path) with n=100.
func BenchmarkCreateRunsBatch(b *testing.B) {
	for _, n := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			s, err := Open(driver.Config{Mode: driver.ModeEphemeral})
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
