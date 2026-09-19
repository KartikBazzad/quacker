package quacker

import (
	"context"
	"time"

	"github.com/kartikbazzad/quacker/internal/engine"
)

// ErrJournalMisaligned is returned when a durable task's replayed durable
// calls do not match its persisted journal — almost always because task code
// changed while runs were suspended. It fails the step loudly instead of
// resuming with wrong data.
var ErrJournalMisaligned = engine.ErrJournalMisaligned

// SleepDurable suspends the running step for at least d, durably. It does not
// consume an attempt or hold a queue or per-key slot while sleeping, and it
// survives a process restart (File storage): when the wake time passes the
// step is claimed again and the task is re-invoked from the top, at which
// point this call returns nil immediately.
//
// Because the task re-runs from the beginning after a suspension, any
// side effects before a durable await happen again on resume; wrap them in
// RunOnce to execute exactly once. Do not recover() around a durable helper.
// Only valid inside a task or workflow step.
func SleepDurable(ctx context.Context, d time.Duration) error {
	return engine.SleepDurable(ctx, d)
}

// RunOnce runs fn at most once per call site across suspensions and retries,
// memoizing its result. Use it for side effects that must not repeat when a
// durable task replays after a SleepDurable or WaitFor. fn's error is
// propagated without being memoized, so a retry re-runs fn (exactly-once on
// success, at-least-once on failure). It never suspends. Only valid inside a
// task or workflow step.
func RunOnce[T any](ctx context.Context, key string, fn func() (T, error)) (T, error) {
	return engine.RunOnce(ctx, key, fn)
}
