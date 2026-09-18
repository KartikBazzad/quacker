package quacker

import (
	"context"
	"log/slog"

	"github.com/kartikbazzad/quacker/internal/engine"
)

// StepContext identifies the currently executing step inside a task function.
type StepContext = engine.StepContext

// StepFromContext returns the executing step's identity inside a task
// function (ok=false otherwise).
func StepFromContext(ctx context.Context) (StepContext, bool) {
	return engine.StepFromContext(ctx)
}

// RunIDFromContext returns the current run's ID inside a task function
// (empty otherwise).
func RunIDFromContext(ctx context.Context) string {
	return engine.RunIDFromContext(ctx)
}

// DepOutput decodes the recorded output of a succeeded dependency step. Only
// usable inside workflow steps that declared the named step as a dependency
// via Step(name, task, "dependency-name").
func DepOutput[T any](ctx context.Context, name string) (T, error) {
	return engine.DepOutput[T](ctx, name)
}

// TaskLogger returns a logger whose lines are persisted with the run and
// readable via Logs. Outside a task function it returns slog's default.
func TaskLogger(ctx context.Context) *slog.Logger {
	return engine.TaskLogger(ctx)
}
