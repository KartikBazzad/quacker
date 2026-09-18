package quacker

import (
	"context"

	"github.com/kartikbazzad/quacker/internal/engine"
	"github.com/kartikbazzad/quacker/internal/store"
)

// WithLogSink returns a context whose TaskLogger writes each line to fn
// instead of the engine's configured sink. Outside a task function it is a
// no-op; a nil fn is ignored. Middleware uses it to route task logs to its
// own storage or forwarder.
//
// fn runs synchronously in the task's goroutine, so it must be fast — buffer
// or hand off if your destination can block.
func WithLogSink(ctx context.Context, fn func(LogEntry)) context.Context {
	if fn == nil {
		return ctx
	}
	return engine.WithLogSink(ctx, func(e store.LogEntry) {
		fn(LogEntry{RunID: e.RunID, Step: e.Step, At: unixToTime(e.At), Level: e.Level, Message: e.Message})
	})
}
