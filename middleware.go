package quacker

import "github.com/kartikbazzad/quacker/internal/engine"

// Handler is a task body at the engine's JSON level: it receives the step
// context and the JSON input and returns the JSON output. Middleware wraps
// Handlers.
type Handler = engine.Handler

// Middleware decorates task execution. Compose with WithMiddleware /
// Quacker.Use (engine-wide) or Wrap (per task). The first middleware
// registered is the outermost: global middlewares run before per-task ones,
// which run before the task body. A middleware may short-circuit by
// returning without calling next.
//
// Inside a middleware the step identity is available via StepFromContext,
// dependency outputs via DepOutput, and task logs can be redirected with
// WithLogSink:
//
//	q.Use(func(next quacker.Handler) quacker.Handler {
//	    return func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
//	        started := time.Now()
//	        ctx = quacker.WithLogSink(ctx, myLogStore)
//	        out, err := next(ctx, in)
//	        observe(quacker.RunIDFromContext(ctx), time.Since(started), err)
//	        return out, err
//	    }
//	})
type Middleware = engine.Middleware
