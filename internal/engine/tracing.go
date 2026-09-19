package engine

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/kartikbazzad/quacker/internal/store"
)

// tracerName is the instrumentation scope reported to the provider.
const tracerName = "github.com/kartikbazzad/quacker"

// traceContext is the W3C propagator used to (de)serialize the stored
// traceparent. It is used directly rather than the global propagator, which
// defaults to no-op and would silently drop the link.
var traceContext = propagation.TraceContext{}

// injectTraceParent serializes the span active in ctx as a W3C traceparent,
// or "" when there is none. Stored on the run so the executing step can link
// back to the producer across processes and restarts.
func injectTraceParent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	traceContext.Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// extractTraceParent rebuilds a context carrying the producer span from a
// stored traceparent (no-op for "").
func extractTraceParent(ctx context.Context, traceParent string) context.Context {
	if traceParent == "" {
		return ctx
	}
	return traceContext.Extract(ctx, propagation.MapCarrier{"traceparent": traceParent})
}

// startSpan starts a child span, or returns (ctx, nil) when tracing is off so
// the disabled path allocates nothing.
func (e *Engine) startSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if !e.tracing {
		return ctx, nil
	}
	return e.tracer.Start(ctx, name, trace.WithAttributes(attrs...))
}

// startStepSpan starts the consumer span for one step execution. Because the
// producer is long gone (the enqueue may have been another process or a
// previous boot), the step is a new root that *links* to the enqueue span
// rather than parented to it — the standard async-trace shape.
func (e *Engine) startStepSpan(ctx context.Context, st *store.Step, run *store.Run, taskName string) (context.Context, trace.Span) {
	if !e.tracing {
		return ctx, nil
	}
	opts := []trace.SpanStartOption{
		trace.WithNewRoot(),
		trace.WithAttributes(
			attribute.String("quacker.run_id", st.RunID),
			attribute.String("quacker.step", st.Name),
			attribute.String("quacker.task", taskName),
			attribute.Int("quacker.attempt", int(st.Attempts)),
			attribute.String("quacker.queue", st.Queue),
		),
	}
	if run != nil {
		if pc := trace.SpanContextFromContext(extractTraceParent(context.Background(), run.TraceParent)); pc.IsValid() {
			opts = append(opts, trace.WithLinks(trace.Link{SpanContext: pc}))
		}
	}
	return e.tracer.Start(ctx, "quacker.step", opts...)
}

// endSpan records the outcome and ends span; nil is a no-op.
func endSpan(span trace.Span, err error) {
	if span == nil {
		return
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}
	span.End()
}
