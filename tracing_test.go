package quacker

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func spanByName(spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	for _, s := range spans {
		if s.Name() == name {
			return s
		}
	}
	return nil
}

// TestTracingSpans: enqueue and step spans are emitted, and the step links
// back to the enqueue span (the async-trace shape).
func TestTracingSpans(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	defer tp.Shutdown(context.Background())

	q := newTestQ(t, WithTracerProvider(tp))
	task := NewTask("traced", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	})
	h, err := Enqueue(context.Background(), q, task, greetIn{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool {
		return spanByName(sr.Ended(), "quacker.step") != nil
	})

	enq := spanByName(sr.Ended(), "quacker.enqueue")
	step := spanByName(sr.Ended(), "quacker.step")
	if enq == nil || step == nil {
		t.Fatalf("spans = enqueue:%v step:%v", enq != nil, step != nil)
	}
	if len(step.Links()) != 1 || step.Links()[0].SpanContext.TraceID() != enq.SpanContext().TraceID() {
		t.Fatalf("step links = %+v, want a link to the enqueue span", step.Links())
	}
	// The step span is a separate root (new trace? no — linked, so its own
	// trace is the step's; ensure it is not parented to enqueue).
	if step.Parent().IsValid() {
		t.Fatalf("step span should be a root with a link, got parent %v", step.Parent())
	}
	if got := attrValue(step.Attributes(), "quacker.run_id"); got != h.RunID() {
		t.Fatalf("step run_id attr = %q, want %q", got, h.RunID())
	}
	if got := attrValue(enq.Attributes(), "quacker.workflow"); got != "traced" {
		t.Fatalf("enqueue workflow attr = %q", got)
	}

	snap, _ := q.Execution(context.Background(), h.RunID())
	if snap.TraceParent == "" {
		t.Fatal("run has no persisted traceparent")
	}
}

// TestTracingEmit: emitting an event makes a span.
func TestTracingEmit(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	defer tp.Shutdown(context.Background())
	q := newTestQ(t, WithTracerProvider(tp))

	if _, err := q.Emit(context.Background(), "order.placed", greetIn{}); err != nil {
		t.Fatal(err)
	}
	em := spanByName(sr.Ended(), "quacker.emit")
	if em == nil || attrValue(em.Attributes(), "quacker.event") != "order.placed" {
		t.Fatalf("emit span = %v", em)
	}
}

// TestTracingDisabledNoTraceParent: without a provider the run carries no
// trace context (and the engine allocates no spans).
func TestTracingDisabledNoTraceParent(t *testing.T) {
	q := newTestQ(t)
	task := NewTask("untraced", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	})
	h, _ := Enqueue(context.Background(), q, task, greetIn{})
	if _, err := h.Result(context.Background()); err != nil {
		t.Fatal(err)
	}
	snap, _ := q.Execution(context.Background(), h.RunID())
	if snap.TraceParent != "" {
		t.Fatalf("traceparent = %q, want empty without a provider", snap.TraceParent)
	}
}

// TestTracingSurvivesRestart: the traceparent stored at enqueue is used to
// link the step span after a process restart.
func TestTracingSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trace.db")

	sr1 := tracetest.NewSpanRecorder()
	tp1 := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr1))
	q1, err := Open(WithStorage(File(path)), WithPollInterval(5*time.Millisecond), WithTracerProvider(tp1))
	if err != nil {
		t.Fatal(err)
	}
	task := NewTask("tr-restart", func(ctx context.Context, in greetIn) (greetOut, error) {
		if err := SleepDurable(ctx, 150*time.Millisecond); err != nil {
			return greetOut{}, err
		}
		return greetOut{}, nil
	})
	h, err := Enqueue(context.Background(), q1, task, greetIn{})
	if err != nil {
		t.Fatal(err)
	}
	waitSuspended(t, q1, h.RunID())
	snap, _ := q1.Execution(context.Background(), h.RunID())
	traceParent := snap.TraceParent
	if traceParent == "" {
		t.Fatal("no traceparent stored at enqueue")
	}
	closeQ(t, q1)
	tp1.Shutdown(context.Background())

	sr2 := tracetest.NewSpanRecorder()
	tp2 := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr2))
	defer tp2.Shutdown(context.Background())
	q2, err := Open(WithStorage(File(path)), WithPollInterval(5*time.Millisecond), WithTracerProvider(tp2))
	if err != nil {
		t.Fatal(err)
	}
	defer closeQ(t, q2)
	task2 := NewTask("tr-restart", func(ctx context.Context, in greetIn) (greetOut, error) {
		if err := SleepDurable(ctx, 150*time.Millisecond); err != nil {
			return greetOut{}, err
		}
		return greetOut{}, nil
	})
	Register(q2, task2)
	waitFor(t, 3*time.Second, func() bool {
		s, err := q2.Execution(context.Background(), h.RunID())
		return err == nil && s.Status == StatusSucceeded
	})

	step := spanByName(sr2.Ended(), "quacker.step")
	if step == nil || len(step.Links()) != 1 {
		t.Fatalf("step span links = %v, want one", step)
	}
	producerCtx := propagation.TraceContext{}.Extract(context.Background(),
		propagation.MapCarrier{"traceparent": traceParent})
	if want := trace.SpanContextFromContext(producerCtx).TraceID(); step.Links()[0].SpanContext.TraceID() != want {
		t.Fatalf("linked trace %s, want %s", step.Links()[0].SpanContext.TraceID(), want)
	}
}

func attrValue(attrs []attribute.KeyValue, key string) string {
	for _, a := range attrs {
		if string(a.Key) == key {
			return a.Value.AsString()
		}
	}
	return ""
}
