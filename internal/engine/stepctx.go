package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/kartikbazzad/quacker/internal/store"
)

// StepContext carries per-execution information into task functions via
// context.Context.
type StepContext struct {
	RunID   string
	Step    string
	Attempt int
}

type ctxKey struct{}

// StepFromContext returns the executing step's identity, or ok=false outside
// a task function.
func StepFromContext(ctx context.Context) (StepContext, bool) {
	ss, ok := ctx.Value(ctxKey{}).(*stepState)
	if !ok || ss == nil {
		return StepContext{}, false
	}
	return ss.StepContext, true
}

// RunIDFromContext returns the current run ID (empty outside a task).
func RunIDFromContext(ctx context.Context) string {
	if sc, ok := StepFromContext(ctx); ok {
		return sc.RunID
	}
	return ""
}

// DepOutput decodes the JSON output of a succeeded dependency step into out.
// It returns an error outside a task function, for unknown dependencies, or
// on JSON mismatch.
func DepOutput[T any](ctx context.Context, name string) (T, error) {
	var zero T
	ss, ok := ctx.Value(ctxKey{}).(*stepState)
	if !ok || ss == nil {
		return zero, fmt.Errorf("quacker: no step context: dep %q unavailable", name)
	}
	depRaw, ok := ss.depOutputs[name]
	if !ok {
		return zero, fmt.Errorf("quacker: dep %q has no recorded output", name)
	}
	var out T
	if err := json.Unmarshal(depRaw, &out); err != nil {
		return zero, fmt.Errorf("quacker: decode dep %q: %w", name, err)
	}
	return out, nil
}

// stepState is the internal context payload: identity, dependency outputs
// loaded at claim time, and this engine's log sink (engine-scoped so
// multiple engines in one process never misroute each other's logs).
type stepState struct {
	StepContext
	depOutputs map[string]json.RawMessage
	send       func(store.LogEntry)
}

func withStepContext(ctx context.Context, c *store.Claim, depOutputs map[string]json.RawMessage, send func(store.LogEntry)) context.Context {
	ss := &stepState{
		StepContext: StepContext{RunID: c.Step.RunID, Step: c.Step.Name, Attempt: int(c.Step.Attempts)},
		depOutputs:  depOutputs,
		send:        send,
	}
	return context.WithValue(ctx, ctxKey{}, ss)
}

// TaskLogger returns a logger that persists log lines to the run, visible via
// Logs(runID). Lines are buffered and written in batches; when the buffer is
// full lines are dropped rather than blocking the task.
func TaskLogger(ctx context.Context) *slog.Logger {
	ss, _ := ctx.Value(ctxKey{}).(*stepState)
	if ss == nil || ss.send == nil {
		return slog.Default()
	}
	return slog.New(&logHandler{ss: ss})
}

// logHandler writes slog records onto the owning engine's log channel.
type logHandler struct {
	ss    *stepState
	attrs []slog.Attr
}

func (h *logHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *logHandler) Handle(_ context.Context, r slog.Record) error {
	msg := r.Message
	for _, a := range h.attrs {
		msg += " " + a.Key + "=" + valueString(a.Value)
	}
	r.Attrs(func(a slog.Attr) bool {
		msg += " " + a.Key + "=" + valueString(a.Value)
		return true
	})
	h.ss.send(store.LogEntry{
		RunID: h.ss.RunID, Step: h.ss.Step,
		At: time.Now().UnixNano(), Level: r.Level.String(), Message: msg,
	})
	return nil
}

func (h *logHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	// Fresh backing slice: append into the parent's slice would let a later
	// parent append overwrite this handler's attrs.
	clone := &logHandler{ss: h.ss}
	clone.attrs = make([]slog.Attr, len(h.attrs), len(h.attrs)+len(attrs))
	copy(clone.attrs, h.attrs)
	clone.attrs = append(clone.attrs, attrs...)
	return clone
}

func (h *logHandler) WithGroup(string) slog.Handler { return h }

func valueString(v slog.Value) string {
	v = v.Resolve() // expand LogValuer values
	switch v.Kind() {
	case slog.KindString:
		return v.String()
	case slog.KindInt64:
		return strconv.FormatInt(v.Int64(), 10)
	case slog.KindDuration:
		return v.Duration().String()
	case slog.KindTime:
		return v.Time().Format(time.RFC3339)
	case slog.KindFloat64:
		return strconv.FormatFloat(v.Float64(), 'g', -1, 64)
	default:
		return fmt.Sprint(v.Any())
	}
}

// stack returns the current goroutine stack trace (panic capture).
func stack() string { return string(debug.Stack()) }
