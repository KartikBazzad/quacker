package engine

import (
	"context"
	"log/slog"
	"time"
)

// debugBuffer bounds the engine debug stream. Records are dropped when the
// buffer is full and nobody is consuming, so a forgotten stream cannot grow
// memory.
const debugBuffer = 1024

// DebugRecord is one engine debug log line, streamed to consumers via
// DebugLogs rather than served over HTTP. Attrs is flattened to strings.
type DebugRecord struct {
	At      time.Time         `json:"at"`
	Level   string            `json:"level"`
	Message string            `json:"message"`
	Attrs   map[string]string `json:"attrs,omitempty"`
}

// DebugLogger returns a logger whose records go to this engine's debug stream.
// Use it to interleave your own diagnostics with the engine's.
func (e *Engine) DebugLogger() *slog.Logger {
	return slog.New(&debugHandler{send: e.debugSend})
}

// DebugLogs returns the engine's debug stream. It is bounded and drops records
// when the consumer falls behind; it closes on Close, ending a range. Only one
// consumer should read it.
func (e *Engine) DebugLogs() <-chan DebugRecord { return e.debugCh }

func (e *Engine) debugSend(r DebugRecord) {
	e.debugMu.Lock()
	defer e.debugMu.Unlock()
	if e.debugClosed {
		return
	}
	if r.At.IsZero() {
		r.At = e.now()
	}
	select {
	case e.debugCh <- r:
	default: // slow/absent consumer: drop
	}
}

// debugHandler writes slog records to the engine's debug stream.
type debugHandler struct {
	send  func(DebugRecord)
	attrs []slog.Attr
}

func (h *debugHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *debugHandler) Handle(_ context.Context, r slog.Record) error {
	if h.send == nil {
		return nil
	}
	attrs := make(map[string]string, len(h.attrs)+r.NumAttrs())
	for _, a := range h.attrs {
		attrs[a.Key] = valueString(a.Value)
	}
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = valueString(a.Value)
		return true
	})
	h.send(DebugRecord{At: r.Time, Level: r.Level.String(), Message: r.Message, Attrs: attrs})
	return nil
}

func (h *debugHandler) WithAttrs(as []slog.Attr) slog.Handler {
	clone := &debugHandler{send: h.send}
	clone.attrs = make([]slog.Attr, len(h.attrs), len(h.attrs)+len(as))
	copy(clone.attrs, h.attrs)
	clone.attrs = append(clone.attrs, as...)
	return clone
}

func (h *debugHandler) WithGroup(string) slog.Handler { return h }

// fanoutHandler sends a record to two handlers; the engine logger is wrapped
// with one so its diagnostics reach both the user's logger and the debug
// stream.
type fanoutHandler struct{ a, b slog.Handler }

func (f *fanoutHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return f.a.Enabled(ctx, l) || f.b.Enabled(ctx, l)
}

func (f *fanoutHandler) Handle(ctx context.Context, r slog.Record) error {
	if f.a.Enabled(ctx, r.Level) {
		_ = f.a.Handle(ctx, r)
	}
	if f.b.Enabled(ctx, r.Level) {
		_ = f.b.Handle(ctx, r)
	}
	return nil
}

func (f *fanoutHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &fanoutHandler{a: f.a.WithAttrs(attrs), b: f.b.WithAttrs(attrs)}
}

func (f *fanoutHandler) WithGroup(name string) slog.Handler {
	return &fanoutHandler{a: f.a.WithGroup(name), b: f.b.WithGroup(name)}
}
