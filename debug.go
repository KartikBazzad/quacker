package quacker

import (
	"log/slog"

	"github.com/kartikbazzad/quacker/internal/engine"
)

// DebugRecord is one engine debug log line, streamed via DebugLogs.
type DebugRecord = engine.DebugRecord

// DebugLogger returns a logger whose records join the engine's debug stream.
// Use it to interleave your own diagnostics with the engine's.
func (q *Quacker) DebugLogger() *slog.Logger { return q.eng.DebugLogger() }

// DebugLogs returns a bounded stream of engine debug records — claims,
// retries, suspensions, run completions, and anything logged through
// DebugLogger — so the engine can be observed without serving HTTP:
//
//	for rec := range q.DebugLogs() {
//	    fmt.Println(rec.Level, rec.Message, rec.Attrs)
//	}
//
// The stream drops records when the consumer falls behind (it never blocks
// the engine) and closes on Close, ending a range. Only one consumer should
// read it; use Subscribe for a lossless-per-subscriber transition stream.
func (q *Quacker) DebugLogs() <-chan DebugRecord { return q.eng.DebugLogs() }
