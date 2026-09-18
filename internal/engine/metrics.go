package engine

import (
	"fmt"
	"time"

	"github.com/kartikbazzad/quacker/internal/store"
)

// MetricsSnapshot is a point-in-time engine-wide state snapshot handed to the
// OnMetrics callback: run counts keyed by status and per-queue depths.
type MetricsSnapshot struct {
	Runs   map[string]int64
	Queues map[string]store.QueueStats
}

// metricsLoop periodically snapshots the store and invokes the callback. It
// is joined via loopWG before Close closes the store.
func (e *Engine) metricsLoop() {
	t := time.NewTicker(e.metricsInterval)
	defer t.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-t.C:
			e.emitMetrics()
		}
	}
}

func (e *Engine) emitMetrics() {
	byStatus, queues, err := e.st.Metrics(e.ctx)
	if err != nil {
		if e.ctx.Err() == nil {
			e.log.Error("quacker: metrics snapshot failed", "err", err)
		}
		return
	}
	snap := MetricsSnapshot{Runs: byStatus, Queues: make(map[string]store.QueueStats, len(queues))}
	for name, qs := range queues {
		snap.Queues[name] = *qs
	}
	// A panicking callback must never take down the loop or the engine.
	func() {
		defer func() {
			if r := recover(); r != nil {
				e.log.Error("quacker: metrics callback panicked", "panic", fmt.Sprint(r))
			}
		}()
		e.onMetrics(snap)
	}()
}
