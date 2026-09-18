package engine

import (
	"time"

	"github.com/kartikbazzad/quacker/internal/store"
)

// RetentionPolicy configures the automatic purge loop. A zero OlderThan
// disables purging; a zero Interval uses one minute (clamped to >= 1s).
type RetentionPolicy struct {
	OlderThan time.Duration
	// Statuses restricts which terminal statuses may be purged; empty means
	// every terminal status.
	Statuses []string
	// KeepLogs retains the logs of purged runs (and skips the orphan sweep).
	KeepLogs bool
	Interval time.Duration
}

func (e *Engine) retentionLoop() {
	interval := e.retention.Interval
	if interval <= 0 {
		interval = time.Minute
	}
	if interval < time.Second {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-t.C:
			e.runRetention()
		}
	}
}

func (e *Engine) runRetention() {
	p := e.retention
	if p == nil || p.OlderThan <= 0 {
		return
	}
	cutoff := e.now().Add(-p.OlderThan).UnixNano()
	res, err := e.st.PurgeRuns(e.ctx, store.PurgeOptions{
		Before:   cutoff,
		Statuses: p.Statuses,
		KeepLogs: p.KeepLogs,
	})
	if err != nil {
		if e.ctx.Err() == nil {
			e.log.Error("quacker: retention purge failed", "err", err)
		}
		return
	}
	if res.Runs > 0 || res.Logs > 0 {
		e.log.Info("quacker: retention purge", "runs", res.Runs, "steps", res.Steps, "logs", res.Logs)
	}
}
