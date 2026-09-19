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
	// Queue, when non-empty, restricts the policy to that queue.
	Queue string
	// ExcludeDeadLettered keeps dead-lettered runs out of the policy.
	ExcludeDeadLettered bool
	// KeepLogs retains the logs of purged runs (and skips the orphan sweep).
	KeepLogs bool
	Interval time.Duration
}

func (e *Engine) retentionLoop(p *RetentionPolicy) {
	interval := p.Interval
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
			e.runRetention(p)
		}
	}
}

func (e *Engine) runRetention(p *RetentionPolicy) {
	if p == nil || p.OlderThan <= 0 {
		return
	}
	cutoff := e.now().Add(-p.OlderThan).UnixNano()
	res, err := e.st.PurgeRuns(e.ctx, store.PurgeOptions{
		Before:              cutoff,
		Statuses:            p.Statuses,
		Queue:               p.Queue,
		ExcludeDeadLettered: p.ExcludeDeadLettered,
		KeepLogs:            p.KeepLogs,
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
