package quacker

import (
	"context"
	"errors"
	"time"

	"github.com/kartikbazzad/quacker/internal/store"
)

// ErrNonTerminalPurge is returned when a purge is asked to delete a
// non-terminal status. Only terminal runs may ever be purged.
var ErrNonTerminalPurge = store.ErrNonTerminalPurge

// PurgeOptions selects terminal runs to delete with Quacker.Purge.
type PurgeOptions struct {
	// OlderThan is how long a terminal run must have been finished before it
	// is eligible. Required (> 0). Purging is storage-agnostic: it bounds
	// growth in every mode, including Memory.
	OlderThan time.Duration
	// Statuses restricts the purge to these terminal statuses; empty means
	// all four. A non-terminal status returns ErrNonTerminalPurge.
	Statuses []Status
	// Queue, when non-empty, restricts the purge to runs in that queue.
	Queue string
	// ExcludeDeadLettered keeps dead-lettered runs out of the purge.
	ExcludeDeadLettered bool
	// KeepLogs retains the logs of purged runs (and skips the orphan sweep).
	// The default (false) deletes them with the run.
	KeepLogs bool
	// BatchSize bounds rows deleted per transaction. <= 0 uses 500.
	BatchSize int
}

// PurgeResult reports how many rows a purge removed.
type PurgeResult = store.PurgeResult

// RetentionPolicy configures automatic purging via WithRetention.
type RetentionPolicy struct {
	// OlderThan must be > 0 for the loop to act.
	OlderThan time.Duration
	// Statuses restricts which terminal statuses may be purged; empty means
	// all of them.
	Statuses []Status
	// Queue, when non-empty, restricts the policy to runs in that queue. Use
	// several WithRetention policies (a global one plus per-queue overrides)
	// for different cutoffs per queue.
	Queue string
	// ExcludeDeadLettered keeps dead-lettered runs out of this policy.
	ExcludeDeadLettered bool
	// KeepLogs retains the logs of purged runs.
	KeepLogs bool
	// Interval is how often the purge runs; <= 0 uses one minute (min 1s).
	Interval time.Duration
}

// Purge deletes terminal runs older than opts.OlderThan, along with their
// steps and (unless KeepLogs) their logs, in batches. It is safe to call
// while work is running: a run is never eligible while any of its steps is
// still RUNNING. Use it to bound a long-lived process's growth.
func (q *Quacker) Purge(ctx context.Context, opts PurgeOptions) (PurgeResult, error) {
	if opts.OlderThan <= 0 {
		return PurgeResult{}, errors.New("quacker: Purge requires OlderThan > 0")
	}
	return q.st.PurgeRuns(ctx, store.PurgeOptions{
		Before:              time.Now().Add(-opts.OlderThan).UnixNano(),
		Statuses:            statusStrings(opts.Statuses),
		Queue:               opts.Queue,
		ExcludeDeadLettered: opts.ExcludeDeadLettered,
		KeepLogs:            opts.KeepLogs,
		BatchSize:           opts.BatchSize,
	})
}

// statusStrings converts public statuses to the store's string form; an empty
// slice stays nil so the store applies its default (all terminal statuses).
func statusStrings(ss []Status) []string {
	if len(ss) == 0 {
		return nil
	}
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = string(s)
	}
	return out
}
