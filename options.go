package quacker

import (
	"log/slog"
	"time"

	"github.com/kartikbazzad/quacker/internal/store"
)

// Storage selects where run state lives. Construct with Memory, Ephemeral,
// or File.
type Storage struct {
	mode    store.Mode
	path    string
	recover bool
}

// Memory keeps all state in a pure in-memory SQLite database. Nothing is
// written to the filesystem and everything is lost on Close. Note: SQLite
// :memory: databases cannot use WAL journaling, so under heavy write load
// introspection reads may briefly wait for an in-flight write transaction.
func Memory() Storage { return Storage{mode: store.ModeMemory} }

// Ephemeral keeps state in a temporary WAL-backed database that is deleted
// on Close. Durability semantics are identical to Memory (nothing survives
// the process), but readers never block the write path. This is the default.
func Ephemeral() Storage { return Storage{mode: store.ModeEphemeral, recover: true} }

// File persists state to path (WAL mode) so runs survive restarts. Runs
// interrupted by a previous process are handled according to
// RecoverRunningOnBoot.
func File(path string) Storage { return Storage{mode: store.ModeFile, path: path, recover: true} }

// RecoverRunningOnBoot configures startup recovery for File storage: when
// true (default) runs left RUNNING/INTERRUPTED by a previous process are
// re-queued; when false they are marked FAILED.
func (s Storage) RecoverRunningOnBoot(requeue bool) Storage {
	s.recover = requeue
	return s
}

type config struct {
	storage            store.Config
	checkpointInterval time.Duration
	queues             map[string]int
	poll               time.Duration
	logger             *slog.Logger
}

// Option configures Open.
type Option func(*config)

// WithStorage selects the storage backend (default Ephemeral).
func WithStorage(s Storage) Option {
	return func(c *config) {
		c.storage = store.Config{Mode: s.mode, Path: s.path, RecoverRunningOnBoot: s.recover}
	}
}

// WithQueue sets a queue's maximum concurrency. Queues that are never
// configured run with a default concurrency of 10.
func WithQueue(name string, concurrency int) Option {
	return func(c *config) {
		if c.queues == nil {
			c.queues = map[string]int{}
		}
		c.queues[name] = concurrency
	}
}

// WithPollInterval sets how often the scheduler scans for due work. Enqueues
// and retries wake the scheduler immediately, so the interval only bounds
// how late a delayed run can start after its run_at passes. Default 50ms.
func WithPollInterval(d time.Duration) Option {
	return func(c *config) { c.poll = d }
}

// WithCheckpointInterval sets how often WAL-backed storage (Ephemeral,
// File) runs a passive wal_checkpoint to bound WAL growth. The checkpoint
// never blocks readers or the writer. <=0 uses the 60s default; values
// below 10ms are clamped to 10ms. Memory storage has no WAL and never
// checkpoints.
func WithCheckpointInterval(d time.Duration) Option {
	return func(c *config) { c.checkpointInterval = d }
}

// WithLogger sets the engine logger (default slog.Default()).
func WithLogger(l *slog.Logger) Option {
	return func(c *config) { c.logger = l }
}
