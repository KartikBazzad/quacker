package quacker

import (
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/kartikbazzad/quacker/internal/engine"
	"github.com/kartikbazzad/quacker/internal/store"
)

// Storage selects where run state lives. Construct with Memory, Ephemeral,
// File, or Postgres.
type Storage struct {
	mode    store.Mode
	path    string
	dsn     string
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

// Postgres persists state to the database at dsn. It requires importing the
// driver package for its side effect:
//
//	import _ "github.com/kartikbazzad/quacker/postgres"
//
// Without that import, Open fails with a clear "backend not registered" error.
// Single-instance only for now: boot recovery and shutdown assume one engine
// owns the database; multi-instance leases are a later phase.
func Postgres(dsn string) Storage {
	return Storage{mode: store.ModePostgres, dsn: dsn, recover: true}
}

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
	rates              map[string]rateConfig
	poll               time.Duration
	logger             *slog.Logger
	middleware         []engine.Middleware
	workerLabels       []string
	workerID           string
	leaseTTL           time.Duration
	plugins            []engine.Hooks
	codec              engine.Codec
	logSink            func(LogEntry)
	logStorage         bool
	metricsFn          func(*Metrics)
	metricsInterval    time.Duration
	retention          *engine.RetentionPolicy
	tracerProvider     trace.TracerProvider
}

type rateConfig struct {
	limit  int64
	window time.Duration
}

// Option configures Open.
type Option func(*config)

// WithStorage selects the storage backend (default Ephemeral).
func WithStorage(s Storage) Option {
	return func(c *config) {
		c.storage = store.Config{Mode: s.mode, Path: s.path, DSN: s.dsn, RecoverRunningOnBoot: s.recover}
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

// WithRate caps how many runs a queue may start per window — a sliding
// window counted over claim timestamps, so the cap survives a File-mode
// restart instead of allowing a fresh-process burst. The scheduler simply
// holds due work QUEUED while the window is full; nothing is rejected.
// n<=0 disables the cap; per<=0 is treated as one second.
func WithRate(queue string, n int, per time.Duration) Option {
	return func(c *config) {
		if c.rates == nil {
			c.rates = map[string]rateConfig{}
		}
		c.rates[queue] = rateConfig{limit: int64(n), window: per}
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

// WithTracerProvider enables OpenTelemetry tracing: a span per enqueue, one
// per step execution (a new root linked to the enqueue span, the standard
// async shape), and one per emit. The library imports only the OTel API —
// pass the provider from your SDK/exporter setup, or omit this to disable
// tracing entirely (no overhead). When enabled, the W3C traceparent active at
// enqueue is persisted on the run, so execution links back across restarts.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(c *config) { c.tracerProvider = tp }
}

// WithMiddleware registers engine-wide middleware, applied outside every
// task's own Wrap middleware. The first registered is outermost.
func WithMiddleware(mw ...Middleware) Option {
	return func(c *config) { c.middleware = append(c.middleware, mw...) }
}

// WithWorkerLabels declares this engine's worker labels. It claims a task's
// steps only when their WithLabels set is a subset of these; an engine with
// no labels claims only unlabeled tasks. Use it to route work to the workers
// that can run it (e.g. "gpu", "linux").
func WithWorkerLabels(labels ...string) Option {
	return func(c *config) { c.workerLabels = append(c.workerLabels, labels...) }
}

// WithWorkerID sets this engine's identity for multi-instance step leases.
// Must be unique per running engine against a shared database; empty generates
// one. Only meaningful for leases backends (Postgres).
func WithWorkerID(id string) Option {
	return func(c *config) { c.workerID = id }
}

// WithLeaseTTL sets how long a claimed step's lease lasts before a reaper may
// requeue it (multi-instance leases only). Keep it comfortably longer than a
// scheduler stall; a running step's lease is extended by a heartbeat at a
// third of this interval. Default 30s.
func WithLeaseTTL(d time.Duration) Option {
	return func(c *config) { c.leaseTTL = d }
}

// WithTaskLogSink sets the base destination for task log lines, replacing
// the default engine-logger sink. It composes with WithLogStorage(true),
// which adds SQLite persistence in addition. A nil fn is ignored.
func WithTaskLogSink(fn func(LogEntry)) Option {
	return func(c *config) { c.logSink = fn }
}

// WithLogStorage enables the built-in SQLite task-log store (and q.Logs).
// It is off by default so a long-lived engine does not grow logs it never
// reads; when off, lines go to the base sink (engine logger unless
// WithTaskLogSink is set). When on and no custom sink is set, logs persist
// to SQLite only. Log lines are written through the same batched path as
// before.
func WithLogStorage(enabled bool) Option {
	return func(c *config) { c.logStorage = enabled }
}

// WithMetricsFunc registers a callback invoked with a fresh snapshot every
// WithMetricsInterval (default 15s). Panics in the callback are recovered
// and logged. The callback should not block for long; it runs on its own
// goroutine and never delays the scheduler.
func WithMetricsFunc(fn func(*Metrics)) Option {
	return func(c *config) { c.metricsFn = fn }
}

// WithMetricsInterval sets how often the metrics callback runs. <=0 uses the
// 15s default; values below 100ms are clamped to 100ms. Without
// WithMetricsFunc no loop is started.
func WithMetricsInterval(d time.Duration) Option {
	return func(c *config) { c.metricsInterval = d }
}

// WithRetention enables automatic purging of terminal runs older than the
// policy's OlderThan, on its own interval. Purging is storage-agnostic and
// bounds growth in every mode (Memory included). OlderThan must be > 0 for
// the loop to act; interval defaults to one minute.
func WithRetention(p RetentionPolicy) Option {
	return func(c *config) {
		rp := &engine.RetentionPolicy{
			OlderThan: p.OlderThan,
			Statuses:  statusStrings(p.Statuses),
			KeepLogs:  p.KeepLogs,
			Interval:  p.Interval,
		}
		c.retention = rp
	}
}
