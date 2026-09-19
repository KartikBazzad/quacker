package quacker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kartikbazzad/quacker/internal/engine"
)

// Backoff configures retry delays. A zero value is not "no delay": empty
// fields fall back to the library defaults (base 500ms, factor 2, cap 30s,
// no jitter) — use Exponential or Constant (or set fields) for real policies.
type Backoff = engine.Backoff

// Exponential returns a backoff that doubles from base, capped at 30s, with
// the default 10% upward jitter.
func Exponential(base time.Duration) Backoff {
	return Backoff{Base: base, Factor: 2, Max: 30 * time.Second, Jitter: 0.1}
}

// Constant returns a fixed retry delay with no jitter.
func Constant(d time.Duration) Backoff { return Backoff{Base: d, Factor: 1, Jitter: 0} }

// TaskOption configures a Task.
type TaskOption func(*taskConfig)

type taskConfig struct {
	queue       string
	maxAttempts int
	timeout     time.Duration
	backoff     Backoff
	keyFn       func(engine.Codec, json.RawMessage) string
	keyLimit    int
	uniqueFn    func(engine.Codec, json.RawMessage) string
	uniqueOn    bool
	uniqueMode  UniqueConflict
	deadLetter  bool
	sequenceFn  func(engine.Codec, json.RawMessage) string
	sequenceOn  bool
	wrap        []engine.Middleware
	labels      []string
}

// Retries sets how many times a failed attempt is retried (maxAttempts =
// retries + 1). Default: no retries.
func Retries(n int) TaskOption {
	return func(c *taskConfig) { c.maxAttempts = n + 1 }
}

// Backoff sets the retry delay policy (default: exponential from 500ms,
// capped at 30s, with 10% jitter).
func BackoffPolicy(b Backoff) TaskOption {
	return func(c *taskConfig) { c.backoff = b }
}

// Timeout bounds each attempt. A timed-out attempt is treated like any other
// failure (retried when retries remain). Default: no timeout.
func Timeout(d time.Duration) TaskOption {
	return func(c *taskConfig) { c.timeout = d }
}

// Queue routes the task to a named queue with its own concurrency limit.
// Default: "default".
func Queue(name string) TaskOption {
	return func(c *taskConfig) { c.queue = name }
}

// WithKey extracts a concurrency key from the task input. Steps sharing a
// key are capped at WithKeyConcurrency (default 1) simultaneous executions
// across all queues — the scheduler holds a due step QUEUED until its key
// has a free slot. The cap is enforced by counting RUNNING rows at claim
// time, so it survives restarts and needs no per-key bookkeeping. A task
// without WithKey is never key-gated; a key function returning "" leaves
// that run unkeyed.
func WithKey[I any](fn func(I) string) TaskOption {
	return func(c *taskConfig) {
		c.keyFn = func(codec engine.Codec, raw json.RawMessage) string {
			var in I
			if len(raw) > 0 {
				if err := codec.Unmarshal(raw, &in); err != nil {
					return "" // undecodable input leaves the run unkeyed; the task's own decode surfaces the error at execution
				}
			}
			return fn(in)
		}
	}
}

// WithKeyConcurrency sets how many runs sharing one WithKey may execute
// concurrently (default 1 = strict serialization per key). Meaningless
// without WithKey; values <1 are treated as 1.
func WithKeyConcurrency(n int) TaskOption {
	return func(c *taskConfig) { c.keyLimit = n }
}

// WithUnique makes runs of this task unique per derived key among non-terminal
// runs: while a run with the key is QUEUED/RUNNING/BLOCKED/SUSPENDED, another
// enqueue with the same key resolves by the conflict policy (default
// UniqueReuse). The key is computed from input at enqueue; returning "" leaves
// that run not unique. Uniqueness is scoped to the task name.
func WithUnique[I any](fn func(I) string) TaskOption {
	return func(c *taskConfig) {
		c.uniqueOn = true
		c.uniqueFn = func(codec engine.Codec, raw json.RawMessage) string {
			var in I
			if len(raw) > 0 {
				if err := codec.Unmarshal(raw, &in); err != nil {
					return "" // undecodable input: not unique; the task's own decode surfaces the error
				}
			}
			return fn(in)
		}
	}
}

// WithUniqueConflict sets what a unique run does when its key is already held:
// UniqueReuse (default) returns the live run's handle, UniqueError fails with
// ErrDuplicateJob, UniqueReplace cancels the live run and enqueues the new one.
func WithUniqueConflict(m UniqueConflict) TaskOption {
	return func(c *taskConfig) { c.uniqueOn = true; c.uniqueMode = m }
}

// WithDeadLetter marks a run dead-lettered when it exhausts its retries, so it
// appears in DeadLetters and can be retried with RetryDeadLetter. Runs of a
// task without it that fail are ordinary FAILED runs.
func WithDeadLetter() TaskOption {
	return func(c *taskConfig) { c.deadLetter = true }
}

// Wrap attaches per-task middleware around this task's body. Engine-wide
// middleware (WithMiddleware / Quacker.Use) still wraps the result, so the
// order is global → per-task → body.
func Wrap(mw ...Middleware) TaskOption {
	return func(c *taskConfig) { c.wrap = append(c.wrap, mw...) }
}

// WithLabels requires worker labels for this task: only an engine opened with
// WithWorkerLabels covering all of them claims its steps (labels are a
// subset — an engine with extra labels may still run it). Without a matching
// engine the runs stay QUEUED. No labels means any engine.
func WithLabels(labels ...string) TaskOption {
	return func(c *taskConfig) { c.labels = append(c.labels, labels...) }
}

// Task is a named, typed unit of work. Create with NewTask; the same value
// is used to enqueue runs, register for restart recovery, and attach crons.
type Task[I, O any] struct {
	name string
	cfg  taskConfig
	fn   func(ctx context.Context, in I) (O, error)
}

// defaultBackoff matches the documented default: exponential from 500ms,
// capped at 30s, with 10% jitter.
var defaultBackoff = Exponential(500 * time.Millisecond)

// NewTask creates a task named name. Names must be unique per process and
// are persisted with run state, so they are part of the storage format:
// renaming a task orphans its in-flight runs.
func NewTask[I, O any](name string, fn func(ctx context.Context, in I) (O, error), opts ...TaskOption) *Task[I, O] {
	cfg := taskConfig{maxAttempts: 1, backoff: defaultBackoff}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Task[I, O]{name: name, cfg: cfg, fn: fn}
}

// Name returns the task's registered name.
func (t *Task[I, O]) Name() string { return t.name }

// toDef adapts the typed task to the engine's JSON-based definition.
func (t *Task[I, O]) toDef() *engine.TaskDef {
	def := &engine.TaskDef{
		Name:        t.name,
		Queue:       t.cfg.queue,
		MaxAttempts: t.cfg.maxAttempts,
		Timeout:     t.cfg.timeout,
		Backoff:     t.cfg.backoff,
		Fn: func(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
			codec := engine.CodecFromContext(ctx)
			var in I
			if len(raw) > 0 {
				if err := codec.Unmarshal(raw, &in); err != nil {
					return nil, fmt.Errorf("quacker: decode input for task %q: %w", t.name, err)
				}
			}
			out, err := t.fn(ctx, in)
			if err != nil {
				return nil, err
			}
			return codec.Marshal(out)
		},
	}
	def.KeyFn = t.cfg.keyFn
	def.KeyLimit = t.cfg.keyLimit
	def.Wrappers = t.cfg.wrap
	def.Labels = t.cfg.labels
	def.DeadLetter = t.cfg.deadLetter
	return def
}
