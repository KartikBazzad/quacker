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
	keyStrategy ConcurrencyStrategy
	keys        []keyDef
	uniqueFn    func(engine.Codec, json.RawMessage) string
	uniqueOn    bool
	uniqueMode  UniqueConflict
	deadLetter  bool
	ephemeral   bool
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

type keyDef struct {
	name  string
	fn    func(engine.Codec, json.RawMessage) string
	limit int
}

// WithKeyLimit declares an additional named concurrency key with its own limit.
// A step is claimed only when every key it declares has a free slot, so
// declaring several gates on multiple dimensions at once. Keys are scoped by
// name and value: two tasks using the same name and equal values share one
// budget, which is how a limit is "shared" across tasks.
func WithKeyLimit[I any](name string, fn func(I) string, limit int) TaskOption {
	return func(c *taskConfig) {
		c.keys = append(c.keys, keyDef{
			name:  name,
			limit: limit,
			fn: func(codec engine.Codec, raw json.RawMessage) string {
				var in I
				if len(raw) > 0 {
					if err := codec.Unmarshal(raw, &in); err != nil {
						return "" // undecodable input leaves this key ungated
					}
				}
				return fn(in)
			},
		})
	}
}

// WithKeyStrategy selects what happens when a run is enqueued while its key is
// already at WithKeyConcurrency: ConcurrencyHold (default) queues it, while the
// cancel strategies cancel running or queued same-key runs (or the incoming
// run) instead. Meaningless without WithKey.
func WithKeyStrategy(s ConcurrencyStrategy) TaskOption {
	return func(c *taskConfig) { c.keyStrategy = s }
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

// WithSequence makes runs of this task execute strictly one-at-a-time in
// insertion order within a sequence key derived from the input, while
// different keys run in parallel. An earlier run that is still queued,
// running, retrying, or suspended holds the line (head-of-line blocking); a
// key returning "" is unsequenced. Sequences are a stronger guarantee than
// WithKey's concurrency cap, which does not fix execution order across
// retries.
func WithSequence[I any](fn func(I) string) TaskOption {
	return func(c *taskConfig) {
		c.sequenceOn = true
		c.sequenceFn = func(codec engine.Codec, raw json.RawMessage) string {
			var in I
			if len(raw) > 0 {
				if err := codec.Unmarshal(raw, &in); err != nil {
					return "" // undecodable input: unsequenced
				}
			}
			return fn(in)
		}
	}
}

// WithEphemeral makes runs of this task ephemeral: persisted while in flight
// (so claiming, retries, and introspection work) but deleted when they reach a
// terminal state and discarded on restart rather than recovered. No history
// remains; a cross-engine Result returns ErrRunGone if the run was deleted
// before it could be observed.
func WithEphemeral() TaskOption {
	return func(c *taskConfig) { c.ephemeral = true }
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
	def.KeyStrategy = t.cfg.keyStrategy
	for _, kd := range t.cfg.keys {
		def.ExtraKeys = append(def.ExtraKeys, engine.KeyDef{Name: kd.name, Fn: kd.fn, Limit: kd.limit})
	}
	def.Wrappers = t.cfg.wrap
	def.Labels = t.cfg.labels
	def.DeadLetter = t.cfg.deadLetter
	def.SequenceFn = t.cfg.sequenceFn
	def.Ephemeral = t.cfg.ephemeral
	return def
}
