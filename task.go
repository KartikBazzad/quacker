package quacker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kartikbazzad/quacker/internal/engine"
)

// Backoff configures retry delays. The zero value means "no delay, no
// jitter" — use Exponential or Constant (or set fields) for real policies.
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
	return &engine.TaskDef{
		Name:        t.name,
		Queue:       t.cfg.queue,
		MaxAttempts: t.cfg.maxAttempts,
		Timeout:     t.cfg.timeout,
		Backoff:     t.cfg.backoff,
		Fn: func(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
			var in I
			if len(raw) > 0 && string(raw) != "null" {
				if err := json.Unmarshal(raw, &in); err != nil {
					return nil, fmt.Errorf("quacker: decode input for task %q: %w", t.name, err)
				}
			}
			out, err := t.fn(ctx, in)
			if err != nil {
				return nil, err
			}
			return json.Marshal(out)
		},
	}
}
