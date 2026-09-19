// Package quacker is an embeddable task and workflow orchestration engine —
// a single-process alternative to Hatchet. State lives in in-memory SQLite
// (or a file, if you want durability), tasks are plain Go functions, and
// execution state is readable at any moment without pausing execution:
//
//	q := quacker.Open()  // ephemeral in-memory state
//	task := quacker.NewTask("greet", func(ctx context.Context, in Input) (string, error) {
//	    return "hi " + in.Name, nil
//	})
//	h, _ := quacker.Enqueue(ctx, q, task, Input{Name: "world"})
//	out, _ := h.Result(ctx)
//	snap, _ := q.Execution(ctx, h.RunID()) // QUEUED/RUNNING/... at any instant
package quacker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/kartikbazzad/quacker/internal/engine"
	"github.com/kartikbazzad/quacker/internal/store"
)

// Quacker is an embedded orchestration engine. Create with Open; safe for
// concurrent use.
type Quacker struct {
	st  *store.Store
	eng *engine.Engine
	log *slog.Logger
}

// Open starts an engine. State is stored per WithStorage (default
// Ephemeral: a temp-file WAL database deleted on Close).
func Open(opts ...Option) (*Quacker, error) {
	cfg := config{storage: store.Config{Mode: store.ModeEphemeral, RecoverRunningOnBoot: true}}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.logger == nil {
		cfg.logger = slog.Default()
	}
	cfg.storage.CheckpointInterval = cfg.checkpointInterval
	st, err := store.Open(cfg.storage)
	if err != nil {
		return nil, err
	}
	eopts := engine.Options{
		Store: st, Log: cfg.logger, PollInterval: cfg.poll,
		Middleware:      cfg.middleware,
		WorkerLabels:    cfg.workerLabels,
		LogStorage:      cfg.logStorage,
		MetricsInterval: cfg.metricsInterval,
		Retention:       cfg.retention,
		TracerProvider:  cfg.tracerProvider,
		WorkerID:        cfg.workerID,
		LeaseTTL:        cfg.leaseTTL,
	}
	if cfg.logSink != nil {
		fn := cfg.logSink
		eopts.LogSink = func(e store.LogEntry) {
			fn(LogEntry{RunID: e.RunID, Step: e.Step, At: unixToTime(e.At), Level: e.Level, Message: e.Message})
		}
	}
	if cfg.metricsFn != nil {
		fn := cfg.metricsFn
		eopts.OnMetrics = func(s engine.MetricsSnapshot) { fn(metricsFromSnapshot(s)) }
	}
	eng, err := engine.New(eopts)
	if err != nil {
		st.Close()
		return nil, err
	}
	for name, conc := range cfg.queues {
		eng.SetQueue(name, conc)
	}
	for name, rc := range cfg.rates {
		eng.SetRateLimit(name, rc.limit, rc.window)
	}
	eng.Start()
	return &Quacker{st: st, eng: eng, log: cfg.logger}, nil
}

// Close shuts down gracefully: no new work is claimed, in-flight tasks drain
// until ctx expires (30s if ctx has no deadline), leftovers are marked
// INTERRUPTED, and pending Results return ErrRunInterrupted. Ephemeral and
// Memory storage are deleted; File storage is kept for the next Open.
func (q *Quacker) Close(ctx context.Context) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}
	err := q.eng.Close(ctx)
	if serr := q.st.Close(); serr != nil && err == nil {
		err = serr
	}
	return err
}

// Cancel cancels a run. Queued/blocked steps become CANCELLED immediately;
// running steps have their contexts cancelled. Waiters receive
// ErrRunCancelled. Cancelling an unknown or finished run is a no-op.
func (q *Quacker) Cancel(runID string) error { return q.eng.Cancel(runID) }

// SetQueue adjusts a queue's concurrency at runtime.
func (q *Quacker) SetQueue(name string, concurrency int) { q.eng.SetQueue(name, concurrency) }

// Use appends engine-wide middleware at runtime; safe before or after work
// starts. The first registered middleware is the outermost wrapper.
func (q *Quacker) Use(mw ...Middleware) { q.eng.Use(mw...) }

// SetRateLimit adjusts a queue's start-rate cap at runtime — at most n runs
// started per sliding window. n<=0 disables the cap; window<=0 is one second.
func (q *Quacker) SetRateLimit(name string, n int, window time.Duration) {
	q.eng.SetRateLimit(name, int64(n), window)
}

// Crons lists registered cron triggers.
func (q *Quacker) Crons() []CronInfo {
	infos := q.eng.Crons()
	out := make([]CronInfo, 0, len(infos))
	for _, c := range infos {
		out = append(out, CronInfo{Name: c.Name, Spec: c.Spec, Task: c.Task, Next: c.Next})
	}
	return out
}

// RemoveCron deletes a cron trigger.
func (q *Quacker) RemoveCron(name string) error { return q.eng.RemoveCron(name) }

// EnqueueOption configures a single enqueued run.
type EnqueueOption func(*enqueueConfig)

type enqueueConfig struct {
	runAt    time.Time
	priority int64
}

// WithDelay schedules the run to start at least d from now.
func WithDelay(d time.Duration) EnqueueOption {
	return func(c *enqueueConfig) { c.runAt = time.Now().Add(d) }
}

// WithRunAt schedules the run to start no earlier than t.
func WithRunAt(t time.Time) EnqueueOption {
	return func(c *enqueueConfig) { c.runAt = t }
}

// WithPriority orders the run ahead of lower-priority work in the same
// queue. Higher numbers run first.
func WithPriority(p int64) EnqueueOption {
	return func(c *enqueueConfig) { c.priority = p }
}

// Enqueue enqueues one run of task with input.
func Enqueue[I any, O any](ctx context.Context, q *Quacker, t *Task[I, O], input I, opts ...EnqueueOption) (*RunHandle[O], error) {
	return enqueueTask(ctx, q, t, input, "", opts...)
}

// EnqueueBatch enqueues one run of task per input in a single write
// transaction, returning handles in input order. It is all-or-nothing: a
// validation or insert failure creates no runs. Use it to fan out many runs
// cheaply instead of calling Enqueue in a loop.
func EnqueueBatch[I any, O any](ctx context.Context, q *Quacker, t *Task[I, O], inputs []I, opts ...EnqueueOption) ([]*RunHandle[O], error) {
	ec := enqueueConfig{}
	for _, opt := range opts {
		opt(&ec)
	}
	reqs := make([]*engine.EnqueueRequest, len(inputs))
	for i, in := range inputs {
		b, err := json.Marshal(in)
		if err != nil {
			return nil, err
		}
		reqs[i] = &engine.EnqueueRequest{
			Workflow: t.name,
			Kind:     store.KindTask,
			Input:    b,
			Priority: ec.priority,
			RunAt:    ec.runAt,
			Steps:    []engine.StepReq{{Name: t.name, Def: t.toDef()}},
		}
	}
	ws, err := q.eng.EnqueueBatch(ctx, reqs)
	if err != nil {
		return nil, err
	}
	out := make([]*RunHandle[O], len(ws))
	for i, w := range ws {
		out[i] = &RunHandle[O]{runID: w.RunID, w: w}
	}
	return out, nil
}

// EnqueueChild enqueues one run of task as a child of the run currently
// executing (from RunIDFromContext). It is only valid inside a task or
// workflow step; the child runs independently (no implicit join), and
// Execution exposes it under Children.
func EnqueueChild[I any, O any](ctx context.Context, q *Quacker, t *Task[I, O], input I, opts ...EnqueueOption) (*RunHandle[O], error) {
	parent := RunIDFromContext(ctx)
	if parent == "" {
		return nil, errors.New("quacker: EnqueueChild requires a task context")
	}
	return enqueueTask(ctx, q, t, input, parent, opts...)
}

func enqueueTask[I any, O any](ctx context.Context, q *Quacker, t *Task[I, O], input I, parent string, opts ...EnqueueOption) (*RunHandle[O], error) {
	ec := enqueueConfig{}
	for _, opt := range opts {
		opt(&ec)
	}
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	w, err := q.eng.Enqueue(ctx, &engine.EnqueueRequest{
		Workflow: t.name,
		Kind:     store.KindTask,
		Input:    inputJSON,
		Priority: ec.priority,
		RunAt:    ec.runAt,
		ParentID: parent,
		Steps:    []engine.StepReq{{Name: t.name, Def: t.toDef()}},
	})
	if err != nil {
		return nil, err
	}
	return &RunHandle[O]{runID: w.RunID, w: w}, nil
}

// EnqueueWorkflow enqueues a workflow run. All steps receive input; the
// handle's Result decodes the output of the last declared step, so callers
// specify O explicitly while I is inferred from the workflow:
//
//	h, err := quacker.EnqueueWorkflow[Shipment](ctx, q, wf, order)
func EnqueueWorkflow[O any, I any](ctx context.Context, q *Quacker, wf *Workflow[I], input I, opts ...EnqueueOption) (*RunHandle[O], error) {
	return enqueueWorkflow[O](ctx, q, wf, input, "", opts...)
}

// EnqueueWorkflowChild enqueues a workflow run as a child of the currently
// executing run. Only valid inside a task or workflow step.
func EnqueueWorkflowChild[O any, I any](ctx context.Context, q *Quacker, wf *Workflow[I], input I, opts ...EnqueueOption) (*RunHandle[O], error) {
	parent := RunIDFromContext(ctx)
	if parent == "" {
		return nil, errors.New("quacker: EnqueueWorkflowChild requires a task context")
	}
	return enqueueWorkflow[O](ctx, q, wf, input, parent, opts...)
}

func enqueueWorkflow[O any, I any](ctx context.Context, q *Quacker, wf *Workflow[I], input I, parent string, opts ...EnqueueOption) (*RunHandle[O], error) {
	ec := enqueueConfig{}
	for _, opt := range opts {
		opt(&ec)
	}
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	steps := make([]engine.StepReq, len(wf.steps))
	for i, s := range wf.steps {
		steps[i] = engine.StepReq{Name: s.name, Deps: s.deps, Def: s.def}
	}
	w, err := q.eng.Enqueue(ctx, &engine.EnqueueRequest{
		Workflow: wf.name,
		Kind:     store.KindWorkflow,
		Input:    inputJSON,
		Priority: ec.priority,
		RunAt:    ec.runAt,
		ParentID: parent,
		Steps:    steps,
	})
	if err != nil {
		return nil, err
	}
	return &RunHandle[O]{runID: w.RunID, w: w}, nil
}

// Register makes a task executable without enqueuing a run. Needed when
// using File storage: register tasks at startup so runs recovered from the
// previous process can execute. Registration also happens automatically on
// Enqueue and Cron.
func Register[I, O any](q *Quacker, t *Task[I, O]) {
	q.eng.RegisterTask(t.toDef())
}

// Cron registers a trigger that enqueues task with input on the cron spec
// (standard 5-field syntax plus descriptors like "@hourly" and "@every 5m").
// Crons are in-memory: re-register at startup, ideally before Open resumes
// work.
func Cron[I any, O any](q *Quacker, name, spec string, t *Task[I, O], input I) error {
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return err
	}
	return q.eng.RegisterCron(name, spec, t.toDef(), inputJSON)
}

// metricsFromSnapshot converts the engine's string-keyed snapshot into the
// public Metrics shape used by the WithMetricsFunc callback.
func metricsFromSnapshot(s engine.MetricsSnapshot) *Metrics {
	m := &Metrics{Runs: make(map[Status]int64, len(s.Runs)), Queues: make(map[string]QueueStats, len(s.Queues))}
	for st, n := range s.Runs {
		m.Runs[Status(st)] = n
	}
	for name, qs := range s.Queues {
		m.Queues[name] = QueueStats{Queued: qs.Queued, Running: qs.Running, Blocked: qs.Blocked, Suspended: qs.Suspended}
	}
	return m
}
