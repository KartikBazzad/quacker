// Package engine is quacker's execution core: a scheduler that claims due
// steps from SQLite, per-queue worker pools, retries, DAG progression, cron
// firing, and graceful shutdown.
package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kartikbazzad/quacker/internal/bus"
	"github.com/kartikbazzad/quacker/internal/store"
)

var (
	// ErrRunCancelled is returned to callers waiting on a cancelled run.
	ErrRunCancelled = errors.New("quacker: run cancelled")
	// ErrRunInterrupted is returned when a run was dropped by shutdown.
	ErrRunInterrupted = errors.New("quacker: run interrupted by shutdown")
	// ErrClosed is returned by enqueue calls after Close.
	ErrClosed = errors.New("quacker: engine is closed")
	// ErrUnknownTask is returned by RegisterCron for an unregistered task.
	ErrUnknownTask = errors.New("quacker: unknown task")
)

// DefaultQueueConcurrency is the concurrency assigned to queues that were
// never configured via SetQueue.
const DefaultQueueConcurrency = 10

// TaskDef is the engine-level, non-generic description of a task.
type TaskDef struct {
	Name        string
	Queue       string
	MaxAttempts int
	Timeout     time.Duration
	Backoff     Backoff
	Fn          func(ctx context.Context, input json.RawMessage) (json.RawMessage, error)
}

// StepReq is one step of an enqueue request.
type StepReq struct {
	Name string
	Deps []string
	Def  *TaskDef
}

// EnqueueRequest creates a run. For a single task there is exactly one step.
type EnqueueRequest struct {
	Workflow string
	Kind     string // store.KindTask or store.KindWorkflow
	Input    json.RawMessage
	Queue    string
	Priority int64
	RunAt    time.Time // zero = now
	Steps    []StepReq
}

// CronInfo describes a registered cron trigger.
type CronInfo struct {
	Name string
	Spec string
	Task string
	Next time.Time
}

type queueState struct {
	// concurrency is atomic: tick reads it from snapshot pointers outside
	// e.mu while SetQueue may write concurrently.
	concurrency atomic.Int64
	inFlight    atomic.Int64
}

type cronEntry struct {
	spec  string
	task  string
	sched cronSchedule
	input json.RawMessage
	next  time.Time
}

// Engine executes runs against a store.
type Engine struct {
	st   *store.Store
	bus  *bus.Bus
	log  *slog.Logger
	poll time.Duration
	now  func() time.Time

	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.RWMutex
	queues  map[string]*queueState
	tasks   map[string]*TaskDef
	crons   map[string]*cronEntry
	waiters map[string]*Waiter
	cancels map[string]context.CancelFunc
	// haltSteps marks steps that were cancelled or orphaned by a failed run
	// before their executor registered a cancel func — the executor checks
	// (and clears) its tombstone right after registering, closing the
	// claim/cancel race window.
	haltSteps map[string]struct{}

	wg      sync.WaitGroup // in-flight step executions
	wake    chan struct{}
	closing atomic.Bool

	logCh       chan store.LogEntry
	logWG       sync.WaitGroup
	logMu       sync.Mutex
	logClosed   bool
	droppedLogs atomic.Int64

	unknownWarned sync.Map // task name -> warned once
}

// Options configures New.
type Options struct {
	Store *store.Store
	Bus   *bus.Bus
	Log   *slog.Logger
	// PollInterval is how often the scheduler re-checks for due work.
	// Enqueues and retries wake the scheduler immediately regardless.
	PollInterval time.Duration
	// Now overrides the clock (tests).
	Now func() time.Time
}

// New returns an engine. Call Start to begin scheduling.
func New(o Options) (*Engine, error) {
	if o.Store == nil {
		return nil, errors.New("quacker: engine requires a store")
	}
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if o.PollInterval <= 0 {
		o.PollInterval = 50 * time.Millisecond
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Bus == nil {
		o.Bus = bus.New()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Engine{
		st:        o.Store,
		bus:       o.Bus,
		log:       o.Log,
		poll:      o.PollInterval,
		now:       o.Now,
		ctx:       ctx,
		cancel:    cancel,
		queues:    map[string]*queueState{},
		tasks:     map[string]*TaskDef{},
		crons:     map[string]*cronEntry{},
		waiters:   map[string]*Waiter{},
		cancels:   map[string]context.CancelFunc{},
		haltSteps: map[string]struct{}{},
		wake:      make(chan struct{}, 1),
		logCh:     make(chan store.LogEntry, 4096),
	}, nil
}

// Start launches the scheduler, cron, and log-flush loops. Queues that exist
// in the store (e.g. recovered runs after a restart) are registered with
// default concurrency unless already configured.
func (e *Engine) Start() {
	if queues, err := e.st.KnownQueues(context.Background()); err == nil {
		for _, q := range queues {
			e.ensureQueue(q)
		}
	}
	e.logWG.Add(1)
	go e.flushLogs()
	go e.schedulerLoop()
	go e.cronLoop()
}

// Close stops the engine: no new work is claimed, in-flight executions drain
// until ctx expires, leftovers are marked INTERRUPTED, and pending waiters
// are released with ErrRunInterrupted.
func (e *Engine) Close(ctx context.Context) error {
	if !e.closing.Swap(true) {
		e.cancel()
	}
	done := make(chan struct{})
	go func() { e.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
	now := e.now().UnixNano()
	// Steps still RUNNING (drain timeout or forced close) become INTERRUPTED.
	_ = e.interruptAll(now)
	e.mu.Lock()
	ws := make([]*Waiter, 0, len(e.waiters))
	for runID, w := range e.waiters {
		ws = append(ws, w)
		delete(e.waiters, runID)
	}
	e.mu.Unlock()
	for _, w := range ws {
		w.finish(store.StatusInterrupted, nil, ErrRunInterrupted)
	}
	// Seal the log channel before closing it: logSend checks logClosed under
	// the same mutex, so a task that outlived the drain can never send on a
	// closed channel.
	e.logMu.Lock()
	e.logClosed = true
	e.logMu.Unlock()
	close(e.logCh)
	e.logWG.Wait()
	return ctx.Err()
}

// SetQueue configures a queue's concurrency, creating it if needed.
func (e *Engine) SetQueue(name string, concurrency int) {
	if concurrency <= 0 {
		concurrency = DefaultQueueConcurrency
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if q := e.queues[name]; q != nil {
		q.concurrency.Store(int64(concurrency))
		return
	}
	e.queues[name] = newQueueState(concurrency)
}

// ensureQueue creates a queue with default concurrency if it doesn't exist;
// it never overrides an existing configuration.
func (e *Engine) ensureQueue(name string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.queues[name]; !ok {
		e.queues[name] = newQueueState(DefaultQueueConcurrency)
	}
}

func newQueueState(concurrency int) *queueState {
	qs := &queueState{}
	qs.concurrency.Store(int64(concurrency))
	return qs
}

func normQueue(q string) string {
	if q == "" {
		return "default"
	}
	return q
}

// RegisterTask makes a task executable by name. Registration is idempotent;
// it overwrites any previous definition. The def is stored as-is (never
// mutated) so concurrent enqueues of the same task are race-free.
func (e *Engine) RegisterTask(def *TaskDef) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.tasks[def.Name] = def
}

// ---------------------------------------------------------------------------
// Enqueue + waiting.

// Waiter tracks a run created by this process until it reaches a terminal
// status. Safe for concurrent use.
type Waiter struct {
	RunID string

	mu     sync.Mutex
	ch     chan struct{}
	done   bool
	status string
	output json.RawMessage
	err    error
}

func newWaiter(runID string) *Waiter {
	return &Waiter{RunID: runID, ch: make(chan struct{})}
}

func (w *Waiter) finish(status string, output json.RawMessage, err error) {
	w.mu.Lock()
	if w.done {
		w.mu.Unlock()
		return
	}
	w.done = true
	w.status, w.output, w.err = status, output, err
	close(w.ch)
	w.mu.Unlock()
}

// Wait blocks until the run finishes or ctx expires.
func (w *Waiter) Wait(ctx context.Context) (status string, output json.RawMessage, err error) {
	select {
	case <-w.ch:
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.status, w.output, w.err
	case <-ctx.Done():
		return "", nil, ctx.Err()
	}
}

// Done returns a channel closed when the run reaches a terminal status.
func (w *Waiter) Done() <-chan struct{} { return w.ch }

// Enqueue validates req, persists the run and its steps, and returns a
// waiter for the run. The first wake-up signal is sent to the scheduler.
// The caller's ctx governs the insert: a cancelled ctx means no run.
func (e *Engine) Enqueue(ctx context.Context, req *EnqueueRequest) (*Waiter, error) {
	if ctx == nil {
		ctx = e.ctx
	}
	if len(req.Steps) == 0 {
		return nil, errors.New("quacker: enqueue requires at least one step")
	}
	queue := normQueue(req.Queue)
	if queue == "default" && len(req.Steps) > 0 && req.Steps[0].Def != nil {
		queue = normQueue(req.Steps[0].Def.Queue)
	}
	e.ensureQueue(queue)
	if err := validateDAG(req.Steps); err != nil {
		return nil, err
	}
	now := e.now()
	runAt := req.RunAt
	if runAt.IsZero() {
		runAt = now
	}
	runID := newID(now)

	run := &store.Run{
		ID: runID, Workflow: req.Workflow, Kind: req.Kind, Status: store.StatusQueued,
		Queue: queue, Priority: req.Priority, Input: req.Input,
		MaxAttempts: 1, RunAt: runAt.UnixNano(), CreatedAt: now.UnixNano(),
	}
	var steps []*store.Step
	for i, sr := range req.Steps {
		def := sr.Def
		if def == nil {
			return nil, fmt.Errorf("quacker: step %q has no task definition", sr.Name)
		}
		e.mu.Lock()
		e.tasks[def.Name] = def // latest definition wins, like RegisterTask
		e.mu.Unlock()
		stepQueue := normQueue(def.Queue)
		e.ensureQueue(stepQueue)
		maxAtt := def.MaxAttempts
		if maxAtt < 1 {
			maxAtt = 1
		}
		status := store.StatusQueued
		if len(sr.Deps) > 0 {
			status = store.StatusBlocked
		}
		steps = append(steps, &store.Step{
			ID: runID + "/" + sr.Name, RunID: runID, Name: sr.Name, Task: def.Name, Ord: int64(i),
			Status: status, DependsOn: sr.Deps, Queue: stepQueue, Priority: req.Priority,
			Input: req.Input, MaxAttempts: int64(maxAtt), Timeout: def.Timeout,
			RunAt: runAt.UnixNano(), CreatedAt: now.UnixNano(),
		})
	}
	// run-level max attempts mirrors the first step for introspection.
	run.MaxAttempts = steps[0].MaxAttempts

	// Registration and the closing check happen under one lock so an enqueue
	// racing Close either lands in the sweep or is rejected — never stranded
	// as a waiter nobody will ever release.
	w := newWaiter(runID)
	e.mu.Lock()
	if e.closing.Load() {
		e.mu.Unlock()
		return nil, ErrClosed
	}
	e.waiters[runID] = w
	e.mu.Unlock()

	if err := e.st.CreateRun(ctx, run, steps); err != nil {
		e.mu.Lock()
		delete(e.waiters, runID)
		e.mu.Unlock()
		return nil, err
	}
	e.wakeScheduler()
	return w, nil
}

// Cancel cancels a run. Queued/blocked steps are CANCELLED immediately and
// running steps have their contexts cancelled. Waiting callers receive
// ErrRunCancelled. Cancelling an unknown or already-finished run is a no-op.
func (e *Engine) Cancel(runID string) error {
	now := e.now().UnixNano()
	running, prevStatus, ok, err := e.st.CancelRun(e.ctx, runID, now)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil || !ok {
		return err
	}
	e.mu.Lock()
	var cancels []context.CancelFunc
	for _, stepID := range running {
		if c := e.cancels[stepID]; c != nil {
			cancels = append(cancels, c)
			continue
		}
		// Claimed but its executor hasn't registered a cancel func yet: leave
		// a tombstone so the executor cancels itself before running the task.
		e.haltSteps[stepID] = struct{}{}
	}
	w := e.waiters[runID]
	delete(e.waiters, runID)
	e.mu.Unlock()
	for _, c := range cancels {
		c()
	}
	if w != nil {
		w.finish(store.StatusCancelled, nil, ErrRunCancelled)
	}
	e.publish(runID, "", prevStatus, store.StatusCancelled, "", now)
	return nil
}

// ---------------------------------------------------------------------------
// Scheduler.

func (e *Engine) wakeScheduler() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

func (e *Engine) schedulerLoop() {
	ticker := time.NewTicker(e.poll)
	defer ticker.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
		case <-e.wake:
		}
		e.tick()
	}
}

func (e *Engine) tick() {
	e.mu.Lock()
	snapshot := make(map[string]*queueState, len(e.queues))
	for q, qs := range e.queues {
		snapshot[q] = qs
	}
	e.mu.Unlock()

	now := e.now().UnixNano()
	runStarted := map[string]bool{} // one run-level QUEUED→RUNNING per run per tick
	for q, qs := range snapshot {
		for {
			slots := int(qs.concurrency.Load()) - int(qs.inFlight.Load())
			if slots <= 0 {
				break
			}
			claims, err := e.st.ClaimDue(e.ctx, q, slots, now)
			if err != nil {
				if e.ctx.Err() == nil {
					e.log.Error("quacker: claim failed", "queue", q, "err", err)
				}
				break
			}
			if len(claims) == 0 {
				break
			}
			for _, c := range claims {
				qs.inFlight.Add(1)
				e.wg.Add(1)
				// Publish before spawning: a fast task's completion event must
				// never overtake its claim event.
				e.publish(c.Step.RunID, c.Step.Name, store.StatusQueued, store.StatusRunning, "", now)
				if c.Run != nil && c.Run.StartedAt == now && !runStarted[c.Step.RunID] {
					runStarted[c.Step.RunID] = true
					e.publish(c.Step.RunID, "", store.StatusQueued, store.StatusRunning, "", now)
				}
				go e.execute(c, qs)
			}
			if len(claims) < slots {
				break // queue drained
			}
		}
	}
}

// execute runs one claimed step and records the outcome.
func (e *Engine) execute(c *store.Claim, qs *queueState) {
	defer e.wg.Done()
	defer qs.inFlight.Add(-1)

	st := c.Step
	// The step's task name (st.Task) identifies the registered function; the
	// step name (st.Name) is its DAG identity and may differ in workflows.
	taskName := st.Task
	if taskName == "" {
		taskName = st.Name
	}
	def := e.taskDef(taskName)
	if def == nil {
		// Not registered in this process (e.g. File-mode restart): park the
		// step briefly so the app can call Register before it retries. The
		// park is not a real attempt — give the claimed attempt back.
		e.warnUnknownTask(taskName)
		_ = e.st.ParkStep(context.Background(), st.ID, "task not registered in this process", e.now().Add(5*time.Second).UnixNano(), e.now().UnixNano())
		return
	}

	// Register the cancel func and consume any halt tombstone atomically, so
	// a Cancel racing the claim→registration window either sees this cancel
	// func or leaves a tombstone this executor will honor.
	taskCtx, cancel := context.WithCancel(e.ctx)
	var timeoutCancel context.CancelFunc
	if st.Timeout > 0 {
		taskCtx, timeoutCancel = context.WithTimeout(taskCtx, st.Timeout)
	}
	defer cancel()
	if timeoutCancel != nil {
		defer timeoutCancel()
	}
	e.mu.Lock()
	e.cancels[st.ID] = cancel
	_, halted := e.haltSteps[st.ID]
	delete(e.haltSteps, st.ID)
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		delete(e.cancels, st.ID)
		e.mu.Unlock()
	}()

	bg := context.Background()
	if halted {
		// The run was cancelled or failed while this step sat between claim
		// and registration: don't start the task's side effects at all.
		now := e.now().UnixNano()
		cancel()
		_ = e.st.StepCancelled(bg, st.ID, now)
		e.publish(st.RunID, st.Name, store.StatusRunning, store.StatusCancelled, "", now)
		return
	}

	var out json.RawMessage
	var err error
	depOutputs := map[string]json.RawMessage{}
	if len(st.DependsOn) > 0 {
		if outs, derr := e.st.GetStepOutputs(context.Background(), st.RunID, st.DependsOn); derr == nil {
			for k, v := range outs {
				depOutputs[k] = v
			}
		}
	}
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic: %v\n%s", r, stack())
			}
		}()
		out, err = def.Fn(withStepContext(taskCtx, c, depOutputs, e.logSend), st.Input)
	}()

	now := e.now()
	errStr := truncateErr(err)
	switch {
	case err == nil:
		res, cerr := e.st.CompleteStep(bg, st.ID, st.RunID, out, now.UnixNano())
		if cerr != nil {
			e.log.Error("quacker: record success", "run", st.RunID, "step", st.Name, "err", cerr)
			return
		}
		if !res.StepDone {
			// Another path (Cancel / run failure) already made this step
			// terminal; its outcome must not be resurrected to SUCCEEDED.
			// The CompleteStep tx converged it to CANCELLED, so publish that
			// transition — subscribers would otherwise see it stuck RUNNING.
			e.publish(st.RunID, st.Name, store.StatusRunning, store.StatusCancelled, "", now.UnixNano())
			return
		}
		e.publish(st.RunID, st.Name, store.StatusRunning, store.StatusSucceeded, "", now.UnixNano())
		for _, name := range res.ReadySteps {
			e.publish(st.RunID, name, store.StatusBlocked, store.StatusQueued, "", now.UnixNano())
		}
		if res.RunTerminal {
			e.finishRun(st.RunID, res.RunStatus, res.RunOutput, res.RunError, now.UnixNano())
		} else {
			e.wakeScheduler()
		}
	case e.runTerminal(st.RunID):
		// Run reached a terminal state (cancelled by the user, failed via a
		// sibling, interrupted): record this step CANCELLED rather than
		// retrying or failing it against a dead run.
		_ = e.st.StepCancelled(bg, st.ID, now.UnixNano())
		e.publish(st.RunID, st.Name, store.StatusRunning, store.StatusCancelled, errStr, now.UnixNano())
	case e.closing.Load():
		// Shutdown: Close() sweeps remaining RUNNING rows to INTERRUPTED.
	case st.Attempts < st.MaxAttempts:
		delay := def.Backoff.Delay(int(st.Attempts), now)
		if cerr := e.st.RetryStep(bg, st.ID, errStr, now.Add(delay).UnixNano(), now.UnixNano()); cerr != nil {
			e.log.Error("quacker: record retry", "run", st.RunID, "step", st.Name, "err", cerr)
		}
		e.publish(st.RunID, st.Name, store.StatusRunning, store.StatusQueued, errStr, now.UnixNano())
		e.wakeScheduler()
	default:
		res, cerr := e.st.FinalFailStep(bg, st.ID, st.RunID, errStr, now.UnixNano())
		if cerr != nil {
			e.log.Error("quacker: record failure", "run", st.RunID, "step", st.Name, "err", cerr)
			return
		}
		e.publish(st.RunID, st.Name, store.StatusRunning, store.StatusFailed, errStr, now.UnixNano())
		for _, name := range res.CancelledSteps {
			e.publish(st.RunID, name, store.StatusRunning, store.StatusCancelled, "", now.UnixNano())
		}
		e.cancelStepContexts(res.CancelledStepIDs)
		if res.RunTerminal {
			e.finishRun(st.RunID, res.RunStatus, nil, res.RunError, now.UnixNano())
		}
	}
}

// cancelStepContexts cancels the live contexts of the given steps, leaving
// halt tombstones for any whose executor hasn't registered yet.
func (e *Engine) cancelStepContexts(stepIDs []string) {
	if len(stepIDs) == 0 {
		return
	}
	e.mu.Lock()
	var cancels []context.CancelFunc
	for _, stepID := range stepIDs {
		if c := e.cancels[stepID]; c != nil {
			cancels = append(cancels, c)
			continue
		}
		e.haltSteps[stepID] = struct{}{}
	}
	e.mu.Unlock()
	for _, c := range cancels {
		c()
	}
}

// finishRun releases the waiter and publishes the run-level transition.
func (e *Engine) finishRun(runID, status string, output json.RawMessage, errMsg string, now int64) {
	var err error
	switch status {
	case store.StatusFailed:
		err = &RunError{RunID: runID, Msg: errMsg}
	case store.StatusCancelled:
		err = ErrRunCancelled
	case store.StatusInterrupted:
		err = ErrRunInterrupted
	}
	e.mu.Lock()
	w := e.waiters[runID]
	delete(e.waiters, runID) // handles are short-lived; keep the map bounded
	e.mu.Unlock()
	if w != nil {
		w.finish(status, output, err)
	}
	e.publish(runID, "", store.StatusRunning, status, errMsg, now)
}

func (e *Engine) runTerminal(runID string) bool {
	r, err := e.st.GetRun(context.Background(), runID)
	return err == nil && store.IsTerminal(r.Status)
}

func (e *Engine) taskDef(name string) *TaskDef {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.tasks[name]
}

func (e *Engine) publish(runID, step, from, to, errMsg string, at int64) {
	e.bus.Publish(bus.Event{RunID: runID, Step: step, From: from, To: to, At: at, Error: errMsg})
}

// Subscribe exposes the engine's transition bus. Cancel closes the channel.
func (e *Engine) Subscribe(runID string, buffer int) (<-chan bus.Event, func()) {
	return e.bus.Subscribe(runID, buffer)
}

// WaitersLen returns the number of unreleased run waiters.
func (e *Engine) WaitersLen() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.waiters)
}

// ForgetTask drops a task registration (used by tests to exercise the
// unregistered-task park path).
func (e *Engine) ForgetTask(name string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.tasks, name)
}

// logSend funnels a task log line into the engine's batched flusher. It is
// engine-scoped (never a package global) and seals itself off at Close so a
// task that outlived the drain cannot send on the closed channel.
func (e *Engine) logSend(entry store.LogEntry) {
	e.logMu.Lock()
	defer e.logMu.Unlock()
	if e.logClosed {
		return
	}
	select {
	case e.logCh <- entry:
	default:
		e.droppedLogs.Add(1)
	}
}

// flushLogs batches task log lines into the store until the channel closes.
func (e *Engine) flushLogs() {
	defer e.logWG.Done()
	const maxBatch = 64
	batch := make([]store.LogEntry, 0, maxBatch)
	drain := func() {
		if len(batch) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := e.st.AppendLogs(ctx, batch); err != nil && e.ctx.Err() == nil {
			e.log.Error("quacker: flush logs", "err", err)
		}
		cancel()
		batch = batch[:0]
	}
	for {
		select {
		case entry, ok := <-e.logCh:
			if !ok {
				drain()
				if n := e.droppedLogs.Load(); n > 0 {
					e.log.Warn("quacker: dropped task log lines (buffer full)", "count", n)
				}
				return
			}
			batch = append(batch, entry)
			if len(batch) >= maxBatch {
				drain()
			}
		case <-time.After(50 * time.Millisecond):
			drain()
		}
	}
}

func (e *Engine) warnUnknownTask(name string) {
	if _, ok := e.unknownWarned.LoadOrStore(name, struct{}{}); !ok {
		e.log.Warn("quacker: claimed step for unregistered task; will retry every 5s until registered", "task", name)
	}
}

// RunError is the error surfaced to waiters when a run fails.
type RunError struct {
	RunID string
	Msg   string
}

func (e *RunError) Error() string {
	return fmt.Sprintf("quacker: run %s failed: %s", e.RunID, e.Msg)
}

// interruptAll marks in-flight work INTERRUPTED (shutdown sweep).
func (e *Engine) interruptAll(now int64) error {
	bg := context.Background()
	if _, err := e.st.Write().ExecContext(bg,
		`UPDATE steps SET status=?, completed_at=? WHERE status=?`,
		store.StatusInterrupted, now, store.StatusRunning); err != nil {
		return err
	}
	_, err := e.st.Write().ExecContext(bg,
		`UPDATE runs SET status=?, completed_at=? WHERE status=?`,
		store.StatusInterrupted, now, store.StatusRunning)
	return err
}

// ---------------------------------------------------------------------------

func newID(now time.Time) string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return strconv.FormatInt(now.UnixNano(), 36) + "-" + hex.EncodeToString(b[:])
}

func truncateErr(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	const max = 4000
	if len(s) > max {
		return s[:max]
	}
	return s
}
