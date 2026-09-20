package quacker

import (
	"context"
	"encoding/json"
	"time"

	"github.com/kartikbazzad/quacker/internal/store"
)

// Status is the lifecycle state of a run or step.
type Status string

const (
	StatusQueued      Status = store.StatusQueued
	StatusRunning     Status = store.StatusRunning
	StatusBlocked     Status = store.StatusBlocked
	StatusSuspended   Status = store.StatusSuspended
	StatusPaused      Status = store.StatusPaused
	StatusSucceeded   Status = store.StatusSucceeded
	StatusFailed      Status = store.StatusFailed
	StatusCancelled   Status = store.StatusCancelled
	StatusInterrupted Status = store.StatusInterrupted
)

// Kind distinguishes single-task runs from workflow runs.
type Kind string

const (
	KindTask     Kind = store.KindTask
	KindWorkflow Kind = store.KindWorkflow
)

func unixToTime(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

// StepState is a point-in-time snapshot of one step.
type StepState struct {
	Name string `json:"name"`
	// Task is the registered task that executes the step; it differs from
	// Name for named workflow steps.
	Task   string   `json:"task,omitempty"`
	Status Status   `json:"status"`
	Deps   []string `json:"deps,omitempty"`
	// Key is the step's concurrency key (empty when unkeyed). Steps
	// sharing a key run at most KeyConcurrency at a time.
	Key         string          `json:"key,omitempty"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	Timeout     time.Duration   `json:"timeout,omitempty"`
	Input       json.RawMessage `json:"input,omitempty"`
	Output      json.RawMessage `json:"output,omitempty"`
	Error       string          `json:"error,omitempty"`
	RunAt       time.Time       `json:"run_at"`
	CreatedAt   time.Time       `json:"created_at"`
	StartedAt   time.Time       `json:"started_at,omitempty"`
	CompletedAt time.Time       `json:"completed_at,omitempty"`
	// WaitEvent is the event a SUSPENDED step awaits ("" for a sleep or a
	// non-suspended step); ResumeAt is when a sleeping/suspended step becomes
	// claimable again (zero for event-only waits).
	WaitEvent string    `json:"wait_event,omitempty"`
	ResumeAt  time.Time `json:"resume_at,omitempty"`
	// Labels are the worker labels the step requires (empty = any engine).
	Labels []string `json:"labels,omitempty"`
}

// Execution is a consistent point-in-time view of a run. Reads never pause
// task execution.
type Execution struct {
	RunID       string          `json:"run_id"`
	Workflow    string          `json:"workflow"`
	Kind        Kind            `json:"kind"`
	Status      Status          `json:"status"`
	Queue       string          `json:"queue"`
	Priority    int64           `json:"priority"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	Input       json.RawMessage `json:"input,omitempty"`
	Output      json.RawMessage `json:"output,omitempty"`
	Error       string          `json:"error,omitempty"`
	// Key mirrors the first step's concurrency key (per-step keys in
	// workflows may differ).
	Key string `json:"key,omitempty"`
	// TraceParent is the W3C traceparent captured at enqueue ("" without
	// tracing), for correlating this run with your traces.
	TraceParent string      `json:"trace_parent,omitempty"`
	RunAt       time.Time   `json:"run_at"`
	CreatedAt   time.Time   `json:"created_at"`
	StartedAt   time.Time   `json:"started_at,omitempty"`
	CompletedAt time.Time   `json:"completed_at,omitempty"`
	Steps       []StepState `json:"steps"`
	// Groups are the run's logical groups (from a grouped workflow), each with
	// its group-level dependencies and member step names. Empty for a plain
	// workflow.
	Groups []RunGroup `json:"groups,omitempty"`
	// Children are runs enqueued from inside this run (via EnqueueChild),
	// oldest first. They run independently; the parent does not wait for them.
	Children []RunSummary `json:"children,omitempty"`
}

// RunGroup is a named set of steps that share a group-level dependency. Group
// deps gate the whole group; member steps do not repeat them.
type RunGroup struct {
	Name string `json:"name"`
	// Deps are the names of groups this group runs after.
	Deps []string `json:"deps,omitempty"`
	// Steps are the member step names.
	Steps []string `json:"steps"`
}

// RunSummary is a run without payloads, for list views.
type RunSummary struct {
	RunID    string `json:"run_id"`
	Workflow string `json:"workflow"`
	Kind     Kind   `json:"kind"`
	Status   Status `json:"status"`
	Queue    string `json:"queue"`
	Priority int64  `json:"priority"`
	Error    string `json:"error,omitempty"`
	ParentID string `json:"parent_id,omitempty"`
	// ParentStep is the step (in ParentID) that spawned this run.
	ParentStep  string    `json:"parent_step,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	StartedAt   time.Time `json:"started_at,omitempty"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
	// DeadLetteredAt is set when the run was dead-lettered (zero otherwise).
	DeadLetteredAt time.Time `json:"dead_lettered_at,omitempty"`
	// Ephemeral is true for a run that is deleted on terminal.
	Ephemeral bool `json:"ephemeral,omitempty"`
}

// RunFilter selects runs for Runs.
type RunFilter struct {
	Status   Status
	Queue    string
	Workflow string
	// ParentID limits the result to a run's children.
	ParentID string
	Limit    int
	Offset   int
}

// QueueStats is a per-queue depth snapshot.
type QueueStats struct {
	Queued    int64 `json:"queued"`
	Running   int64 `json:"running"`
	Blocked   int64 `json:"blocked"`
	Suspended int64 `json:"suspended"`
}

// Metrics is a global state snapshot.
type Metrics struct {
	Runs   map[Status]int64      `json:"runs"`
	Queues map[string]QueueStats `json:"queues"`
}

// MetricsSnapshot is the value handed to the WithMetricsFunc callback; it has
// the same shape as Metrics.
type MetricsSnapshot = Metrics

// LogEntry is one task log line captured via TaskLogger.
type LogEntry struct {
	RunID   string    `json:"run_id"`
	Step    string    `json:"step"`
	At      time.Time `json:"at"`
	Level   string    `json:"level"`
	Message string    `json:"message"`
}

// Event is a live status transition delivered by Subscribe.
type Event struct {
	RunID string    `json:"run_id"`
	Step  string    `json:"step,omitempty"` // empty for run-level transitions
	From  Status    `json:"from"`
	To    Status    `json:"to"`
	At    time.Time `json:"at"`
	Error string    `json:"error,omitempty"`
}

// CronInfo describes a registered cron trigger.
type CronInfo struct {
	Name string    `json:"name"`
	Spec string    `json:"spec"`
	Task string    `json:"task"`
	Next time.Time `json:"next"`
}

// Execution returns a snapshot of a run. Safe to call at any moment: it reads
// through the read-only connection pool and never blocks (or is blocked by)
// the workers' write path. The run row is read before its steps and step
// statuses only advance forward, so a terminal run is never shown with
// non-terminal steps. Returns ErrNotFound for unknown runs.
//
// For workflow runs, Attempts counts total step attempts across the DAG and
// MaxAttempts mirrors the first step's setting; per-step values are on Steps.
func (q *Quacker) Execution(ctx context.Context, runID string) (*Execution, error) {
	run, steps, err := q.st.GetRunWithSteps(ctx, runID)
	if err != nil {
		return nil, err
	}
	ex := &Execution{
		RunID: run.ID, Workflow: run.Workflow, Kind: Kind(run.Kind), Status: Status(run.Status),
		Queue: run.Queue, Priority: run.Priority, Attempts: int(run.Attempts), MaxAttempts: int(run.MaxAttempts),
		Input: run.Input, Output: run.Output, Error: run.Error, Key: run.ConcurrencyKey,
		TraceParent: run.TraceParent,
		RunAt:       unixToTime(run.RunAt), CreatedAt: unixToTime(run.CreatedAt),
		StartedAt: unixToTime(run.StartedAt), CompletedAt: unixToTime(run.CompletedAt),
		Steps: make([]StepState, 0, len(steps)),
	}
	for _, s := range steps {
		ex.Steps = append(ex.Steps, StepState{
			Name: s.Name, Task: s.Task, Status: Status(s.Status), Deps: s.DependsOn, Key: s.ConcurrencyKey,
			Attempts: int(s.Attempts), MaxAttempts: int(s.MaxAttempts), Timeout: s.Timeout,
			Input: s.Input, Output: s.Output, Error: s.Error,
			RunAt: unixToTime(s.RunAt), CreatedAt: unixToTime(s.CreatedAt),
			StartedAt: unixToTime(s.StartedAt), CompletedAt: unixToTime(s.CompletedAt),
			WaitEvent: s.WaitEvent, ResumeAt: unixToTime(s.ResumeAt), Labels: s.Labels,
		})
	}
	if children, cerr := q.st.ListChildren(ctx, runID); cerr == nil {
		for _, c := range children {
			ex.Children = append(ex.Children, RunSummary{
				RunID: c.ID, Workflow: c.Workflow, Kind: Kind(c.Kind), Status: Status(c.Status),
				Queue: c.Queue, Priority: c.Priority, Error: c.Error, ParentID: c.ParentID,
				CreatedAt: unixToTime(c.CreatedAt), StartedAt: unixToTime(c.StartedAt),
				CompletedAt: unixToTime(c.CompletedAt),
			})
		}
	}
	if len(run.Groups) > 0 {
		_ = json.Unmarshal(run.Groups, &ex.Groups)
	}
	return ex, nil
}

// Runs lists runs matching the filter, newest first.
func (q *Quacker) Runs(ctx context.Context, f RunFilter) ([]RunSummary, error) {
	rs, err := q.st.ListRuns(ctx, store.Filter{
		Status: string(f.Status), Queue: f.Queue, Workflow: f.Workflow, ParentID: f.ParentID,
		Limit: f.Limit, Offset: f.Offset,
	})
	if err != nil {
		return nil, err
	}
	out := make([]RunSummary, 0, len(rs))
	for _, r := range rs {
		out = append(out, runSummary(r))
	}
	return out, nil
}

func runSummary(r *store.Run) RunSummary {
	return RunSummary{
		RunID: r.ID, Workflow: r.Workflow, Kind: Kind(r.Kind), Status: Status(r.Status),
		Queue: r.Queue, Priority: r.Priority, Error: r.Error, ParentID: r.ParentID, ParentStep: r.ParentStep,
		CreatedAt: unixToTime(r.CreatedAt), StartedAt: unixToTime(r.StartedAt),
		CompletedAt: unixToTime(r.CompletedAt), DeadLetteredAt: unixToTime(r.DeadLetteredAt),
		Ephemeral: r.Ephemeral,
	}
}

// DeadLetterFilter selects dead-lettered runs for DeadLetters.
type DeadLetterFilter struct {
	// Workflow and Queue, when non-empty, restrict the result.
	Workflow string
	Queue    string
	// Limit caps the result (default 100); Offset skips that many.
	Limit  int
	Offset int
}

// DeadLetters lists runs dead-lettered by a task with WithDeadLetter, newest
// first.
func (q *Quacker) DeadLetters(ctx context.Context, f DeadLetterFilter) ([]RunSummary, error) {
	rs, err := q.eng.DeadLetters(ctx, f.Workflow, f.Queue, f.Limit, f.Offset)
	if err != nil {
		return nil, err
	}
	out := make([]RunSummary, 0, len(rs))
	for _, r := range rs {
		out = append(out, runSummary(r))
	}
	return out, nil
}

// RetryDeadLetter reopens a dead-lettered run in place and returns it to the
// queue. It returns ErrNotDeadLetter if the run exists but is not
// dead-lettered, and ErrNotFound if it does not exist.
func (q *Quacker) RetryDeadLetter(ctx context.Context, runID string) error {
	return q.eng.RetryDeadLetter(ctx, runID)
}

// DismissDeadLetter clears a run's dead-letter marker without retrying it.
func (q *Quacker) DismissDeadLetter(ctx context.Context, runID string) error {
	return q.eng.DismissDeadLetter(ctx, runID)
}

// Metrics returns run counts by status and per-queue depths.
func (q *Quacker) Metrics(ctx context.Context) (*Metrics, error) {
	byStatus, queues, err := q.st.Metrics(ctx)
	if err != nil {
		return nil, err
	}
	m := &Metrics{Runs: map[Status]int64{}, Queues: map[string]QueueStats{}}
	for st, n := range byStatus {
		m.Runs[Status(st)] = n
	}
	for name, qs := range queues {
		m.Queues[name] = QueueStats{Queued: qs.Queued, Running: qs.Running, Blocked: qs.Blocked}
	}
	return m, nil
}

// Logs returns up to limit task log lines for a run, oldest first. Lines are
// emitted with TaskLogger inside task functions and are batched (~50ms), so
// very recent lines may lag slightly.
func (q *Quacker) Logs(ctx context.Context, runID string, limit int) ([]LogEntry, error) {
	entries, err := q.st.GetLogs(ctx, runID, limit)
	if err != nil {
		return nil, err
	}
	out := make([]LogEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, LogEntry{
			RunID: e.RunID, Step: e.Step, At: unixToTime(e.At), Level: e.Level, Message: e.Message,
		})
	}
	return out, nil
}

// Subscribe returns a channel of live status transitions for runID (pass ""
// to observe all runs) and a cancel function that stops the stream and closes
// the channel. Events are dropped rather than delivered late if a consumer
// stalls — use Execution for an authoritative snapshot.
func (q *Quacker) Subscribe(runID string) (<-chan Event, func()) {
	ch, cancel := q.eng.Subscribe(runID, 256)
	out := make(chan Event, 256)
	go func() {
		defer close(out)
		for ev := range ch {
			out <- Event{
				RunID: ev.RunID, Step: ev.Step, From: Status(ev.From), To: Status(ev.To),
				At: unixToTime(ev.At), Error: ev.Error,
			}
		}
	}()
	return out, cancel
}
