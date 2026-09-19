package store

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// ErrNotFound is returned when a run does not exist.
var ErrNotFound = errors.New("quacker: run not found")

// Sentinel errors for run-level operations.
var (
	// ErrRunRunning reports that a run has a RUNNING step and cannot be
	// snoozed or paused in place.
	ErrRunRunning = errors.New("quacker: run is running")
	// ErrRunTerminal reports that a run has already finished.
	ErrRunTerminal = errors.New("quacker: run is terminal")
	// ErrNotDeadLetter reports that a run is not dead-lettered.
	ErrNotDeadLetter = errors.New("quacker: run is not dead-lettered")
)

// Run is a row in the runs table. All timestamps are unix nanoseconds
// (0 means unset).
type Run struct {
	ID          string
	Workflow    string
	Kind        string
	Status      string
	Queue       string
	Priority    int64
	Input       []byte
	Output      []byte
	Error       string
	Attempts    int64
	MaxAttempts int64
	RunAt       int64
	CreatedAt   int64
	StartedAt   int64
	CompletedAt int64
	// ConcurrencyKey mirrors the first step's key (informational; per-step
	// keys in workflows may differ).
	ConcurrencyKey string
	// ParentID is the run this one was enqueued from ("" for a root run).
	// Lineage is informational: a purged parent leaves a dangling id.
	ParentID string
	// TraceParent is the W3C traceparent of the span active at enqueue ("" if
	// tracing is disabled), used to link the executing span to its producer.
	TraceParent string
	// UniqueKey, when non-empty, makes the run unique per (workflow, key): at
	// most one non-terminal run may hold it. It is NULL in the database for
	// non-unique runs so a unique index can ignore them.
	UniqueKey string
	// PausedAt is when the run was paused (unix nanos; 0 when not paused).
	PausedAt int64
	// DeadLetteredAt is when the run was dead-lettered (unix nanos; 0 when
	// not). Set for an opted-in task's run when it exhausts retries.
	DeadLetteredAt int64
	// SequenceKey, when non-empty, partitions the run into a sequence; Seq is
	// its insertion order within that sequence.
	SequenceKey string
	Seq         int64
	// Ephemeral runs are persisted while in flight but deleted on terminal and
	// never recovered.
	Ephemeral bool
}

// Step is a row in the steps table. A single-task run has exactly one step.
// Name is the step's DAG identity; Task is the registered task that executes
// it (identical for single tasks).
type Step struct {
	ID          string
	RunID       string
	Name        string
	Task        string
	Ord         int64
	Status      string
	DependsOn   []string
	Queue       string
	Priority    int64
	Input       []byte
	Output      []byte
	Error       string
	Attempts    int64
	MaxAttempts int64
	Timeout     time.Duration
	RunAt       int64
	CreatedAt   int64
	StartedAt   int64
	CompletedAt int64
	// ConcurrencyKey groups steps whose concurrent execution is capped at
	// KeyLimit across all queues ("" = unkeyed, never gated).
	ConcurrencyKey string
	// KeyLimit is the max simultaneously-RUNNING steps sharing
	// ConcurrencyKey; <=0 is treated as unlimited by the claim gate.
	KeyLimit int64
	// ClaimedAt is stamped on every QUEUED→RUNNING transition so a queue's
	// sliding-window start rate can be counted; ParkStep clears it since a
	// parked claim never executed. Resumes also stamp it (a resumed step
	// spends start budget), but do not increment Attempts.
	ClaimedAt int64
	// ResumeAt is when a SUSPENDED step becomes claimable again: a sleep's
	// wake time, or a wait's timeout deadline. 0 means event-only (never
	// self-claims).
	ResumeAt int64
	// WaitKind is "sleep" or "wait" for a SUSPENDED step ("" otherwise);
	// WaitEvent is the awaited event name for a wait.
	WaitKind  string
	WaitEvent string
	// Labels are the worker labels a step requires; an engine claims it only
	// when its own worker labels are a superset. Empty means any engine.
	Labels []string
	// WorkerID is the engine that holds this RUNNING step's lease ("" when
	// unclaimed or when leases are off).
	WorkerID string
	// LeaseExpiresAt is when the lease lapses ("" / 0 when leases are off); a
	// reaper re-queues a RUNNING step whose lease has expired.
	LeaseExpiresAt int64
	// SequenceKey/Seq mirror the run's sequence membership for the claim gate;
	// a step is claimable only when no earlier same-sequence step is
	// unfinished.
	SequenceKey string
	Seq         int64
	// Keys are extra concurrency keys beyond ConcurrencyKey, each with its own
	// limit; all must have a free slot for the step to be claimed.
	Keys []StepKey
}

// StepKey is one named extra concurrency key on a step.
type StepKey struct {
	Name  string
	Value string
	Limit int64
}

// Cron is a row in the crons table.
type Cron struct {
	Name      string
	Spec      string
	Task      string
	Input     []byte
	NextAt    int64
	CreatedAt int64
}

// LogEntry is one task log line.
type LogEntry struct {
	RunID   string
	Step    string
	At      int64
	Level   string
	Message string
}

// Event is a persisted in-process event (a best-effort audit of emits).
type Event struct {
	Seq       int64
	Name      string
	Payload   []byte
	CreatedAt int64
}

// EventSub binds an event name to a task that is enqueued on every emit.
type EventSub struct {
	Event     string
	Task      string
	CreatedAt int64
}

func nowUnix() int64 { return time.Now().UnixNano() }

// encodeList encodes a string slice for a TEXT column as a JSON array
// (["a","b"]) rather than a delimited string: a value containing any
// delimiter cannot corrupt the list. Used for steps.depends_on and
// steps.labels.
func encodeList(xs []string) string {
	if len(xs) == 0 {
		return "[]"
	}
	b, err := json.Marshal(xs)
	if err != nil {
		return "[]"
	}
	return string(b)
}

// decodeList decodes a JSON-array column. It still tolerates the legacy
// comma-joined form for depends_on as a safety net for databases written
// before migration 7.
func decodeList(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" || s == "[]" || s == "null" {
		return nil
	}
	if !strings.HasPrefix(s, "[") {
		var legacy []string
		for _, p := range strings.Split(s, ",") {
			if p = strings.TrimSpace(p); p != "" {
				legacy = append(legacy, p)
			}
		}
		return legacy
	}
	var out []string
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil
	}
	return out
}

const runCols = `id, workflow, kind, status, queue, priority, input, output, error,
	attempts, max_attempts, run_at, created_at, started_at, completed_at, concurrency_key, parent_id, trace_parent,
	COALESCE(unique_key, ''), paused_at, dead_lettered_at, sequence_key, seq, ephemeral`

const stepCols = `id, run_id, name, task, ord, status, depends_on, queue, priority, input, output, error,
	attempts, max_attempts, timeout_ns, run_at, created_at, started_at, completed_at,
	concurrency_key, key_limit, claimed_at, resume_at, wait_kind, wait_event, labels,
	worker_id, lease_expires_at, sequence_key, seq`

func scanRun(row interface{ Scan(...any) error }) (*Run, error) {
	var r Run
	var ephemeral int64
	err := row.Scan(&r.ID, &r.Workflow, &r.Kind, &r.Status, &r.Queue, &r.Priority,
		&r.Input, &r.Output, &r.Error, &r.Attempts, &r.MaxAttempts,
		&r.RunAt, &r.CreatedAt, &r.StartedAt, &r.CompletedAt, &r.ConcurrencyKey, &r.ParentID, &r.TraceParent,
		&r.UniqueKey, &r.PausedAt, &r.DeadLetteredAt, &r.SequenceKey, &r.Seq, &ephemeral)
	if err != nil {
		return nil, err
	}
	r.Ephemeral = ephemeral != 0
	return &r, nil
}

func scanStep(row interface{ Scan(...any) error }) (*Step, error) {
	var s Step
	var deps, labels string
	err := row.Scan(&s.ID, &s.RunID, &s.Name, &s.Task, &s.Ord, &s.Status, &deps, &s.Queue, &s.Priority,
		&s.Input, &s.Output, &s.Error, &s.Attempts, &s.MaxAttempts, &s.Timeout,
		&s.RunAt, &s.CreatedAt, &s.StartedAt, &s.CompletedAt,
		&s.ConcurrencyKey, &s.KeyLimit, &s.ClaimedAt,
		&s.ResumeAt, &s.WaitKind, &s.WaitEvent, &labels,
		&s.WorkerID, &s.LeaseExpiresAt, &s.SequenceKey, &s.Seq)
	if err != nil {
		return nil, err
	}
	s.DependsOn = decodeList(deps)
	s.Labels = decodeList(labels)
	return &s, nil
}
