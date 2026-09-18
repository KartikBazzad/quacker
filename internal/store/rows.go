package store

import (
	"errors"
	"strings"
	"time"
)

// ErrNotFound is returned when a run does not exist.
var ErrNotFound = errors.New("quacker: run not found")

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

func nowUnix() int64 { return time.Now().UnixNano() }

func joinDeps(deps []string) string { return strings.Join(deps, ",") }

func splitDeps(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

const runCols = `id, workflow, kind, status, queue, priority, input, output, error,
	attempts, max_attempts, run_at, created_at, started_at, completed_at`

const stepCols = `id, run_id, name, task, ord, status, depends_on, queue, priority, input, output, error,
	attempts, max_attempts, timeout_ns, run_at, created_at, started_at, completed_at`

func scanRun(row interface{ Scan(...any) error }) (*Run, error) {
	var r Run
	err := row.Scan(&r.ID, &r.Workflow, &r.Kind, &r.Status, &r.Queue, &r.Priority,
		&r.Input, &r.Output, &r.Error, &r.Attempts, &r.MaxAttempts,
		&r.RunAt, &r.CreatedAt, &r.StartedAt, &r.CompletedAt)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func scanStep(row interface{ Scan(...any) error }) (*Step, error) {
	var s Step
	var deps string
	err := row.Scan(&s.ID, &s.RunID, &s.Name, &s.Task, &s.Ord, &s.Status, &deps, &s.Queue, &s.Priority,
		&s.Input, &s.Output, &s.Error, &s.Attempts, &s.MaxAttempts, &s.Timeout,
		&s.RunAt, &s.CreatedAt, &s.StartedAt, &s.CompletedAt)
	if err != nil {
		return nil, err
	}
	s.DependsOn = splitDeps(deps)
	return &s, nil
}
