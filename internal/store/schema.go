package store

import (
	"context"
	"fmt"
)

// Status values shared by runs and steps.
const (
	StatusQueued      = "QUEUED"
	StatusRunning     = "RUNNING"
	StatusBlocked     = "BLOCKED" // DAG step waiting on dependencies
	StatusSucceeded   = "SUCCEEDED"
	StatusFailed      = "FAILED"
	StatusCancelled   = "CANCELLED"
	StatusInterrupted = "INTERRUPTED" // lost to an unclean shutdown/close
)

// Kind values for runs.
const (
	KindTask     = "task"
	KindWorkflow = "workflow"
)

// IsTerminal reports whether a status is final.
func IsTerminal(status string) bool {
	switch status {
	case StatusSucceeded, StatusFailed, StatusCancelled, StatusInterrupted:
		return true
	}
	return false
}

const schema = `
CREATE TABLE IF NOT EXISTS runs (
	id            TEXT PRIMARY KEY,
	workflow      TEXT NOT NULL,
	kind          TEXT NOT NULL DEFAULT 'task',
	status        TEXT NOT NULL,
	queue         TEXT NOT NULL DEFAULT 'default',
	priority      INTEGER NOT NULL DEFAULT 0,
	input         BLOB,
	output        BLOB,
	error         TEXT NOT NULL DEFAULT '',
	attempts      INTEGER NOT NULL DEFAULT 0,
	max_attempts  INTEGER NOT NULL DEFAULT 1,
	run_at        INTEGER NOT NULL,
	created_at    INTEGER NOT NULL,
	started_at    INTEGER NOT NULL DEFAULT 0,
	completed_at  INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_runs_claim ON runs (status, run_at);
CREATE INDEX IF NOT EXISTS idx_runs_list  ON runs (workflow, created_at);

CREATE TABLE IF NOT EXISTS steps (
	id            TEXT PRIMARY KEY,
	run_id        TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
	name          TEXT NOT NULL,
	task          TEXT NOT NULL DEFAULT '',
	ord           INTEGER NOT NULL DEFAULT 0,
	status        TEXT NOT NULL,
	depends_on    TEXT NOT NULL DEFAULT '',
	queue         TEXT NOT NULL DEFAULT 'default',
	priority      INTEGER NOT NULL DEFAULT 0,
	input         BLOB,
	output        BLOB,
	error         TEXT NOT NULL DEFAULT '',
	attempts      INTEGER NOT NULL DEFAULT 0,
	max_attempts  INTEGER NOT NULL DEFAULT 1,
	timeout_ns    INTEGER NOT NULL DEFAULT 0,
	run_at        INTEGER NOT NULL,
	created_at    INTEGER NOT NULL,
	started_at    INTEGER NOT NULL DEFAULT 0,
	completed_at  INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_steps_claim ON steps (status, run_at);
CREATE INDEX IF NOT EXISTS idx_steps_run   ON steps (run_id, ord);

CREATE TABLE IF NOT EXISTS crons (
	id       TEXT PRIMARY KEY,
	name     TEXT NOT NULL UNIQUE,
	spec     TEXT NOT NULL,
	task     TEXT NOT NULL,
	input    BLOB,
	next_at  INTEGER NOT NULL,
	created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS logs (
	seq    INTEGER PRIMARY KEY AUTOINCREMENT,
	run_id TEXT NOT NULL,
	step   TEXT NOT NULL DEFAULT '',
	at     INTEGER NOT NULL,
	level  TEXT NOT NULL DEFAULT 'INFO',
	message TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_logs_run ON logs (run_id, seq);
`

func (s *Store) migrate(ctx context.Context) error {
	if _, err := s.write.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("quacker: migrate: %w", err)
	}
	return nil
}

// recoverInterrupted applies boot-time recovery for File mode: runs and steps
// left RUNNING or INTERRUPTED by a previous process are re-queued (keep=true)
// or failed (keep=false). Attempt counts are preserved; completed steps are
// never touched.
func (s *Store) recoverInterrupted(ctx context.Context, keep bool) error {
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	newStatus := StatusQueued
	runErr := ""
	if !keep {
		newStatus = StatusFailed
		runErr = "interrupted by restart"
	}
	now := nowUnix()
	for _, table := range []string{"steps", "runs"} {
		q := fmt.Sprintf(`UPDATE %s SET
			status = ?,
			run_at = ?,
			error = CASE WHEN ? = 'FAILED' THEN ? ELSE error END,
			completed_at = CASE WHEN ? = 'FAILED' THEN ? ELSE 0 END
			WHERE status IN (?, ?)`, table)
		if _, err := tx.ExecContext(ctx, q,
			newStatus, now, newStatus, runErr, newStatus, now,
			StatusRunning, StatusInterrupted); err != nil {
			return fmt.Errorf("quacker: recover %s: %w", table, err)
		}
	}
	return tx.Commit()
}
