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

// migration is one schema change applied atomically with its version row.
// Migrations run in list order; each applies only when its version is ahead
// of the database's recorded version.
type migration struct {
	version int
	sql     string
}

var migrations = []migration{
	{version: 1, sql: schema},
}

// init validates the migration list's invariant before any Open can rely
// on it: non-empty, with strictly ascending versions. migrate assumes the
// last entry is the maximum version and applies entries in list order —
// a duplicate or out-of-order version would silently skip or misorder
// DDL, so it panics at startup instead.
func init() {
	if len(migrations) == 0 {
		panic("quacker: store: no schema migrations defined")
	}
	for i := 1; i < len(migrations); i++ {
		if migrations[i].version <= migrations[i-1].version {
			panic(fmt.Sprintf("quacker: store: migrations out of order: version %d follows version %d",
				migrations[i].version, migrations[i-1].version))
		}
	}
}

// migrate brings the database to the latest schema version. The
// schema_migrations table is created first so every later migration can be
// recorded in the same transaction as its DDL. Migration 1 is all
// CREATE-IF-NOT-EXISTS, so a v0.1 File database (tables present, no version
// row) baselines to version 1 without touching existing data.
func (s *Store) migrate(ctx context.Context) error {
	if _, err := s.write.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY,
		applied_at INTEGER NOT NULL
	)`); err != nil {
		return fmt.Errorf("quacker: migrate: %w", err)
	}
	var version int
	if err := s.write.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(version),0) FROM schema_migrations`).Scan(&version); err != nil {
		return fmt.Errorf("quacker: migrate: %w", err)
	}
	// A database written by a newer build records a version this build
	// doesn't know; refuse rather than run it through older assumptions.
	// migrations is ordered ascending, so the last entry is the max.
	if max := migrations[len(migrations)-1].version; version > max {
		return fmt.Errorf("quacker: database schema version %d is newer than this build supports (max %d)", version, max)
	}
	for _, m := range migrations {
		if m.version <= version {
			continue
		}
		tx, err := s.write.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("quacker: migrate: %w", err)
		}
		if _, err := tx.ExecContext(ctx, m.sql); err != nil {
			tx.Rollback()
			return fmt.Errorf("quacker: migrate to version %d: %w", m.version, err)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO schema_migrations (version, applied_at) VALUES (?,?)`, m.version, nowUnix()); err != nil {
			tx.Rollback()
			return fmt.Errorf("quacker: record migration %d: %w", m.version, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("quacker: migrate to version %d: %w", m.version, err)
		}
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
