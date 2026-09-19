package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/kartikbazzad/quacker/driver"
)

// Status values shared by runs and steps.
const (
	StatusQueued      = "QUEUED"
	StatusRunning     = "RUNNING"
	StatusBlocked     = "BLOCKED"   // DAG step waiting on dependencies
	StatusSuspended   = "SUSPENDED" // durable sleep/wait: not holding a slot
	StatusPaused      = "PAUSED"    // run-level pause: claims held until resume
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

// migration 2 adds per-key concurrency and rate-limit columns. The key
// gate counts RUNNING rows sharing a step's concurrency_key; claimed_at is
// stamped on every claim so a queue's sliding-window start rate can be
// counted directly (started_at stays first-start only). All ALTERs use
// defaults so existing rows need no backfill.
const migration2 = `
ALTER TABLE runs ADD COLUMN concurrency_key TEXT NOT NULL DEFAULT '';
ALTER TABLE steps ADD COLUMN concurrency_key TEXT NOT NULL DEFAULT '';
ALTER TABLE steps ADD COLUMN key_limit INTEGER NOT NULL DEFAULT 0;
ALTER TABLE steps ADD COLUMN claimed_at INTEGER NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_steps_key ON steps (concurrency_key, status);
CREATE INDEX IF NOT EXISTS idx_steps_claimed ON steps (queue, claimed_at);
`

// migration 3 adds an index supporting retention/purge, which selects
// terminal runs by completion time (status IN (...) AND completed_at < ?).
const migration3 = `
CREATE INDEX IF NOT EXISTS idx_runs_purge ON runs (status, completed_at);
`

// migration 4 adds in-process events and their event→task bindings.
const migration4 = `
CREATE TABLE IF NOT EXISTS events (
	seq        INTEGER PRIMARY KEY AUTOINCREMENT,
	name       TEXT NOT NULL,
	payload    BLOB,
	created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_name ON events (name, seq);

CREATE TABLE IF NOT EXISTS event_subscriptions (
	id         TEXT PRIMARY KEY,
	event      TEXT NOT NULL,
	task       TEXT NOT NULL,
	created_at INTEGER NOT NULL,
	UNIQUE(event, task)
);
`

// migration 5 adds the durable-execution substrate: a SUSPENDED step's resume
// time/wait identity, and the per-step journal of durable awaits replayed on
// every invocation of a durable task.
const migration5 = `
ALTER TABLE steps ADD COLUMN resume_at INTEGER NOT NULL DEFAULT 0;
ALTER TABLE steps ADD COLUMN wait_kind TEXT NOT NULL DEFAULT '';
ALTER TABLE steps ADD COLUMN wait_event TEXT NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_steps_resume ON steps (status, resume_at);

CREATE TABLE IF NOT EXISTS step_journal (
	step_id   TEXT NOT NULL REFERENCES steps(id) ON DELETE CASCADE,
	idx       INTEGER NOT NULL,
	kind      TEXT NOT NULL,
	key       TEXT NOT NULL DEFAULT '',
	event     TEXT NOT NULL DEFAULT '',
	wake_at   INTEGER NOT NULL DEFAULT 0,
	deadline  INTEGER NOT NULL DEFAULT 0,
	payload   BLOB,
	result    BLOB,
	err       TEXT NOT NULL DEFAULT '',
	done      INTEGER NOT NULL DEFAULT 0,
	timed_out INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (step_id, idx)
);
CREATE INDEX IF NOT EXISTS idx_journal_wait ON step_journal (kind, done, event);
`

// migration 6 adds child-run lineage: a run enqueued from inside another run
// records its parent. No foreign key (lineage is informational, and a purged
// parent may leave a dangling id).
const migration6 = `
ALTER TABLE runs ADD COLUMN parent_id TEXT NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_runs_parent ON runs (parent_id, created_at);
`

// migration 7 converts steps.depends_on from the legacy comma-joined text to
// a JSON array (["a","b"]), so a step name containing a comma can no longer
// corrupt the dependency list. char(34) is a double quote; an empty list
// becomes [].
const migration7 = `
UPDATE steps SET depends_on = CASE
	WHEN depends_on = '' THEN '[]'
	ELSE '[' || char(34) || replace(depends_on, ',', char(34) || ',' || char(34)) || char(34) || ']'
END;
`

// migration 8 adds worker-label routing: a step carries a JSON array of
// required labels, and an engine only claims a step whose labels are a subset
// of its own worker labels.
const migration8 = `
ALTER TABLE steps ADD COLUMN labels TEXT NOT NULL DEFAULT '[]';
`

// migration 9 stores the W3C traceparent of the span active at enqueue, so a
// step executed later (possibly after a restart) can link to its producer.
const migration9 = `
ALTER TABLE runs ADD COLUMN trace_parent TEXT NOT NULL DEFAULT '';
`

// migration 10 adds multi-instance step leases (worker identity + expiry).
// SQLite is single-process and does not use them, but the columns keep the
// schema aligned with Postgres and let tests exercise the lease path.
const migration10 = `
ALTER TABLE steps ADD COLUMN worker_id TEXT NOT NULL DEFAULT '';
ALTER TABLE steps ADD COLUMN lease_expires_at BIGINT NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_steps_lease  ON steps (status, lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_steps_worker ON steps (worker_id, status);
`

// migration 11 adds indexes for the DAG completion hot path: (run_id, status)
// so the terminal check stops at the first active step and BLOCKED steps are
// read directly, and (run_id, name) so dependency statuses are looked up
// without scanning the run.
const migration11 = `
CREATE INDEX IF NOT EXISTS idx_steps_run_status ON steps (run_id, status);
CREATE INDEX IF NOT EXISTS idx_steps_run_name   ON steps (run_id, name);
`

// migration 12 renames step_journal.key to wkey: KEY is a reserved word in
// MySQL, and the query layer must reference the column unquoted on every
// dialect.
const migration12 = `
ALTER TABLE step_journal RENAME COLUMN key TO wkey;
`

// migration13 adds unique jobs: a nullable unique_key (NULL for non-unique
// runs, which unique indexes ignore) and a unique index per (workflow, key).
const migration13 = `
ALTER TABLE runs ADD COLUMN unique_key TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS uq_runs_unique ON runs (workflow, unique_key);
`

// migration14 adds queue pauses: a paused queue's steps are not claimed until
// the row is deleted, enforced directly in the claim predicate.
const migration14 = `
CREATE TABLE IF NOT EXISTS queue_pauses (
	queue     TEXT PRIMARY KEY,
	paused_at INTEGER NOT NULL
);
`

// migration19 adds extra concurrency keys: a per-step list of named keys,
// each with its own limit, so a task can be gated on several keys at once and
// share a named budget with other tasks.
const migration19 = `
CREATE TABLE IF NOT EXISTS step_keys (
	step_id   TEXT NOT NULL REFERENCES steps(id) ON DELETE CASCADE,
	name      TEXT NOT NULL,
	value     TEXT NOT NULL,
	key_limit BIGINT NOT NULL,
	PRIMARY KEY (step_id, name)
);
CREATE INDEX IF NOT EXISTS idx_step_keys_value ON step_keys (name, value);
`

// migration18 adds ephemeral runs: persisted while in flight, deleted on
// terminal and never recovered.
const migration18 = `
ALTER TABLE runs ADD COLUMN ephemeral BIGINT NOT NULL DEFAULT 0;
`

// migration17 adds sequences: a per-run sequence key and insertion-order
// value (denormalized to steps) plus the counter that allocates it.
const migration17 = `
ALTER TABLE runs ADD COLUMN sequence_key TEXT NOT NULL DEFAULT '';
ALTER TABLE runs ADD COLUMN seq BIGINT NOT NULL DEFAULT 0;
ALTER TABLE steps ADD COLUMN sequence_key TEXT NOT NULL DEFAULT '';
ALTER TABLE steps ADD COLUMN seq BIGINT NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_steps_sequence ON steps (sequence_key, seq);
CREATE TABLE IF NOT EXISTS counters (
	name TEXT PRIMARY KEY,
	next BIGINT NOT NULL
);
INSERT INTO counters (name, next) VALUES ('run', 0) ON CONFLICT (name) DO NOTHING;
`

// migration16 adds the dead-letter marker: set when an opted-in task's run
// exhausts its retries.
const migration16 = `
ALTER TABLE runs ADD COLUMN dead_lettered_at BIGINT NOT NULL DEFAULT 0;
`

// migration15 adds run-level pause: a run paused by the user holds its claims
// until resumed (paused_at is informational).
const migration15 = `
ALTER TABLE runs ADD COLUMN paused_at BIGINT NOT NULL DEFAULT 0;
`

var sqliteMigrations = []driver.Migration{
	{Version: 1, SQL: schema},
	{Version: 2, SQL: migration2},
	{Version: 3, SQL: migration3},
	{Version: 4, SQL: migration4},
	{Version: 5, SQL: migration5},
	{Version: 6, SQL: migration6},
	{Version: 7, SQL: migration7},
	{Version: 8, SQL: migration8},
	{Version: 9, SQL: migration9},
	{Version: 10, SQL: migration10},
	{Version: 11, SQL: migration11},
	{Version: 12, SQL: migration12},
	{Version: 13, SQL: migration13},
	{Version: 14, SQL: migration14},
	{Version: 15, SQL: migration15},
	{Version: 16, SQL: migration16},
	{Version: 17, SQL: migration17},
	{Version: 18, SQL: migration18},
	{Version: 19, SQL: migration19},
}

// init validates the built-in migration list's invariant before any Open can
// rely on it; drivers validate their own lists the same way.
func init() {
	if err := driver.ValidateMigrations(sqliteMigrations); err != nil {
		panic(err)
	}
}

// migrate brings the database to the latest schema version. The
// schema_migrations table is created first so every later migration can be
// recorded in the same transaction as its DDL. Migration 1 is all
// CREATE-IF-NOT-EXISTS, so a v0.1 File database (tables present, no version
// row) baselines to version 1 without touching existing data.
func (s *Store) migrate(ctx context.Context) error {
	ms := s.be.Migrations()
	// BIGINT, not INTEGER: applied_at holds unix nanoseconds, which overflow
	// Postgres' 32-bit INTEGER. SQLite's INTEGER affinity accepts BIGINT.
	if _, err := s.write.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version BIGINT PRIMARY KEY,
		applied_at BIGINT NOT NULL
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
	// ms is ordered ascending, so the last entry is the max.
	if max := ms[len(ms)-1].Version; version > max {
		return fmt.Errorf("quacker: database schema version %d is newer than this build supports (max %d)", version, max)
	}
	if version >= ms[len(ms)-1].Version {
		return nil
	}
	// A session-scoped lock (MySQL) covers the whole run; otherwise each
	// per-version transaction takes its own lock (Postgres advisory, SQLite
	// no-op).
	var release func()
	sessionLocked := false
	if ml, ok := s.be.(driver.MigrateLocker); ok {
		rel, err := ml.LockMigration(ctx, s.write.DB)
		if err != nil {
			return fmt.Errorf("quacker: migrate lock: %w", err)
		}
		release = rel
		sessionLocked = true
		defer func() {
			if release != nil {
				release()
			}
		}()
	}
	for _, m := range ms {
		if m.Version <= version {
			continue
		}
		tx, err := s.beginTx(ctx)
		if err != nil {
			return fmt.Errorf("quacker: migrate: %w", err)
		}
		// Serialize concurrent migrations across processes (no-op for SQLite)
		// so two nodes booting at once can't race DDL.
		if !sessionLocked {
			if err := s.be.MigrateLock(ctx, tx.Tx); err != nil {
				tx.Rollback()
				return fmt.Errorf("quacker: migrate lock: %w", err)
			}
		}
		for _, stmt := range splitStatements(m.SQL) {
			if _, err := tx.exec(ctx, stmt); err != nil {
				tx.Rollback()
				return fmt.Errorf("quacker: migrate to version %d: %w", m.Version, err)
			}
		}
		if _, err := tx.exec(ctx,
			`INSERT INTO schema_migrations (version, applied_at) VALUES (?,?)`, m.Version, nowUnix()); err != nil {
			tx.Rollback()
			return fmt.Errorf("quacker: record migration %d: %w", m.Version, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("quacker: migrate to version %d: %w", m.Version, err)
		}
	}
	return nil
}

// splitStatements breaks a migration script into individual statements on
// ";", so each executes separately — Postgres' extended protocol rejects a
// multi-statement Exec. Migration SQL must not contain a semicolon inside a
// string literal (none does).
func splitStatements(script string) []string {
	var out []string
	for _, s := range strings.Split(script, ";") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// recoverInterrupted applies boot-time recovery for File mode: runs and steps
// left RUNNING or INTERRUPTED by a previous process are re-queued (keep=true)
// or failed (keep=false). Attempt counts are preserved; completed steps are
// never touched.
func (s *Store) recoverInterrupted(ctx context.Context, keep bool) error {
	tx, err := s.beginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Ephemeral runs are never recovered: this process owns the file, so any
	// left by a previous process are discarded.
	if rows, err := tx.query(ctx, `SELECT id FROM runs WHERE ephemeral=1`); err != nil {
		return err
	} else {
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, id := range ids {
			if err := deleteEphemeralRunTx(ctx, tx, id); err != nil {
				return err
			}
		}
	}

	now := nowUnix()
	for _, table := range []string{"steps", "runs"} {
		statuses := []string{StatusRunning, StatusInterrupted}
		extra := ""
		if table == "steps" {
			// Clearing the wait fields keeps a failed run's steps from
			// showing a stale wait/resume.
			extra = ", resume_at = 0, wait_kind = '', wait_event = ''"
			if !keep {
				// A run failed on restart must not orphan a SUSPENDED step;
				// fail it with the run. (On requeue, SUSPENDED is left intact
				// so it resumes when due.)
				statuses = append(statuses, StatusSuspended)
			}
		}
		if table == "runs" && !keep {
			// A failed run frees its unique key; a requeued one keeps it.
			extra = ", unique_key = NULL"
		}
		// keep and !keep are distinct statements rather than one CASE: a CASE
		// with an integer ELSE makes Postgres infer a 32-bit type for the
		// unix-nanos parameter, which overflows.
		var (
			q    string
			args []any
		)
		if keep {
			q = fmt.Sprintf(`UPDATE %s SET status = ?, run_at = ?%s WHERE status IN (%s)`,
				table, extra, placeholders(len(statuses)))
			args = []any{StatusQueued, now}
		} else {
			q = fmt.Sprintf(`UPDATE %s SET status = ?, run_at = ?, error = ?, completed_at = ?%s WHERE status IN (%s)`,
				table, extra, placeholders(len(statuses)))
			args = []any{StatusFailed, now, "interrupted by restart", now}
		}
		for _, st := range statuses {
			args = append(args, st)
		}
		if _, err := tx.exec(ctx, q, args...); err != nil {
			return fmt.Errorf("quacker: recover %s: %w", table, err)
		}
	}
	return tx.Commit()
}
