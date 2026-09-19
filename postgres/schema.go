package postgres

// pgSchema is the initial Postgres schema (version 1). Version numbers are
// per-dialect: a database is never both SQLite and Postgres, so Postgres grows
// its own migration list from here. Note BIGINT for every counter/timestamp —
// unix nanoseconds overflow Postgres' 32-bit INTEGER — and BOOLEAN for the
// journal flags. JSON (depends_on/labels) is stored as TEXT and cast to jsonb
// in the label gate.
const pgSchema = `
CREATE TABLE IF NOT EXISTS runs (
	id              TEXT PRIMARY KEY,
	workflow        TEXT NOT NULL,
	kind            TEXT NOT NULL DEFAULT 'task',
	status          TEXT NOT NULL,
	queue           TEXT NOT NULL DEFAULT 'default',
	priority        BIGINT NOT NULL DEFAULT 0,
	input           BYTEA,
	output          BYTEA,
	error           TEXT NOT NULL DEFAULT '',
	attempts        BIGINT NOT NULL DEFAULT 0,
	max_attempts    BIGINT NOT NULL DEFAULT 1,
	run_at          BIGINT NOT NULL,
	created_at      BIGINT NOT NULL,
	started_at      BIGINT NOT NULL DEFAULT 0,
	completed_at    BIGINT NOT NULL DEFAULT 0,
	concurrency_key TEXT NOT NULL DEFAULT '',
	parent_id       TEXT NOT NULL DEFAULT '',
	trace_parent    TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_runs_claim ON runs (status, run_at);
CREATE INDEX IF NOT EXISTS idx_runs_list  ON runs (workflow, created_at);
CREATE INDEX IF NOT EXISTS idx_runs_purge ON runs (status, completed_at);
CREATE INDEX IF NOT EXISTS idx_runs_parent ON runs (parent_id, created_at);

CREATE TABLE IF NOT EXISTS steps (
	id              TEXT PRIMARY KEY,
	run_id          TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
	name            TEXT NOT NULL,
	task            TEXT NOT NULL DEFAULT '',
	ord             BIGINT NOT NULL DEFAULT 0,
	status          TEXT NOT NULL,
	depends_on      TEXT NOT NULL DEFAULT '[]',
	queue           TEXT NOT NULL DEFAULT 'default',
	priority        BIGINT NOT NULL DEFAULT 0,
	input           BYTEA,
	output          BYTEA,
	error           TEXT NOT NULL DEFAULT '',
	attempts        BIGINT NOT NULL DEFAULT 0,
	max_attempts    BIGINT NOT NULL DEFAULT 1,
	timeout_ns      BIGINT NOT NULL DEFAULT 0,
	run_at          BIGINT NOT NULL,
	created_at      BIGINT NOT NULL,
	started_at      BIGINT NOT NULL DEFAULT 0,
	completed_at    BIGINT NOT NULL DEFAULT 0,
	concurrency_key TEXT NOT NULL DEFAULT '',
	key_limit       BIGINT NOT NULL DEFAULT 0,
	claimed_at      BIGINT NOT NULL DEFAULT 0,
	resume_at       BIGINT NOT NULL DEFAULT 0,
	wait_kind       TEXT NOT NULL DEFAULT '',
	wait_event      TEXT NOT NULL DEFAULT '',
	labels          TEXT NOT NULL DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS idx_steps_claim   ON steps (status, run_at);
CREATE INDEX IF NOT EXISTS idx_steps_run     ON steps (run_id, ord);
CREATE INDEX IF NOT EXISTS idx_steps_key     ON steps (concurrency_key, status);
CREATE INDEX IF NOT EXISTS idx_steps_claimed ON steps (queue, claimed_at);
CREATE INDEX IF NOT EXISTS idx_steps_resume  ON steps (status, resume_at);

CREATE TABLE IF NOT EXISTS crons (
	id         TEXT PRIMARY KEY,
	name       TEXT NOT NULL UNIQUE,
	spec       TEXT NOT NULL,
	task       TEXT NOT NULL,
	input      BYTEA,
	next_at    BIGINT NOT NULL,
	created_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
	seq        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	name       TEXT NOT NULL,
	payload    BYTEA,
	created_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_name ON events (name, seq);

CREATE TABLE IF NOT EXISTS event_subscriptions (
	id         TEXT PRIMARY KEY,
	event      TEXT NOT NULL,
	task       TEXT NOT NULL,
	created_at BIGINT NOT NULL,
	UNIQUE (event, task)
);

CREATE TABLE IF NOT EXISTS logs (
	seq     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	run_id  TEXT NOT NULL,
	step    TEXT NOT NULL DEFAULT '',
	at      BIGINT NOT NULL,
	level   TEXT NOT NULL DEFAULT 'INFO',
	message TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_logs_run ON logs (run_id, seq);

CREATE TABLE IF NOT EXISTS step_journal (
	step_id   TEXT NOT NULL REFERENCES steps(id) ON DELETE CASCADE,
	idx       BIGINT NOT NULL,
	kind      TEXT NOT NULL,
	key       TEXT NOT NULL DEFAULT '',
	event     TEXT NOT NULL DEFAULT '',
	wake_at   BIGINT NOT NULL DEFAULT 0,
	deadline  BIGINT NOT NULL DEFAULT 0,
	payload   BYTEA,
	result    BYTEA,
	err       TEXT NOT NULL DEFAULT '',
	done      BOOLEAN NOT NULL DEFAULT FALSE,
	timed_out BOOLEAN NOT NULL DEFAULT FALSE,
	PRIMARY KEY (step_id, idx)
);
CREATE INDEX IF NOT EXISTS idx_journal_wait ON step_journal (kind, done, event);
`

// pgMigration3 adds indexes for the DAG completion hot path.
const pgMigration3 = `
CREATE INDEX IF NOT EXISTS idx_steps_run_status ON steps (run_id, status);
CREATE INDEX IF NOT EXISTS idx_steps_run_name   ON steps (run_id, name);
`

// pgMigration2 adds multi-instance step leases.
const pgMigration2 = `
ALTER TABLE steps ADD COLUMN worker_id TEXT NOT NULL DEFAULT '';
ALTER TABLE steps ADD COLUMN lease_expires_at BIGINT NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_steps_lease  ON steps (status, lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_steps_worker ON steps (worker_id, status);
`

// pgMigration4 renames step_journal.key to wkey: KEY is a reserved word in
// MySQL, and the query layer must reference the column unquoted on every
// dialect.
const pgMigration4 = `
ALTER TABLE step_journal RENAME COLUMN key TO wkey;
`

// pgMigration5 adds unique jobs: a nullable unique_key (NULL for non-unique
// runs, which unique indexes ignore) and a unique index per (workflow, key).
const pgMigration5 = `
ALTER TABLE runs ADD COLUMN unique_key TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS uq_runs_unique ON runs (workflow, unique_key);
`

// pgMigration6 adds queue pauses.
const pgMigration6 = `
CREATE TABLE IF NOT EXISTS queue_pauses (
	queue     TEXT PRIMARY KEY,
	paused_at BIGINT NOT NULL
);
`
