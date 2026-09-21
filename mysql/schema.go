package mysql

// mySchema is the complete MySQL/MariaDB schema at version 1. There are no
// incremental migrations: the MySQL driver shipped after the schema was
// complete, so a fresh database is created in one step. Column names and
// order match what the shared query layer reads; integer columns are BIGINT
// because unix-nanosecond timestamps overflow 32-bit INTEGER. Text columns
// that a default is never read from are NOT NULL without a default, because
// MySQL forbids defaults on TEXT/BLOB.
const mySchema = `
CREATE TABLE IF NOT EXISTS runs (
	id              VARCHAR(64) NOT NULL PRIMARY KEY,
	workflow        VARCHAR(255) NOT NULL,
	kind            VARCHAR(32) NOT NULL DEFAULT 'task',
	status          VARCHAR(32) NOT NULL,
	queue           VARCHAR(255) NOT NULL DEFAULT 'default',
	priority        BIGINT NOT NULL DEFAULT 0,
	input           LONGBLOB NULL,
	output          LONGBLOB NULL,
	error           LONGTEXT NOT NULL,
	attempts        BIGINT NOT NULL DEFAULT 0,
	max_attempts    BIGINT NOT NULL DEFAULT 1,
	run_at          BIGINT NOT NULL,
	created_at      BIGINT NOT NULL,
	started_at      BIGINT NOT NULL DEFAULT 0,
	completed_at    BIGINT NOT NULL DEFAULT 0,
	concurrency_key VARCHAR(255) NOT NULL DEFAULT '',
	parent_id       VARCHAR(64) NOT NULL DEFAULT '',
	trace_parent    VARCHAR(255) NOT NULL DEFAULT '',
	KEY idx_runs_claim  (status, run_at),
	KEY idx_runs_list   (workflow, created_at),
	KEY idx_runs_parent (parent_id, created_at),
	KEY idx_runs_purge  (status, completed_at)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS steps (
	id              VARCHAR(64) NOT NULL PRIMARY KEY,
	run_id          VARCHAR(64) NOT NULL,
	name            VARCHAR(255) NOT NULL,
	task            VARCHAR(255) NOT NULL DEFAULT '',
	ord             BIGINT NOT NULL DEFAULT 0,
	status          VARCHAR(32) NOT NULL,
	depends_on      JSON NOT NULL,
	queue           VARCHAR(255) NOT NULL DEFAULT 'default',
	priority        BIGINT NOT NULL DEFAULT 0,
	input           LONGBLOB NULL,
	output          LONGBLOB NULL,
	error           LONGTEXT NOT NULL,
	attempts        BIGINT NOT NULL DEFAULT 0,
	max_attempts    BIGINT NOT NULL DEFAULT 1,
	timeout_ns      BIGINT NOT NULL DEFAULT 0,
	run_at          BIGINT NOT NULL,
	created_at      BIGINT NOT NULL,
	started_at      BIGINT NOT NULL DEFAULT 0,
	completed_at    BIGINT NOT NULL DEFAULT 0,
	concurrency_key VARCHAR(255) NOT NULL DEFAULT '',
	key_limit       BIGINT NOT NULL DEFAULT 0,
	claimed_at      BIGINT NOT NULL DEFAULT 0,
	labels          JSON NOT NULL,
	resume_at       BIGINT NOT NULL DEFAULT 0,
	wait_kind       VARCHAR(32) NOT NULL DEFAULT '',
	wait_event      VARCHAR(255) NOT NULL DEFAULT '',
	worker_id       VARCHAR(128) NOT NULL DEFAULT '',
	lease_expires_at BIGINT NOT NULL DEFAULT 0,
	CONSTRAINT fk_steps_run FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE,
	KEY idx_steps_claim      (status, run_at),
	KEY idx_steps_run        (run_id, ord),
	KEY idx_steps_key        (concurrency_key, status),
	KEY idx_steps_claimed    (queue, claimed_at),
	KEY idx_steps_resume     (status, resume_at),
	KEY idx_steps_lease      (status, lease_expires_at),
	KEY idx_steps_worker     (worker_id, status),
	KEY idx_steps_run_status (run_id, status),
	KEY idx_steps_run_name   (run_id, name)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS crons (
	id         VARCHAR(64) NOT NULL PRIMARY KEY,
	name       VARCHAR(255) NOT NULL,
	spec       TEXT NOT NULL,
	task       VARCHAR(255) NOT NULL,
	input      LONGBLOB NULL,
	next_at    BIGINT NOT NULL,
	created_at BIGINT NOT NULL,
	UNIQUE KEY uq_crons_name (name)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS logs (
	seq     BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	run_id  VARCHAR(64) NOT NULL,
	step    VARCHAR(255) NOT NULL DEFAULT '',
	at      BIGINT NOT NULL,
	level   VARCHAR(16) NOT NULL DEFAULT 'INFO',
	message LONGTEXT NOT NULL,
	KEY idx_logs_run (run_id, seq)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS events (
	seq        BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	name       VARCHAR(255) NOT NULL,
	payload    LONGBLOB NULL,
	created_at BIGINT NOT NULL,
	KEY idx_events_name (name, seq)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS event_subscriptions (
	id         VARCHAR(255) NOT NULL PRIMARY KEY,
	event      VARCHAR(255) NOT NULL,
	task       VARCHAR(255) NOT NULL,
	created_at BIGINT NOT NULL,
	UNIQUE KEY uq_event_task (event, task)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS step_journal (
	step_id   VARCHAR(64) NOT NULL,
	idx       BIGINT NOT NULL,
	kind      VARCHAR(32) NOT NULL,
	wkey      VARCHAR(255) NOT NULL DEFAULT '',
	event     VARCHAR(255) NOT NULL DEFAULT '',
	wake_at   BIGINT NOT NULL DEFAULT 0,
	deadline  BIGINT NOT NULL DEFAULT 0,
	payload   LONGBLOB NULL,
	result    LONGBLOB NULL,
	err       LONGTEXT NOT NULL,
	done      TINYINT(1) NOT NULL DEFAULT 0,
	timed_out TINYINT(1) NOT NULL DEFAULT 0,
	PRIMARY KEY (step_id, idx),
	KEY idx_journal_wait (kind, done, event),
	CONSTRAINT fk_journal_step FOREIGN KEY (step_id) REFERENCES steps(id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS locks (
	name VARCHAR(64) NOT NULL PRIMARY KEY
) ENGINE=InnoDB;

INSERT IGNORE INTO locks (name) VALUES ('claim');
`

// myMigration2 adds unique jobs: a nullable unique_key (NULL for non-unique
// runs, which unique indexes ignore) and a unique index per (workflow, key).
const myMigration2 = `
ALTER TABLE runs ADD COLUMN unique_key VARCHAR(255) NULL;
CREATE UNIQUE INDEX uq_runs_unique ON runs (workflow, unique_key);
`

// myMigration3 adds queue pauses.
const myMigration3 = `
CREATE TABLE IF NOT EXISTS queue_pauses (
	queue     VARCHAR(255) NOT NULL PRIMARY KEY,
	paused_at BIGINT NOT NULL
) ENGINE=InnoDB;
`

// myMigration4 adds run-level pause.
const myMigration4 = `
ALTER TABLE runs ADD COLUMN paused_at BIGINT NOT NULL DEFAULT 0;
`

// myMigration5 adds the dead-letter marker.
const myMigration5 = `
ALTER TABLE runs ADD COLUMN dead_lettered_at BIGINT NOT NULL DEFAULT 0;
`

// myMigration6 adds sequences.
const myMigration6 = `
ALTER TABLE runs ADD COLUMN sequence_key VARCHAR(255) NOT NULL DEFAULT '';
ALTER TABLE runs ADD COLUMN seq BIGINT NOT NULL DEFAULT 0;
ALTER TABLE steps ADD COLUMN sequence_key VARCHAR(255) NOT NULL DEFAULT '';
ALTER TABLE steps ADD COLUMN seq BIGINT NOT NULL DEFAULT 0;
CREATE INDEX idx_steps_sequence ON steps (sequence_key, seq);
CREATE TABLE IF NOT EXISTS counters (
	name VARCHAR(64) NOT NULL PRIMARY KEY,
	next BIGINT NOT NULL
) ENGINE=InnoDB;
INSERT IGNORE INTO counters (name, next) VALUES ('run', 0);
`

// myMigration7 adds ephemeral runs.
const myMigration7 = `
ALTER TABLE runs ADD COLUMN ephemeral BIGINT NOT NULL DEFAULT 0;
`

// myMigration8 adds extra concurrency keys.
const myMigration8 = `
CREATE TABLE IF NOT EXISTS step_keys (
	step_id   VARCHAR(64) NOT NULL,
	name      VARCHAR(255) NOT NULL,
	value     VARCHAR(255) NOT NULL,
	key_limit BIGINT NOT NULL,
	PRIMARY KEY (step_id, name),
	KEY idx_step_keys_value (name, value),
	CONSTRAINT fk_step_keys_step FOREIGN KEY (step_id) REFERENCES steps(id) ON DELETE CASCADE
) ENGINE=InnoDB;
`

// myMigration9 records the step that spawned a child run.
const myMigration9 = `
ALTER TABLE runs ADD COLUMN parent_step VARCHAR(255) NOT NULL DEFAULT '';
`

// myMigration10 stores a grouped workflow's group structure (name, group-level
// deps, member steps) as JSON for DAG rendering. NULL rather than NOT NULL
// because MySQL forbids defaults on TEXT columns.
const myMigration10 = `
ALTER TABLE runs ADD COLUMN groups_json LONGTEXT NULL;
`

// myMigration11 adds a queue-leading index matching the claim query's ORDER BY
// (priority DESC, run_at, ord), so the claim scan stops at LIMIT instead of
// collecting every due step and sorting it. MySQL 8 supports the DESC index
// key part; on older versions it is parsed and ignored, so the plan may still
// sort but nothing breaks.
const myMigration11 = `
CREATE INDEX idx_steps_queue_claim ON steps (queue, priority DESC, run_at, ord);
`
