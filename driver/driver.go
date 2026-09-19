// Package driver defines quacker's storage-driver contract.
//
// A driver is a SQL dialect. It supplies connection setup, placeholder syntax,
// DDL/migrations, and the handful of SQL constructs that differ between
// databases — nothing else. It does not reimplement quacker's transactional
// behavior: claim key/rate/label gating, DAG completion, event delivery, step
// leases, and cron single-fire all live in the engine's single query layer,
// which drives every dialect through this interface.
//
// PostgreSQL and MySQL/MariaDB ship as drivers in this module. A third-party
// driver lives in its own package, registers from init, and is selected by
// name:
//
//	package mydriver // implements driver.Backend
//	func init() { driver.RegisterBackend(myBackend{}) }
//
//	import _ "example.com/my/mydriver"
//	q, _ := quacker.Open(quacker.WithStorage(quacker.Driver("mydb", dsn)))
//
// A driver's DDL must create the tables and columns the query layer reads;
// postgres/ and mysql/ are the reference implementations.
package driver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// Mode selects how the built-in SQLite driver stores its database. Custom
// drivers ignore it and select on Config.Driver or Config.DSN.
type Mode int

const (
	// ModeMemory keeps all state in a pure in-memory SQLite database
	// (shared-cache). Nothing touches the filesystem.
	ModeMemory Mode = iota
	// ModeEphemeral uses a temporary file with WAL journaling and
	// synchronous=OFF, deleted on Close.
	ModeEphemeral
	// ModeFile persists to a user-supplied path with WAL journaling.
	ModeFile
)

// Config configures a driver's connection and describes the requested storage.
type Config struct {
	// Mode is the built-in SQLite storage mode; ignored by custom drivers.
	Mode Mode
	// Path is the SQLite database file path; required for ModeFile.
	Path string
	// DSN is the driver-specific connection string.
	DSN string
	// DB, when non-nil, is an existing pool owned by the caller. Drivers with
	// a single-pool model (Postgres, MySQL) reuse it instead of dialing DSN,
	// and cleanup must leave it open.
	DB *sql.DB
	// Driver is the registered driver name ("sqlite", "postgres", "mysql",
	// ...). Empty selects the built-in SQLite driver from Mode.
	Driver string
	// RecoverRunningOnBoot (single-process drivers only): when true, runs left
	// RUNNING or INTERRUPTED by a previous process are re-queued on Open; when
	// false they are marked FAILED.
	RecoverRunningOnBoot bool
	// CheckpointInterval is how often WAL drivers run a passive checkpoint to
	// bound WAL growth. <=0 uses the driver's default; values below 10ms are
	// clamped to 10ms.
	CheckpointInterval time.Duration
}

// Migration is one schema change applied atomically with its version row.
// Migrations run in list order; each applies only when its version is ahead of
// the database's recorded version. Version numbers are per-dialect.
type Migration struct {
	Version int
	SQL     string
}

// Backend is the dialect-specific half of the store: connection setup, SQL
// placeholder syntax, DDL, and the few dialect-only operations. Register one
// with RegisterBackend from a package init.
type Backend interface {
	Name() string
	// OpenPools establishes the writer and reader pools plus any dialect
	// resources (file lock, keeper pool). The returned cleanup releases them
	// in the correct order, and must not close a caller-owned Config.DB.
	OpenPools(ctx context.Context, cfg Config) (write, read *sql.DB, cleanup func() error, err error)
	// Rebind rewrites '?' placeholders to the dialect's form (identity for
	// SQLite and MySQL, '$n' for Postgres).
	Rebind(query string) string
	// Migrations returns the ordered DDL for this dialect. Version numbers are
	// per-dialect (a database is never more than one dialect).
	Migrations() []Migration
	// MigrateLock serializes concurrent migrations across processes. It runs
	// inside each per-version transaction (no-op when the database is private
	// to one process).
	MigrateLock(ctx context.Context, tx *sql.Tx) error
	// ClaimLock serializes claim transactions across processes. It runs at the
	// start of ClaimDueMulti (no-op for single-process SQLite).
	ClaimLock(ctx context.Context, tx *sql.Tx) error
	// RunLock locks a run row for the duration of a DAG-mutating transaction
	// (complete/fail/cancel), serializing per-run decisions across processes
	// (no-op for single-process SQLite).
	RunLock(ctx context.Context, tx *sql.Tx, runID string) error
	// SupportsLeases reports whether step leases are meaningful for this
	// dialect (true for networked databases; false for single-process SQLite).
	SupportsLeases() bool
	// SupportsCheckpoint reports whether the WAL checkpoint loop applies.
	SupportsCheckpoint(cfg Config) bool
	// RecoverOnBoot reports whether boot-time recovery of RUNNING rows applies.
	RecoverOnBoot(cfg Config) bool
	// KeyGate is the per-key concurrency predicate (no placeholders) used on
	// the claim path: it admits a step only while fewer than key_limit RUNNING
	// steps share its concurrency_key. SQLite/Postgres use the correlated form
	// from CorrelatedKeyGate; MySQL must use a derived-table form because it
	// forbids reading the table being updated in a subquery (error 1093).
	KeyGate() string
	// LabelGate is the SQL predicate (containing one '?' for the worker-labels
	// JSON) admitting a step only when its labels are a subset of the worker's.
	LabelGate() string
	// BlockedDependentsSQL returns a statement ('?' placeholders: runID,
	// blockedStatus, dependency name) selecting the BLOCKED steps of a run
	// that depend on the named step, returning id, name, depends_on. It lets
	// completion inspect only the direct dependents of the step that finished
	// rather than every blocked step in the run.
	BlockedDependentsSQL() string
	// UpsertSQL returns an INSERT that updates the given columns when the
	// conflict target already exists. insertCols are the destination columns,
	// conflictCols the unique key, updateCols the columns to overwrite on
	// conflict; the returned statement takes one '?' per insert column.
	UpsertSQL(table string, insertCols, conflictCols, updateCols []string) string
}

var backends = map[string]Backend{}

// RegisterBackend installs a driver under its Name(). Driver packages call it
// from init; the built-in SQLite driver registers itself in this package.
func RegisterBackend(b Backend) { backends[b.Name()] = b }

// Lookup returns the driver registered under name.
func Lookup(name string) (Backend, bool) {
	b, ok := backends[name]
	return b, ok
}

// CorrelatedKeyGate is the default per-key concurrency predicate. It counts
// RUNNING siblings of the step with the same concurrency_key, referencing the
// enclosing statement's steps row. The statement must expose `steps` (the
// candidate SELECT and the claim UPDATE both do).
func CorrelatedKeyGate() string {
	return `(concurrency_key = '' OR key_limit <= 0 OR
	(SELECT COUNT(*) FROM steps r
		WHERE r.concurrency_key = steps.concurrency_key
		AND r.status = 'RUNNING') < steps.key_limit)`
}

// UniqueViolationer is an optional Backend extension: it reports whether an
// error is a unique-constraint violation, so the engine can resolve unique-job
// conflicts. Drivers that do not implement it fall back to matching the error
// text for "unique"/"duplicate".
type UniqueViolationer interface {
	IsUniqueViolation(err error) bool
}

// IsUniqueViolation reports whether err is a unique-constraint violation from
// be, using its UniqueViolationer if implemented and a text heuristic
// otherwise.
func IsUniqueViolation(be Backend, err error) bool {
	if err == nil {
		return false
	}
	if u, ok := be.(UniqueViolationer); ok {
		return u.IsUniqueViolation(err)
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "unique") || strings.Contains(s, "duplicate")
}

// MigrateLocker is an optional Backend extension for drivers whose migration
// lock is session-scoped (MySQL's GET_LOCK) rather than transaction-scoped.
// When a driver implements it, migrate acquires the lock for the whole
// migration run through LockMigration and calls the returned release once at
// the end (even on error); MigrateLock is then unused. Drivers that do not
// implement it keep using the per-transaction MigrateLock.
type MigrateLocker interface {
	LockMigration(ctx context.Context, db *sql.DB) (release func(), err error)
}

// OnConflictUpsert builds an ON CONFLICT (conflictCols) DO UPDATE upsert, the
// syntax shared by SQLite and Postgres (both accept excluded.<col>). The
// statement uses '?' placeholders, one per insert column.
func OnConflictUpsert(table string, insertCols, conflictCols, updateCols []string) string {
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(table)
	b.WriteString(" (")
	b.WriteString(strings.Join(insertCols, ", "))
	b.WriteString(") VALUES (")
	b.WriteString(questionMarks(len(insertCols)))
	b.WriteString(") ON CONFLICT (")
	b.WriteString(strings.Join(conflictCols, ", "))
	b.WriteString(") DO UPDATE SET ")
	assignments(&b, updateCols, func(c string) string { return "excluded." + c })
	return b.String()
}

// DuplicateKeyUpsert builds a MySQL/MariaDB INSERT ... ON DUPLICATE KEY UPDATE
// upsert. The conflict target is implicit: the table's unique/primary keys.
// The statement uses '?' placeholders, one per insert column.
func DuplicateKeyUpsert(table string, insertCols, updateCols []string) string {
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(table)
	b.WriteString(" (")
	b.WriteString(strings.Join(insertCols, ", "))
	b.WriteString(") VALUES (")
	b.WriteString(questionMarks(len(insertCols)))
	b.WriteString(") ON DUPLICATE KEY UPDATE ")
	assignments(&b, updateCols, func(c string) string { return "VALUES(" + c + ")" })
	return b.String()
}

func questionMarks(n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString("?")
	}
	return b.String()
}

func assignments(b *strings.Builder, cols []string, rhs func(string) string) {
	for i, c := range cols {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(c)
		b.WriteString("=")
		b.WriteString(rhs(c))
	}
}

// ValidateMigrations enforces the migration-list invariant every driver must
// satisfy: non-empty with strictly ascending versions. migrate assumes the
// last entry is the maximum and applies entries in list order, so a duplicate
// or out-of-order version would silently skip or misorder DDL.
func ValidateMigrations(ms []Migration) error {
	if len(ms) == 0 {
		return fmt.Errorf("quacker: driver: no schema migrations defined")
	}
	for i := 1; i < len(ms); i++ {
		if ms[i].Version <= ms[i-1].Version {
			return fmt.Errorf("quacker: driver: migrations out of order: version %d follows version %d",
				ms[i].Version, ms[i-1].Version)
		}
	}
	return nil
}
