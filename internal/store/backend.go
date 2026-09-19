package store

import (
	"context"
	"database/sql"
	"fmt"
)

// Backend is the dialect-specific half of the store: connection setup, SQL
// placeholder syntax, DDL, and the few dialect-only operations. It is an
// internal seam, not public API — driver packages (e.g. postgres) register a
// Backend from init, and users opt in with a blank import.
type Backend interface {
	Name() string
	// OpenPools establishes the writer and reader pools plus any dialect
	// resources (file lock, keeper pool). The returned cleanup releases them
	// in the correct order.
	OpenPools(ctx context.Context, cfg Config) (write, read *sql.DB, cleanup func() error, err error)
	// Rebind rewrites '?' placeholders to the dialect's form (identity for
	// SQLite, '$n' for Postgres).
	Rebind(query string) string
	// Migrations returns the ordered DDL for this dialect. Version numbers are
	// per-dialect (a database is never both SQLite and Postgres).
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
	// dialect (true for networked Postgres; false for single-process SQLite).
	SupportsLeases() bool
	// SupportsCheckpoint reports whether the WAL checkpoint loop applies.
	SupportsCheckpoint(cfg Config) bool
	// RecoverOnBoot reports whether boot-time recovery of RUNNING rows applies.
	RecoverOnBoot(cfg Config) bool
	// LabelGate is the SQL predicate (containing one '?' for the worker-labels
	// JSON) admitting a step only when its labels are a subset of the worker's.
	LabelGate() string
}

var backends = map[string]Backend{}

// RegisterBackend installs a dialect. Called by driver packages from init;
// SQLite registers itself in this package.
func RegisterBackend(b Backend) { backends[b.Name()] = b }

func backendFor(cfg Config) (Backend, error) {
	name := "sqlite"
	if cfg.Mode == ModePostgres {
		name = "postgres"
	}
	b, ok := backends[name]
	if !ok {
		return nil, fmt.Errorf("quacker: storage backend %q is not registered; import its driver package (e.g. _ %q)",
			name, "github.com/kartikbazzad/quacker/postgres")
	}
	return b, nil
}

// dbConn is a *sql.DB whose statements are rebound for the dialect, so the
// query layer keeps using '?' placeholders everywhere.
type dbConn struct {
	*sql.DB
	be Backend
}

func (c *dbConn) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return c.DB.ExecContext(ctx, c.be.Rebind(q), args...)
}

func (c *dbConn) QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	return c.DB.QueryContext(ctx, c.be.Rebind(q), args...)
}

func (c *dbConn) QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row {
	return c.DB.QueryRowContext(ctx, c.be.Rebind(q), args...)
}

// txn is a *sql.Tx whose statements are rebound for the dialect.
type txn struct {
	*sql.Tx
	be Backend
}

func (t *txn) exec(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return t.Tx.ExecContext(ctx, t.be.Rebind(q), args...)
}

func (t *txn) query(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	return t.Tx.QueryContext(ctx, t.be.Rebind(q), args...)
}

func (t *txn) queryRow(ctx context.Context, q string, args ...any) *sql.Row {
	return t.Tx.QueryRowContext(ctx, t.be.Rebind(q), args...)
}
