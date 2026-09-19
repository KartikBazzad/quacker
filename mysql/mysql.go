// Package mysql registers the MySQL/MariaDB storage driver for quacker.
//
// Import it for its side effect before opening MySQL storage:
//
//	import (
//	    "github.com/kartikbazzad/quacker"
//	    _ "github.com/kartikbazzad/quacker/mysql"
//	)
//
//	q, err := quacker.Open(quacker.WithStorage(quacker.Driver("mysql", dsn)))
//
// The DSN is the go-sql-driver/mysql form, e.g.
// "user:pass@tcp(127.0.0.1:3306)/quacker?parseTime=true". It is multi-instance:
// step leases, a claim lock row, and per-run row locks let several engines
// share one database.
package mysql

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql" // registers the "mysql" database/sql driver

	"github.com/kartikbazzad/quacker/driver"
)

func init() { driver.RegisterBackend(myBackend{}) }

type myBackend struct{}

func (myBackend) Name() string { return "mysql" }

// Rebind is identity: MySQL uses '?' placeholders like SQLite.
func (myBackend) Rebind(q string) string { return q }

func (myBackend) Migrations() []driver.Migration {
	return []driver.Migration{{Version: 1, SQL: mySchema}}
}

// MigrateLock is unused: the MySQL driver implements LockMigration instead,
// because GET_LOCK is session-scoped, not transaction-scoped.
func (myBackend) MigrateLock(context.Context, *sql.Tx) error { return nil }

// LockMigration takes a named session lock for the whole migration run so two
// nodes booting at once cannot race DDL. GET_LOCK is released at the end (or
// if the holding connection drops).
func (myBackend) LockMigration(ctx context.Context, db *sql.DB) (func(), error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, `SELECT GET_LOCK(?, ?)`, migrateLockName, lockTimeoutSeconds).Scan(&got); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("quacker: mysql migrate lock: %w", err)
	}
	if !got.Valid || got.Int64 != 1 {
		_ = conn.Close()
		return nil, fmt.Errorf("quacker: mysql migrate lock: timed out after %ds", lockTimeoutSeconds)
	}
	return func() {
		_, _ = conn.ExecContext(context.Background(), `SELECT RELEASE_LOCK(?)`, migrateLockName)
		_ = conn.Close()
	}, nil
}

// ClaimLock serializes claim transactions cluster-wide by locking a dedicated
// row FOR UPDATE, standing in for Postgres' transaction-scoped advisory lock.
// With the counting gates evaluated inside the transaction, this makes them
// atomic across nodes.
func (myBackend) ClaimLock(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `SELECT 1 FROM locks WHERE name='claim' FOR UPDATE`)
	return err
}

// RunLock locks a run row for the duration of a DAG-mutating transaction,
// serializing per-run decisions across nodes.
func (myBackend) RunLock(ctx context.Context, tx *sql.Tx, runID string) error {
	_, err := tx.ExecContext(ctx, `SELECT 1 FROM runs WHERE id=? FOR UPDATE`, runID)
	return err
}

// SupportsLeases is true: several engines may share the database, so worker
// identity + leases recover a crashed node's work.
func (myBackend) SupportsLeases() bool { return true }

func (myBackend) SupportsCheckpoint(driver.Config) bool { return false }

// RecoverOnBoot is false: re-queueing every RUNNING row would steal a peer
// node's in-flight work. The lease reaper recovers crashed nodes instead.
func (myBackend) RecoverOnBoot(driver.Config) bool { return false }

// KeyGate uses a derived table rather than a directly correlated subquery:
// MySQL rejects reading the table being updated in a subquery (error 1093), and
// routing the count through a derived table satisfies the parser. The
// optimizer then *merges* the derived table into `Covering index lookup on
// steps using idx_steps_key (concurrency_key=…, status='RUNNING')`, so nothing
// is materialized in practice — the form is purely to get past the 1093 check.
// A GROUP BY/count-per-key variant would block the merge, force materialization
// of the whole RUNNING set, and measure 2-3x slower; keep this shape.
func (myBackend) KeyGate() string {
	return `(concurrency_key = '' OR key_limit <= 0 OR
	(SELECT COUNT(*) FROM (SELECT concurrency_key FROM steps WHERE status = 'RUNNING') AS r
		WHERE r.concurrency_key = steps.concurrency_key) < steps.key_limit)`
}

// LabelGate admits a step only when its labels array is a subset of the
// worker's: JSON_CONTAINS(worker, step) is true when the step's elements are
// all present in the worker's. MySQL/MariaDB parse the '?' string argument as
// JSON, and both sides are valid JSON arrays.
func (myBackend) LabelGate() string {
	return `JSON_CONTAINS(?, steps.labels)`
}

func (myBackend) BlockedDependentsSQL() string {
	return `SELECT id, name, depends_on FROM steps
		WHERE run_id=? AND status=?
		  AND JSON_CONTAINS(steps.depends_on, JSON_QUOTE(?))`
}

// UpsertSQL uses MySQL's INSERT ... ON DUPLICATE KEY UPDATE (the conflict
// target is the table's unique/primary key, so conflictCols is unused).
func (myBackend) UpsertSQL(table string, insertCols, conflictCols, updateCols []string) string {
	return driver.DuplicateKeyUpsert(table, insertCols, updateCols)
}

// OpenPools opens a single pool (InnoDB MVCC needs no separate read-only
// pool). A caller-owned Config.DB is reused and left open.
func (myBackend) OpenPools(ctx context.Context, cfg driver.Config) (w, r *sql.DB, cleanup func() error, err error) {
	if cfg.DB != nil {
		if err := cfg.DB.PingContext(ctx); err != nil {
			return nil, nil, nil, fmt.Errorf("quacker: external mysql pool: %w", err)
		}
		return cfg.DB, cfg.DB, nil, nil
	}
	if cfg.DSN == "" {
		return nil, nil, nil, fmt.Errorf("quacker: storage.Driver(\"mysql\", ...) requires a DSN or an *sql.DB")
	}
	db, err := sql.Open("mysql", cfg.DSN)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("quacker: open mysql: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, nil, nil, fmt.Errorf("quacker: ping mysql: %w", err)
	}
	return db, db, db.Close, nil
}

const (
	migrateLockName    = "quacker_migrate"
	lockTimeoutSeconds = 30
)
