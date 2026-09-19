package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite" // registers the pure-Go "sqlite" driver

	"github.com/kartikbazzad/quacker/driver"
)

func init() { driver.RegisterBackend(sqliteBackend{}) }

// sqliteBackend is quacker's built-in, default backend: pure-Go SQLite with a
// single writer connection and a read-only reader pool.
type sqliteBackend struct{}

func (sqliteBackend) Name() string { return "sqlite" }

func (sqliteBackend) Rebind(q string) string { return q }

func (sqliteBackend) Migrations() []driver.Migration { return sqliteMigrations }

func (sqliteBackend) MigrateLock(context.Context, *sql.Tx) error { return nil }

func (sqliteBackend) ClaimLock(context.Context, *sql.Tx) error { return nil }

func (sqliteBackend) RunLock(context.Context, *sql.Tx, string) error { return nil }

// SupportsLeases is false: SQLite is single-process, so boot recovery and the
// single writer already serialize everything; leases would only risk
// requeueing a live long-running step.
func (sqliteBackend) SupportsLeases() bool { return false }

func (sqliteBackend) SupportsCheckpoint(cfg driver.Config) bool { return cfg.Mode != driver.ModeMemory }

func (sqliteBackend) RecoverOnBoot(cfg driver.Config) bool { return cfg.Mode == driver.ModeFile }

func (sqliteBackend) KeyGate() string { return driver.CorrelatedKeyGate() }

func (sqliteBackend) LabelGate() string {
	return `NOT EXISTS (
	SELECT 1 FROM json_each(steps.labels) AS l
	WHERE l.value NOT IN (SELECT value FROM json_each(?))
)`
}

func (sqliteBackend) BlockedDependentsSQL() string {
	return `SELECT id, name, depends_on FROM steps
		WHERE run_id=? AND status=?
		  AND EXISTS (SELECT 1 FROM json_each(steps.depends_on) AS d WHERE d.value=?)`
}

// UpsertSQL uses the SQLite/Postgres ON CONFLICT ... DO UPDATE syntax.
func (sqliteBackend) UpsertSQL(table string, insertCols, conflictCols, updateCols []string) string {
	return driver.OnConflictUpsert(table, insertCols, conflictCols, updateCols)
}

var memSerial atomic.Int64

// unique gives each in-memory database a distinct name so two quacker
// instances in one process don't share a cache.
// uniqueSerial gives each in-memory database a distinct name so two quacker
// instances in one process don't share a cache.
func uniqueSerial() int64 { return memSerial.Add(1) }

// dsn returns the writer DSN, reader DSN, and (for ModeEphemeral) the temp
// file path to remove on Close.
func sqliteDSN(c driver.Config) (wdsn, rdsn, tmpPath string, err error) {
	common := "_pragma=busy_timeout(10000)&_pragma=foreign_keys(1)"
	switch c.Mode {
	case driver.ModeMemory:
		// A named in-memory database with a shared cache so both pools see
		// the same data. journal_mode defaults to MEMORY there.
		// No _txlock=immediate here (unlike the file modes): on a shared
		// cache an immediate write txn's RESERVED lock makes same-cache
		// readers hit SQLITE_LOCKED, which bypasses busy_timeout. WAL/file
		// readers don't contend, so _txlock=immediate is used only there.
		name := fmt.Sprintf("quacker-%d-%d", os.Getpid(), uniqueSerial())
		dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared&%s", name, common)
		return dsn, dsn, "", nil
	case driver.ModeEphemeral:
		tmp, err := os.CreateTemp("", "quacker-*.db")
		if err != nil {
			return "", "", "", fmt.Errorf("quacker: create temp db: %w", err)
		}
		path := tmp.Name()
		if err := tmp.Close(); err != nil {
			return "", "", "", err
		}
		dsn := fmt.Sprintf("file:%s?%s&_pragma=journal_mode(WAL)&_pragma=synchronous(0)&_txlock=immediate", path, common)
		return dsn, dsn, path, nil
	case driver.ModeFile:
		if c.Path == "" {
			return "", "", "", fmt.Errorf("quacker: storage.File requires a path")
		}
		// The path is embedded into a "file:" DSN and SQLite parses it as
		// a URI: ?/#/& become DSN pragmas or a fragment, % is URI-decoded
		// ("dir/a%2fb.db" opens "dir/a/b.db"), and a leading "//" is
		// stripped as a URI authority ("//host/dir/x.db" opens
		// "/dir/x.db") — each makes SQLite open a different file than
		// the .quacker.lock sidecar guards, or injects options. (Windows
		// UNC paths use backslashes, so the "//" check doesn't touch
		// them.) A path cleaning to ":memory:" would silently swap
		// durable storage for an in-memory database behind a useless
		// on-disk sidecar. Control runes have no business in a path
		// either.
		if strings.ContainsAny(c.Path, "?&#%") ||
			strings.HasPrefix(c.Path, "//") ||
			filepath.Clean(c.Path) == ":memory:" ||
			strings.IndexFunc(c.Path, func(r rune) bool { return r < 0x20 }) >= 0 {
			return "", "", "", fmt.Errorf("quacker: storage.File: invalid path %q", c.Path)
		}
		if dir := filepath.Dir(c.Path); dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return "", "", "", fmt.Errorf("quacker: create db dir: %w", err)
			}
		}
		dsn := fmt.Sprintf("file:%s?%s&_pragma=journal_mode(WAL)&_pragma=synchronous(1)&_txlock=immediate", c.Path, common)
		return dsn, dsn, "", nil
	default:
		return "", "", "", fmt.Errorf("quacker: unknown storage mode %d", c.Mode)
	}
}

// OpenPools opens the writer (single connection), reader (query_only), and —
// for ModeMemory — a keeper connection that keeps the shared-cache database
// alive. For ModeFile it takes the kernel file lock first.
func (b sqliteBackend) OpenPools(ctx context.Context, cfg driver.Config) (w, r *sql.DB, cleanup func() error, err error) {
	if cfg.DB != nil {
		return nil, nil, nil, fmt.Errorf("quacker: SQLite storage does not accept an external *sql.DB; use File or Memory")
	}
	wdsn, rdsn, tmpPath, derr := sqliteDSN(cfg)
	if derr != nil {
		return nil, nil, nil, derr
	}
	wal := cfg.Mode != driver.ModeMemory

	var lock *os.File
	if cfg.Mode == driver.ModeFile {
		lock, err = acquireFileLock(cfg.Path)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	closeSide := func() {
		if lock != nil {
			releaseFileLock(lock)
		}
		if tmpPath != "" {
			for _, p := range []string{tmpPath, tmpPath + "-wal", tmpPath + "-shm"} {
				_ = os.Remove(p)
			}
		}
	}

	wdb, err := sql.Open("sqlite", wdsn)
	if err != nil {
		closeSide()
		return nil, nil, nil, fmt.Errorf("quacker: open writer: %w", err)
	}
	wdb.SetMaxOpenConns(1)
	wdb.SetMaxIdleConns(1)
	wdb.SetConnMaxLifetime(0)
	if err := wdb.PingContext(ctx); err != nil {
		_ = wdb.Close()
		closeSide()
		return nil, nil, nil, fmt.Errorf("quacker: ping writer: %w", err)
	}

	var keep *sql.DB
	if cfg.Mode == driver.ModeMemory {
		// Pin the shared-cache in-memory database with its own dedicated
		// pool: such a database lives only while at least one connection is
		// open, and borrowing from the single-slot write pool would starve
		// every write. The keeper pool is never used for queries.
		keep, err = sql.Open("sqlite", wdsn)
		if err != nil {
			_ = wdb.Close()
			closeSide()
			return nil, nil, nil, fmt.Errorf("quacker: pin memory db: %w", err)
		}
		keep.SetMaxOpenConns(1)
		keep.SetMaxIdleConns(1)
		if err := keep.PingContext(ctx); err != nil {
			_ = keep.Close()
			_ = wdb.Close()
			closeSide()
			return nil, nil, nil, fmt.Errorf("quacker: pin memory db: %w", err)
		}
	}

	rdb, err := sql.Open("sqlite", rdsn+"&_pragma=query_only(1)")
	if err != nil {
		if keep != nil {
			_ = keep.Close()
		}
		_ = wdb.Close()
		closeSide()
		return nil, nil, nil, fmt.Errorf("quacker: open reader: %w", err)
	}
	rdb.SetMaxOpenConns(4)
	if err := rdb.PingContext(ctx); err != nil {
		_ = rdb.Close()
		if keep != nil {
			_ = keep.Close()
		}
		_ = wdb.Close()
		closeSide()
		return nil, nil, nil, fmt.Errorf("quacker: ping reader: %w", err)
	}

	cleanup = func() error {
		var errs []error
		if keep != nil {
			if e := keep.Close(); e != nil {
				errs = append(errs, e)
			}
		}
		if e := rdb.Close(); e != nil {
			errs = append(errs, e)
		}
		if wal && tmpPath == "" {
			// File mode only (Ephemeral deletes its file right after anyway):
			// with the read pool closed no snapshot remains, so TRUNCATE
			// merges and empties the WAL in one shot. Best-effort.
			c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if _, e := wdb.ExecContext(c, "PRAGMA wal_checkpoint(TRUNCATE)"); e != nil {
				errs = append(errs, fmt.Errorf("quacker: wal checkpoint on close: %w", e))
			}
			cancel()
		}
		if e := wdb.Close(); e != nil {
			errs = append(errs, e)
		}
		closeSide()
		if len(errs) > 0 {
			return errs[0]
		}
		return nil
	}
	return wdb, rdb, cleanup, nil
}
