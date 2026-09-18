// Package store owns all SQLite access for quacker.
//
// It maintains two connection pools over the same database: a single writer
// connection (SQLite permits only one writer at a time) and a pool of
// read-only connections. In WAL mode readers never block the writer, so
// introspection queries (Execution, Runs, Metrics, Logs) can run at any time
// without stalling task execution.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	_ "modernc.org/sqlite" // registers the pure-Go "sqlite" driver
)

// Mode selects how the underlying SQLite database is stored.
type Mode int

const (
	// ModeMemory keeps all state in a pure in-memory SQLite database
	// (shared-cache). Nothing touches the filesystem. Note: WAL is not
	// available to :memory: databases, so reads may briefly contend with
	// writes; busy_timeout retries absorb this.
	ModeMemory Mode = iota
	// ModeEphemeral uses a temporary file with WAL journaling and
	// synchronous=OFF. State is discarded (file deleted) on Close. This is
	// the default: same zero-durability semantics as memory, but readers are
	// truly non-blocking, because :memory: databases cannot use WAL.
	ModeEphemeral
	// ModeFile persists to a user-supplied path with WAL journaling.
	ModeFile
)

// Config configures the storage backend.
type Config struct {
	Mode Mode
	// Path is the database file path; required for ModeFile, ignored otherwise.
	Path string
	// RecoverRunningOnBoot (ModeFile only): when true, runs left RUNNING or
	// INTERRUPTED by a previous process are re-queued on Open; when false
	// they are marked FAILED with an "interrupted" error. Default true.
	RecoverRunningOnBoot bool
}

// dsn returns the writer DSN, reader DSN, and (for ModeEphemeral) the temp
// file path to remove on Close.
func (c Config) dsn() (wdsn, rdsn, tmpPath string, err error) {
	common := "_pragma=busy_timeout(10000)&_pragma=foreign_keys(1)"
	switch c.Mode {
	case ModeMemory:
		// A named in-memory database with a shared cache so both pools see
		// the same data. journal_mode defaults to MEMORY there.
		// No _txlock=immediate here (unlike the file modes): on a shared
		// cache an immediate write txn's RESERVED lock makes same-cache
		// readers hit SQLITE_LOCKED, which bypasses busy_timeout. WAL/file
		// readers don't contend, so _txlock=immediate is used only there.
		name := fmt.Sprintf("quacker-%d-%d", os.Getpid(), c.unique())
		dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared&%s", name, common)
		return dsn, dsn, "", nil
	case ModeEphemeral:
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
	case ModeFile:
		if c.Path == "" {
			return "", "", "", fmt.Errorf("quacker: storage.File requires a path")
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

// unique gives each in-memory database a distinct name so two quacker
// instances in one process don't share a cache. Uses a package-level counter
// seeded once; no need for heavy randomness.
func (c Config) unique() int64 { return memSerial.Add(1) }

// Store wraps the SQLite database. It is safe for concurrent use.
type Store struct {
	write   *sql.DB // exactly one connection; all mutations go through here
	read    *sql.DB // read-only connections (query_only)
	keep    *sql.DB // keeper pool pinning the shared-cache in-memory DB
	tmpPath string
	closed  atomic.Bool
}

var memSerial atomic.Int64

// Open opens (and migrates) the database described by cfg.
func Open(cfg Config) (*Store, error) {
	wdsn, rdsn, tmpPath, err := cfg.dsn()
	if err != nil {
		return nil, err
	}
	s := &Store{tmpPath: tmpPath}

	w, err := sql.Open("sqlite", wdsn)
	if err != nil {
		s.Close()
		return nil, fmt.Errorf("quacker: open writer: %w", err)
	}
	s.write = w
	w.SetMaxOpenConns(1)
	w.SetMaxIdleConns(1)
	w.SetConnMaxLifetime(0)

	ctx := context.Background()
	if err := w.PingContext(ctx); err != nil {
		s.Close()
		return nil, fmt.Errorf("quacker: ping writer: %w", err)
	}

	if cfg.Mode == ModeMemory {
		// Pin the shared-cache in-memory database with its own dedicated
		// pool: such a database lives only while at least one connection is
		// open, and borrowing from the single-slot write pool would starve
		// every write. The keeper pool is never used for queries.
		keep, err := sql.Open("sqlite", wdsn)
		if err != nil {
			s.Close()
			return nil, fmt.Errorf("quacker: pin memory db: %w", err)
		}
		keep.SetMaxOpenConns(1)
		keep.SetMaxIdleConns(1)
		if err := keep.PingContext(ctx); err != nil {
			s.Close()
			return nil, fmt.Errorf("quacker: pin memory db: %w", err)
		}
		s.keep = keep
	}

	r, err := sql.Open("sqlite", rdsn+"&_pragma=query_only(1)")
	if err != nil {
		s.Close()
		return nil, fmt.Errorf("quacker: open reader: %w", err)
	}
	s.read = r
	r.SetMaxOpenConns(4)
	if err := r.PingContext(ctx); err != nil {
		s.Close()
		return nil, fmt.Errorf("quacker: ping reader: %w", err)
	}

	if err := s.migrate(ctx); err != nil {
		s.Close()
		return nil, err
	}
	if cfg.Mode == ModeFile {
		if err := s.recoverInterrupted(ctx, cfg.RecoverRunningOnBoot); err != nil {
			s.Close()
			return nil, err
		}
	}
	return s, nil
}

// Write returns the single-connection writer pool. All mutations must use it.
func (s *Store) Write() *sql.DB { return s.write }

// Read returns the read-only pool. Introspection queries must use it; in WAL
// mode its queries never block (or get blocked by) the write path.
func (s *Store) Read() *sql.DB { return s.read }

// Close closes both pools and removes any ephemeral database file.
func (s *Store) Close() error {
	if s == nil || s.closed.Swap(true) {
		return nil
	}
	var errs []error
	if s.keep != nil {
		if err := s.keep.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.read != nil {
		if err := s.read.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.write != nil {
		if err := s.write.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.tmpPath != "" {
		for _, p := range []string{s.tmpPath, s.tmpPath + "-wal", s.tmpPath + "-shm"} {
			_ = os.Remove(p)
		}
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}
