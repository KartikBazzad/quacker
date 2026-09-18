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
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
	// CheckpointInterval is how often WAL modes (Ephemeral, File) run a
	// passive wal_checkpoint to bound WAL growth. <=0 uses the 60s
	// default; values below 10ms are clamped to 10ms. ModeMemory never
	// checkpoints: :memory: databases have no WAL.
	CheckpointInterval time.Duration
}

const (
	// defaultCheckpointInterval bounds WAL growth when CheckpointInterval
	// is unset.
	defaultCheckpointInterval = 60 * time.Second
	// minCheckpointInterval keeps a hot-ticker configuration from turning
	// the checkpoint loop into a spin; smaller values are clamped up.
	minCheckpointInterval = 10 * time.Millisecond
)

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
	// lock is the open, kernel-locked sidecar file held for the store's
	// lifetime (File mode only); Close releases it.
	lock       *os.File
	wal        bool // WAL journal modes run checkpoints
	ckptCancel context.CancelFunc
	ckptWG     sync.WaitGroup
	// ckptCount ticks once per checkpoint attempt; tests read it to prove
	// the loop runs without waiting a full interval.
	ckptCount atomic.Int64
	closed    atomic.Bool
}

var memSerial atomic.Int64

// Open opens (and migrates) the database described by cfg.
func Open(cfg Config) (*Store, error) {
	wdsn, rdsn, tmpPath, err := cfg.dsn()
	if err != nil {
		return nil, err
	}
	s := &Store{tmpPath: tmpPath, wal: cfg.Mode != ModeMemory}

	if cfg.Mode == ModeFile {
		// Take the single-writer lock before any connection touches the
		// file: a kernel advisory lock on the sidecar, so opens serialize
		// in the kernel and the lock dies with its holder.
		lock, err := acquireFileLock(cfg.Path)
		if err != nil {
			return nil, err
		}
		s.lock = lock
	}

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
	if s.wal {
		interval := cfg.CheckpointInterval
		if interval <= 0 {
			interval = defaultCheckpointInterval
		}
		if interval < minCheckpointInterval {
			interval = minCheckpointInterval
		}
		ckptCtx, ckptCancel := context.WithCancel(context.Background())
		s.ckptCancel = ckptCancel
		s.ckptWG.Add(1)
		go s.checkpointLoop(ckptCtx, interval)
	}
	return s, nil
}

// Write returns the single-connection writer pool. All mutations must use it.
func (s *Store) Write() *sql.DB { return s.write }

// Read returns the read-only pool. Introspection queries must use it; in WAL
// mode its queries never block (or get blocked by) the write path.
func (s *Store) Read() *sql.DB { return s.read }

// checkpointLoop runs a PASSIVE wal_checkpoint on the writer every d.
// PASSIVE never waits on the writer or readers — it checkpoints whatever
// is safely checkpointable and returns — so the timer can sit on the hot
// path and still cap WAL growth whenever readers are momentarily idle.
func (s *Store) checkpointLoop(ctx context.Context, d time.Duration) {
	defer s.ckptWG.Done()
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// ExecContext lets Close interrupt an Exec queued behind a
			// long write tx on the single-conn pool.
			_, _ = s.write.ExecContext(ctx, "PRAGMA wal_checkpoint(PASSIVE)")
			s.ckptCount.Add(1)
		}
	}
}

// Close closes both pools and removes any ephemeral database file. On WAL
// modes it first stops the checkpoint goroutine, then — once the read pool
// is closed and no reader snapshot can linger — runs a truncating
// checkpoint (File mode only) so a clean close leaves an empty (usually
// deleted) -wal file.
func (s *Store) Close() error {
	if s == nil || s.closed.Swap(true) {
		return nil
	}
	if s.ckptCancel != nil {
		s.ckptCancel()
		s.ckptWG.Wait()
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
	if s.wal && s.tmpPath == "" && s.write != nil {
		// File mode only (Ephemeral deletes its file right after anyway):
		// with the read pool closed no snapshot remains, so TRUNCATE merges
		// and empties the WAL in one shot. Still best-effort — Close
		// proceeds regardless — but the failure is reported to the caller.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := s.write.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
		cancel()
		if err != nil {
			errs = append(errs, fmt.Errorf("quacker: wal checkpoint on close: %w", err))
		}
	}
	if s.write != nil {
		if err := s.write.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.lock != nil {
		releaseFileLock(s.lock)
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
