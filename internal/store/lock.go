package store

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The single-writer guard is a kernel-held advisory lock on a permanent
// sidecar file (dbPath+".quacker.lock"), held open for the Store's whole
// lifetime. Unlike the old pid-recording lockfile this is correct by
// construction: the lock lives in the kernel, not the file content, so it
// is acquired atomically and released automatically when the holder dies
// — even via SIGKILL — with no stale-lock reclamation and no
// check-then-remove TOCTOU.
//
// The lock goes on a sidecar, never the database file: on NFS, flock(2)
// is emulated as a whole-file fcntl lock, which would collide with
// SQLite's own fcntl byte-range locks on the same inode (every write
// transaction would hit SQLITE_BUSY). SQLite never touches the sidecar,
// so the lock is conflict-free.
//
// The sidecar is created once and never deleted: unlinking a file another
// process holds open lets a new inode be created for the same path, and
// two inodes mean two independent locks — a double-owner split-brain.
// Its content (pid/host/time) is diagnostic only, written best-effort
// after the lock is taken; an empty or stale file is fine.
//
// fileLocks adds same-process exclusion on top of the kernel lock. POSIX
// fcntl locks are per-process — two Stores in one process could both
// "acquire" the OS lock — so the registry is consulted first and makes
// the second Open fail deterministically on every platform.
var fileLocks sync.Map // absolute lock path -> *os.File holding it;
// a value is briefly nil between the LoadOrStore reservation and the
// Store that upgrades it to the locked file once the OS call lands.

// Sentinels the platform lock helpers classify their failures into, so
// lock.go can build the right error without knowing OS-specific errnos.
var (
	// errLockHeld means another process holds the lock.
	errLockHeld = errors.New("quacker: file lock held")
	// errLockUnsupported means the filesystem cannot do advisory locks.
	errLockUnsupported = errors.New("quacker: advisory file locking unsupported")
)

// acquireFileLock takes the single-writer lock for a File-mode database
// and returns the open, locked sidecar file; the Store holds it and
// passes it to releaseFileLock on Close.
func acquireFileLock(dbPath string) (*os.File, error) {
	lockPath := dbPath + ".quacker.lock"
	key := lockPath
	if abs, err := filepath.Abs(lockPath); err == nil {
		key = abs
	}
	// Same-process check first, before any OS call: it must win even on
	// platforms whose OS lock wouldn't conflict within one process.
	if _, loaded := fileLocks.LoadOrStore(key, nil); loaded {
		return nil, fmt.Errorf("quacker: state file %s is already in use by this process", dbPath)
	}
	f, err := openLockFile(lockPath)
	if err != nil {
		fileLocks.Delete(key)
		return nil, fmt.Errorf("quacker: create lock %s: %w", lockPath, err)
	}
	if err := lockPlatformFile(f); err != nil {
		_ = f.Close()
		fileLocks.Delete(key)
		switch {
		case errors.Is(err, errLockHeld):
			return nil, lockInUseError(dbPath, lockPath)
		case errors.Is(err, errLockUnsupported):
			return nil, fmt.Errorf("quacker: state file %s: filesystem does not support advisory file locking", dbPath)
		default:
			return nil, fmt.Errorf("quacker: lock %s: %w", lockPath, err)
		}
	}
	writeLockDiagnostics(f)
	fileLocks.Store(key, f)
	return f, nil
}

// releaseFileLock unlocks (best-effort — closing the fd releases the
// kernel lock anyway), closes the sidecar, and frees the in-process
// registry entry.
func releaseFileLock(f *os.File) {
	if f == nil {
		return
	}
	unlockPlatformFile(f)
	_ = f.Close()
	// Delete by value rather than recomputing the key: the registry holds
	// this exact *os.File, so the match is unambiguous even if the process
	// changed directories since Open (a recomputed relative path could
	// collide with a different store's entry). The map holds one entry per
	// open File store, so the sweep is trivial.
	fileLocks.Range(func(k, v any) bool {
		if v == f {
			fileLocks.Delete(k)
		}
		return true
	})
}

// lockInUseError builds the "already in use" failure, enriching it with
// the holder's pid and host when the sidecar's diagnostic content parses.
func lockInUseError(dbPath, lockPath string) error {
	if pid, host, ok := readLockDiagnostics(lockPath); ok {
		// The holder can be this very process: an aliased path (a symlinked
		// directory component, say) resolves to the same sidecar but a
		// different registry key, so the kernel lock reports the conflict
		// the in-process map missed. Say so rather than blaming a stranger.
		if pid == os.Getpid() {
			return fmt.Errorf("quacker: state file %s is already in use by this process", dbPath)
		}
		return fmt.Errorf("quacker: state file %s is already in use by another quacker process (pid %d on host %q)", dbPath, pid, host)
	}
	return fmt.Errorf("quacker: state file %s is already in use by another quacker process", dbPath)
}

// writeLockDiagnostics records pid, hostname, and time in the sidecar
// through the already-open fd — purely informational for debugging a held
// lock; the lock state itself lives in the kernel.
func writeLockDiagnostics(f *os.File) {
	hostname, _ := os.Hostname()
	_ = f.Truncate(0)
	_, _ = f.Seek(0, io.SeekStart)
	_, _ = fmt.Fprintf(f, "%d\n%s\n%s\n", os.Getpid(), hostname, time.Now().Format(time.RFC3339))
}

// readLockDiagnostics parses the sidecar's pid and host lines. ok=false
// means empty or unparseable content — no holder identity to report.
func readLockDiagnostics(lockPath string) (pid int, host string, ok bool) {
	b, err := os.ReadFile(lockPath)
	if err != nil {
		return 0, "", false
	}
	line, rest, _ := strings.Cut(string(b), "\n")
	host, _, _ = strings.Cut(rest, "\n")
	n, perr := strconv.Atoi(strings.TrimSpace(line))
	if perr != nil || n <= 0 {
		return 0, "", false
	}
	return n, strings.TrimSpace(host), true
}
