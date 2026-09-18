package store

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// acquireFileLock takes the single-writer lock for a File-mode database: a
// lockfile at dbPath+".quacker.lock" recording pid, host, and time. The
// content is written to a sibling temp file and link(2)ed into place —
// link fails EEXIST atomically when the lock exists, so a concurrent
// opener can never observe a half-written lockfile, call it stale, and
// steal the lock (the earlier O_EXCL create-then-write left that window).
// A lock whose recorded pid is alive on this host fails fast; a stale lock
// (dead pid, unparseable foreign garbage) is reclaimed. Returns the lock
// path so the owner can release it on Close.
func acquireFileLock(dbPath string) (string, error) {
	lockPath := dbPath + ".quacker.lock"
	hostname, _ := os.Hostname()
	// The pid in the temp name keeps two racing processes from sharing one
	// temp file; same-process racers write identical content, which is
	// harmless.
	tmpPath := fmt.Sprintf("%s.tmp.%d", lockPath, os.Getpid())
	content := fmt.Sprintf("%d\n%s\n%s\n", os.Getpid(), hostname, time.Now().Format(time.RFC3339))
	if err := os.WriteFile(tmpPath, []byte(content), 0o644); err != nil {
		return "", fmt.Errorf("quacker: create lock %s: %w", lockPath, err)
	}
	defer os.Remove(tmpPath)

	for attempt := 0; attempt < 2; attempt++ {
		err := os.Link(tmpPath, lockPath)
		if err == nil {
			return lockPath, nil
		}
		if !os.IsExist(err) {
			return "", fmt.Errorf("quacker: create lock %s: %w", lockPath, err)
		}
		// The lock exists: reclaim it only if it's provably stale.
		pid, host, ok, rerr := readLockFile(lockPath)
		switch {
		case rerr != nil && !os.IsNotExist(rerr):
			// Present but unreadable: assume a live lock rather than
			// stealing one we merely can't inspect.
			return "", fmt.Errorf("quacker: state file %s is already in use by another quacker process", dbPath)
		case rerr == nil && ok:
			// A pid recorded under a different hostname lives in another
			// namespace (shared FS, containers) where kill(2) can't prove
			// it dead; without a local hostname we can't even compare.
			// Both cases fail safe: report the lock in use.
			if hostname == "" || host != hostname {
				return "", fmt.Errorf("quacker: state file %s is already in use by quacker on host %q (pid %d)", dbPath, host, pid)
			}
			if pidAlive(pid) {
				return "", fmt.Errorf("quacker: state file %s is already in use by another quacker process (pid %d)", dbPath, pid)
			}
		}
		// Stale — dead pid, unparseable garbage, or vanished between the
		// failed link and the read. Reclaim and retry.
		if err := os.Remove(lockPath); err != nil && !os.IsNotExist(err) {
			return "", fmt.Errorf("quacker: remove stale lock %s: %w", lockPath, err)
		}
	}
	return "", fmt.Errorf("quacker: state file %s is already in use by another quacker process", dbPath)
}

// releaseFileLock removes the lockfile on Close, but only if it's still
// ours: a lock recording our pid, or unparseable foreign garbage, is
// removed; a lock recording a different pid belongs to a process that
// replaced ours (stolen lock, admin recreation) and is left alone.
func releaseFileLock(lockPath string) {
	pid, _, ok, err := readLockFile(lockPath)
	if err != nil {
		return // gone or unreadable: nothing safe to remove
	}
	if !ok || pid == os.Getpid() {
		_ = os.Remove(lockPath)
	}
}

// readLockFile reads the lockfile's pid and host. ok=false means the file
// read fine but doesn't parse — foreign garbage, treated as stale. A read
// error is returned separately so a present-but-unreadable lock is never
// mistaken for a dead one.
func readLockFile(lockPath string) (pid int, host string, ok bool, err error) {
	b, err := os.ReadFile(lockPath)
	if err != nil {
		return 0, "", false, err
	}
	line, rest, _ := strings.Cut(string(b), "\n")
	host, _, _ = strings.Cut(rest, "\n")
	n, perr := strconv.Atoi(strings.TrimSpace(line))
	if perr != nil || n <= 0 {
		return 0, "", false, nil
	}
	return n, strings.TrimSpace(host), true, nil
}
