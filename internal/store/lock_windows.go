//go:build windows

package store

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

// openLockFile keeps the plain os.OpenFile: unlike unix, creating a
// symlink on Windows requires SeCreateSymbolicLinkPrivilege (granted to
// administrators and developer-mode machines only), so a planted-symlink
// truncation primitive via the sidecar isn't reachable to the unprivileged
// attacker the unix O_NOFOLLOW open guards against.
func openLockFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
}

// lockPlatformFile takes an exclusive byte-range lock on byte 0 of the
// sidecar via LockFileEx (non-blocking). The lock sits on the sidecar,
// never the database file, so SQLite's own byte-range locks are
// undisturbed.
func lockPlatformFile(f *os.File) error {
	h := windows.Handle(f.Fd())
	err := windows.LockFileEx(h,
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0, 1, 0, &windows.Overlapped{})
	switch {
	case err == nil:
		return nil
	case errors.Is(err, windows.ERROR_LOCK_VIOLATION) || errors.Is(err, windows.ERROR_IO_PENDING):
		return errLockHeld
	default:
		return err
	}
}

// unlockPlatformFile drops the byte-range lock. Best-effort: closing the
// handle releases it regardless.
func unlockPlatformFile(f *os.File) {
	_ = windows.UnlockFileEx(windows.Handle(f.Fd()), 0, 1, 0, &windows.Overlapped{})
}
