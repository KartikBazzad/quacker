//go:build unix && !aix && !hurd && !zos

package store

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// openLockFile opens the sidecar without following symlinks: the
// diagnostics write truncates it, so following a planted symlink would
// hand anyone able to write the database directory a predictable-path
// truncation primitive. unix.Open is the raw syscall — it adds no
// O_CLOEXEC — so it is passed explicitly; without it an exec'd child
// inheriting the fd would keep the kernel lock held after the parent's
// death. ELOOP is the O_NOFOLLOW refusal of a symlink.
func openLockFile(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o644)
	if err != nil {
		if errors.Is(err, unix.ELOOP) {
			return nil, fmt.Errorf("quacker: lock %s is a symlink", path)
		}
		return nil, err
	}
	return os.NewFile(uintptr(fd), path), nil
}

// lockPlatformFile takes a non-blocking exclusive flock on the sidecar
// file. flock is per open-file-description, so two opens in this process
// would also conflict — the in-process registry in lock.go is consulted
// first anyway, keeping the same-process failure deterministic and
// identical across platforms.
//
// (illumos builds this file via the solaris tag in x/sys; ios via
// darwin. aix and zos lack unix.Flock and use lock_fcntl.go; hurd has
// neither and falls back to lock_other.go.)
func lockPlatformFile(f *os.File) error {
	err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN):
		return errLockHeld
	// ENOLCK means the kernel/NFS lock table is out of entries —
	// classically that the NFS server runs no lockd — so it is
	// "cannot lock for us", not "someone holds it". EINTR is left to
	// the generic error path: rare, and failing closed is correct.
	case errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOSYS) ||
		errors.Is(err, unix.ENOLCK):
		return errLockUnsupported
	default:
		return err
	}
}

// unlockPlatformFile drops the flock. Best-effort: closing the fd
// releases the lock regardless.
func unlockPlatformFile(f *os.File) {
	_ = unix.Flock(int(f.Fd()), unix.LOCK_UN)
}
