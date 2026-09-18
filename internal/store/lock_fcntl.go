//go:build aix || zos

package store

import (
	"errors"
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// openLockFile opens the sidecar without following symlinks, same as
// lock_unix.go: the diagnostics write truncates it, so a planted symlink
// would otherwise be a predictable-path truncation primitive for anyone
// able to write the database directory. O_CLOEXEC is passed explicitly
// because raw unix.Open does not add it (a fd inherited across exec
// would keep the lock held). ELOOP is the O_NOFOLLOW refusal.
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

// lockPlatformFile takes a whole-file POSIX write lock on the sidecar via
// fcntl(F_SETLK) — these unix platforms ship no flock(2) in x/sys. POSIX
// locks are per-process: two Stores in one process cannot self-conflict,
// so same-process exclusion rests entirely on the in-process registry in
// lock.go (which is consulted before this OS call on every platform).
func lockPlatformFile(f *os.File) error {
	lk := &unix.Flock_t{
		Type:   unix.F_WRLCK,
		Whence: io.SeekStart,
		Start:  0,
		Len:    0, // to EOF — the whole sidecar
	}
	err := unix.FcntlFlock(f.Fd(), unix.F_SETLK, lk)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, unix.EACCES) || errors.Is(err, unix.EAGAIN):
		return errLockHeld
	// ENOLCK — the kernel/NFS lock table is exhausted (classically no
	// lockd on the NFS server) — is "cannot lock for us", not "held".
	case errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOSYS) ||
		errors.Is(err, unix.ENOLCK):
		return errLockUnsupported
	default:
		return err
	}
}

// unlockPlatformFile drops the POSIX lock. Best-effort: closing the fd
// releases it regardless.
func unlockPlatformFile(f *os.File) {
	lk := &unix.Flock_t{Type: unix.F_UNLCK, Whence: io.SeekStart}
	_ = unix.FcntlFlock(f.Fd(), unix.F_SETLK, lk)
}
