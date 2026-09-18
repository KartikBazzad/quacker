//go:build unix

package store

import (
	"errors"
	"syscall"
)

// pidAlive probes a pid with signal 0: ESRCH means the process is dead,
// while EPERM (exists, owned by someone else) and nil mean alive. Any
// other error is treated conservatively as alive.
func pidAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || !errors.Is(err, syscall.ESRCH)
}
