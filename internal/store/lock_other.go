//go:build !windows && !aix && !zos && (!unix || hurd)

package store

import "os"

// Degraded fallback: x/sys exposes no usable kernel advisory lock here
// (no flock, no fcntl F_SETLK), so no OS-level exclusion exists — two
// different processes CAN both open the same File database. Same-process
// exclusion still holds via the in-process registry in lock.go, which is
// checked before this no-op on every platform.
//
// The tag must be the exact complement of the other lock_*.go files:
// zos is not in Go's "unix" build-tag set, so it is excluded explicitly
// like aix — both route to lock_fcntl.go — while hurd is in "unix" yet
// ships neither flock nor fcntl locking, so it lands here.
func lockPlatformFile(*os.File) error { return nil }

func unlockPlatformFile(*os.File) {}

// openLockFile keeps the plain os.OpenFile: these platforms (plan9,
// js/wasm, hurd) have no O_NOFOLLOW in x/sys — and barely have symlinks
// (plan9 has none at all) — so the follow-symlink truncation primitive
// the unix helpers guard against isn't reachable anyway.
func openLockFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
}
