//go:build !unix

package store

// Conservative fallback: without kill(2) we can't prove a pid is dead, so
// every recorded pid is treated as alive and a stale lock is never
// reclaimed automatically.
func pidAlive(int) bool { return true }
