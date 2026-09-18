package quacker

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// TestSchemaMigrationsBaseline: a fresh File database baselines to schema
// version 1, and reopening it stays at version 1 with no error.
func TestSchemaMigrationsBaseline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	q1, err := Open(WithStorage(File(path)), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	var v int
	if err := q1.st.Read().QueryRow(
		`SELECT COALESCE(MAX(version),0) FROM schema_migrations`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != 1 {
		t.Fatalf("schema version = %d, want 1", v)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := q1.Close(ctx); err != nil {
		t.Fatal(err)
	}

	q2, err := Open(WithStorage(File(path)), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		_ = q2.Close(c)
	})
	if err := q2.st.Read().QueryRow(
		`SELECT COALESCE(MAX(version),0) FROM schema_migrations`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != 1 {
		t.Fatalf("after reopen schema version = %d, want 1", v)
	}
}

// TestFileLockExcludesSecondEngine: a second Open on the same File path
// fails fast while the first engine holds the lock; after Close it succeeds.
func TestFileLockExcludesSecondEngine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	q1, err := Open(WithStorage(File(path)), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(WithStorage(File(path))); err == nil || !strings.Contains(err.Error(), "in use") {
		t.Fatalf("second Open err = %v, want an in-use error", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := q1.Close(ctx); err != nil {
		t.Fatal(err)
	}

	q2, err := Open(WithStorage(File(path)), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatalf("Open after Close: %v", err)
	}
	t.Cleanup(func() {
		c, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		_ = q2.Close(c)
	})
}

// TestStaleLockReclaimed: a lockfile whose recorded pid cannot be alive is
// treated as stale and reclaimed on Open.
func TestStaleLockReclaimed(t *testing.T) {
	// pidAlive falls back to "always alive" off unix, where no lock is
	// ever reclaimed.
	switch runtime.GOOS {
	case "windows", "plan9", "js", "wasip1":
		t.Skip("pid liveness probing is unix-only")
	}
	// pid_max can exceed 999999 on Linux, so a "dead" constant may name a
	// live pid; run a process that has already exited to get one that is
	// guaranteed dead.
	cmd := exec.Command("true")
	if err := cmd.Run(); err != nil {
		t.Fatalf("probe a dead pid: %v", err)
	}
	host, _ := os.Hostname()
	path := filepath.Join(t.TempDir(), "state.db")
	if err := os.WriteFile(path+".quacker.lock",
		[]byte(fmt.Sprintf("%d\n%s\n2020-01-01T00:00:00Z\n", cmd.Process.Pid, host)), 0o644); err != nil {
		t.Fatal(err)
	}
	q, err := Open(WithStorage(File(path)), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatalf("Open with stale lock: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = q.Close(ctx)
	})
}

// TestCheckpointOnClose: a clean File-mode close truncates the WAL, leaving
// the -wal file gone or empty — the periodic PASSIVE checkpoints plus the
// final TRUNCATE keep it from growing unboundedly.
func TestCheckpointOnClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	q, err := Open(WithStorage(File(path)), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	task := NewTask("wal-churn", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	})
	for i := 0; i < 50; i++ {
		h, err := Enqueue(context.Background(), q, task, greetIn{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.Result(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := q.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(path + "-wal"); err == nil && fi.Size() > 0 {
		t.Fatalf("wal size after close = %d, want truncated to 0", fi.Size())
	}
}

// TestCheckpointLoopRuns exercises the periodic PASSIVE checkpoint: with a
// millisecond interval, several ticks fire while tasks churn (30 tasks at
// a 5ms poll guarantees >50ms of runtime), then Close still truncates the
// WAL cleanly.
func TestCheckpointLoopRuns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	q, err := Open(WithStorage(File(path)), WithPollInterval(5*time.Millisecond),
		WithCheckpointInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	task := NewTask("wal-churn-loop", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	})
	for i := 0; i < 30; i++ {
		h, err := Enqueue(context.Background(), q, task, greetIn{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.Result(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := q.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(path + "-wal"); err == nil && fi.Size() > 0 {
		t.Fatalf("wal size after close = %d, want truncated to 0", fi.Size())
	}
}
