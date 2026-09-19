package quacker

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestSchemaMigrationsBaseline: a fresh File database baselines to the
// latest schema version, and reopening it stays there with no error.
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
	if want := 18; v != want {
		t.Fatalf("schema version = %d, want %d", v, want)
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
	if want := 18; v != want {
		t.Fatalf("after reopen schema version = %d, want %d", v, want)
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

// TestLockHelperProcess is the child half of TestCrossProcessLockExclusion,
// in the standard Go helper-process idiom: it does nothing unless
// GO_QUACKER_LOCK_HELPER=1, then opens the File path named by
// GO_QUACKER_LOCK_PATH, prints READY once the lock is held, and sleeps
// until the parent kills it.
func TestLockHelperProcess(t *testing.T) {
	if os.Getenv("GO_QUACKER_LOCK_HELPER") != "1" {
		return
	}
	path := os.Getenv("GO_QUACKER_LOCK_PATH")
	q, err := Open(WithStorage(File(path)))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("READY")
	time.Sleep(60 * time.Second)
	_ = q.Close(context.Background())
	os.Exit(0)
}

// TestCrossProcessLockExclusion: the kernel-held advisory lock — not the
// in-process registry — is what excludes a second process. A child holding
// the lock makes the parent's Open fail with "in use", and killing the
// child releases it (the kernel drops the lock on process death, even via
// SIGKILL).
func TestCrossProcessLockExclusion(t *testing.T) {
	switch runtime.GOOS {
	case "js", "wasip1", "plan9":
		t.Skip("helper-process test requires a spawnable test binary")
	}
	path := filepath.Join(t.TempDir(), "state.db")
	cmd := exec.Command(os.Args[0], "-test.run=^TestLockHelperProcess$")
	cmd.Env = append(os.Environ(),
		"GO_QUACKER_LOCK_HELPER=1",
		"GO_QUACKER_LOCK_PATH="+path)
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	// The helper prints READY only after its Open won the lock.
	ready := make(chan error, 1)
	go func() {
		line, err := bufio.NewReader(stdout).ReadString('\n')
		if err != nil {
			ready <- err
			return
		}
		if strings.TrimSpace(line) != "READY" {
			ready <- fmt.Errorf("helper printed %q, want READY", line)
			return
		}
		ready <- nil
	}()
	select {
	case err := <-ready:
		if err != nil {
			t.Fatalf("helper did not acquire the lock: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for helper's READY")
	}

	// (a) A different process can't share our in-process registry; only
	// the kernel lock can exclude it.
	if q2, err := Open(WithStorage(File(path))); err == nil ||
		!strings.Contains(err.Error(), "in use") {
		if q2 != nil {
			_ = q2.Close(context.Background())
		}
		t.Fatalf("Open while helper holds the lock: err = %v, want an in-use error", err)
	}

	// (b)+(c) Kill the helper; the kernel releases the lock on process
	// death, so our Open must succeed shortly after (poll to absorb OS
	// teardown latency).
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	var q *Quacker
	var lastErr error
	for i := 0; i < 50; i++ {
		q, lastErr = Open(WithStorage(File(path)), WithPollInterval(5*time.Millisecond))
		if lastErr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("Open after helper killed: %v", lastErr)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = q.Close(ctx)
	})
}

// TestNewerSchemaRefused: a File database stamped with a schema version
// newer than this build is refused, and the refusal releases the file
// lock — a second Open must also fail "newer", never "in use".
func TestNewerSchemaRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE schema_migrations (
		version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO schema_migrations (version, applied_at) VALUES (999, 0)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		_, err := Open(WithStorage(File(path)), WithPollInterval(5*time.Millisecond))
		if err == nil || !strings.Contains(err.Error(), "newer") {
			t.Fatalf("Open attempt %d err = %v, want a newer-schema error", i+1, err)
		}
		if strings.Contains(err.Error(), "in use") {
			t.Fatalf("Open attempt %d err = %v — the first failure leaked the lock", i+1, err)
		}
	}
}

// TestConcurrentOpenSingleWinner: eight simultaneous Opens on one File
// path yield exactly one winner — the in-process registry and the kernel
// lock serialize the rest into "in use" failures.
func TestConcurrentOpenSingleWinner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	const n = 8
	var wg sync.WaitGroup
	qs := make([]*Quacker, n)
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			qs[i], errs[i] = Open(WithStorage(File(path)))
		}(i)
	}
	wg.Wait()
	var winner *Quacker
	wins := 0
	for i := 0; i < n; i++ {
		if errs[i] == nil {
			wins++
			winner = qs[i]
			continue
		}
		if !strings.Contains(errs[i].Error(), "in use") {
			t.Fatalf("losing Open err = %v, want an in-use error", errs[i])
		}
	}
	if wins != 1 || winner == nil {
		t.Fatalf("successful Opens = %d, want exactly 1", wins)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := winner.Close(ctx); err != nil {
		t.Fatal(err)
	}
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
