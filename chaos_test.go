package quacker

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

const (
	chaosChildEnv = "QUACKER_CHAOS_CHILD"
	chaosDBEnv    = "QUACKER_CHAOS_DB"
	chaosNEnv     = "QUACKER_CHAOS_N"
)

// newChaosTask is the workload both the killed child and the recovering parent
// register. It always succeeds, so after recovery every run must end
// SUCCEEDED (at-least-once, no lost terminal states).
func newChaosTask() *Task[int, int] {
	return NewTask("chaos", func(ctx context.Context, in int) (int, error) {
		time.Sleep(time.Millisecond)
		return in + 1, nil
	})
}

// TestFileChaosRecovery kills a child process mid-execution against a File
// database, reopens it, and asserts every run recovers to SUCCEEDED. It is
// best-effort chaos (a random kill point per iteration), not deterministic
// fault injection at every write site.
func TestFileChaosRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos suite skipped with -short")
	}
	const n = 25
	for iter := 0; iter < 3; iter++ {
		t.Run(fmt.Sprintf("iter%d", iter), func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "chaos.db")
			ready := path + ".ready"

			cmd := exec.Command(os.Args[0], "-test.run=^TestChaosChildProcess$")
			cmd.Env = append(os.Environ(),
				chaosChildEnv+"=1",
				chaosDBEnv+"="+path,
				chaosNEnv+"="+strconv.Itoa(n),
			)
			if err := cmd.Start(); err != nil {
				t.Fatalf("start child: %v", err)
			}
			// Wait until the child has enqueued all runs, then let it execute
			// for a variable slice of time before SIGKILL.
			deadline := time.Now().Add(5 * time.Second)
			for {
				if _, err := os.Stat(ready); err == nil {
					break
				}
				if time.Now().After(deadline) {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
					t.Fatal("child never signalled readiness")
				}
				time.Sleep(2 * time.Millisecond)
			}
			time.Sleep(time.Duration(5+iter*15) * time.Millisecond)
			_ = cmd.Process.Kill()
			_ = cmd.Wait()

			q, err := Open(WithStorage(File(path)), WithPollInterval(2*time.Millisecond))
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer closeQ(t, q)
			Register(q, newChaosTask())

			waitFor(t, 20*time.Second, func() bool {
				runs, err := q.Runs(context.Background(), RunFilter{Workflow: "chaos"})
				if err != nil || len(runs) == 0 {
					return false
				}
				for _, r := range runs {
					if r.Status != StatusSucceeded {
						return false
					}
				}
				return true
			})
			runs, _ := q.Runs(context.Background(), RunFilter{Workflow: "chaos"})
			if len(runs) == 0 {
				t.Fatal("no runs survived the kill")
			}
			if len(runs) > n {
				t.Fatalf("runs = %d, want <= %d", len(runs), n)
			}
		})
	}
}

// TestChaosChildProcess is the helper that runs the workload and then blocks
// until the parent SIGKILLs it. Guarded by an env var so it is a no-op in a
// normal test run.
func TestChaosChildProcess(t *testing.T) {
	if os.Getenv(chaosChildEnv) != "1" {
		t.Skip("chaos child helper")
	}
	path := os.Getenv(chaosDBEnv)
	n, _ := strconv.Atoi(os.Getenv(chaosNEnv))
	q, err := Open(WithStorage(File(path)), WithPollInterval(time.Millisecond))
	if err != nil {
		t.Fatalf("child open: %v", err)
	}
	task := newChaosTask()
	for i := 0; i < n; i++ {
		if _, err := Enqueue(context.Background(), q, task, i); err != nil {
			t.Fatalf("child enqueue: %v", err)
		}
	}
	if err := os.WriteFile(path+".ready", []byte("1"), 0o644); err != nil {
		t.Fatalf("child ready: %v", err)
	}
	// Process until killed.
	for {
		time.Sleep(50 * time.Millisecond)
	}
}
