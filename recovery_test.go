package quacker

import (
	"context"
	json "github.com/goccy/go-json"
	"path/filepath"
	"testing"
	"time"
)

// TestFileRecoveryRequeues: a run interrupted by a hard shutdown is
// re-queued when the same File storage is reopened.
func TestFileRecoveryRequeues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	release := make(chan struct{})
	task := NewTask("recover", func(ctx context.Context, in greetIn) (greetOut, error) {
		<-release
		return greetOut{Greeting: "recovered"}, nil
	})

	q1, err := Open(WithStorage(File(path)), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	Register(q1, task)
	h, err := Enqueue(context.Background(), q1, task, greetIn{})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool {
		s, _ := q1.Execution(context.Background(), h.RunID())
		return s != nil && s.Status == StatusRunning
	})

	// Hard close: drain deadline expires while the task is stuck.
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	if err := q1.Close(ctx); err == nil {
		t.Fatal("expected drain timeout")
	}
	close(release)

	// Reopen: recovered run should execute to completion.
	q2, err := Open(WithStorage(File(path)), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		_ = q2.Close(c)
	})
	Register(q2, task)

	var snap *Execution
	waitFor(t, 3*time.Second, func() bool {
		snap, _ = q2.Execution(context.Background(), h.RunID())
		return snap != nil && snap.Status == StatusSucceeded
	})
	var out greetOut
	if err := json.Unmarshal(snap.Output, &out); err != nil || out.Greeting != "recovered" {
		t.Fatalf("output = %+v err = %v", out, err)
	}
}

// TestFileRecoveryFailsInstead: with RecoverRunningOnBoot(false), interrupted
// runs are marked FAILED on reopen.
func TestFileRecoveryFailsInstead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	stuck := make(chan struct{})
	task := NewTask("recover2", func(ctx context.Context, in greetIn) (greetOut, error) {
		<-stuck
		return greetOut{}, nil
	})

	q1, err := Open(WithStorage(File(path)), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	Register(q1, task)
	h, _ := Enqueue(context.Background(), q1, task, greetIn{})
	waitFor(t, 2*time.Second, func() bool {
		s, _ := q1.Execution(context.Background(), h.RunID())
		return s != nil && s.Status == StatusRunning
	})
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_ = q1.Close(ctx)
	close(stuck)

	q2, err := Open(WithStorage(File(path).RecoverRunningOnBoot(false)), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		_ = q2.Close(c)
	})
	Register(q2, task)

	var snap *Execution
	waitFor(t, 3*time.Second, func() bool {
		snap, _ = q2.Execution(context.Background(), h.RunID())
		return snap != nil && snap.Status == StatusFailed
	})
	if snap.Error == "" {
		t.Fatal("expected interrupted error message")
	}
}

// TestQueuedWorkSurvivesRestart: runs still QUEUED at close are picked up by
// the next process without any recovery logic.
func TestQueuedWorkSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	task := NewTask("queued-survives", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{Greeting: in.Name}, nil
	})

	q1, err := Open(WithStorage(File(path)), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	// Enqueue without delay but close immediately; whether the first run
	// completes or not, the delayed one must still be QUEUED in the file.
	h1, _ := Enqueue(context.Background(), q1, task, greetIn{Name: "first"})
	h2, _ := Enqueue(context.Background(), q1, task, greetIn{Name: "second"}, WithDelay(50*time.Millisecond))
	cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	_ = q1.Close(cctx)
	ccancel()
	_ = h1
	_ = h2

	q2, err := Open(WithStorage(File(path)), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		_ = q2.Close(c)
	})
	Register(q2, task)

	waitFor(t, 5*time.Second, func() bool {
		snap, _ := q2.Execution(context.Background(), h2.RunID())
		return snap != nil && snap.Status == StatusSucceeded
	})
}
