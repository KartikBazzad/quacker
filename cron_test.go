package quacker

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// TestSubSecondCron: "@every" below one second fires accurately. The stock
// cron parser cannot do this (ConstantDelaySchedule.Next is wrong for
// sub-second delays), so the engine schedules it itself.
func TestSubSecondCron(t *testing.T) {
	q := newTestQ(t)
	var fires atomic.Int32
	task := NewTask("subcron", func(ctx context.Context, in greetIn) (greetOut, error) {
		fires.Add(1)
		return greetOut{}, nil
	})
	if err := Cron(q, "fast", "@every 40ms", task, greetIn{}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return fires.Load() >= 3 })
}

// TestInvalidEveryCron: malformed or non-positive durations are rejected.
func TestInvalidEveryCron(t *testing.T) {
	q := newTestQ(t)
	task := NewTask("badcron", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	})
	for _, spec := range []string{"@every nope", "@every 0s", "@every -1s"} {
		if err := Cron(q, "bad", spec, task, greetIn{}); err == nil {
			t.Fatalf("Cron(%q) succeeded, want error", spec)
		}
	}
}

func openFileQ(t *testing.T, path string) *Quacker {
	t.Helper()
	q, err := Open(WithStorage(File(path)), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func closeQ(t *testing.T, q *Quacker) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = q.Close(ctx)
}

// TestPersistentCronArming: a cron persisted by one process re-arms in the
// next as soon as its task is registered — the app does not re-Cron.
func TestPersistentCronArming(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	task := NewTask("pcron", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	})

	q1 := openFileQ(t, path)
	if err := Cron(q1, "pcron-trig", "@every 200ms", task, greetIn{}); err != nil {
		t.Fatal(err)
	}
	closeQ(t, q1)

	var fires atomic.Int32
	task2 := NewTask("pcron", func(ctx context.Context, in greetIn) (greetOut, error) {
		fires.Add(1)
		return greetOut{}, nil
	})
	q2 := openFileQ(t, path)
	defer closeQ(t, q2)

	// Loaded from the store into the pending set, visible but not armed.
	var found bool
	for _, c := range q2.Crons() {
		if c.Name == "pcron-trig" {
			found = true
		}
	}
	if !found {
		t.Fatal("persisted cron not listed after reopen")
	}

	Register(q2, task2) // registration arms it
	waitFor(t, 3*time.Second, func() bool { return fires.Load() >= 1 })
}

// TestRemovePendingCron: RemoveCron clears a cron that was loaded but never
// armed (its task was never registered).
func TestRemovePendingCron(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	task := NewTask("rcron", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{}, nil
	})
	q1 := openFileQ(t, path)
	if err := Cron(q1, "rcron-trig", "@every 1h", task, greetIn{}); err != nil {
		t.Fatal(err)
	}
	closeQ(t, q1)

	q2 := openFileQ(t, path)
	defer closeQ(t, q2)
	if err := q2.RemoveCron("rcron-trig"); err != nil {
		t.Fatal(err)
	}
	for _, c := range q2.Crons() {
		if c.Name == "rcron-trig" {
			t.Fatal("cron still listed after RemoveCron")
		}
	}
}
