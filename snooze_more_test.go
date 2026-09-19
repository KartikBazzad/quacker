package quacker

import (
	"context"
	"testing"
	"time"
)

func TestSnoozeDAGMidFlight(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	a := NewTask("snm.a", func(ctx context.Context, in string) (string, error) { return "A", nil })
	b := NewTask("snm.b", func(ctx context.Context, in string) (string, error) { return "B", nil })
	wf := NewWorkflow[string]("snm.wf", Step("a", a), Step("b", b, "a"))

	h, err := EnqueueWorkflow[string](ctx, q, wf, "in", WithRunAt(time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(50 * time.Millisecond)
	if err := q.Snooze(ctx, h.RunID(), until); err != nil {
		t.Fatal(err)
	}
	snap, err := q.Execution(ctx, h.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if !snap.RunAt.Equal(until) {
		t.Fatalf("run_at = %s, want %s", snap.RunAt, until)
	}
	var stepA, stepB *StepState
	for i := range snap.Steps {
		switch snap.Steps[i].Name {
		case "a":
			stepA = &snap.Steps[i]
		case "b":
			stepB = &snap.Steps[i]
		}
	}
	if stepA == nil || stepA.Status != StatusQueued || !stepA.RunAt.Equal(until) {
		t.Fatalf("step a = %+v, want QUEUED at %s", stepA, until)
	}
	if stepB == nil || stepB.Status != StatusBlocked {
		t.Fatalf("step b = %+v, want BLOCKED", stepB)
	}
	if out, err := h.Result(ctx); err != nil || out != "B" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

func TestSnoozePersists(t *testing.T) {
	path := t.TempDir() + "/snz.db"
	ctx := context.Background()
	q1, err := Open(WithStorage(File(path)), WithPollInterval(2*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	task := NewTask("snm.file", func(ctx context.Context, in string) (string, error) { return in, nil })
	h, err := Enqueue(ctx, q1, task, "x", WithRunAt(time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(2 * time.Hour)
	if err := q1.Snooze(ctx, h.RunID(), until); err != nil {
		t.Fatal(err)
	}
	closeQ(t, q1)

	q2, err := Open(WithStorage(File(path)), WithPollInterval(2*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer closeQ(t, q2)
	Register(q2, task)
	if snap, err := q2.Execution(ctx, h.RunID()); err != nil || !snap.RunAt.Equal(until) || snap.Status != StatusQueued {
		t.Fatalf("reopened = %+v err=%v, want QUEUED at %s", snap, err, until)
	}
	// Bring it forward and run.
	if err := q2.SnoozeFor(ctx, h.RunID(), time.Millisecond); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool {
		s, err := q2.Execution(ctx, h.RunID())
		return err == nil && s.Status == StatusSucceeded
	})
}

func TestSnoozeForAndPausedRun(t *testing.T) {
	q := newTestQ(t)
	ctx := context.Background()
	task := NewTask("snm.paused", func(ctx context.Context, in string) (string, error) { return in, nil })

	h, err := Enqueue(ctx, q, task, "x", WithRunAt(time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	if err := q.PauseRun(ctx, h.RunID()); err != nil {
		t.Fatal(err)
	}
	// Snoozing a paused run is allowed (no RUNNING step) and does not resume it.
	if err := q.SnoozeFor(ctx, h.RunID(), time.Hour); err != nil {
		t.Fatal(err)
	}
	if snap, err := q.Execution(ctx, h.RunID()); err != nil || snap.Status != StatusPaused {
		t.Fatalf("status = %+v err=%v, want still PAUSED", snap, err)
	}
	if err := q.ResumeRun(ctx, h.RunID()); err != nil {
		t.Fatal(err)
	}
	if out, err := h.Result(ctx); err != nil || out != "x" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}
