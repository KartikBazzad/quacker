package store

import (
	"context"
	"testing"
	"time"
)

func newRunStep(id, queue string, now int64) (*Run, []*Step) {
	run := &Run{ID: id, Workflow: "w", Kind: KindTask, Status: StatusQueued, Queue: queue, RunAt: now, CreatedAt: now, MaxAttempts: 1}
	step := &Step{ID: id + "/s", RunID: id, Name: "s", Task: "t", Ord: 0, Status: StatusQueued, Queue: queue, RunAt: now, CreatedAt: now, MaxAttempts: 1}
	return run, []*Step{step}
}

func stepStr(t *testing.T, s *Store, id, col string) string {
	t.Helper()
	var v string
	if err := s.Read().QueryRow(`SELECT `+col+` FROM steps WHERE id=?`, id).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func stepInt(t *testing.T, s *Store, id, col string) int64 {
	t.Helper()
	var v int64
	if err := s.Read().QueryRow(`SELECT `+col+` FROM steps WHERE id=?`, id).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func runStatus(t *testing.T, s *Store, id string) string {
	t.Helper()
	var st string
	if err := s.Read().QueryRow(`SELECT status FROM runs WHERE id=?`, id).Scan(&st); err != nil {
		t.Fatal(err)
	}
	return st
}

func TestLeaseStampHeartbeatReap(t *testing.T) {
	s, err := Open(Config{Mode: ModeEphemeral})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	now := nowUnix()
	sec := int64(time.Second)

	run, steps := newRunStep("r1", "q", now)
	if err := s.CreateRun(ctx, run, steps); err != nil {
		t.Fatal(err)
	}
	lease := now + sec
	claims, err := s.ClaimDue(ctx, "q", 1, now, 0, 0, nil, "w1", lease)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim = %+v err=%v", claims, err)
	}
	if w, le := stepStr(t, s, "r1/s", "worker_id"), stepInt(t, s, "r1/s", "lease_expires_at"); w != "w1" || le != lease {
		t.Fatalf("lease stamp = (%q,%d), want (w1,%d)", w, le, lease)
	}

	// Heartbeat extends the lease; a reap before expiry leaves the step alone.
	if err := s.HeartbeatWorker(ctx, "w1", now+2*sec); err != nil {
		t.Fatal(err)
	}
	if n, err := s.ReapExpired(ctx, now+sec+sec/2, 10); err != nil || n != 0 {
		t.Fatalf("early reap = %d err=%v, want 0", n, err)
	}
	if st := stepStr(t, s, "r1/s", "status"); st != StatusRunning {
		t.Fatalf("status after early reap = %s, want RUNNING", st)
	}

	// A reap after expiry requeues the step and clears the lease.
	n, err := s.ReapExpired(ctx, now+3*sec, 10)
	if err != nil || n != 1 {
		t.Fatalf("reap = %d err=%v, want 1", n, err)
	}
	if st := stepStr(t, s, "r1/s", "status"); st != StatusQueued {
		t.Fatalf("status after reap = %s, want QUEUED", st)
	}
	if w, le := stepStr(t, s, "r1/s", "worker_id"), stepInt(t, s, "r1/s", "lease_expires_at"); w != "" || le != 0 {
		t.Fatalf("lease after reap = (%q,%d), want cleared", w, le)
	}

	// Another worker can claim it again.
	claims, err = s.ClaimDue(ctx, "q", 1, now+3*sec, 0, 0, nil, "w2", now+4*sec)
	if err != nil || len(claims) != 1 {
		t.Fatalf("reclaim = %+v err=%v", claims, err)
	}
	if w := stepStr(t, s, "r1/s", "worker_id"); w != "w2" {
		t.Fatalf("reclaim worker = %q, want w2", w)
	}
}

func TestInterruptWorkerIsScoped(t *testing.T) {
	s, err := Open(Config{Mode: ModeEphemeral})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	now := nowUnix()
	lease := now + int64(time.Second)
	for _, r := range []struct{ id, q string }{{"r1", "q1"}, {"r2", "q2"}} {
		run, steps := newRunStep(r.id, r.q, now)
		if err := s.CreateRun(ctx, run, steps); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.ClaimDue(ctx, "q1", 1, now, 0, 0, nil, "w1", lease); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimDue(ctx, "q2", 1, now, 0, 0, nil, "w2", lease); err != nil {
		t.Fatal(err)
	}
	if err := s.InterruptAll(ctx, "w1", now); err != nil {
		t.Fatal(err)
	}
	if st := stepStr(t, s, "r1/s", "status"); st != StatusInterrupted {
		t.Fatalf("w1 step = %s, want INTERRUPTED", st)
	}
	if st := runStatus(t, s, "r1"); st != StatusInterrupted {
		t.Fatalf("w1 run = %s, want INTERRUPTED", st)
	}
	if st := stepStr(t, s, "r2/s", "status"); st != StatusRunning {
		t.Fatalf("w2 step = %s, want RUNNING (untouched)", st)
	}
	if st := runStatus(t, s, "r2"); st != StatusRunning {
		t.Fatalf("w2 run = %s, want RUNNING (untouched)", st)
	}
}

func TestFireCronOnce(t *testing.T) {
	s, err := Open(Config{Mode: ModeEphemeral})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	now := nowUnix()
	next, following := now+100, now+200
	if err := s.UpsertCron(ctx, &Cron{Name: "c", Spec: "@every 1s", Task: "t", NextAt: next, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	run1, steps1 := newRunStep("cr1", "q", now)
	if fired, err := s.FireCron(ctx, "c", next, following, run1, steps1); err != nil || !fired {
		t.Fatalf("first fire = %v err=%v, want true", fired, err)
	}
	run2, steps2 := newRunStep("cr2", "q", now)
	if fired, err := s.FireCron(ctx, "c", next, following, run2, steps2); err != nil || fired {
		t.Fatalf("second fire = %v err=%v, want false (lost the race)", fired, err)
	}
	var runs int
	if err := s.Read().QueryRow(`SELECT COUNT(*) FROM runs`).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if runs != 1 {
		t.Fatalf("runs = %d, want 1 (only the winner enqueues)", runs)
	}
	var gotNext int64
	if err := s.Read().QueryRow(`SELECT next_at FROM crons WHERE name='c'`).Scan(&gotNext); err != nil {
		t.Fatal(err)
	}
	if gotNext != following {
		t.Fatalf("next_at = %d, want %d", gotNext, following)
	}
}
