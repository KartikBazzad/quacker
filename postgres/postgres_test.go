package postgres

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/kartikbazzad/quacker"
	"github.com/kartikbazzad/quacker/driver"
	"github.com/kartikbazzad/quacker/internal/store"
)

func testDSN(t *testing.T) string {
	t.Helper()
	d := os.Getenv("QUACKER_TEST_POSTGRES_DSN")
	if d == "" {
		t.Skip("set QUACKER_TEST_POSTGRES_DSN to run Postgres integration tests")
	}
	return d
}

// resetDB drops quacker's tables so each test starts clean.
func resetDB(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, tbl := range []string{
		"step_journal", "steps", "runs", "logs", "events",
		"event_subscriptions", "crons", "schema_migrations",
	} {
		if _, err := db.Exec("DROP TABLE IF EXISTS " + tbl + " CASCADE"); err != nil {
			t.Fatalf("drop %s: %v", tbl, err)
		}
	}
}

func openQ(t *testing.T, dsn string, opts ...quacker.Option) *quacker.Quacker {
	t.Helper()
	all := append([]quacker.Option{
		quacker.WithStorage(quacker.Postgres(dsn)),
		quacker.WithPollInterval(5 * time.Millisecond),
	}, opts...)
	q, err := quacker.Open(all...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = q.Close(ctx)
	})
	return q
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within", d)
}

// TestPostgresMigrationsIdempotent: opening an already-migrated database is a
// no-op.
func TestPostgresMigrationsIdempotent(t *testing.T) {
	dsn := testDSN(t)
	resetDB(t, dsn)
	q1 := openQ(t, dsn)
	_ = q1
	q2 := openQ(t, dsn) // second Open re-runs migrate against version 1
	task := quacker.NewTask("pg.idem", func(ctx context.Context, in string) (string, error) {
		return in, nil
	})
	h, err := quacker.Enqueue(context.Background(), q2, task, "ok")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := h.Result(context.Background()); err != nil || out != "ok" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

// TestPostgresWithDB: a caller-owned pool is reused, migrated, and left open
// (and usable) after the engine closes.
func TestPostgresWithDB(t *testing.T) {
	dsn := testDSN(t)
	resetDB(t, dsn)
	ctx := context.Background()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(4)

	q, err := quacker.Open(
		quacker.WithStorage(quacker.PostgresWithDB(db)),
		quacker.WithPollInterval(5*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	task := quacker.NewTask("pg.reuse", func(ctx context.Context, in string) (string, error) {
		return "hi " + in, nil
	})
	h, err := quacker.Enqueue(ctx, q, task, "x")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := h.Result(ctx); err != nil || out != "hi x" {
		t.Fatalf("out=%q err=%v", out, err)
	}
	if err := q.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// The pool outlives the engine: still alive and holding the run.
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("external pool closed by quacker: %v", err)
	}
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM runs`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("runs=%d err=%v", n, err)
	}
}

// TestPostgresTaskAndWorkflow: basic run lifecycle plus a DAG with DepOutput.
func TestPostgresTaskAndWorkflow(t *testing.T) {
	dsn := testDSN(t)
	resetDB(t, dsn)
	q := openQ(t, dsn)
	ctx := context.Background()

	task := quacker.NewTask("pg.echo", func(ctx context.Context, in string) (string, error) {
		return "hi " + in, nil
	})
	h, err := quacker.Enqueue(ctx, q, task, "x")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := h.Result(ctx); err != nil || out != "hi x" {
		t.Fatalf("out=%q err=%v", out, err)
	}
	snap, err := q.Execution(ctx, h.RunID())
	if err != nil || snap.Status != quacker.StatusSucceeded || snap.Steps[0].Output == nil {
		t.Fatalf("snapshot = %+v err=%v", snap, err)
	}

	charge := quacker.NewTask("pg.charge", func(ctx context.Context, in string) (string, error) {
		return "charged " + in, nil
	})
	ship := quacker.NewTask("pg.ship", func(ctx context.Context, in string) (string, error) {
		rec, err := quacker.DepOutput[string](ctx, "charge")
		if err != nil {
			return "", err
		}
		return "shipped(" + rec + ")", nil
	})
	wf := quacker.NewWorkflow[string]("pg.flow",
		quacker.Step("charge", charge),
		quacker.Step("ship", ship, "charge"),
	)
	wh, err := quacker.EnqueueWorkflow[string](ctx, q, wf, "o-1")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := wh.Result(ctx); err != nil || out != "shipped(charged o-1)" {
		t.Fatalf("workflow out=%q err=%v", out, err)
	}
}

// TestPostgresDurable: suspend/resume (sleep + event wait) on Postgres.
func TestPostgresDurable(t *testing.T) {
	dsn := testDSN(t)
	resetDB(t, dsn)
	q := openQ(t, dsn)
	ctx := context.Background()

	task := quacker.NewTask("pg.durable", func(ctx context.Context, in string) (string, error) {
		if err := quacker.SleepDurable(ctx, 40*time.Millisecond); err != nil {
			return "", err
		}
		return quacker.WaitFor[string](ctx, "pg.event", 5*time.Second)
	})
	h, err := quacker.Enqueue(ctx, q, task, "go")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		s, err := q.Execution(ctx, h.RunID())
		return err == nil && len(s.Steps) == 1 && s.Steps[0].WaitEvent == "pg.event"
	})
	if _, err := q.Emit(ctx, "pg.event", "delivered"); err != nil {
		t.Fatal(err)
	}
	if out, err := h.Result(ctx); err != nil || out != "delivered" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

// TestPostgresTwoEngines: two engines share one database and split the work.
func TestPostgresTwoEngines(t *testing.T) {
	dsn := testDSN(t)
	resetDB(t, dsn)
	a := openQ(t, dsn, quacker.WithPollInterval(10*time.Millisecond))
	b := openQ(t, dsn, quacker.WithPollInterval(10*time.Millisecond))
	ctx := context.Background()

	task := quacker.NewTask("pg.shared", func(ctx context.Context, in int) (int, error) {
		return in * 2, nil
	})
	inputs := make([]int, 20)
	for i := range inputs {
		inputs[i] = i
	}
	hs, err := quacker.EnqueueBatch(ctx, a, task, inputs)
	if err != nil {
		t.Fatal(err)
	}
	for i, h := range hs {
		out, err := h.Result(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if out != i*2 {
			t.Fatalf("out[%d] = %d, want %d", i, out, i*2)
		}
	}
	// Node B's view reads the shared DB, so it sees all 20 regardless of which
	// node executed them.
	waitFor(t, 5*time.Second, func() bool {
		s, err := b.Runs(ctx, quacker.RunFilter{Workflow: "pg.shared", Status: quacker.StatusSucceeded})
		return err == nil && len(s) == 20
	})
}

// TestPostgresUniqueReuse: a unique key held by one engine's run is reused by
// another engine sharing the database.
func TestPostgresUniqueReuse(t *testing.T) {
	dsn := testDSN(t)
	resetDB(t, dsn)
	a := openQ(t, dsn)
	b := openQ(t, dsn)
	ctx := context.Background()

	task := quacker.NewTask("pg.unique", func(ctx context.Context, in string) (string, error) {
		return in, nil
	}, quacker.WithUnique(func(in string) string { return in }))
	// Scheduled ahead so the run stays QUEUED and the key stays held.
	future := quacker.WithRunAt(time.Now().Add(time.Hour))

	h1, err := quacker.Enqueue(ctx, a, task, "k", future)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := quacker.Enqueue(ctx, b, task, "k", future)
	if err != nil {
		t.Fatal(err)
	}
	if h1.RunID() != h2.RunID() {
		t.Fatalf("cross-engine reuse: %s vs %s", h1.RunID(), h2.RunID())
	}
	if h3, err := quacker.Enqueue(ctx, b, task, "other", future); err != nil {
		t.Fatal(err)
	} else if h3.RunID() == h1.RunID() {
		t.Fatal("distinct key reused")
	}

	errTask := quacker.NewTask("pg.unique.err", func(ctx context.Context, in string) (string, error) {
		return in, nil
	}, quacker.WithUnique(func(in string) string { return in }),
		quacker.WithUniqueConflict(quacker.UniqueError))
	if _, err := quacker.Enqueue(ctx, a, errTask, "e", future); err != nil {
		t.Fatal(err)
	}
	if _, err := quacker.Enqueue(ctx, b, errTask, "e", future); !errors.Is(err, quacker.ErrDuplicateJob) {
		t.Fatalf("want ErrDuplicateJob, got %v", err)
	}
}

// TestPostgresRunPause: a run paused by one engine is visible to another and
// resumes on the engine that executes it.
func TestPostgresRunPause(t *testing.T) {
	dsn := testDSN(t)
	resetDB(t, dsn)
	a := openQ(t, dsn)
	b := openQ(t, dsn)
	ctx := context.Background()

	task := quacker.NewTask("pg.runpause", func(ctx context.Context, in string) (string, error) {
		return in, nil
	})
	h, err := quacker.Enqueue(ctx, a, task, "x", quacker.WithRunAt(time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	if err := b.PauseRun(ctx, h.RunID()); err != nil {
		t.Fatal(err)
	}
	if snap, err := a.Execution(ctx, h.RunID()); err != nil || snap.Status != quacker.StatusPaused {
		t.Fatalf("status = %+v err=%v, want PAUSED", snap, err)
	}
	// Make a the only engine, then resume.
	if err := b.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := a.ResumeRun(ctx, h.RunID()); err != nil {
		t.Fatal(err)
	}
	if out, err := h.Result(ctx); err != nil || out != "x" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

// TestPostgresRunPauseCrossEngineBoundary: engine A pauses a run whose step is
// executing on engine B. A cannot interrupt B's in-flight step, but the pause
// takes effect at the next boundary — the dependent step is not claimed until
// B resumes.
func TestPostgresRunPauseCrossEngineBoundary(t *testing.T) {
	dsn := testDSN(t)
	resetDB(t, dsn)
	// Only B has the matching worker label, so B executes the labeled steps.
	a := openQ(t, dsn, quacker.WithWorkerLabels("observer"))
	b := openQ(t, dsn, quacker.WithWorkerLabels("worker-b"))
	ctx := context.Background()

	started := make(chan struct{})
	release := make(chan struct{})
	step1 := quacker.NewTask("pg.x1", func(ctx context.Context, in string) (string, error) {
		close(started)
		<-release // ignores ctx
		return "A", nil
	}, quacker.WithLabels("worker-b"))
	step2 := quacker.NewTask("pg.x2", func(ctx context.Context, in string) (string, error) {
		return "B", nil
	}, quacker.WithLabels("worker-b"))
	wf := quacker.NewWorkflow[string]("pg.xwf",
		quacker.Step("a", step1), quacker.Step("b", step2, "a"))

	h, err := quacker.EnqueueWorkflow[string](ctx, b, wf, "in")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("step a never started on b")
	}
	if err := a.PauseRun(ctx, h.RunID()); err != nil {
		t.Fatal(err)
	}
	close(release)
	waitFor(t, 5*time.Second, func() bool {
		s, err := a.Execution(ctx, h.RunID())
		if err != nil {
			return false
		}
		for _, st := range s.Steps {
			if st.Name == "a" && st.Status == quacker.StatusSucceeded {
				return true
			}
		}
		return false
	})
	time.Sleep(80 * time.Millisecond)
	snap, err := a.Execution(ctx, h.RunID())
	if err != nil || snap.Status != quacker.StatusPaused {
		t.Fatalf("run = %+v err=%v, want PAUSED", snap, err)
	}
	for _, st := range snap.Steps {
		if st.Name == "b" && st.Status != quacker.StatusQueued {
			t.Fatalf("step b = %s, want QUEUED while paused", st.Status)
		}
	}
	if err := b.ResumeRun(ctx, h.RunID()); err != nil {
		t.Fatal(err)
	}
	if out, err := h.Result(ctx); err != nil || out != "B" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

// TestPostgresQueuePause: a pause set by one engine holds claims on another,
// and a resume from either engine releases them.
func TestPostgresQueuePause(t *testing.T) {
	dsn := testDSN(t)
	resetDB(t, dsn)
	a := openQ(t, dsn)
	b := openQ(t, dsn)
	ctx := context.Background()

	if err := a.PauseQueue(ctx, "shared"); err != nil {
		t.Fatal(err)
	}
	task := quacker.NewTask("pg.paused", func(ctx context.Context, in string) (string, error) {
		return in, nil
	}, quacker.Queue("shared"))
	h, err := quacker.Enqueue(ctx, b, task, "x")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if snap, err := a.Execution(ctx, h.RunID()); err != nil || snap.Status != quacker.StatusQueued {
		t.Fatalf("run = %+v err=%v, want QUEUED", snap, err)
	}
	if paused, err := b.PausedQueues(ctx); err != nil || len(paused) != 1 {
		t.Fatalf("b paused = %v err=%v, want [shared]", paused, err)
	}
	if err := b.ResumeQueue(ctx, "shared"); err != nil {
		t.Fatal(err)
	}
	if out, err := h.Result(ctx); err != nil || out != "x" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

// TestPostgresLeaseReap: a store-level claim carries a worker + lease, and an
// expired lease is re-queued by the reaper.
func TestPostgresLeaseReap(t *testing.T) {
	dsn := testDSN(t)
	resetDB(t, dsn)
	s, err := store.Open(driver.Config{Driver: "postgres", DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if !s.SupportsLeases() {
		t.Fatal("Postgres should support leases")
	}
	ctx := context.Background()
	now := time.Now().UnixNano()
	sec := int64(time.Second)
	run := &store.Run{ID: "r", Workflow: "w", Kind: store.KindTask, Status: store.StatusQueued, Queue: "q", RunAt: now, CreatedAt: now, MaxAttempts: 1}
	step := &store.Step{ID: "r/s", RunID: "r", Name: "s", Task: "t", Ord: 0, Status: store.StatusQueued, Queue: "q", RunAt: now, CreatedAt: now, MaxAttempts: 1}
	if err := s.CreateRun(ctx, run, []*store.Step{step}); err != nil {
		t.Fatal(err)
	}
	claims, err := s.ClaimDue(ctx, "q", 1, now, 0, 0, nil, "w1", now+sec)
	if err != nil || len(claims) != 1 || claims[0].Step.WorkerID != "w1" {
		t.Fatalf("claim = %+v err=%v", claims, err)
	}
	if n, err := s.ReapExpired(ctx, now+sec/2, 10); err != nil || n != 0 {
		t.Fatalf("early reap = %d err=%v, want 0", n, err)
	}
	if n, err := s.ReapExpired(ctx, now+2*sec, 10); err != nil || n != 1 {
		t.Fatalf("reap = %d err=%v, want 1", n, err)
	}
	if _, err := s.ClaimDue(ctx, "q", 1, now+2*sec, 0, 0, nil, "w2", now+3*sec); err != nil {
		t.Fatal(err)
	}
}

// TestPostgresLabelsAndPurge: worker-label routing and retention.
func TestPostgresLabelsAndPurge(t *testing.T) {
	dsn := testDSN(t)
	resetDB(t, dsn)
	q := openQ(t, dsn, quacker.WithWorkerLabels("gpu"))
	ctx := context.Background()

	task := quacker.NewTask("pg.gpu", func(ctx context.Context, in string) (string, error) {
		return in, nil
	}, quacker.WithLabels("gpu"))
	h, err := quacker.Enqueue(ctx, q, task, "g")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	res, err := q.Purge(ctx, quacker.PurgeOptions{OlderThan: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if res.Runs != 1 {
		t.Fatalf("purged %d runs, want 1", res.Runs)
	}
	if _, err := q.Execution(ctx, h.RunID()); !errors.Is(err, quacker.ErrNotFound) {
		t.Fatalf("run still present after purge: %v", err)
	}
}
