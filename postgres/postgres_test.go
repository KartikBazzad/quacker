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

// TestPostgresLeaseReap: a store-level claim carries a worker + lease, and an
// expired lease is re-queued by the reaper.
func TestPostgresLeaseReap(t *testing.T) {
	dsn := testDSN(t)
	resetDB(t, dsn)
	s, err := store.Open(store.Config{Mode: store.ModePostgres, DSN: dsn})
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
