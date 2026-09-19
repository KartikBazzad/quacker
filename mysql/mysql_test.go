package mysql

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/kartikbazzad/quacker"
)

func testDSN(t *testing.T) string {
	t.Helper()
	d := os.Getenv("QUACKER_TEST_MYSQL_DSN")
	if d == "" {
		t.Skip("set QUACKER_TEST_MYSQL_DSN to run MySQL integration tests")
	}
	return d
}

// resetDB drops quacker's tables so each test starts clean.
func resetDB(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("SET FOREIGN_KEY_CHECKS=0"); err != nil {
		t.Fatalf("disable fk checks: %v", err)
	}
	for _, tbl := range []string{
		"step_journal", "steps", "runs", "logs", "events",
		"event_subscriptions", "crons", "locks", "schema_migrations",
	} {
		if _, err := db.Exec("DROP TABLE IF EXISTS " + tbl); err != nil {
			t.Fatalf("drop %s: %v", tbl, err)
		}
	}
}

func openQ(t *testing.T, dsn string, opts ...quacker.Option) *quacker.Quacker {
	t.Helper()
	all := append([]quacker.Option{
		quacker.WithStorage(quacker.Driver("mysql", dsn)),
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
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met before deadline")
}

func TestMySQLTaskAndWorkflow(t *testing.T) {
	dsn := testDSN(t)
	resetDB(t, dsn)
	q := openQ(t, dsn)
	ctx := context.Background()

	task := quacker.NewTask("my.echo", func(ctx context.Context, in string) (string, error) {
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

	charge := quacker.NewTask("my.charge", func(ctx context.Context, in string) (string, error) {
		return "charged " + in, nil
	})
	ship := quacker.NewTask("my.ship", func(ctx context.Context, in string) (string, error) {
		rec, err := quacker.DepOutput[string](ctx, "charge")
		if err != nil {
			return "", err
		}
		return "shipped(" + rec + ")", nil
	})
	wf := quacker.NewWorkflow[string]("my.flow",
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

func TestMySQLDurable(t *testing.T) {
	dsn := testDSN(t)
	resetDB(t, dsn)
	q := openQ(t, dsn)
	ctx := context.Background()

	task := quacker.NewTask("my.durable", func(ctx context.Context, in string) (string, error) {
		if err := quacker.SleepDurable(ctx, 40*time.Millisecond); err != nil {
			return "", err
		}
		return quacker.WaitFor[string](ctx, "my.event", 5*time.Second)
	})
	h, err := quacker.Enqueue(ctx, q, task, "go")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		s, err := q.Execution(ctx, h.RunID())
		return err == nil && len(s.Steps) == 1 && s.Steps[0].WaitEvent == "my.event"
	})
	if _, err := q.Emit(ctx, "my.event", "delivered"); err != nil {
		t.Fatal(err)
	}
	if out, err := h.Result(ctx); err != nil || out != "delivered" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

func TestMySQLTwoEngines(t *testing.T) {
	dsn := testDSN(t)
	resetDB(t, dsn)
	a := openQ(t, dsn, quacker.WithPollInterval(10*time.Millisecond))
	b := openQ(t, dsn, quacker.WithPollInterval(10*time.Millisecond))
	ctx := context.Background()

	task := quacker.NewTask("my.shared", func(ctx context.Context, in int) (int, error) {
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
	waitFor(t, 5*time.Second, func() bool {
		s, err := b.Runs(ctx, quacker.RunFilter{Workflow: "my.shared", Status: quacker.StatusSucceeded})
		return err == nil && len(s) == 20
	})
}

func TestMySQLLabelsAndPurge(t *testing.T) {
	dsn := testDSN(t)
	resetDB(t, dsn)
	q := openQ(t, dsn, quacker.WithWorkerLabels("gpu"))
	ctx := context.Background()

	task := quacker.NewTask("my.gpu", func(ctx context.Context, in string) (string, error) {
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

func TestMySQLUniqueReuse(t *testing.T) {
	dsn := testDSN(t)
	resetDB(t, dsn)
	a := openQ(t, dsn)
	b := openQ(t, dsn)
	ctx := context.Background()

	task := quacker.NewTask("my.unique", func(ctx context.Context, in string) (string, error) {
		return in, nil
	}, quacker.WithUnique(func(in string) string { return in }))
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

	errTask := quacker.NewTask("my.unique.err", func(ctx context.Context, in string) (string, error) {
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

func TestMySQLQueuePause(t *testing.T) {
	dsn := testDSN(t)
	resetDB(t, dsn)
	a := openQ(t, dsn)
	b := openQ(t, dsn)
	ctx := context.Background()

	if err := a.PauseQueue(ctx, "shared"); err != nil {
		t.Fatal(err)
	}
	task := quacker.NewTask("my.paused", func(ctx context.Context, in string) (string, error) {
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
	if err := b.ResumeQueue(ctx, "shared"); err != nil {
		t.Fatal(err)
	}
	if out, err := h.Result(ctx); err != nil || out != "x" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

func TestMySQLRunPause(t *testing.T) {
	dsn := testDSN(t)
	resetDB(t, dsn)
	a := openQ(t, dsn)
	b := openQ(t, dsn)
	ctx := context.Background()

	task := quacker.NewTask("my.runpause", func(ctx context.Context, in string) (string, error) {
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

func TestMySQLRunPauseCrossEngineBoundary(t *testing.T) {
	dsn := testDSN(t)
	resetDB(t, dsn)
	a := openQ(t, dsn, quacker.WithWorkerLabels("observer"))
	b := openQ(t, dsn, quacker.WithWorkerLabels("worker-b"))
	ctx := context.Background()

	started := make(chan struct{})
	release := make(chan struct{})
	step1 := quacker.NewTask("my.x1", func(ctx context.Context, in string) (string, error) {
		close(started)
		<-release
		return "A", nil
	}, quacker.WithLabels("worker-b"))
	step2 := quacker.NewTask("my.x2", func(ctx context.Context, in string) (string, error) {
		return "B", nil
	}, quacker.WithLabels("worker-b"))
	wf := quacker.NewWorkflow[string]("my.xwf",
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

func TestMySQLUniqueReplaceCrossEngine(t *testing.T) {
	dsn := testDSN(t)
	resetDB(t, dsn)
	a := openQ(t, dsn, quacker.WithWorkerLabels("observer"))
	b := openQ(t, dsn, quacker.WithWorkerLabels("worker-b"))
	ctx := context.Background()

	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var calls atomic.Int32
	task := quacker.NewTask("my.repl", func(ctx context.Context, in string) (string, error) {
		if calls.Add(1) == 1 {
			once.Do(func() { close(started) })
			<-release
		}
		return "ok", nil
	}, quacker.WithUnique(func(in string) string { return in }),
		quacker.WithUniqueConflict(quacker.UniqueReplace),
		quacker.WithLabels("worker-b"))

	old, err := quacker.Enqueue(ctx, b, task, "k")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("old run never started")
	}
	repl, err := quacker.Enqueue(ctx, a, task, "k")
	if err != nil {
		t.Fatal(err)
	}
	if repl.RunID() == old.RunID() {
		t.Fatal("replace reused the old run id")
	}
	waitFor(t, 5*time.Second, func() bool {
		s, err := a.Execution(ctx, old.RunID())
		return err == nil && s.Status == quacker.StatusCancelled
	})
	close(release)
	waitFor(t, 5*time.Second, func() bool {
		s, err := a.Execution(ctx, repl.RunID())
		return err == nil && s.Status == quacker.StatusSucceeded
	})
}

func TestMySQLUniqueRaceCrossEngine(t *testing.T) {
	dsn := testDSN(t)
	resetDB(t, dsn)
	a := openQ(t, dsn)
	b := openQ(t, dsn)
	ctx := context.Background()

	release := make(chan struct{})
	defer close(release)
	task := quacker.NewTask("my.race", func(ctx context.Context, in string) (string, error) {
		<-release
		return in, nil
	}, quacker.WithUnique(func(in string) string { return in }),
		quacker.WithUniqueConflict(quacker.UniqueError))

	start := make(chan struct{})
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i, q := range []*quacker.Quacker{a, b} {
		wg.Add(1)
		go func(i int, q *quacker.Quacker) {
			defer wg.Done()
			<-start
			_, errs[i] = quacker.Enqueue(ctx, q, task, "same")
		}(i, q)
	}
	close(start)
	wg.Wait()

	var ok, dup int
	for _, err := range errs {
		switch {
		case err == nil:
			ok++
		case errors.Is(err, quacker.ErrDuplicateJob):
			dup++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if ok != 1 || dup != 1 {
		t.Fatalf("ok=%d dup=%d, want 1/1 (errs=%v)", ok, dup, errs)
	}
	runs, err := a.Runs(ctx, quacker.RunFilter{Workflow: "my.race"})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 {
		t.Fatalf("persisted %d runs, want 1", len(runs))
	}
}

func TestMySQLEnqueueTx(t *testing.T) {
	dsn := testDSN(t)
	resetDB(t, dsn)
	q := openQ(t, dsn)
	ctx := context.Background()
	task := quacker.NewTask("my.tx", func(ctx context.Context, in string) (string, error) { return in, nil })

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`DROP TABLE IF EXISTS biz`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE biz (id VARCHAR(32) PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO biz (id) VALUES ('a')`); err != nil {
		t.Fatal(err)
	}
	id, err := quacker.EnqueueTx(ctx, tx, q, task, "x")
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool {
		s, err := q.Execution(ctx, id)
		return err == nil && s.Status == quacker.StatusSucceeded
	})

	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	id2, err := quacker.EnqueueTx(ctx, tx2, q, task, "y")
	if err != nil {
		t.Fatal(err)
	}
	if err := tx2.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Execution(ctx, id2); !errors.Is(err, quacker.ErrNotFound) {
		t.Fatalf("rolled-back run is visible: %v", err)
	}
}

func TestMySQLDeadLetter(t *testing.T) {
	dsn := testDSN(t)
	resetDB(t, dsn)
	q := openQ(t, dsn)
	ctx := context.Background()
	var calls atomic.Int32
	task := quacker.NewTask("my.dlq", func(ctx context.Context, in string) (string, error) {
		if calls.Add(1) == 1 {
			return "", errors.New("boom")
		}
		return in, nil
	}, quacker.WithDeadLetter())

	h, err := quacker.Enqueue(ctx, q, task, "x")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool {
		s, err := q.Execution(ctx, h.RunID())
		return err == nil && s.Status == quacker.StatusFailed
	})
	if letters, err := q.DeadLetters(ctx, quacker.DeadLetterFilter{Workflow: "my.dlq"}); err != nil || len(letters) != 1 {
		t.Fatalf("dead letters = %+v err=%v", letters, err)
	}
	if err := q.RetryDeadLetter(ctx, h.RunID()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool {
		s, err := q.Execution(ctx, h.RunID())
		return err == nil && s.Status == quacker.StatusSucceeded
	})
}

func TestMySQLWithDB(t *testing.T) {
	dsn := testDSN(t)
	resetDB(t, dsn)
	ctx := context.Background()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(4)

	q, err := quacker.Open(
		quacker.WithStorage(quacker.Driver("mysql", "").WithDB(db)),
		quacker.WithPollInterval(5*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	task := quacker.NewTask("my.reuse", func(ctx context.Context, in string) (string, error) {
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
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("external pool closed by quacker: %v", err)
	}
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM runs`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("runs=%d err=%v", n, err)
	}
}
