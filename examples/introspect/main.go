// Live introspection: watch a slow workflow's state change in real time
// while it keeps executing — nothing is paused or slowed down.
package main

import (
	"context"
	"fmt"
	json "github.com/goccy/go-json"
	"log"
	"time"

	"github.com/kartikbazzad/quacker"
)

type Job struct{ Name string }

func main() {
	q, err := quacker.Open(
		quacker.WithQueue("workers", 2),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close(context.Background())

	process := quacker.NewTask("process", func(ctx context.Context, j Job) (string, error) {
		quacker.TaskLogger(ctx).Info("working", "job", j.Name)
		time.Sleep(300 * time.Millisecond)
		return "done:" + j.Name, nil
	}, quacker.Queue("workers"))

	wf := quacker.NewWorkflow[Job]("pipeline",
		quacker.Step("process", process),
		quacker.Step("audit", process, "process"),
	)
	h, err := quacker.EnqueueWorkflow[string](context.Background(), q, wf, Job{Name: "demo"})
	if err != nil {
		log.Fatal(err)
	}

	// 1) Live transition stream — push, no polling.
	events, stop := q.Subscribe(h.RunID())
	defer stop()
	go func() {
		for ev := range events {
			what := ev.Step
			if what == "" {
				what = "run"
			}
			fmt.Printf("  event: %-8s %-10s -> %s\n", what, ev.From, ev.To)
		}
	}()

	// 2) Point-in-time snapshots — safe to hammer while workers write.
	ticker := time.NewTicker(120 * time.Millisecond)
	defer ticker.Stop()
	result := make(chan error, 1)
	go func() { _, err := h.Result(context.Background()); result <- err }()
	for done := false; !done; {
		select {
		case err := <-result:
			if err != nil {
				log.Fatal(err)
			}
			done = true
		case <-ticker.C:
			snap, err := q.Execution(context.Background(), h.RunID())
			if err != nil {
				log.Fatal(err)
			}
			b, _ := json.Marshal(snap)
			fmt.Printf("  snapshot: %s\n", b)
			m, _ := q.Metrics(context.Background())
			fmt.Printf("  metrics:  %+v\n", m.Runs)
		}
	}

	snap, _ := q.Execution(context.Background(), h.RunID())
	fmt.Printf("final: %s (steps: %d)\n", snap.Status, len(snap.Steps))
	logs, _ := q.Logs(context.Background(), h.RunID(), 10)
	for _, l := range logs {
		fmt.Printf("  log: [%s] %s\n", l.Level, l.Message)
	}
}
