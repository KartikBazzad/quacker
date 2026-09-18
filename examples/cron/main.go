// Cron and delayed triggers: a task that runs on a schedule, plus one-off
// delayed runs, cancellable while waiting.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/kartikbazzad/quacker"
)

type Report struct{ Day string }

func main() {
	q, err := quacker.Open()
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close(context.Background())

	report := quacker.NewTask("send-report", func(ctx context.Context, r Report) (string, error) {
		log := quacker.TaskLogger(ctx)
		log.Info("generating report", "day", r.Day)
		return "report for " + r.Day, nil
	})

	// Standard 5-field specs and @descriptors both work. Note: "@every"
	// intervals round up to 1 second.
	if err := quacker.Cron(q, "nightly", "@every 2s", report, Report{Day: "yesterday"}); err != nil {
		log.Fatal(err)
	}

	// One-off delayed run.
	h, err := quacker.Enqueue(context.Background(), q, report, Report{Day: "today"},
		quacker.WithDelay(1*time.Second), quacker.WithPriority(5))
	if err != nil {
		log.Fatal(err)
	}

	for _, c := range q.Crons() {
		fmt.Printf("cron %s (%s) next fire: %s\n", c.Name, c.Spec, c.Next.Format(time.Kitchen))
	}

	out, err := h.Result(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(out)

	_ = q.RemoveCron("nightly")
}
