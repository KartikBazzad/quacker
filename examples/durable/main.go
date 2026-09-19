// Durable execution: a task that sleeps and waits for an event without
// holding a worker slot, resuming where it left off with side effects that
// run exactly once.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/kartikbazzad/quacker"
)

func main() {
	q, err := quacker.Open()
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close(context.Background())

	order := quacker.NewTask("orders.fulfill", func(ctx context.Context, id string) (string, error) {
		// RunOnce makes this side effect happen exactly once even though the
		// task replays from the top after each suspension.
		receipt, err := quacker.RunOnce(ctx, "reserve", func() (string, error) {
			return "reserved " + id, nil
		})
		if err != nil {
			return "", err
		}

		// Suspend for a while: no worker, queue, or key slot is held, and
		// with File storage this survives a process restart.
		if err := quacker.SleepDurable(ctx, 500*time.Millisecond); err != nil {
			return "", err
		}

		// Suspend until the payment event arrives (or time out).
		payment, err := quacker.WaitFor[string](ctx, "payment.received", 5*time.Second)
		if err != nil {
			return "", err
		}
		return receipt + " -> shipped (" + payment + ")", nil
	})

	h, err := quacker.Enqueue(context.Background(), q, order, "o-1")
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if snap, err := q.Execution(context.Background(), h.RunID()); err == nil {
		fmt.Printf("while sleeping: %s\n", snap.Steps[0].Status)
	}

	// Wait until the task has moved on to waiting for the event, then deliver
	// it (an event emitted before the wait registers does not count).
	for {
		snap, err := q.Execution(context.Background(), h.RunID())
		if err == nil && len(snap.Steps) == 1 && snap.Steps[0].WaitEvent == "payment.received" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := q.Emit(context.Background(), "payment.received", "paid-42"); err != nil {
		log.Fatal(err)
	}

	out, err := h.Result(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(out)
}
