// Durable execution: a task that sleeps without holding a worker slot and
// resumes where it left off, with side effects that run exactly once.
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
		// task replays from the top after the durable sleep.
		receipt, err := quacker.RunOnce(ctx, "reserve", func() (string, error) {
			return "reserved " + id, nil
		})
		if err != nil {
			return "", err
		}

		// Suspend for two seconds: no worker, queue, or key slot is held, and
		// with File storage this survives a process restart.
		if err := quacker.SleepDurable(ctx, 2*time.Second); err != nil {
			return "", err
		}
		return receipt + " -> shipped", nil
	})

	h, err := quacker.Enqueue(context.Background(), q, order, "o-1")
	if err != nil {
		log.Fatal(err)
	}
	// Observe it suspended before it finishes.
	time.Sleep(300 * time.Millisecond)
	if snap, err := q.Execution(context.Background(), h.RunID()); err == nil {
		fmt.Printf("while sleeping: %s\n", snap.Steps[0].Status)
	}

	out, err := h.Result(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(out)
}
