// In-process events: emit an event and every bound task runs with the event
// payload as its input.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/kartikbazzad/quacker"
)

type OrderPlaced struct {
	ID    string
	Total int
}

func main() {
	q, err := quacker.Open()
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close(context.Background())

	receipt := quacker.NewTask("orders.receipt", func(ctx context.Context, o OrderPlaced) (string, error) {
		return fmt.Sprintf("receipt for %s ($%d)", o.ID, o.Total), nil
	})
	notify := quacker.NewTask("orders.notify", func(ctx context.Context, o OrderPlaced) (string, error) {
		return "notified " + o.ID, nil
	})

	if err := quacker.On(q, "order.placed", receipt); err != nil {
		log.Fatal(err)
	}
	if err := quacker.On(q, "order.placed", notify); err != nil {
		log.Fatal(err)
	}

	n, err := q.Emit(context.Background(), "order.placed", OrderPlaced{ID: "o-1", Total: 4200})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("dispatched %d runs\n", n)

	time.Sleep(200 * time.Millisecond) // let the runs finish
	events, _ := q.Events(context.Background(), 10)
	for _, e := range events {
		fmt.Printf("event %s at %s: %s\n", e.Name, e.At.Format(time.Kitchen), e.Payload)
	}
}
