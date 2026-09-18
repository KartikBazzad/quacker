// Simplest possible quacker app: define a task, enqueue it, await the result.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/kartikbazzad/quacker"
)

type Order struct {
	ID    string
	Total int
}

type Confirmation struct {
	OrderID   string
	Amount    int
	WaitedFor time.Duration
}

func main() {
	q, err := quacker.Open() // ephemeral in-memory state
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close(context.Background())

	charge := quacker.NewTask("orders.charge", func(ctx context.Context, o Order) (Confirmation, error) {
		return Confirmation{OrderID: o.ID, Amount: o.Total}, nil
	})

	h, err := quacker.Enqueue(context.Background(), q, charge, Order{ID: "o-1", Total: 4200})
	if err != nil {
		log.Fatal(err)
	}

	conf, err := h.Result(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("charged %+v\n", conf)
}
