// A workflow DAG: charge, then ship, then notify — with data flowing from
// step to step via dependency outputs.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/kartikbazzad/quacker"
)

type Order struct{ ID string }
type Receipt struct{ ChargeID string }
type Shipment struct{ Tracking string }

func main() {
	q, err := quacker.Open()
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close(context.Background())

	charge := quacker.NewTask("charge", func(ctx context.Context, o Order) (Receipt, error) {
		return Receipt{ChargeID: "ch_" + o.ID}, nil
	})
	ship := quacker.NewTask("ship", func(ctx context.Context, o Order) (Shipment, error) {
		rec, err := quacker.DepOutput[Receipt](ctx, "charge") // upstream output
		if err != nil {
			return Shipment{}, err
		}
		return Shipment{Tracking: rec.ChargeID + "_track"}, nil
	}, quacker.Retries(2))
	notify := quacker.NewTask("notify", func(ctx context.Context, o Order) (string, error) {
		ship, _ := quacker.DepOutput[Shipment](ctx, "ship")
		return "email for " + o.ID + ": " + ship.Tracking, nil
	})

	wf := quacker.NewWorkflow("fulfill",
		quacker.Step("charge", charge),
		quacker.Step("ship", ship, "charge"),             // after charge
		quacker.Step("notify", notify, "charge", "ship"), // after both
	)

	h, err := quacker.EnqueueWorkflow[string](context.Background(), q, wf, Order{ID: "o-9"})
	if err != nil {
		log.Fatal(err)
	}
	out, err := h.Result(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(out)
}
