// DAG visualization: run a workflow, then emit its current step graph as
// JSON and as a standalone SVG file.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/kartikbazzad/quacker"
)

func main() {
	q, err := quacker.Open()
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close(context.Background())

	charge := quacker.NewTask("charge", func(ctx context.Context, in string) (string, error) {
		return "charged " + in, nil
	})
	ship := quacker.NewTask("ship", func(ctx context.Context, in string) (string, error) {
		return "", errors.New("carrier unavailable") // makes the DAG colourful
	})
	notify := quacker.NewTask("notify", func(ctx context.Context, in string) (string, error) {
		return "notified", nil
	})

	wf := quacker.NewWorkflow[string]("fulfill",
		quacker.Step("charge", charge),
		quacker.Step("ship", ship, "charge"),
		quacker.Step("notify", notify, "ship"),
	)
	h, err := quacker.EnqueueWorkflow[string](context.Background(), q, wf, "order-42")
	if err != nil {
		log.Fatal(err)
	}
	_, _ = h.Result(context.Background()) // expected to fail at "ship"

	// The current state of the whole DAG, as JSON...
	js, err := q.DAGJSON(context.Background(), h.RunID())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(js))

	// ...and as an SVG image.
	svg, err := q.DAGSVG(context.Background(), h.RunID())
	if err != nil {
		log.Fatal(err)
	}
	const path = "dag.svg"
	if err := os.WriteFile(path, svg, 0o644); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("wrote %s (%d bytes)\n", path, len(svg))
}
