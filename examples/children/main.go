// Child runs: a parent task fans out to per-item child runs, and the lineage
// is visible through Execution/Runs.
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

	process := quacker.NewTask("rows.process", func(ctx context.Context, row string) (string, error) {
		return "processed " + row, nil
	})

	importTask := quacker.NewTask("rows.import", func(ctx context.Context, rows []string) (int, error) {
		for _, row := range rows {
			// Runs independently, recorded as a child of this run.
			if _, err := quacker.EnqueueChild(ctx, q, process, row); err != nil {
				return 0, err
			}
		}
		return len(rows), nil
	})

	h, err := quacker.Enqueue(context.Background(), q, importTask, []string{"a", "b", "c"})
	if err != nil {
		log.Fatal(err)
	}
	if _, err := h.Result(context.Background()); err != nil {
		log.Fatal(err)
	}

	time.Sleep(300 * time.Millisecond) // let the children finish
	kids, err := q.Runs(context.Background(), quacker.RunFilter{ParentID: h.RunID()})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("parent queued %d children:\n", len(kids))
	for _, k := range kids {
		fmt.Printf("  %s %s\n", k.RunID, k.Status)
	}
}
