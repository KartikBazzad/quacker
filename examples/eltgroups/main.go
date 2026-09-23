// Grouped ELT DAG: four groups — extract, transform, load, report — where each
// group fans out into parallel steps, the next group runs only after the
// previous group (declared once with After), and a step may depend on a
// sibling in its own group.
//
//	go run ./examples/eltgroups        # run the pipeline, print the groups
//	go run ./examples/eltgroups -out . # also write dag.json and dag.svg
//
// The DAG view draws group boxes and group-to-group edges; the cross-group
// dependencies are not repeated on every member step:
//
//	EXTRACT ──► TRANSFORM ──► LOAD ──► REPORT
//	customers   customers ─┐  customers  dashboard ─┐
//	orders      orders ────┤  orders     metrics   ─┼─► send_alert
//	products    products   │  products   └──────────┘
//	                       └─ transform_customers → transform_orders
//
// The ELT idiom: one task per stage, each deriving its table from
// StepFromContext(ctx).Step. Task names are process-unique, so registering a
// different closure per table under one name would silently keep the last one.
package main

import (
	"context"
	"flag"
	"fmt"
	json "github.com/goccy/go-json"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/kartikbazzad/quacker"
)

// batch is the workflow input; every step receives it.
type batch struct {
	ID int `json:"id"`
}

// rows is the payload that flows along the DAG edges.
type rows struct {
	Table string `json:"table"`
	Count int    `json:"count"`
}

// tables is the fan-out width of every stage: one parallel step per table.
var tables = []string{"customers", "orders", "products"}

// groupOrder is the display order of the groups.
var groupOrder = []string{"extract", "transform", "load", "report"}

// stepSuffix returns the part of the step name after prefix, so a shared task
// can tell which table it is running.
func stepSuffix(ctx context.Context, prefix string) (string, error) {
	sc, ok := quacker.StepFromContext(ctx)
	if !ok {
		return "", fmt.Errorf("no step context")
	}
	return strings.TrimPrefix(sc.Step, prefix), nil
}

// concurrency tracks how many leaf steps run at once, to show the fan-out is
// actually parallel.
var (
	inFlight    atomic.Int64
	maxInFlight atomic.Int64
)

func enter() {
	n := inFlight.Add(1)
	for {
		m := maxInFlight.Load()
		if n <= m || maxInFlight.CompareAndSwap(m, n) {
			return
		}
	}
}

func main() {
	failTable := flag.String("fail", "", "table whose extract fails (customers|orders|products)")
	work := flag.Duration("work", 60*time.Millisecond, "simulated per-step work")
	outDir := flag.String("out", "", "directory to also write dag.json and dag.svg")
	flag.Parse()

	q, err := quacker.Open(
		quacker.WithStorage(quacker.Ephemeral()),
		quacker.WithQueue("default", 32),
		quacker.WithPollInterval(2*time.Millisecond),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close(context.Background())

	// --- One task per stage. Each derives its table from the step name. ---

	extract := quacker.NewTask("elt.extract", func(ctx context.Context, in batch) (rows, error) {
		table, err := stepSuffix(ctx, "extract_")
		if err != nil {
			return rows{}, err
		}
		if table == *failTable {
			return rows{}, fmt.Errorf("source %q unavailable", table)
		}
		enter()
		defer inFlight.Add(-1)
		time.Sleep(*work) // simulate reading the source
		return rows{Table: table, Count: (len(table)+in.ID)*10 + 1}, nil
	}, quacker.Retries(2))

	transform := quacker.NewTask("elt.transform", func(ctx context.Context, in batch) (rows, error) {
		table, err := stepSuffix(ctx, "transform_")
		if err != nil {
			return rows{}, err
		}
		// The group dep expanded onto this step, so the matching extract step
		// is a recorded dependency.
		up, err := quacker.DepOutput[rows](ctx, "extract_"+table)
		if err != nil {
			return rows{}, err
		}
		out := rows{Table: table, Count: up.Count * 2}
		if table == "orders" {
			// Same-group dependency: orders are enriched with the customers
			// dimension built by transform_customers in this same group.
			cust, err := quacker.DepOutput[rows](ctx, "transform_customers")
			if err != nil {
				return rows{}, err
			}
			out.Count += cust.Count / 10
		}
		enter()
		defer inFlight.Add(-1)
		time.Sleep(*work)
		return out, nil
	})

	load := quacker.NewTask("elt.load", func(ctx context.Context, in batch) (rows, error) {
		table, err := stepSuffix(ctx, "load_")
		if err != nil {
			return rows{}, err
		}
		up, err := quacker.DepOutput[rows](ctx, "transform_"+table)
		if err != nil {
			return rows{}, err
		}
		enter()
		defer inFlight.Add(-1)
		time.Sleep(*work / 2)
		return rows{Table: table, Count: up.Count + 5}, nil
	})

	// loaded sums every load member; the report group depends on the whole
	// load group, so all of them are recorded dependencies.
	loaded := func(ctx context.Context) (int, error) {
		total := 0
		for _, t := range tables {
			r, err := quacker.DepOutput[rows](ctx, "load_"+t)
			if err != nil {
				return 0, err
			}
			total += r.Count
		}
		return total, nil
	}
	dashboard := quacker.NewTask("elt.refresh_dashboard", func(ctx context.Context, in batch) (rows, error) {
		n, err := loaded(ctx)
		return rows{Table: "dashboard", Count: n}, err
	})
	metrics := quacker.NewTask("elt.publish_metrics", func(ctx context.Context, in batch) (rows, error) {
		n, err := loaded(ctx)
		return rows{Table: "metrics", Count: n}, err
	})
	alert := quacker.NewTask("elt.send_alert", func(ctx context.Context, in batch) (rows, error) {
		// Same-group dependency: the alert needs both report siblings.
		d, err := quacker.DepOutput[rows](ctx, "refresh_dashboard")
		if err != nil {
			return rows{}, err
		}
		m, err := quacker.DepOutput[rows](ctx, "publish_metrics")
		if err != nil {
			return rows{}, err
		}
		return rows{Table: "alert", Count: d.Count + m.Count}, nil
	})

	// --- Compose the groups: parallel steps inside, one After edge between. ---

	extractG := quacker.NewGroup[batch]("extract",
		quacker.Step("extract_customers", extract),
		quacker.Step("extract_orders", extract),
		quacker.Step("extract_products", extract),
	)
	transformG := quacker.NewGroup[batch]("transform",
		quacker.Step("transform_customers", transform),
		quacker.Step("transform_orders", transform, "transform_customers"), // same group
		quacker.Step("transform_products", transform),
	).After("extract")
	loadG := quacker.NewGroup[batch]("load",
		quacker.Step("load_customers", load),
		quacker.Step("load_orders", load),
		quacker.Step("load_products", load),
	).After("transform")
	reportG := quacker.NewGroup[batch]("report",
		quacker.Step("refresh_dashboard", dashboard),
		quacker.Step("publish_metrics", metrics),
		quacker.Step("send_alert", alert, "refresh_dashboard", "publish_metrics"), // same group
	).After("load")

	wf := quacker.NewGroupWorkflow[batch]("elt.grouped_pipeline", extractG, transformG, loadG, reportG)
	ctx := context.Background()
	h, err := quacker.EnqueueWorkflow[rows](ctx, q, wf, batch{ID: 7})
	if err != nil {
		log.Fatal(err)
	}
	out, err := h.Result(ctx)
	if err != nil {
		// A failed step cancels the rest of the run; the report below still
		// shows how far each group got.
		fmt.Printf("pipeline failed: %v\n", err)
	} else {
		fmt.Printf("pipeline succeeded: %+v\n", out)
	}

	report(ctx, q, h.RunID())

	if *outDir != "" {
		writeDAG(ctx, q, h.RunID(), *outDir)
	}
}

// report prints the run's groups with their members, the wall-clock span of
// each group, and the peak parallelism observed.
func report(ctx context.Context, q *quacker.Quacker, runID string) {
	ex, err := q.Execution(ctx, runID)
	if err != nil {
		log.Fatal(err)
	}
	member := map[string]quacker.StepState{}
	for _, s := range ex.Steps {
		member[s.Name] = s
	}
	fmt.Printf("\nrun %s  [%s]\n", ex.RunID, ex.Status)
	for _, g := range ex.Groups {
		var start, end time.Time
		for _, name := range g.Steps {
			s := member[name]
			if s.StartedAt.IsZero() {
				continue
			}
			if start.IsZero() || s.StartedAt.Before(start) {
				start = s.StartedAt
			}
			if s.CompletedAt.After(end) {
				end = s.CompletedAt
			}
		}
		deps := "-"
		if len(g.Deps) > 0 {
			deps = strings.Join(g.Deps, ",")
		}
		fmt.Printf("\n%-9s after[%s]  wall %s\n", g.Name, deps, fmtDur(end.Sub(start)))
		for _, name := range g.Steps {
			s := member[name]
			ran := ""
			if !s.StartedAt.IsZero() {
				ran = fmtDur(s.CompletedAt.Sub(s.StartedAt))
			}
			fmt.Printf("  %-20s %-10s %s\n", s.Name, s.Status, ran)
		}
	}
	fmt.Printf("\npeak parallel steps: %d\n", maxInFlight.Load())
}

func fmtDur(d time.Duration) string {
	if d <= 0 {
		return "-"
	}
	return d.Round(time.Millisecond).String()
}

// writeDAG persists the step graph as JSON and SVG (groups appear as boxes
// connected by group-to-group edges).
func writeDAG(ctx context.Context, q *quacker.Quacker, runID, dir string) {
	js, err := q.DAGJSON(ctx, runID)
	if err != nil {
		log.Fatal(err)
	}
	svg, err := q.DAGSVG(ctx, runID)
	if err != nil {
		log.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Fatal(err)
	}
	jsonPath, svgPath := dir+"/dag.json", dir+"/dag.svg"
	if err := os.WriteFile(jsonPath, js, 0o644); err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile(svgPath, svg, 0o644); err != nil {
		log.Fatal(err)
	}
	var d quacker.DAG
	_ = json.Unmarshal(js, &d)
	groupEdges, stepEdges := 0, 0
	inGroup := map[string]bool{}
	for _, g := range d.Groups {
		inGroup[g.Name] = true
	}
	for _, e := range d.Edges {
		if inGroup[e.From] && inGroup[e.To] {
			groupEdges++
		} else {
			stepEdges++
		}
	}
	fmt.Printf("wrote %s (%d groups, %d group edges, %d step edges) and %s\n",
		jsonPath, len(d.Groups), groupEdges, stepEdges, svgPath)
}
