// Prototype: ELT groups as nested (child) workflows.
//
// Each stage is its own workflow run, spawned from a parent step that awaits it.
// The parent therefore depends on a whole child run — not on individual steps —
// and DAGTree shows each child run as a labelled group.
//
//	go run ./examples/eltnested -out /tmp/eltnested
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

type batch struct {
	ID       int `json:"id"`
	Upstream int `json:"upstream"`
}

type rows struct {
	Table string `json:"table"`
	Count int    `json:"count"`
}

type groupResult struct {
	Group  string         `json:"group"`
	Tables map[string]int `json:"tables"`
	Total  int            `json:"total"`
}

var tables = []string{"customers", "orders", "products"}

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

func suffix(ctx context.Context, prefix string) (string, error) {
	sc, ok := quacker.StepFromContext(ctx)
	if !ok {
		return "", fmt.Errorf("no step context")
	}
	return strings.TrimPrefix(sc.Step, prefix), nil
}

func main() {
	work := flag.Duration("work", 60*time.Millisecond, "simulated per-step work")
	outDir := flag.String("out", "", "directory to write the DAG tree")
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

	// --- Leaf tasks, one per stage; the table comes from the step name. ---

	extract := quacker.NewTask("nested.extract", func(ctx context.Context, in batch) (rows, error) {
		table, err := suffix(ctx, "extract_")
		if err != nil {
			return rows{}, err
		}
		enter()
		defer inFlight.Add(-1)
		time.Sleep(*work)
		return rows{Table: table, Count: (len(table)+in.ID)*10 + 1}, nil
	})

	transform := quacker.NewTask("nested.transform", func(ctx context.Context, in batch) (rows, error) {
		table, err := suffix(ctx, "transform_")
		if err != nil {
			return rows{}, err
		}
		out := rows{Table: table, Count: in.Upstream + 100}
		if table == "orders" {
			// Same-group dependency, exactly as in the flat example.
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

	load := quacker.NewTask("nested.load", func(ctx context.Context, in batch) (rows, error) {
		table, err := suffix(ctx, "load_")
		if err != nil {
			return rows{}, err
		}
		enter()
		defer inFlight.Add(-1)
		time.Sleep(*work / 2)
		return rows{Table: table, Count: in.Upstream + 5}, nil
	})

	// join is the group's INTERNAL fan-in. It lives inside the child run, so it
	// is part of the group rather than a top-level gate node.
	join := quacker.NewTask("nested.join", func(ctx context.Context, in batch) (groupResult, error) {
		group, err := suffix(ctx, "")
		if err != nil {
			return groupResult{}, err
		}
		group = strings.TrimSuffix(group, "_join")
		res := groupResult{Group: group, Tables: map[string]int{}}
		for _, t := range tables {
			r, err := quacker.DepOutput[rows](ctx, group+"_"+t)
			if err != nil {
				return groupResult{}, err
			}
			res.Tables[t] = r.Count
			res.Total += r.Count
		}
		return res, nil
	})

	// --- Each stage is a workflow. ---

	extractWF := quacker.NewWorkflow[batch]("elt.extract_group",
		quacker.Step("extract_customers", extract),
		quacker.Step("extract_orders", extract),
		quacker.Step("extract_products", extract),
		quacker.Step("extract_join", join, "extract_customers", "extract_orders", "extract_products"),
	)

	transformWF := quacker.NewWorkflow[batch]("elt.transform_group",
		quacker.Step("transform_customers", transform),
		quacker.Step("transform_orders", transform, "transform_customers"),
		quacker.Step("transform_products", transform),
		quacker.Step("transform_join", join, "transform_customers", "transform_orders", "transform_products"),
	)

	loadWF := quacker.NewWorkflow[batch]("elt.load_group",
		quacker.Step("load_customers", load),
		quacker.Step("load_orders", load),
		quacker.Step("load_products", load),
		quacker.Step("load_join", join, "load_customers", "load_orders", "load_products"),
	)

	// --- A spawner task per stage: enqueue the child run, await it, return its
	// result. This is the whole "depend on the group" mechanism. ---

	spawnExtract := quacker.NewTask("nested.spawn_extract", func(ctx context.Context, in batch) (groupResult, error) {
		h, err := quacker.EnqueueWorkflowChild[groupResult](ctx, q, extractWF, in)
		if err != nil {
			return groupResult{}, err
		}
		return h.Result(ctx)
	})
	// The next group reads the previous group's result and passes it down.
	spawnTransform := quacker.NewTask("nested.spawn_transform", func(ctx context.Context, in batch) (groupResult, error) {
		up, err := quacker.DepOutput[groupResult](ctx, "extract_group")
		if err != nil {
			return groupResult{}, err
		}
		h, err := quacker.EnqueueWorkflowChild[groupResult](ctx, q, transformWF, batch{ID: in.ID, Upstream: up.Total})
		if err != nil {
			return groupResult{}, err
		}
		return h.Result(ctx)
	})
	spawnLoad := quacker.NewTask("nested.spawn_load", func(ctx context.Context, in batch) (groupResult, error) {
		up, err := quacker.DepOutput[groupResult](ctx, "transform_group")
		if err != nil {
			return groupResult{}, err
		}
		h, err := quacker.EnqueueWorkflowChild[groupResult](ctx, q, loadWF, batch{ID: in.ID, Upstream: up.Total})
		if err != nil {
			return groupResult{}, err
		}
		return h.Result(ctx)
	})

	// Parent: one step per group; the next group's step depends on the previous
	// group's step, so a group starts only after the whole previous run finishes.
	parent := quacker.NewWorkflow[batch]("elt.nested_pipeline",
		quacker.Step("extract_group", spawnExtract),
		quacker.Step("transform_group", spawnTransform, "extract_group"),
		quacker.Step("load_group", spawnLoad, "transform_group"),
	)

	ctx := context.Background()
	h, err := quacker.EnqueueWorkflow[groupResult](ctx, q, parent, batch{ID: 7})
	if err != nil {
		log.Fatal(err)
	}
	out, err := h.Result(ctx)
	if err != nil {
		fmt.Printf("pipeline failed: %v\n", err)
	} else {
		fmt.Printf("pipeline succeeded: total=%d\n", out.Total)
	}

	// The parent DAG is tiny: one node per group.
	dag, _ := q.DAG(ctx, h.RunID())
	fmt.Printf("\nparent DAG: %d nodes, %d edges\n", len(dag.Nodes), len(dag.Edges))
	for _, n := range dag.Nodes {
		fmt.Printf("  %-16s %s\n", n.Name, n.Status)
	}

	// DAGTree walks into the child runs and shows each as a group.
	tree, _ := q.DAGTree(ctx, h.RunID())
	fmt.Printf("\nrun tree: %d groups, %d nodes\n", len(tree.Groups), len(tree.Nodes))
	for _, g := range tree.Groups {
		fmt.Printf("  group %-24s %s\n", g.Label, g.Status)
	}
	fmt.Printf("peak parallel steps (across all runs): %d\n", maxInFlight.Load())

	if *outDir != "" {
		if err := os.MkdirAll(*outDir, 0o755); err != nil {
			log.Fatal(err)
		}
		tsvg, err := q.DAGTreeSVG(ctx, h.RunID())
		if err != nil {
			log.Fatal(err)
		}
		tjs, _ := q.DAGTreeJSON(ctx, h.RunID())
		if err := os.WriteFile(*outDir+"/tree.svg", tsvg, 0o644); err != nil {
			log.Fatal(err)
		}
		if err := os.WriteFile(*outDir+"/tree.json", tjs, 0o644); err != nil {
			log.Fatal(err)
		}
		var meta struct {
			Groups []struct {
				Label string `json:"label"`
			} `json:"groups"`
		}
		_ = json.Unmarshal(tjs, &meta)
		fmt.Printf("wrote %s/tree.svg and %s/tree.json (%d groups)\n", *outDir, *outDir, len(meta.Groups))
	}
}
