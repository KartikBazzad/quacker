// ELT DAG visualization: build an extract -> transform -> stage -> load
// pipeline (parallel per-shard fan-out, a per-shard series chain, a fan-in, and
// per-partition child workflows), run it, then save the run's step graph as
// dag.json and dag.svg.
//
//	go run ./examples/eltdag          # all shards succeed (green)
//	go run ./examples/eltdag -fail 3  # shard 3 fails (failure colours)
//
// Note the ELT pattern: there is ONE "extract" task and ONE "transform" task,
// and each step derives its shard from StepFromContext(ctx).Step (its step
// name). Task names are process-unique, so registering a different closure per
// shard under the same name would silently make the last one win.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kartikbazzad/quacker"
)

type extractOut struct {
	Shard int `json:"shard"`
	Rows  int `json:"rows"`
}

func shardOf(ctx context.Context, prefix string) (int, error) {
	sc, ok := quacker.StepFromContext(ctx)
	if !ok {
		return 0, errors.New("no step context")
	}
	return strconv.Atoi(strings.TrimPrefix(sc.Step, prefix))
}

func main() {
	failShard := flag.Int("fail", -1, "shard index whose extract fails (-1 = none)")
	shards := flag.Int("shards", 8, "number of shards")
	dir := flag.String("out", ".", "directory to write dag.json and dag.svg")
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

	plan := quacker.NewTask("elt.plan", func(ctx context.Context, in int) (int, error) {
		return in, nil
	})
	extract := quacker.NewTask("elt.extract", func(ctx context.Context, in int) (extractOut, error) {
		shard, err := shardOf(ctx, "extract_")
		if err != nil {
			return extractOut{}, err
		}
		if shard == *failShard {
			return extractOut{}, errors.New("source unavailable")
		}
		return extractOut{Shard: shard, Rows: (shard + 1) * 100}, nil
	}, quacker.Retries(2))
	transform := quacker.NewTask("elt.transform", func(ctx context.Context, in int) (extractOut, error) {
		shard, err := shardOf(ctx, "transform_")
		if err != nil {
			return extractOut{}, err
		}
		up, err := quacker.DepOutput[extractOut](ctx, fmt.Sprintf("extract_%d", shard))
		if err != nil {
			return extractOut{}, err
		}
		up.Rows++ // clean / normalize
		return up, nil
	})

	steps := []quacker.StepDef[int]{quacker.Step("plan", plan)}
	var transforms []string
	for i := 0; i < *shards; i++ {
		ename := fmt.Sprintf("extract_%d", i)
		tname := fmt.Sprintf("transform_%d", i)
		transforms = append(transforms, tname)
		steps = append(steps,
			quacker.Step(ename, extract, "plan"),
			quacker.Step(tname, transform, ename),
		)
	}

	stage := quacker.NewTask("elt.stage", func(ctx context.Context, in int) (int, error) {
		total := 0
		for i := 0; i < *shards; i++ {
			up, err := quacker.DepOutput[extractOut](ctx, fmt.Sprintf("transform_%d", i))
			if err != nil {
				return 0, err
			}
			total += up.Rows
		}
		return total, nil
	})
	steps = append(steps, quacker.Step("stage", stage, transforms...))

	load := quacker.NewTask("elt.load", func(ctx context.Context, in int) (int, error) {
		return quacker.DepOutput[int](ctx, "stage")
	})
	steps = append(steps, quacker.Step("load", load, "stage"))

	// Per-partition child workflows, spawned from inside a step and awaited.
	childJob := quacker.NewTask("elt.partition_job", func(ctx context.Context, in int) (int, error) {
		return in + 1, nil
	})
	childWF := quacker.NewWorkflow[int]("partition_jobs", quacker.Step("job", childJob))
	partition := quacker.NewTask("elt.partition", func(ctx context.Context, in int) (int, error) {
		hs := make([]*quacker.RunHandle[int], *shards)
		for i := range hs {
			h, err := quacker.EnqueueWorkflowChild[int](ctx, q, childWF, i)
			if err != nil {
				return 0, err
			}
			hs[i] = h
		}
		total := 0
		for _, h := range hs {
			v, err := h.Result(ctx)
			if err != nil {
				return 0, err
			}
			total += v
		}
		return total, nil
	})
	steps = append(steps, quacker.Step("partition", partition, "stage"))

	report := quacker.NewTask("elt.report", func(ctx context.Context, in int) (int, error) {
		rows, err := quacker.DepOutput[int](ctx, "load")
		if err != nil {
			return 0, err
		}
		parts, err := quacker.DepOutput[int](ctx, "partition")
		if err != nil {
			return 0, err
		}
		return rows + parts, nil
	})
	steps = append(steps, quacker.Step("report", report, "load", "partition"))

	wf := quacker.NewWorkflow[int]("elt.pipeline", steps...)
	h, err := quacker.EnqueueWorkflow[int](context.Background(), q, wf, *shards)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	if _, err := h.Result(ctx); err != nil {
		fmt.Printf("pipeline finished with: %v\n", err)
	}

	js, err := q.DAGJSON(ctx, h.RunID())
	if err != nil {
		log.Fatal(err)
	}
	svg, err := q.DAGSVG(ctx, h.RunID())
	if err != nil {
		log.Fatal(err)
	}
	jsonPath := *dir + "/dag.json"
	svgPath := *dir + "/dag.svg"
	if err := os.WriteFile(jsonPath, js, 0o644); err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile(svgPath, svg, 0o644); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("wrote %s (%d bytes) and %s (%d bytes)\n", jsonPath, len(js), svgPath, len(svg))
}
