package quacker

import (
	"context"
	"strings"
)

// DAGTree expands a run's DAG to include its child runs: each child run's steps
// are added, namespaced by a short child-run id to keep names unique, and
// attached to the step that spawned them (Recorded as runs.parent_step). It
// recurses into grandchildren, bounded by depth and node count.
//
// This is the run-level view: DAG/DAGSVG show only one run's own steps, because
// a child run is an independent run (EnqueueChild does not join it to the
// parent). DAGTree is what draws the whole family.
func (q *Quacker) DAGTree(ctx context.Context, runID string) (*DAG, error) {
	const (
		maxDepth    = 8
		maxNodes    = 2000
		maxChildren = 200
	)
	out := &DAG{}
	seen := map[string]bool{}

	var add func(id, prefix, from string, depth int) error
	add = func(id, prefix, from string, depth int) error {
		if depth > maxDepth || seen[id] || len(out.Nodes) >= maxNodes {
			return nil
		}
		seen[id] = true
		ex, err := q.Execution(ctx, id)
		if err != nil {
			return err
		}
		if out.RunID == "" {
			out.RunID, out.Workflow, out.Kind, out.Status, out.Queue =
				ex.RunID, ex.Workflow, ex.Kind, ex.Status, ex.Queue
		}
		for _, s := range ex.Steps {
			name := prefix + s.Name
			node := DAGNode{
				Name: name, Task: s.Task, Status: s.Status,
				Attempts: s.Attempts, MaxAttempts: s.MaxAttempts, Error: s.Error,
			}
			for _, d := range s.Deps {
				dd := prefix + d
				node.Deps = append(node.Deps, dd)
				out.Edges = append(out.Edges, DAGEdge{From: dd, To: name})
			}
			// A child run's root steps attach to the step that spawned it. The
			// spawner goes into Deps too, so the level layout places the child
			// after its spawner rather than in the root column.
			if len(s.Deps) == 0 && from != "" {
				node.Deps = append(node.Deps, from)
				out.Edges = append(out.Edges, DAGEdge{From: from, To: name})
			}
			out.Nodes = append(out.Nodes, node)
		}
		kids, err := q.Runs(ctx, RunFilter{ParentID: id, Limit: maxChildren})
		if err != nil {
			return err
		}
		for _, k := range kids {
			childPrefix := prefix + shortRunID(k.RunID) + "/"
			childFrom := ""
			if k.ParentStep != "" {
				childFrom = prefix + k.ParentStep
			}
			if err := add(k.RunID, childPrefix, childFrom, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	if err := add(runID, "", "", 0); err != nil {
		return nil, err
	}
	return out, nil
}

// DAGTreeJSON renders the run tree (parent + child runs, with current state) as
// indented JSON.
func (q *Quacker) DAGTreeJSON(ctx context.Context, runID string) ([]byte, error) {
	d, err := q.DAGTree(ctx, runID)
	if err != nil {
		return nil, err
	}
	return d.JSON()
}

// DAGTreeSVG renders the run tree as a standalone SVG document — the parent
// DAG with each child run's steps attached to the step that spawned them.
func (q *Quacker) DAGTreeSVG(ctx context.Context, runID string) ([]byte, error) {
	d, err := q.DAGTree(ctx, runID)
	if err != nil {
		return nil, err
	}
	return d.SVG(), nil
}

// shortRunID is the disambiguating suffix of a run id (e.g. "10f2ba08"), used
// to namespace a child run's step names in the tree.
func shortRunID(id string) string {
	if i := strings.LastIndexByte(id, '-'); i >= 0 && i+1 < len(id) {
		return id[i+1:]
	}
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
