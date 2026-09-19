package quacker

import (
	"context"
	"fmt"
	"strings"
)

// DAGTree expands a run's DAG to include its child runs: each child run's steps
// are added, namespaced by a short child-run id to keep names unique, and
// attached to the step that spawned them (Recorded as runs.parent_step). It
// recurses into grandchildren, bounded by depth and node count.
//
// Nodes are grouped: the root run's steps, then one group per spawning step
// whose child runs are clustered together (a fan-out of N children is one
// group, not N).
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
	// The root run's own steps form the first group.
	out.Groups = append(out.Groups, DAGGroup{Name: runID})

	var addRun func(id, prefix, from, groupKey string, depth int) error
	addRun = func(id, prefix, from, groupKey string, depth int) error {
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
				Name: name, Task: s.Task, Status: s.Status, Group: groupKey,
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
		// Group a step's child runs together: one group per spawning step, so
		// a fan-out of N children is one cluster, not N clusters.
		type stepGroup struct {
			step string
			kids []RunSummary
		}
		var order []*stepGroup
		byStep := map[string]*stepGroup{}
		for _, k := range kids {
			g, ok := byStep[k.ParentStep]
			if !ok {
				g = &stepGroup{step: k.ParentStep}
				byStep[k.ParentStep] = g
				order = append(order, g)
			}
			g.kids = append(g.kids, k)
		}
		for _, g := range order {
			label := fmt.Sprintf("%d child runs", len(g.kids))
			if g.step != "" {
				label = fmt.Sprintf("%s → %d child runs", g.step, len(g.kids))
			}
			groupKey := id + "/" + g.step
			out.Groups = append(out.Groups, DAGGroup{Name: groupKey, Label: label, Status: aggregateRunStatus(g.kids)})
			for _, k := range g.kids {
				kPrefix := prefix + shortRunID(k.RunID) + "/"
				kFrom := ""
				if g.step != "" {
					kFrom = prefix + g.step
				}
				if err := addRun(k.RunID, kPrefix, kFrom, groupKey, depth+1); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := addRun(runID, "", "", runID, 0); err != nil {
		return nil, err
	}
	out.Groups[0].Label = out.Workflow
	out.Groups[0].Status = out.Status
	return out, nil
}

// aggregateRunStatus summarizes a group of runs: a failure or cancellation in
// the group wins, otherwise any in-flight run makes it RUNNING, else the shared
// status.
func aggregateRunStatus(rs []RunSummary) Status {
	status := StatusSucceeded
	for _, r := range rs {
		switch r.Status {
		case StatusFailed:
			return StatusFailed
		case StatusCancelled:
			status = StatusCancelled
		case StatusInterrupted:
			if status != StatusCancelled {
				status = StatusInterrupted
			}
		case StatusRunning, StatusQueued, StatusBlocked, StatusSuspended, StatusPaused:
			if status == StatusSucceeded {
				status = StatusRunning
			}
		}
	}
	return status
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
