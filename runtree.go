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
// Groups are first-class: a step that spawns child runs is contracted into the
// group those runs form (the step node disappears; the group box stands in for
// it), and the group inherits that step's dependencies as group-level edges.
// A group therefore runs after the groups/nodes it depends on without every
// member step repeating the dependency. Intra-group task edges are kept.
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

	var addRun func(id, prefix, parentGroup string, depth int) error
	addRun = func(id, prefix, parentGroup string, depth int) error {
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

		// Group a step's child runs together: one group per spawning step, so
		// a fan-out of N children is one cluster, not N clusters.
		kids, err := q.Runs(ctx, RunFilter{ParentID: id, Limit: maxChildren})
		if err != nil {
			return err
		}
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
		// A step that spawned children is contracted into its group; edges to
		// it are redirected to the group.
		spawnerGroup := map[string]string{}
		for _, g := range order {
			if g.step == "" {
				continue
			}
			spawnerGroup[prefix+g.step] = id + "/" + g.step
		}
		redirect := func(dep string) string {
			if gn, ok := spawnerGroup[dep]; ok {
				return gn
			}
			return dep
		}
		stepDeps := map[string][]string{}
		for _, s := range ex.Steps {
			for _, d := range s.Deps {
				stepDeps[prefix+s.Name] = append(stepDeps[prefix+s.Name], prefix+d)
			}
		}

		// Nodes: every step except contracted spawners, with deps redirected
		// to the group when they point at a spawner.
		for _, s := range ex.Steps {
			name := prefix + s.Name
			if _, isSpawner := spawnerGroup[name]; isSpawner {
				continue
			}
			node := DAGNode{
				Name: name, Task: s.Task, Status: s.Status, Group: parentGroup,
				Attempts: s.Attempts, MaxAttempts: s.MaxAttempts, Error: s.Error,
			}
			for _, d := range s.Deps {
				dd := redirect(prefix + d)
				node.Deps = append(node.Deps, dd)
				out.Edges = append(out.Edges, DAGEdge{From: dd, To: name})
			}
			out.Nodes = append(out.Nodes, node)
		}

		// Groups: one per spawning step. Its dependencies are the spawner's
		// own deps (mapped to groups where the dep is another spawner), drawn
		// as group-to-group edges.
		for _, g := range order {
			groupName := id + "/" + g.step
			label := fmt.Sprintf("%d child runs", len(g.kids))
			if g.step != "" {
				label = fmt.Sprintf("%s → %d child runs", g.step, len(g.kids))
			}
			var deps []string
			for _, dep := range stepDeps[prefix+g.step] {
				deps = append(deps, redirect(dep))
			}
			deps = dedupStrings(deps)
			out.Groups = append(out.Groups, DAGGroup{
				Name: groupName, Label: label, Status: aggregateRunStatus(g.kids),
				Parent: parentGroup, Deps: deps,
			})
			for _, dep := range deps {
				out.Edges = append(out.Edges, DAGEdge{From: dep, To: groupName})
			}
			for _, k := range g.kids {
				kPrefix := prefix + shortRunID(k.RunID) + "/"
				if err := addRun(k.RunID, kPrefix, groupName, depth+1); err != nil {
					return err
				}
			}
		}
		return nil
	}
	// The root run's own steps are left ungrouped (no box); only a spawner's
	// child runs form a group. An empty parentGroup keeps the root nodes bare.
	if err := addRun(runID, "", "", 0); err != nil {
		return nil, err
	}
	return out, nil
}

func dedupStrings(xs []string) []string {
	if len(xs) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(xs))
	out := xs[:0:0]
	for _, x := range xs {
		if seen[x] {
			continue
		}
		seen[x] = true
		out = append(out, x)
	}
	return out
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
