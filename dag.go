package quacker

import (
	"context"
	"encoding/json"
)

// DAGNode is one step of a run's DAG with its current state.
type DAGNode struct {
	Name string `json:"name"`
	// Task is the registered task that executes the step (differs from Name
	// for named workflow steps).
	Task        string   `json:"task,omitempty"`
	Status      Status   `json:"status"`
	Deps        []string `json:"deps,omitempty"`
	Attempts    int      `json:"attempts"`
	MaxAttempts int      `json:"max_attempts"`
	Error       string   `json:"error,omitempty"`
	// Group is the run this node belongs to, when the DAG spans several runs
	// (a DAGTree). Empty for a single-run DAG.
	Group string `json:"group,omitempty"`
}

// DAGGroup is one run's cluster of nodes within a DAGTree — the root run or a
// child run — drawn as a labelled box behind its nodes. A plain single-run DAG
// has no groups.
type DAGGroup struct {
	Name   string `json:"name"`  // run id
	Label  string `json:"label"` // human label, e.g. "partition_jobs #3"
	Status Status `json:"status"`
	// Parent is the enclosing group's Name, or "" when this group hangs off
	// the root run. It lets the renderer nest clusters instead of guessing
	// containment from node coordinates.
	Parent string `json:"parent,omitempty"`
	// Deps are group-level dependencies: names of other groups this whole
	// group waits on. They render as box-to-box edges; individual member
	// steps do not repeat them.
	Deps []string `json:"deps,omitempty"`
	// Steps are the group's member step names (a flat grouped workflow). Empty
	// for a run-tree group, whose members are the child run's own nodes.
	Steps []string `json:"steps,omitempty"`
}

// DAGEdge is a dependency edge: From must succeed before To runs.
type DAGEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// DAG is a run's step graph with the current state of every node. It is a
// point-in-time snapshot: reads never pause execution, so a later call sees
// later states.
type DAG struct {
	RunID    string     `json:"run_id"`
	Workflow string     `json:"workflow"`
	Kind     Kind       `json:"kind"`
	Status   Status     `json:"status"`
	Queue    string     `json:"queue"`
	Nodes    []DAGNode  `json:"nodes"`
	Edges    []DAGEdge  `json:"edges"`
	Groups   []DAGGroup `json:"groups,omitempty"`
}

// DAG returns the step graph of a run with the current state of every node.
// For a single-task run it is one node with no edges. Returns ErrNotFound for
// an unknown run. It covers only this run's own steps; use DAGTree to include
// child runs (spawned with EnqueueChild).
//
// For a grouped workflow (NewGroupWorkflow), member steps carry their group in
// DAGNode.Group, group-level dependencies become group-to-group edges, and the
// expanded cross-group step edges are omitted so the graph reads as groups
// rather than a step mesh.
func (q *Quacker) DAG(ctx context.Context, runID string) (*DAG, error) {
	ex, err := q.Execution(ctx, runID)
	if err != nil {
		return nil, err
	}
	d := &DAG{RunID: ex.RunID, Workflow: ex.Workflow, Kind: ex.Kind, Status: ex.Status, Queue: ex.Queue}

	groupOf := map[string]string{}
	groupStatus := map[string][]Status{}
	for _, g := range ex.Groups {
		for _, s := range g.Steps {
			groupOf[s] = g.Name
		}
	}
	for _, s := range ex.Steps {
		g := groupOf[s.Name]
		if g != "" {
			groupStatus[g] = append(groupStatus[g], s.Status)
		}
		d.Nodes = append(d.Nodes, DAGNode{
			Name: s.Name, Task: s.Task, Status: s.Status, Deps: s.Deps,
			Attempts: s.Attempts, MaxAttempts: s.MaxAttempts, Error: s.Error, Group: g,
		})
		for _, dep := range s.Deps {
			// A dep across two different groups is a group-level edge, drawn
			// once between the boxes; a dep involving an ungrouped node (or
			// within one group) is a real step edge.
			if groupOf[dep] != "" && g != "" && groupOf[dep] != g {
				continue
			}
			d.Edges = append(d.Edges, DAGEdge{From: dep, To: s.Name})
		}
	}
	for _, g := range ex.Groups {
		d.Groups = append(d.Groups, DAGGroup{
			Name: g.Name, Label: g.Name, Status: aggregateStepStatus(groupStatus[g.Name]),
			Deps: g.Deps, Steps: g.Steps,
		})
		for _, dep := range g.Deps {
			d.Edges = append(d.Edges, DAGEdge{From: dep, To: g.Name})
		}
	}
	return d, nil
}

// aggregateStepStatus summarizes a group's member statuses: a failure wins,
// then cancellation/interruption, else in-flight work makes it RUNNING.
func aggregateStepStatus(ss []Status) Status {
	if len(ss) == 0 {
		return ""
	}
	status := StatusSucceeded
	inflight := false
	for _, s := range ss {
		switch s {
		case StatusFailed:
			return StatusFailed
		case StatusCancelled:
			status = StatusCancelled
		case StatusInterrupted:
			if status != StatusCancelled {
				status = StatusInterrupted
			}
		case StatusRunning, StatusQueued, StatusBlocked, StatusSuspended, StatusPaused:
			inflight = true
		}
	}
	if status == StatusSucceeded && inflight {
		return StatusRunning
	}
	return status
}

// JSON renders the DAG (with current state) as indented JSON.
func (d *DAG) JSON() ([]byte, error) { return json.MarshalIndent(d, "", "  ") }

// DAGJSON returns a run's DAG, with the current state of every step, as
// indented JSON — easy to persist or expose over HTTP.
func (q *Quacker) DAGJSON(ctx context.Context, runID string) ([]byte, error) {
	d, err := q.DAG(ctx, runID)
	if err != nil {
		return nil, err
	}
	return d.JSON()
}

// DAGSVG returns a run's DAG rendered as a standalone SVG document: steps as
// status-coloured boxes in dependency order, edges as arrows.
func (q *Quacker) DAGSVG(ctx context.Context, runID string) ([]byte, error) {
	d, err := q.DAG(ctx, runID)
	if err != nil {
		return nil, err
	}
	return d.SVG(), nil
}
