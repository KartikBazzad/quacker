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
	RunID    string    `json:"run_id"`
	Workflow string    `json:"workflow"`
	Kind     Kind      `json:"kind"`
	Status   Status    `json:"status"`
	Queue    string    `json:"queue"`
	Nodes    []DAGNode `json:"nodes"`
	Edges    []DAGEdge `json:"edges"`
}

// DAG returns the step graph of a run with the current state of every node.
// For a single-task run it is one node with no edges. Returns ErrNotFound for
// an unknown run. It covers only this run's own steps; use DAGTree to include
// child runs (spawned with EnqueueChild).
func (q *Quacker) DAG(ctx context.Context, runID string) (*DAG, error) {
	ex, err := q.Execution(ctx, runID)
	if err != nil {
		return nil, err
	}
	d := &DAG{RunID: ex.RunID, Workflow: ex.Workflow, Kind: ex.Kind, Status: ex.Status, Queue: ex.Queue}
	for _, s := range ex.Steps {
		d.Nodes = append(d.Nodes, DAGNode{
			Name: s.Name, Task: s.Task, Status: s.Status, Deps: s.Deps,
			Attempts: s.Attempts, MaxAttempts: s.MaxAttempts, Error: s.Error,
		})
		for _, dep := range s.Deps {
			d.Edges = append(d.Edges, DAGEdge{From: dep, To: s.Name})
		}
	}
	return d, nil
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
