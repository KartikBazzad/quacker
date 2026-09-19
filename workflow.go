package quacker

import "github.com/kartikbazzad/quacker/internal/engine"

// StepDef declares one step of a workflow. Create with Step.
type StepDef[I any] struct {
	name string
	deps []string
	def  *engine.TaskDef
}

// Step declares a workflow step running task t (which takes the workflow
// input type I). Optional deps name steps that must succeed first; a step
// with dependencies stays BLOCKED until all of them succeed.
func Step[I any, O any](name string, t *Task[I, O], deps ...string) StepDef[I] {
	return StepDef[I]{name: name, deps: deps, def: t.toDef()}
}

// Workflow is a DAG of steps sharing one input. Steps run as soon as their
// dependencies succeed; the run output is the output of the last declared
// step. If any step exhausts its retries the run fails and remaining steps
// are cancelled.
type Workflow[I any] struct {
	name  string
	steps []StepDef[I]
}

// NewWorkflow creates a workflow from step declarations. Step names must be
// unique and dependencies must form a DAG (cycles are rejected at enqueue).
func NewWorkflow[I any](name string, steps ...StepDef[I]) *Workflow[I] {
	return &Workflow[I]{name: name, steps: steps}
}

// Name returns the workflow's registered name.
func (w *Workflow[I]) Name() string { return w.name }
