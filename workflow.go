package quacker

import (
	"fmt"

	"github.com/kartikbazzad/quacker/internal/engine"
)

// StepDef declares one step of a workflow. Create with Step.
type StepDef[I any] struct {
	name    string
	deps    []string
	rawDeps []any
	def     *engine.TaskDef
}

// TaskRef is implemented by every Task; a Task may be passed to Step as a
// dependency, resolving to the step that runs it. The interface is unexported
// (sealed), so only quacker tasks can satisfy it.
type TaskRef interface{ dependencyName() string }

func (t *Task[I, O]) dependencyName() string { return t.name }

// Step declares a workflow step running task t (which takes the workflow
// input type I). Optional deps name steps that must succeed first; a step with
// dependencies stays BLOCKED until all of them succeed.
func Step[I any, O any](name string, t *Task[I, O], deps ...string) StepDef[I] {
	raw := make([]any, len(deps))
	for i, d := range deps {
		raw[i] = d
	}
	return StepDef[I]{name: name, rawDeps: raw, def: t.toDef()}
}

// StepOn is Step with dependencies given as *Task values (or step names):
//
//	Step("charge", charge),
//	StepOn("ship", ship, charge)      // depends on the step running charge
//
// A task resolves to the step that runs it, so it is ambiguous when the same
// task backs several steps — use Step with step names there. A task dependency
// that is unknown, ambiguous, or of an unsupported type is reported by
// EnqueueWorkflow.
func StepOn[I any, O any](name string, t *Task[I, O], deps ...any) StepDef[I] {
	return StepDef[I]{name: name, rawDeps: deps, def: t.toDef()}
}

// Workflow is a DAG of steps sharing one input. Steps run as soon as their
// dependencies succeed; the run output is the output of the last declared
// step. If any step exhausts its retries the run fails and remaining steps
// are cancelled.
type Workflow[I any] struct {
	name  string
	steps []StepDef[I]
	err   error
}

// NewWorkflow creates a workflow from step declarations. Step names must be
// unique and dependencies must form a DAG (cycles are rejected at enqueue).
// Dependencies given as a *Task are resolved to that task's step here, and a
// resolution problem (unknown, ambiguous, or unsupported dep) is returned by
// EnqueueWorkflow.
func NewWorkflow[I any](name string, steps ...StepDef[I]) *Workflow[I] {
	w := &Workflow[I]{name: name, steps: steps}
	w.resolveDeps()
	return w
}

// resolveDeps turns raw dependencies (step names or tasks) into step names. A
// task reference resolves to the step that runs it; a task that backs more
// than one step is ambiguous and rejected.
func (w *Workflow[I]) resolveDeps() {
	taskSteps := map[string][]string{}
	for _, s := range w.steps {
		if s.def != nil {
			taskSteps[s.def.Name] = append(taskSteps[s.def.Name], s.name)
		}
	}
	for i := range w.steps {
		s := &w.steps[i]
		resolved := make([]string, 0, len(s.rawDeps))
		for _, d := range s.rawDeps {
			switch v := d.(type) {
			case string:
				resolved = append(resolved, v)
			case TaskRef:
				name := v.dependencyName()
				steps := taskSteps[name]
				switch {
				case len(steps) == 0:
					w.err = fmt.Errorf("quacker: step %q depends on task %q, which is not a step of workflow %q",
						s.name, name, w.name)
					return
				case len(steps) > 1:
					w.err = fmt.Errorf("quacker: step %q depends on task %q, which runs %d steps; depend by step name instead",
						s.name, name, len(steps))
					return
				}
				resolved = append(resolved, steps[0])
			default:
				w.err = fmt.Errorf("quacker: step %q has an unsupported dependency %T (use a step name or a *Task)",
					s.name, d)
				return
			}
		}
		s.deps = resolved
	}
}

// Name returns the workflow's registered name.
func (w *Workflow[I]) Name() string { return w.name }
