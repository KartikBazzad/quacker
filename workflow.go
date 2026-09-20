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

// Group is a named set of workflow steps with a group-level dependency: the
// whole group runs after the groups named in After, and member steps only
// declare their intra-group dependencies. Group deps expand to member steps at
// enqueue, so nothing extra runs and no gate step is needed.
type Group[I any] struct {
	name  string
	steps []StepDef[I]
	after []string
}

// NewGroup creates a group named name from step declarations. A group can be
// passed to NewGroupWorkflow and chained with After.
func NewGroup[I any](name string, steps ...StepDef[I]) *Group[I] {
	return &Group[I]{name: name, steps: steps}
}

// After declares that this whole group runs only after the named groups have
// completed. It returns the group so calls can be chained.
func (g *Group[I]) After(groups ...string) *Group[I] {
	g.after = append(g.after, groups...)
	return g
}

// Name returns the group's name.
func (g *Group[I]) Name() string { return g.name }

// Workflow is a DAG of steps sharing one input. Steps run as soon as their
// dependencies succeed; the run output is the output of the last declared
// step. If any step exhausts its retries the run fails and remaining steps
// are cancelled.
type Workflow[I any] struct {
	name   string
	steps  []StepDef[I]
	groups []RunGroup
}

// NewWorkflow creates a workflow from step declarations. Step names must be
// unique and dependencies must form a DAG (cycles are rejected at enqueue).
func NewWorkflow[I any](name string, steps ...StepDef[I]) *Workflow[I] {
	return &Workflow[I]{name: name, steps: steps}
}

// NewGroupWorkflow creates a workflow from groups. Each group's After
// dependencies expand to the member steps of those groups, so a group runs
// only after its dependencies; member steps never repeat the group dep. Group
// structure is persisted with the run so DAG/DAGSVG draw group boxes and
// group-to-group edges instead of the expanded step mesh.
func NewGroupWorkflow[I any](name string, groups ...*Group[I]) *Workflow[I] {
	wf := &Workflow[I]{name: name}
	byName := map[string]*Group[I]{}
	for _, g := range groups {
		if g == nil || g.name == "" {
			continue
		}
		byName[g.name] = g
	}
	for _, g := range groups {
		if g == nil {
			continue
		}
		// The step names every group this one waits on contributes.
		var depSteps []string
		for _, dep := range g.after {
			if dg := byName[dep]; dg != nil {
				for _, s := range dg.steps {
					depSteps = append(depSteps, s.name)
				}
			}
		}
		depSteps = dedupStrings(depSteps)
		members := make([]string, 0, len(g.steps))
		for _, s := range g.steps {
			members = append(members, s.name)
			ns := s
			ns.deps = dedupStrings(append(append([]string{}, s.deps...), depSteps...))
			wf.steps = append(wf.steps, ns)
		}
		wf.groups = append(wf.groups, RunGroup{Name: g.name, Deps: append([]string{}, g.after...), Steps: members})
	}
	return wf
}

// Name returns the workflow's registered name.
func (w *Workflow[I]) Name() string { return w.name }
