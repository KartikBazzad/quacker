package engine

import "fmt"

// validateDAG checks step names are unique and dependencies exist and are
// acyclic (Kahn's algorithm).
func validateDAG(steps []StepReq) error {
	names := make(map[string]bool, len(steps))
	for _, s := range steps {
		if s.Name == "" {
			return fmt.Errorf("quacker: steps require names")
		}
		if names[s.Name] {
			return fmt.Errorf("quacker: duplicate step name %q", s.Name)
		}
		names[s.Name] = true
	}
	indeg := make(map[string]int, len(steps))
	adj := make(map[string][]string, len(steps))
	for _, s := range steps {
		indeg[s.Name] = len(s.Deps)
		for _, dep := range s.Deps {
			if !names[dep] {
				return fmt.Errorf("quacker: step %q depends on unknown step %q", s.Name, dep)
			}
			adj[dep] = append(adj[dep], s.Name)
		}
	}
	var queue []string
	for _, s := range steps {
		if indeg[s.Name] == 0 {
			queue = append(queue, s.Name)
		}
	}
	visited := 0
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		visited++
		for _, m := range adj[n] {
			indeg[m]--
			if indeg[m] == 0 {
				queue = append(queue, m)
			}
		}
	}
	if visited != len(steps) {
		return fmt.Errorf("quacker: workflow has a dependency cycle")
	}
	return nil
}
