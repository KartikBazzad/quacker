package quacker

import (
	"bytes"
	"testing"
)

// routeHitsRect reports whether any axis-aligned segment of pts crosses the
// interior of r.
func routeHitsRect(pts []routePoint, r svgRect) bool {
	for i := 1; i < len(pts); i++ {
		a, b := pts[i-1], pts[i]
		xa, xb := a.x, b.x
		if xa > xb {
			xa, xb = xb, xa
		}
		ya, yb := a.y, b.y
		if ya > yb {
			ya, yb = yb, ya
		}
		if xb > r.x+rrEps && xa < r.x+r.w-rrEps && yb > r.y+rrEps && ya < r.y+r.h-rrEps {
			return true
		}
	}
	return false
}

// TestRouteAvoidsNodeBetweenPorts: a blocker sitting on the straight line
// between two ports forces a detour that never enters the inflated blocker.
func TestRouteAvoidsNodeBetweenPorts(t *testing.T) {
	src := svgRect{x: 0, y: 100, w: 80, h: 44}
	blocker := svgRect{x: 120, y: 100, w: 80, h: 44}
	dst := svgRect{x: 240, y: 100, w: 80, h: 44}
	inflated := inflateRect(blocker, routeClearance)

	pts := routeEdge(src.x+src.w, src.y+src.h/2, dst.x, dst.y+dst.h/2,
		[]svgRect{src, dst, inflated})
	if len(pts) < 2 {
		t.Fatal("no route around the blocker")
	}
	if routeHitsRect(pts, inflated) {
		t.Fatalf("route crosses the blocker: %+v", pts)
	}
	if len(pts) < 4 {
		t.Fatalf("expected a detour, got a straight path: %+v", pts)
	}
}

// TestRoutedEdgesAvoidNodes: every edge of a skip-level DAG routes without
// crossing any non-endpoint node.
func TestRoutedEdgesAvoidNodes(t *testing.T) {
	d := &DAG{
		RunID: "r", Workflow: "w", Status: StatusSucceeded,
		Nodes: []DAGNode{{Name: "a"}, {Name: "b"}, {Name: "c"}},
		// a->c skips over b, which sits on the straight line between them.
		Edges: []DAGEdge{{From: "a", To: "b"}, {From: "a", To: "c"}, {From: "b", To: "c"}},
	}
	nodeW := dagNodeWidth(d.Nodes)
	pos, _, _, _ := layoutDAG(d, nodeW)
	rects := dagNodeRects(d, pos, nodeW)

	for _, e := range d.Edges {
		x1, y1 := pos[e.From][0]+nodeW, pos[e.From][1]+dagNodeH/2
		x2, y2 := pos[e.To][0], pos[e.To][1]+dagNodeH/2
		pts := routeEdge(x1, y1, x2, y2, dagEdgeObstacles(d, rects, e.From, e.To))
		if len(pts) < 2 {
			t.Fatalf("edge %s->%s: no route", e.From, e.To)
		}
		for _, n := range d.Nodes {
			if n.Name == e.From || n.Name == e.To {
				continue
			}
			if routeHitsRect(pts, inflateRect(rects[n.Name], routeClearance)) {
				t.Fatalf("edge %s->%s crosses node %s: %+v", e.From, e.To, n.Name, pts)
			}
		}
	}
}

// TestDAGSVGRoutingDeterministic: the router is stable for a given DAG.
func TestDAGSVGRoutingDeterministic(t *testing.T) {
	d := &DAG{
		RunID: "r", Workflow: "w", Status: StatusSucceeded,
		Nodes: []DAGNode{{Name: "a"}, {Name: "b"}, {Name: "c"}, {Name: "d"}},
		Edges: []DAGEdge{
			{From: "a", To: "b"}, {From: "a", To: "c"}, {From: "b", To: "d"},
			{From: "c", To: "d"}, {From: "a", To: "d"},
		},
	}
	if !bytes.Equal(renderDAGSVG(d), renderDAGSVG(d)) {
		t.Fatal("renderDAGSVG output is not deterministic")
	}
}

// TestRouteFallback: an un-routable (backward) edge yields nil so the caller
// falls back to a bezier, and routePathD handles degenerate inputs.
func TestRouteFallback(t *testing.T) {
	if pts := routeEdge(100, 50, 100, 80, nil); pts != nil {
		t.Fatalf("backward edge should not route, got %+v", pts)
	}
	if got := routePathD(nil); got != "" {
		t.Fatalf("empty path = %q", got)
	}
	if got := routePathD([]routePoint{{x: 1, y: 2}}); got != "M 1.0 2.0" {
		t.Fatalf("single point path = %q", got)
	}
}
