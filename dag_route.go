package quacker

import (
	"container/heap"
	"fmt"
	"math"
	"sort"
	"strings"
)

const (
	// routeClearance inflates every obstacle rectangle by this many pixels:
	// the router keeps this margin between an edge and any node it avoids.
	routeClearance = 7.0
	// routeBendPenalty is added to the cost of every corner, so the router
	// prefers one long straight run over a staircase of equally short ones.
	routeBendPenalty = 24.0
	// routeMargin places an outer border around every obstacle, guaranteeing a
	// detour exists even when the direct corridor is fully walled off.
	routeMargin = 16.0
	// routeCorner is the radius used to round a corner when the path is drawn.
	routeCorner = 6.0
	// rrEps keeps a segment that lies exactly on an obstacle boundary (the
	// port of the node it starts or ends at) from counting as an intersection.
	rrEps = 1e-6
)

// routePoint is a vertex of an orthogonal route in SVG coordinates.
type routePoint struct{ x, y float64 }

// inflateRect grows r by by on every side.
func inflateRect(r svgRect, by float64) svgRect {
	return svgRect{x: r.x - by, y: r.y - by, w: r.w + 2*by, h: r.h + 2*by}
}

// routeEdge finds a right-angle path from (x1,y1) to (x2,y2) that does not pass
// through any obstacle, using a visibility grid built from the obstacle edges
// and Dijkstra with a bend penalty. The caller supplies obstacles exactly as
// they should be respected (endpoint nodes are usually passed un-inflated so
// the path can leave and enter their ports). It returns the simplified
// waypoints, or nil when no route exists (callers fall back to a bezier).
func routeEdge(x1, y1, x2, y2 float64, obstacles []svgRect) []routePoint {
	if x2 <= x1 {
		// Ports are right-edge to left-edge, so a route only ever runs
		// forward. Anything else is left to the fallback.
		return nil
	}

	xs := []float64{x1, x2}
	ys := []float64{y1, y2}
	for _, r := range obstacles {
		xs = append(xs, r.x, r.x+r.w)
		ys = append(ys, r.y, r.y+r.h)
	}
	xs = uniqSortedFloat(xs)
	ys = uniqSortedFloat(ys)
	xs = append([]float64{xs[0] - routeMargin}, append(xs, xs[len(xs)-1]+routeMargin)...)
	ys = append([]float64{ys[0] - routeMargin}, append(ys, ys[len(ys)-1]+routeMargin)...)

	width, height := len(xs), len(ys)
	xi := make(map[float64]int, width)
	for i, v := range xs {
		xi[v] = i
	}
	yi := make(map[float64]int, height)
	for j, v := range ys {
		yi[v] = j
	}
	si, sj := xi[x1], yi[y1]
	gi, gj := xi[x2], yi[y2]

	const (
		dirRight = 0
		dirLeft  = 1
		dirDown  = 2
		dirUp    = 3
		dirNone  = 4 // the seeded start state, before the first move
	)
	dxs := [4]int{1, -1, 0, 0}
	dys := [4]int{0, 0, 1, -1}

	state := func(i, j, dir int) int { return (j*width+i)*5 + dir }
	dist := make([]float64, width*height*5)
	prev := make([]int, len(dist))
	for i := range dist {
		dist[i] = math.Inf(1)
		prev[i] = -1
	}

	// clearH reports whether the horizontal segment between columns i and ni at
	// row j is free of obstacles.
	clearH := func(i, ni, j int) bool {
		xa, xb := xs[i], xs[ni]
		if xa > xb {
			xa, xb = xb, xa
		}
		y := ys[j]
		for _, r := range obstacles {
			if y > r.y+rrEps && y < r.y+r.h-rrEps && xb > r.x+rrEps && xa < r.x+r.w-rrEps {
				return false
			}
		}
		return true
	}
	// clearV reports whether the vertical segment between rows j and nj at
	// column i is free of obstacles.
	clearV := func(j, nj, i int) bool {
		ya, yb := ys[j], ys[nj]
		if ya > yb {
			ya, yb = yb, ya
		}
		x := xs[i]
		for _, r := range obstacles {
			if x > r.x+rrEps && x < r.x+r.w-rrEps && yb > r.y+rrEps && ya < r.y+r.h-rrEps {
				return false
			}
		}
		return true
	}

	start := state(si, sj, dirNone)
	goal := state(gi, gj, dirRight)
	dist[start] = 0
	h := &rrHeap{{state: start, i: si, j: sj, dir: dirNone, dist: 0}}
	heap.Init(h)

	for h.Len() > 0 {
		cur := heap.Pop(h).(rrNode)
		if cur.dist > dist[cur.state] {
			continue
		}
		if cur.state == goal {
			break
		}
		for d := 0; d < 4; d++ {
			if cur.dir == dirNone && d != dirRight {
				continue // leave the source port heading right, away from the node
			}
			ni, nj := cur.i+dxs[d], cur.j+dys[d]
			if ni < 0 || ni >= width || nj < 0 || nj >= height {
				continue
			}
			var seg float64
			if d == dirRight || d == dirLeft {
				if !clearH(cur.i, ni, cur.j) {
					continue
				}
				seg = math.Abs(xs[ni] - xs[cur.i])
			} else {
				if !clearV(cur.j, nj, cur.i) {
					continue
				}
				seg = math.Abs(ys[nj] - ys[cur.j])
			}
			cost := cur.dist + seg
			if cur.dir != dirNone && cur.dir != d {
				cost += routeBendPenalty
			}
			ns := state(ni, nj, d)
			if cost < dist[ns] {
				dist[ns] = cost
				prev[ns] = cur.state
				heap.Push(h, rrNode{state: ns, i: ni, j: nj, dir: d, dist: cost})
			}
		}
	}
	if math.IsInf(dist[goal], 1) {
		return nil
	}

	var states []int
	for s := goal; s != -1; s = prev[s] {
		states = append(states, s)
	}
	for l, r := 0, len(states)-1; l < r; l, r = l+1, r-1 {
		states[l], states[r] = states[r], states[l]
	}
	pts := make([]routePoint, 0, len(states))
	for _, s := range states {
		cell := s / 5
		pts = append(pts, routePoint{x: xs[cell%width], y: ys[cell/width]})
	}
	return simplifyRoute(pts)
}

// simplifyRoute drops collinear intermediate waypoints.
func simplifyRoute(pts []routePoint) []routePoint {
	if len(pts) < 3 {
		return pts
	}
	out := make([]routePoint, 0, len(pts))
	out = append(out, pts[0])
	for i := 1; i < len(pts)-1; i++ {
		a, b, c := out[len(out)-1], pts[i], pts[i+1]
		if (a.x == b.x && b.x == c.x) || (a.y == b.y && b.y == c.y) {
			continue
		}
		out = append(out, b)
	}
	return append(out, pts[len(pts)-1])
}

// routePathD renders waypoints as an SVG path, rounding interior corners.
func routePathD(pts []routePoint) string {
	if len(pts) == 0 {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "M %.1f %.1f", pts[0].x, pts[0].y)
	if len(pts) == 1 {
		return b.String()
	}
	for i := 1; i < len(pts)-1; i++ {
		prev, cur, next := pts[i-1], pts[i], pts[i+1]
		r := math.Min(routeCorner, math.Min(routeDist(prev, cur), routeDist(cur, next))/2)
		if r < 0.5 {
			fmt.Fprintf(&b, " L %.1f %.1f", cur.x, cur.y)
			continue
		}
		ex, ey := routeToward(cur, prev, r)
		fx, fy := routeToward(cur, next, r)
		fmt.Fprintf(&b, " L %.1f %.1f Q %.1f %.1f %.1f %.1f", ex, ey, cur.x, cur.y, fx, fy)
	}
	last := pts[len(pts)-1]
	fmt.Fprintf(&b, " L %.1f %.1f", last.x, last.y)
	return b.String()
}

func routeDist(a, b routePoint) float64 { return math.Hypot(b.x-a.x, b.y-a.y) }

// routeToward returns the point r pixels from `from` in the direction of `to`.
func routeToward(from, to routePoint, r float64) (float64, float64) {
	d := routeDist(from, to)
	if d == 0 {
		return from.x, from.y
	}
	return from.x + (to.x-from.x)/d*r, from.y + (to.y-from.y)/d*r
}

// uniqSortedFloat sorts and de-duplicates.
func uniqSortedFloat(vals []float64) []float64 {
	sort.Float64s(vals)
	out := make([]float64, 0, len(vals))
	for i, v := range vals {
		if i == 0 || v != vals[i-1] {
			out = append(out, v)
		}
	}
	return out
}

// rrNode is a Dijkstra state: a grid vertex plus the direction of arrival.
type rrNode struct {
	state int
	i, j  int
	dir   int
	dist  float64
}

type rrHeap []rrNode

func (h rrHeap) Len() int { return len(h) }
func (h rrHeap) Less(a, b int) bool {
	if h[a].dist != h[b].dist {
		return h[a].dist < h[b].dist
	}
	return h[a].state < h[b].state
}
func (h rrHeap) Swap(a, b int) { h[a], h[b] = h[b], h[a] }
func (h *rrHeap) Push(x any)   { *h = append(*h, x.(rrNode)) }
func (h *rrHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}
