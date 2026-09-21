package quacker

import (
	"fmt"
	"strings"
)

// SVG renders the DAG as a standalone SVG document.
func (d *DAG) SVG() []byte { return renderDAGSVG(d) }

const (
	dagNodeH  = 44.0
	dagHGap   = 72.0
	dagVGap   = 20.0
	dagPadX   = 28.0
	dagPadY   = 24.0
	dagTitleH = 54.0
	dagLegend = 46.0

	// A group (cluster) box reserves padding around its members plus a strip
	// at the top for its label.
	clusterPadX   = 16.0
	clusterPadY   = 12.0
	clusterLabelH = 18.0
)

// dagNodes returns the standard legend order.
func dagLegendStatuses() []Status {
	return []Status{
		StatusQueued, StatusRunning, StatusBlocked, StatusSucceeded, StatusFailed,
		StatusCancelled, StatusSuspended, StatusPaused, StatusInterrupted,
	}
}

// svgRect is an axis-aligned rectangle in SVG coordinates.
type svgRect struct{ x, y, w, h float64 }

// dagCluster is one node of the layout tree: the root run's own steps (key "")
// or one group's steps. Children are nested groups.
type dagCluster struct {
	key      string
	deps     []string // group-level dependencies (names of nodes or groups)
	children []*dagCluster
	nodes    []int // indices into d.Nodes of this cluster's direct members
	// local holds the direct members' positions relative to this cluster's
	// outer box; unitPos holds each child cluster's position there.
	local   map[string][2]float64
	unitPos map[string][2]float64
	w, h    float64
}

// layoutDAG computes absolute node positions and group boxes with a
// cluster-aware layout: each group is laid out on its own, sized, then placed
// as one atomic box in its parent's layout and translated into position. This
// keeps a group's box around only its own nodes — unlike a bounding box over
// globally layered members, which can enclose nodes from a sibling group.
func layoutDAG(d *DAG, nodeW float64) (pos map[string][2]float64, boxes map[string]svgRect, width, height float64) {
	root := buildClusters(d)
	layoutCluster(root, d, nodeW, true)
	pos = make(map[string][2]float64, len(d.Nodes))
	boxes = make(map[string]svgRect, len(d.Groups))
	absolutize(root, 0, dagTitleH, pos, boxes)
	return pos, boxes, root.w, dagTitleH + root.h + dagLegend
}

// buildClusters builds the layout tree from the flat node/group lists. A group
// whose Parent is missing or self-referential hangs off the root.
func buildClusters(d *DAG) *dagCluster {
	root := &dagCluster{key: ""}
	byKey := map[string]*dagCluster{"": root}
	for _, g := range d.Groups {
		if _, ok := byKey[g.Name]; !ok {
			byKey[g.Name] = &dagCluster{key: g.Name}
		}
	}
	for _, g := range d.Groups {
		c := byKey[g.Name]
		c.deps = g.Deps
		p := byKey[g.Parent]
		if p == nil || p == c {
			p = root
		}
		p.children = append(p.children, c)
	}
	for i, n := range d.Nodes {
		c := byKey[n.Group]
		if c == nil {
			c = root
		}
		c.nodes = append(c.nodes, i)
	}
	return root
}

// layoutCluster lays out one cluster in its own coordinate space, with (0,0)
// at the top-left of its outer box, and records its size. Child clusters are
// already laid out and are treated as atomic units here.
func layoutCluster(c *dagCluster, d *DAG, nodeW float64, root bool) {
	c.local = map[string][2]float64{}
	c.unitPos = map[string][2]float64{}
	for _, ch := range c.children {
		layoutCluster(ch, d, nodeW, false)
	}

	type unit struct {
		key   string
		w, h  float64
		child *dagCluster
	}
	var units []*unit
	for _, ni := range c.nodes {
		units = append(units, &unit{key: d.Nodes[ni].Name, w: nodeW, h: dagNodeH})
	}
	for _, ch := range c.children {
		units = append(units, &unit{key: ch.key, w: ch.w, h: ch.h, child: ch})
	}
	if len(units) == 0 {
		return
	}

	// Map every node in this subtree to the unit that represents it here, so
	// edges can be lifted to cluster level (a node inside a child cluster is
	// represented by that cluster).
	owner := make(map[string]*unit, len(d.Nodes))
	for _, u := range units {
		if u.child == nil {
			owner[u.key] = u
			continue
		}
		var collect func(*dagCluster)
		collect = func(cc *dagCluster) {
			for _, ni := range cc.nodes {
				owner[d.Nodes[ni].Name] = u
			}
			for _, gc := range cc.children {
				collect(gc)
			}
		}
		collect(u.child)
	}

	indeg := make(map[*unit]int, len(units))
	adj := make(map[*unit][]*unit, len(units))
	for _, u := range units {
		indeg[u] = 0
	}
	for _, e := range d.Edges {
		fu, tu := owner[e.From], owner[e.To]
		if fu == nil || tu == nil || fu == tu {
			continue // internal to a unit: laid out inside that unit
		}
		adj[fu] = append(adj[fu], tu)
		indeg[tu]++
	}
	// Group-level dependencies: a child cluster's Deps point at nodes or at
	// other groups; lift them to the unit graph too, so a group is placed
	// after whatever it waits on.
	groupUnit := map[string]*unit{}
	for _, u := range units {
		if u.child != nil {
			groupUnit[u.key] = u
		}
	}
	for _, ch := range c.children {
		cu := groupUnit[ch.key]
		if cu == nil {
			continue
		}
		for _, dep := range ch.deps {
			du := owner[dep]
			if du == nil {
				du = groupUnit[dep]
			}
			if du == nil || du == cu {
				continue
			}
			adj[du] = append(adj[du], cu)
			indeg[cu]++
		}
	}

	// Longest-path levels over units.
	level := make(map[*unit]int, len(units))
	var queue []*unit
	for _, u := range units {
		if indeg[u] == 0 {
			level[u] = 0
			queue = append(queue, u)
		}
	}
	for len(queue) > 0 {
		u := queue[0]
		queue = queue[1:]
		for _, v := range adj[u] {
			if level[u]+1 > level[v] {
				level[v] = level[u] + 1
			}
			indeg[v]--
			if indeg[v] == 0 {
				queue = append(queue, v)
			}
		}
	}

	maxLevel := 0
	byLevel := map[int][]*unit{}
	for _, u := range units {
		l, ok := level[u]
		if !ok {
			l = 0 // malformed/cyclic input: fall back to the first column
		}
		byLevel[l] = append(byLevel[l], u)
		if l > maxLevel {
			maxLevel = l
		}
	}

	// Column widths/heights, then center each unit in its column.
	colW := make([]float64, maxLevel+1)
	colH := make([]float64, maxLevel+1)
	for l, us := range byLevel {
		var h float64
		for i, u := range us {
			if u.w > colW[l] {
				colW[l] = u.w
			}
			h += u.h
			if i > 0 {
				h += dagVGap
			}
		}
		colH[l] = h
	}
	var contentW, contentH float64
	for l := 0; l <= maxLevel; l++ {
		contentW += colW[l]
		if l < maxLevel {
			contentW += dagHGap
		}
		if colH[l] > contentH {
			contentH = colH[l]
		}
	}
	colX := make([]float64, maxLevel+1)
	x := 0.0
	for l := 0; l <= maxLevel; l++ {
		colX[l] = x
		x += colW[l] + dagHGap
	}
	for l, us := range byLevel {
		y := (contentH - colH[l]) / 2
		for _, u := range us {
			ux := colX[l] + (colW[l]-u.w)/2
			if u.child == nil {
				c.local[u.key] = [2]float64{ux, y}
			} else {
				c.unitPos[u.key] = [2]float64{ux, y}
			}
			y += u.h + dagVGap
		}
	}

	padX, padY, labelH := dagPadX, dagPadY, 0.0
	if !root {
		padX, padY, labelH = clusterPadX, clusterPadY, clusterLabelH
	}
	c.w = contentW + 2*padX
	c.h = contentH + 2*padY + labelH
	for k, p := range c.local {
		c.local[k] = [2]float64{p[0] + padX, p[1] + padY + labelH}
	}
	for k, p := range c.unitPos {
		c.unitPos[k] = [2]float64{p[0] + padX, p[1] + padY + labelH}
	}
}

// absolutize translates a cluster's local coordinates into absolute SVG
// coordinates, recursing into child clusters at their unit positions.
func absolutize(c *dagCluster, dx, dy float64, pos map[string][2]float64, boxes map[string]svgRect) {
	for name, p := range c.local {
		pos[name] = [2]float64{p[0] + dx, p[1] + dy}
	}
	if c.key != "" {
		boxes[c.key] = svgRect{x: dx, y: dy, w: c.w, h: c.h}
	}
	for _, ch := range c.children {
		p := c.unitPos[ch.key]
		absolutize(ch, dx+p[0], dy+p[1], pos, boxes)
	}
}

// dagNodeRects maps every node to its rectangle from the layout.
func dagNodeRects(d *DAG, pos map[string][2]float64, nodeW float64) map[string]svgRect {
	rects := make(map[string]svgRect, len(d.Nodes))
	for _, n := range d.Nodes {
		p := pos[n.Name]
		rects[n.Name] = svgRect{x: p[0], y: p[1], w: nodeW, h: dagNodeH}
	}
	return rects
}

// dagEdgeObstacles is the obstacle set for one edge: every node except the
// edge's own endpoints, which are passed exactly so the route can leave and
// enter their ports. Other nodes are inflated by the routing clearance.
func dagEdgeObstacles(d *DAG, rects map[string]svgRect, from, to string) []svgRect {
	obstacles := make([]svgRect, 0, len(d.Nodes))
	for _, n := range d.Nodes {
		r := rects[n.Name]
		if n.Name == from || n.Name == to {
			obstacles = append(obstacles, r)
		} else {
			obstacles = append(obstacles, inflateRect(r, routeClearance))
		}
	}
	return obstacles
}

// renderDAGSVG lays nodes out in dependency levels and draws edges
// left-to-right. It is dependency-free and deterministic for a given DAG.
func renderDAGSVG(d *DAG) []byte {
	if len(d.Nodes) == 0 {
		return []byte(`<svg xmlns="http://www.w3.org/2000/svg" width="10" height="10"></svg>`)
	}

	nodeW := dagNodeWidth(d.Nodes)
	pos, boxes, width, height := layoutDAG(d, nodeW)

	legend := dagLegendStatuses()
	legendWidth := dagPadX
	for _, s := range legend {
		legendWidth += 18 + float64(len(s))*7 + 16
	}
	if legendWidth > width {
		width = legendWidth + dagPadX
	}

	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" width="%.0f" height="%.0f" viewBox="0 0 %.0f %.0f" font-family="-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif">`,
		width, height, width, height)
	b.WriteString(`<defs><marker id="qarrow" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto" markerUnits="strokeWidth"><path d="M0,0 L8,3 L0,6 z" fill="#5f6368"/></marker></defs>`)
	fmt.Fprintf(&b, `<rect width="%.0f" height="%.0f" fill="#ffffff"/>`, width, height)

	fmt.Fprintf(&b, `<text x="%.0f" y="30" font-size="16" font-weight="600" fill="#202124">%s</text>`,
		dagPadX, escapeXML(d.Workflow))
	fmt.Fprintf(&b, `<text x="%.0f" y="47" font-size="12" fill="#5f6368">%s · %s · %s</text>`,
		dagPadX, escapeXML(string(d.Kind)), escapeXML(string(d.Status)), escapeXML(d.RunID))

	// Group boxes first (behind everything). Their rectangles come from the
	// layout, so they never enclose a node from another group.
	for _, g := range d.Groups {
		bx, ok := boxes[g.Name]
		if !ok || bx.w <= 0 || bx.h <= 0 {
			continue
		}
		fmt.Fprintf(&b, `<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" rx="12" fill="#fafaf9" stroke="#d6d3d1" stroke-dasharray="4 4"/>`,
			bx.x, bx.y, bx.w, bx.h)
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" font-size="11" font-weight="600" fill="#78716c">%s</text>`,
			bx.x+10, bx.y+14, escapeXML(g.Label))
	}

	// Edges next, then nodes on top. An endpoint is a node name or, for a
	// group-level edge, a group name resolved to its box.
	anchor := func(name string, right bool) (float64, float64, bool) {
		if p, ok := pos[name]; ok {
			x := p[0]
			if right {
				x += nodeW
			}
			return x, p[1] + dagNodeH/2, true
		}
		if bx, ok := boxes[name]; ok {
			x := bx.x
			if right {
				x += bx.w
			}
			return x, bx.y + bx.h/2, true
		}
		return 0, 0, false
	}
	rects := dagNodeRects(d, pos, nodeW)
	for _, e := range d.Edges {
		x1, y1, ok1 := anchor(e.From, true)
		x2, y2, ok2 := anchor(e.To, false)
		if !ok1 || !ok2 {
			continue
		}
		// Route around other nodes; fall back to a bezier when no clean
		// right-angle path exists (e.g. a backward edge).
		if pts := routeEdge(x1, y1, x2, y2, dagEdgeObstacles(d, rects, e.From, e.To)); len(pts) >= 2 {
			fmt.Fprintf(&b, `<path d="%s" fill="none" stroke="#9aa0a6" stroke-width="1.6" marker-end="url(#qarrow)"/>`, routePathD(pts))
			continue
		}
		dx := (x2 - x1) / 2
		if dx < 20 {
			dx = 20
		}
		fmt.Fprintf(&b, `<path d="M %.1f %.1f C %.1f %.1f, %.1f %.1f, %.1f %.1f" fill="none" stroke="#9aa0a6" stroke-width="1.6" marker-end="url(#qarrow)"/>`,
			x1, y1, x1+dx, y1, x2-dx, y2, x2, y2)
	}

	for _, n := range d.Nodes {
		p := pos[n.Name]
		fill, stroke, text := dagStatusColors(n.Status)
		cx := p[0] + nodeW/2
		fmt.Fprintf(&b, `<g><title>%s — %s (attempt %d/%d)</title>`, escapeXML(n.Name), escapeXML(string(n.Status)), n.Attempts, n.MaxAttempts)
		fmt.Fprintf(&b, `<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" rx="8" fill="%s" stroke="%s" stroke-width="1.6"/>`,
			p[0], p[1], nodeW, dagNodeH, fill, stroke)
		subtitle := n.Task != "" && n.Task != n.Name
		nameY := p[1] + dagNodeH/2 + 1
		if subtitle {
			nameY = p[1] + 18
		}
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" font-size="13" font-weight="600" fill="%s" text-anchor="middle">%s</text>`,
			cx, nameY, text, escapeXML(truncateLabel(n.Name, nodeW)))
		if subtitle {
			fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" font-size="10" fill="#5f6368" text-anchor="middle">%s</text>`,
				cx, p[1]+33, escapeXML(truncateLabel(n.Task, nodeW)))
		}
		b.WriteString(`</g>`)
	}

	legendY := height - dagLegend + 16
	x := dagPadX
	for _, s := range legend {
		fill, stroke, text := dagStatusColors(s)
		fmt.Fprintf(&b, `<rect x="%.1f" y="%.1f" width="12" height="12" rx="3" fill="%s" stroke="%s"/>`, x, legendY-10, fill, stroke)
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" font-size="11" fill="%s">%s</text>`, x+18, legendY, text, escapeXML(string(s)))
		x += 18 + float64(len(s))*7 + 16
	}
	b.WriteString(`</svg>`)
	return []byte(b.String())
}

// dagLevels assigns each node a dependency level: 0 for a root, otherwise one
// past the deepest dependency. Kahn's algorithm keeps it safe on malformed
// (cyclic) data, which gets level 0.
func dagLevels(d *DAG) map[string]int {
	known := make(map[string]bool, len(d.Nodes))
	for _, n := range d.Nodes {
		known[n.Name] = true
	}
	indeg := make(map[string]int, len(d.Nodes))
	adj := map[string][]string{}
	for _, n := range d.Nodes {
		if _, ok := indeg[n.Name]; !ok {
			indeg[n.Name] = 0
		}
	}
	for _, n := range d.Nodes {
		for _, dep := range n.Deps {
			if !known[dep] {
				continue // ignore dangling dependency names
			}
			adj[dep] = append(adj[dep], n.Name)
			indeg[n.Name]++
		}
	}
	level := make(map[string]int, len(d.Nodes))
	var queue []string
	for _, n := range d.Nodes {
		if indeg[n.Name] == 0 {
			level[n.Name] = 0
			queue = append(queue, n.Name)
		}
	}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, m := range adj[cur] {
			if l := level[cur] + 1; l > level[m] {
				level[m] = l
			}
			indeg[m]--
			if indeg[m] == 0 {
				queue = append(queue, m)
			}
		}
	}
	for _, n := range d.Nodes {
		if _, ok := level[n.Name]; !ok {
			level[n.Name] = 0
		}
	}
	return level
}

// dagNodeWidth picks a uniform box width from the longest label, clamped to
// keep very long or very short names readable.
func dagNodeWidth(nodes []DAGNode) float64 {
	longest := 0
	for _, n := range nodes {
		if len(n.Name) > longest {
			longest = len(n.Name)
		}
		if len(n.Task) > longest {
			longest = len(n.Task)
		}
	}
	w := float64(longest)*7.4 + 34
	if w < 130 {
		w = 130
	}
	if w > 300 {
		w = 300
	}
	return w
}

// dagStatusColors returns fill, stroke, and text colours for a status.
func dagStatusColors(s Status) (fill, stroke, text string) {
	switch s {
	case StatusSucceeded:
		return "#e6f4ea", "#34a853", "#137333"
	case StatusRunning:
		return "#e8f0fe", "#1a73e8", "#174ea6"
	case StatusQueued:
		return "#f1f3f4", "#9aa0a6", "#3c4043"
	case StatusBlocked:
		return "#fef7e0", "#f9ab00", "#b06000"
	case StatusSuspended:
		return "#f3e8fd", "#9334e6", "#6a1b9a"
	case StatusPaused:
		return "#e0f2f1", "#00897b", "#00695c"
	case StatusFailed:
		return "#fce8e6", "#d93025", "#a50e0e"
	case StatusCancelled:
		return "#fef3e2", "#e37400", "#a05a00"
	case StatusInterrupted:
		return "#eceff1", "#546e7a", "#37474f"
	default:
		return "#f1f3f4", "#9aa0a6", "#3c4043"
	}
}

func truncateLabel(s string, nodeW float64) string {
	max := int((nodeW - 20) / 7.0)
	if max < 4 {
		max = 4
	}
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	if max <= 1 {
		return string(r[:max])
	}
	return string(r[:max-1]) + "…"
}

func escapeXML(s string) string {
	return strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		`"`, "&quot;",
		"'", "&#39;",
	).Replace(s)
}
