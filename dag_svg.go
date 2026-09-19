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
)

// dagNodes returns the standard legend order.
func dagLegendStatuses() []Status {
	return []Status{
		StatusQueued, StatusRunning, StatusBlocked, StatusSucceeded, StatusFailed,
		StatusCancelled, StatusSuspended, StatusPaused, StatusInterrupted,
	}
}

// renderDAGSVG lays nodes out in dependency levels (longest-path) and draws
// edges left-to-right. It is dependency-free and deterministic for a given
// DAG.
func renderDAGSVG(d *DAG) []byte {
	if len(d.Nodes) == 0 {
		return []byte(`<svg xmlns="http://www.w3.org/2000/svg" width="10" height="10"></svg>`)
	}

	levels := dagLevels(d)
	byLevel := map[int][]int{}
	maxLevel, maxRows := 0, 0
	for i, n := range d.Nodes {
		l := levels[n.Name]
		byLevel[l] = append(byLevel[l], i)
		if l > maxLevel {
			maxLevel = l
		}
	}
	for _, idxs := range byLevel {
		if len(idxs) > maxRows {
			maxRows = len(idxs)
		}
	}

	nodeW := dagNodeWidth(d.Nodes)
	rowsH := float64(maxRows)*dagNodeH + float64(maxRows-1)*dagVGap
	if maxRows <= 0 {
		rowsH = 0
	}
	width := dagPadX*2 + float64(maxLevel+1)*nodeW + float64(maxLevel)*dagHGap

	legend := dagLegendStatuses()
	legendWidth := dagPadX
	for _, s := range legend {
		legendWidth += 18 + float64(len(s))*7 + 16
	}
	if legendWidth > width {
		width = legendWidth + dagPadX
	}
	height := dagTitleH + dagPadY*2 + rowsH + dagLegend

	pos := map[string][2]float64{}
	for l, idxs := range byLevel {
		x := dagPadX + float64(l)*(nodeW+dagHGap)
		stackH := float64(len(idxs))*dagNodeH + float64(len(idxs)-1)*dagVGap
		top := dagTitleH + dagPadY + (rowsH-stackH)/2
		for i, ni := range idxs {
			pos[d.Nodes[ni].Name] = [2]float64{x, top + float64(i)*(dagNodeH+dagVGap)}
		}
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

	// Edges first, so boxes cover their endpoints.
	for _, e := range d.Edges {
		from, ok1 := pos[e.From]
		to, ok2 := pos[e.To]
		if !ok1 || !ok2 {
			continue
		}
		x1 := from[0] + nodeW
		y1 := from[1] + dagNodeH/2
		x2 := to[0]
		y2 := to[1] + dagNodeH/2
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
