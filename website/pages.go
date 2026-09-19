package main

import "html/template"

// pageDef describes one output page: its file, sidebar entry, and content
// source. Order in pages is the sidebar order.
type pageDef struct {
	slug    string // output <slug>.html
	title   string // <title> and top heading
	nav     string // sidebar label; "" hides the page from the nav
	group   string // nav group label
	src     func(b *builder, p pageDef) (template.HTML, error)
	content string // content/*.md source, when src is markdownPage
	docs    string // ../docs/*.md source, when src is docsPage
}

var pages = []pageDef{
	{slug: "index", title: "quacker", nav: "Home", group: "Start", src: renderHome},

	{slug: "getting-started", title: "Getting started", nav: "Getting started", group: "Guides", src: markdownPage, content: "getting-started.md"},
	{slug: "tasks", title: "Tasks", nav: "Tasks", group: "Guides", src: markdownPage, content: "tasks.md"},
	{slug: "workflows", title: "Workflows & DAGs", nav: "Workflows", group: "Guides", src: markdownPage, content: "workflows.md"},
	{slug: "durable", title: "Durable execution", nav: "Durable execution", group: "Guides", src: markdownPage, content: "durable.md"},
	{slug: "concurrency", title: "Concurrency & rate limits", nav: "Concurrency", group: "Guides", src: markdownPage, content: "concurrency.md"},
	{slug: "triggers", title: "Triggers: cron, events, children", nav: "Triggers", group: "Guides", src: markdownPage, content: "triggers.md"},
	{slug: "operations", title: "Operations", nav: "Operations", group: "Guides", src: markdownPage, content: "operations.md"},
	{slug: "plugins", title: "Plugins", nav: "Plugins", group: "Guides", src: markdownPage, content: "plugins.md"},

	{slug: "api", title: "API reference", nav: "API reference", group: "Reference", src: renderAPI},
	{slug: "examples", title: "Examples", nav: "Examples", group: "Reference", src: renderExamples},

	{slug: "architecture", title: "Architecture", nav: "Architecture", group: "Internals", src: docsPage, docs: "ARCHITECTURE.md"},
	{slug: "design-notes", title: "Design notes", nav: "Design notes", group: "Internals", src: docsPage, docs: "DESIGN_NOTES.md"},
	{slug: "benchmarks", title: "Benchmarks", nav: "Benchmarks", group: "Internals", src: docsPage, docs: "BENCHMARKS.md"},
	{slug: "roadmap", title: "Roadmap", nav: "Roadmap", group: "Internals", src: docsPage, docs: "ROADMAP.md"},
}

// navGroup is one sidebar section.
type navGroup struct {
	Label string
	Items []navItem
}

type navItem struct {
	Label  string
	Href   string
	Active bool
}

func navModel(active string) []navGroup {
	var groups []navGroup
	seen := map[string]int{}
	for _, p := range pages {
		if p.nav == "" {
			continue
		}
		i, ok := seen[p.group]
		if !ok {
			groups = append(groups, navGroup{Label: p.group})
			i = len(groups) - 1
			seen[p.group] = i
		}
		groups[i].Items = append(groups[i].Items, navItem{
			Label: p.nav, Href: p.slug + ".html", Active: p.slug == active,
		})
	}
	return groups
}
