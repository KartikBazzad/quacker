package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/doc"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// wikiName maps page slugs to GitHub-wiki page names (a page's filename is
// its URL slug).
var wikiName = map[string]string{
	"index":           "Home",
	"features":        "Features",
	"use-cases":       "Use-Cases",
	"getting-started": "Getting-Started",
	"tasks":           "Tasks",
	"workflows":       "Workflows",
	"durable":         "Durable-Execution",
	"concurrency":     "Concurrency",
	"triggers":        "Triggers",
	"operations":      "Operations",
	"plugins":         "Plugins",
	"storage-drivers": "Storage-Drivers",
	"api":             "API-Reference",
	"examples":        "Examples",
	"architecture":    "Architecture",
	"design-notes":    "Design-Notes",
	"benchmarks":      "Benchmarks",
	"roadmap":         "Roadmap",
	"stability":       "Stability",
}

const repoURL = "https://github.com/KartikBazzad/quacker"

// buildWiki emits the GitHub wiki (markdown pages + _Sidebar) into b.out.
func (b *builder) buildWiki() error {
	if err := os.RemoveAll(b.out); err != nil {
		return err
	}
	if err := os.MkdirAll(b.out, 0o755); err != nil {
		return err
	}
	write := func(name string, body []byte) error {
		return os.WriteFile(filepath.Join(b.out, name), body, 0o644)
	}
	for _, p := range pages {
		name := wikiName[p.slug]
		if name == "" { // a content/docs page with no explicit wiki name
			name = strings.ReplaceAll(p.title, " ", "-")
		}
		switch {
		case p.content != "":
			src, err := os.ReadFile(filepath.Join("content", p.content))
			if err != nil {
				return err
			}
			if err := write(name+".md", wikiTransform(src)); err != nil {
				return err
			}
		case p.docs != "":
			src, err := os.ReadFile(filepath.Join(b.root, "docs", p.docs))
			if err != nil {
				return err
			}
			if err := write(name+".md", wikiTransform(src)); err != nil {
				return err
			}
		case p.slug == "api":
			out, err := wikiAPI(b)
			if err != nil {
				return err
			}
			if err := write(name+".md", out); err != nil {
				return err
			}
		case p.slug == "examples":
			out, err := wikiExamples(b)
			if err != nil {
				return err
			}
			if err := write(name+".md", out); err != nil {
				return err
			}
		}
	}
	for name, body := range map[string][]byte{
		"Home.md":     wikiHome(),
		"_Sidebar.md": wikiSidebar(),
		"_Footer.md":  wikiFooter(),
	} {
		if err := write(name, body); err != nil {
			return err
		}
	}
	fmt.Printf("docsite: wiki -> %s\n", b.out)
	return nil
}

// htmlLink rewrites [x](slug.html) links to wiki page names.
var htmlLink = regexp.MustCompile(`\]\(([a-zA-Z0-9-]+)\.html(#[^)]+)?\)`)

// mdFileLink rewrites [x](FILE.md) links: known docs go to their wiki page,
// everything else to the file on GitHub.
var mdFileLink = regexp.MustCompile(`\]\(([^)]+?)\.md(#[^)]+)?\)`)

var mdToWiki = map[string]string{
	"ARCHITECTURE.md": "Architecture",
	"DESIGN_NOTES.md": "Design-Notes",
	"BENCHMARKS.md":   "Benchmarks",
	"ROADMAP.md":      "Roadmap",
	"STABILITY.md":    "Stability",
	"DRIVERS.md":      "Storage-Drivers",
}

// wikiTransform adapts repo markdown for the wiki: internal links to wiki
// page names, file links to GitHub blob URLs.
func wikiTransform(src []byte) []byte {
	src = htmlLink.ReplaceAllFunc(src, func(m []byte) []byte {
		sub := htmlLink.FindSubmatch(m)
		if w, ok := wikiName[string(sub[1])]; ok {
			return []byte("](" + w + string(sub[2]) + ")")
		}
		return m
	})
	src = mdFileLink.ReplaceAllFunc(src, func(m []byte) []byte {
		sub := mdFileLink.FindSubmatch(m)
		target := strings.TrimPrefix(string(sub[1]), "../")
		if w, ok := mdToWiki[filepath.Base(target)]; ok {
			return []byte("](" + w + string(sub[2]) + ")")
		}
		return []byte("](" + repoURL + "/blob/main/" + target + string(sub[2]) + ")")
	})
	return src
}

// wikiHome generates the wiki landing page.
func wikiHome() []byte {
	return []byte(`# quacker

An embeddable task & workflow orchestration engine for Go. Tasks are plain
functions; state is durable in SQLite. No brokers, no extra infrastructure —
open it inside your process and it just runs.

` + "```sh\ngo get github.com/kartikbazzad/quacker\n```" + `

` + "```go" + `
q, _ := quacker.Open(quacker.WithStorage(quacker.File("state.db")))
defer q.Close(ctx)

greet := quacker.NewTask("greet", func(ctx context.Context, in In) (Out, error) {
    return Out{Greeting: "hi " + in.Name}, nil
}, quacker.Retries(3), quacker.Timeout(30*time.Second))

h, _ := quacker.Enqueue(ctx, q, greet, In{Name: "ada"})
out, _ := h.Result(ctx)   // waits for the run, decodes Out
` + "```" + `

## Documentation

| | |
|---|---|
| [[Features]] | The full capability surface |
| [[Use-Cases]] | Background jobs, async APIs, pipelines, multi-instance |
| [[Getting-Started]] | Install, storage modes, first run, shutdown |
| [[Tasks]] | Retries, timeouts, queues, priorities, keys, labels |
| [[Workflows]] | DAG steps, dependency outputs, live DAG introspection |
| [[Durable-Execution]] | SleepDurable, WaitFor, RunOnce, the determinism contract |
| [[Concurrency]] | Queue limits, per-key gates, rate windows, worker labels |
| [[Triggers]] | Cron, events, child runs |
| [[Operations]] | Middleware, logs, retention, metrics, OTel, introspection |
| [[Plugins]] | Lifecycle hooks and the payload codec |
| [[Storage-Drivers]] | SQLite, Postgres, MySQL, and the driver contract |
| [[API-Reference]] | Every exported symbol (generated from source) |
| [[Examples]] | Runnable programs in the repo |

Internals: [[Architecture]] · [[Design-Notes]] · [[Benchmarks]] · [[Roadmap]] · [[Stability]]

Also available as a rendered site — see ` + "`website/`" + ` in the repo.
`)
}

// wikiSidebar generates the _Sidebar.md navigation.
func wikiSidebar() []byte {
	var buf bytes.Buffer
	group := ""
	for _, p := range pages {
		if p.nav == "" {
			continue
		}
		if p.group != group {
			group = p.group
			buf.WriteString("\n**" + group + "**\n\n")
		}
		buf.WriteString("- [[" + wikiName[p.slug] + "|" + p.nav + "]]\n")
	}
	return buf.Bytes()
}

func wikiFooter() []byte {
	return []byte("Generated from the quacker repository — regenerate with `cd website && go run . -mode wiki`. " +
		"Do not edit the wiki repo directly; changes are overwritten.\n")
}

// wikiAPI renders the API reference as wiki markdown: signatures in go
// fences, doc comments as plain text.
func wikiAPI(b *builder) ([]byte, error) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, b.root, func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	pkg, ok := pkgs["quacker"]
	if !ok {
		return nil, fmt.Errorf("package quacker not found under %s", b.root)
	}
	d := doc.New(pkg, "github.com/kartikbazzad/quacker", 0)

	var buf bytes.Buffer
	buf.WriteString("# API reference\n\nThe public surface of `package quacker`, generated from the source's\ndoc comments.\n\n")
	if d.Doc != "" {
		buf.WriteString(d.Doc + "\n\n")
	}
	writeWikiTypes(&buf, fset, d.Types)
	sort.Slice(d.Funcs, func(i, j int) bool { return d.Funcs[i].Name < d.Funcs[j].Name })
	if len(d.Funcs) > 0 {
		buf.WriteString("## Functions\n\n")
		for _, f := range d.Funcs {
			writeWikiDecl(&buf, fset, "### `"+f.Name+"`", f.Decl, f.Doc)
		}
	}
	writeWikiValues(&buf, fset, "Constants", d.Consts)
	writeWikiValues(&buf, fset, "Variables", d.Vars)
	return buf.Bytes(), nil
}

func writeWikiTypes(buf *bytes.Buffer, fset *token.FileSet, types []*doc.Type) {
	if len(types) == 0 {
		return
	}
	sort.Slice(types, func(i, j int) bool { return types[i].Name < types[j].Name })
	buf.WriteString("## Types\n\n")
	for _, t := range types {
		writeWikiDecl(buf, fset, "### `"+t.Name+"`", t.Decl, t.Doc)
		for _, v := range append(append([]*doc.Value{}, t.Consts...), t.Vars...) {
			writeWikiDecl(buf, fset, "#### `"+strings.Join(v.Names, ", ")+"`", v.Decl, v.Doc)
		}
		for _, f := range t.Funcs {
			writeWikiDecl(buf, fset, "#### `"+f.Name+"`", f.Decl, f.Doc)
		}
		for _, f := range t.Methods {
			writeWikiDecl(buf, fset, "#### `"+t.Name+"."+f.Name+"`", f.Decl, f.Doc)
		}
	}
}

func writeWikiValues(buf *bytes.Buffer, fset *token.FileSet, label string, vals []*doc.Value) {
	if len(vals) == 0 {
		return
	}
	buf.WriteString("## " + label + "\n\n")
	for _, v := range vals {
		writeWikiDecl(buf, fset, "### `"+strings.Join(v.Names, ", ")+"`", v.Decl, v.Doc)
	}
}

func writeWikiDecl(buf *bytes.Buffer, fset *token.FileSet, heading string, decl ast.Decl, doc string) {
	buf.WriteString(heading + "\n\n```go\n" + signature(fset, decl) + "\n```\n")
	if doc != "" {
		buf.WriteString("\n" + doc + "\n")
	}
	buf.WriteString("\n")
}

// wikiExamples renders the examples gallery: heading + doc-comment blurb +
// repo link + source inside a <details> block.
func wikiExamples(b *builder) ([]byte, error) {
	entries, err := filepath.Glob(filepath.Join(b.root, "examples", "*", "main.go"))
	if err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool {
		return exampleRank(entries[i]) < exampleRank(entries[j])
	})
	var buf bytes.Buffer
	buf.WriteString("# Examples\n\nRunnable programs in `examples/`. Run any of them with `go run ./examples/<name>`\nfrom the repository root.\n")
	for _, path := range entries {
		name := filepath.Base(filepath.Dir(path))
		src, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		buf.WriteString("\n## `examples/" + name + "`\n\n")
		if desc := exampleDoc(src); desc != "" {
			buf.WriteString(desc + "\n\n")
		}
		buf.WriteString("[Source](" + repoURL + "/blob/main/examples/" + name + "/main.go) · ")
		buf.WriteString("`go run ./examples/" + name + "`\n\n")
		buf.WriteString("<details><summary>main.go</summary>\n\n```go\n")
		buf.Write(src)
		buf.WriteString("```\n</details>\n")
	}
	return buf.Bytes(), nil
}
