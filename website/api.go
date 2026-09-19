package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/doc"
	"go/doc/comment"
	"go/parser"
	"go/printer"
	"go/token"
	"html/template"
	"io/fs"
	"sort"
	"strings"
)

// renderAPI generates the API reference from the root package's exported
// declarations using go/doc — no external tooling, always in sync with code.
func renderAPI(b *builder, p pageDef) (template.HTML, error) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, b.root, func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, parser.ParseComments)
	if err != nil {
		return "", err
	}
	pkg, ok := pkgs["quacker"]
	if !ok {
		return "", fmt.Errorf("package quacker not found under %s", b.root)
	}
	d := doc.New(pkg, "github.com/kartikbazzad/quacker", 0)

	var buf bytes.Buffer
	buf.WriteString(`<h1>API reference</h1>
<p>The public surface of <code>package quacker</code>, generated from the source's
doc comments. Also on <a href="https://pkg.go.dev/github.com/kartikbazzad/quacker">pkg.go.dev</a>.</p>`)
	if d.Doc != "" {
		buf.WriteString(`<div class="api-doc">`)
		buf.WriteString(docHTML(d.Doc))
		buf.WriteString(`</div>`)
	}

	writeDecls(&buf, fset, "Types", d.Types, func(t *doc.Type) []declItem {
		var items []declItem
		items = append(items, declItem{kind: "type", name: t.Name, anchor: t.Name, decl: t.Decl, doc: t.Doc})
		// Typed constants/vars nest under their type (godoc convention).
		for _, v := range append(append([]*doc.Value{}, t.Consts...), t.Vars...) {
			items = append(items, declItem{kind: "values", name: strings.Join(v.Names, ", "),
				anchor: t.Name + "." + v.Names[0], decl: v.Decl, doc: v.Doc})
		}
		for _, f := range t.Funcs {
			items = append(items, declItem{kind: "func", name: f.Name, anchor: t.Name + "." + f.Name, decl: f.Decl, doc: f.Doc})
		}
		for _, f := range t.Methods {
			items = append(items, declItem{kind: "method", name: f.Name, anchor: t.Name + "." + f.Name, decl: f.Decl, doc: f.Doc})
		}
		return items
	})

	// Package-level functions, sorted by name.
	sort.Slice(d.Funcs, func(i, j int) bool { return d.Funcs[i].Name < d.Funcs[j].Name })
	if len(d.Funcs) > 0 {
		buf.WriteString(`<div class="api-kind">Functions</div>`)
		for _, f := range d.Funcs {
			writeItem(&buf, fset, declItem{kind: "func", name: f.Name, anchor: f.Name, decl: f.Decl, doc: f.Doc})
		}
	}
	writeValueGroups(&buf, fset, "Constants", d.Consts)
	writeValueGroups(&buf, fset, "Variables", d.Vars)
	return template.HTML(buf.String()), nil
}

// declItem is one rendered declaration (type, func, method, or values).
type declItem struct {
	kind   string
	name   string
	anchor string
	decl   ast.Decl
	doc    string
}

// writeDecls renders a group of *doc.Type values with their members.
func writeDecls(buf *bytes.Buffer, fset *token.FileSet, label string, types []*doc.Type, expand func(*doc.Type) []declItem) {
	if len(types) == 0 {
		return
	}
	sort.Slice(types, func(i, j int) bool { return types[i].Name < types[j].Name })
	buf.WriteString(`<div class="api-kind">` + label + `</div>`)
	for _, t := range types {
		for _, it := range expand(t) {
			writeItem(buf, fset, it)
		}
	}
}

func writeItem(buf *bytes.Buffer, fset *token.FileSet, it declItem) {
	buf.WriteString(`<div class="api-decl">`)
	fmt.Fprintf(buf, `<h3><a class="api-anchor" id="%s" href="#%s">%s</a></h3>`, it.anchor, it.anchor, it.name)
	buf.WriteString(`<div class="api-sig">`)
	buf.WriteString(template.HTMLEscapeString(signature(fset, it.decl)))
	buf.WriteString(`</div>`)
	if it.doc != "" {
		buf.WriteString(`<div class="api-doc">`)
		buf.WriteString(docHTML(it.doc))
		buf.WriteString(`</div>`)
	}
	buf.WriteString(`</div>`)
}

// signature renders a declaration without its body (func signature or full
// type/var/const spec).
func signature(fset *token.FileSet, decl ast.Decl) string {
	switch d := decl.(type) {
	case *ast.FuncDecl:
		cp := *d
		cp.Body = nil
		var sb strings.Builder
		_ = printer.Fprint(&sb, fset, &cp)
		return sb.String()
	default:
		var sb strings.Builder
		_ = printer.Fprint(&sb, fset, decl)
		return sb.String()
	}
}

// docHTML renders a doc comment to HTML using the stdlib comment printer.
func docHTML(text string) string {
	var p comment.Parser
	pd := p.Parse(text)
	var pr comment.Printer
	return string(pr.HTML(pd))
}

// writeValueGroups renders const/var groups (deduped by spec text).
func writeValueGroups(buf *bytes.Buffer, fset *token.FileSet, label string, vals []*doc.Value) {
	if len(vals) == 0 {
		return
	}
	buf.WriteString(`<div class="api-kind">` + label + `</div>`)
	for _, v := range vals {
		anchor := v.Names[0]
		fmt.Fprintf(buf, `<div class="api-decl"><h3><a class="api-anchor" id="%s" href="#%s">%s</a></h3><div class="api-sig">`,
			anchor, anchor, strings.Join(v.Names, ", "))
		buf.WriteString(template.HTMLEscapeString(signature(fset, v.Decl)))
		buf.WriteString(`</div>`)
		if v.Doc != "" {
			buf.WriteString(`<div class="api-doc">`)
			buf.WriteString(docHTML(v.Doc))
			buf.WriteString(`</div>`)
		}
		buf.WriteString(`</div>`)
	}
}
