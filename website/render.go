package main

import (
	"bytes"
	"html/template"
	"os"
	"path/filepath"
	"regexp"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/parser"
	"github.com/yuin/goldmark/renderer/html"
)

var md = goldmark.New(
	goldmark.WithExtensions(extension.GFM, extension.Typographer),
	goldmark.WithParserOptions(parser.WithAutoHeadingID()), // deep-linkable headings
	goldmark.WithRendererOptions(html.WithUnsafe()),        // docs are trusted content
)

// mdLink rewrites links to .md files into .html so cross-references between
// docs keep working once rendered.
var mdLink = regexp.MustCompile(`\]\(([^):#]+)\.md(#[^)]+)?\)`)

func renderMarkdown(src []byte) (template.HTML, error) {
	src = mdLink.ReplaceAll(src, []byte("]($1.html$2)"))
	var buf bytes.Buffer
	if err := md.Convert(src, &buf); err != nil {
		return "", err
	}
	return template.HTML(buf.String()), nil
}

// markdownPage renders a guide from content/<p.content>.
func markdownPage(b *builder, p pageDef) (template.HTML, error) {
	src, err := os.ReadFile(filepath.Join("content", p.content))
	if err != nil {
		return "", err
	}
	return renderMarkdown(src)
}

// docsPage renders an internals page from <root>/docs/<p.docs>.
func docsPage(b *builder, p pageDef) (template.HTML, error) {
	src, err := os.ReadFile(filepath.Join(b.root, "docs", p.docs))
	if err != nil {
		return "", err
	}
	return renderMarkdown(src)
}

// shell wraps page content in the site layout.
var shell = template.Must(template.New("shell").Parse(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{.Title}} — quacker docs</title>
<link rel="stylesheet" href="style.css">
</head>
<body>
<div class="layout">
<aside class="sidebar">
  <a class="brand" href="index.html">quacker<span class="duck">.</span></a>
  <div class="tagline">embeddable Go orchestration</div>
  <nav>
  {{range .Nav}}
    <div class="nav-group">{{.Label}}</div>
    {{range .Items}}
    <a class="nav-item{{if .Active}} active{{end}}" href="{{.Href}}">{{.Label}}</a>
    {{end}}
  {{end}}
  </nav>
  <div class="sidebar-foot">
    <a href="https://github.com/kartikbazzad/quacker">github.com/kartikbazzad/quacker</a>
  </div>
</aside>
<main class="content">
{{.Body}}
</main>
</div>
</body>
</html>`))

func renderShell(p pageDef, body template.HTML) (string, error) {
	var buf bytes.Buffer
	err := shell.Execute(&buf, struct {
		Title string
		Nav   []navGroup
		Body  template.HTML
	}{Title: p.title, Nav: navModel(p.slug), Body: body})
	return buf.String(), err
}
