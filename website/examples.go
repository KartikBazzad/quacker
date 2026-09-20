package main

import (
	"bytes"
	"fmt"
	"go/scanner"
	"go/token"
	"html/template"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// exampleOrder curates the gallery: simple first, then increasingly
// advanced programs.
var exampleOrder = []string{
	"simple", "dag", "cron", "events", "children", "durable",
	"introspect", "dagsvg", "eltgroups",
}

// renderExamples builds the examples gallery: each example's doc comment as
// the blurb, its source highlighted with go/scanner.
func renderExamples(b *builder, p pageDef) (template.HTML, error) {
	entries, err := filepath.Glob(filepath.Join(b.root, "examples", "*", "main.go"))
	if err != nil {
		return "", err
	}
	sort.Slice(entries, func(i, j int) bool {
		return exampleRank(entries[i]) < exampleRank(entries[j])
	})

	var buf bytes.Buffer
	buf.WriteString(`<h1>Examples</h1>
<p>Runnable programs in <code>examples/</code>. Run any of them with
<code>go run ./examples/&lt;name&gt;</code> from the repository root.</p>`)
	for _, path := range entries {
		name := filepath.Base(filepath.Dir(path))
		src, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		buf.WriteString(`<div class="example">`)
		fmt.Fprintf(&buf, `<h2><a class="api-anchor" id="ex-%s" href="#ex-%s">examples/%s</a></h2>`, name, name, name)
		if desc := exampleDoc(src); desc != "" {
			buf.WriteString(`<div class="exdesc">`)
			buf.WriteString(template.HTMLEscapeString(desc))
			buf.WriteString(`</div>`)
		}
		buf.WriteString(`<pre><code>`)
		buf.WriteString(string(highlightGo(src)))
		buf.WriteString(`</code></pre></div>`)
	}
	return template.HTML(buf.String()), nil
}

func exampleRank(path string) int {
	name := filepath.Base(filepath.Dir(path))
	for i, n := range exampleOrder {
		if n == name {
			return i
		}
	}
	return len(exampleOrder)
}

// exampleDoc extracts the leading comment block (package doc) as the
// example's one-paragraph description.
func exampleDoc(src []byte) string {
	var s scanner.Scanner
	fset := token.NewFileSet()
	f := fset.AddFile("", fset.Base(), len(src))
	s.Init(f, src, nil, scanner.ScanComments)
	for {
		_, tok, lit := s.Scan()
		switch tok {
		case token.COMMENT:
			txt := strings.TrimSpace(strings.TrimPrefix(lit, "//"))
			if txt != "" {
				// Stop at the end of the first doc block; single-line
				// comments joined into paragraphs.
				lines := []string{txt}
				for {
					_, t2, l2 := s.Scan()
					if t2 != token.COMMENT {
						return strings.Join(lines, " ")
					}
					l2 = strings.TrimSpace(strings.TrimPrefix(l2, "//"))
					if l2 == "" {
						return strings.Join(lines, " ")
					}
					lines = append(lines, l2)
				}
			}
		case token.PACKAGE, token.EOF:
			return ""
		}
	}
}

// highlightGo syntax-colors Go source using go/scanner: keywords, strings,
// comments, and numbers get token classes.
func highlightGo(src []byte) template.HTML {
	var s scanner.Scanner
	fset := token.NewFileSet()
	f := fset.AddFile("", fset.Base(), len(src))
	s.Init(f, src, nil, scanner.ScanComments)

	var buf bytes.Buffer
	prev := 0
	write := func(off int, class string, lit string) {
		buf.WriteString(template.HTMLEscapeString(string(src[prev:off])))
		if class != "" {
			buf.WriteString(`<span class="` + class + `">`)
			buf.WriteString(template.HTMLEscapeString(lit))
			buf.WriteString(`</span>`)
		} else {
			buf.WriteString(template.HTMLEscapeString(lit))
		}
		prev = off + len(lit)
	}
	for {
		pos, tok, lit := s.Scan()
		off := f.Offset(pos)
		if tok == token.EOF {
			break
		}
		switch {
		case tok == token.COMMENT:
			write(off, "tk-com", lit)
		case tok.IsKeyword():
			write(off, "tk-kw", lit)
		case tok == token.STRING || tok == token.CHAR:
			write(off, "tk-str", lit)
		case tok == token.INT || tok == token.FLOAT || tok == token.IMAG:
			write(off, "tk-num", lit)
		}
	}
	buf.WriteString(template.HTMLEscapeString(string(src[prev:])))
	return template.HTML(buf.String())
}
