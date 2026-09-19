// Command docsite builds the quacker documentation website into public/.
//
// Usage:
//
//	cd website && go run .
//
// Guides come from content/*.md, the internals pages render the repo's
// docs/*.md directly (single source of truth), the API reference is
// generated from go/doc, and the examples gallery reads ../examples.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// builder carries the build context: repo root and output dir.
type builder struct {
	out  string
	root string
}

func main() {
	out := flag.String("out", "public", "output directory")
	root := flag.String("root", "..", "quacker repository root")
	flag.Parse()
	b := &builder{out: *out, root: *root}
	if err := b.build(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("docsite: %d pages -> %s\n", len(pages), filepath.Join(*out, "index.html"))
}

func (b *builder) build() error {
	if err := os.RemoveAll(b.out); err != nil {
		return err
	}
	if err := os.MkdirAll(b.out, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(b.out, "style.css"), []byte(styleCSS), 0o644); err != nil {
		return err
	}
	for _, p := range pages {
		body, err := p.src(b, p)
		if err != nil {
			return fmt.Errorf("%s: %w", p.slug, err)
		}
		html, err := renderShell(p, body)
		if err != nil {
			return fmt.Errorf("%s: %w", p.slug, err)
		}
		if err := os.WriteFile(filepath.Join(b.out, p.slug+".html"), []byte(html), 0o644); err != nil {
			return err
		}
	}
	return nil
}
