# quacker docs website

A small Go-native static generator that renders the documentation site into
`public/` — no external toolchain beyond Go itself.

```sh
cd website && go run .
# then serve or open public/index.html
```

Sources:

| Page | Source |
|---|---|
| Home | `site.go` (`renderHome`) |
| Guides | `content/*.md` (authored for the site) |
| Internals | `../docs/*.md` — rendered directly, no duplication |
| API reference | `go/doc` over the root package — always in sync |
| Examples | `../examples/*/main.go`, highlighted via `go/scanner` |

This is its own module so goldmark stays out of the library's `go.mod`.
`public/` is gitignored — regenerate instead of committing output.
