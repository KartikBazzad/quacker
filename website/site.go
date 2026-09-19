package main

import (
	"bytes"
	"html/template"
)

// styleCSS is the site's single stylesheet: warm-paper background, serif
// display headings, one teal accent, dark code blocks, no shadows.
const styleCSS = `
:root {
  --bg: #fafaf9; --panel: #f5f5f4; --border: #e7e5e4;
  --ink: #1c1917; --muted: #78716c; --accent: #0f766e; --accent-ink: #115e59;
  --accent-soft: #d3e9e6;
  --code-bg: #1c1917; --code-ink: #e7e5e4;
  --mono: ui-monospace, "SF Mono", "JetBrains Mono", Menlo, Consolas, monospace;
  --sans: -apple-system, "SF Pro Display", "Segoe UI", "Helvetica Neue", Arial, sans-serif;
  --serif: "New York", ui-serif, Georgia, "Times New Roman", serif;
}
* { box-sizing: border-box; }
html { scroll-behavior: smooth; }
body { margin: 0; background: var(--bg); color: var(--ink); font-family: var(--sans);
       font-size: 15px; line-height: 1.65; }
.layout { display: flex; min-height: 100vh; }
a:focus-visible, button:focus-visible {
  outline: 2px solid var(--accent); outline-offset: 2px; border-radius: 2px;
}

.sidebar { width: 248px; flex-shrink: 0; position: sticky; top: 0; height: 100vh;
           overflow-y: auto; background: var(--panel); border-right: 1px solid var(--border);
           padding: 24px 16px; display: flex; flex-direction: column; }
.brand { font-family: var(--serif); font-weight: 700; font-size: 24px; color: var(--ink);
         text-decoration: none; letter-spacing: -0.03em; }
.brand .duck { color: var(--accent); }
.tagline { color: var(--muted); font-size: 12px; margin: 4px 0 20px; }
.nav-group { font-size: 11px; font-weight: 700; text-transform: uppercase;
             letter-spacing: 0.08em; color: var(--muted); margin: 20px 0 4px; }
.nav-item { display: block; padding: 4px 8px; margin: 1px -8px; border-radius: 6px;
            color: var(--ink); text-decoration: none; font-size: 14px; }
.nav-item:hover { background: #ececea; }
.nav-item.active { background: var(--accent-soft); color: var(--accent-ink); font-weight: 600; }
.sidebar-foot { margin-top: auto; padding-top: 20px; font-size: 12px; }
.sidebar-foot a { color: var(--muted); text-decoration: none; word-break: break-all; }
.sidebar-foot a:hover { color: var(--accent); }

.content { flex: 1; max-width: 860px; padding: 40px 56px 96px; }
h1 { font-family: var(--serif); font-size: 32px; font-weight: 600;
     letter-spacing: -0.03em; line-height: 1.15; margin: 0 0 16px; }
h2 { font-size: 20px; letter-spacing: -0.01em; margin: 36px 0 12px;
     padding-bottom: 8px; border-bottom: 1px solid var(--border); }
h3 { font-size: 16px; margin: 28px 0 8px; }
h4 { font-size: 14px; margin: 20px 0 4px; }
p { margin: 12px 0; }
a { color: var(--accent); }
a:hover { color: var(--accent-ink); }
ul, ol { padding-left: 24px; margin: 12px 0; }
li { margin: 4px 0; }
hr { border: none; border-top: 1px solid var(--border); margin: 32px 0; }
blockquote { margin: 12px 0; padding: 4px 16px; border-left: 3px solid var(--border);
             color: var(--muted); }
code { font-family: var(--mono); font-size: 0.87em; background: var(--panel);
       border: 1px solid var(--border); border-radius: 4px; padding: 1px 5px; }
pre { background: var(--code-bg); color: var(--code-ink); border-radius: 8px;
      padding: 16px; overflow-x: auto; line-height: 1.55; }
pre code { background: none; border: none; padding: 0; font-size: 13px; }
table { border-collapse: collapse; width: 100%; margin: 16px 0; font-size: 14px; }
th, td { border: 1px solid var(--border); padding: 8px 12px; text-align: left; }
td { font-variant-numeric: tabular-nums; }
th { background: var(--panel); font-weight: 600; }
img { max-width: 100%; }

/* home */
.hero { padding: 32px 0 8px; }
.hero h1 { font-size: 52px; margin-bottom: 12px; }
.hero h1 .duck { color: var(--accent); }
.hero .lede { font-size: 17px; color: var(--muted); max-width: 620px; margin: 0 0 24px; }
.install { font-family: var(--mono); font-size: 13px; background: var(--code-bg);
           color: var(--code-ink); display: inline-block; padding: 8px 16px;
           border-radius: 8px; }
.features { display: grid; grid-template-columns: repeat(auto-fill, minmax(240px, 1fr));
            gap: 12px; margin: 28px 0 8px; }
.feature { border: 1px solid var(--border); border-radius: 8px; padding: 16px;
           background: #fff; }
.feature b { display: block; font-size: 14px; margin-bottom: 4px; }
.feature span { font-size: 13px; color: var(--muted); line-height: 1.5; }

/* api reference */
.api-kind { font-size: 12px; font-weight: 700; text-transform: uppercase;
            letter-spacing: 0.07em; color: var(--muted); margin: 32px 0 4px; }
.api-decl { margin: 0 0 24px; }
.api-sig { font-family: var(--mono); font-size: 12.5px; background: var(--panel);
           border: 1px solid var(--border); border-radius: 8px; padding: 12px 16px;
           overflow-x: auto; white-space: pre; margin: 4px 0 8px; }
.api-doc { margin-left: 2px; }
.api-doc p:first-child { margin-top: 4px; }
.api-anchor { color: inherit; text-decoration: none; }
.api-anchor:hover::before { content: "#"; color: var(--accent); margin-right: 4px; }

/* examples */
.example { margin-bottom: 40px; }
.example h2 { border-bottom: none; padding-bottom: 0; margin-bottom: 4px;
              font-family: var(--mono); font-size: 17px; }
.example .exdesc { color: var(--muted); margin-bottom: 8px; max-width: 640px; }

/* syntax tokens (examples) */
.tk-kw { color: #7dd3fc; }
.tk-str { color: #86efac; }
.tk-com { color: #a8a29e; font-style: italic; }
.tk-num { color: #fbbf24; }

@media (max-width: 840px) {
  .layout { flex-direction: column; }
  .sidebar { width: 100%; height: auto; position: static; border-right: none;
             border-bottom: 1px solid var(--border); }
  .content { padding: 24px 20px 64px; }
}
`

// renderHome builds the landing page: hero, install, quickstart, features.
func renderHome(b *builder, p pageDef) (template.HTML, error) {
	var buf bytes.Buffer
	buf.WriteString(`<div class="hero">
<h1>quacker<span class="duck">.</span></h1>
<p class="lede">An embeddable task &amp; workflow orchestration engine for Go.
Tasks are plain functions; state is durable in SQLite. No brokers, no extra
infrastructure — open it inside your process and it just runs.</p>
<div class="install">go get github.com/kartikbazzad/quacker</div>
</div>

<h2>60-second tour</h2>
<pre><code>q, _ := quacker.Open(quacker.WithStorage(quacker.File("state.db")))
defer q.Close(ctx)

greet := quacker.NewTask("greet", func(ctx context.Context, in In) (Out, error) {
    return Out{Greeting: "hi " + in.Name}, nil
}, quacker.Retries(3), quacker.Timeout(30*time.Second))

h, _ := quacker.Enqueue(ctx, q, greet, In{Name: "ada"})
out, _ := h.Result(ctx)   // waits for the run, decodes Out</code></pre>

<div class="features">
<div class="feature"><b>Durable execution</b><span>SleepDurable, WaitFor events, RunOnce — suspend without a worker slot, resume across restarts.</span></div>
<div class="feature"><b>Plain Go tasks</b><span>Generic typed inputs/outputs; retries, timeouts, priorities, queues.</span></div>
<div class="feature"><b>DAG workflows</b><span>Step dependencies, upstream outputs, per-step tasks, live DAG introspection.</span></div>
<div class="feature"><b>Triggers</b><span>Cron (incl. sub-second @every), in-process events, child runs — persisted and re-armed.</span></div>
<div class="feature"><b>Concurrency control</b><span>Per-key limits and queue rate windows, enforced in the claim transaction.</span></div>
<div class="feature"><b>Operations</b><span>Middleware, log sinks, retention/purge, metrics, OTel spans, debug stream.</span></div>
<div class="feature"><b>SQLite inside</b><span>Memory, ephemeral, or file storage with migrations and restart recovery.</span></div>
<div class="feature"><b>No infrastructure</b><span>One process, one writer connection, no broker to operate.</span></div>
</div>

<h2>Where next</h2>
<ul>
<li><a href="getting-started.html">Getting started</a> — install, open options, first run</li>
<li><a href="durable.html">Durable execution</a> — sleeps, event waits, exactly-once side effects</li>
<li><a href="api.html">API reference</a> — every exported symbol</li>
<li><a href="examples.html">Examples</a> — runnable programs in the repo</li>
</ul>`)
	return template.HTML(buf.String()), nil
}
