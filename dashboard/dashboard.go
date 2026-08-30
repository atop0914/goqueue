// Package dashboard provides an embedded HTTP operations console for a
// goqueue Client: a lightweight HTML overview page, JSON API endpoints for
// status/stats/dead-letter inspection, admin operations (pause, purge,
// requeue dead-letter jobs) and liveness/readiness probes. It has zero
// external dependencies (stdlib only) and is safe to mount under any path
// of an existing HTTP server.
//
//	cli := goqueue.New()
//	dash := dashboard.New(cli, dashboard.WithTitle("My Worker"))
//	http.Handle("/", dash)
//	http.ListenAndServe(":8080", nil)
//
// Endpoints:
//
//	/            HTML overview (auto-refreshing, with admin buttons)
//	/api/status  JSON: queue depth gauges (pending/running/dead, workers)
//	/api/stats   JSON: cumulative lifecycle counters + per-type breakdown
//	/api/jobs    JSON: recent dead-letter jobs
//	POST /api/admin/pause        pause job delivery
//	POST /api/admin/resume       lift a pause
//	POST /api/admin/purge        drop pending jobs; JSON body {"dead":true}
//	                             also drops the DLQ
//	POST /api/admin/requeue-dead requeue the DLQ; JSON body {"id":"..."}
//	                             requeues a single job
//	/healthz       liveness probe (always 200 while the process is up)
//	/healthz/ready readiness probe (200, or 503 when a custom check fails)
//
// Admin endpoints answer POST only; a GET returns 405. Unsupported
// operations (backend without the matching capability) answer 501 with
// {"ok":false,"error":...}.
package dashboard

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"time"

	"github.com/atop0914/goqueue"
)

// Defaults for optional parameters.
const (
	// defaultMaxDeadJobs caps how many dead-letter jobs /api/jobs and the
	// overview page return (newest first).
	defaultMaxDeadJobs = 50
	// defaultRefresh is the JavaScript auto-refresh interval of the HTML
	// overview page.
	defaultRefresh = 2 * time.Second
)

// Dashboard renders live queue state for a goqueue Client over HTTP.
// It is safe for concurrent use: it keeps no mutable per-request state
// and reads everything through Client.Stats and Queue.Dead.
type Dashboard struct {
	cli       *goqueue.Client
	title     string
	maxDead   int
	refresh   time.Duration
	ready     func() error
	startedAt time.Time
	mux       *http.ServeMux
}

// Option configures a Dashboard.
type Option func(*Dashboard)

// WithTitle sets the page title shown in the HTML overview. Defaults to
// "GoQueue".
func WithTitle(title string) Option {
	return func(d *Dashboard) { d.title = title }
}

// WithMaxDeadJobs caps the number of dead-letter jobs returned by
// /api/jobs and shown in the overview (newest first). Defaults to 50.
func WithMaxDeadJobs(n int) Option {
	return func(d *Dashboard) {
		if n > 0 {
			d.maxDead = n
		}
	}
}

// WithRefreshInterval sets how often the HTML overview auto-refreshes its
// numbers. Defaults to 2s.
func WithRefreshInterval(d time.Duration) Option {
	return func(dd *Dashboard) {
		if d > 0 {
			dd.refresh = d
		}
	}
}

// WithReadyCheck installs a custom readiness probe called on every
// /healthz/ready request. A non-nil error makes the probe answer 503 with
// the error message. Without a check the probe always answers 200 ("ready").
func WithReadyCheck(fn func() error) Option {
	return func(d *Dashboard) { d.ready = fn }
}

// New creates a Dashboard bound to cli. It registers all routes on an
// internal ServeMux; use Handler to mount it under a path prefix.
func New(cli *goqueue.Client, opts ...Option) *Dashboard {
	d := &Dashboard{
		cli:       cli,
		title:     "GoQueue",
		maxDead:   defaultMaxDeadJobs,
		refresh:   defaultRefresh,
		startedAt: time.Now(),
	}
	for _, opt := range opts {
		opt(d)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", d.handleOverview)
	mux.HandleFunc("/api/status", d.handleStatus)
	mux.HandleFunc("/api/stats", d.handleStats)
	mux.HandleFunc("/api/jobs", d.handleJobs)
	mux.HandleFunc("POST /api/admin/pause", d.handlePause)
	mux.HandleFunc("POST /api/admin/resume", d.handleResume)
	mux.HandleFunc("POST /api/admin/purge", d.handlePurge)
	mux.HandleFunc("POST /api/admin/requeue-dead", d.handleRequeueDead)
	mux.HandleFunc("/healthz", d.handleLiveness)
	mux.HandleFunc("/healthz/ready", d.handleReadiness)
	d.mux = mux
	return d
}

// Handler returns the http.Handler serving the dashboard routes. Mount it
// anywhere, e.g. http.Handle("/queue/", http.StripPrefix("/queue/", dash.Handler())).
func (d *Dashboard) Handler() http.Handler { return d.mux }

// ServeHTTP implements http.Handler, so a Dashboard can be registered
// directly as the root handler.
func (d *Dashboard) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	d.mux.ServeHTTP(w, r)
}

// ---- JSON API ----

// statusResponse is the /api/status payload: instantaneous gauges.
type statusResponse struct {
	Pending       int64 `json:"pending"`
	Running       int64 `json:"running"`
	Dead          int64 `json:"dead"`
	Workers       int   `json:"workers"`
	Started       bool  `json:"started"`
	UptimeSeconds int64 `json:"uptime_seconds"`
}

// statsResponse is the /api/stats payload: cumulative lifecycle counters.
type statsResponse struct {
	Enqueued  int64            `json:"enqueued"`
	Succeeded int64            `json:"succeeded"`
	Failed    int64            `json:"failed"`
	DeadTotal int64            `json:"dead_total"`
	ByType    map[string]int64 `json:"by_type"`
}

// deadJobDTO is the JSON shape of one dead-letter job.
type deadJobDTO struct {
	ID         string    `json:"id"`
	Type       string    `json:"type"`
	State      string    `json:"state"`
	Attempts   int       `json:"attempts"`
	MaxRetry   int       `json:"max_retry"`
	Priority   int       `json:"priority"`
	LastError  string    `json:"last_error,omitempty"`
	EnqueuedAt time.Time `json:"enqueued_at"`
	DeadAt     time.Time `json:"dead_at"`
}

func (d *Dashboard) handleStatus(w http.ResponseWriter, r *http.Request) {
	s := d.cli.Stats()
	writeJSON(w, statusResponse{
		Pending:       s.Pending,
		Running:       s.Running,
		Dead:          s.Dead,
		Workers:       s.Workers,
		Started:       s.Started,
		UptimeSeconds: int64(time.Since(d.startedAt).Seconds()),
	})
}

func (d *Dashboard) handleStats(w http.ResponseWriter, r *http.Request) {
	s := d.cli.Stats()
	writeJSON(w, statsResponse{
		Enqueued:  s.Enqueued,
		Succeeded: s.Succeeded,
		Failed:    s.Failed,
		DeadTotal: s.DeadTotal,
		ByType:    s.ByType,
	})
}

func (d *Dashboard) handleJobs(w http.ResponseWriter, r *http.Request) {
	jobs := d.deadJobs()
	writeJSON(w, map[string]any{"dead": jobs})
}

// deadJobs returns the newest maxDead dead-letter jobs.
func (d *Dashboard) deadJobs() []deadJobDTO {
	all := d.cli.Queue().Dead() // sorted by death time ascending
	// Newest first: iterate backwards.
	from := len(all) - 1
	to := from - d.maxDead + 1
	if to < 0 {
		to = 0
	}
	out := make([]deadJobDTO, 0, from-to+1)
	for i := from; i >= to; i-- {
		ji := all[i]
		out = append(out, deadJobDTO{
			ID:         ji.ID,
			Type:       ji.Type,
			State:      ji.State.String(),
			Attempts:   ji.Attempts,
			MaxRetry:   ji.MaxRetry,
			Priority:   ji.Priority,
			LastError:  ji.LastError,
			EnqueuedAt: ji.EnqueuedAt,
			DeadAt:     ji.DeadAt,
		})
	}
	return out
}

// ---- health probes ----

func (d *Dashboard) handleLiveness(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintln(w, "ok")
}

func (d *Dashboard) handleReadiness(w http.ResponseWriter, r *http.Request) {
	if d.ready != nil {
		if err := d.ready(); err != nil {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprintf(w, "not ready: %v\n", err)
			return
		}
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintln(w, "ready")
}

// ---- HTML overview ----

// overviewData is the server-side initial render payload.
type overviewData struct {
	Title         string
	RefreshMillis int64
	Status        statusResponse
	Stats         statsResponse
	DeadJobs      []deadJobDTO
	ByType        []typeStat // sorted for deterministic rendering
	HasDead       bool
}

// typeStat is one row of the per-type throughput table.
type typeStat struct {
	Type  string
	Count int64
	Pct   float64
}

func (d *Dashboard) handleOverview(w http.ResponseWriter, r *http.Request) {
	// The mux routes everything unknown to "/"; give a 404 for paths that
	// are not the root dashboard page.
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	s := d.cli.Stats()
	data := overviewData{
		Title:         d.title,
		RefreshMillis: d.refresh.Milliseconds(),
		Status: statusResponse{
			Pending:       s.Pending,
			Running:       s.Running,
			Dead:          s.Dead,
			Workers:       s.Workers,
			Started:       s.Started,
			UptimeSeconds: int64(time.Since(d.startedAt).Seconds()),
		},
		Stats: statsResponse{
			Enqueued:  s.Enqueued,
			Succeeded: s.Succeeded,
			Failed:    s.Failed,
			DeadTotal: s.DeadTotal,
			ByType:    s.ByType,
		},
		DeadJobs: d.deadJobs(),
	}
	data.HasDead = len(data.DeadJobs) > 0
	for typ, n := range s.ByType {
		data.ByType = append(data.ByType, typeStat{Type: typ, Count: n})
	}
	sort.Slice(data.ByType, func(i, j int) bool {
		if data.ByType[i].Count != data.ByType[j].Count {
			return data.ByType[i].Count > data.ByType[j].Count
		}
		return data.ByType[i].Type < data.ByType[j].Type
	})
	if s.Enqueued > 0 {
		for i := range data.ByType {
			data.ByType[i].Pct = float64(data.ByType[i].Count) / float64(s.Enqueued) * 100
		}
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := overviewTemplate.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		// The body is already part-written; nothing sensible to do but log
		// via the error path. Encoding a plain struct cannot realistically
		// fail, so this is purely defensive.
		_ = err
	}
}

// overviewTemplate is the embedded dashboard page. It renders the initial
// snapshot server-side and then refreshes the numbers in place via the JSON
// endpoints, so the page ships with no external assets.
var overviewTemplate = template.Must(template.New("overview").Parse(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{.Title}} — Queue Dashboard</title>
<style>
  :root { color-scheme: light dark; }
  body { font-family: system-ui, -apple-system, sans-serif; margin: 0; padding: 24px;
         background: #f6f7f9; color: #1c2733; }
  h1 { margin: 0 0 4px; font-size: 22px; }
  .sub { color: #6b7787; font-size: 13px; margin-bottom: 20px; }
  .cards { display: flex; flex-wrap: wrap; gap: 12px; margin-bottom: 20px; }
  .card { background: #fff; border: 1px solid #e2e6eb; border-radius: 10px;
          padding: 14px 18px; min-width: 130px; box-shadow: 0 1px 2px rgba(0,0,0,.04); }
  .card .num { font-size: 28px; font-weight: 600; }
  .card .lbl { font-size: 12px; color: #6b7787; text-transform: uppercase; letter-spacing: .04em; }
  .card.dead .num { color: #c0392b; }
  .card.running .num { color: #1a6feb; }
  h2 { font-size: 15px; margin: 22px 0 8px; }
  table { border-collapse: collapse; width: 100%; max-width: 640px; background: #fff;
          border: 1px solid #e2e6eb; border-radius: 10px; overflow: hidden; }
  th, td { text-align: left; padding: 8px 12px; font-size: 13px; border-bottom: 1px solid #eef1f4; }
  th { background: #fafbfc; color: #6b7787; font-weight: 600; text-transform: uppercase; font-size: 11px; }
  tr:last-child td { border-bottom: none; }
  .err { color: #c0392b; font-family: ui-monospace, monospace; font-size: 12px; }
  .pill { display: inline-block; padding: 1px 8px; border-radius: 999px; font-size: 11px; }
  .pill.running { background: #e8f0fe; color: #1a6feb; }
  .admin { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; margin-bottom: 8px; }
  .admin button { padding: 6px 14px; border: 1px solid #cfd6de; border-radius: 8px;
                  background: #fff; font-size: 13px; cursor: pointer; }
  .admin button:hover { background: #f0f4f8; }
  #admin-msg { font-size: 12px; color: #6b7787; font-family: ui-monospace, monospace; }
  .badge { display:inline-block; padding: 2px 10px; border-radius: 999px; font-size: 12px; font-weight:600; }
  .badge.on { background: #e4f5e9; color: #1e7c3a; }
  .badge.off { background: #fdecec; color: #c0392b; }
  @media (prefers-color-scheme: dark) {
    body { background: #14181d; color: #dbe2ea; }
    .card, table { background: #1d242c; border-color: #2a333d; }
    th { background: #161c22; color: #8b98a7; }
    td { border-color: #242d36; }
    .sub { color: #8b98a7; }
    .card .lbl { color: #8b98a7; }
  }
</style>
</head>
<body>
<h1>{{.Title}}</h1>
<div class="sub">Queue: <span class="pill running">{{.Status.Pending}}</span> pending ·
  <span id="uptime">{{.Status.UptimeSeconds}}</span>s uptime ·
  workers {{.Status.Workers}} ·
  <span id="started" class="badge {{if .Status.Started}}on{{else}}off{{end}}">{{if .Status.Started}}running{{else}}stopped{{end}}</span></div>

<div class="cards">
  <div class="card"><div class="num" id="c-pending">{{.Status.Pending}}</div><div class="lbl">Pending</div></div>
  <div class="card running"><div class="num" id="c-running">{{.Status.Running}}</div><div class="lbl">Running</div></div>
  <div class="card"><div class="num" id="c-succeeded">{{.Stats.Succeeded}}</div><div class="lbl">Succeeded</div></div>
  <div class="card dead"><div class="num" id="c-failed">{{.Stats.Failed}}</div><div class="lbl">Failed attempts</div></div>
  <div class="card dead"><div class="num" id="c-dead">{{.Status.Dead}}</div><div class="lbl">Dead (DLQ)</div></div>
</div>

<h2>Throughput (cumulative)</h2>
<table>
  <tr><th>Metric</th><th>Value</th></tr>
  <tr><td>Enqueued</td><td id="t-enqueued">{{.Stats.Enqueued}}</td></tr>
  <tr><td>Succeeded</td><td id="t-succeeded">{{.Stats.Succeeded}}</td></tr>
  <tr><td>Failed</td><td id="t-failed">{{.Stats.Failed}}</td></tr>
  <tr><td>Dead total</td><td id="t-deadtotal">{{.Stats.DeadTotal}}</td></tr>
</table>

<h2>By job type</h2>
<table>
  <tr><th>Type</th><th>Enqueued</th><th>Share</th></tr>
  {{range .ByType}}<tr><td>{{.Type}}</td><td>{{.Count}}</td><td>{{printf "%.1f" .Pct}}%</td></tr>{{else}}
  <tr><td colspan="3">No jobs enqueued yet.</td></tr>{{end}}
</table>

<h2>Dead-letter queue <small>(latest {{len .DeadJobs}})</small></h2>
<table>
  <tr><th>ID</th><th>Type</th><th>Attempts</th><th>Error</th><th>Died at</th></tr>
  {{range .DeadJobs}}
  <tr>
    <td>{{.ID}}</td><td>{{.Type}}</td><td>{{.Attempts}}/{{.MaxRetry}}</td>
    <td class="err">{{.LastError}}</td><td>{{.DeadAt.Format "2006-01-02 15:04:05"}}</td>
  </tr>
  {{else}}
  <tr><td colspan="5">No dead letters — all clear.</td></tr>
  {{end}}
</table>

<h2>Admin</h2>
<div class="admin">
  <button id="b-pause" onclick="admin('pause')">Pause</button>
  <button id="b-resume" onclick="admin('resume')">Resume</button>
  <button onclick="admin('purge')">Purge pending</button>
  <button onclick="admin('purge', {dead: true})">Purge pending + DLQ</button>
  <button onclick="admin('requeue-dead', {all: true})">Requeue DLQ</button>
  <span id="admin-msg"></span>
</div>

<script>
const refreshMs = {{.RefreshMillis}};
async function admin(op, body) {
  const msg = document.getElementById('admin-msg');
  try {
    const resp = await fetch('/api/admin/' + op, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: body ? JSON.stringify(body) : undefined,
    });
    const data = await resp.json();
    msg.textContent = data.ok ? ('ok: ' + JSON.stringify(data)) : ('error: ' + data.error);
    if (data.ok) tick();
  } catch (e) { msg.textContent = 'error: ' + e; }
}
async function tick() {
  try {
    const [status, stats] = await Promise.all([
      fetch('/api/status').then(r => r.json()),
      fetch('/api/stats').then(r => r.json()),
    ]);
    const $ = id => document.getElementById(id);
    $('c-pending').textContent = status.pending;
    $('c-running').textContent = status.running;
    $('c-failed').textContent = status.failed;
    $('c-dead').textContent = status.dead;
    $('c-succeeded').textContent = stats.succeeded;
    $('t-enqueued').textContent = stats.enqueued;
    $('t-succeeded').textContent = stats.succeeded;
    $('t-failed').textContent = stats.failed;
    $('t-deadtotal').textContent = stats.dead_total;
    $('uptime').textContent = status.uptime_seconds;
    const badge = $('started');
    badge.textContent = status.started ? 'running' : 'stopped';
    badge.className = 'badge ' + (status.started ? 'on' : 'off');
  } catch (e) { /* transient fetch failure; retry on next tick */ }
}
tick();
setInterval(tick, refreshMs);
</script>
</body>
</html>`))
