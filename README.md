# GoQueue

Lightweight, multi-backend background job queue for Go.

```go
q := goqueue.New(goqueue.WithWorkers(10))
q.Register("email", func(ctx context.Context, payload []byte) error {
    // send email...
    return nil
})
q.Start()

id, err := q.Enqueue(ctx, goqueue.Job{
    Type:    "email",
    Payload: []byte(`{"to":"user@example.com"}`),
})
```

## Features

- **Zero-dependency core** — no external packages required for in-memory mode
- **Pluggable backends** — memory (built-in), SQLite, Redis (optional extensions)
- **Priority & delayed jobs** — heap-based scheduling, priority + `RunAfter`
- **Unique jobs** — at most one in-flight job per `UniqueKey`
- **At-least-once delivery** — exponential backoff retry with max attempts
- **Dead-letter queue** — isolate jobs that exhausted their retries
- **Recurring tasks** — interval (`Every`) and cron-style (`Cron`) schedules
- **Typed results** — `RegisterWithResult` + `Task[T]` await handler output
- **Lifecycle hooks** — `OnEnqueue` / `OnSuccess` / `OnFailure` / `OnRetry` / `OnDead`
- **Worker pool** — configurable concurrency (`WithMaxConcurrency`), graceful
  shutdown (`WithDrainOnShutdown`), panic recovery
- **HTTP dashboard** — embedded ops console: overview page, status/stats/dead-letter
  JSON APIs, liveness & readiness probes
- **Observability** — Prometheus metrics + OpenTelemetry tracing (optional extension modules)

## Retry & backoff

Failed jobs are re-scheduled with exponential backoff instead of being
re-queued immediately. The default schedule is 100ms doubling per attempt,
capped at 30s; override it per client:

```go
q := goqueue.New(
    goqueue.WithRetryBackoff(goqueue.RetryBackoff{
        InitialInterval: 250 * time.Millisecond,
        MaxInterval:     30 * time.Second,
        Multiplier:      2.0,
    }),
)
```

A job whose `MaxRetry` attempts are exhausted moves to the dead-letter set.
The `OnDead` callback receives the full job snapshot (attempts, last error,
enqueue/death timestamps), and `Queue().Dead()` lists the DLQ ordered by
death time.

## Examples

Run any of them directly (`go run ./examples/<name>`); each completes on its
own:

| Example | Pattern |
|---------|---------|
| [`basic`](./examples/basic) | register → enqueue → process → shutdown |
| [`retry-dlq`](./examples/retry-dlq) | exponential backoff, failure hooks, DLQ isolation |
| [`scheduled`](./examples/scheduled) | `Every` interval + `Cron` recurring tasks, `ScheduleStop` |
| [`unique`](./examples/unique) | `UniqueKey` dedup and `ErrJobExists` rejection |
| [`hooks-results`](./examples/hooks-results) | lifecycle hooks + typed results via `Task[T]` |
| [`drain`](./examples/drain) | drain-mode shutdown finishes the backlog |
| [`concurrency`](./examples/concurrency) | `WithMaxConcurrency` caps running handlers |
| [`dashboard`](./examples/dashboard) | worker + embedded HTTP dashboard on `:8080` |
| [`admin-ops`](./examples/admin-ops) | programmatic admin: `Pause`/`Purge`/`RequeueDead`/`RequeueDeadJob` |

## SQLite backend (persistent)

The [`store/sqlite`](./store/sqlite) subpackage adds a durable `Queue`
implementation on top of an embedded SQLite database, using the pure-Go
`modernc.org/sqlite` driver — no CGo, no external database process. Drop it in
via `goqueue.WithQueue` and every job survives a restart:

```go
import (
    goqueue "github.com/atop0914/goqueue"
    "github.com/atop0914/goqueue/store/sqlite"
)

st, err := sqlite.Open("jobs.db") // created on first run
if err != nil {
    log.Fatal(err)
}
defer st.CloseDB()

cli := goqueue.New(goqueue.WithQueue(st), goqueue.WithWorkers(4))
```

Behavior:

- **Crash recovery** — jobs found in `running` state at startup are returned
  to `pending` immediately; their attempt count is preserved, so the
  at-least-once contract holds across hard kills. The recovery sweep never
  resurrects terminal rows: `succeeded`/`dead` (and their `dead_at` /
  `last_error` metadata) survive restarts untouched, and a delayed row keeps
  its future `run_after` — it fires late, never early.
- **Recovery vs. retry budget** — a job whose final fair delivery was killed
  mid-flight is redelivered once more after restart (at-least-once), and if
  that attempt fails, the next `Nack` dead-letters it even when the caller
  labels the error retryable. Poison jobs therefore terminate instead of
  looping across restarts.
- **Same scheduling policy as memory** — ready jobs dequeue by
  `run_after ASC, priority DESC, seq`; retries reuse the delayed-job path.
- **Unique jobs** — enforced with a partial unique index; the key is held
  across retries *and across crashes* (a recovered keyed job still blocks
  duplicates) and is released on success or DLQ.
- **WAL journaling + busy timeout + immediate transactions** — write
  transactions take the write lock up front (`txlock=IMMEDIATE`), avoiding
  lock-upgrade busy storms; safe under concurrent producers and workers in
  one process. Throughput baseline on a 2-core Xeon (file mode): Enqueue
  ≈0.1–0.4 ms/op, Dequeue+Ack ≈0.6–1.5 ms/op; per-op cost grows slowly with
  accumulated terminal rows in the same table.

API notes: `Close()` stops job pickup and unblocks `Dequeue` waiters but
leaves data readable; `CloseDB()` additionally releases the file handle.
`Stats()` reports per-state row counts for dashboards.

## Redis backend (distributed)

The [`store/redis`](./store/redis) subpackage adds a distributed `Queue`
implementation on top of Redis (via `github.com/redis/go-redis/v9`). Where
SQLite coordinates one process, Redis coordinates many: several application
instances can share one queue safely. Tests use miniredis (an in-process
server), so trying the backend out needs no Docker either:

```go
import (
    goqueue "github.com/atop0914/goqueue"
    "github.com/atop0914/goqueue/store/redis"
)

st, err := redis.Open("localhost:6379") // options: WithQueue, WithPassword, WithDB
if err != nil {
    log.Fatal(err)
}
defer st.CloseClient()

cli := goqueue.New(goqueue.WithQueue(st), goqueue.WithWorkers(4))
```

Behavior:

- **Atomic everywhere** — unique-key claim, ready-queue claim, ack and
  nack/retry/DLQ routing each run as a Lua script, so concurrent consumers —
  in one process or across a fleet — can never interleave a read and write of
  the same key. Jobs are never double-claimed and unique keys never slip.
- **Same scheduling policy as memory and SQLite** — ready jobs dequeue by
  `run_after ASC, priority DESC, seq`. The ready set is one ZSET scored by
  run-after (ms) minus a clamped priority (±499); the zero-padded seq inside
  the member breaks remaining ties. Priorities beyond ±499 are clamped, not
  rejected.
- **Crash recovery** — jobs found in the `running` set at startup are
  returned to the ready queue immediately with their attempt count preserved
  (at-least-once across hard kills). Unique keys held by crashed jobs stay
  held until a real ack or DLQ releases them.
- **Unique jobs** — claimed `SETNX`-style inside the enqueue script; the key
  is held across retries and crashes, released on success or DLQ.
- **Multi-queue namespaces** — `redis.Open(addr, redis.WithQueue("orders"))`
  keys everything under `goqueue:orders:*`, so independent queues share one
  Redis server without seeing each other's jobs.

API notes mirror the SQLite backend: `Close()` stops job pickup and unblocks
`Dequeue` waiters but leaves all data in Redis; `CloseClient()` additionally
closes the connection. `Stats()` reports pending/running/succeeded/dead
counts. `Len` is context-aware (implements `LenAwareQueue`) like SQLite, so
the client's drain probe cannot wedge shutdown.

## Observability

Two optional extension modules add Prometheus metrics and OpenTelemetry
tracing. Both live outside the core module, so importers who do not need
them never pull the dependencies.

### Prometheus metrics (`obs/metrics`)

```go
import (
    "net/http"

    goqueue "github.com/atop0914/goqueue"
    "github.com/atop0914/goqueue/obs/metrics"
)

col := metrics.NewCollector(metrics.Options{Namespace: "goqueue"})
client := goqueue.New(goqueue.WithHooks(col.Hooks()))
col.Watch(client) // queue_depth / jobs_running / dead_jobs gauges

http.Handle("/metrics", col.Handler()) // Prometheus text format
```

Hook wiring produces `jobs_enqueued_total`, `jobs_succeeded_total`,
`jobs_failed_total`, `jobs_retried_total` and `jobs_dead_total` counters
(per job `type`) plus the `job_duration_seconds` end-to-end histogram;
`Watch(client)` registers pull-based gauges backed by `client.Stats()` on
every scrape. Use `goqueue.CombineHooks` when you also need your own hooks.

### OpenTelemetry tracing (`obs/tracing`)

```go
import (
    goqueue "github.com/atop0914/goqueue"
    "github.com/atop0914/goqueue/obs/tracing"
)

tr := tracing.NewTracer(tracing.Options{ServiceName: "workers"})
reg := tracing.NewRegistry()
defer reg.Close()

client := goqueue.New(
    goqueue.WithHooks(tr.Hooks(reg)),
    goqueue.WithContextDecorator(tr.ContextDecorator(reg)),
)
```

Each job gets one end-to-end `goqueue.process` span: started at enqueue,
kept open across retries and waits, finished when the job succeeds (OK) or
lands in the dead-letter queue (error, with `goqueue.retry` and
`goqueue.dead_letter` events). Producer-side spans join an existing request
trace and, via `StartJobSpan` + `WithContextDecorator`, become the parent
context of handler invocations so spans emitted inside handlers nest
correctly:

```go
ctx, span := tr.StartJobSpan(reg, ctx, "emails", emailJobID)
defer span.End()
_, err := client.Enqueue(ctx, goqueue.Job{ID: emailJobID, Type: "emails"})
```

## Dashboard

The [`dashboard`](./dashboard) subpackage is a zero-dependency embedded
operations console for a `Client`. It serves an auto-refreshing HTML
overview plus JSON endpoints, so you get visibility without wiring up a
metrics stack:

```go
cli := goqueue.New(goqueue.WithWorkers(4))
// ... register handlers, Start, Enqueue ...

dash := dashboard.New(cli, dashboard.WithTitle("API Workers"))
http.Handle("/", dash)
log.Fatal(http.ListenAndServe(":8080", nil))
```

| Endpoint | Content |
|----------|---------|
| `/` | HTML overview (cards, throughput table, per-type share, DLQ list) |
| `/api/status` | gauges: `pending`, `running`, `dead`, `workers`, `started`, `uptime_seconds` |
| `/api/stats` | cumulative counters: `enqueued`, `succeeded`, `failed`, `dead_total`, `by_type` |
| `/api/jobs` | latest dead-letter jobs (newest first, capped at 50 by default) |
| `/healthz` | liveness probe — always `200` while the process is up |
| `/healthz/ready` | readiness probe — `200`, or `503` when a custom check fails |

Admin endpoints (POST only — a GET answers `405`; unsupported backend
capabilities answer `501`):

| Endpoint | Effect |
|----------|--------|
| `POST /api/admin/pause` | stop job delivery (workers idle, jobs retained) |
| `POST /api/admin/resume` | lift a pause |
| `POST /api/admin/purge` | drop pending jobs; body `{"dead":true}` also drops the DLQ |
| `POST /api/admin/requeue-dead` | requeue the whole DLQ; body `{"id":"..."}` requeues one job |

Requeued jobs restart with a fresh retry budget (attempts reset, errors
cleared, due immediately). A job whose unique key is held by another
in-flight job stays dead (`409` for the single-job form). The overview page
ships with an admin button row wired to these endpoints.

The counters behind `/api/stats` are maintained by the core `Client`
itself (`Stats()`, `Running()`), so the dashboard works even when no hooks
are configured. Mount the console under a path prefix if needed:

```go
mux := http.NewServeMux()
mux.Handle("/queue/", http.StripPrefix("/queue/", dash.Handler()))
```

Options: `WithTitle`, `WithMaxDeadJobs`, `WithRefreshInterval`,
`WithReadyCheck` (custom readiness probe, e.g. database reachability).

## Admin operations (programmatic)

All dashboard admin endpoints are thin wrappers over the `Client` API —
use it directly for maintenance tooling, tests or ops scripts:

```go
cli.Pause()                          // delivery stops; enqueues still accepted
// ... maintenance window: drain, deploy, inspect ...
cli.Resume()

n, _ := cli.Purge(ctx, false)        // drop pending jobs (unique keys released)
n, _ := cli.Purge(ctx, true)         // ... and the dead-letter set
n, _ := cli.RequeueDead(ctx)         // requeue the whole DLQ, attempts reset
_ = cli.RequeueDeadJob(ctx, id)      // cherry-pick one dead job
```

Operations are optional capabilities the backend may implement
(`goqueue.AdminQueue` = `Pauser` + `Purger` + `DeadRequeuer`); all three
built-in backends (memory, SQLite, Redis) implement the full set. A backend
without a capability makes the matching `Client` method return
`ErrAdminUnsupported`, so custom `Queue` implementations stay compatible.

Semantic notes:

- **Pause** is per-process (memory and Redis: local flag; SQLite: runtime
  flag surviving nothing) — a restarted process resumes delivery; enqueued
  jobs and their priorities/retry schedules are retained verbatim.
- **Purge** never touches running jobs; purged jobs' unique keys are
  released, so the same work can be re-enqueued. Idempotent.
- **RequeueDead / RequeueDeadJob** reset attempts to zero and clear
  `last_error`/`dead_at`; a dead job whose unique key is held by another
  live job stays dead (`ErrJobExists` for the single-job form, silently
  skipped for the wholesale form).

## Performance

Benchmarks (`go test -bench=. -benchmem ./...`), 2-core Xeon Gold 6148:

| Benchmark | ns/op | B/op | allocs/op |
|-----------|-------|------|-----------|
| Enqueue | ~1.2-2.4µs | 257 | 2 |
| Dequeue + Ack | ~1.0-1.9µs | 144 | 1 |
| Client full cycle (enqueue→ack, 8 workers) | ~3.0-3.6µs | 545 | 6 |
| Client, parallel producers | ~1.7-2.2µs | 384 | 3 |
| Client, `WithMaxConcurrency(4)` | ~2.4µs | 549 | 7 |
| Client, hooks attached | ~3.5µs | 557 | 7 |
| Drain lifecycle (64 jobs, 4 workers) | ~3.9µs/job | — | — |
| Backoff schedule (`Delay`) | ~43ns | 0 | 0 |
| Redis Enqueue (miniredis) | ~0.6ms | 208k | 854 |
| Redis Dequeue + Ack (miniredis) | ~1.1ms | 488k | 1786 |

Redis numbers are miniredis-based, i.e. the in-process Lua-script cost with
the network round-trip excluded; against a real server add one loopback RTT
per operation (two scripts for the Dequeue+Ack pair). SQLite baselines:
Enqueue ≈0.1–0.4 ms/op, Dequeue+Ack ≈0.6–1.5 ms/op (file mode, see above).

## Status

Under active development (2-week bootcamp, started 2026-08-13). Day 12
added queue administration: `Pause`/`Resume`, `Purge` and
`RequeueDead`/`RequeueDeadJob` as optional backend capabilities
(`goqueue.AdminQueue`), implemented by all three built-in backends, plus
POST admin endpoints and an admin button row in the
[`dashboard`](./dashboard) console.
