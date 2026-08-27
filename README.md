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
- **Observability** — Prometheus metrics + OpenTelemetry (optional, planned)

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

The counters behind `/api/stats` are maintained by the core `Client`
itself (`Stats()`, `Running()`), so the dashboard works even when no hooks
are configured. Mount the console under a path prefix if needed:

```go
mux := http.NewServeMux()
mux.Handle("/queue/", http.StripPrefix("/queue/", dash.Handler()))
```

Options: `WithTitle`, `WithMaxDeadJobs`, `WithRefreshInterval`,
`WithReadyCheck` (custom readiness probe, e.g. database reachability).

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

## Status

Under active development (2-week bootcamp, started 2026-08-13). Day 8:
SQLite backend ([`store/sqlite`](./store/sqlite), modernc pure-Go driver,
crash recovery, unique jobs, WAL) complete.