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

Under active development (2-week bootcamp, started 2026-08-13). Day 6:
benchmark suite, API review and runnable examples complete.