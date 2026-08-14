# GoQueue

Lightweight, multi-backend background job queue for Go.

```go
q := goqueue.New(goqueue.WithWorkers(10))
q.Register("email", func(ctx context.Context, payload []byte) error {
    // send email...
    return nil
})
q.Start(context.Background())

id, err := q.Enqueue(ctx, goqueue.Job{
    Type:    "email",
    Payload: []byte(`{"to":"user@example.com"}`),
})
```

## Features

- **Zero-dependency core** — no external packages required for in-memory mode
- **Pluggable backends** — memory (built-in), SQLite, Redis (optional extensions)
- **Priority & delayed jobs** — heap-based scheduling
- **At-least-once delivery** — exponential backoff retry with max attempts
- **Dead-letter queue** — isolate jobs that exhausted their retries
- **Worker pool** — configurable concurrency, graceful shutdown, panic recovery
- **Observability** — Prometheus metrics + OpenTelemetry (optional)

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

## Status

Under active development (2-week bootcamp, started 2026-08-13).
