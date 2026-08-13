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

## Status

Under active development (2-week bootcamp, started 2026-08-13).
