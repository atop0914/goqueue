package goqueue

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// BenchmarkClientFullCycle measures the end-to-end cost of submitting a job
// and waiting for it to be processed and acked through the client: enqueue +
// worker dequeue + handler + ack.
func BenchmarkClientFullCycle(b *testing.B) {
	c := New(WithWorkers(8))
	defer c.Shutdown(context.Background())
	c.Register("noop", func(ctx context.Context, payload []byte) error { return nil })
	c.Start()

	ctx := context.Background()
	job := Job{Type: "noop"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Enqueue(ctx, job); err != nil {
			b.Fatal(err)
		}
	}
	// The worker pool consumes asynchronously; wait until the whole batch
	// has been processed so the measured time covers the full cycle.
	for c.Queue().Len() > 0 {
		time.Sleep(10 * time.Microsecond)
	}
}

// BenchmarkClientThroughputSemaphore measures the client pipeline when
// handler concurrency is capped by WithMaxConcurrency — the common
// configuration for handlers that contend on a limited external resource.
// The semaphore adds a channel acquire/release per job.
func BenchmarkClientThroughputSemaphore(b *testing.B) {
	c := New(WithWorkers(8), WithMaxConcurrency(4))
	defer c.Shutdown(context.Background())
	c.Register("noop", func(ctx context.Context, payload []byte) error { return nil })
	c.Start()

	ctx := context.Background()
	job := Job{Type: "noop"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Enqueue(ctx, job); err != nil {
			b.Fatal(err)
		}
	}
	for c.Queue().Len() > 0 {
		time.Sleep(10 * time.Microsecond)
	}
}

// BenchmarkClientDrainShutdown measures the "submit then stop" pattern for
// ephemeral workers: launch a client, enqueue a fixed batch, then Shutdown
// with drain mode enabled which keeps consuming until the backlog is empty.
// Each iteration is one full lifecycle; the metric is normalised per job.
func BenchmarkClientDrainShutdown(b *testing.B) {
	const batch = 64
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c := New(WithWorkers(4), WithDrainOnShutdown(true))
		c.Register("noop", func(ctx context.Context, payload []byte) error { return nil })
		job := Job{Type: "noop"}
		for j := 0; j < batch; j++ {
			if _, err := c.Enqueue(context.Background(), job); err != nil {
				b.Fatal(err)
			}
		}
		c.Start()
		if err := c.Shutdown(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/batch, "ns/job")
}

// BenchmarkEnqueueDelayed measures the cost of enqueuing delayed jobs, which
// go through the same heap path as immediate jobs plus a readiness check.
func BenchmarkEnqueueDelayed(b *testing.B) {
	q := NewInMemoryQueue()
	defer q.Close()
	ctx := context.Background()
	job := Job{Type: "t", RunAfter: time.Now().Add(time.Hour)}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := q.Enqueue(ctx, job); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEnqueueUniqueJob measures the overhead of the UniqueKey dedup
// bookkeeping (map insert) on the enqueue path.
func BenchmarkEnqueueUniqueJob(b *testing.B) {
	q := NewInMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		job := Job{Type: "t", UniqueKey: fmt.Sprintf("key-%d", i)}
		if _, err := q.Enqueue(ctx, job); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkClientParallelProducers measures throughput when several
// goroutines enqueue concurrently into a running client.
func BenchmarkClientParallelProducers(b *testing.B) {
	c := New(WithWorkers(8))
	defer c.Shutdown(context.Background())
	c.Register("noop", func(ctx context.Context, payload []byte) error { return nil })
	c.Start()

	ctx := context.Background()
	job := Job{Type: "noop"}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := c.Enqueue(ctx, job); err != nil {
				b.Fatal(err)
			}
		}
	})
	for c.Queue().Len() > 0 {
		time.Sleep(10 * time.Microsecond)
	}
}

// BenchmarkClientWithHooks measures the client pipeline with a full set of
// lifecycle hooks attached — the production telemetry configuration.
func BenchmarkClientWithHooks(b *testing.B) {
	hook := func(JobInfo) {}
	c := New(
		WithWorkers(8),
		WithHooks(Hooks{
			OnEnqueue: hook,
			OnSuccess: hook,
		}),
	)
	defer c.Shutdown(context.Background())
	c.Register("noop", func(ctx context.Context, payload []byte) error { return nil })
	c.Start()

	ctx := context.Background()
	job := Job{Type: "noop"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Enqueue(ctx, job); err != nil {
			b.Fatal(err)
		}
	}
	for c.Queue().Len() > 0 {
		time.Sleep(10 * time.Microsecond)
	}
}

// BenchmarkDelay computes the deterministic backoff schedule cost.
func BenchmarkDelay(b *testing.B) {
	bf := DefaultRetryBackoff()
	for i := 0; i < b.N; i++ {
		_ = bf.Delay(i % 10)
	}
}