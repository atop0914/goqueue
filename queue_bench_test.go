package goqueue

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkEnqueue(b *testing.B) {
	q := NewInMemoryQueue()
	defer q.Close()
	ctx := context.Background()
	job := Job{Type: "t"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := q.Enqueue(ctx, job); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDequeue(b *testing.B) {
	q := NewInMemoryQueue()
	defer q.Close()
	ctx := context.Background()
	job := Job{Type: "t"}

	// Pre-fill the queue, then measure dequeue + ack.
	for i := 0; i < b.N; i++ {
		if _, err := q.Enqueue(ctx, job); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dj, err := q.Dequeue(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if err := q.Ack(ctx, dj.ID); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClientThroughput(b *testing.B) {
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
}

func BenchmarkEnqueueDequeueParallel(b *testing.B) {
	q := NewInMemoryQueue()
	defer q.Close()
	ctx := context.Background()
	job := Job{Type: "t"}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := q.Enqueue(ctx, job); err != nil {
				b.Fatal(err)
			}
			dj, err := q.Dequeue(ctx)
			if err != nil {
				b.Fatal(err)
			}
			if err := q.Ack(ctx, dj.ID); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPriorityQueueMixed(b *testing.B) {
	q := NewInMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		job := Job{Type: "t", Priority: i % 10}
		if _, err := q.Enqueue(ctx, job); err != nil {
			b.Fatal(err)
		}
		if i%2 == 0 {
			dj, err := q.Dequeue(ctx)
			if err != nil {
				b.Fatal(err)
			}
			if err := q.Ack(ctx, dj.ID); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func ExampleInMemoryQueue() {
	q := NewInMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	q.Enqueue(ctx, Job{Type: "email", Payload: []byte("welcome"), Priority: 5})
	dj, err := q.Dequeue(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("dequeued %s (priority %d)\n", dj.Payload, dj.Priority)
	// Output: dequeued welcome (priority 5)
}
