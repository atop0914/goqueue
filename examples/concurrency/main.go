// Command concurrency demonstrates WithMaxConcurrency: the worker pool
// spawns many goroutines, but the number of handler invocations running at
// the same time is capped — useful when handlers contend for a limited
// external resource such as a database connection pool or API quota.
package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/atop0914/goqueue"
)

func main() {
	// 8 workers, but at most 2 handlers running at any moment.
	q := goqueue.New(
		goqueue.WithWorkers(8),
		goqueue.WithMaxConcurrency(2),
	)

	var running, peak int64
	q.Register("io", func(ctx context.Context, payload []byte) error {
		cur := atomic.AddInt64(&running, 1)
		for {
			// Track the peak concurrent handler count.
			p := atomic.LoadInt64(&peak)
			if cur <= p || atomic.CompareAndSwapInt64(&peak, p, cur) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond) // simulate I/O
		atomic.AddInt64(&running, -1)
		return nil
	})
	q.Start()

	for i := 0; i < 16; i++ {
		if _, err := q.Enqueue(context.Background(), goqueue.Job{Type: "io"}); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("enqueued 16 jobs")

	// Wait for the batch to be consumed.
	for q.Queue().Len() > 0 {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)

	if err := q.Shutdown(context.Background()); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("peak concurrent handlers: %d (capped at 2)\n", atomic.LoadInt64(&peak))
}