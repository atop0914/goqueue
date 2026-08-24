// Command drain demonstrates drain-mode graceful shutdown: with
// WithDrainOnShutdown, Shutdown keeps the workers consuming the queue until
// every already-enqueued job (including delayed ones) has been processed —
// a "finish the backlog before we exit" guarantee for graceful pod shutdown.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/atop0914/goqueue"
)

func main() {
	q := goqueue.New(
		goqueue.WithWorkers(3),
		goqueue.WithDrainOnShutdown(true),
	)

	processed := 0
	q.Register("job", func(ctx context.Context, payload []byte) error {
		processed++
		fmt.Printf("processed %s (total %d)\n", payload, processed)
		return nil
	})

	// Enqueue a batch before starting the pool.
	for i := 0; i < 10; i++ {
		if _, err := q.Enqueue(context.Background(), goqueue.Job{
			Type:    "job",
			Payload: []byte(fmt.Sprintf("job-%02d", i)),
		}); err != nil {
			log.Fatal(err)
		}
	}

	// Start and shut down immediately: drain mode consumes the whole
	// backlog before Shutdown returns.
	q.Start()
	if err := q.Shutdown(context.Background()); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("drained: %d jobs processed before exit\n", processed)
}
