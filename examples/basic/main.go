// Command basic demonstrates the minimal GoQueue workflow: register a
// handler, enqueue jobs, process them with the worker pool and shut down.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/atop0914/goqueue"
)

func main() {
	// A client is both the producer (Enqueue) and the consumer (Start).
	q := goqueue.New(goqueue.WithWorkers(4))

	// Register the handler for the "greet" job type.
	q.Register("greet", func(ctx context.Context, payload []byte) error {
		fmt.Printf("hello, %s\n", payload)
		return nil
	})

	// Start the worker pool (returns immediately).
	q.Start()

	// Enqueue a small batch.
	for i := 0; i < 5; i++ {
		id, err := q.Enqueue(context.Background(), goqueue.Job{
			Type:    "greet",
			Payload: []byte(fmt.Sprintf("user-%d", i)),
		})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("enqueued %s (priority=%d)\n", id, i%2)
	}

	// Wait until the whole batch has been dequeued, then give the workers
	// a moment to finish their handlers before shutting down.
	for q.Queue().Len() > 0 {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)

	// Shutdown stops the pool gracefully: running handlers finish, then
	// the workers exit.
	if err := q.Shutdown(context.Background()); err != nil {
		log.Fatal(err)
	}
	fmt.Println("all done")
}