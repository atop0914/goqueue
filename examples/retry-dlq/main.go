// Command retry-dlq demonstrates retries with exponential backoff and the
// dead-letter queue: a handler that always fails exhausts its MaxRetry
// attempts and the job is isolated in the DLQ with the full failure story.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/atop0914/goqueue"
)

func main() {
	q := goqueue.New(
		goqueue.WithWorkers(2),
		// Aggressive backoff so the demo finishes fast (20ms, 40ms, ...).
		goqueue.WithRetryBackoff(goqueue.RetryBackoff{
			InitialInterval: 20 * time.Millisecond,
			MaxInterval:     40 * time.Millisecond,
			Multiplier:      2.0,
		}),
		goqueue.WithHooks(goqueue.Hooks{
			OnFailure: func(info goqueue.JobInfo) {
				fmt.Printf("  attempt %d failed: %s\n", info.Attempts, info.LastError)
			},
			OnDead: func(info goqueue.JobInfo) {
				fmt.Printf("  job %s dead after %d attempts\n", info.ID, info.Attempts)
			},
		}),
	)

	// A handler that always fails.
	q.Register("flaky", func(ctx context.Context, payload []byte) error {
		return fmt.Errorf("upstream unavailable")
	})
	q.Start()

	id, err := q.Enqueue(context.Background(), goqueue.Job{
		Type:     "flaky",
		Payload:  []byte("important"),
		MaxRetry: 3, // first run + 3 retries = 4 attempts
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("enqueued %s with MaxRetry=3\n", id)

	// Wait for the job to land in the DLQ.
	deadline := time.After(3 * time.Second)
	for len(q.Queue().Dead()) == 0 {
		select {
		case <-deadline:
			log.Fatal("timed out waiting for the DLQ")
		case <-time.After(10 * time.Millisecond):
		}
	}

	for _, d := range q.Queue().Dead() {
		fmt.Printf("DLQ entry: %s state=%s attempts=%d lastError=%q\n",
			d.ID, d.State, d.Attempts, d.LastError)
	}

	if err := q.Shutdown(context.Background()); err != nil {
		log.Fatal(err)
	}
	fmt.Println("done")
}
