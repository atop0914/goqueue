// Command unique demonstrates unique jobs: at most one job with a given
// UniqueKey may be pending or running at a time. Duplicate enqueues are
// rejected with ErrJobExists until the key is released on success or death.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/atop0914/goqueue"
)

func main() {
	q := goqueue.New(goqueue.WithWorkers(1))
	q.Register("sync-user", func(ctx context.Context, payload []byte) error {
		time.Sleep(50 * time.Millisecond)
		fmt.Printf("synced %s\n", payload)
		return nil
	})
	q.Start()

	job := goqueue.Job{
		Type:      "sync-user",
		UniqueKey: "user:42",
		Payload:   []byte("user:42"),
	}

	id1, err := q.Enqueue(context.Background(), job)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("first enqueue ok: %s\n", id1)

	// The unique key is still held (job pending/running): reject.
	_, err = q.Enqueue(context.Background(), job)
	if errors.Is(err, goqueue.ErrJobExists) {
		fmt.Println("duplicate rejected with ErrJobExists")
	} else {
		log.Fatalf("expected ErrJobExists, got %v", err)
	}

	// Wait for the first job to finish; the key is released on Ack.
	time.Sleep(100 * time.Millisecond)

	id2, err := q.Enqueue(context.Background(), job)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("re-enqueue after release ok: %s\n", id2)

	time.Sleep(100 * time.Millisecond)
	if err := q.Shutdown(context.Background()); err != nil {
		log.Fatal(err)
	}
	fmt.Println("done")
}