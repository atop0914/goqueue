// Command hooks-results demonstrates the lifecycle hooks system and typed
// job results: RegisterWithResult publishes a typed value per attempt that
// callers await through the Task handle.
package main

import (
	"context"
	"fmt"
	"log"
	"strconv"

	"github.com/atop0914/goqueue"
)

func main() {
	q := goqueue.New(
		goqueue.WithWorkers(2),
		goqueue.WithHooks(goqueue.Hooks{
			OnEnqueue: func(info goqueue.JobInfo) {
				fmt.Printf("hook enqueue: %s %s\n", info.ID, info.Type)
			},
			OnSuccess: func(info goqueue.JobInfo) {
				fmt.Printf("hook success: %s (attempt %d)\n", info.ID, info.Attempts)
			},
			OnFailure: func(info goqueue.JobInfo) {
				fmt.Printf("hook failure: %s: %s\n", info.ID, info.LastError)
			},
		}),
	)

	// Typed result handler: the returned int is stored per attempt.
	goqueue.RegisterWithResult(q, "double", func(ctx context.Context, payload []byte) (int, error) {
		n, err := strconv.Atoi(string(payload))
		if err != nil {
			return 0, err
		}
		return n * 2, nil
	})
	q.Start()

	id, err := q.Enqueue(context.Background(), goqueue.Job{Type: "double", Payload: []byte("21")})
	if err != nil {
		log.Fatal(err)
	}

	// Block until the result is published, then read it.
	handle := goqueue.Task[int](q, id)
	value, err := handle.Get(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("double(21) = %d\n", value)

	// Non-blocking convenience form.
	if v, _, ok := goqueue.TryGetResult[int](q, id); ok {
		fmt.Printf("TryGetResult = %d\n", v)
	}

	if err := q.Shutdown(context.Background()); err != nil {
		log.Fatal(err)
	}
	fmt.Println("done")
}
