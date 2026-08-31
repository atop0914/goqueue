// Command admin-ops demonstrates queue administration with the built-in
// memory backend: Pause, Purge, RequeueDead and the optional capability
// interface (AdminQueue). All three operations are safe to call from
// maintenance tooling or ops scripts; unsupported backends return
// ErrAdminUnsupported instead of panicking.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	goqueue "github.com/atop0914/goqueue"
)

func main() {
	cli := goqueue.New(
		goqueue.WithWorkers(2),
		goqueue.WithHooks(goqueue.Hooks{
			OnDead: func(info goqueue.JobInfo) {
				fmt.Printf("dead: %s after %d attempts: %s\n", info.ID, info.Attempts, info.LastError)
			},
		}),
	)
	cli.Register("email", func(ctx context.Context, payload []byte) error {
		return fmt.Errorf("smtp: %s", string(payload))
	})

	cli.Start()

	// Enqueue a few jobs that are guaranteed to die.
	for i := 0; i < 3; i++ {
		if _, err := cli.Enqueue(context.Background(), goqueue.Job{
			Type:    "email",
			Payload: []byte(fmt.Sprintf("fail-%d@example.com", i)),
		}); err != nil {
			log.Fatal(err)
		}
	}

	// Wait for the DLQ to collect them.
	time.Sleep(500 * time.Millisecond)
	dead := cli.Queue().Dead()
	fmt.Printf("DLQ size: %d\n", len(dead))

	// 1) Pause delivery. Producers may keep enqueueing while workers idle.
	cli.Pause()
	if cli.IsPaused() {
		fmt.Println("queue paused")
	}

	// 2) Purge pending jobs (running jobs are never touched).
	n, err := cli.Purge(context.Background(), false)
	fmt.Printf("purged %d pending jobs (err=%v)\n", n, err)

	// 3) Drop the DLQ as well.
	n, err = cli.Purge(context.Background(), true)
	fmt.Printf("purged %d dead jobs (err=%v)\n", n, err)

	// 4) Requeue everything in the DLQ (attempts reset, errors cleared).
	cli.Resume()
	// Re-fill the DLQ for the demo.
	for i := 0; i < 3; i++ {
		cli.Enqueue(context.Background(), goqueue.Job{
			Type:    "email",
			Payload: []byte(fmt.Sprintf("fail-%d@example.com", i)),
		})
	}
	time.Sleep(500 * time.Millisecond)
	requeued, err := cli.RequeueDead(context.Background())
	fmt.Printf("requeued %d dead jobs (err=%v)\n", requeued, err)

	// 5) Cherry-pick one dead job by ID.
	if len(dead) > 0 {
		id := dead[0].ID
		if err := cli.RequeueDeadJob(context.Background(), id); err != nil {
			if errors.Is(err, goqueue.ErrJobNotFound) {
				fmt.Println("single requeue: not found (already requeued)")
			} else {
				fmt.Printf("single requeue err: %v\n", err)
			}
		} else {
			fmt.Printf("single requeue ok: %s\n", id)
		}
	}

	// 6) Capability discovery: custom backends may not implement admin ops.
	// Check with errors.Is before relying on them.
	_ = cli.Queue()

	_ = cli.Shutdown(context.Background())
	fmt.Println("done")
}
