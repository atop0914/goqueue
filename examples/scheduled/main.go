// Command scheduled demonstrates recurring tasks: interval schedules via
// Every and cron expressions via Cron, both running on the built-in
// scheduler and stopped with ScheduleStop.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/atop0914/goqueue"
)

func main() {
	q := goqueue.New(goqueue.WithWorkers(2))
	tickNo := 0
	q.Register("tick", func(ctx context.Context, payload []byte) error {
		tickNo++
		fmt.Printf("tick %2d: %s\n", tickNo, payload)
		return nil
	})
	q.Start()

	// Interval schedule: fires every 100ms.
	everyID := q.Schedule(goqueue.Every(100*time.Millisecond), func() goqueue.Job {
		return goqueue.Job{Type: "tick", Payload: []byte("every 100ms")}
	})

	// Cron schedule: 6-field form (second minute hour day month weekday).
	// Fires once per second.
	cronSpec, err := goqueue.Cron("*/1 * * * * *")
	if err != nil {
		log.Fatal(err)
	}
	cronID := q.Schedule(cronSpec, func() goqueue.Job {
		return goqueue.Job{Type: "tick", Payload: []byte("cron */1s")}
	})

	// Let both run for a moment, then stop them.
	time.Sleep(1300 * time.Millisecond)
	q.ScheduleStop(everyID)
	q.ScheduleStop(cronID)
	fmt.Println("schedules stopped")

	if err := q.Shutdown(context.Background()); err != nil {
		log.Fatal(err)
	}
	fmt.Println("done")
}
