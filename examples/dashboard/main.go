// Command dashboard runs a tiny worker with the embedded dashboard
// listening on :8080, then exercises the queue so the overview has
// something to show.
//
//	curl localhost:8080/healthz
//	curl localhost:8080/api/status
//	curl localhost:8080/api/stats
//	curl localhost:8080/api/jobs
//	open  http://localhost:8080/
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/atop0914/goqueue"
	"github.com/atop0914/goqueue/dashboard"
)

func main() {
	cli := goqueue.New(goqueue.WithWorkers(4), goqueue.WithPollInterval(10*time.Millisecond))
	cli.Register("email", func(ctx context.Context, p []byte) error {
		time.Sleep(2 * time.Millisecond)
		return nil
	})
	cli.Register("sms", func(ctx context.Context, p []byte) error { return nil })
	cli.Register("flaky", func(ctx context.Context, p []byte) error {
		if time.Now().UnixNano()%3 == 0 {
			return errors.New("upstream timeout")
		}
		return nil
	})

	dash := dashboard.New(cli, dashboard.WithTitle("Demo Worker"))
	http.Handle("/", dash)
	go func() {
		log.Fatal(http.ListenAndServe(":8080", nil))
	}()

	cli.Start()
	defer func() { _ = cli.Shutdown(context.Background()) }()

	// Produce traffic so the dashboard has live numbers.
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		typ := "email"
		switch {
		case i%4 == 0:
			typ = "sms"
		case i%3 == 0:
			typ = "flaky"
		}
		id, err := cli.Enqueue(ctx, goqueue.Job{Type: typ})
		if err != nil {
			log.Printf("enqueue %s: %v", typ, err)
		} else {
			fmt.Printf("enqueued %-6s %s\n", typ, id)
		}
		time.Sleep(15 * time.Millisecond)
	}

	time.Sleep(500 * time.Millisecond) // let workers drain

	s := cli.Stats()
	fmt.Printf("\ndashboard: %d enqueued, %d succeeded, %d failed, %d dead\n",
		s.Enqueued, s.Succeeded, s.Failed, s.DeadTotal)
	fmt.Printf("dashboard live at http://localhost:8080/ (pending=%d running=%d dead=%d)\n",
		s.Pending, s.Running, s.Dead)
}
