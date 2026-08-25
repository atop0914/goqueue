package goqueue

import "sync/atomic"

// Stats is a point-in-time snapshot of a Client's queue depth and its
// monotonic lifecycle counters. It is the data source for the dashboard
// package and for operators wiring up monitoring.
//
// Pending and Running are instantaneous gauges; Enqueued, Succeeded,
// Failed and DeadTotal are cumulative counters that only grow. ByType
// breaks Enqueued down per job type (again cumulative). The zero Stats is
// never returned by Client.Stats, but is a valid empty snapshot.
type Stats struct {
	// Pending is the number of jobs waiting in the queue (not yet dequeued).
	Pending int64
	// Running is the number of jobs currently being processed by workers.
	Running int64
	// Dead is the number of jobs in the dead-letter set right now.
	Dead int64
	// Workers is the configured worker pool size.
	Workers int
	// Started reports whether the worker pool is currently running.
	Started bool
	// Enqueued is the total number of jobs enqueued since New.
	Enqueued int64
	// Succeeded is the total number of jobs that finished without error.
	Succeeded int64
	// Failed is the total number of failed handler attempts (retriable or
	// not). A job that exhausts its retries counts once for Failed and once
	// for DeadTotal.
	Failed int64
	// DeadTotal is the total number of jobs moved to the dead-letter set.
	DeadTotal int64
	// ByType maps job type to its cumulative enqueue count.
	ByType map[string]int64
}

// Running returns how many jobs are currently being processed by workers
// (dequeued but not yet acked or nacked).
func (c *Client) Running() int64 { return c.inflight.Load() }

// Stats returns a point-in-time snapshot of queue depth and lifecycle
// counters. The call is cheap (a few atomic loads plus one queue depth
// read) and safe to invoke from any goroutine, e.g. on every dashboard
// scrape.
func (c *Client) Stats() Stats {
	s := Stats{
		Pending:   int64(c.cfg.Queue.Len()),
		Running:   c.inflight.Load(),
		Dead:      int64(len(c.cfg.Queue.Dead())),
		Workers:   c.cfg.Workers,
		Started:   c.started.Load(),
		Enqueued:  c.enqueued.Load(),
		Succeeded: c.succeeded.Load(),
		Failed:    c.failed.Load(),
		DeadTotal: c.deadTotal.Load(),
		ByType:    make(map[string]int64),
	}
	c.typeCount.Range(func(k, v any) bool {
		s.ByType[k.(string)] = v.(*atomic.Int64).Load()
		return true
	})
	return s
}