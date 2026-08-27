package goqueue

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Client is the single entry point: it is both the producer (Enqueue) and
// the consumer (Start spawns the worker pool). Use New to create one.
type Client struct {
	cfg        Config
	mu         sync.RWMutex
	types      map[string]Handler
	tasks      map[string]*scheduledTask
	results    sync.Map // job ID -> *resultSlot (typed-result jobs only)
	started    atomic.Bool
	stop       chan struct{}
	baseCtx    context.Context
	baseCancel context.CancelFunc
	wg         sync.WaitGroup
	// sem caps concurrent handler invocations; nil when MaxConcurrency is
	// unset (unlimited).
	sem chan struct{}
	// inflight counts jobs that have been dequeued but not yet Acked/Nacked.
	// Drain mode waits for it to reach zero together with an empty queue.
	inflight atomic.Int64
	// Monotonic lifecycle counters, exposed via Stats() and consumed by the
	// dashboard package. They are updated for every event regardless of
	// whether hooks are configured, so observability does not depend on the
	// caller wiring up callbacks.
	enqueued  atomic.Int64
	succeeded atomic.Int64
	failed    atomic.Int64
	deadTotal atomic.Int64
	// typeCount tracks enqueue counts per job type (key: string, value:
	// *atomic.Int64). Unbounded by design: job types are a small, finite set
	// chosen at deployment time.
	typeCount sync.Map
}

// New creates a Client with the given options. The default queue is an
// in-memory backend; pass WithQueue to use a persistent one.
func New(opts ...Option) *Client {
	cfg := Config{
		Queue:        NewInMemoryQueue(),
		Workers:      DefaultWorkers,
		PollInterval: DefaultPollInterval,
		RetryBackoff: DefaultRetryBackoff(),
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	baseCtx, baseCancel := context.WithCancel(context.Background())
	cli := &Client{
		cfg:        cfg,
		types:      make(map[string]Handler),
		tasks:      make(map[string]*scheduledTask),
		stop:       make(chan struct{}),
		baseCtx:    baseCtx,
		baseCancel: baseCancel,
	}
	if cfg.MaxConcurrency > 0 {
		cli.sem = make(chan struct{}, cfg.MaxConcurrency)
	}
	return cli
}

// Register binds a handler to a job type. Enqueuing a job whose type has no
// handler fails with ErrUnknownType. Register must be called before Start.
func (c *Client) Register(taskType string, h Handler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.types[taskType] = h
}

// Enqueue submits a job and returns its ID. The job type must be registered.
// After a successful enqueue the OnEnqueue hook (if set) fires with the
// pending job's snapshot.
func (c *Client) Enqueue(ctx context.Context, job Job) (string, error) {
	c.mu.RLock()
	_, ok := c.types[job.Type]
	c.mu.RUnlock()
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrUnknownType, job.Type)
	}
	id, err := c.cfg.Queue.Enqueue(ctx, job)
	if err != nil {
		return "", err
	}
	if h := c.cfg.Hooks.OnEnqueue; h != nil {
		h(JobInfo{
			ID:         id,
			Type:       job.Type,
			State:      StatePending,
			Attempts:   0,
			MaxRetry:   job.MaxRetry,
			Priority:   job.Priority,
			EnqueuedAt: time.Now(),
		})
	}
	c.countEnqueued(job.Type)
	return id, nil
}

// countEnqueued bumps the lifecycle counters after a successful enqueue.
func (c *Client) countEnqueued(taskType string) {
	c.enqueued.Add(1)
	counter, _ := c.typeCount.LoadOrStore(taskType, new(atomic.Int64))
	counter.(*atomic.Int64).Add(1)
}

// Start launches the worker pool. It returns immediately; workers run in the
// background until Shutdown is called. Start is idempotent.
func (c *Client) Start() {
	if !c.started.CompareAndSwap(false, true) {
		return
	}
	for i := 0; i < c.cfg.Workers; i++ {
		c.wg.Add(1)
		go c.worker(i)
	}
}

// Shutdown stops the worker pool gracefully. In the default mode workers
// finish the job they are currently handling, then exit; jobs still pending
// in the queue are left for the next Start. With WithDrainOnShutdown, the
// workers instead keep consuming the queue until every pending job has been
// processed, which is useful for "finish the backlog before we stop"
// scenarios (e.g. graceful pod shutdown in a deployment). In both modes the
// given ctx bounds the total wait: once it expires, Shutdown returns
// ctx.Err() and any remaining workers exit on their own schedule.
func (c *Client) Shutdown(ctx context.Context) error {
	if !c.started.CompareAndSwap(true, false) {
		return nil
	}
	close(c.stop)
	c.stopTasks() // stop recurring scheduled tasks
	if c.draining() {
		// Drain mode: keep the base context alive so Dequeue keeps
		// delivering the remaining backlog. Workers cancel it themselves
		// once the queue is empty and nothing is in flight. Nothing to
		// drain? Wake everyone immediately.
		if c.drainDone() {
			c.baseCancel()
		}
	} else {
		c.baseCancel() // unblock workers waiting in Dequeue
	}
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Queue exposes the underlying backend (inspection, tests, direct access).
func (c *Client) Queue() Queue { return c.cfg.Queue }

// Info returns the current number of pending jobs and dead-letter jobs.
func (c *Client) Info() (pending, dead int) {
	return c.cfg.Queue.Len(), len(c.cfg.Queue.Dead())
}

// worker runs the dequeue -> handle -> ack/nack loop.
func (c *Client) worker(id int) {
	defer c.wg.Done()
	for {
		// Stop received? Normal mode exits immediately; drain mode keeps
		// consuming and only exits once the queue is fully drained (nothing
		// pending, nothing in flight). Before stop arrives, an empty queue
		// is merely a waiting state — the worker must never leave early,
		// otherwise a pool started before jobs are enqueued would exit
		// before doing any work.
		select {
		case <-c.stop:
			if !c.draining() {
				return
			}
			if c.drainDone() {
				c.baseCancel() // wake peers blocked in Dequeue
				return
			}
		default:
		}

		dj, err := c.cfg.Queue.Dequeue(c.baseCtx)
		if err != nil {
			if err == ErrQueueClosed {
				return
			}
			// Canceled or transient error: re-check everything. In drain
			// mode a base-context cancel is not an exit signal — the queue
			// may still hold jobs — so only leave when drained.
			if c.draining() && c.drainDone() {
				c.baseCancel()
				return
			}
			select {
			case <-c.stop:
				if !c.draining() {
					return
				}
			case <-time.After(c.cfg.PollInterval):
			}
			continue
		}

		c.inflight.Add(1)
		c.process(dj)
		c.inflight.Add(-1)
	}
}

// process runs one dequeued job through the semaphore (if configured), the
// registered handler and the ack/nack + hooks flow. It is called by workers;
// panics inside handlers are recovered by invoke and surface as errors here,
// so a panicking handler never kills the pool.
func (c *Client) process(dj *DequeuedJob) {
	// Cap concurrent handlers with the semaphore, if configured. The job is
	// already dequeued, so we block rather than drop it: a free slot is
	// guaranteed once the running handlers finish.
	if c.sem != nil {
		c.sem <- struct{}{}
		defer func() { <-c.sem }()
	}

	// Run the handler with a per-attempt timeout if configured. The job ID
	// is carried in the context so typed-result handlers can publish their
	// outcome.
	runCtx, cancelRun := context.WithCancel(context.Background())
	if dj.Timeout > 0 {
		runCtx, cancelRun = context.WithTimeout(runCtx, dj.Timeout)
	}
	runCtx = context.WithValue(runCtx, jobIDKey{}, dj.ID)

	err := c.invoke(runCtx, dj)
	cancelRun()

	if err == nil {
		c.succeeded.Add(1)
		_ = c.cfg.Queue.Ack(context.Background(), dj.ID)
		if h := c.cfg.Hooks.OnSuccess; h != nil {
			h(dj.jobInfo(StateSucceeded, "", time.Time{}))
		}
		return
	}
	c.failed.Add(1)
	retryable := dj.Attempt <= dj.MaxRetry
	// Backoff the next attempt: the queue schedules it RunAfter now+delay.
	delay := c.cfg.RetryBackoff.Delay(dj.Attempt)
	_ = c.cfg.Queue.Nack(context.Background(), dj.ID, err, retryable, delay)
	// Every failed attempt fires OnFailure; retriable attempts additionally
	// fire OnRetry, exhausted attempts fire OnDead.
	info := dj.jobInfo(StateFailed, err.Error(), time.Time{})
	if h := c.cfg.Hooks.OnFailure; h != nil {
		h(info)
	}
	if retryable {
		if h := c.cfg.Hooks.OnRetry; h != nil {
			h(info)
		}
		return
	}
	info.State = StateDead
	info.DeadAt = time.Now()
	c.deadTotal.Add(1)
	if h := c.cfg.Hooks.OnDead; h != nil {
		h(info)
	} else if c.cfg.OnDead != nil {
		c.cfg.OnDead(info) // backward compatibility
	}
}

// draining reports whether Shutdown must drain the queue before returning.
func (c *Client) draining() bool { return c.cfg.DrainOnShutdown }

// drainDone reports whether the queue holds nothing left to process: no
// pending jobs and no dequeued-but-unfinished jobs. Only meaningful while
// draining; callers must hold no assumptions about queue internals.
func (c *Client) drainDone() bool {
	if c.inflight.Load() != 0 {
		return false
	}
	if lq, ok := c.cfg.Queue.(LenAwareQueue); ok {
		ctx, cancel := context.WithTimeout(context.Background(), drainProbeTimeout)
		defer cancel()
		return lq.LenContext(ctx) == 0
	}
	return c.cfg.Queue.Len() == 0
}

// drainProbeTimeout bounds each drain-completion probe against the backend.
const drainProbeTimeout = 2 * time.Second

// invoke looks up the handler and runs it, recovering from panics.
func (c *Client) invoke(ctx context.Context, dj *DequeuedJob) (err error) {
	c.mu.RLock()
	h, ok := c.types[dj.Type]
	c.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownType, dj.Type)
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in handler for %q: %v", dj.Type, r)
		}
	}()
	return h(ctx, dj.Payload)
}
