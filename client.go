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
	return &Client{
		cfg:        cfg,
		types:      make(map[string]Handler),
		tasks:      make(map[string]*scheduledTask),
		stop:       make(chan struct{}),
		baseCtx:    baseCtx,
		baseCancel: baseCancel,
	}
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
	return id, nil
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

// Shutdown stops the worker pool gracefully: workers finish the job they are
// currently handling, then exit. Pending jobs remain in the queue and can be
// picked up after restart. It blocks until all workers have exited.
func (c *Client) Shutdown(ctx context.Context) error {
	if !c.started.CompareAndSwap(true, false) {
		return nil
	}
	close(c.stop)
	c.baseCancel() // unblock workers waiting in Dequeue
	c.stopTasks()  // stop recurring scheduled tasks
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
		select {
		case <-c.stop:
			return
		default:
		}

		ctx := c.baseCtx
		dj, err := c.cfg.Queue.Dequeue(ctx)
		if err != nil {
			if err == ErrQueueClosed {
				return
			}
			// Cancelled (shutdown) or transient error: brief backoff, then
			// re-check stop at the top of the loop.
			select {
			case <-c.stop:
				return
			case <-time.After(c.cfg.PollInterval):
			}
			continue
		}

		// Run the handler with a per-attempt timeout if configured. The job ID
		// is carried in the context so typed-result handlers can publish their
		// outcome.
		runCtx, cancelRun := context.WithCancel(context.Background())
		if dj.Timeout > 0 {
			runCtx, cancelRun = context.WithTimeout(runCtx, dj.Timeout)
		}
		runCtx = context.WithValue(runCtx, jobIDKey{}, dj.ID)

		err = c.invoke(runCtx, dj)
		cancelRun()

		if err == nil {
			_ = c.cfg.Queue.Ack(context.Background(), dj.ID)
			if h := c.cfg.Hooks.OnSuccess; h != nil {
				h(dj.jobInfo(StateSucceeded, "", time.Time{}))
			}
			continue
		}
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
			continue
		}
		info.State = StateDead
		info.DeadAt = time.Now()
		if h := c.cfg.Hooks.OnDead; h != nil {
			h(info)
		} else if c.cfg.OnDead != nil {
			c.cfg.OnDead(info) // backward compatibility
		}
	}
}

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
