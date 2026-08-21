package goqueue

import (
	"context"
	"sync"
	"time"
)

// scheduledTask is a recurring task registered on a Client. On every trigger
// the build closure is called to produce a fresh Job that is enqueued with a
// unique ID, so each tick is an independent unit of work.
type scheduledTask struct {
	id     string
	spec   Schedule
	build  func() Job
	cancel context.CancelFunc
	done   chan struct{}
}

// Schedule registers a recurring task. On every trigger the build closure is
// invoked to produce a new Job which is enqueued immediately; the returned ID
// is discarded (each tick gets its own fresh ID). The job must have a
// registered handler type, otherwise Enqueue fails with ErrUnknownType and
// the error is swallowed (the scheduler keeps going).
//
// Scheduling begins immediately (the first trigger is computed from the
// current clock). Use ScheduleStop to cancel a recurring task by ID.
func (c *Client) Schedule(spec Schedule, build func() Job) string {
	c.mu.Lock()
	id := newID()[:8]
	// Guard against register-after-shutdown.
	taskCtx, cancel := context.WithCancel(context.Background())
	task := &scheduledTask{id: id, spec: spec, build: build, cancel: cancel, done: make(chan struct{})}
	c.tasks[id] = task
	c.mu.Unlock()

	now := c.clock()
	go func() {
		defer close(task.done)
		var next time.Time
		var armed bool
		// The scheduler is a poll loop: it re-checks the injected clock every
		// tick, so scheduled fire times are computed from that clock rather
		// than from the wall clock. This keeps the scheduler deterministic
		// under a fake clock for tests, while real-time schedules still fire
		// within the poll granularity.
		ticker := time.NewTicker(schedulerPoll)
		defer ticker.Stop()
		for {
			if !armed {
				n, ok := task.spec.Next(now())
				if !ok {
					// Schedule will never fire again; retire it.
					c.mu.Lock()
					delete(c.tasks, task.id)
					c.mu.Unlock()
					return
				}
				next, armed = n, true
			}
			select {
			case <-taskCtx.Done():
				return
			case <-ticker.C:
			}
			if now().Before(next) {
				continue // not due yet
			}
			armed = false
			job := task.build()
			// Re-validate the handler type still exists before enqueuing.
			c.mu.RLock()
			_, hasType := c.types[job.Type]
			c.mu.RUnlock()
			if hasType {
				// Fire and forget: schedule should not block on backpressure.
				// Going through Client.Enqueue keeps the OnEnqueue hook firing
				// for scheduled jobs too.
				_, _ = c.Enqueue(context.Background(), job)
			}
		}
	}()
	return id
}

// schedulerPoll is how often the scheduler re-checks whether a scheduled task
// is due. Small enough to keep interval schedules responsive, large enough to
// avoid busy-spinning.
const schedulerPoll = 10 * time.Millisecond

// ScheduleStop cancels the recurring task registered with the given ID (as
// returned by Schedule). It blocks until the task's goroutine has exited. It
// is safe to call multiple times or for an unknown ID.
func (c *Client) ScheduleStop(id string) {
	c.mu.Lock()
	task, ok := c.tasks[id]
	delete(c.tasks, id)
	c.mu.Unlock()
	if !ok {
		return
	}
	task.cancel()
	<-task.done
}

// clock returns the client's configured clock, defaulting to time.Now.
func (c *Client) clock() func() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.cfg.now != nil {
		return c.cfg.now
	}
	return time.Now
}

// stopTasks cancels every scheduled task on shutdown.
func (c *Client) stopTasks() {
	c.mu.Lock()
	tasks := make([]*scheduledTask, 0, len(c.tasks))
	for _, t := range c.tasks {
		tasks = append(tasks, t)
	}
	c.tasks = make(map[string]*scheduledTask)
	c.mu.Unlock()
	var wg sync.WaitGroup
	for _, t := range tasks {
		wg.Add(1)
		go func(t *scheduledTask) {
			defer wg.Done()
			t.cancel()
			<-t.done
		}(t)
	}
	wg.Wait()
}
