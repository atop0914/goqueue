package goqueue

import (
	"context"
	"fmt"
	"sync"
)

// ResultHandler is a handler that produces a typed result alongside an error
// status. Register with RegisterWithResult to make the result queryable
// through Task.
type ResultHandler[T any] func(ctx context.Context, payload []byte) (T, error)

// RegisterWithResult binds a typed result handler to a job type. The handler
// runs like a normal Handler but its return value is stored so callers can
// retrieve it later with Task.
//
// The wrapper handler behaves exactly like a plain Handler for retries,
// timeouts, panics and dead-lettering: a returned error (or panic) counts as
// a failed attempt and follows the normal retry/DLQ path. The stored result
// is always the outcome of the latest attempt — a failed attempt stores an
// error result, a later successful attempt overwrites it with the value.
//
// T is inferred from the handler argument:
//
//	RegisterWithResult(c, "sum", func(ctx context.Context, p []byte) (int, error) { ... })
func RegisterWithResult[T any](c *Client, taskType string, h ResultHandler[T]) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.types[taskType] = func(ctx context.Context, payload []byte) (err error) {
		var v T
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic in result handler for %q: %v", taskType, r)
			}
		}()
		v, err = h(ctx, payload)
		id, ok := jobIDFromContext(ctx)
		if ok {
			c.storeResult(id, v, err)
		}
		return err
	}
}

// Task returns a handle to query the typed result of a job. The handle is
// valid even before the job finishes (and even before it is started): Get
// blocks until a result exists or the context is done. Only jobs registered
// with RegisterWithResult have results; for other jobs the handle never
// becomes ready (Get returns ctx.Err()).
//
// The result follows the job lifecycle: it is published after every attempt,
// so on retries callers observing late may see the final value. A type
// mismatch (querying with the wrong T) is reported as an error by Get.
func Task[T any](c *Client, id string) *TaskHandle[T] {
	slot, _ := c.results.LoadOrStore(id, newResultSlot())
	return &TaskHandle[T]{slot: slot.(*resultSlot)}
}

// TryGetResult is the non-blocking convenience form:
//
//	value, err, ok := TryGetResult[T](c, id)
//
// ok is false when the job has not produced a result yet (still running,
// queued, or not a typed-result job).
func TryGetResult[T any](c *Client, id string) (value T, err error, ok bool) {
	return Task[T](c, id).TryGet()
}

// TaskHandle lets callers await the typed result of a single job. It is
// safe for concurrent use by multiple goroutines.
type TaskHandle[T any] struct {
	slot *resultSlot
}

// Done reports whether a result is already available (the job finished at
// least one attempt). It never blocks.
func (h *TaskHandle[T]) Done() bool {
	_, _, ok := h.slot.get()
	return ok
}

// TryGet returns the current result without blocking. ok is false when no
// result exists yet. When ok is true, err mirrors the handler's error: nil
// on success, the handler error on a failed attempt.
func (h *TaskHandle[T]) TryGet() (value T, err error, ok bool) {
	anyVal, anyErr, ready := h.slot.get()
	if !ready {
		var zero T
		return zero, nil, false
	}
	v, typeOK := anyVal.(T)
	if !typeOK {
		var zero T
		return zero, fmt.Errorf("goqueue: result type mismatch: stored %T, queried %T", anyVal, zero), true
	}
	return v, anyErr, true
}

// Get blocks until a result is available (of any attempt) or ctx is done.
// On ctx expiry it returns ctx.Err(). On a type mismatch it returns a
// descriptive error.
func (h *TaskHandle[T]) Get(ctx context.Context) (T, error) {
	select {
	case <-h.slot.done:
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
	val, err, _ := h.TryGet()
	return val, err
}

// resultSlot stores the latest result of a job and signals readiness. The
// done channel is closed on the first published result; later attempts
// update the value without closing again, so late readers always observe
// the most recent outcome.
type resultSlot struct {
	mu    sync.Mutex
	done  chan struct{}
	value any
	err   error
	ready bool
}

func newResultSlot() *resultSlot {
	return &resultSlot{done: make(chan struct{})}
}

func (s *resultSlot) set(v any, err error) {
	s.mu.Lock()
	s.value, s.err = v, err
	if !s.ready {
		s.ready = true
		close(s.done)
	}
	s.mu.Unlock()
}

func (s *resultSlot) get() (any, error, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.value, s.err, s.ready
}

// storeResult publishes the typed result of a job. Safe for concurrent use:
// the slot is created on first sight and updated under its mutex.
func (c *Client) storeResult(id string, v any, err error) {
	if id == "" {
		return
	}
	slot, _ := c.results.LoadOrStore(id, newResultSlot())
	slot.(*resultSlot).set(v, err)
}

// jobIDKey is the context key carrying the current job ID through the
// handler invocation. Result handlers read it to publish their outcome.
type jobIDKey struct{}

func jobIDFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(jobIDKey{}).(string)
	return id, ok
}