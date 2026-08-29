// Package tracing adds OpenTelemetry spans to goqueue job processing. It is
// an optional extension in its own module: importers who do not need
// distributed tracing never pull the OTel dependencies into the core.
//
// The integration mirrors the metrics package - lifecycle hooks plus one
// core option:
//
//	tracer := tracing.NewTracer(tracing.Options{ServiceName: "workers"})
//	reg := tracing.NewRegistry()
//	defer reg.Close()
//	client := goqueue.New(
//	    goqueue.WithHooks(tracer.Hooks(reg)),
//	    goqueue.WithContextDecorator(tracer.ContextDecorator()),
//	)
//
// With the hooks wired, the end-to-end "goqueue.process" span starts when
// a job is enqueued, stays open across retries and waits, and is finished
// by the worker when the job finally succeeds or dies - so one span covers
// the whole life of the job, with retry and dead-letter events attached.
// Producer-side "goqueue.enqueue" spans (StartJobSpan) join an incoming
// request's trace and, via WithJobContext + ContextDecorator, become the
// parent context of handler invocations so handler-emitted spans nest
// correctly.
package tracing

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/atop0914/goqueue"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Framework is the semantic-convention component name recorded on every
// span and event produced by this package.
const Framework = "goqueue"

// contextKey stores the per-job end-to-end span inside a job context.
type contextKey struct{}

// jobCtxKey marks the producer context stored inside the handler context
// by WithJobContext (see ContextDecorator).
type jobCtxKey struct{}

// Options tunes span naming and resource attributes.
type Options struct {
	// ServiceName is recorded as the service.name span attribute. Empty
	// omits the attribute (the SDK's resource usually supplies it).
	ServiceName string

	// TracerName selects the instrumentation-scope name for the tracer.
	// Empty uses "github.com/atop0914/goqueue/obs/tracing".
	TracerName string
}

// Tracer produces goqueue lifecycle hooks and a context decorator that
// create and complete OpenTelemetry spans. All methods are safe for
// concurrent use; the hooks fire on whatever goroutine owns the lifecycle
// event, exactly like any goqueue hook.
type Tracer struct {
	tracer trace.Tracer
	opts   Options
}

// NewTracer builds a Tracer on the global tracer provider. Configure your
// SDK with otel.SetTracerProvider before use; with the default no-op
// provider every span is a no-op, so wiring this package is always safe.
func NewTracer(opts Options) *Tracer {
	name := opts.TracerName
	if name == "" {
		name = "github.com/atop0914/goqueue/obs/tracing"
	}
	return &Tracer{tracer: otel.Tracer(name), opts: opts}
}

// jobState is the per-job state shared between the enqueue-side hook (which
// creates the span) and the worker-side hooks (which record attempts and
// finish the span).
type jobState struct {
	span      trace.Span
	attempts  int
	lastTouch time.Time
}

// Registry tracks the end-to-end span of every in-flight job so worker
// hooks can find and finish the span started at enqueue time. Entries are
// removed when the span ends; a background sweeper also reaps entries that
// have seen no activity for an hour (a job whose worker process crashed
// mid-run would otherwise leak). Stop the sweeper with Close.
type Registry struct {
	mu   sync.Mutex
	jobs map[string]*jobState
	// ctxs remembers the producer context of a job (attached via
	// StartJobSpan on the Enqueue call). The queue cannot carry a Go
	// context across the enqueue/dequeue boundary, so the Registry does:
	// the ContextDecorator looks the context up by job ID at execution
	// time. Entries live exactly as long as their jobs entry.
	ctxs    map[string]context.Context
	nowFunc func() time.Time
	stop    chan struct{}
	done    chan struct{}
}

// NewRegistry starts the background sweeper. Call Close when your program
// exits.
func NewRegistry() *Registry {
	r := &Registry{
		jobs:    make(map[string]*jobState),
		ctxs:    make(map[string]context.Context),
		nowFunc: time.Now,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	go r.loop()
	return r
}

// Close stops the background sweeper and drops all tracked spans. Close
// does not end spans: jobs that are still running elsewhere keep their
// trace alive by design.
func (r *Registry) Close() {
	close(r.stop)
	<-r.done
	r.mu.Lock()
	r.jobs = make(map[string]*jobState)
	r.ctxs = make(map[string]context.Context)
	r.mu.Unlock()
}

func (r *Registry) loop() {
	defer close(r.done)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-ticker.C:
			r.reap()
		}
	}
}

// reapAge bounds how long an untouched entry may live. Long enough for any
// realistic retry schedule (MaxInterval caps at 30s by default), short
// enough to not leak.
const reapAge = time.Hour

func (r *Registry) reap() {
	cutoff := r.nowFunc().Add(-reapAge)
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, st := range r.jobs {
		if st.lastTouch.Before(cutoff) {
			delete(r.jobs, id)
			delete(r.ctxs, id)
		}
	}
}

func (t *Tracer) attrs(i goqueue.JobInfo, attempt int) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("messaging.system", Framework),
		attribute.String("goqueue.job.id", i.ID),
		attribute.String("goqueue.job.type", i.Type),
		attribute.Int("goqueue.job.priority", i.Priority),
		attribute.Int("goqueue.job.max_retry", i.MaxRetry),
	}
	if attempt > 0 {
		attrs = append(attrs, attribute.Int("goqueue.job.attempt", attempt))
	}
	if t.opts.ServiceName != "" {
		attrs = append(attrs, attribute.String("service.name", t.opts.ServiceName))
	}
	return attrs
}

// Hooks returns the lifecycle callbacks that create and complete the
// end-to-end "goqueue.process" span:
//
//   - OnEnqueue starts the span and tracks it under the job ID.
//   - OnSuccess marks the span OK, ends it and untracks the job.
//   - OnFailure records the error on every failed attempt; a retriable
//     failure keeps the span open so the trace covers the whole job.
//   - OnRetry bumps the attempt and adds a retry event.
//   - OnDead adds a dead-letter event, ends the span with an error and
//     untracks the job.
//
// Worker-side callbacks are no-ops for jobs they have no span for (hooks
// wired without OnEnqueue, or jobs enqueued before the tracer), so
// partial wiring degrades gracefully instead of panicking.
func (t *Tracer) Hooks(r *Registry) goqueue.Hooks {
	return goqueue.Hooks{
		OnEnqueue: func(i goqueue.JobInfo) {
			if i.ID == "" {
				return
			}
			_, span := t.tracer.Start(context.Background(), Framework+".process", trace.WithAttributes(t.attrs(i, 0)...))
			r.mu.Lock()
			r.jobs[i.ID] = &jobState{span: span, lastTouch: time.Now()}
			r.mu.Unlock()
		},
		OnSuccess: func(i goqueue.JobInfo) {
			r.mu.Lock()
			st, ok := r.jobs[i.ID]
			if ok {
				delete(r.jobs, i.ID)
			}
			r.mu.Unlock()
			if !ok {
				return
			}
			st.span.SetAttributes(t.attrs(i, i.Attempts)...)
			st.span.SetStatus(codes.Ok, "")
			st.span.End()
		},
		OnFailure: func(i goqueue.JobInfo) {
			r.mu.Lock()
			st, ok := r.jobs[i.ID]
			if ok {
				st.lastTouch = time.Now()
			}
			r.mu.Unlock()
			if !ok {
				return
			}
			st.span.RecordError(errors.New(i.LastError), trace.WithAttributes(
				attribute.Int("goqueue.job.attempt", i.Attempts),
			))
		},
		OnRetry: func(i goqueue.JobInfo) {
			r.mu.Lock()
			st, ok := r.jobs[i.ID]
			if ok {
				st.attempts = i.Attempts
				st.lastTouch = time.Now()
			}
			r.mu.Unlock()
			if !ok {
				return
			}
			st.span.AddEvent(Framework+".retry", trace.WithAttributes(
				attribute.Int("goqueue.job.attempt", i.Attempts),
			))
		},
		OnDead: func(i goqueue.JobInfo) {
			r.mu.Lock()
			st, ok := r.jobs[i.ID]
			if ok {
				delete(r.jobs, i.ID)
			}
			r.mu.Unlock()
			if !ok {
				return
			}
			st.span.AddEvent(Framework+".dead_letter", trace.WithAttributes(
				attribute.String("goqueue.job.last_error", i.LastError),
			))
			st.span.SetAttributes(t.attrs(i, i.Attempts)...)
			st.span.SetStatus(codes.Error, "job moved to dead-letter queue")
			st.span.End()
		},
	}
}

// WithJobContext stores a producer-side context (carrying the span created
// by StartJobSpan) inside the context passed to Enqueue, so the span is
// retrievable with FromJobContext. Prefer StartJobSpan, which also
// registers the context for the ContextDecorator path.
func WithJobContext(ctx context.Context, span trace.Span) context.Context {
	withSpan := context.WithValue(ctx, contextKey{}, span)
	// Tag the span-carrying context so the ContextDecorator can recognize
	// it; because withSpan is also the parent, a plain chain lookup of
	// contextKey (FromJobContext, otel tooling) finds the span too.
	return context.WithValue(withSpan, jobCtxKey{}, withSpan)
}

// FromJobContext returns the end-to-end span previously attached with
// WithJobContext, or nil.
func FromJobContext(ctx context.Context) trace.Span {
	if span, ok := ctx.Value(contextKey{}).(trace.Span); ok {
		return span
	}
	return nil
}

// ContextDecorator returns the decorator to register with
// goqueue.WithContextDecorator. Go contexts cannot cross the queue
// boundary (workers run on fresh contexts), so the decorator resolves the
// producer context by job ID (looked up in r): when the job was enqueued
// with a context carrying tracing baggage (see WithJobContext /
// StartJobSpan), handler
// invocations are based on that context and spans emitted inside handlers
// become children of the producer-side span. Jobs without one get the
// worker's plain context.
func (t *Tracer) ContextDecorator(r *Registry) func(ctx context.Context) context.Context {
	return func(ctx context.Context) context.Context {
		id, ok := goqueue.JobIDFromContext(ctx)
		if !ok {
			return ctx
		}
		r.mu.Lock()
		jobCtx := r.ctxs[id]
		r.mu.Unlock()
		if jobCtx == nil {
			return ctx
		}
		return jobCtx
	}
}

// StartJobSpan starts a producer-side "goqueue.enqueue" span bound to the
// producer's ctx (joining an incoming request's trace) and remembers the
// context under the given job ID for the ContextDecorator path. Use it
// around Enqueue, passing the same ID to the job:
//
//	ctx, span := tracer.StartJobSpan(reg, ctx, "emails", emailJobID)
//	_, err := client.Enqueue(ctx, goqueue.Job{ID: emailJobID, Type: "emails"})
//	span.End()
//
// The handler-ctx linkage therefore requires an explicit job ID. Jobs with
// an auto-generated ID are still fully traced by the Hooks path (the
// "goqueue.process" span) - only the handler-side context inheritance is
// limited. The registry entry is released when the job succeeds, dies, or
// is reaped.
func (t *Tracer) StartJobSpan(r *Registry, ctx context.Context, jobType, jobID string) (context.Context, trace.Span) {
	ctx, span := t.tracer.Start(ctx, Framework+".enqueue",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("goqueue.job.type", jobType),
			attribute.String("goqueue.job.id", jobID),
		),
	)
	wrapped := WithJobContext(ctx, span)
	if jobID != "" {
		r.mu.Lock()
		r.ctxs[jobID] = wrapped
		r.mu.Unlock()
	}
	return wrapped, span
}
