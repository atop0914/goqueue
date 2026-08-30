// Package redis provides a distributed Queue backend for goqueue backed by
// Redis (github.com/redis/go-redis/v9).
//
// All lifecycle transitions (unique-key claim, ready-queue claim, ack, nack,
// retry/DLQ routing) run inside Lua scripts, so any number of producers and
// consumers — across processes — observe atomic state changes. Unlike the
// embedded SQLite backend, a Redis store can safely be shared by several
// application instances. Tests use miniredis; no Docker or real server is
// required.
//
// Data model per queue (prefix defaults to "goqueue:<queue>"):
//
//	<prefix>:ready     ZSET  member="<seq>|<id>" score=run_after_ms*1000-priority
//	<prefix>:running   SET   of currently claimed job IDs
//	<prefix>:dead      ZSET  member=<id> score=dead_at_ms
//	<prefix>:unique    HASH  unique_key -> holding job ID
//	<prefix>:seq       counter for insertion-order tie-breaks
//	<prefix>:done      counter of acked jobs
//	<prefix>:job:<id>  HASH  job metadata + payload
//
// Scheduling order matches the memory and SQLite backends:
// (run_after ASC, priority DESC, seq ASC). Ready scores pack run_after at
// millisecond resolution and a priority clamped to ±499 into one integer
// (higher priority = smaller score = earlier); zero-padded seq inside the
// member breaks remaining ties lexicographically, which is exactly seq ASC.
//
// As with SQLite: Close only stops claiming new jobs (Dequeue returns
// ErrQueueClosed) and leaves all data intact; CloseClient additionally closes
// the Redis connection, after which Enqueue fails as well. On Open, jobs left
// in the running set by a previous process are returned to the ready queue
// with their attempt count preserved (at-least-once semantics).
package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	goqueue "github.com/atop0914/goqueue"
)

// Sentinel errors re-exported so callers of this package do not need the
// root import just for error checks.
var (
	ErrQueueClosed = goqueue.ErrQueueClosed
	ErrJobExists   = goqueue.ErrJobExists
	ErrJobNotFound = goqueue.ErrJobNotFound
)

const (
	// pollInterval is how often an idle Dequeue re-checks Redis for due
	// jobs. Redis has no blocking pop for scored sets that also records the
	// claim atomically, so the store polls — the same design as the SQLite
	// backend — and local arrivals wake pollers early via notify.
	pollInterval = 25 * time.Millisecond

	// opTimeout bounds single administrative calls (Dead, Stats, Open's
	// recovery scan). Dequeue claims inherit the caller's context instead.
	opTimeout = 5 * time.Second

	// Priority clamp for score packing: run_after occupies the top bits at
	// millisecond resolution (x1000), leaving ±499 for priority without
	// overflowing float64-exact integers (< 2^53).
	maxPrio = 499
	minPrio = -499
)

// Lua scripts. Every state transition is a single script so concurrent
// callers can never interleave between read and write of the same key.

const luaEnqueue = `
local ukey = tostring(ARGV[8])
if ukey ~= '' and redis.call('HSETNX', KEYS[2], ukey, ARGV[1]) == 0 then
  return 0
end
local seq = redis.call('INCR', KEYS[3])
local member = string.format('%019d', seq) .. '|' .. ARGV[1]
local score = tonumber(ARGV[9]) * 1000 - tonumber(ARGV[5])
redis.call('HSET', KEYS[4],
  'id', ARGV[1], 'type', ARGV[2], 'payload', ARGV[3],
  'priority', ARGV[4], 'max_retry', ARGV[6], 'timeout', ARGV[7],
  'ukey', ukey, 'attempts', '0', 'seq', tostring(seq),
  'enqueued_at', ARGV[10], 'last_error', '', 'dead_at', '0')
redis.call('ZADD', KEYS[1], score, member)
return 1
`

// KEYS: ready, running, job-key-prefix. ARGV: max due score.
// Returns nil when nothing is due, else the claimed job fields.
const luaClaim = `
local m = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 1)
if #m == 0 then return nil end
local member = m[1]
local pos = string.find(member, '|', 1, true)
local id = string.sub(member, pos + 1)
local jk = KEYS[3] .. id
local attempts = redis.call('HINCRBY', jk, 'attempts', 1)
redis.call('SADD', KEYS[2], id)
redis.call('ZREM', KEYS[1], member)
return {
  id, tostring(attempts),
  redis.call('HGET', jk, 'type') or '',
  redis.call('HGET', jk, 'payload') or '',
  redis.call('HGET', jk, 'priority') or '0',
  redis.call('HGET', jk, 'max_retry') or '0',
  redis.call('HGET', jk, 'timeout') or '0',
  redis.call('HGET', jk, 'enqueued_at') or '0'
}
`

// KEYS: running, job:<id>, unique, done. ARGV: id.
// Returns 0 when the job was not claimed (unknown), 1 on success.
const luaAck = `
if redis.call('SREM', KEYS[1], ARGV[1]) == 0 then return 0 end
local ukey = redis.call('HGET', KEYS[2], 'ukey')
if ukey and ukey ~= '' and redis.call('HGET', KEYS[3], ukey) == ARGV[1] then
  redis.call('HDEL', KEYS[3], ukey)
end
redis.call('INCR', KEYS[4])
redis.call('DEL', KEYS[2])
return 1
`

// KEYS: running, ready, dead, job:<id>, unique.
// ARGV: id, retryable('0'/'1'), retry_base_ms, now_ms, now_ns, last_error.
// Returns 0 when the job was not claimed, 1 when re-queued for retry,
// 2 when moved to the dead-letter set.
const luaNack = `
if redis.call('SREM', KEYS[1], ARGV[1]) == 0 then return 0 end
local jk = KEYS[4]
redis.call('HSET', jk, 'last_error', ARGV[6])
local attempts = tonumber(redis.call('HGET', jk, 'attempts') or '0')
local maxRetry = tonumber(redis.call('HGET', jk, 'max_retry') or '0')
if ARGV[2] == '1' and attempts <= maxRetry then
  local prio = tonumber(redis.call('HGET', jk, 'priority') or '0')
  if prio > 499 then prio = 499 elseif prio < -499 then prio = -499 end
  local seq = tonumber(redis.call('HGET', jk, 'seq') or '0')
  local member = string.format('%019d', seq) .. '|' .. ARGV[1]
  redis.call('ZADD', KEYS[2], tonumber(ARGV[3]) * 1000 - prio, member)
  return 1
end
redis.call('HSET', jk, 'dead_at', ARGV[5])
local ukey = redis.call('HGET', jk, 'ukey')
if ukey and ukey ~= '' and redis.call('HGET', KEYS[5], ukey) == ARGV[1] then
  redis.call('HDEL', KEYS[5], ukey)
end
redis.call('ZADD', KEYS[3], tonumber(ARGV[4]), ARGV[1])
return 2
`

// Crash recovery: move one running job back to the ready queue, due
// immediately, attempts preserved. KEYS: ready, running, job:<id>.
// ARGV: id, now_ms. Returns 0 when the id was not in the running set.
const luaRequeue = `
if redis.call('SREM', KEYS[2], ARGV[1]) == 0 then return 0 end
local prio = tonumber(redis.call('HGET', KEYS[3], 'priority') or '0')
if prio > 499 then prio = 499 elseif prio < -499 then prio = -499 end
local seq = tonumber(redis.call('HGET', KEYS[3], 'seq') or '0')
local member = string.format('%019d', seq) .. '|' .. ARGV[1]
redis.call('ZADD', KEYS[1], tonumber(ARGV[2]) * 1000 - prio, member)
return 1
`

// KEYS: dead, ready, job:<id>, unique. ARGV: id, now_ms.
// Returns 0 when the id is not in the dead set, 2 when the unique key is
// held by another job (job stays dead), 1 on success. Resets attempts,
// clears last_error/dead_at and makes the job due immediately (score base
// 0 minus priority, matching the enqueue scoring); the unique key (if any)
// is re-claimed atomically via HSETNX.
const luaRequeueDead = `
if redis.call('ZREM', KEYS[1], ARGV[1]) == 0 then return 0 end
local jk = KEYS[3]
local ukey = redis.call('HGET', jk, 'ukey') or ''
if ukey ~= '' and redis.call('HSETNX', KEYS[4], ukey, ARGV[1]) == 0 then
  redis.call('ZADD', KEYS[1], tonumber(ARGV[2]), ARGV[1])
  return 2
end
redis.call('HSET', jk, 'attempts', '0', 'last_error', '', 'dead_at', '0')
local prio = tonumber(redis.call('HGET', jk, 'priority') or '0')
if prio > 499 then prio = 499 elseif prio < -499 then prio = -499 end
local seq = tonumber(redis.call('HGET', jk, 'seq') or '0')
local member = string.format('%019d', seq) .. '|' .. ARGV[1]
redis.call('ZADD', KEYS[2], -prio, member)
return 1
`

// KEYS: ready, dead, unique. ARGV: job-key-prefix, dead_flag.
// Deletes all ready members (and, with the flag set, all dead members),
// releasing the unique keys of purged jobs. Running jobs are untouched.
// Returns the number of jobs removed.
const luaPurge = `
local n = 0
local members = redis.call('ZRANGE', KEYS[1], 0, -1)
for _, m in ipairs(members) do
  local pos = string.find(m, '|', 1, true)
  local id = string.sub(m, pos + 1)
  local jk = ARGV[1] .. id
  local ukey = redis.call('HGET', jk, 'ukey') or ''
  if ukey ~= '' and redis.call('HGET', KEYS[3], ukey) == id then
    redis.call('HDEL', KEYS[3], ukey)
  end
  redis.call('DEL', jk)
end
n = n + #members
redis.call('DEL', KEYS[1])
if ARGV[2] == '1' then
  local dead = redis.call('ZRANGE', KEYS[2], 0, -1)
  for _, id in ipairs(dead) do
    local jk = ARGV[1] .. id
    local ukey = redis.call('HGET', jk, 'ukey') or ''
    if ukey ~= '' and redis.call('HGET', KEYS[3], ukey) == id then
      redis.call('HDEL', KEYS[3], ukey)
    end
    redis.call('DEL', jk)
  end
  n = n + #dead
  redis.call('DEL', KEYS[2])
end
return n
`

var (
	enqueueScript = redis.NewScript(luaEnqueue)
	claimScript   = redis.NewScript(luaClaim)
	ackScript     = redis.NewScript(luaAck)
	nackScript    = redis.NewScript(luaNack)
	requeueScript = redis.NewScript(luaRequeue)

	purgeScript       = redis.NewScript(luaPurge)
	requeueDeadScript = redis.NewScript(luaRequeueDead)
)

// Option configures Open.
type Option func(*options)

type options struct {
	queue    string
	password string
	db       int
	client   *redis.Client
}

// WithQueue selects the key namespace slice. Multiple independent queues can
// share one Redis database. Defaults to "default".
func WithQueue(name string) Option {
	return func(o *options) { o.queue = name }
}

// WithPassword sets the AUTH password (CONNECTIONS: go-redis Options).
func WithPassword(pw string) Option {
	return func(o *options) { o.password = pw }
}

// WithDB selects the logical Redis database index (default 0).
func WithDB(db int) Option {
	return func(o *options) { o.db = db }
}

// WithClient supplies a pre-built client instead of constructing one from
// addr/password/db. CloseClient will close it.
func WithClient(c *redis.Client) Option {
	return func(o *options) { o.client = c }
}

// Store is a Redis-backed implementation of goqueue.Queue.
type Store struct {
	client *redis.Client
	prefix string // "<ns>:<queue>", e.g. "goqueue:default"

	closed   atomic.Bool // Close: stop claiming, wake waiters
	connDown atomic.Bool // CloseClient: connection closed, Enqueue fails too

	// paused stops Dequeue from claiming jobs (admin Pause). Local runtime
	// flag, deliberately NOT shared through Redis: pausing one consumer
	// must not pause independent consumers of the same queue. See admin.go.
	paused atomic.Bool

	// notify wakes idle Dequeue pollers on local arrivals; buffer of 1
	// coalesces.
	notify chan struct{}
}

var (
	_ goqueue.Queue         = (*Store)(nil)
	_ goqueue.LenAwareQueue = (*Store)(nil)
)

// Open connects to Redis at addr, verifies reachability with a PING and
// recovers jobs stranded in the running set by a previous process (returned
// to ready immediately, attempt counts preserved).
func Open(addr string, opts ...Option) (*Store, error) {
	o := options{queue: "default"}
	for _, f := range opts {
		f(&o)
	}
	client := o.client
	if client == nil {
		client = redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: o.password,
			DB:       o.db,
		})
	}
	s := &Store{
		client: client,
		prefix: "goqueue:" + o.queue,
		notify: make(chan struct{}, 1),
	}
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("goqueue/redis: ping %s: %w", addr, err)
	}
	if err := s.recoverRunning(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}
	return s, nil
}

// Ping verifies the underlying Redis server is reachable.
func (s *Store) Ping(ctx context.Context) error { return s.client.Ping(ctx).Err() }

// Enqueue implements goqueue.Queue. The unique-key claim and the ready-queue
// insert happen inside one Lua script, so a duplicate unique job can never
// slip in between check and set.
func (s *Store) Enqueue(ctx context.Context, job goqueue.Job) (string, error) {
	if s.connDown.Load() {
		return "", errors.New("goqueue/redis: store is closed")
	}
	id := job.ID
	if id == "" {
		id = newID()
	}
	maxRetry := job.MaxRetry
	if maxRetry == 0 {
		maxRetry = goqueue.DefaultMaxRetry
	}
	// Zero RunAfter maps to the constant epoch 0 ("always due"), mirroring
	// the SQLite backend. A past RunAfter is equally due.
	runAfterMs := int64(0)
	if !job.RunAfter.IsZero() {
		runAfterMs = job.RunAfter.UnixMilli()
		if runAfterMs < 0 {
			runAfterMs = 0
		}
	}
	enqueuedAt := time.Now().UnixNano()

	ok, err := enqueueScript.Run(ctx, s.client,
		[]string{s.kReady(), s.kUnique(), s.kSeq(), s.kJob(id)},
		id, job.Type, string(job.Payload), job.Priority, clampPrio(job.Priority),
		maxRetry, int64(job.Timeout), job.UniqueKey, runAfterMs, enqueuedAt,
	).Int()
	if err != nil {
		return "", fmt.Errorf("goqueue/redis: enqueue: %w", err)
	}
	if ok == 0 {
		return "", goqueue.ErrJobExists
	}
	s.signal()
	return id, nil
}

// Dequeue implements goqueue.Queue. Idle callers poll on a short ticker;
// local arrivals and closes wake them early through notify.
func (s *Store) Dequeue(ctx context.Context) (*goqueue.DequeuedJob, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if s.closed.Load() {
			return nil, goqueue.ErrQueueClosed
		}
		// While paused, no jobs are claimed; pollers keep waiting until
		// Resume lifts the pause (or the caller's context expires).
		var dj *goqueue.DequeuedJob
		var err error
		if !s.paused.Load() {
			dj, err = s.claim(ctx)
			if err != nil {
				return nil, err
			}
		}
		if dj != nil {
			return dj, nil
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-s.notify:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// claim atomically moves the highest-ranked due job from ready to running.
// Returns (nil, nil) when nothing is due.
func (s *Store) claim(ctx context.Context) (*goqueue.DequeuedJob, error) {
	cctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	maxDue := time.Now().UnixMilli()*1000 + maxPrio
	res, err := claimScript.Run(cctx, s.client,
		[]string{s.kReady(), s.kRunning(), s.kJobPrefix()}, maxDue).Slice()
	if errors.Is(err, redis.Nil) {
		return nil, nil // nothing due
	}
	if err != nil {
		return nil, fmt.Errorf("goqueue/redis: claim: %w", err)
	}
	fields := res
	if len(fields) < 8 {
		return nil, fmt.Errorf("goqueue/redis: claim: unexpected reply %v", res)
	}
	str := make([]string, len(fields))
	for i, f := range fields {
		sv, _ := f.(string)
		str[i] = sv
	}
	id := str[0]
	attempts, err := strconv.Atoi(str[1])
	if err != nil {
		return nil, fmt.Errorf("goqueue/redis: claim: attempts %q: %w", str[1], err)
	}
	priority, err := strconv.Atoi(str[4])
	if err != nil {
		return nil, fmt.Errorf("goqueue/redis: claim: priority %q: %w", str[4], err)
	}
	maxRetry, err := strconv.Atoi(str[5])
	if err != nil {
		return nil, fmt.Errorf("goqueue/redis: claim: max_retry %q: %w", str[5], err)
	}
	timeoutNs, err := strconv.ParseInt(str[6], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("goqueue/redis: claim: timeout %q: %w", str[6], err)
	}
	enqNs, err := strconv.ParseInt(str[7], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("goqueue/redis: claim: enqueued_at %q: %w", str[7], err)
	}
	return &goqueue.DequeuedJob{
		ID:         id,
		Type:       str[2],
		Payload:    []byte(str[3]),
		Priority:   priority,
		Attempt:    attempts,
		MaxRetry:   maxRetry,
		Timeout:    time.Duration(timeoutNs),
		DequeuedAt: time.Now(),
		EnqueuedAt: time.Unix(0, enqNs),
	}, nil
}

// Ack implements goqueue.Queue. Releasing the unique key here is what frees
// a successful job's uniqueness slot; the done counter feeds Stats.
func (s *Store) Ack(ctx context.Context, id string) error {
	n, err := ackScript.Run(ctx, s.client,
		[]string{s.kRunning(), s.kJob(id), s.kUnique(), s.kDone()}, id).Int()
	if err != nil {
		return fmt.Errorf("goqueue/redis: ack: %w", err)
	}
	if n == 0 {
		return goqueue.ErrJobNotFound
	}
	return nil
}

// Nack implements goqueue.Queue. Retryable failures within budget re-enter
// the ready queue after delay (unique key stays held across retries);
// exhausted or non-retryable ones move to the dead ZSET and release the key.
func (s *Store) Nack(ctx context.Context, id string, err error, retryable bool, delay time.Duration) error {
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	now := time.Now()
	retryBaseMs := now.UnixMilli()
	if delay > 0 {
		retryBaseMs = now.Add(delay).UnixMilli()
	}
	r := "0"
	if retryable {
		r = "1"
	}
	n, err := nackScript.Run(ctx, s.client,
		[]string{s.kRunning(), s.kReady(), s.kDead(), s.kJob(id), s.kUnique()},
		id, r, retryBaseMs, now.UnixMilli(), now.UnixNano(), msg,
	).Int()
	if err != nil {
		return fmt.Errorf("goqueue/redis: nack: %w", err)
	}
	if n == 0 {
		return goqueue.ErrJobNotFound
	}
	return nil
}

// Dead implements goqueue.Queue, ordered like the other backends: death time
// ascending (the dead ZSET's score), ID tie-break (member order).
func (s *Store) Dead() []goqueue.JobInfo {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	ids, err := s.client.ZRange(ctx, s.kDead(), 0, -1).Result()
	if err != nil {
		return []goqueue.JobInfo{}
	}
	out := []goqueue.JobInfo{}
	for _, id := range ids {
		fields, err := s.client.HGetAll(ctx, s.kJob(id)).Result()
		if err != nil || len(fields) == 0 {
			continue
		}
		info := goqueue.JobInfo{
			ID:        id,
			Type:      fields["type"],
			State:     goqueue.StateDead,
			LastError: fields["last_error"],
		}
		info.Attempts, _ = strconv.Atoi(fields["attempts"])
		info.MaxRetry, _ = strconv.Atoi(fields["max_retry"])
		info.Priority, _ = strconv.Atoi(fields["priority"])
		if ns, err := strconv.ParseInt(fields["enqueued_at"], 10, 64); err == nil {
			info.EnqueuedAt = time.Unix(0, ns)
		}
		if ns, err := strconv.ParseInt(fields["dead_at"], 10, 64); err == nil {
			info.DeadAt = time.Unix(0, ns)
		}
		out = append(out, info)
	}
	return out
}

// Len implements goqueue.Queue: the ready ZSET size, including delayed and
// awaiting-retry jobs — the same population the other backends report.
func (s *Store) Len() int { return s.LenContext(context.Background()) }

// LenContext returns the ready-job count, honoring ctx. Used by the client's
// drain probe: on an indeterminate answer it conservatively reports "work may
// remain" so a Shutdown loop keeps retrying instead of stopping early.
func (s *Store) LenContext(ctx context.Context) int {
	if ctx.Err() != nil {
		return 1 // unknown: conservatively "work may remain"
	}
	n, err := s.client.ZCard(ctx, s.kReady()).Result()
	if err != nil || ctx.Err() != nil {
		return 1 // indeterminate: keep the drain loop going
	}
	return int(n)
}

// Stats reports counts per lifecycle stage: pending (ready, including
// delayed and awaiting-retry), running, succeeded (acked) and dead.
func (s *Store) Stats() (pending, running, succeeded, dead int) {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	pipe := s.client.Pipeline()
	pCmd := pipe.ZCard(ctx, s.kReady())
	rCmd := pipe.SCard(ctx, s.kRunning())
	sCmd := pipe.Get(ctx, s.kDone())
	dCmd := pipe.ZCard(ctx, s.kDead())
	_, _ = pipe.Exec(ctx)
	pending = int(pCmd.Val())
	running = int(rCmd.Val())
	dead = int(dCmd.Val())
	if n, err := sCmd.Int(); err == nil {
		succeeded = n
	}
	return pending, running, succeeded, dead
}

// Close implements goqueue.Queue: the store stops claiming jobs and blocked
// Dequeue calls return ErrQueueClosed. All data stays in Redis — call
// CloseClient to also release the connection.
func (s *Store) Close() error {
	s.closed.Store(true)
	s.signal()
	return nil
}

// CloseClient additionally closes the Redis connection, after which Enqueue
// fails as well. Safe to call multiple times and after Close.
func (s *Store) CloseClient() error {
	s.closed.Store(true)
	s.signal()
	if !s.connDown.CompareAndSwap(false, true) {
		return nil
	}
	return s.client.Close()
}

// ---- internals ----

func (s *Store) kReady() string        { return s.prefix + ":ready" }
func (s *Store) kRunning() string      { return s.prefix + ":running" }
func (s *Store) kDead() string         { return s.prefix + ":dead" }
func (s *Store) kUnique() string       { return s.prefix + ":unique" }
func (s *Store) kSeq() string          { return s.prefix + ":seq" }
func (s *Store) kDone() string         { return s.prefix + ":done" }
func (s *Store) kJobPrefix() string    { return s.prefix + ":job:" }
func (s *Store) kJob(id string) string { return s.prefix + ":job:" + id }

// recoverRunning returns jobs stranded in the running set to the ready queue
// (due immediately, attempts preserved). Restarts are rare, so one script
// call per stranded job is fine.
func (s *Store) recoverRunning(ctx context.Context) error {
	ids, err := s.client.SMembers(ctx, s.kRunning()).Result()
	if err != nil {
		return fmt.Errorf("goqueue/redis: recovery scan: %w", err)
	}
	nowMs := time.Now().UnixMilli()
	for _, id := range ids {
		if _, err := requeueScript.Run(ctx, s.client,
			[]string{s.kReady(), s.kRunning(), s.kJob(id)}, id, nowMs).Int(); err != nil {
			return fmt.Errorf("goqueue/redis: recovery of %s: %w", id, err)
		}
	}
	return nil
}

func (s *Store) signal() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func clampPrio(p int) int {
	if p > maxPrio {
		return maxPrio
	}
	if p < minPrio {
		return minPrio
	}
	return p
}

// newID mirrors the core generator: millisecond timestamp + random bytes,
// hex encoded. Kept local so this subpackage depends only on the public API
// of the root module.
func newID() string {
	var b [16]byte
	ts := uint64(time.Now().UnixMilli())
	for i := 0; i < 8; i++ {
		b[i] = byte(ts >> (56 - 8*i))
	}
	if _, err := rand.Read(b[8:]); err != nil {
		panic("goqueue/redis: crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
