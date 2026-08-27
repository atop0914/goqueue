// Package sqlite provides a persistent Queue backend for goqueue backed by
// an embedded SQLite database (via the pure-Go modernc.org/sqlite driver —
// no CGo required).
//
// All state transitions happen inside SQLite transactions, so visibility and
// at-least-once semantics survive process crashes. On Open, jobs left in
// running state by a previous process are returned to pending and retried
// (their attempt count is preserved — the work really was attempted).
//
// Divergence from the in-memory backend: Close only stops handing out new
// jobs (Dequeue returns ErrQueueClosed). Because data persists, late Enqueue
// calls from independent producers remain valid until CloseDB physically
// closes the handle.
package sqlite

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite" // pure-Go SQLite driver, registered as "sqlite"

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
	// stmtTimeout bounds every individual statement. SQLite may block on its
	// internal lock while another goroutine writes; without a bound a lost
	// contention race would hang a worker forever.
	stmtTimeout = 10 * time.Second

	// pollInterval is how often an idle Dequeue re-checks the database for
	// due jobs. SQLite has no native blocking-pop; short polling keeps the
	// code obviously correct. New arrivals wake pollers early via notify.
	pollInterval = 25 * time.Millisecond
)

// Schema. Only three resting states are ever stored: pending (waiting for a
// worker, possibly delayed via run_after), succeeded and dead. Retries go
// straight back to pending with a future run_after, mirroring the in-memory
// backend where a nacked record rejoins the heap. The partial unique index
// on unique_key implements unique-job semantics: the key is held while the
// row is pending/running/retrying and released on Ack or DLQ by clearing the
// column.
const schema = `
CREATE TABLE IF NOT EXISTS jobs (
	id          TEXT PRIMARY KEY,
	type        TEXT NOT NULL,
	payload     BLOB,
	priority    INTEGER NOT NULL DEFAULT 0,
	run_after   INTEGER NOT NULL,
	max_retry   INTEGER NOT NULL DEFAULT 3,
	timeout_ns  INTEGER NOT NULL DEFAULT 0,
	unique_key  TEXT NOT NULL DEFAULT '',
	attempts    INTEGER NOT NULL DEFAULT 0,
	state       INTEGER NOT NULL,
	last_error  TEXT NOT NULL DEFAULT '',
	seq         INTEGER NOT NULL,
	enqueued_at INTEGER NOT NULL,
	dead_at     INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state);
CREATE INDEX IF NOT EXISTS idx_jobs_ready ON jobs(run_after, priority DESC, seq) WHERE state = 0;
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_unique ON jobs(unique_key) WHERE unique_key != '';
`

// Store is a SQLite-backed implementation of goqueue.Queue.
//
// Ready jobs are dequeued in (run_after ASC, priority DESC, seq ASC) order,
// identical to the in-memory backend's scheduling policy.
type Store struct {
	db *sql.DB

	seq atomic.Uint64 // insertion-order tie-breaker

	closed atomic.Bool // Close: stop claiming, wake waiters
	dbDown atomic.Bool // CloseDB: physical close, Enqueue fails too

	// notify wakes idle Dequeue pollers on arrival; buffer of 1 coalesces.
	notify chan struct{}
}

var _ goqueue.Queue = (*Store)(nil)

// Open opens (creating if necessary) the database file and returns a ready
// Store. WAL journaling keeps readers cheap; busy_timeout serializes the
// occasional writer contention instead of failing with SQLITE_BUSY. A single
// connection is used: the pure-Go driver serializes writes anyway, and one
// connection rules out cross-connection lock juggling.
func Open(path string) (*Store, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("goqueue/sqlite: open %s: %w", path, err)
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db, notify: make(chan struct{}, 1)}
	if err := s.init(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) init() error {
	if _, err := s.exec(schema); err != nil {
		return fmt.Errorf("goqueue/sqlite: schema: %w", err)
	}
	// Crash recovery: the previous process died holding running jobs. They
	// were dequeued but never acked/nacked, so under at-least-once they must
	// run again. Attempts stay counted (memory.Nack counts them the same
	// way) and run_after is reset so recovery is immediate.
	if _, err := s.exec(`UPDATE jobs
		SET state = ?, run_after = ?
		WHERE state = ?`,
		int(goqueue.StatePending), time.Now().UnixNano(), int(goqueue.StateRunning)); err != nil {
		return fmt.Errorf("goqueue/sqlite: crash recovery: %w", err)
	}
	return nil
}

// Ping verifies the underlying database is reachable.
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

// Enqueue implements goqueue.Queue.
func (s *Store) Enqueue(_ context.Context, job goqueue.Job) (string, error) {
	if s.dbDown.Load() {
		return "", errors.New("goqueue/sqlite: store is closed")
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
	// the in-memory backend where zero-valued RunAfter sorts identically for
	// all such jobs, letting priority/insertion-order decide. Storing the
	// enqueue timestamp here instead would let FIFO beat priority.
	runAfter := int64(0)
	if !job.RunAfter.IsZero() {
		runAfter = job.RunAfter.UnixNano()
	}
	now := time.Now()

	_, err := s.exec(`INSERT INTO jobs
		(id, type, payload, priority, run_after, max_retry, timeout_ns, unique_key,
		 attempts, state, last_error, seq, enqueued_at, dead_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, '', ?, ?, 0)`,
		id, job.Type, job.Payload, job.Priority, runAfter,
		maxRetry, int64(job.Timeout), job.UniqueKey,
		int(goqueue.StatePending), s.nextSeq(), now.UnixNano())
	if err != nil {
		return "", translate(err)
	}
	s.signal()
	return id, nil
}

// Dequeue implements goqueue.Queue. Idle callers poll on a short ticker;
// arrivals and closes wake them early through notify.
func (s *Store) Dequeue(ctx context.Context) (*goqueue.DequeuedJob, error) {
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if dj, ok := s.claim(); ok {
			return dj, nil
		}
		if s.closed.Load() {
			return nil, ErrQueueClosed
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

// claim picks the highest-priority due job and marks it running inside one
// transaction, so concurrent consumers can never grab the same row.
func (s *Store) claim() (*goqueue.DequeuedJob, bool) {
	var dj *goqueue.DequeuedJob
	err := s.withTx(func(tx *sql.Tx) error {
		if s.closed.Load() {
			return nil
		}
		var (
			id        string
			typ       string
			payload   []byte
			priority  int
			maxRetry  int
			timeoutNs int64
			attempts  int
			enqNanos  int64
		)
		err := tx.QueryRow(`SELECT id, type, payload, priority, max_retry, timeout_ns, attempts, enqueued_at
			FROM jobs
			WHERE state = ? AND run_after <= ?
			ORDER BY run_after ASC, priority DESC, seq ASC
			LIMIT 1`,
			int(goqueue.StatePending), time.Now().UnixNano()).
			Scan(&id, &typ, &payload, &priority, &maxRetry, &timeoutNs, &attempts, &enqNanos)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE jobs SET state = ?, attempts = ? WHERE id = ?`,
			int(goqueue.StateRunning), attempts+1, id); err != nil {
			return err
		}
		dj = &goqueue.DequeuedJob{
			ID:         id,
			Type:       typ,
			Payload:    payload,
			Priority:   priority,
			Attempt:    attempts + 1,
			MaxRetry:   maxRetry,
			Timeout:    time.Duration(timeoutNs),
			DequeuedAt: time.Now(),
			EnqueuedAt: time.Unix(0, enqNanos),
		}
		return nil
	})
	if err != nil || dj == nil {
		return nil, false
	}
	return dj, true
}

// Ack implements goqueue.Queue. Clearing unique_key here is what releases a
// successful job's uniqueness slot (the partial unique index only covers
// non-empty keys).
func (s *Store) Ack(_ context.Context, id string) error {
	n, err := s.execRows(`UPDATE jobs
		SET state = ?, last_error = '', unique_key = ''
		WHERE id = ? AND state = ?`,
		int(goqueue.StateSucceeded), id, int(goqueue.StateRunning))
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrJobNotFound
	}
	return nil
}

// Nack implements goqueue.Queue. Retryable failures within budget push
// run_after forward by delay and return the job to pending (unique key stays
// held across retries); exhausted or non-retryable ones move to the DLQ and
// release the key.
func (s *Store) Nack(_ context.Context, id string, err error, retryable bool, delay time.Duration) error {
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	return s.withTx(func(tx *sql.Tx) error {
		var attempts, maxRetry int
		var ukey string
		e := tx.QueryRow(`SELECT attempts, max_retry, unique_key FROM jobs WHERE id = ? AND state = ?`,
			id, int(goqueue.StateRunning)).Scan(&attempts, &maxRetry, &ukey)
		if errors.Is(e, sql.ErrNoRows) {
			return ErrJobNotFound
		}
		if e != nil {
			return e
		}
		if retryable && attempts <= maxRetry {
			runAfter := time.Now().UnixNano()
			if delay > 0 {
				runAfter = time.Now().Add(delay).UnixNano()
			}
			_, e = tx.Exec(`UPDATE jobs SET state = ?, last_error = ?, run_after = ? WHERE id = ?`,
				int(goqueue.StatePending), msg, runAfter, id)
			return e
		}
		if ukey != "" {
			if _, e = tx.Exec(`UPDATE jobs SET unique_key = '' WHERE id = ?`, id); e != nil {
				return e
			}
		}
		_, e = tx.Exec(`UPDATE jobs SET state = ?, last_error = ?, dead_at = ? WHERE id = ?`,
			int(goqueue.StateDead), msg, time.Now().UnixNano(), id)
		return e
	})
}

// Dead implements goqueue.Queue, ordered like the memory backend: death time
// ascending, ID tie-break.
func (s *Store) Dead() []goqueue.JobInfo {
	rows, err := s.db.Query(`SELECT id, type, attempts, max_retry, priority, last_error, enqueued_at, dead_at
		FROM jobs WHERE state = ?
		ORDER BY dead_at ASC, id ASC`, int(goqueue.StateDead))
	if err != nil {
		return []goqueue.JobInfo{}
	}
	defer rows.Close()
	out := []goqueue.JobInfo{}
	for rows.Next() {
		var info goqueue.JobInfo
		var enq, dead int64
		if err := rows.Scan(&info.ID, &info.Type, &info.Attempts, &info.MaxRetry,
			&info.Priority, &info.LastError, &enq, &dead); err != nil {
			continue
		}
		info.State = goqueue.StateDead
		info.EnqueuedAt = time.Unix(0, enq)
		info.DeadAt = time.Unix(0, dead)
		out = append(out, info)
	}
	return out
}

// Len implements goqueue.Queue: all pending rows, including delayed and
// awaiting-retry ones — the same population the in-memory heap Len reports.
func (s *Store) Len() int {
	return s.queryInt(`SELECT COUNT(*) FROM jobs WHERE state = ?`, int(goqueue.StatePending))
}

// Stats reports row counts per lifecycle state. Useful for dashboards and
// tests; succeeds-failed breakdown distinguishes "retry scheduled" from
// fresh pending rows.
func (s *Store) Stats() (pending, running, succeeded, dead int) {
	row := s.db.QueryRow(`SELECT
		COALESCE(SUM(state = ?), 0),
		COALESCE(SUM(state = ?), 0),
		COALESCE(SUM(state = ?), 0),
		COALESCE(SUM(state = ?), 0)
		FROM jobs`,
		int(goqueue.StatePending), int(goqueue.StateRunning),
		int(goqueue.StateSucceeded), int(goqueue.StateDead))
	_ = row.Scan(&pending, &running, &succeeded, &dead)
	return
}

// Close implements goqueue.Queue: the store stops claiming jobs out of the
// database and blocked Dequeue calls return ErrQueueClosed. Persisted rows
// stay intact for later pickup — call CloseDB to also release the file.
func (s *Store) Close() error {
	s.closed.Store(true)
	s.signal()
	return nil
}

// CloseDB additionally closes the physical database handle, after which
// Enqueue fails as well. Safe to call multiple times and after Close.
func (s *Store) CloseDB() error {
	s.closed.Store(true)
	s.signal()
	if !s.dbDown.CompareAndSwap(false, true) {
		return nil
	}
	return s.db.Close()
}

// ---- internals ----

func (s *Store) nextSeq() int64 { return int64(s.seq.Add(1)) }

func (s *Store) signal() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *Store) exec(q string, args ...any) (sql.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), stmtTimeout)
	defer cancel()
	return s.db.ExecContext(ctx, q, args...)
}

func (s *Store) execRows(q string, args ...any) (int64, error) {
	res, err := s.exec(q, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (s *Store) queryInt(q string, args ...any) int {
	var n int
	ctx, cancel := context.WithTimeout(context.Background(), stmtTimeout)
	defer cancel()
	_ = s.db.QueryRowContext(ctx, q, args...).Scan(&n)
	return n
}

func (s *Store) withTx(fn func(tx *sql.Tx) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), stmtTimeout)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// translate maps storage-level insert failures onto package sentinels. Only
// the unique-key collision is interesting; everything else bubbles up.
func translate(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if strings.Contains(msg, "UNIQUE constraint failed") || strings.Contains(msg, "SQLITE_CONSTRAINT") {
		return ErrJobExists
	}
	return fmt.Errorf("goqueue/sqlite: %w", err)
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
		panic("goqueue/sqlite: crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
