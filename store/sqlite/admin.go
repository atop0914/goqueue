package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	goqueue "github.com/atop0914/goqueue"
)

// ---- admin operations for the SQLite Store ----

// Pause implements goqueue.Pauser: Dequeue stops claiming jobs until Resume.
// Pending rows are left untouched. Pause is a runtime concern (it makes no
// sense to stay paused after a process restart), so the flag lives in memory
// only. Idempotent; returns goqueue.ErrQueueClosed after Close.
func (s *Store) Pause() error {
	if s.closed.Load() {
		return goqueue.ErrQueueClosed
	}
	s.paused.Store(true)
	s.signal()
	return nil
}

// Resume implements goqueue.Pauser: idle claimers wake up and delivery
// continues. Idempotent.
func (s *Store) Resume() {
	if !s.paused.CompareAndSwap(true, false) {
		return
	}
	s.signal()
}

// IsPaused implements goqueue.Pauser.
func (s *Store) IsPaused() bool { return s.paused.Load() }

// Purge implements goqueue.Purger: delete pending rows (waiting, delayed and
// awaiting-retry) and optionally the dead ones too, inside one transaction so
// the returned count is exactly the number of rows removed. Running jobs are
// never touched — at-least-once delivery means their worker will ack/nack
// them regardless of the purge. Unique keys of purged pending jobs are
// released (the column is cleared) so the same work can be re-enqueued.
// Idempotent: purging an empty queue returns 0.
func (s *Store) Purge(_ context.Context, dead bool) (int, error) {
	var removed int
	err := s.withTx(func(tx *sql.Tx) error {
		// Release unique keys of purged pending jobs first; the partial
		// unique index only covers non-empty keys, so clearing the column
		// frees the slot.
		if _, err := tx.Exec(`UPDATE jobs SET unique_key = ''
			WHERE state = ? AND unique_key != ''`,
			int(goqueue.StatePending)); err != nil {
			return err
		}
		res, err := tx.Exec(`DELETE FROM jobs WHERE state = ?`,
			int(goqueue.StatePending))
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		removed = int(n)
		if dead {
			res, err := tx.Exec(`DELETE FROM jobs WHERE state = ?`,
				int(goqueue.StateDead))
			if err != nil {
				return err
			}
			dn, _ := res.RowsAffected()
			removed += int(dn)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return removed, nil
}

// RequeueDead implements goqueue.DeadRequeuer: requeue every dead job (see
// RequeueDeadJob) and return the count. Jobs whose unique key is contested
// stay dead and are not counted; the single-connection design makes per-job
// transactions cheap.
func (s *Store) RequeueDead(ctx context.Context) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, stmtTimeout)
	defer cancel()
	rows, err := s.db.QueryContext(ctx,
		`SELECT id FROM jobs WHERE state = ? ORDER BY dead_at ASC, id ASC`,
		int(goqueue.StateDead))
	if err != nil {
		return 0, fmt.Errorf("goqueue/sqlite: requeue-dead scan: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	_ = rows.Close()

	n := 0
	for _, id := range ids {
		if s.RequeueDeadJob(ctx, id) == nil {
			n++
		}
	}
	return n, nil
}

// RequeueDeadJob implements the single-job form of goqueue.DeadRequeuer: one
// transaction flips a dead row back to pending — attempts reset,
// last_error/dead_at cleared, run_after zeroed (due immediately) — and
// re-claims the unique key. Semantics match the in-memory backend:
//
//   - unknown or non-dead IDs return goqueue.ErrJobNotFound;
//   - a unique key held by another in-flight job leaves the dead job
//     untouched and returns goqueue.ErrJobExists.
//
// The key re-claim is an UPDATE guarded by the partial unique index: if
// another row holds the key, SQLite rejects the write and the transaction
// aborts — which is exactly the ErrJobExists case.
func (s *Store) RequeueDeadJob(_ context.Context, id string) error {
	return s.withTx(func(tx *sql.Tx) error {
		var ukey string
		err := tx.QueryRow(`SELECT unique_key FROM jobs WHERE id = ? AND state = ?`,
			id, int(goqueue.StateDead)).Scan(&ukey)
		if errors.Is(err, sql.ErrNoRows) {
			return goqueue.ErrJobNotFound
		}
		if err != nil {
			return err
		}
		// NOTE: with a contested key this UPDATE is the one that violates
		// the partial unique index (the row re-enters it the moment state
		// leaves the dead range while still holding unique_key).
		if _, err := tx.Exec(`UPDATE jobs
			SET state = ?, attempts = 0, last_error = '', dead_at = 0, run_after = 0
			WHERE id = ?`, int(goqueue.StatePending), id); err != nil {
			return translate(err)
		}
		if ukey != "" {
			// Re-claim last: a violation here aborts the whole transaction,
			// leaving the job dead — the documented ErrJobExists behavior.
			if _, err := tx.Exec(`UPDATE jobs SET unique_key = ? WHERE id = ?`,
				ukey, id); err != nil {
				return translate(err)
			}
		}
		return nil
	})
}

// Compile-time checks: the SQLite store implements the full admin surface.
var (
	_ goqueue.AdminQueue = (*Store)(nil)
	_ atomicBool         = (*atomic.Bool)(nil)
)

// atomicBool is the minimal shape of sync/atomic.Bool used above; aliasing
// it keeps the import list honest without a second import block.
type atomicBool interface {
	Load() bool
	CompareAndSwap(old, new bool) bool
	Store(v bool)
}

// pausedDeadline documents (via a compile-time reference) that pause waiting
// in Dequeue honors the caller's context deadline exactly like the empty-
// queue wait does; see Dequeue for the actual select.
var _ = time.Second
