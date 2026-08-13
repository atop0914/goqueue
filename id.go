package goqueue

import (
	"crypto/rand"
	"encoding/hex"
	"sync/atomic"
	"time"
)

// newID generates a unique, sortable job ID: 8 bytes of millisecond
// timestamp followed by 8 bytes of randomness, hex-encoded (32 chars).
// Zero dependencies; good enough for single-node queues. Persistent
// backends may substitute their own ID scheme.
func newID() string {
	var b [16]byte
	// Timestamp (big-endian ms) makes IDs roughly time-ordered.
	ts := uint64(time.Now().UnixMilli())
	b[0] = byte(ts >> 56)
	b[1] = byte(ts >> 48)
	b[2] = byte(ts >> 40)
	b[3] = byte(ts >> 32)
	b[4] = byte(ts >> 24)
	b[5] = byte(ts >> 16)
	b[6] = byte(ts >> 8)
	b[7] = byte(ts)

	if _, err := rand.Read(b[8:]); err != nil {
		// Fallback: counter + nanos, still unique within the process.
		n := atomic.AddUint64(&idFallback, 1)
		nanos := uint64(time.Now().UnixNano())
		b[8] = byte(nanos >> 56)
		b[9] = byte(nanos >> 48)
		b[10] = byte(nanos >> 40)
		b[11] = byte(nanos >> 32)
		b[12] = byte(n >> 24)
		b[13] = byte(n >> 16)
		b[14] = byte(n >> 8)
		b[15] = byte(n)
	}
	return hex.EncodeToString(b[:])
}

var idFallback uint64
