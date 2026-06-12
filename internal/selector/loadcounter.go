package selector

import (
	"sync"
	"time"
)

// retention bounds how long timestamps are kept; entries older than this are
// dropped on write so the counter does not grow without bound.
const retention = time.Hour

// LoadCounter is a thread-safe in-process sliding-window counter of operations
// and errors per bot. It backs the least-loaded tie-breaking in the pick
// functions.
type LoadCounter struct {
	mu   sync.Mutex
	ops  map[int64][]time.Time
	errs map[int64][]time.Time
	now  func() time.Time // overridable in tests
}

// NewLoadCounter returns an empty LoadCounter using time.Now as its clock.
func NewLoadCounter() *LoadCounter {
	return &LoadCounter{
		ops:  make(map[int64][]time.Time),
		errs: make(map[int64][]time.Time),
		now:  time.Now,
	}
}

// Record registers an operation for the bot at the current time.
func (lc *LoadCounter) Record(botID int64) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.ops[botID] = append(prune(lc.ops[botID], lc.now().Add(-retention)), lc.now())
}

// RecordError registers an error for the bot at the current time.
func (lc *LoadCounter) RecordError(botID int64) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.errs[botID] = append(prune(lc.errs[botID], lc.now().Add(-retention)), lc.now())
}

// CountSince returns the number of operations recorded for the bot within the
// given window.
func (lc *LoadCounter) CountSince(botID int64, window time.Duration) int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return countAfter(lc.ops[botID], lc.now().Add(-window))
}

// ErrorsSince returns the number of errors recorded for the bot within the
// given window.
func (lc *LoadCounter) ErrorsSince(botID int64, window time.Duration) int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return countAfter(lc.errs[botID], lc.now().Add(-window))
}

// prune drops timestamps at or before cutoff. Timestamps are appended in
// chronological order, so the slice stays sorted.
func prune(ts []time.Time, cutoff time.Time) []time.Time {
	i := 0
	for i < len(ts) && !ts[i].After(cutoff) {
		i++
	}
	return ts[i:]
}

func countAfter(ts []time.Time, cutoff time.Time) int {
	n := 0
	for _, t := range ts {
		if t.After(cutoff) {
			n++
		}
	}
	return n
}
