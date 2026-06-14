// lru.go — NewLRU: byte-bounded LRU disk cache layered over golang-lru/v2,
// with restart adoption and file deletion on eviction.

package cache

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	lru "github.com/hashicorp/golang-lru/v2"
)

// lruCache is a byte-bounded disk cache layered over a count-bounded
// golang-lru/v2 cache.
//
// golang-lru is bounded by entry count, not bytes, so the byte budget is
// enforced here: the lru maps carID -> sizeBytes purely to track recency
// ordering, while this cache keeps the running total and, after every Add,
// evicts the oldest entries until the total fits within maxBytes.
//
// Locking: golang-lru/v2's top-level Cache invokes the eviction callback
// AFTER releasing its internal lock, so the callback runs in the goroutine
// that triggered the eviction. To avoid a re-entrant deadlock on this
// cache's own mutex, onEvict touches no cache mutex — it only deletes the
// file and adjusts the atomic total. putMu merely serializes the Put-time
// eviction loop so concurrent Puts don't interleave RemoveOldest calls.
//
// Oversized entries: a single CAR larger than maxBytes is still stored — the
// daemon must be able to serve blocks from the archive it just downloaded.
// Everything else is evicted; the cache temporarily holds more than maxBytes
// until that entry is itself evicted by a later Put.
type lruCache struct {
	dir      string
	maxBytes int64

	lru   *lru.Cache[int64, int64] // carID -> sizeBytes, recency-ordered
	total atomic.Int64

	putMu  sync.Mutex
	closed bool
}

// minLRUCapacity is a large floor for the lru's count capacity. The real bound
// is bytes; the count cap only needs to be large enough that the byte budget
// always bites first. Small or atypically tiny entries (e.g. test fixtures)
// must not trip a count-based eviction, so the count cap is never less than
// this.
const minLRUCapacity = 1 << 20

// NewLRU opens an LRU-by-bytes disk cache rooted at dir, backed by
// golang-lru/v2. Existing "<carID>.car" files are adopted with recency derived
// from mtime order (newest = most recently used); if they exceed maxBytes, the
// oldest are evicted immediately.
func NewLRU(dir string, maxBytes int64) (Cache, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("cache: maxBytes must be positive, got %d", maxBytes)
	}
	existing, err := scanDir(dir)
	if err != nil {
		return nil, err
	}

	c := &lruCache{dir: dir, maxBytes: maxBytes}

	// Capacity hint: large enough that the byte budget always bites before the
	// count cap, and never below the number of adopted entries so adoption never
	// evicts by count. golang-lru rejects size <= 0.
	capacity := minLRUCapacity
	if capacity < len(existing) {
		capacity = len(existing)
	}

	l, err := lru.NewWithEvict[int64, int64](capacity, c.onEvict)
	if err != nil {
		return nil, fmt.Errorf("cache: new lru: %w", err)
	}
	c.lru = l

	// Oldest mtime first, so each Add leaves the most recently modified file as
	// most recently used.
	for _, a := range existing {
		c.lru.Add(a.carID, a.size)
		c.total.Add(a.size)
	}
	c.enforceBudget()
	return c, nil
}

// onEvict is golang-lru's eviction callback. It deletes the file and subtracts
// its size from the running total. It must not touch any cache mutex (see the
// type doc): for the top-level Cache it runs in the triggering goroutine after
// the lru lock is released; holding putMu here would deadlock the Put loop.
func (c *lruCache) onEvict(carID int64, size int64) {
	c.total.Add(-size)
	os.Remove(entryPath(c.dir, carID))
}

func (c *lruCache) Get(carID int64) (string, bool) {
	// lru.Get updates recency under the library's own lock.
	if _, ok := c.lru.Get(carID); !ok {
		return "", false
	}
	return entryPath(c.dir, carID), true
}

// Put admits the file at src into the cache. Eviction happens before the move:
// if moveFile then fails, the evicted entries are already gone and the cache
// ends up emptier than strictly necessary. This is a documented tradeoff —
// evicting first keeps the on-disk total within maxBytes at all times, and a
// failed Put only costs re-downloadable cache entries.
func (c *lruCache) Put(carID int64, src string, size int64) (string, error) {
	dst := entryPath(c.dir, carID)

	c.putMu.Lock()
	defer c.putMu.Unlock()

	// Replace any existing entry for the same carID first. Remove fires onEvict
	// (deleting the old file and subtracting its size).
	c.lru.Remove(carID)

	// Account for the incoming entry, then evict the oldest until we fit. The
	// new entry is added last so it is the most recently used and survives the
	// loop (unless it is itself oversized, in which case it is the only one
	// left).
	c.lru.Add(carID, size)
	c.total.Add(size)
	c.evictUntilFits()

	if err := moveFile(src, dst); err != nil {
		// Roll back the bookkeeping for the entry we never managed to store.
		c.lru.Remove(carID)
		return "", err
	}
	return dst, nil
}

func (c *lruCache) Close() error {
	c.putMu.Lock()
	defer c.putMu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	return nil
}

// enforceBudget evicts least-recently-used entries until the total fits within
// maxBytes. Used at construction time. Caller need not hold putMu.
func (c *lruCache) enforceBudget() {
	for c.total.Load() > c.maxBytes && c.lru.Len() > 0 {
		c.lru.RemoveOldest()
	}
}

// evictUntilFits is enforceBudget for Put. The just-inserted entry is the most
// recently used, so RemoveOldest never touches it while another entry remains;
// the Len() > 1 guard stops before evicting that final entry, so an oversized
// fresh entry is still stored (see the type doc). Caller holds putMu.
func (c *lruCache) evictUntilFits() {
	for c.total.Load() > c.maxBytes && c.lru.Len() > 1 {
		c.lru.RemoveOldest()
	}
}
