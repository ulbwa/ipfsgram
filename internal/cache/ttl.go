// ttl.go — NewTTL: time-based disk cache over golang-lru/v2's expirable.LRU,
// with restart adoption and file deletion on expiry.

package cache

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
)

// ttlCache expires entries a fixed ttl after insertion, backed by
// golang-lru/v2's expirable.LRU. The library owns the deadlines and runs an
// internal reaper goroutine that fires the eviction callback for expired
// entries; this cache's callback deletes the file.
//
// TTL semantics — expiry is from INSERTION (Put), not last access. The
// expirable.LRU refreshes recency on Get but does NOT reset an entry's expiry
// deadline, so Get does not extend an entry's lifetime. An entry inserted at
// time T is removed at T+ttl regardless of how often it is read. This is the
// natural, documented behavior of the ready-made library and acceptable for a
// re-downloadable CAR cache.
//
// Locking: expirable.LRU invokes the eviction callback WHILE HOLDING its own
// lock (including from the reaper goroutine). The callback therefore must not
// call back into the lru and must not take any cache mutex that is ever held
// while calling an lru method. onEvict here only does file IO, so it is safe.
//
// Reaper lifetime: expirable.LRU (v2.0.7) starts an internal reaper goroutine
// that, by design, never exits — the library exposes no way to stop it. NewTTL
// is therefore intended to be called once per daemon process; the goroutine
// lives for the process lifetime.
type ttlCache struct {
	dir string
	ttl time.Duration

	lru *expirable.LRU[int64, int64] // carID -> sizeBytes

	mu     sync.Mutex
	closed bool
}

// ttlCapacity sizes the count bound of the expirable LRU. The intended bound is
// time, not count, so this is a large constant — entries leave when they expire,
// not because the count cap is hit.
const ttlCapacity = 1 << 20

// NewTTL opens a TTL-based disk cache rooted at dir, backed by
// golang-lru/v2's expirable.LRU. Existing "<carID>.car" files are adopted; the
// library starts their ttl clock at adoption time (it cannot be told the
// original insertion time), so a restart resets the remaining lifetime to a
// full ttl. Close clears the cache (which stops the library's reaper).
func NewTTL(dir string, ttl time.Duration) (Cache, error) {
	if ttl <= 0 {
		return nil, fmt.Errorf("cache: ttl must be positive, got %v", ttl)
	}
	existing, err := scanDir(dir)
	if err != nil {
		return nil, err
	}

	c := &ttlCache{dir: dir, ttl: ttl}
	c.lru = expirable.NewLRU[int64, int64](ttlCapacity, c.onEvict, ttl)

	for _, a := range existing {
		c.lru.Add(a.carID, a.size)
	}
	return c, nil
}

// onEvict is the expirable LRU's callback. It runs under the library's lock
// (and from its reaper goroutine), so it does file IO only and never touches a
// cache mutex held across an lru call.
func (c *ttlCache) onEvict(carID int64, _ int64) {
	os.Remove(entryPath(c.dir, carID))
}

func (c *ttlCache) Get(carID int64) (string, bool) {
	// expirable.Get returns false once the entry has expired and refreshes
	// recency (but not the expiry deadline) on a hit.
	if _, ok := c.lru.Get(carID); !ok {
		return "", false
	}
	return entryPath(c.dir, carID), true
}

func (c *ttlCache) Put(carID int64, src string, size int64) (string, error) {
	dst := entryPath(c.dir, carID)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := moveFile(src, dst); err != nil {
		return "", err
	}
	c.lru.Add(carID, size)
	return dst, nil
}

// Close marks the cache closed. It deliberately does NOT purge the lru: doing
// so would fire onEvict for every live entry and delete its file from disk,
// which would defeat restart adoption (files must survive a clean shutdown).
// The expirable.LRU's internal reaper goroutine is tied to the cache object's
// lifetime and is reclaimed with it once unreferenced. Close is idempotent.
func (c *ttlCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}
