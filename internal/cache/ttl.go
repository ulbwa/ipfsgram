package cache

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// ttlCache expires entries that have not been touched (Put or Get) within
// the configured ttl. A background goroutine sweeps expired files; Get
// refreshes both the in-memory deadline and the file mtime so adoption after
// a restart keeps reasonably accurate ages.
type ttlCache struct {
	dir string
	ttl time.Duration

	mu       sync.Mutex
	deadline map[int64]time.Time

	done chan struct{}
	wg   sync.WaitGroup
}

// maxSweepInterval caps how rarely the background sweeper runs.
const maxSweepInterval = time.Minute

// NewTTL opens a TTL-based disk cache rooted at dir. Existing "<carID>.car"
// files are adopted; their remaining lifetime is computed from mtime, and
// files already past their deadline are removed on the first sweep. Close
// stops the background sweeper.
func NewTTL(dir string, ttl time.Duration) (Cache, error) {
	if ttl <= 0 {
		return nil, fmt.Errorf("cache: ttl must be positive, got %v", ttl)
	}
	existing, err := scanDir(dir)
	if err != nil {
		return nil, err
	}
	c := &ttlCache{
		dir:      dir,
		ttl:      ttl,
		deadline: make(map[int64]time.Time, len(existing)),
		done:     make(chan struct{}),
	}
	for _, a := range existing {
		c.deadline[a.carID] = time.Unix(0, a.mtime).Add(ttl)
	}

	interval := ttl / 2
	if interval > maxSweepInterval {
		interval = maxSweepInterval
	}
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	c.wg.Add(1)
	go c.sweepLoop(interval)
	return c, nil
}

func (c *ttlCache) Get(carID int64) (string, bool) {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	dl, ok := c.deadline[carID]
	if !ok {
		return "", false
	}
	path := entryPath(c.dir, carID)
	if now.After(dl) {
		// Expired but not yet swept: treat as a miss and clean up eagerly.
		delete(c.deadline, carID)
		os.Remove(path)
		return "", false
	}
	c.deadline[carID] = now.Add(c.ttl)
	// Touch mtime so a restarted daemon adopts the entry with a fresh age.
	// Best effort: an error here only shortens the post-restart lifetime.
	_ = os.Chtimes(path, now, now)
	return path, true
}

func (c *ttlCache) Put(carID int64, src string, size int64) (string, error) {
	dst := entryPath(c.dir, carID)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := moveFile(src, dst); err != nil {
		return "", err
	}
	c.deadline[carID] = time.Now().Add(c.ttl)
	return dst, nil
}

func (c *ttlCache) Close() error {
	c.mu.Lock()
	select {
	case <-c.done:
		// already closed
	default:
		close(c.done)
	}
	c.mu.Unlock()
	c.wg.Wait()
	return nil
}

func (c *ttlCache) sweepLoop(interval time.Duration) {
	defer c.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			c.sweep()
		}
	}
}

// sweep removes all entries past their deadline. It is called periodically
// by the background goroutine and directly by tests.
func (c *ttlCache) sweep() {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	for carID, dl := range c.deadline {
		if now.After(dl) {
			delete(c.deadline, carID)
			os.Remove(entryPath(c.dir, carID))
		}
	}
}
