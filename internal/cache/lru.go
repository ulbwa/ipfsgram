package cache

import (
	"container/list"
	"fmt"
	"os"
	"sync"
)

// lruCache evicts least-recently-used entries once the total size of cached
// files exceeds maxBytes.
//
// Oversized entries: a single CAR larger than maxBytes is still stored — the
// daemon must be able to serve blocks from the archive it just downloaded.
// In that case every other entry is evicted and the cache temporarily holds
// more than maxBytes until the oversized entry itself is evicted by a later
// Put.
type lruCache struct {
	dir      string
	maxBytes int64

	mu    sync.Mutex
	ll    *list.List // front = most recently used; values are *lruEntry
	items map[int64]*list.Element
	total int64
}

type lruEntry struct {
	carID int64
	size  int64
}

// NewLRU opens an LRU-by-bytes disk cache rooted at dir. Existing
// "<carID>.car" files are adopted with recency derived from mtime order;
// if they exceed maxBytes, the oldest are evicted immediately.
func NewLRU(dir string, maxBytes int64) (Cache, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("cache: maxBytes must be positive, got %d", maxBytes)
	}
	existing, err := scanDir(dir)
	if err != nil {
		return nil, err
	}
	c := &lruCache{
		dir:      dir,
		maxBytes: maxBytes,
		ll:       list.New(),
		items:    make(map[int64]*list.Element),
	}
	// Oldest mtime first, so pushing each to the front leaves the most
	// recently modified file as most recently used.
	for _, a := range existing {
		c.items[a.carID] = c.ll.PushFront(&lruEntry{carID: a.carID, size: a.size})
		c.total += a.size
	}
	c.mu.Lock()
	c.evictUntilFits(0)
	c.mu.Unlock()
	return c, nil
}

func (c *lruCache) Get(carID int64) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[carID]
	if !ok {
		return "", false
	}
	c.ll.MoveToFront(el)
	return entryPath(c.dir, carID), true
}

func (c *lruCache) Put(carID int64, src string, size int64) (string, error) {
	dst := entryPath(c.dir, carID)

	c.mu.Lock()
	defer c.mu.Unlock()

	// Replace an existing entry for the same carID.
	if el, ok := c.items[carID]; ok {
		c.removeElement(el)
	}
	// Make room before moving the file in. An oversized entry (size >
	// maxBytes) evicts everything else but is still admitted; see type doc.
	c.evictUntilFits(size)

	if err := moveFile(src, dst); err != nil {
		return "", err
	}
	c.items[carID] = c.ll.PushFront(&lruEntry{carID: carID, size: size})
	c.total += size
	return dst, nil
}

func (c *lruCache) Close() error { return nil }

// evictUntilFits removes least-recently-used entries until total+incoming
// fits within maxBytes or the cache is empty. Caller must hold c.mu.
func (c *lruCache) evictUntilFits(incoming int64) {
	for c.total+incoming > c.maxBytes && c.ll.Len() > 0 {
		c.removeElement(c.ll.Back())
	}
}

// removeElement drops an entry from bookkeeping and deletes its file.
// Caller must hold c.mu.
func (c *lruCache) removeElement(el *list.Element) {
	ent := el.Value.(*lruEntry)
	c.ll.Remove(el)
	delete(c.items, ent.carID)
	c.total -= ent.size
	os.Remove(entryPath(c.dir, ent.carID))
}
