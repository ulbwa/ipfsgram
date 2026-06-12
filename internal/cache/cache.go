// Package cache implements the daemon's local disk cache for CAR archives
// downloaded from Telegram. Entries are keyed by carID and stored as
// "<carID>.car" files inside a single directory. Two eviction strategies are
// provided: LRU bounded by total bytes (NewLRU) and time-based expiry
// (NewTTL). Both implementations are safe for concurrent use and adopt files
// already present in the directory, so the cache survives daemon restarts.
package cache

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Cache is the daemon's disk cache of CAR files, keyed by carID.
type Cache interface {
	// Get returns the path to the cached file and true when the archive is
	// present. A hit refreshes the entry (LRU recency or TTL deadline).
	Get(carID int64) (path string, ok bool)
	// Put moves the file at src into the cache (os.Rename, with a copy+remove
	// fallback for cross-filesystem moves), possibly evicting other entries.
	Put(carID int64, src string, size int64) (path string, err error)
	Close() error
}

const carExt = ".car"

// entryPath returns the cache file path for a carID.
func entryPath(dir string, carID int64) string {
	return filepath.Join(dir, strconv.FormatInt(carID, 10)+carExt)
}

// adopted describes a pre-existing cache file found during directory scan.
type adopted struct {
	carID int64
	size  int64
	mtime int64 // unix nanos, used to restore recency ordering
}

// scanDir creates dir if needed and returns the existing "<carID>.car" files
// sorted by mtime, oldest first. Files that do not match the naming scheme
// are ignored.
func scanDir(dir string) ([]adopted, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("cache: create dir: %w", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("cache: scan dir: %w", err)
	}
	var out []adopted
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		base, found := strings.CutSuffix(name, carExt)
		if !found {
			continue
		}
		carID, err := strconv.ParseInt(base, 10, 64)
		if err != nil {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue // raced with deletion
		}
		out = append(out, adopted{
			carID: carID,
			size:  info.Size(),
			mtime: info.ModTime().UnixNano(),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].mtime < out[j].mtime })
	return out, nil
}

// moveFile renames src to dst, falling back to copy+remove when the rename
// fails because src lives on a different filesystem (temp downloads may be
// on another mount).
func moveFile(src, dst string) error {
	if err := os.Rename(src, dst); err == nil {
		return nil
	}
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("cache: open src: %w", err)
	}
	defer in.Close()

	tmp, err := os.CreateTemp(filepath.Dir(dst), ".put-*")
	if err != nil {
		return fmt.Errorf("cache: create tmp: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := io.Copy(tmp, in); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("cache: copy: %w", err)
	}
	// Sync before close: the rename below makes the file visible under dst,
	// so flush its contents to disk first for crash safety.
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("cache: sync tmp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("cache: close tmp: %w", err)
	}
	if err := os.Rename(tmpName, dst); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("cache: rename tmp: %w", err)
	}
	os.Remove(src)
	return nil
}
