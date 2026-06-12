package cache

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// writeTempFile creates a file with the given content in a fresh temp dir
// (separate from the cache dir, mimicking a downloaded temp file).
func writeTempFile(t *testing.T, content []byte) string {
	t.Helper()
	src := filepath.Join(t.TempDir(), "download.tmp")
	if err := os.WriteFile(src, content, 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	return src
}

func TestLRUPutGetRoundtrip(t *testing.T) {
	dir := t.TempDir()
	c, err := NewLRU(dir, 1<<20)
	if err != nil {
		t.Fatalf("NewLRU: %v", err)
	}
	defer c.Close()

	content := []byte("hello car archive")
	src := writeTempFile(t, content)

	path, err := c.Put(42, src, int64(len(content)))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if want := filepath.Join(dir, "42.car"); path != want {
		t.Errorf("Put path = %q, want %q", path, want)
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Errorf("src file should be moved away, stat err = %v", err)
	}

	got, ok := c.Get(42)
	if !ok {
		t.Fatal("Get(42) = false, want true")
	}
	if got != path {
		t.Errorf("Get path = %q, want %q", got, path)
	}
	data, err := os.ReadFile(got)
	if err != nil {
		t.Fatalf("read cached file: %v", err)
	}
	if string(data) != string(content) {
		t.Errorf("cached contents = %q, want %q", data, content)
	}

	if _, ok := c.Get(99); ok {
		t.Error("Get(99) = true, want false for missing entry")
	}
}

func TestLRUEviction(t *testing.T) {
	dir := t.TempDir()
	c, err := NewLRU(dir, 100)
	if err != nil {
		t.Fatalf("NewLRU: %v", err)
	}
	defer c.Close()

	content := make([]byte, 40)
	for id := int64(1); id <= 3; id++ {
		src := writeTempFile(t, content)
		if _, err := c.Put(id, src, 40); err != nil {
			t.Fatalf("Put(%d): %v", id, err)
		}
	}

	if _, ok := c.Get(1); ok {
		t.Error("entry 1 should be evicted")
	}
	if _, err := os.Stat(filepath.Join(dir, "1.car")); !os.IsNotExist(err) {
		t.Errorf("file 1.car should be deleted from disk, stat err = %v", err)
	}
	for id := int64(2); id <= 3; id++ {
		if _, ok := c.Get(id); !ok {
			t.Errorf("entry %d should still be cached", id)
		}
	}
}

func TestLRURecency(t *testing.T) {
	dir := t.TempDir()
	c, err := NewLRU(dir, 100)
	if err != nil {
		t.Fatalf("NewLRU: %v", err)
	}
	defer c.Close()

	content := make([]byte, 40)
	for _, id := range []int64{1, 2} {
		if _, err := c.Put(id, writeTempFile(t, content), 40); err != nil {
			t.Fatalf("Put(%d): %v", id, err)
		}
	}
	// Touch 1 so 2 becomes least recently used.
	if _, ok := c.Get(1); !ok {
		t.Fatal("Get(1) should hit")
	}
	if _, err := c.Put(3, writeTempFile(t, content), 40); err != nil {
		t.Fatalf("Put(3): %v", err)
	}

	if _, ok := c.Get(2); ok {
		t.Error("entry 2 should be evicted (least recently used)")
	}
	if _, ok := c.Get(1); !ok {
		t.Error("entry 1 should survive (recently used)")
	}
	if _, ok := c.Get(3); !ok {
		t.Error("entry 3 should survive (just inserted)")
	}
}

func TestLRUOversizedEntry(t *testing.T) {
	dir := t.TempDir()
	c, err := NewLRU(dir, 10)
	if err != nil {
		t.Fatalf("NewLRU: %v", err)
	}
	defer c.Close()

	if _, err := c.Put(1, writeTempFile(t, make([]byte, 5)), 5); err != nil {
		t.Fatalf("Put(1): %v", err)
	}

	// A single entry bigger than maxBytes is still stored: the daemon must be
	// able to serve the just-downloaded CAR. Everything else gets evicted.
	big := make([]byte, 40)
	path, err := c.Put(2, writeTempFile(t, big), 40)
	if err != nil {
		t.Fatalf("Put(2) oversized: %v", err)
	}
	if _, ok := c.Get(2); !ok {
		t.Error("oversized entry should be stored")
	}
	if fi, err := os.Stat(path); err != nil || fi.Size() != 40 {
		t.Errorf("oversized file on disk: fi=%v err=%v", fi, err)
	}
	if _, ok := c.Get(1); ok {
		t.Error("entry 1 should be evicted to make room for oversized entry")
	}
}

func TestLRUConcurrency(t *testing.T) {
	dir := t.TempDir()
	c, err := NewLRU(dir, 1<<20)
	if err != nil {
		t.Fatalf("NewLRU: %v", err)
	}
	defer c.Close()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			content := []byte(fmt.Sprintf("payload-%d", id))
			src := writeTempFile(t, content)
			if _, err := c.Put(id, src, int64(len(content))); err != nil {
				t.Errorf("Put(%d): %v", id, err)
				return
			}
			if _, ok := c.Get(id); !ok {
				t.Errorf("Get(%d) miss right after Put", id)
			}
		}(int64(i))
	}
	wg.Wait()
}

func TestLRURestartAdoption(t *testing.T) {
	dir := t.TempDir()
	c1, err := NewLRU(dir, 1<<20)
	if err != nil {
		t.Fatalf("NewLRU: %v", err)
	}
	for _, id := range []int64{7, 8} {
		content := []byte(fmt.Sprintf("car-%d", id))
		if _, err := c1.Put(id, writeTempFile(t, content), int64(len(content))); err != nil {
			t.Fatalf("Put(%d): %v", id, err)
		}
	}
	if err := c1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	c2, err := NewLRU(dir, 1<<20)
	if err != nil {
		t.Fatalf("NewLRU (restart): %v", err)
	}
	defer c2.Close()
	for _, id := range []int64{7, 8} {
		path, ok := c2.Get(id)
		if !ok {
			t.Errorf("Get(%d) after restart = miss, want hit", id)
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read adopted file: %v", err)
		}
		if want := fmt.Sprintf("car-%d", id); string(data) != want {
			t.Errorf("adopted contents = %q, want %q", data, want)
		}
	}
}

func TestLRURestartAdoptionEnforcesBudget(t *testing.T) {
	dir := t.TempDir()
	c1, err := NewLRU(dir, 1<<20)
	if err != nil {
		t.Fatalf("NewLRU: %v", err)
	}
	for id := int64(1); id <= 3; id++ {
		if _, err := c1.Put(id, writeTempFile(t, make([]byte, 40)), 40); err != nil {
			t.Fatalf("Put(%d): %v", id, err)
		}
	}
	c1.Close()

	// Re-open with a smaller budget: oldest adopted files must be evicted.
	c2, err := NewLRU(dir, 100)
	if err != nil {
		t.Fatalf("NewLRU (smaller budget): %v", err)
	}
	defer c2.Close()
	hits := 0
	for id := int64(1); id <= 3; id++ {
		if _, ok := c2.Get(id); ok {
			hits++
		}
	}
	if hits != 2 {
		t.Errorf("hits after re-adoption with budget 100 = %d, want 2", hits)
	}
}

func TestTTLExpiry(t *testing.T) {
	dir := t.TempDir()
	c, err := NewTTL(dir, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("NewTTL: %v", err)
	}
	defer c.Close()

	content := []byte("ephemeral")
	if _, err := c.Put(1, writeTempFile(t, content), int64(len(content))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, ok := c.Get(1); !ok {
		t.Fatal("Get right after Put should hit")
	}

	// Poll for expiry without calling Get: Get would refresh the deadline.
	file := filepath.Join(dir, "1.car")
	deadline := time.Now().Add(2 * time.Second)
	for {
		c.(*ttlCache).sweep()
		if _, err := os.Stat(file); os.IsNotExist(err) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("entry never expired")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, ok := c.Get(1); ok {
		t.Error("Get after expiry = hit, want miss")
	}
}

func TestTTLGetRefreshesDeadline(t *testing.T) {
	dir := t.TempDir()
	c, err := NewTTL(dir, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("NewTTL: %v", err)
	}
	defer c.Close()

	content := []byte("sticky")
	if _, err := c.Put(1, writeTempFile(t, content), int64(len(content))); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Keep touching the entry past its original TTL; it must survive.
	stop := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(stop) {
		if _, ok := c.Get(1); !ok {
			t.Fatal("entry expired despite being refreshed by Get")
		}
		c.(*ttlCache).sweep()
		time.Sleep(20 * time.Millisecond)
	}
}

func TestTTLRestartAdoption(t *testing.T) {
	dir := t.TempDir()
	c1, err := NewTTL(dir, time.Hour)
	if err != nil {
		t.Fatalf("NewTTL: %v", err)
	}
	content := []byte("persisted")
	if _, err := c1.Put(5, writeTempFile(t, content), int64(len(content))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := c1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	c2, err := NewTTL(dir, time.Hour)
	if err != nil {
		t.Fatalf("NewTTL (restart): %v", err)
	}
	defer c2.Close()
	if _, ok := c2.Get(5); !ok {
		t.Error("Get(5) after restart = miss, want hit")
	}
}

func TestTTLConcurrency(t *testing.T) {
	dir := t.TempDir()
	c, err := NewTTL(dir, time.Minute)
	if err != nil {
		t.Fatalf("NewTTL: %v", err)
	}
	defer c.Close()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			content := []byte(fmt.Sprintf("payload-%d", id))
			if _, err := c.Put(id, writeTempFile(t, content), int64(len(content))); err != nil {
				t.Errorf("Put(%d): %v", id, err)
				return
			}
			if _, ok := c.Get(id); !ok {
				t.Errorf("Get(%d) miss right after Put", id)
			}
		}(int64(i))
	}
	wg.Wait()
}

func TestPutReplacesExistingEntry(t *testing.T) {
	dir := t.TempDir()
	c, err := NewLRU(dir, 1<<20)
	if err != nil {
		t.Fatalf("NewLRU: %v", err)
	}
	defer c.Close()

	if _, err := c.Put(1, writeTempFile(t, []byte("old")), 3); err != nil {
		t.Fatalf("first Put: %v", err)
	}
	if _, err := c.Put(1, writeTempFile(t, []byte("newer")), 5); err != nil {
		t.Fatalf("second Put: %v", err)
	}
	path, ok := c.Get(1)
	if !ok {
		t.Fatal("Get(1) miss after replace")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(data) != "newer" {
		t.Errorf("contents = %q, want %q", data, "newer")
	}
}
