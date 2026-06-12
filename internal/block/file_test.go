// File: internal/block/file_test.go
// Tests for the local-file UnixFS block source (FromFile): single raw leaf,
// multi-chunk layout, codecs, traversal order and import determinism.
package block

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ipfs/go-cid"
)

func writeTempFile(t *testing.T, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "data.bin")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestFromFileSmall(t *testing.T) {
	data := []byte("hello ipfsgram")
	root, blocks, err := FromFile(context.Background(), writeTempFile(t, data))
	if err != nil {
		t.Fatalf("FromFile: %v", err)
	}
	if len(blocks) != 1 {
		t.Fatalf("len(blocks) = %d, want 1 (single raw leaf)", len(blocks))
	}
	if !blocks[0].CID.Equals(root) {
		t.Errorf("single-block root must equal the leaf cid")
	}
	if root.Version() != 1 {
		t.Errorf("root version = %d, want CIDv1", root.Version())
	}
	if root.Prefix().Codec != cid.Raw {
		t.Errorf("single-chunk root codec = %d, want raw", root.Prefix().Codec)
	}
	if !bytes.Equal(blocks[0].Data, data) {
		t.Errorf("raw leaf data mismatch")
	}
}

func TestFromFileMultiChunk(t *testing.T) {
	// 3 chunks of distinct content (identical chunks would dedup by CID).
	data := make([]byte, chunkSize*2+100)
	for i := range data {
		data[i] = byte(i / chunkSize) // 0x00.. / 0x01.. / 0x02..
	}
	root, blocks, err := FromFile(context.Background(), writeTempFile(t, data))
	if err != nil {
		t.Fatalf("FromFile: %v", err)
	}
	if len(blocks) != 4 { // root + 3 raw leaves
		t.Fatalf("len(blocks) = %d, want 4", len(blocks))
	}
	if !blocks[0].CID.Equals(root) {
		t.Errorf("traversal must start at the root (preorder)")
	}
	if root.Prefix().Codec != cid.DagProtobuf {
		t.Errorf("multi-chunk root codec = %d, want dag-pb", root.Prefix().Codec)
	}
	var leafBytes int
	for _, b := range blocks[1:] {
		if b.CID.Prefix().Codec != cid.Raw {
			t.Errorf("leaf codec = %d, want raw", b.CID.Prefix().Codec)
		}
		leafBytes += len(b.Data)
	}
	if leafBytes != len(data) {
		t.Errorf("leaves hold %d bytes, want %d", leafBytes, len(data))
	}

	// Determinism: the same content must produce the same root.
	root2, _, err := FromFile(context.Background(), writeTempFile(t, data))
	if err != nil {
		t.Fatal(err)
	}
	if !root.Equals(root2) {
		t.Errorf("import is not deterministic: %s != %s", root, root2)
	}
}
