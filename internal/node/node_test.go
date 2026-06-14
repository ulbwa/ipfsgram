// node_test.go — unit tests for the node package's lightweight parts:
// validation error paths in New and the persisted identity round-trip. These do
// not stand up a real DHT/network.

package node

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ipfs/boxo/provider"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"
)

// stubBlockstore is a non-nil boxo Blockstore whose methods are never called:
// the nil-ProvideKeys check in New returns before the blockstore is touched.
type stubBlockstore struct{}

func (stubBlockstore) DeleteBlock(context.Context, cid.Cid) error { panic("unused") }
func (stubBlockstore) Has(context.Context, cid.Cid) (bool, error) { panic("unused") }
func (stubBlockstore) Get(context.Context, cid.Cid) (blocks.Block, error) {
	panic("unused")
}
func (stubBlockstore) GetSize(context.Context, cid.Cid) (int, error) { panic("unused") }
func (stubBlockstore) Put(context.Context, blocks.Block) error       { panic("unused") }
func (stubBlockstore) PutMany(context.Context, []blocks.Block) error { panic("unused") }
func (stubBlockstore) AllKeysChan(context.Context) (<-chan cid.Cid, error) {
	panic("unused")
}
func (stubBlockstore) HashOnRead(bool) { panic("unused") }

func TestNewNilBlockstore(t *testing.T) {
	_, err := New(context.Background(), Config{
		Blockstore:  nil,
		ProvideKeys: emptyKeys,
	})
	if err == nil || err.Error() != "node: nil blockstore" {
		t.Fatalf("err = %v, want \"node: nil blockstore\"", err)
	}
}

func TestNewNilProvideKeys(t *testing.T) {
	_, err := New(context.Background(), Config{
		Blockstore:  stubBlockstore{},
		ProvideKeys: nil,
	})
	if err == nil || err.Error() != "node: nil ProvideKeys" {
		t.Fatalf("err = %v, want \"node: nil ProvideKeys\"", err)
	}
}

// emptyKeys is a trivial KeyChanFunc yielding no CIDs.
func emptyKeys(context.Context) (<-chan cid.Cid, error) {
	ch := make(chan cid.Cid)
	close(ch)
	return ch, nil
}

var _ provider.KeyChanFunc = emptyKeys

func TestLoadOrCreateIdentityCreatesAndReloads(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "identity.key")

	priv1, err := loadOrCreateIdentity(path)
	if err != nil {
		t.Fatalf("first loadOrCreateIdentity: %v", err)
	}

	// File created with 0600 permissions.
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat key file: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("key file mode = %o, want 600", perm)
	}

	// Second call loads the same key.
	priv2, err := loadOrCreateIdentity(path)
	if err != nil {
		t.Fatalf("second loadOrCreateIdentity: %v", err)
	}
	raw1, err := crypto.MarshalPrivateKey(priv1)
	if err != nil {
		t.Fatalf("marshal priv1: %v", err)
	}
	raw2, err := crypto.MarshalPrivateKey(priv2)
	if err != nil {
		t.Fatalf("marshal priv2: %v", err)
	}
	if string(raw1) != string(raw2) {
		t.Error("reloaded identity differs from the created one")
	}
	if !priv1.Equals(priv2) {
		t.Error("priv1 and priv2 should be equal keys")
	}
}

func TestLoadOrCreateIdentityCorruptFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "identity.key")
	if err := os.WriteFile(path, []byte("not a valid key"), 0o600); err != nil {
		t.Fatalf("write corrupt file: %v", err)
	}
	if _, err := loadOrCreateIdentity(path); err == nil {
		t.Fatal("loadOrCreateIdentity: want error on corrupt file")
	}
}
