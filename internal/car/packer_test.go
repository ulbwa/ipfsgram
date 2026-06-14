// packer_test.go — round-trip, rotation, offset, and edge-case tests for
// RotatingPacker and ReadBlockAt, verified against go-car/v2's parser.

package car

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"testing"

	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	mh "github.com/multiformats/go-multihash"
)

// makeBlock builds a raw-codec CIDv1 over data using sha2-256.
func makeBlock(t *testing.T, data []byte) cid.Cid {
	t.Helper()
	h, err := mh.Sum(data, mh.SHA2_256, -1)
	if err != nil {
		t.Fatalf("multihash: %v", err)
	}
	return cid.NewCidV1(cid.Raw, h)
}

func randBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return b
}

// verifyCarWithBlockReader parses path with go-car/v2 and checks the blocks
// match want (same order, same CIDs, same bytes) and the header root.
func verifyCarWithBlockReader(t *testing.T, path string, root cid.Cid, wantCIDs []cid.Cid, wantData [][]byte) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	br, err := carv2.NewBlockReader(f)
	if err != nil {
		t.Fatalf("NewBlockReader(%s): %v", path, err)
	}
	if br.Version != 1 {
		t.Fatalf("car version = %d, want 1", br.Version)
	}
	if len(br.Roots) != 1 || !br.Roots[0].Equals(root) {
		t.Fatalf("roots = %v, want [%s]", br.Roots, root)
	}

	i := 0
	for {
		blk, err := br.Next()
		if err != nil {
			break
		}
		if i >= len(wantCIDs) {
			t.Fatalf("car %s has more than %d blocks", path, len(wantCIDs))
		}
		if !blk.Cid().Equals(wantCIDs[i]) {
			t.Fatalf("block %d cid = %s, want %s", i, blk.Cid(), wantCIDs[i])
		}
		if !bytes.Equal(blk.RawData(), wantData[i]) {
			t.Fatalf("block %d data mismatch", i)
		}
		i++
	}
	if i != len(wantCIDs) {
		t.Fatalf("car %s has %d blocks, want %d", path, i, len(wantCIDs))
	}
}

func TestPackerSingleCarRoundTrip(t *testing.T) {
	dir := t.TempDir()

	const n = 10
	data := make([][]byte, n)
	cids := make([]cid.Cid, n)
	for i := range data {
		data[i] = randBytes(t, 64+i)
		cids[i] = makeBlock(t, data[i])
	}
	root := cids[0]

	p, err := NewRotatingPacker(dir, 1<<20, root)
	if err != nil {
		t.Fatalf("NewRotatingPacker: %v", err)
	}
	for i := range data {
		if err := p.Add(cids[i], data[i]); err != nil {
			t.Fatalf("Add(%d): %v", i, err)
		}
	}
	cars, err := p.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if len(cars) != 1 {
		t.Fatalf("got %d cars, want 1", len(cars))
	}
	car := cars[0]
	if len(car.Blocks) != n {
		t.Fatalf("got %d blocks, want %d", len(car.Blocks), n)
	}

	st, err := os.Stat(car.Path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if st.Size() != car.Size {
		t.Fatalf("PackedCar.Size = %d, file size = %d", car.Size, st.Size())
	}

	verifyCarWithBlockReader(t, car.Path, root, cids, data)
}

func TestPackerRotation(t *testing.T) {
	dir := t.TempDir()

	const maxSize = 4096
	const blockSize = 1000
	const n = 12 // ~12KB of payload, must split into multiple cars

	data := make([][]byte, n)
	cids := make([]cid.Cid, n)
	for i := range data {
		data[i] = randBytes(t, blockSize)
		cids[i] = makeBlock(t, data[i])
	}
	root := cids[0]

	p, err := NewRotatingPacker(dir, maxSize, root)
	if err != nil {
		t.Fatalf("NewRotatingPacker: %v", err)
	}
	for i := range data {
		if err := p.Add(cids[i], data[i]); err != nil {
			t.Fatalf("Add(%d): %v", i, err)
		}
	}
	cars, err := p.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if len(cars) < 2 {
		t.Fatalf("got %d cars, want >= 2", len(cars))
	}

	total := 0
	bi := 0
	for ci, car := range cars {
		if len(car.Blocks) == 0 {
			t.Fatalf("car %d has no blocks", ci)
		}
		if car.Size > maxSize && len(car.Blocks) > 1 {
			t.Fatalf("car %d size %d exceeds maxSize %d with %d blocks", ci, car.Size, maxSize, len(car.Blocks))
		}
		wantCIDs := cids[bi : bi+len(car.Blocks)]
		wantData := data[bi : bi+len(car.Blocks)]
		verifyCarWithBlockReader(t, car.Path, root, wantCIDs, wantData)
		bi += len(car.Blocks)
		total += len(car.Blocks)
	}
	if total != n {
		t.Fatalf("total blocks across cars = %d, want %d", total, n)
	}
}

func TestReadBlockAtInvariant(t *testing.T) {
	dir := t.TempDir()

	const maxSize = 4096
	sizes := []int{1, 31, 200, 1000, 1500, 3000, 5000, 7, 999}
	data := make([][]byte, len(sizes))
	cids := make([]cid.Cid, len(sizes))
	for i, s := range sizes {
		data[i] = randBytes(t, s)
		cids[i] = makeBlock(t, data[i])
	}
	root := cids[0]

	p, err := NewRotatingPacker(dir, maxSize, root)
	if err != nil {
		t.Fatalf("NewRotatingPacker: %v", err)
	}
	for i := range data {
		if err := p.Add(cids[i], data[i]); err != nil {
			t.Fatalf("Add(%d): %v", i, err)
		}
	}
	cars, err := p.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}

	bi := 0
	for _, car := range cars {
		for _, blk := range car.Blocks {
			if !blk.CID.Equals(cids[bi]) {
				t.Fatalf("block %d cid = %s, want %s", bi, blk.CID, cids[bi])
			}
			if int(blk.Length) != len(data[bi]) {
				t.Fatalf("block %d length = %d, want %d", bi, blk.Length, len(data[bi]))
			}
			got, err := ReadBlockAt(car.Path, blk.Offset, blk.Length)
			if err != nil {
				t.Fatalf("ReadBlockAt(%s, %d, %d): %v", car.Path, blk.Offset, blk.Length, err)
			}
			if !bytes.Equal(got, data[bi]) {
				t.Fatalf("block %d: ReadBlockAt returned wrong bytes", bi)
			}
			bi++
		}
	}
	if bi != len(data) {
		t.Fatalf("read back %d blocks, want %d", bi, len(data))
	}
}

func TestEmptyInput(t *testing.T) {
	dir := t.TempDir()
	root := makeBlock(t, []byte("root"))

	p, err := NewRotatingPacker(dir, 1<<20, root)
	if err != nil {
		t.Fatalf("NewRotatingPacker: %v", err)
	}
	cars, err := p.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if len(cars) != 0 {
		t.Fatalf("got %d cars, want 0", len(cars))
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("dir not empty: %v", entries)
	}
}

func TestOversizedBlockAlone(t *testing.T) {
	dir := t.TempDir()

	const maxSize = 1024
	small1 := randBytes(t, 100)
	big := randBytes(t, 5000) // alone exceeds maxSize
	small2 := randBytes(t, 100)
	data := [][]byte{small1, big, small2}
	cids := make([]cid.Cid, len(data))
	for i := range data {
		cids[i] = makeBlock(t, data[i])
	}
	root := cids[0]

	p, err := NewRotatingPacker(dir, maxSize, root)
	if err != nil {
		t.Fatalf("NewRotatingPacker: %v", err)
	}
	for i := range data {
		if err := p.Add(cids[i], data[i]); err != nil {
			t.Fatalf("Add(%d): %v", i, err)
		}
	}
	cars, err := p.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// The big block must sit alone in its own CAR.
	var bigCar *PackedCar
	for i := range cars {
		for _, blk := range cars[i].Blocks {
			if blk.CID.Equals(cids[1]) {
				bigCar = &cars[i]
			}
		}
	}
	if bigCar == nil {
		t.Fatal("big block not found in any car")
	}
	if len(bigCar.Blocks) != 1 {
		t.Fatalf("car with oversized block has %d blocks, want 1", len(bigCar.Blocks))
	}
	verifyCarWithBlockReader(t, bigCar.Path, root, []cid.Cid{cids[1]}, [][]byte{big})

	got, err := ReadBlockAt(bigCar.Path, bigCar.Blocks[0].Offset, bigCar.Blocks[0].Length)
	if err != nil {
		t.Fatalf("ReadBlockAt: %v", err)
	}
	if !bytes.Equal(got, big) {
		t.Fatal("oversized block bytes mismatch")
	}
}

func TestOversizedSingleBlockOnly(t *testing.T) {
	dir := t.TempDir()

	const maxSize = 512
	big := randBytes(t, 4096)
	c := makeBlock(t, big)

	p, err := NewRotatingPacker(dir, maxSize, c)
	if err != nil {
		t.Fatalf("NewRotatingPacker: %v", err)
	}
	if err := p.Add(c, big); err != nil {
		t.Fatalf("Add: %v", err)
	}
	cars, err := p.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if len(cars) != 1 {
		t.Fatalf("got %d cars, want 1", len(cars))
	}
	verifyCarWithBlockReader(t, cars[0].Path, c, []cid.Cid{c}, [][]byte{big})
}

func TestDeterministicFileNames(t *testing.T) {
	dir := t.TempDir()

	const maxSize = 600
	var cids []cid.Cid
	var data [][]byte
	for i := 0; i < 6; i++ {
		d := randBytes(t, 400)
		data = append(data, d)
		cids = append(cids, makeBlock(t, d))
	}
	root := cids[0]

	p, err := NewRotatingPacker(dir, maxSize, root)
	if err != nil {
		t.Fatalf("NewRotatingPacker: %v", err)
	}
	for i := range data {
		if err := p.Add(cids[i], data[i]); err != nil {
			t.Fatalf("Add(%d): %v", i, err)
		}
	}
	cars, err := p.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	for i, car := range cars {
		want := fmt.Sprintf("car-%06d.car", i+1)
		if got := car.Path; got != "" {
			base := got[len(got)-len(want):]
			if base != want {
				t.Fatalf("car %d path %q, want suffix %q", i, got, want)
			}
		}
	}
}
