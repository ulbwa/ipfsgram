// Package carpack writes IPFS blocks into size-capped CARv1 archives and
// records, for every block, the absolute byte offset of its payload inside
// the archive. This lets a reader serve a block later with a single
// ReadAt(offset, length) call, without parsing the CAR on the read path.
package carpack

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-varint"
)

// PackedBlock describes a single block inside a finished CAR file.
type PackedBlock struct {
	CID cid.Cid
	// Offset is the absolute file offset of the first byte of the block
	// payload (the data bytes that follow the CID inside the section).
	Offset int64
	// Length is the payload length in bytes.
	Length int32
}

// PackedCar is a finished CARv1 file in the packer's directory.
type PackedCar struct {
	Path   string
	Size   int64
	Blocks []PackedBlock
}

// RotatingPacker writes blocks into CARv1 files, rotating to a new file when
// the configured size cap would be exceeded. Every produced file carries the
// same root in its header. The limit is best effort: a single block whose
// section does not fit into maxSize is written alone into its own CAR.
//
// RotatingPacker is not safe for concurrent use.
type RotatingPacker struct {
	dir     string
	maxSize int64
	header  []byte // encoded CARv1 header (uvarint length prefix + dag-cbor)

	file     *os.File
	size     int64 // bytes written to the current file
	seq      int   // 1-based index of the current file
	current  PackedCar
	finished []PackedCar
	done     bool
}

// NewRotatingPacker creates a packer that writes CAR files named
// car-000001.car, car-000002.car, ... inside dir. Each file's CARv1 header
// declares root as the single root CID. Files are only created once the
// first block is added.
func NewRotatingPacker(dir string, maxSize int64, root cid.Cid) (*RotatingPacker, error) {
	if maxSize <= 0 {
		return nil, fmt.Errorf("carpack: maxSize must be positive, got %d", maxSize)
	}
	if !root.Defined() {
		return nil, errors.New("carpack: root cid is undefined")
	}
	if st, err := os.Stat(dir); err != nil {
		return nil, fmt.Errorf("carpack: dir: %w", err)
	} else if !st.IsDir() {
		return nil, fmt.Errorf("carpack: %s is not a directory", dir)
	}
	return &RotatingPacker{
		dir:     dir,
		maxSize: maxSize,
		header:  encodeCarV1Header(root),
	}, nil
}

// Add appends a block to the current CAR file, rotating to a new file first
// if the block's section would push the current file past the size cap and
// the current file already holds at least one block.
func (p *RotatingPacker) Add(c cid.Cid, data []byte) error {
	if p.done {
		return errors.New("carpack: Add after Finish")
	}
	if !c.Defined() {
		return errors.New("carpack: block cid is undefined")
	}

	cidBytes := c.Bytes()
	sectionLen := uint64(len(cidBytes) + len(data))
	prefix := varint.ToUvarint(sectionLen)
	sectionSize := int64(len(prefix)) + int64(sectionLen)

	if p.file != nil && p.size+sectionSize > p.maxSize && len(p.current.Blocks) > 0 {
		if err := p.rotate(); err != nil {
			return err
		}
	}
	if p.file == nil {
		if err := p.openNext(); err != nil {
			return err
		}
	}

	payloadOffset := p.size + int64(len(prefix)) + int64(len(cidBytes))

	if _, err := p.file.Write(prefix); err != nil {
		return fmt.Errorf("carpack: write section length: %w", err)
	}
	if _, err := p.file.Write(cidBytes); err != nil {
		return fmt.Errorf("carpack: write cid: %w", err)
	}
	if _, err := p.file.Write(data); err != nil {
		return fmt.Errorf("carpack: write payload: %w", err)
	}
	p.size += sectionSize
	p.current.Blocks = append(p.current.Blocks, PackedBlock{
		CID:    c,
		Offset: payloadOffset,
		Length: int32(len(data)),
	})
	return nil
}

// Finish closes the current file, if any, and returns all produced CAR files
// in the order they were written. Calling Finish with no blocks added returns
// an empty slice. The packer must not be used after Finish.
func (p *RotatingPacker) Finish() ([]PackedCar, error) {
	if p.done {
		return nil, errors.New("carpack: Finish called twice")
	}
	p.done = true
	if p.file != nil {
		if err := p.rotate(); err != nil {
			return nil, err
		}
	}
	return p.finished, nil
}

// openNext creates the next CAR file and writes its header.
func (p *RotatingPacker) openNext() error {
	p.seq++
	path := filepath.Join(p.dir, fmt.Sprintf("car-%06d.car", p.seq))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("carpack: create car file: %w", err)
	}
	if _, err := f.Write(p.header); err != nil {
		f.Close()
		os.Remove(path)
		return fmt.Errorf("carpack: write car header: %w", err)
	}
	p.file = f
	p.size = int64(len(p.header))
	p.current = PackedCar{Path: path}
	return nil
}

// rotate finalizes the current file and records it.
func (p *RotatingPacker) rotate() error {
	f := p.file
	p.file = nil
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("carpack: sync car file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("carpack: close car file: %w", err)
	}
	p.current.Size = p.size
	p.finished = append(p.finished, p.current)
	p.current = PackedCar{}
	p.size = 0
	return nil
}

// encodeCarV1Header returns the CARv1 header for a single root: a uvarint
// length prefix followed by the dag-cbor map {"roots": [root], "version": 1}.
// Keys are emitted in dag-cbor canonical order (length-first, then
// lexicographic): "roots" before "version". A CID inside dag-cbor is encoded
// as tag 42 over a byte string holding 0x00 (multibase identity prefix)
// followed by the binary CID.
func encodeCarV1Header(root cid.Cid) []byte {
	var body []byte
	body = append(body, 0xa2) // map(2)

	// "roots": [ tag(42) bytes(0x00 || cid) ]
	body = append(body, 0x65) // text(5)
	body = append(body, "roots"...)
	body = append(body, 0x81)       // array(1)
	body = append(body, 0xd8, 0x2a) // tag(42)
	cidBytes := root.Bytes()
	body = appendCborBytesHeader(body, uint64(len(cidBytes)+1))
	body = append(body, 0x00) // identity multibase prefix
	body = append(body, cidBytes...)

	// "version": 1
	body = append(body, 0x67) // text(7)
	body = append(body, "version"...)
	body = append(body, 0x01) // unsigned(1)

	return append(varint.ToUvarint(uint64(len(body))), body...)
}

// appendCborBytesHeader appends the CBOR major-type-2 (byte string) header
// for a payload of n bytes.
func appendCborBytesHeader(dst []byte, n uint64) []byte {
	const major = 2 << 5
	switch {
	case n < 24:
		return append(dst, major|byte(n))
	case n <= 0xff:
		return append(dst, major|24, byte(n))
	case n <= 0xffff:
		return append(dst, major|25, byte(n>>8), byte(n))
	case n <= 0xffffffff:
		return append(dst, major|26, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	default:
		return append(dst, major|27,
			byte(n>>56), byte(n>>48), byte(n>>40), byte(n>>32),
			byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	}
}
