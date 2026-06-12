package carpack

import (
	"fmt"
	"io"
	"os"

	"github.com/ulbwa/ipfsgram/internal/port"
)

// Compile-time assertion that Reader satisfies its port.
var _ port.BlockReader = (*Reader)(nil)

// Reader serves a block payload from a CAR file by absolute offset/length
// without parsing the CAR. It implements port.BlockReader.
type Reader struct{}

// NewReader returns a Reader.
func NewReader() port.BlockReader { return Reader{} }

// ReadAt reads exactly length bytes starting at offset off from the file at
// path. It is the read-path counterpart of Packer: given a domain.PackedBlock's
// Offset and Length it returns the original block payload without any CAR
// parsing.
func (Reader) ReadAt(path string, off int64, length int32) ([]byte, error) {
	if off < 0 {
		return nil, fmt.Errorf("carpack: negative offset %d", off)
	}
	if length < 0 {
		return nil, fmt.Errorf("carpack: negative length %d", length)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("carpack: open car file: %w", err)
	}
	defer f.Close()

	buf := make([]byte, length)
	if _, err := f.ReadAt(buf, off); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, fmt.Errorf("carpack: read %d bytes at %d from %s: %w", length, off, path, err)
	}
	return buf, nil
}
