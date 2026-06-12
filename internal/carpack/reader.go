package carpack

import (
	"fmt"
	"io"
	"os"
)

// ReadBlockAt reads exactly length bytes starting at offset off from the file
// at path. It is the read-path counterpart of RotatingPacker: given a
// PackedBlock's Offset and Length it returns the original block payload
// without any CAR parsing.
func ReadBlockAt(path string, off int64, length int32) ([]byte, error) {
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
