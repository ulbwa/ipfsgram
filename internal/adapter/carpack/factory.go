package carpack

import (
	"github.com/ipfs/go-cid"

	"github.com/ulbwa/ipfsgram/internal/port"
)

// Factory constructs Packers writing into a directory, capped at a maximum
// size, with the given DAG root in each CAR header. It implements
// port.PackerFactory.
type Factory struct{}

// NewFactory returns a Factory.
func NewFactory() port.PackerFactory { return Factory{} }

// New returns a rotating Packer writing into dir, capped at maxSize, with root
// as the single root CID in every produced CAR header.
func (Factory) New(dir string, maxSize int64, root cid.Cid) (port.Packer, error) {
	return newPacker(dir, maxSize, root)
}
