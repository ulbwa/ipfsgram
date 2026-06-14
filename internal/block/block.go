// block.go — the in-memory content block: a CID and its raw payload, the unit
// the file/network sources produce and the publish pipeline consumes.

// Package block provides the content sources for the publish pipeline: FromFile
// imports a local file as a UnixFS DAG, FromNetwork pulls a DAG from the public
// IPFS network. Both return the DAG root and all blocks in traversal order.
package block

import "github.com/ipfs/go-cid"

// Block is a block held in memory: its CID and raw payload.
type Block struct {
	CID  cid.Cid
	Data []byte
}
