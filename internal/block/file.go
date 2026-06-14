// file.go — FromFile imports a local file into a balanced UnixFS DAG over an
// in-memory blockstore, plus the shared collectDAG traversal helper.

package block

import (
	"context"
	"fmt"
	"os"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	chunker "github.com/ipfs/boxo/chunker"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	uihelpers "github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	"github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	ipld "github.com/ipfs/go-ipld-format"
	mh "github.com/multiformats/go-multihash"
)

// chunkSize is the fixed UnixFS chunk size (256 KiB).
const chunkSize = 256 << 10

// FromFile imports the file at path into a balanced UnixFS DAG (256 KiB fixed
// chunker, raw leaves, CIDv1) over an in-memory blockstore and returns the root
// plus all blocks in DAG traversal order. Memory usage is proportional to the
// file size.
func FromFile(ctx context.Context, path string) (cid.Cid, []Block, error) {
	f, err := os.Open(path)
	if err != nil {
		return cid.Undef, nil, err
	}
	defer f.Close()

	bstore := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	bserv := blockservice.New(bstore, offline.Exchange(bstore))
	dserv := merkledag.NewDAGService(bserv)

	params := uihelpers.DagBuilderParams{
		Maxlinks:   uihelpers.DefaultLinksPerBlock,
		RawLeaves:  true,
		CidBuilder: cid.V1Builder{Codec: cid.DagProtobuf, MhType: mh.SHA2_256},
		Dagserv:    dserv,
	}
	dbh, err := params.New(chunker.NewSizeSplitter(f, chunkSize))
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("unixfs import: %w", err)
	}
	root, err := balanced.Layout(dbh)
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("unixfs import: %w", err)
	}

	blocks, err := collectDAG(ctx, dserv, root.Cid())
	if err != nil {
		return cid.Undef, nil, err
	}
	return root.Cid(), blocks, nil
}

// collectDAG walks the DAG from root depth-first (preorder, links left-to-right),
// visiting every CID once, and returns the blocks in that order.
func collectDAG(ctx context.Context, ng ipld.NodeGetter, root cid.Cid) ([]Block, error) {
	var blocks []Block
	seen := make(map[cid.Cid]bool)
	stack := []cid.Cid{root}
	for len(stack) > 0 {
		c := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[c] {
			continue
		}
		seen[c] = true

		nd, err := ng.Get(ctx, c)
		if err != nil {
			return nil, fmt.Errorf("get block %s: %w", c, err)
		}
		blocks = append(blocks, Block{CID: c, Data: nd.RawData()})

		links := nd.Links()
		for i := len(links) - 1; i >= 0; i-- { // reversed: pop order = left-to-right
			if !seen[links[i].Cid] {
				stack = append(stack, links[i].Cid)
			}
		}
	}
	return blocks, nil
}
