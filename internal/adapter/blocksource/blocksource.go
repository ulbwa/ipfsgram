// Package blocksource provides port.BlockSource implementations that load a DAG
// into a root CID and an ordered slice of domain.RawBlock:
//
//   - NewFile imports a local file as a UnixFS DAG (256 KiB fixed chunker,
//     balanced layout, raw leaves, CIDv1) over an in-memory blockstore.
//   - NewNetwork fetches a DAG by CID from the public IPFS network using a
//     temporary lightweight libp2p node (host + DHT client + Bitswap client).
//
// Both implementations hold every block in memory (in-memory blockstore plus
// the returned slice): acceptable for v1, with memory usage proportional to the
// content size.
package blocksource

import (
	"context"
	"fmt"
	"os"
	"sync"

	bsclient "github.com/ipfs/boxo/bitswap/client"
	"github.com/ipfs/boxo/bitswap/network/bsnet"
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
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	mh "github.com/multiformats/go-multihash"

	"github.com/ulbwa/ipfsgram/internal/domain"
	"github.com/ulbwa/ipfsgram/internal/port"
)

// chunkSize is the fixed UnixFS chunk size (256 KiB).
const chunkSize = 256 << 10

var (
	_ port.BlockSource = (*fileSource)(nil)
	_ port.BlockSource = (*networkSource)(nil)
)

// fileSource imports a local file as a UnixFS DAG.
type fileSource struct {
	path string
}

// NewFile returns a port.BlockSource that imports the file at path into a
// balanced UnixFS DAG (256 KiB fixed chunker, raw leaves, CIDv1) over an
// in-memory blockstore. Memory usage is proportional to the file size.
func NewFile(path string) port.BlockSource {
	return &fileSource{path: path}
}

// Load chunks the file into a balanced UnixFS DAG over an in-memory blockstore
// and returns the root plus all blocks in DAG traversal order.
func (s *fileSource) Load(ctx context.Context) (cid.Cid, []domain.RawBlock, error) {
	f, err := os.Open(s.path)
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

// networkSource fetches a DAG by CID from the public IPFS network.
type networkSource struct {
	root cid.Cid
}

// NewNetwork returns a port.BlockSource that fetches the DAG rooted at c from
// the public IPFS network using a temporary lightweight libp2p node. All blocks
// are collected into memory; memory usage is proportional to the DAG size.
func NewNetwork(c cid.Cid) port.BlockSource {
	return &networkSource{root: c}
}

// Load spins up a temporary lightweight IPFS client node (libp2p host + DHT
// client + Bitswap client), fetches the whole DAG rooted at s.root into memory,
// and tears the temporary node down before returning.
func (s *networkSource) Load(ctx context.Context) (cid.Cid, []domain.RawBlock, error) {
	host, err := libp2p.New(libp2p.NoListenAddrs)
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("libp2p host: %w", err)
	}
	defer host.Close()

	bootstrap := dht.GetDefaultBootstrapPeerAddrInfos()
	router, err := dht.New(ctx, host, dht.Mode(dht.ModeClient), dht.BootstrapPeers(bootstrap...))
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("dht client: %w", err)
	}
	defer router.Close()

	var wg sync.WaitGroup
	for _, pi := range bootstrap {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = host.Connect(ctx, pi) // best effort
		}()
	}
	wg.Wait()
	if err := router.Bootstrap(ctx); err != nil {
		return cid.Undef, nil, fmt.Errorf("dht bootstrap: %w", err)
	}

	bstore := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	net := bsnet.NewFromIpfsHost(host)
	bswap := bsclient.New(ctx, net, router, bstore)
	net.Start(bswap)
	defer net.Stop()
	defer bswap.Close()

	bserv := blockservice.New(bstore, bswap)
	defer bserv.Close()
	dserv := merkledag.NewDAGService(bserv)

	blocks, err := collectDAG(ctx, merkledag.NewSession(ctx, dserv), s.root)
	if err != nil {
		return cid.Undef, nil, err
	}
	return s.root, blocks, nil
}

// collectDAG walks the DAG from root depth-first (preorder, links left-to-right),
// visiting every CID once, and returns the blocks in that order.
func collectDAG(ctx context.Context, ng ipld.NodeGetter, root cid.Cid) ([]domain.RawBlock, error) {
	var blocks []domain.RawBlock
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
		blocks = append(blocks, domain.RawBlock{CID: c, Data: nd.RawData()})

		links := nd.Links()
		for i := len(links) - 1; i >= 0; i-- { // reversed: pop order = left-to-right
			if !seen[links[i].Cid] {
				stack = append(stack, links[i].Cid)
			}
		}
	}
	return blocks, nil
}
