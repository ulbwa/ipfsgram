package cli

// Block sources for `ipfsgram add`: a local file imported as UnixFS, or a DAG
// fetched from the public IPFS network with a temporary lightweight node.
//
// Both paths hold all blocks in memory (in-memory blockstore / entry slice):
// acceptable for v1, memory usage is proportional to the content size.

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	chunker "github.com/ipfs/boxo/chunker"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	uihelpers "github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	mh "github.com/multiformats/go-multihash"

	bsclient "github.com/ipfs/boxo/bitswap/client"
	"github.com/ipfs/boxo/bitswap/network/bsnet"
)

// chunkSize is the fixed UnixFS chunk size (256 KiB).
const chunkSize = 256 << 10

// blockEntry is a single DAG block in traversal order.
type blockEntry struct {
	cid  cid.Cid
	data []byte
}

// importLocalFile chunks the file into a balanced UnixFS DAG (raw leaves,
// CIDv1) over an in-memory blockstore and returns the root plus all blocks
// in DAG traversal order.
func importLocalFile(ctx context.Context, path string) (cid.Cid, []blockEntry, error) {
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

	entries, err := collectDAG(ctx, dserv, root.Cid())
	if err != nil {
		return cid.Undef, nil, err
	}
	return root.Cid(), entries, nil
}

// fetchFromNetwork spins up a temporary lightweight IPFS client node (libp2p
// host + DHT client + bitswap client) and fetches the whole DAG rooted at
// root into memory.
func fetchFromNetwork(ctx context.Context, root cid.Cid) ([]blockEntry, error) {
	host, err := libp2p.New(libp2p.NoListenAddrs)
	if err != nil {
		return nil, fmt.Errorf("libp2p host: %w", err)
	}
	defer host.Close()

	bootstrap := dht.GetDefaultBootstrapPeerAddrInfos()
	router, err := dht.New(ctx, host, dht.Mode(dht.ModeClient), dht.BootstrapPeers(bootstrap...))
	if err != nil {
		return nil, fmt.Errorf("dht client: %w", err)
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
		return nil, fmt.Errorf("dht bootstrap: %w", err)
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

	return collectDAG(ctx, merkledag.NewSession(ctx, dserv), root)
}

// collectDAG walks the DAG from root depth-first (preorder, links
// left-to-right), visiting every CID once, and returns the blocks in that
// order.
func collectDAG(ctx context.Context, ng ipld.NodeGetter, root cid.Cid) ([]blockEntry, error) {
	var entries []blockEntry
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
			return nil, fmt.Errorf("получение блока %s: %w", c, err)
		}
		entries = append(entries, blockEntry{cid: c, data: nd.RawData()})

		links := nd.Links()
		for i := len(links) - 1; i >= 0; i-- { // reversed: pop order = left-to-right
			if !seen[links[i].Cid] {
				stack = append(stack, links[i].Cid)
			}
		}
	}
	return entries, nil
}
