// network.go — FromNetwork fetches a whole DAG from the public IPFS network
// through a throwaway lightweight client node (libp2p host + DHT + Bitswap).

package block

import (
	"context"
	"fmt"
	"sync"

	bsclient "github.com/ipfs/boxo/bitswap/client"
	"github.com/ipfs/boxo/bitswap/network/bsnet"
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
)

// FromNetwork spins up a temporary lightweight IPFS client node (libp2p host +
// DHT client + Bitswap client), fetches the whole DAG rooted at root into
// memory, and tears the temporary node down before returning. Memory usage is
// proportional to the DAG size.
func FromNetwork(ctx context.Context, root cid.Cid) (cid.Cid, []Block, error) {
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

	// Start the bitswap network before dialing anyone: the client's peer
	// manager only learns about peers from connection events, so peers
	// connected before Start would never receive our wantlist broadcasts.
	bstore := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	net := bsnet.NewFromIpfsHost(host)
	bswap := bsclient.New(ctx, net, router, bstore)
	net.Start(bswap)
	defer net.Stop()
	defer bswap.Close()

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

	bserv := blockservice.New(bstore, bswap)
	defer bserv.Close()
	dserv := merkledag.NewDAGService(bserv)

	blocks, err := collectDAG(ctx, merkledag.NewSession(ctx, dserv), root)
	if err != nil {
		return cid.Undef, nil, err
	}
	return root, blocks, nil
}
