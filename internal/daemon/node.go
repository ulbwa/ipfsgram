// node.go — the libp2p stack the daemon serves content over: a libp2p host
// with a persisted ed25519 identity, a Kademlia DHT in server mode, a Bitswap
// server reading from the read-only blockstore, and a reprovider announcing
// every stored CID to the network.

package daemon

import (
	"context"
	"errors"
	"fmt"

	"github.com/ipfs/boxo/bitswap"
	bsnet "github.com/ipfs/boxo/bitswap/network/bsnet"
	boxoblockstore "github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/provider"
	datastore "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
)

// NodeConfig configures a Node.
type NodeConfig struct {
	// IdentityPath is the on-disk path of the ed25519 libp2p private key. It is
	// created with 0600 permissions on first run and loaded on subsequent runs.
	IdentityPath string
	// ListenAddrs are the multiaddrs the host listens on (e.g.
	// "/ip4/0.0.0.0/tcp/4001"). May be empty.
	ListenAddrs []string
	// Blockstore is the boxo blockstore Bitswap serves blocks from.
	Blockstore boxoblockstore.Blockstore
	// ProvideKeys streams every CID that should be announced to the network.
	// It is the reprovider's KeyChanFunc; build it with ProvideKeys from the
	// store's CID stream. Required.
	ProvideKeys provider.KeyChanFunc
}

// Node bundles the libp2p stack: host, Kademlia DHT (server mode), Bitswap and
// the reprovider announcing every stored CID.
type Node struct {
	Host     host.Host
	DHT      *dht.IpfsDHT
	Bitswap  *bitswap.Bitswap
	Provider provider.System
}

// NewNode brings up the full libp2p node on top of cfg.Blockstore. The
// reprovider streams its CID set from cfg.ProvideKeys.
func NewNode(ctx context.Context, cfg NodeConfig) (*Node, error) {
	if cfg.Blockstore == nil {
		return nil, errors.New("daemon: nil blockstore")
	}
	if cfg.ProvideKeys == nil {
		return nil, errors.New("daemon: nil ProvideKeys")
	}

	priv, err := loadOrCreateIdentity(cfg.IdentityPath)
	if err != nil {
		return nil, err
	}

	h, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(cfg.ListenAddrs...),
	)
	if err != nil {
		return nil, fmt.Errorf("daemon: start libp2p host: %w", err)
	}

	kadDHT, err := dht.New(ctx, h,
		dht.Mode(dht.ModeServer),
		dht.BootstrapPeers(dht.GetDefaultBootstrapPeerAddrInfos()...),
	)
	if err != nil {
		h.Close()
		return nil, fmt.Errorf("daemon: start dht: %w", err)
	}
	if err := kadDHT.Bootstrap(ctx); err != nil {
		kadDHT.Close()
		h.Close()
		return nil, fmt.Errorf("daemon: bootstrap dht: %w", err)
	}

	bswap := bitswap.New(ctx, bsnet.NewFromIpfsHost(h), kadDHT, cfg.Blockstore)

	// The provide queue is in-memory only: on restart the reprovider streams
	// the full CID set from the store again, so nothing is lost.
	prov, err := provider.New(dssync.MutexWrap(datastore.NewMapDatastore()),
		provider.Online(kadDHT),
		provider.KeyProvider(cfg.ProvideKeys),
		provider.ReproviderInterval(provider.DefaultReproviderInterval),
	)
	if err != nil {
		bswap.Close()
		kadDHT.Close()
		h.Close()
		return nil, fmt.Errorf("daemon: start reprovider: %w", err)
	}

	return &Node{Host: h, DHT: kadDHT, Bitswap: bswap, Provider: prov}, nil
}

// Close shuts the stack down in reverse start order, joining any errors.
func (n *Node) Close() error {
	var errs []error
	if err := n.Provider.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close provider: %w", err))
	}
	if err := n.Bitswap.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close bitswap: %w", err))
	}
	if err := n.DHT.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close dht: %w", err))
	}
	if err := n.Host.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close host: %w", err))
	}
	return errors.Join(errs...)
}
