// Package libp2pnode wires the full libp2p stack IPFSgram serves content over:
// a libp2p host with a persisted ed25519 identity, a Kademlia DHT in server
// mode, a Bitswap server reading from a provided boxo blockstore, and a
// reprovider that announces every stored CID to the network.
//
// The CID set the reprovider announces is supplied as a provider.KeyChanFunc
// dependency (see Config.ProvideKeys), so this package never depends on the
// repository/database layer: the daemon wiring builds the key function from
// port.BlockRepository.StreamAllCIDs and passes it in.
package libp2pnode

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/ipfs/boxo/bitswap"
	bsnet "github.com/ipfs/boxo/bitswap/network/bsnet"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/provider"
	datastore "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/rs/zerolog/log"
)

// Config configures a Node.
type Config struct {
	// IdentityPath is the on-disk path of the ed25519 libp2p private key. It is
	// created with 0600 permissions on first run and loaded on subsequent runs.
	IdentityPath string
	// ListenAddrs are the multiaddrs the host listens on (e.g.
	// "/ip4/0.0.0.0/tcp/4001"). May be empty.
	ListenAddrs []string
	// Blockstore is the boxo blockstore Bitswap serves blocks from.
	Blockstore blockstore.Blockstore
	// ProvideKeys streams every CID that should be announced to the network.
	// It is the reprovider's KeyChanFunc; the daemon builds it from
	// port.BlockRepository.StreamAllCIDs. Required.
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

// New brings up the full libp2p node on top of cfg.Blockstore. The reprovider
// streams its CID set from cfg.ProvideKeys.
func New(ctx context.Context, cfg Config) (*Node, error) {
	if cfg.Blockstore == nil {
		return nil, errors.New("libp2pnode: nil blockstore")
	}
	if cfg.ProvideKeys == nil {
		return nil, errors.New("libp2pnode: nil ProvideKeys")
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
		return nil, fmt.Errorf("libp2pnode: start libp2p host: %w", err)
	}

	kadDHT, err := dht.New(ctx, h,
		dht.Mode(dht.ModeServer),
		dht.BootstrapPeers(dht.GetDefaultBootstrapPeerAddrInfos()...),
	)
	if err != nil {
		h.Close()
		return nil, fmt.Errorf("libp2pnode: start dht: %w", err)
	}
	if err := kadDHT.Bootstrap(ctx); err != nil {
		kadDHT.Close()
		h.Close()
		return nil, fmt.Errorf("libp2pnode: bootstrap dht: %w", err)
	}

	bswap := bitswap.New(ctx, bsnet.NewFromIpfsHost(h), kadDHT, cfg.Blockstore)

	// The provide queue is in-memory only: on restart the reprovider streams
	// the full CID set from the repository again, so nothing is lost.
	prov, err := provider.New(dssync.MutexWrap(datastore.NewMapDatastore()),
		provider.Online(kadDHT),
		provider.KeyProvider(cfg.ProvideKeys),
		provider.ReproviderInterval(provider.DefaultReproviderInterval),
	)
	if err != nil {
		bswap.Close()
		kadDHT.Close()
		h.Close()
		return nil, fmt.Errorf("libp2pnode: start reprovider: %w", err)
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

// loadOrCreateIdentity loads the ed25519 libp2p identity from path, creating it
// with 0600 permissions on first start.
func loadOrCreateIdentity(path string) (crypto.PrivKey, error) {
	raw, err := os.ReadFile(path)
	switch {
	case err == nil:
		priv, err := crypto.UnmarshalPrivateKey(raw)
		if err != nil {
			return nil, fmt.Errorf("libp2pnode: parse identity key %s: %w", path, err)
		}
		return priv, nil
	case errors.Is(err, fs.ErrNotExist):
		// fall through to creation
	default:
		return nil, fmt.Errorf("libp2pnode: read identity key %s: %w", path, err)
	}

	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("libp2pnode: generate identity key: %w", err)
	}
	raw, err = crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("libp2pnode: marshal identity key: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("libp2pnode: create data dir: %w", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return nil, fmt.Errorf("libp2pnode: write identity key %s: %w", path, err)
	}
	log.Info().Str("path", path).Msg("generated new libp2p identity")
	return priv, nil
}
