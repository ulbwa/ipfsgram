package daemon

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
	bstore "github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/provider"
	datastore "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/rs/zerolog/log"
)

// Blockstore must satisfy the boxo interface Bitswap serves from.
var _ bstore.Blockstore = (*Blockstore)(nil)

// identityFile is the on-disk name of the libp2p private key inside data-dir.
const identityFile = "identity.key"

// Node bundles the libp2p stack: host, Kademlia DHT (server mode), Bitswap
// and the reprovider announcing every stored CID.
type Node struct {
	Host     host.Host
	DHT      *dht.IpfsDHT
	Bitswap  *bitswap.Bitswap
	Provider provider.System
}

// StartNode brings up the full IPFS node on top of the read-only blockstore.
// keys feeds the reprovider with every stored CID.
func StartNode(ctx context.Context, dataDir string, listen []string, blockstore *Blockstore, keys provider.KeyChanFunc) (*Node, error) {
	priv, err := loadOrCreateIdentity(filepath.Join(dataDir, identityFile))
	if err != nil {
		return nil, err
	}

	h, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(listen...),
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

	bswap := bitswap.New(ctx, bsnet.NewFromIpfsHost(h), kadDHT, blockstore)

	// The provide queue is in-memory only: on restart the reprovider streams
	// the full CID set from PostgreSQL again, so nothing is lost.
	prov, err := provider.New(dssync.MutexWrap(datastore.NewMapDatastore()),
		provider.Online(kadDHT),
		provider.KeyProvider(keys),
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

// Close shuts the stack down in reverse start order.
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

// loadOrCreateIdentity loads the ed25519 libp2p identity from path, creating
// it with 0600 permissions on first start.
func loadOrCreateIdentity(path string) (crypto.PrivKey, error) {
	raw, err := os.ReadFile(path)
	switch {
	case err == nil:
		priv, err := crypto.UnmarshalPrivateKey(raw)
		if err != nil {
			return nil, fmt.Errorf("daemon: parse identity key %s: %w", path, err)
		}
		return priv, nil
	case errors.Is(err, fs.ErrNotExist):
		// fall through to creation
	default:
		return nil, fmt.Errorf("daemon: read identity key %s: %w", path, err)
	}

	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("daemon: generate identity key: %w", err)
	}
	raw, err = crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("daemon: marshal identity key: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("daemon: create data dir: %w", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return nil, fmt.Errorf("daemon: write identity key %s: %w", path, err)
	}
	log.Info().Str("path", path).Msg("generated new libp2p identity")
	return priv, nil
}
