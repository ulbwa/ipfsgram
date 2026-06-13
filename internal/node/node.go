// node.go — the libp2p stack the daemon serves content over: a libp2p host
// with a persisted ed25519 identity, a Kademlia DHT in server mode, a Bitswap
// server reading from the read-only blockstore, and a reprovider announcing
// every stored CID to the network.

// Package node bundles the libp2p stack a daemon serves content over: a host
// with a persisted ed25519 identity, a Kademlia DHT in server mode, a Bitswap
// server reading from a read-only boxo blockstore, and a reprovider announcing
// every stored CID. It has no project-internal dependencies; the caller passes
// in the blockstore and the reprovider's KeyChanFunc.
package node

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ipfs/boxo/bitswap"
	bsnet "github.com/ipfs/boxo/bitswap/network/bsnet"
	boxoblockstore "github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/provider"
	datastore "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/discovery"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	routingdisc "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	discutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
	"github.com/libp2p/go-libp2p/p2p/host/autorelay"
	manet "github.com/multiformats/go-multiaddr/net"
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
	Blockstore boxoblockstore.Blockstore
	// ProvideKeys streams every CID that should be announced to the network.
	// It is the reprovider's KeyChanFunc; the caller builds it from the store's
	// CID stream. Required.
	ProvideKeys provider.KeyChanFunc
}

// Node bundles the libp2p stack: host, Kademlia DHT (server mode), Bitswap and
// the reprovider announcing every stored CID.
type Node struct {
	Host     host.Host
	DHT      *dht.IpfsDHT
	Bitswap  *bitswap.Bitswap
	Provider provider.System
	mdns     interface{ Close() error }
}

// mdnsNotifee connects to peers discovered on the local network via mDNS, the
// same zero-config LAN discovery Kubo uses; co-located nodes find each other
// without DHT lookups.
type mdnsNotifee struct{ h host.Host }

func (n *mdnsNotifee) HandlePeerFound(pi peer.AddrInfo) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.h.Connect(ctx, pi); err != nil {
		log.Debug().Str("peer", pi.ID.String()).Err(err).Msg("mdns connect failed")
	}
}

// New brings up the full libp2p node on top of cfg.Blockstore. The reprovider
// streams its CID set from cfg.ProvideKeys.
func New(ctx context.Context, cfg Config) (*Node, error) {
	if cfg.Blockstore == nil {
		return nil, errors.New("node: nil blockstore")
	}
	if cfg.ProvideKeys == nil {
		return nil, errors.New("node: nil ProvideKeys")
	}

	priv, err := loadOrCreateIdentity(cfg.IdentityPath)
	if err != nil {
		return nil, err
	}

	// kadDHT is constructed inside the libp2p.Routing hook (so AutoRelay can use
	// it as a peer source) and captured here for Bitswap and the reprovider.
	var kadDHT *dht.IpfsDHT

	// relayPeerSource feeds AutoRelay with real circuit-relay v2 servers found
	// through DHT routing discovery (peers advertising the "/libp2p/relay"
	// rendezvous), the same way Kubo discovers relays. Feeding arbitrary routing
	// table peers instead would mostly yield non-relays and fail to reserve.
	relayPeerSource := func(ctx context.Context, num int) <-chan peer.AddrInfo {
		out := make(chan peer.AddrInfo)
		go func() {
			defer close(out)
			if kadDHT == nil {
				return
			}
			rd := routingdisc.NewRoutingDiscovery(kadDHT)
			peers, err := discutil.FindPeers(ctx, rd, "/libp2p/relay", discovery.Limit(num))
			if err != nil {
				return
			}
			for _, p := range peers {
				select {
				case out <- p:
				case <-ctx.Done():
					return
				}
			}
		}()
		return out
	}

	h, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(cfg.ListenAddrs...),
		// The default transports already include WebRTC-direct (browser-grade
		// ICE/STUN NAT traversal); we enable it by listening on a webrtc-direct
		// multiaddr (see cmd defaultListen). It makes a node behind a home/CGNAT
		// router directly dialable over UDP where QUIC alone is not — the same
		// mechanism that lets Kubo serve from behind the NAT.
		//
		// NAT traversal: map the port via UPnP/NAT-PMP, run the AutoNAT service,
		// hole-punch (DCUtR), and fall back to circuit relays discovered on the
		// DHT.
		libp2p.NATPortMap(),
		libp2p.EnableNATService(),
		libp2p.EnableHolePunching(),
		libp2p.EnableAutoRelayWithPeerSource(relayPeerSource, autorelay.WithMinInterval(0)),
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			d, derr := dht.New(ctx, h,
				dht.Mode(dht.ModeServer),
				dht.BootstrapPeers(dht.GetDefaultBootstrapPeerAddrInfos()...),
			)
			kadDHT = d
			return d, derr
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("node: start libp2p host: %w", err)
	}
	if kadDHT == nil {
		h.Close()
		return nil, errors.New("node: dht was not initialised")
	}
	if err := kadDHT.Bootstrap(ctx); err != nil {
		kadDHT.Close()
		h.Close()
		return nil, fmt.Errorf("node: bootstrap dht: %w", err)
	}

	logReachability(h)

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
		return nil, fmt.Errorf("node: start reprovider: %w", err)
	}

	// mDNS LAN discovery (same service name as Kubo) so nodes on the same
	// network find each other instantly, without waiting on the DHT.
	mdnsSvc := mdns.NewMdnsService(h, mdns.ServiceName, &mdnsNotifee{h: h})
	if err := mdnsSvc.Start(); err != nil {
		log.Warn().Err(err).Msg("start mdns discovery")
		mdnsSvc = nil
	}

	// Announce every stored CID to the DHT soon after startup, instead of
	// waiting for the first scheduled reprovide (~hours away). Without provider
	// records a remote peer cannot discover that this node holds the content
	// from the CID alone. The announce must wait until the DHT routing table has
	// peers — providing with an empty table fails with "failed to find any peer
	// in table" — and is retried until it succeeds.
	go announceWhenReady(ctx, kadDHT, prov)

	// Re-announce when the node's externally-visible addresses change. Public
	// addresses (UPnP/AutoNAT/WebRTC/relay) appear asynchronously, often after
	// the first announce, so without this the DHT keeps stale records that omit
	// the node's directly-dialable addresses and gateways fall back to slow
	// relays.
	go reannounceOnAddrChange(ctx, h, prov)

	n := &Node{Host: h, DHT: kadDHT, Bitswap: bswap, Provider: prov}
	if mdnsSvc != nil {
		n.mdns = mdnsSvc
	}
	return n, nil
}

// announceWhenReady waits for the DHT routing table to populate, then triggers
// a reprovide so every stored CID is announced to the public DHT. It retries
// until the announce succeeds (or ctx is cancelled), because the first attempt
// at startup races DHT bootstrap and would otherwise fail with an empty table
// and not retry until the next scheduled reprovide hours later.
func announceWhenReady(ctx context.Context, kadDHT *dht.IpfsDHT, prov provider.System) {
	// Wait for at least a few peers in the routing table.
	for kadDHT.RoutingTable().Size() < 1 {
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
	// Give bootstrap a moment to widen the table for better provide placement.
	select {
	case <-ctx.Done():
		return
	case <-time.After(3 * time.Second):
	}

	for {
		err := prov.Reprovide(ctx)
		if err == nil {
			log.Info().Int("dht_peers", kadDHT.RoutingTable().Size()).
				Msg("announced stored CIDs to the DHT")
			return
		}
		if ctx.Err() != nil {
			return
		}
		log.Warn().Err(err).Msg("announce to DHT failed, retrying in 30s")
		select {
		case <-ctx.Done():
			return
		case <-time.After(30 * time.Second):
		}
	}
}

// reannounceOnAddrChange re-provides all stored CIDs whenever the node's public
// addresses or NAT reachability change, so DHT provider records always carry the
// node's current directly-dialable addresses. This matters because public
// addresses (UPnP/AutoNAT/WebRTC) and a Public reachability verdict appear
// asynchronously, usually AFTER the first announce — without a re-announce the
// DHT keeps relay-only records that browser/proxy gateways cannot dial.
// Re-announces are debounced to coalesce startup bursts.
func reannounceOnAddrChange(ctx context.Context, h host.Host, prov provider.System) {
	sub, err := h.EventBus().Subscribe([]any{
		new(event.EvtLocalAddressesUpdated),
		new(event.EvtLocalReachabilityChanged),
	})
	if err != nil {
		log.Warn().Err(err).Msg("subscribe to address-change events")
		return
	}
	defer sub.Close()

	publicSet := func() string {
		var s []string
		for _, a := range h.Addrs() {
			if !manet.IsPrivateAddr(a) {
				s = append(s, a.String())
			}
		}
		sort.Strings(s)
		return strings.Join(s, ",")
	}

	// Start from empty so the first observed set of public addresses always
	// triggers a re-announce, even if those addresses were already present by
	// the time this subscription was established (a common startup race).
	last := ""
	debounce := time.NewTimer(time.Hour)
	debounce.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-sub.Out():
			if !ok {
				return
			}
			if cur := publicSet(); cur != last && cur != "" {
				last = cur
				debounce.Reset(10 * time.Second)
			}
		case <-debounce.C:
			if err := prov.Reprovide(ctx); err != nil && ctx.Err() == nil {
				log.Warn().Err(err).Msg("re-announce after address change")
			} else if ctx.Err() == nil {
				log.Info().Msg("re-announced CIDs after address/reachability change")
			}
		}
	}
}

// logReachability subscribes to the host event bus and logs NAT-reachability
// changes and externally-visible (public/relay) addresses, so an operator can
// tell whether the node became dialable from other networks. The goroutine
// exits when the subscription closes (on host shutdown).
func logReachability(h host.Host) {
	sub, err := h.EventBus().Subscribe([]any{
		new(event.EvtLocalReachabilityChanged),
		new(event.EvtLocalAddressesUpdated),
	})
	if err != nil {
		log.Warn().Err(err).Msg("subscribe to reachability events")
		return
	}
	go func() {
		defer sub.Close()
		for e := range sub.Out() {
			switch ev := e.(type) {
			case event.EvtLocalReachabilityChanged:
				log.Info().Str("reachability", ev.Reachability.String()).
					Msg("nat reachability changed")
			case event.EvtLocalAddressesUpdated:
				var public []string
				for _, ua := range ev.Current {
					if a := ua.Address; a != nil && !manet.IsPrivateAddr(a) {
						public = append(public, a.String())
					}
				}
				if len(public) > 0 {
					log.Info().Strs("addrs", public).
						Msg("externally reachable addresses")
				}
			}
		}
	}()
}

// Close shuts the stack down in reverse start order, joining any errors.
func (n *Node) Close() error {
	var errs []error
	if n.mdns != nil {
		if err := n.mdns.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close mdns: %w", err))
		}
	}
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
