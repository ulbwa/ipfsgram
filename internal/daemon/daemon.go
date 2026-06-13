// daemon.go — Config and Run: assembles the disk cache, the read-only
// blockstore and the libp2p node from the caller-supplied Telegram transport,
// then blocks until the context is cancelled and shuts down gracefully.
// Flag/env parsing and transport assembly stay in cmd/ipfsgram; Run receives
// the already-resolved Config, an open store and the transport.

package daemon

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/event"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/rs/zerolog/log"

	"github.com/ulbwa/ipfsgram/internal/cache"
	"github.com/ulbwa/ipfsgram/internal/node"
	"github.com/ulbwa/ipfsgram/internal/store"
)

// Config carries the resolved daemon settings. cmd/ipfsgram fills it from
// flags and environment fallbacks; Run applies the remaining defaults
// (CacheDir defaults to <DataDir>/cache).
type Config struct {
	// DSN is the PostgreSQL connection string, used for a dedicated
	// LISTEN/NOTIFY connection that announces newly published content instantly.
	DSN string
	// DataDir is the daemon state directory (identity key, MTProto sessions).
	DataDir string
	// CacheDir is the CAR disk cache directory; empty means <DataDir>/cache.
	CacheDir string
	// CacheMaxBytes bounds the cache for the "lru" strategy.
	CacheMaxBytes int64
	// CacheStrategy selects the eviction policy: "lru" (also the default for
	// an empty value) or "ttl".
	CacheStrategy string
	// CacheTTL is the entry lifetime for the "ttl" strategy.
	CacheTTL time.Duration
	// Listen are the libp2p listen multiaddrs.
	Listen []string
	// AutoTLS enables p2p-forge (libp2p.direct) secure-WebSocket AutoTLS,
	// mirroring Kubo. Default true.
	AutoTLS bool
	// DelegatedRouting enables the HTTP delegated router (IPNI) alongside the
	// DHT, mirroring Kubo's Routing.DelegatedRouters. Default true.
	DelegatedRouting bool
	// Relays are circuit-relay-v2 server multiaddrs (each ending in /p2p/<id>).
	// They are added to the DHT bootstrap set and offered to AutoRelay as static
	// relays, mirroring Kubo's Bootstrap + Swarm.RelayClient.StaticRelays, so a
	// node behind NAT gets a public /p2p-circuit address through them.
	Relays []string
}

// Run wires the disk cache, blockstore and libp2p node on top of the supplied
// Telegram transport, then blocks until ctx is cancelled (SIGINT/SIGTERM) and
// shuts down gracefully. The store must be connected and schema-checked, and
// the transport assembled, by the caller.
func Run(ctx context.Context, cfg Config, st *store.Store, tr transport) error {
	if cfg.CacheDir == "" {
		cfg.CacheDir = filepath.Join(cfg.DataDir, "cache")
	}

	carCache, err := buildCache(cfg)
	if err != nil {
		return err
	}
	defer func() {
		if err := carCache.Close(); err != nil {
			log.Error().Err(err).Msg("close cache")
		}
	}()

	bs := NewBlockstore(Deps{
		Blocks:      st,
		Cars:        st,
		Bots:        st,
		Channels:    st,
		Transport:   tr,
		Cache:       carCache,
		Logger:      log.Logger,
		CacheTmpDir: filepath.Join(cfg.CacheDir, "tmp"),
	})

	n, err := node.New(ctx, node.Config{
		IdentityPath:     filepath.Join(cfg.DataDir, "identity.key"),
		ListenAddrs:      cfg.Listen,
		Blockstore:       bs,
		ProvideKeys:      ProvideKeys(st),
		DataDir:          cfg.DataDir,
		AutoTLS:          cfg.AutoTLS,
		DelegatedRouting: cfg.DelegatedRouting,
		BootstrapPeers:   cfg.Relays,
		StaticRelays:     cfg.Relays,
	})
	if err != nil {
		return err
	}
	defer func() {
		if err := n.Close(); err != nil {
			log.Error().Err(err).Msg("shutdown")
		}
	}()

	logEvent := log.Info().Str("peer_id", n.Host.ID().String())
	for _, addr := range n.Host.Addrs() {
		logEvent = logEvent.Str("listen", addr.String())
	}
	logEvent.Msg("daemon started")

	// Announce pin roots to the DHT first (a handful of CIDs), so gateways and
	// the retrieval checker can discover the content by its root CID within
	// seconds, without waiting for the full per-block reprovide to finish.
	go provideRoots(ctx, n, st)

	// Announce newly published content the instant the CLI commits it, via
	// PostgreSQL LISTEN/NOTIFY, rather than waiting for the next reprovide.
	if cfg.DSN != "" {
		go listenAndProvide(ctx, cfg.DSN, n)
	}

	<-ctx.Done()
	log.Info().Msg("shutting down")
	return nil
}

// provideRoots waits for the DHT routing table to populate, announces each pin's
// root CID to the DHT, and re-announces the roots whenever the node's public
// addresses change. Roots are the entry points gateways look up, so announcing
// the few of them is fast (a handful of CIDs) and makes content discoverable
// quickly while the node's full per-block reprovide proceeds in the background.
//
// Re-announcing on address change is what makes a node behind NAT actually
// reachable: a relay /p2p-circuit address is reserved a few minutes after
// startup, and without re-announcing the root records keep only the node's
// stale, undialable direct addresses. This goes through the provider queue, so
// unlike a full Reprovide it is not blocked by the slow startup reprovide and
// publishes the new addresses within seconds.
func provideRoots(ctx context.Context, n *node.Node, st *store.Store) {
	for n.DHT.RoutingTable().Size() < 1 {
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}

	announce := func() {
		pins, err := st.Pins(ctx)
		if err != nil {
			log.Warn().Err(err).Msg("load pins for root announce")
			return
		}
		var done int
		for _, p := range pins {
			c, err := cid.Cast(p.RootCID)
			if err != nil {
				continue
			}
			if err := n.Provider.Provide(ctx, c, true); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Warn().Err(err).Stringer("cid", c).Msg("announce pin root")
				continue
			}
			done++
		}
		log.Info().Int("roots", done).Msg("announced pin roots to the DHT")
	}
	announce()

	sub, err := n.Host.EventBus().Subscribe([]any{
		new(event.EvtLocalAddressesUpdated),
		new(event.EvtLocalReachabilityChanged),
	})
	if err != nil {
		log.Warn().Err(err).Msg("subscribe to address changes for root re-announce")
		return
	}
	defer sub.Close()

	publicSet := func() string {
		var s []string
		for _, a := range n.Host.Addrs() {
			if !manet.IsPrivateAddr(a) {
				s = append(s, a.String())
			}
		}
		sort.Strings(s)
		return strings.Join(s, ",")
	}

	// Start from empty so the first observed public-address set (which may
	// already be present by the time the subscription is established) triggers a
	// re-announce.
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
				debounce.Reset(5 * time.Second)
			}
		case <-debounce.C:
			announce()
		}
	}
}

// listenAndProvide subscribes to the store's new-content notifications and
// announces each freshly published root CID to the DHT immediately. It
// reconnects if the LISTEN connection drops, until ctx is cancelled.
func listenAndProvide(ctx context.Context, dsn string, n *node.Node) {
	for ctx.Err() == nil {
		ch, err := store.ListenContent(ctx, dsn)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Warn().Err(err).Msg("listen for new content failed, retrying in 5s")
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
			continue
		}
		log.Info().Msg("listening for new-content notifications")
		for root := range ch {
			c, err := cid.Cast(root)
			if err != nil {
				continue
			}
			if err := n.Provider.Provide(ctx, c, true); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Warn().Err(err).Stringer("cid", c).Msg("announce new content")
				continue
			}
			log.Info().Stringer("cid", c).Msg("announced new content to the DHT")
		}
		// Channel closed: connection lost or ctx done. Loop to reconnect.
	}
}

// buildCache constructs the disk cache per the configured strategy.
func buildCache(cfg Config) (cache.Cache, error) {
	switch cfg.CacheStrategy {
	case "lru", "":
		return cache.NewLRU(cfg.CacheDir, cfg.CacheMaxBytes)
	case "ttl":
		return cache.NewTTL(cfg.CacheDir, cfg.CacheTTL)
	default:
		return nil, fmt.Errorf("unknown cache strategy %q (want lru or ttl)", cfg.CacheStrategy)
	}
}
