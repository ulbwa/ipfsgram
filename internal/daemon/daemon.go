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
	"time"

	"github.com/ipfs/go-cid"
	"github.com/rs/zerolog/log"

	"github.com/ulbwa/ipfsgram/internal/cache"
	"github.com/ulbwa/ipfsgram/internal/node"
	"github.com/ulbwa/ipfsgram/internal/store"
)

// Config carries the resolved daemon settings. cmd/ipfsgram fills it from
// flags and environment fallbacks; Run applies the remaining defaults
// (CacheDir defaults to <DataDir>/cache).
type Config struct {
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
		IdentityPath: filepath.Join(cfg.DataDir, "identity.key"),
		ListenAddrs:  cfg.Listen,
		Blockstore:   bs,
		ProvideKeys:  ProvideKeys(st),
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

	<-ctx.Done()
	log.Info().Msg("shutting down")
	return nil
}

// provideRoots waits for the DHT routing table to populate, then announces each
// pin's root CID to the DHT. Roots are the entry points gateways look up, so
// announcing the few of them first makes content discoverable quickly while the
// node's full per-block reprovide proceeds in the background.
func provideRoots(ctx context.Context, n *node.Node, st *store.Store) {
	for n.DHT.RoutingTable().Size() < 1 {
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
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
			log.Warn().Err(err).Str("cid", c.String()).Msg("announce pin root")
			continue
		}
		done++
	}
	log.Info().Int("roots", done).Msg("announced pin roots to the DHT")
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
