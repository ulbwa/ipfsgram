// daemon.go — Config and Run: assembles the Telegram transport (from the
// database config, with the MTProto enabled-but-no-credentials fallback), the
// disk cache, the read-only blockstore and the libp2p node, then blocks until
// the context is cancelled and shuts down gracefully. Flag/env parsing stays
// in cmd/ipfsgram; Run receives the already-resolved Config and an open store.

package daemon

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/ulbwa/ipfsgram/internal/cache"
	"github.com/ulbwa/ipfsgram/internal/store"
	"github.com/ulbwa/ipfsgram/internal/telegram"
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

// Run wires the Telegram transport, disk cache, blockstore and libp2p node,
// then blocks until ctx is cancelled (SIGINT/SIGTERM) and shuts down
// gracefully. The store must be connected and schema-checked by the caller.
func Run(ctx context.Context, cfg Config, st *store.Store) error {
	if cfg.CacheDir == "" {
		cfg.CacheDir = filepath.Join(cfg.DataDir, "cache")
	}

	tr, err := buildTransport(ctx, st, filepath.Join(cfg.DataDir, "mtproto-sessions"))
	if err != nil {
		return err
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

	node, err := NewNode(ctx, NodeConfig{
		IdentityPath: filepath.Join(cfg.DataDir, "identity.key"),
		ListenAddrs:  cfg.Listen,
		Blockstore:   bs,
		ProvideKeys:  ProvideKeys(st),
	})
	if err != nil {
		return err
	}
	defer func() {
		if err := node.Close(); err != nil {
			log.Error().Err(err).Msg("shutdown")
		}
	}()

	logEvent := log.Info().Str("peer_id", node.Host.ID().String())
	for _, addr := range node.Host.Addrs() {
		logEvent = logEvent.Str("listen", addr.String())
	}
	logEvent.Msg("daemon started")

	<-ctx.Done()
	log.Info().Msg("shutting down")
	return nil
}

// buildTransport assembles the Telegram client from the database config:
// bot_api_url plus, when mtproto_enabled, the active MTProto credentials. When
// MTProto is enabled but no active credentials exist, it warns and falls back
// to the Bot API only.
func buildTransport(ctx context.Context, st *store.Store, sessionDir string) (telegram.Client, error) {
	apiURL, err := st.ConfigValue(ctx, "bot_api_url")
	if errors.Is(err, store.ErrNotFound) {
		apiURL = "https://api.telegram.org"
	} else if err != nil {
		return nil, err
	}

	enabled, err := st.ConfigBool(ctx, "mtproto_enabled")
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}

	apiID, apiHash := 0, ""
	if enabled {
		creds, err := st.ActiveMTProtoCreds(ctx)
		if err != nil {
			return nil, err
		}
		if creds == nil {
			log.Warn().Msg("mtproto_enabled is true but no active credentials exist, falling back to Bot API only")
		} else {
			apiID, apiHash = creds.APIID, creds.APIHash
		}
	}
	return telegram.New(apiURL, apiID, apiHash, sessionDir), nil
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
