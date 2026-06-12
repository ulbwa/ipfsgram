// Package daemon runs the full IPFS node: a read-only blockstore over CAR
// archives stored in Telegram, served via libp2p/Bitswap, announced through
// the Kademlia DHT (server mode) by a periodic reprovider.
package daemon

import (
	"context"
	"fmt"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/ulbwa/ipfsgram/internal/cache"
	"github.com/ulbwa/ipfsgram/internal/db"
	"github.com/ulbwa/ipfsgram/internal/repo"
	"github.com/ulbwa/ipfsgram/internal/tg"
)

// Config carries the resolved daemon settings (flags + env fallbacks).
type Config struct {
	DSN           string
	DataDir       string
	CacheDir      string
	CacheMaxBytes int64
	CacheStrategy string // "lru" or "ttl"
	CacheTTL      time.Duration
	Listen        []string
}

// Run starts the daemon and blocks until SIGINT/SIGTERM.
func Run(ctx context.Context, cfg Config) error {
	if cfg.DSN == "" {
		return fmt.Errorf("daemon: PostgreSQL DSN is required (--dsn or $IPFSGRAM_DSN)")
	}
	if cfg.CacheDir == "" {
		cfg.CacheDir = filepath.Join(cfg.DataDir, "cache")
	}

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	conn, err := db.Connect(ctx, cfg.DSN)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := db.CheckSchemaVersion(ctx, conn); err != nil {
		return err
	}

	blockRepo := repo.NewBlockRepo(conn)
	carRepo := repo.NewCarRepo(conn)
	botRepo := repo.NewBotRepo(conn)
	channelRepo := repo.NewChannelRepo(conn)
	configRepo := repo.NewConfigRepo(conn)
	queries := newDBQueries(conn)

	botAPIURL, err := configRepo.Get(ctx, "bot_api_url")
	if err != nil {
		return fmt.Errorf("daemon: load bot_api_url: %w", err)
	}
	transport, err := buildTransport(ctx, configRepo, repo.NewMTProtoCredsRepo(conn), botAPIURL, cfg.DataDir)
	if err != nil {
		return err
	}

	carCache, err := buildCache(cfg)
	if err != nil {
		return err
	}
	defer carCache.Close()

	blockstore := NewBlockstore(
		blockRepo, carRepo, botRepo, channelRepo, queries, queries,
		carCache, tmpDirFor(cfg.CacheDir), transport,
	)

	node, err := StartNode(ctx, cfg.DataDir, cfg.Listen, blockstore, queries.AllCIDs)
	if err != nil {
		return err
	}
	defer func() {
		if err := node.Close(); err != nil {
			log.Error().Err(err).Msg("shutdown")
		}
	}()

	logEvent := log.Info().Str("peer_id", node.Host.ID().String())
	for _, a := range node.Host.Addrs() {
		logEvent = logEvent.Str("listen", a.String())
	}
	logEvent.Msg("daemon started")

	<-ctx.Done()
	log.Info().Msg("shutting down")
	return nil
}

// buildTransport assembles the Telegram transport, attaching MTProto when it
// is enabled in config and active credentials exist.
func buildTransport(ctx context.Context, configRepo *repo.ConfigRepo, credsRepo *repo.MTProtoCredsRepo, botAPIURL, dataDir string) (tg.Transport, error) {
	enabled, err := configRepo.GetBool(ctx, "mtproto_enabled")
	if err != nil {
		return nil, fmt.Errorf("daemon: load mtproto_enabled: %w", err)
	}
	if !enabled {
		return tg.New(botAPIURL, nil, ""), nil
	}
	creds, err := credsRepo.Active(ctx)
	if err != nil {
		return nil, fmt.Errorf("daemon: load mtproto credentials: %w", err)
	}
	if creds == nil {
		log.Warn().Msg("mtproto_enabled is true but no active credentials exist, falling back to Bot API only")
		return tg.New(botAPIURL, nil, ""), nil
	}
	return tg.New(botAPIURL, creds, filepath.Join(dataDir, "mtproto-sessions")), nil
}

// buildCache constructs the disk cache per the configured strategy.
func buildCache(cfg Config) (cache.Cache, error) {
	switch cfg.CacheStrategy {
	case "lru", "":
		return cache.NewLRU(cfg.CacheDir, cfg.CacheMaxBytes)
	case "ttl":
		return cache.NewTTL(cfg.CacheDir, cfg.CacheTTL)
	default:
		return nil, fmt.Errorf("daemon: unknown cache strategy %q (want lru or ttl)", cfg.CacheStrategy)
	}
}
