package daemon

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/ulbwa/ipfsgram/internal/cli"
)

// envPrefix prefixes the daemon's environment-variable fallbacks.
const envPrefix = "IPFSGRAM_"

// defaultListen are the multiaddrs the node listens on when --listen is not
// given.
var defaultListen = []string{
	"/ip4/0.0.0.0/tcp/4001",
	"/ip4/0.0.0.0/udp/4001/quic-v1",
}

// Command returns the `ipfsgram daemon` cobra command.
func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "daemon",
		Short: "Run the IPFS node (libp2p, bitswap, DHT)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := configFromFlags(cmd)
			if err != nil {
				return err
			}
			return Run(cmd.Context(), cfg)
		},
	}

	flags := cmd.Flags()
	flags.String("data-dir", "", "daemon state directory (identity key, sessions, cache) [$"+envPrefix+"DATA_DIR, default ~/.ipfsgram]")
	flags.String("cache-dir", "", "CAR disk cache directory [$"+envPrefix+"CACHE_DIR, default <data-dir>/cache]")
	flags.Int64("cache-max-bytes", 0, "cache size limit for the lru strategy [$"+envPrefix+"CACHE_MAX_BYTES, default 1073741824]")
	flags.String("cache-strategy", "", "cache eviction strategy: lru or ttl [$"+envPrefix+"CACHE_STRATEGY, default lru]")
	flags.Duration("cache-ttl", 0, "entry lifetime for the ttl strategy [$"+envPrefix+"CACHE_TTL, default 1h]")
	flags.StringArray("listen", nil, "libp2p listen multiaddr, repeatable [$"+envPrefix+"LISTEN comma-separated, default "+strings.Join(defaultListen, ",")+"]")
	return cmd
}

// configFromFlags resolves every setting as flag → environment → default.
func configFromFlags(cmd *cobra.Command) (Config, error) {
	flags := cmd.Flags()
	cfg := Config{DSN: cli.ResolveDSN(cmd)}

	dataDir, _ := flags.GetString("data-dir")
	if dataDir == "" {
		dataDir = os.Getenv(envPrefix + "DATA_DIR")
	}
	if dataDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return Config{}, fmt.Errorf("daemon: resolve home directory: %w", err)
		}
		dataDir = filepath.Join(home, ".ipfsgram")
	}
	cfg.DataDir = dataDir

	cfg.CacheDir, _ = flags.GetString("cache-dir")
	if cfg.CacheDir == "" {
		cfg.CacheDir = os.Getenv(envPrefix + "CACHE_DIR")
	}
	// Empty stays empty: Run defaults it to <data-dir>/cache.

	cfg.CacheMaxBytes, _ = flags.GetInt64("cache-max-bytes")
	if !flags.Changed("cache-max-bytes") {
		if env := os.Getenv(envPrefix + "CACHE_MAX_BYTES"); env != "" {
			v, err := strconv.ParseInt(env, 10, 64)
			if err != nil {
				return Config{}, fmt.Errorf("daemon: parse $%sCACHE_MAX_BYTES: %w", envPrefix, err)
			}
			cfg.CacheMaxBytes = v
		} else {
			cfg.CacheMaxBytes = 1 << 30 // 1 GiB
		}
	}

	cfg.CacheStrategy, _ = flags.GetString("cache-strategy")
	if cfg.CacheStrategy == "" {
		cfg.CacheStrategy = os.Getenv(envPrefix + "CACHE_STRATEGY")
	}
	if cfg.CacheStrategy == "" {
		cfg.CacheStrategy = "lru"
	}

	cfg.CacheTTL, _ = flags.GetDuration("cache-ttl")
	if !flags.Changed("cache-ttl") {
		if env := os.Getenv(envPrefix + "CACHE_TTL"); env != "" {
			d, err := time.ParseDuration(env)
			if err != nil {
				return Config{}, fmt.Errorf("daemon: parse $%sCACHE_TTL: %w", envPrefix, err)
			}
			cfg.CacheTTL = d
		} else {
			cfg.CacheTTL = time.Hour
		}
	}

	cfg.Listen, _ = flags.GetStringArray("listen")
	if len(cfg.Listen) == 0 {
		if env := os.Getenv(envPrefix + "LISTEN"); env != "" {
			for _, a := range strings.Split(env, ",") {
				if a = strings.TrimSpace(a); a != "" {
					cfg.Listen = append(cfg.Listen, a)
				}
			}
		}
	}
	if len(cfg.Listen) == 0 {
		cfg.Listen = defaultListen
	}
	return cfg, nil
}
