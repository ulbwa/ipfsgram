// daemon.go — the `ipfsgram daemon` command: resolves flags and environment
// fallbacks into a daemon.Config, opens the store, and hands off to
// internal/daemon.Run.

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/ulbwa/ipfsgram/internal/daemon"
)

// envPrefix prefixes the daemon's environment-variable fallbacks.
const envPrefix = "IPFSGRAM_"

// defaultListen are the multiaddrs the node listens on when --listen is unset.
// QUIC (UDP) is essential for NAT hole punching (DCUtR) behind CGNAT/double-NAT,
// where TCP hole punching almost never succeeds; WebTransport additionally lets
// browsers and some public gateways dial in. This mirrors Kubo's defaults.
var defaultListen = []string{
	"/ip4/0.0.0.0/tcp/4001",
	"/ip4/0.0.0.0/udp/4001/quic-v1",
	"/ip4/0.0.0.0/udp/4001/quic-v1/webtransport",
	"/ip4/0.0.0.0/udp/4001/webrtc-direct",
	"/ip6/::/tcp/4001",
	"/ip6/::/udp/4001/quic-v1",
	"/ip6/::/udp/4001/quic-v1/webtransport",
	"/ip6/::/udp/4001/webrtc-direct",
}

// newDaemonCmd returns the `ipfsgram daemon` command.
func newDaemonCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "daemon",
		Short: "Run the IPFS node (libp2p, bitswap, DHT)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := daemonConfigFromFlags(cmd)
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			cfg.DSN = resolveDSN(cmd)
			a, err := openApp(ctx, cmd, filepath.Join(cfg.DataDir, "mtproto-sessions"))
			if err != nil {
				return err
			}
			defer a.Close()
			return daemon.Run(ctx, cfg, a.Store, a.Transport)
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

// stringFlagOrEnv returns the string flag if set, else the matching
// IPFSGRAM_<env> variable, else def.
func stringFlagOrEnv(cmd *cobra.Command, flag, env, def string) string {
	if v, _ := cmd.Flags().GetString(flag); v != "" {
		return v
	}
	if v := os.Getenv(envPrefix + env); v != "" {
		return v
	}
	return def
}

// daemonConfigFromFlags resolves every setting as flag → environment → default.
func daemonConfigFromFlags(cmd *cobra.Command) (daemon.Config, error) {
	flags := cmd.Flags()
	var cfg daemon.Config

	dataDir := stringFlagOrEnv(cmd, "data-dir", "DATA_DIR", "")
	if dataDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return daemon.Config{}, fmt.Errorf("resolve home directory: %w", err)
		}
		dataDir = filepath.Join(home, ".ipfsgram")
	}
	cfg.DataDir = dataDir

	// Empty CacheDir stays empty: daemon.Run defaults it to <data-dir>/cache.
	cfg.CacheDir = stringFlagOrEnv(cmd, "cache-dir", "CACHE_DIR", "")
	cfg.CacheStrategy = stringFlagOrEnv(cmd, "cache-strategy", "CACHE_STRATEGY", "lru")

	cfg.CacheMaxBytes, _ = flags.GetInt64("cache-max-bytes")
	if !flags.Changed("cache-max-bytes") {
		if env := os.Getenv(envPrefix + "CACHE_MAX_BYTES"); env != "" {
			v, err := strconv.ParseInt(env, 10, 64)
			if err != nil {
				return daemon.Config{}, fmt.Errorf("parse $%sCACHE_MAX_BYTES: %w", envPrefix, err)
			}
			cfg.CacheMaxBytes = v
		} else {
			cfg.CacheMaxBytes = 1 << 30 // 1 GiB
		}
	}

	cfg.CacheTTL, _ = flags.GetDuration("cache-ttl")
	if !flags.Changed("cache-ttl") {
		if env := os.Getenv(envPrefix + "CACHE_TTL"); env != "" {
			d, err := time.ParseDuration(env)
			if err != nil {
				return daemon.Config{}, fmt.Errorf("parse $%sCACHE_TTL: %w", envPrefix, err)
			}
			cfg.CacheTTL = d
		} else {
			cfg.CacheTTL = time.Hour
		}
	}

	cfg.Listen = resolveListen(cmd)
	return cfg, nil
}

// resolveListen resolves the libp2p listen multiaddrs as flag → comma-separated
// environment → default.
func resolveListen(cmd *cobra.Command) []string {
	listen, _ := cmd.Flags().GetStringArray("listen")
	if len(listen) == 0 {
		if env := os.Getenv(envPrefix + "LISTEN"); env != "" {
			for _, a := range strings.Split(env, ",") {
				if a = strings.TrimSpace(a); a != "" {
					listen = append(listen, a)
				}
			}
		}
	}
	if len(listen) == 0 {
		listen = defaultListen
	}
	return listen
}
