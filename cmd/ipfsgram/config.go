// config.go — the `ipfsgram config` command group (whitelisted key/value
// settings stored in the database) and the `ipfsgram status` overview.

package main

import (
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/ulbwa/ipfsgram/internal/store"
)

// configValidators whitelists the user-editable config keys and validates their
// values.
var configValidators = map[string]func(value string) error{
	"car_max_size": func(v string) error {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			return fmt.Errorf("car_max_size must be a positive integer number of bytes, got %q", v)
		}
		return nil
	},
	"bot_api_url": func(v string) error {
		u, err := url.Parse(v)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return fmt.Errorf("bot_api_url must be an http/https URL, got %q", v)
		}
		return nil
	},
	"channel_warn_threshold": func(v string) error {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f < 0 || f > 1 {
			return fmt.Errorf("channel_warn_threshold must be a number in 0..1, got %q", v)
		}
		return nil
	},
}

// knownConfigKeys returns the sorted whitelist of editable keys.
func knownConfigKeys() []string {
	keys := make([]string, 0, len(configValidators))
	for k := range configValidators {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// validateConfigKey rejects keys outside the whitelist.
func validateConfigKey(key string) error {
	if _, ok := configValidators[key]; !ok {
		return fmt.Errorf("unknown key %q, valid keys: %s",
			key, strings.Join(knownConfigKeys(), ", "))
	}
	return nil
}

// validateConfigValue validates the value for a whitelisted key.
func validateConfigValue(key, value string) error {
	if err := validateConfigKey(key); err != nil {
		return err
	}
	return configValidators[key](value)
}

// newConfigCmd returns the `ipfsgram config` command group.
func newConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Global configuration stored in the database",
	}
	cmd.AddCommand(
		&cobra.Command{
			Use:   "get <key>",
			Short: "Read a value",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				if err := validateConfigKey(args[0]); err != nil {
					return err
				}
				ctx := cmd.Context()
				a, err := openApp(ctx, cmd, "")
				if err != nil {
					return err
				}
				defer a.Close()
				v, err := a.Store.ConfigValue(ctx, args[0])
				if errors.Is(err, store.ErrNotFound) {
					return fmt.Errorf("key %q is not set", args[0])
				}
				if err != nil {
					return err
				}
				fmt.Fprintln(stdout, v)
				return nil
			},
		},
		&cobra.Command{
			Use:   "set <key> <value>",
			Short: "Set a value",
			Args:  cobra.ExactArgs(2),
			RunE: func(cmd *cobra.Command, args []string) error {
				if err := validateConfigValue(args[0], args[1]); err != nil {
					return err
				}
				ctx := cmd.Context()
				a, err := openApp(ctx, cmd, "")
				if err != nil {
					return err
				}
				defer a.Close()
				if err := a.Store.SetConfig(ctx, args[0], args[1]); err != nil {
					return err
				}
				fmt.Fprintf(stdout, "%s = %s\n", args[0], args[1])
				return nil
			},
		},
	)
	return cmd
}

// newStatusCmd returns the top-level `ipfsgram status` command.
func newStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "System status: channels, bots, counters",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			a, err := openApp(ctx, cmd, "")
			if err != nil {
				return err
			}
			defer a.Close()

			warnThreshold, err := a.Store.ConfigFloat64(ctx, "channel_warn_threshold")
			if errors.Is(err, store.ErrNotFound) {
				warnThreshold = 0.9
			} else if err != nil {
				return err
			}

			channels, err := a.Store.Channels(ctx)
			if err != nil {
				return err
			}
			fmt.Fprintln(stdout, "Channels:")
			for _, ch := range channels {
				fill := 0.0
				if ch.MessageLimit > 0 {
					fill = float64(ch.MessageCount) / float64(ch.MessageLimit)
				}
				mark := ""
				if fill >= warnThreshold {
					mark = " [!]"
				}
				fmt.Fprintf(stdout, "  %s (tg_id %d): %d/%d (%.1f%%) active=%t%s\n",
					ch.Title, ch.TgID, ch.MessageCount, ch.MessageLimit, fill*100, ch.Active, mark)
			}

			bots, err := a.Store.Bots(ctx)
			if err != nil {
				return err
			}
			fmt.Fprintln(stdout, "Bots:")
			for _, b := range bots {
				fw := ""
				if b.UnavailableUntil != nil && time.Now().Before(*b.UnavailableUntil) {
					fw = " flood-wait until " + b.UnavailableUntil.Format(time.RFC3339)
				}
				fmt.Fprintf(stdout, "  @%s (id %d) active=%t%s\n", b.Username, b.ID, b.Active, fw)
			}

			pins, err := a.Store.CountPins(ctx)
			if err != nil {
				return err
			}
			blocks, err := a.Store.CountBlocks(ctx)
			if err != nil {
				return err
			}
			fmt.Fprintf(stdout, "Pins: %d\nBlocks: %d\n", pins, blocks)

			counts, err := a.Store.CountCarsByStatus(ctx)
			if err != nil {
				return err
			}
			statuses := make([]string, 0, len(counts))
			for st := range counts {
				statuses = append(statuses, string(st))
			}
			sort.Strings(statuses)
			fmt.Fprintln(stdout, "CARs by status:")
			for _, st := range statuses {
				fmt.Fprintf(stdout, "  %s: %d\n", st, counts[store.CarStatus(st)])
			}
			return nil
		},
	}
}
