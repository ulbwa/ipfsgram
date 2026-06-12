package cli

import (
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/ulbwa/ipfsgram/internal/repo"
)

// configValidators whitelists the user-editable config keys and validates
// their values.
var configValidators = map[string]func(value string) error{
	"car_max_size": func(v string) error {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			return fmt.Errorf("car_max_size должен быть положительным целым числом байт, получено %q", v)
		}
		return nil
	},
	"bot_api_url": func(v string) error {
		u, err := url.Parse(v)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return fmt.Errorf("bot_api_url должен быть http/https URL, получено %q", v)
		}
		return nil
	},
	"channel_warn_threshold": func(v string) error {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f < 0 || f > 1 {
			return fmt.Errorf("channel_warn_threshold должен быть числом в диапазоне 0..1, получено %q", v)
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
		return fmt.Errorf("неизвестный ключ %q, допустимые: %s",
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
		Short: "Глобальная конфигурация в БД",
	}
	cmd.AddCommand(
		&cobra.Command{
			Use:   "get <key>",
			Short: "Прочитать значение",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				if err := validateConfigKey(args[0]); err != nil {
					return err
				}
				ctx := cmd.Context()
				e, err := openEnv(ctx, cmd)
				if err != nil {
					return err
				}
				defer e.Close()
				v, err := e.Config.Get(ctx, args[0])
				if errors.Is(err, repo.ErrNotFound) {
					return fmt.Errorf("ключ %q не установлен", args[0])
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
			Short: "Установить значение",
			Args:  cobra.ExactArgs(2),
			RunE: func(cmd *cobra.Command, args []string) error {
				if err := validateConfigValue(args[0], args[1]); err != nil {
					return err
				}
				ctx := cmd.Context()
				e, err := openEnv(ctx, cmd)
				if err != nil {
					return err
				}
				defer e.Close()
				if err := e.Config.Set(ctx, args[0], args[1]); err != nil {
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
		Short: "Состояние системы: каналы, боты, счётчики",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			e, err := openEnv(ctx, cmd)
			if err != nil {
				return err
			}
			defer e.Close()

			warnThreshold, err := e.Config.GetFloat64(ctx, "channel_warn_threshold")
			if err != nil {
				warnThreshold = 0.9
			}

			channels, err := e.Channels.List(ctx)
			if err != nil {
				return err
			}
			fmt.Fprintln(stdout, "Каналы:")
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

			bots, err := e.Bots.List(ctx)
			if err != nil {
				return err
			}
			fmt.Fprintln(stdout, "Боты:")
			for _, b := range bots {
				fw := ""
				if b.UnavailableUntil != nil && time.Now().Before(*b.UnavailableUntil) {
					fw = " flood-wait до " + b.UnavailableUntil.Format(time.RFC3339)
				}
				fmt.Fprintf(stdout, "  @%s (id %d) active=%t%s\n", b.Username, b.ID, b.Active, fw)
			}

			var pins, blocks int64
			if err := e.db.GetContext(ctx, &pins, `SELECT count(*) FROM pins`); err != nil {
				return err
			}
			if err := e.db.GetContext(ctx, &blocks, `SELECT count(*) FROM blocks`); err != nil {
				return err
			}
			fmt.Fprintf(stdout, "Пины: %d\nБлоки: %d\n", pins, blocks)

			rows, err := e.db.QueryContext(ctx,
				`SELECT status, count(*) FROM cars GROUP BY status ORDER BY status`)
			if err != nil {
				return err
			}
			defer rows.Close()
			fmt.Fprintln(stdout, "CAR'ы по статусам:")
			for rows.Next() {
				var st string
				var n int64
				if err := rows.Scan(&st, &n); err != nil {
					return err
				}
				fmt.Fprintf(stdout, "  %s: %d\n", st, n)
			}
			return rows.Err()
		},
	}
}
