package cli

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/ulbwa/ipfsgram/internal/repo"
	"github.com/ulbwa/ipfsgram/internal/tg"
)

// newMTProtoCmd returns the `ipfsgram mtproto` command group.
func newMTProtoCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mtproto",
		Short: "Управление MTProto-транспортом",
	}
	cmd.AddCommand(newMTProtoEnableCmd(), newMTProtoDisableCmd(), newMTProtoStatusCmd())
	return cmd
}

func newMTProtoEnableCmd() *cobra.Command {
	var (
		apiID   int
		apiHash string
	)
	cmd := &cobra.Command{
		Use:   "enable",
		Short: "Включить MTProto (api_id/api_hash)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			e, err := openEnv(ctx, cmd)
			if err != nil {
				return err
			}
			defer e.Close()

			if apiID == 0 || apiHash == "" {
				latest, err := e.Creds.Latest(ctx)
				if err != nil {
					return err
				}
				switch {
				case latest != nil && Confirm(fmt.Sprintf(
					"Найдены сохранённые креденшелы api_id=%d. Использовать их?", latest.APIID)):
					apiID, apiHash = latest.APIID, latest.APIHash
				default:
					if apiID, err = promptInt("api_id"); err != nil {
						return err
					}
					if apiHash, err = promptString("api_hash"); err != nil {
						return err
					}
				}
			}

			if err := tg.NewMTProto(apiID, apiHash, "").ValidateCreds(ctx); err != nil {
				return fmt.Errorf("проверка креденшелов не прошла, ничего не сохранено: %w", err)
			}
			if err := e.Creds.Activate(ctx, apiID, apiHash); err != nil {
				return err
			}
			if err := e.Config.Set(ctx, "mtproto_enabled", "true"); err != nil {
				return err
			}
			fmt.Fprintf(stdout, "MTProto включён (api_id=%d)\n", apiID)
			return nil
		},
	}
	cmd.Flags().IntVar(&apiID, "api-id", 0, "Telegram api_id")
	cmd.Flags().StringVar(&apiHash, "api-hash", "", "Telegram api_hash")
	return cmd
}

func newMTProtoDisableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "disable",
		Short: "Выключить MTProto (креденшелы сохраняются в истории)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			e, err := openEnv(ctx, cmd)
			if err != nil {
				return err
			}
			defer e.Close()

			// Deactivate clears the active flag; the row stays so Latest()
			// can offer the credentials on the next enable.
			if err := e.Creds.Deactivate(ctx); err != nil {
				return err
			}
			if err := e.Config.Set(ctx, "mtproto_enabled", "false"); err != nil {
				return err
			}
			fmt.Fprintln(stdout, "MTProto выключен, креденшелы сохранены в истории")
			return nil
		},
	}
}

func newMTProtoStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Статус MTProto",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			e, err := openEnv(ctx, cmd)
			if err != nil {
				return err
			}
			defer e.Close()

			enabled, err := e.Config.GetBool(ctx, "mtproto_enabled")
			if err != nil && !errors.Is(err, repo.ErrNotFound) {
				return err
			}
			active, err := e.Creds.Active(ctx)
			if err != nil {
				return err
			}
			fmt.Fprintf(stdout, "MTProto: %s\n", map[bool]string{true: "включён", false: "выключен"}[enabled])
			if active != nil {
				fmt.Fprintf(stdout, "Активные креденшелы: api_id=%d\n", active.APIID)
			} else {
				fmt.Fprintln(stdout, "Активных креденшелов нет")
			}
			return nil
		},
	}
}
