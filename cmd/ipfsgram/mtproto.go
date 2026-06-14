// mtproto.go — the `ipfsgram mtproto` command group: enable (validate
// credentials before persisting), disable (keep credential history) and status.

package main

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/ulbwa/ipfsgram/internal/store"
	"github.com/ulbwa/ipfsgram/internal/telegram"
)

// newMTProtoCmd returns the `ipfsgram mtproto` command group.
func newMTProtoCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mtproto",
		Short: "Manage the MTProto transport",
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
		Short: "Enable MTProto (api_id/api_hash)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			a, err := openApp(ctx, cmd, "")
			if err != nil {
				return err
			}
			defer a.Close()

			if apiID == 0 || apiHash == "" {
				latest, err := a.Store.LatestMTProtoCreds(ctx)
				if err != nil {
					return err
				}
				switch {
				case latest != nil && Confirm(fmt.Sprintf(
					"Found stored credentials api_id=%d. Use them?", latest.APIID)):
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

			// Validate the credentials BEFORE persisting anything.
			if err := telegram.NewMTProto(apiID, apiHash, "").ValidateCreds(ctx); err != nil {
				return fmt.Errorf("credential validation failed, nothing saved: %w", err)
			}
			if err := a.Store.ActivateMTProto(ctx, apiID, apiHash); err != nil {
				return err
			}
			if err := a.Store.SetConfig(ctx, "mtproto_enabled", "true"); err != nil {
				return err
			}
			fmt.Fprintf(stdout, "MTProto enabled (api_id=%d)\n", apiID)
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
		Short: "Disable MTProto (credentials are kept in history)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			a, err := openApp(ctx, cmd, "")
			if err != nil {
				return err
			}
			defer a.Close()

			// Deactivate clears the active flag; the row stays so
			// LatestMTProtoCreds can offer the credentials on the next enable.
			if err := a.Store.DeactivateMTProto(ctx); err != nil {
				return err
			}
			if err := a.Store.SetConfig(ctx, "mtproto_enabled", "false"); err != nil {
				return err
			}
			fmt.Fprintln(stdout, "MTProto disabled, credentials kept in history")
			return nil
		},
	}
}

func newMTProtoStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "MTProto status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			a, err := openApp(ctx, cmd, "")
			if err != nil {
				return err
			}
			defer a.Close()

			enabled, err := a.Store.ConfigBool(ctx, "mtproto_enabled")
			if err != nil && !errors.Is(err, store.ErrNotFound) {
				return err
			}
			active, err := a.Store.ActiveMTProtoCreds(ctx)
			if err != nil {
				return err
			}
			fmt.Fprintf(stdout, "MTProto: %s\n", map[bool]string{true: "enabled", false: "disabled"}[enabled])
			if active != nil {
				fmt.Fprintf(stdout, "Active credentials: api_id=%d\n", active.APIID)
			} else {
				fmt.Fprintln(stdout, "No active credentials")
			}
			return nil
		},
	}
}
