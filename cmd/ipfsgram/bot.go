package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/ulbwa/ipfsgram/internal/domain"
	"github.com/ulbwa/ipfsgram/internal/port"
)

// findBot resolves a bot by numeric ID or @username.
func (a *app) findBot(ctx context.Context, arg string) (domain.Bot, error) {
	if id, err := strconv.ParseInt(arg, 10, 64); err == nil {
		b, err := a.Bots.GetByID(ctx, id)
		if errors.Is(err, domain.ErrNotFound) {
			return domain.Bot{}, fmt.Errorf("bot %q not found", arg)
		}
		return b, err
	}
	username := strings.TrimPrefix(arg, "@")
	b, err := a.Bots.GetByUsername(ctx, username)
	if errors.Is(err, domain.ErrNotFound) {
		return domain.Bot{}, fmt.Errorf("bot %q not found", arg)
	}
	return b, err
}

// newBotCmd returns the `ipfsgram bot` command group.
func newBotCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bot",
		Short: "Manage bots",
	}
	cmd.AddCommand(newBotAddCmd(), newBotListCmd(), newBotRemoveCmd())
	return cmd
}

func newBotAddCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "add <token>",
		Short: "Add a bot by token",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			a, err := openApp(ctx, cmd, "")
			if err != nil {
				return err
			}
			defer a.Close()

			token := args[0]
			tgID, username, err := a.Transport.ValidateToken(ctx, token)
			if err != nil {
				return fmt.Errorf("invalid token: %w", err)
			}

			botID, err := a.Bots.Add(ctx, domain.Bot{
				TgID: tgID, Username: username, Token: token, Active: true,
			})
			if errors.Is(err, domain.ErrBotExists) {
				return fmt.Errorf("bot @%s is already added", username)
			}
			if err != nil {
				return err
			}

			channels, err := a.Channels.List(ctx)
			if err != nil {
				return err
			}
			member := 0
			for _, ch := range channels {
				info, perr := a.Transport.ProbeChannel(ctx, token, ch.TgID)
				if perr != nil {
					if !errors.Is(perr, domain.ErrNoAccess) {
						log.Warn().Err(perr).Int64("channel_tg_id", ch.TgID).
							Msg("could not probe bot access to channel")
						continue
					}
					info = port.ChannelInfo{} // no access: member=false
				}
				if err := a.Channels.UpsertBotChannel(ctx, domain.BotChannel{
					BotID: botID, ChannelID: ch.ID,
					CanPost: info.CanPost, CanRead: info.CanRead, CanDelete: info.CanDelete,
					Member: info.Member, VerifiedAt: time.Now(),
				}); err != nil {
					return err
				}
				if info.Member {
					member++
				}
			}
			fmt.Fprintf(stdout, "Bot @%s added (id %d), member of %d of %d channels\n",
				username, botID, member, len(channels))
			return nil
		},
	}
}

func newBotListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List bots",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			a, err := openApp(ctx, cmd, "")
			if err != nil {
				return err
			}
			defer a.Close()

			bots, err := a.Bots.List(ctx)
			if err != nil {
				return err
			}

			fmt.Fprintf(stdout, "%-5s %-24s %-8s %s\n",
				"ID", "USERNAME", "ACTIVE", "FLOOD-WAIT")
			for _, b := range bots {
				fw := "-"
				if b.UnavailableUntil != nil && time.Now().Before(*b.UnavailableUntil) {
					fw = b.UnavailableUntil.Format(time.RFC3339)
				}
				fmt.Fprintf(stdout, "%-5d %-24s %-8t %s\n",
					b.ID, "@"+b.Username, b.Active, fw)
			}
			return nil
		},
	}
}

func newBotRemoveCmd() *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "remove <id|@username>",
		Short: "Remove a bot",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			a, err := openApp(ctx, cmd, "")
			if err != nil {
				return err
			}
			defer a.Close()

			bot, err := a.findBot(ctx, args[0])
			if err != nil {
				return err
			}

			// Cars that become unreachable: this bot is the last active member of
			// their channel.
			affected, err := a.Cars.AccessibleOnlyVia(ctx, bot.ID)
			if err != nil {
				return err
			}

			if len(affected) > 0 {
				var total int64
				for _, c := range affected {
					total += c.Size
				}
				fmt.Fprintf(stdout, "%d CARs (%s) will become inaccessible\n",
					len(affected), humanBytes(total))
				if !yes && !Confirm(fmt.Sprintf("Remove bot @%s?", bot.Username)) {
					fmt.Fprintln(stdout, "Cancelled")
					return nil
				}
			}

			if err := a.Bots.Remove(ctx, bot.ID); err != nil {
				return err
			}
			fmt.Fprintf(stdout, "Bot @%s removed\n", bot.Username)

			if len(affected) > 0 {
				purge := yes || Confirm("Purge now-inaccessible records from DB? (otherwise they remain and recover if the bot returns)")
				if purge {
					for _, c := range affected {
						if err := a.Cars.Delete(ctx, c.ID); err != nil && !errors.Is(err, domain.ErrNotFound) {
							return err
						}
					}
					fmt.Fprintf(stdout, "Deleted %d CAR records\n", len(affected))
				} else {
					fmt.Fprintln(stdout, "Records kept: statuses will lazily become no_bot_access")
				}
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "assume yes to all prompts")
	return cmd
}
