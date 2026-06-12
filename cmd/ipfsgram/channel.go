// channel.go — the `ipfsgram channel` command group: add (probing every bot's
// access), list, and removal backed by internal/maintain's plan/execute split.

package main

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/ulbwa/ipfsgram/internal/store"
	"github.com/ulbwa/ipfsgram/internal/telegram"
)

// defaultMessageLimit mirrors the channels.message_limit schema default.
const defaultMessageLimit = 1_000_000

// newChannelCmd returns the `ipfsgram channel` command group.
func newChannelCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "channel",
		Short: "Manage channels",
	}
	cmd.AddCommand(newChannelAddCmd(), newChannelListCmd(), newChannelRemoveCmd())
	return cmd
}

func newChannelAddCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "add <tg_id>",
		Short: "Add a channel by Telegram ID",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			tgID, err := strconv.ParseInt(args[0], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid tg_id %q", args[0])
			}
			ctx := cmd.Context()
			a, err := openApp(ctx, cmd, "")
			if err != nil {
				return err
			}
			defer a.Close()

			if _, err := a.Store.ChannelByTgID(ctx, tgID); err == nil {
				return fmt.Errorf("channel %d is already added", tgID)
			} else if !errors.Is(err, store.ErrNotFound) {
				return err
			}

			bots, err := a.Store.Bots(ctx)
			if err != nil {
				return err
			}
			if len(bots) == 0 {
				return errors.New("add at least one bot first")
			}

			infos := make(map[int64]telegram.ChannelInfo, len(bots))
			title := ""
			members := 0
			for _, b := range bots {
				info, perr := a.Transport.ProbeChannel(ctx, b.Token, tgID)
				if perr != nil {
					if !errors.Is(perr, telegram.ErrNoAccess) {
						log.Warn().Err(perr).Str("bot", b.Username).
							Msg("could not probe bot access to channel")
					}
					infos[b.ID] = telegram.ChannelInfo{}
					continue
				}
				infos[b.ID] = info
				if info.Member {
					members++
					if title == "" {
						title = info.Title
					}
				}
			}
			if members == 0 {
				return fmt.Errorf("no bot has access to channel %d", tgID)
			}

			chID, err := a.Store.AddChannel(ctx, store.Channel{
				TgID: tgID, Title: title,
				MessageLimit: defaultMessageLimit, Active: true,
			})
			if err != nil {
				return err
			}
			for _, b := range bots {
				info := infos[b.ID]
				if err := a.Store.UpsertBotChannel(ctx, store.BotChannel{
					BotID: b.ID, ChannelID: chID,
					CanPost: info.CanPost, CanRead: info.CanRead, CanDelete: info.CanDelete,
					Member: info.Member, VerifiedAt: time.Now(),
				}); err != nil {
					return err
				}
			}

			fmt.Fprintf(stdout, "Channel %q added (id %d), bot membership:\n", title, chID)
			for _, b := range bots {
				info := infos[b.ID]
				fmt.Fprintf(stdout, "  @%-24s member=%-5t post=%-5t read=%-5t delete=%t\n",
					b.Username, info.Member, info.CanPost, info.CanRead, info.CanDelete)
			}
			return nil
		},
	}
}

func newChannelListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List channels",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			a, err := openApp(ctx, cmd, "")
			if err != nil {
				return err
			}
			defer a.Close()

			channels, err := a.Store.Channels(ctx)
			if err != nil {
				return err
			}

			fmt.Fprintf(stdout, "%-5s %-16s %-24s %-24s %-8s %s\n",
				"ID", "TG_ID", "TITLE", "MESSAGES", "ACTIVE", "BOTS")
			for _, ch := range channels {
				members, err := a.Store.ChannelMembers(ctx, ch.ID)
				if err != nil {
					return err
				}
				botCount := 0
				for _, m := range members {
					if m.Member {
						botCount++
					}
				}
				pct := 0.0
				if ch.MessageLimit > 0 {
					pct = float64(ch.MessageCount) / float64(ch.MessageLimit) * 100
				}
				fmt.Fprintf(stdout, "%-5d %-16d %-24s %-24s %-8t %d\n",
					ch.ID, ch.TgID, ch.Title,
					fmt.Sprintf("%d/%d (%.1f%%)", ch.MessageCount, ch.MessageLimit, pct),
					ch.Active, botCount)
			}
			return nil
		},
	}
}

func newChannelRemoveCmd() *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "remove <tg_id>",
		Short: "Remove a channel (database records only)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			tgID, err := strconv.ParseInt(args[0], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid tg_id %q", args[0])
			}
			ctx := cmd.Context()
			a, err := openApp(ctx, cmd, "")
			if err != nil {
				return err
			}
			defer a.Close()

			ch, err := a.Store.ChannelByTgID(ctx, tgID)
			if errors.Is(err, store.ErrNotFound) {
				return fmt.Errorf("channel %d not found", tgID)
			}
			if err != nil {
				return err
			}

			svc := a.maintainService()

			pins, bytes, err := svc.ChannelRemovePlan(ctx, ch.ID)
			if err != nil {
				return err
			}
			prompt := fmt.Sprintf(
				"Delete channel %q and %d pins (size %s)? CAR and block records will be deleted",
				ch.Title, pins, humanBytes(bytes))
			if !yes && !Confirm(prompt) {
				fmt.Fprintln(stdout, "Cancelled")
				return nil
			}

			if err := svc.ChannelRemoveExecute(ctx, ch.ID); err != nil {
				return err
			}

			fmt.Fprintf(stdout, "Channel %q removed from the database. Telegram messages are not deleted by this command.\n", ch.Title)
			return nil
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "assume yes to all prompts")
	return cmd
}
