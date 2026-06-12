package cli

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/ulbwa/ipfsgram/internal/model"
	"github.com/ulbwa/ipfsgram/internal/repo"
	"github.com/ulbwa/ipfsgram/internal/tg"
)

// newBotCmd returns the `ipfsgram bot` command group.
func newBotCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bot",
		Short: "Управление ботами",
	}
	cmd.AddCommand(newBotAddCmd(), newBotListCmd(), newBotRemoveCmd())
	return cmd
}

func newBotAddCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "add <token>",
		Short: "Добавить бота по токену",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			e, err := openEnv(ctx, cmd)
			if err != nil {
				return err
			}
			defer e.Close()

			tr, err := e.transport(ctx)
			if err != nil {
				return err
			}

			token := args[0]
			tgID, username, err := tr.ValidateToken(ctx, token)
			if err != nil {
				return fmt.Errorf("недействительный токен: %w", err)
			}

			botID, err := e.Bots.Add(ctx, model.Bot{
				TgID: tgID, Username: username, Token: token, Active: true,
			})
			if errors.Is(err, repo.ErrBotExists) {
				return fmt.Errorf("бот @%s уже добавлен", username)
			}
			if err != nil {
				return err
			}

			channels, err := e.Channels.List(ctx)
			if err != nil {
				return err
			}
			member := 0
			for _, ch := range channels {
				info, perr := tr.ProbeChannel(ctx, token, ch.TgID)
				if perr != nil {
					if !errors.Is(perr, tg.ErrNoAccess) {
						log.Warn().Err(perr).Int64("channel_tg_id", ch.TgID).
							Msg("не удалось проверить доступ бота к каналу")
						continue
					}
					info = tg.ChannelInfo{} // нет доступа: member=false
				}
				if err := e.Channels.UpsertBotChannel(ctx, model.BotChannel{
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
			fmt.Fprintf(stdout, "Бот @%s добавлен (id %d), состоит в %d из %d каналов\n",
				username, botID, member, len(channels))
			return nil
		},
	}
}

func newBotListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "Список ботов",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			e, err := openEnv(ctx, cmd)
			if err != nil {
				return err
			}
			defer e.Close()

			bots, err := e.Bots.List(ctx)
			if err != nil {
				return err
			}
			counts, err := botChannelCounts(ctx, e)
			if err != nil {
				return err
			}

			fmt.Fprintf(stdout, "%-5s %-24s %-8s %-20s %s\n",
				"ID", "USERNAME", "ACTIVE", "FLOOD-WAIT", "CHANNELS")
			for _, b := range bots {
				fw := "-"
				if b.UnavailableUntil != nil && time.Now().Before(*b.UnavailableUntil) {
					fw = b.UnavailableUntil.Format(time.RFC3339)
				}
				fmt.Fprintf(stdout, "%-5d %-24s %-8t %-20s %d\n",
					b.ID, "@"+b.Username, b.Active, fw, counts[b.ID])
			}
			return nil
		},
	}
}

// botChannelCounts returns, per bot, the number of channels the bot is a
// member of.
func botChannelCounts(ctx context.Context, e *env) (map[int64]int, error) {
	rows, err := e.db.QueryContext(ctx,
		`SELECT bot_id, count(*) FROM bot_channels WHERE member GROUP BY bot_id`)
	if err != nil {
		return nil, fmt.Errorf("счётчики членства ботов: %w", err)
	}
	defer rows.Close()
	out := make(map[int64]int)
	for rows.Next() {
		var id int64
		var n int
		if err := rows.Scan(&id, &n); err != nil {
			return nil, err
		}
		out[id] = n
	}
	return out, rows.Err()
}

func newBotRemoveCmd() *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "remove <id|@username>",
		Short: "Удалить бота",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			e, err := openEnv(ctx, cmd)
			if err != nil {
				return err
			}
			defer e.Close()

			bot, err := e.findBot(ctx, args[0])
			if err != nil {
				return err
			}

			// Cars that become unreachable: this bot is the last active
			// member of their channel. Inline query: the repos expose no
			// "last member" view.
			type carRow struct {
				ID   int64 `db:"id"`
				Size int64 `db:"size"`
			}
			var affected []carRow
			err = e.db.SelectContext(ctx, &affected, `
				SELECT c.id, c.size FROM cars c
				WHERE EXISTS (
					SELECT 1 FROM bot_channels bc
					WHERE bc.channel_id = c.channel_id AND bc.bot_id = $1 AND bc.member
				)
				AND NOT EXISTS (
					SELECT 1 FROM bot_channels bc2
					JOIN bots b ON b.id = bc2.bot_id
					WHERE bc2.channel_id = c.channel_id
					  AND bc2.bot_id <> $1 AND bc2.member AND b.active
				)
				ORDER BY c.id`, bot.ID)
			if err != nil {
				return fmt.Errorf("поиск затронутых архивов: %w", err)
			}

			if len(affected) > 0 {
				var total int64
				for _, c := range affected {
					total += c.Size
				}
				fmt.Fprintf(stdout, "%d архивов (%s) станут недоступны\n",
					len(affected), humanBytes(total))
				if !yes && !Confirm(fmt.Sprintf("Удалить бота @%s?", bot.Username)) {
					fmt.Fprintln(stdout, "Отменено")
					return nil
				}
			}

			if err := e.Bots.Remove(ctx, bot.ID); err != nil {
				return err
			}
			fmt.Fprintf(stdout, "Бот @%s удалён\n", bot.Username)

			if len(affected) > 0 {
				cleanup := yes || Confirm("Очистить недоступные записи из БД? (иначе они останутся и восстановятся при возврате бота)")
				if cleanup {
					for _, c := range affected {
						if err := e.Cars.Delete(ctx, c.ID); err != nil && !errors.Is(err, repo.ErrNotFound) {
							return err
						}
					}
					fmt.Fprintf(stdout, "Удалено %d записей о CAR'ах\n", len(affected))
				} else {
					fmt.Fprintln(stdout, "Записи оставлены: статусы станут no_bot_access лениво")
				}
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "не задавать вопросов, отвечать «да»")
	return cmd
}
