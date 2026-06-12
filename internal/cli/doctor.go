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
	"github.com/ulbwa/ipfsgram/internal/selector"
	"github.com/ulbwa/ipfsgram/internal/tg"
)

// orphanPendingAge is how old a pending car must be to count as orphaned.
const orphanPendingAge = 15 * time.Minute

// newDoctorCmd returns the top-level `ipfsgram doctor` command.
func newDoctorCmd() *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Диагностика: осиротевшие pending, ревалидация членства, восстановление CAR'ов",
		Args:  cobra.NoArgs,
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

			if err := doctorOrphanPending(ctx, e, yes); err != nil {
				return err
			}
			if err := doctorMembership(ctx, e, tr); err != nil {
				return err
			}
			return doctorRecovery(ctx, e, tr)
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "не задавать вопросов, отвечать «да»")
	return cmd
}

// doctorOrphanPending lists cars stuck in pending and deletes them after
// confirmation.
func doctorOrphanPending(ctx context.Context, e *env, yes bool) error {
	fmt.Fprintln(stdout, "Фаза 1: осиротевшие pending CAR'ы")
	orphans, err := e.Cars.OrphanPending(ctx, orphanPendingAge)
	if err != nil {
		return err
	}
	if len(orphans) == 0 {
		fmt.Fprintln(stdout, "  не найдено")
		return nil
	}
	for _, c := range orphans {
		fmt.Fprintf(stdout, "  car %d: канал %d, %s, %d блоков\n",
			c.ID, c.ChannelID, humanBytes(c.Size), c.BlockCount)
	}
	if !yes && !Confirm(fmt.Sprintf("Удалить %d осиротевших записей?", len(orphans))) {
		fmt.Fprintln(stdout, "  пропущено")
		return nil
	}
	for _, c := range orphans {
		if err := e.Cars.Delete(ctx, c.ID); err != nil && !errors.Is(err, repo.ErrNotFound) {
			return err
		}
	}
	fmt.Fprintf(stdout, "  удалено %d записей\n", len(orphans))
	return nil
}

// doctorMembership re-probes every bot × channel pair and reports diffs.
func doctorMembership(ctx context.Context, e *env, tr tg.Transport) error {
	fmt.Fprintln(stdout, "Фаза 2: ревалидация членства ботов")
	bots, err := e.Bots.List(ctx)
	if err != nil {
		return err
	}
	channels, err := e.Channels.List(ctx)
	if err != nil {
		return err
	}

	// Existing membership matrix in one inline query.
	var existing []model.BotChannel
	if err := e.db.SelectContext(ctx, &existing, `
		SELECT bot_id, channel_id, can_post, can_read, can_delete, member, verified_at
		FROM bot_channels`); err != nil {
		return fmt.Errorf("чтение bot_channels: %w", err)
	}
	wasMember := make(map[[2]int64]bool, len(existing))
	for _, bc := range existing {
		wasMember[[2]int64{bc.BotID, bc.ChannelID}] = bc.Member
	}

	changes := 0
	for _, b := range bots {
		for _, ch := range channels {
			info, perr := tr.ProbeChannel(ctx, b.Token, ch.TgID)
			if perr != nil {
				if !errors.Is(perr, tg.ErrNoAccess) {
					log.Warn().Err(perr).Str("bot", b.Username).Int64("channel_tg_id", ch.TgID).
						Msg("не удалось проверить доступ, пропускаем пару")
					continue
				}
				info = tg.ChannelInfo{}
			}
			if err := e.Channels.UpsertBotChannel(ctx, model.BotChannel{
				BotID: b.ID, ChannelID: ch.ID,
				CanPost: info.CanPost, CanRead: info.CanRead, CanDelete: info.CanDelete,
				Member: info.Member, VerifiedAt: time.Now(),
			}); err != nil {
				return err
			}
			was := wasMember[[2]int64{b.ID, ch.ID}]
			switch {
			case info.Member && !was:
				fmt.Fprintf(stdout, "  @%s получил доступ к каналу «%s»\n", b.Username, ch.Title)
				changes++
			case !info.Member && was:
				fmt.Fprintf(stdout, "  @%s потерял доступ к каналу «%s»\n", b.Username, ch.Title)
				changes++
			}
		}
	}
	if changes == 0 {
		fmt.Fprintln(stdout, "  изменений нет")
	}
	return nil
}

// doctorRecovery re-checks cars marked no_bot_access/too_large and restores
// them to published when the message is reachable again.
func doctorRecovery(ctx context.Context, e *env, tr tg.Transport) error {
	fmt.Fprintln(stdout, "Фаза 3: восстановление недоступных CAR'ов")

	// Inline query: the repos expose no status-filtered car listing.
	var cars []model.Car
	if err := e.db.SelectContext(ctx, &cars, `
		SELECT id, channel_id, message_id, size, block_count, status
		FROM cars WHERE status IN ('no_bot_access', 'too_large')
		ORDER BY id`); err != nil {
		return fmt.Errorf("чтение недоступных CAR'ов: %w", err)
	}
	if len(cars) == 0 {
		fmt.Fprintln(stdout, "  не найдено")
		return nil
	}

	bots, err := e.Bots.List(ctx)
	if err != nil {
		return err
	}
	channels, err := e.Channels.List(ctx)
	if err != nil {
		return err
	}
	chByID := make(map[int64]model.Channel, len(channels))
	for _, ch := range channels {
		chByID[ch.ID] = ch
	}

	lc := selector.NewLoadCounter()
	restored, removed := 0, 0
	for _, car := range cars {
		ch, ok := chByID[car.ChannelID]
		if !ok {
			continue
		}
		check, err := e.probeCarMessage(ctx, tr, car, ch, bots, lc)
		if err != nil {
			return err
		}
		switch check {
		case checkOK:
			if err := e.Cars.SetStatus(ctx, car.ID, model.CarPublished); err != nil {
				return err
			}
			restored++
		case checkDeleted:
			log.Warn().Int64("car_id", car.ID).
				Msg("сообщение CAR'а физически удалено — записи удаляются")
			if err := e.Cars.Delete(ctx, car.ID); err != nil && !errors.Is(err, repo.ErrNotFound) {
				return err
			}
			removed++
		case checkTooLarge:
			if car.Status != model.CarTooLarge {
				if err := e.Cars.SetStatus(ctx, car.ID, model.CarTooLarge); err != nil {
					return err
				}
			}
		case checkNoAccess:
			if car.Status != model.CarNoBotAccess {
				if err := e.Cars.SetStatus(ctx, car.ID, model.CarNoBotAccess); err != nil {
					return err
				}
			}
		}
	}
	fmt.Fprintf(stdout, "  восстановлено %d, удалено %d из %d\n", restored, removed, len(cars))
	return nil
}
