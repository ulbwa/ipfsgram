package cli

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/ulbwa/ipfsgram/internal/model"
	"github.com/ulbwa/ipfsgram/internal/repo"
	"github.com/ulbwa/ipfsgram/internal/tg"
)

// gcAdvisoryLockID serializes concurrent `ipfsgram gc` runs (and protects
// against races with parallel publishers) via a transaction-scoped advisory
// lock.
const gcAdvisoryLockID = 496818465

// newRmCmd returns the top-level `ipfsgram rm` command.
func newRmCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "rm <root-cid>",
		Short: "Снять пин с контента",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := cid.Decode(args[0])
			if err != nil {
				return fmt.Errorf("некорректный CID %q: %w", args[0], err)
			}
			ctx := cmd.Context()
			e, err := openEnv(ctx, cmd)
			if err != nil {
				return err
			}
			defer e.Close()

			if err := e.Pins.Remove(ctx, c.Bytes()); err != nil {
				if errors.Is(err, repo.ErrNotFound) {
					return errors.New("пин не найден")
				}
				return err
			}
			fmt.Fprintln(stdout, "Пин снят. Блоки будут освобождены при следующем `ipfsgram gc`.")
			return nil
		},
	}
}

// newGCCmd returns the top-level `ipfsgram gc` command.
func newGCCmd() *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "gc",
		Short: "Удалить CAR'ы, не принадлежащие ни одному пину",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			e, err := openEnv(ctx, cmd)
			if err != nil {
				return err
			}
			defer e.Close()

			// The advisory lock lives for the duration of this transaction,
			// serializing gc against other gc runs while we delete messages
			// and rows through the regular pool connections.
			tx, err := e.db.BeginTxx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback() //nolint:errcheck // no-op after commit
			if _, err := tx.ExecContext(ctx,
				`SELECT pg_advisory_xact_lock($1)`, gcAdvisoryLockID); err != nil {
				return fmt.Errorf("advisory lock: %w", err)
			}

			cars, err := e.Cars.UnpinnedCars(ctx)
			if err != nil {
				return err
			}
			// Pending cars belong to an in-flight or interrupted `add`
			// (no pin yet, no message to delete) — doctor handles them.
			var candidates []model.Car
			var total int64
			for _, c := range cars {
				if c.Status == model.CarPending {
					continue
				}
				candidates = append(candidates, c)
				total += c.Size
			}
			if len(candidates) == 0 {
				fmt.Fprintln(stdout, "нечего собирать")
				return tx.Commit()
			}

			fmt.Fprintf(stdout, "Будет удалено %d CAR'ов общим размером %s\n",
				len(candidates), humanBytes(total))
			if !yes && !Confirm("Продолжить?") {
				fmt.Fprintln(stdout, "Отменено")
				return tx.Commit()
			}

			tr, err := e.transport(ctx)
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
			bots, err := e.Bots.List(ctx)
			if err != nil {
				return err
			}
			botByID := make(map[int64]model.Bot, len(bots))
			for _, b := range bots {
				botByID[b.ID] = b
			}

			deleted := 0
			for _, car := range candidates {
				ch, ok := chByID[car.ChannelID]
				if !ok {
					log.Warn().Int64("car_id", car.ID).Msg("канал CAR'а не найден, пропускаем")
					continue
				}
				if e.gcDeleteCar(ctx, tr, car, ch, botByID) {
					deleted++
				}
			}
			fmt.Fprintf(stdout, "Удалено %d из %d CAR'ов\n", deleted, len(candidates))
			return tx.Commit()
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "не задавать вопросов, отвечать «да»")
	return cmd
}

// gcDeleteCar deletes the car's Telegram message via a bot with can_delete
// and, on success (or if the message is already gone), removes the car row.
// Returns true when the row was deleted.
//
// NOTE: deleting a message does NOT decrement channels.message_count —
// Telegram counts the total number of posts ever made in a channel, so the
// counter must keep growing monotonically.
func (e *env) gcDeleteCar(
	ctx context.Context, tr tg.Transport, car model.Car,
	ch model.Channel, botByID map[int64]model.Bot,
) bool {
	if car.MessageID == nil {
		// Not pending but no message id: bookkeeping leftover, just drop it.
		return e.gcDropRow(ctx, car.ID)
	}
	members, err := e.Channels.MembersOf(ctx, car.ChannelID)
	if err != nil {
		log.Warn().Err(err).Int64("car_id", car.ID).Msg("не удалось получить членство ботов")
		return false
	}

	now := time.Now()
	for _, m := range members {
		if !m.Member || !m.CanDelete {
			continue
		}
		bot, ok := botByID[m.BotID]
		if !ok || !bot.Active {
			continue
		}
		if bot.UnavailableUntil != nil && now.Before(*bot.UnavailableUntil) {
			continue
		}

		err := tr.DeleteMessage(ctx, bot.Token, ch.TgID, *car.MessageID)
		var fw *tg.FloodWaitError
		switch {
		case err == nil, errors.Is(err, tg.ErrMessageDeleted):
			return e.gcDropRow(ctx, car.ID)
		case errors.As(err, &fw):
			if serr := e.Bots.SetUnavailableUntil(ctx, bot.ID, time.Now().Add(fw.RetryAfter)); serr != nil {
				log.Warn().Err(serr).Msg("не удалось записать flood-wait")
			}
			continue // try another bot
		case errors.Is(err, tg.ErrNoAccess):
			continue // try another bot
		default:
			log.Warn().Err(err).Int64("car_id", car.ID).Str("bot", bot.Username).
				Msg("ошибка удаления сообщения, пробуем другого бота")
			continue
		}
	}
	log.Warn().Int64("car_id", car.ID).
		Msg("ни один бот не смог удалить сообщение — записи оставлены")
	return false
}

// gcDropRow deletes the car row (blocks and file_ids cascade).
func (e *env) gcDropRow(ctx context.Context, carID int64) bool {
	if err := e.Cars.Delete(ctx, carID); err != nil && !errors.Is(err, repo.ErrNotFound) {
		log.Warn().Err(err).Int64("car_id", carID).Msg("не удалось удалить запись CAR'а")
		return false
	}
	return true
}
