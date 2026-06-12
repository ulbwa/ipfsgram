// gc.go — garbage collection of unpinned CARs: the lock-free candidate
// preview and the deletion pass under the exclusive advisory lock.

package maintain

import (
	"context"
	"errors"
	"time"

	"github.com/ulbwa/ipfsgram/internal/store"
	"github.com/ulbwa/ipfsgram/internal/telegram"
)

// GCCandidates returns the unpinned cars eligible for collection and their total
// size. It takes no lock so the caller can preview and prompt without stalling
// concurrent publishers. Pending cars belong to an in-flight or interrupted
// publish (no pin yet, no message to delete) — the doctor handles those.
func (s *Service) GCCandidates(ctx context.Context) ([]store.Car, int64, error) {
	cars, err := s.Store.UnpinnedCars(ctx)
	if err != nil {
		return nil, 0, err
	}
	var candidates []store.Car
	var total int64
	for _, c := range cars {
		if c.Status == store.CarPending {
			continue
		}
		candidates = append(candidates, c)
		total += c.Size
	}
	return candidates, total, nil
}

// GC deletes unpinned cars under the exclusive advisory lock (mutually excluding
// every publisher). It re-fetches the candidate list inside the lock — pins may
// have appeared or vanished while the caller's confirmation prompt was open —
// then for each car deletes the Telegram message via a can_delete member bot and
// removes the row. It returns the number of cars deleted.
func (s *Service) GC(ctx context.Context) (int, error) {
	deleted := 0
	err := s.Store.WithExclusiveGCLock(ctx, func(ctx context.Context) error {
		candidates, total, err := s.GCCandidates(ctx)
		if err != nil {
			return err
		}
		if len(candidates) == 0 {
			return nil
		}

		channels, err := s.Store.Channels(ctx)
		if err != nil {
			return err
		}
		chByID := make(map[int64]store.Channel, len(channels))
		for _, ch := range channels {
			chByID[ch.ID] = ch
		}
		bots, err := s.Store.Bots(ctx)
		if err != nil {
			return err
		}
		botByID := make(map[int64]store.Bot, len(bots))
		for _, b := range bots {
			botByID[b.ID] = b
		}

		s.Logger.Info().Int("cars", len(candidates)).Int64("bytes", total).
			Msg("garbage-collecting unpinned cars")

		for _, car := range candidates {
			ch, ok := chByID[car.ChannelID]
			if !ok {
				s.Logger.Warn().Int64("car_id", car.ID).Msg("car channel not found, skipping")
				continue
			}
			if s.deleteCar(ctx, car, ch, botByID) {
				deleted++
			}
		}
		return nil
	})
	if err != nil {
		return deleted, err
	}
	return deleted, nil
}

// deleteCar deletes the car's Telegram message via a bot with can_delete and, on
// success (or if the message is already gone), removes the car row. Returns true
// when the row was deleted. telegram.ErrMessageDeleted is tolerated; on
// telegram.ErrNoAccess the car is skipped and its rows kept.
//
// NOTE: deleting a message does NOT decrement channels.message_count — Telegram
// counts the total number of posts ever made in a channel, so the counter must
// keep growing monotonically.
func (s *Service) deleteCar(
	ctx context.Context, car store.Car, ch store.Channel, botByID map[int64]store.Bot,
) bool {
	if car.MessageID == nil {
		// Not pending but no message id: bookkeeping leftover, just drop it.
		return s.dropRow(ctx, car.ID)
	}
	members, err := s.Store.ChannelMembers(ctx, car.ChannelID)
	if err != nil {
		s.Logger.Warn().Err(err).Int64("car_id", car.ID).Msg("could not load bot membership")
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

		err := s.Transport.DeleteMessage(ctx, bot.Token, ch.TgID, *car.MessageID)
		var fw *telegram.FloodWaitError
		switch {
		case err == nil, errors.Is(err, telegram.ErrMessageDeleted):
			return s.dropRow(ctx, car.ID)
		case errors.As(err, &fw):
			if serr := s.Store.SetBotUnavailable(ctx, bot.ID, time.Now().Add(fw.RetryAfter)); serr != nil {
				s.Logger.Warn().Err(serr).Msg("could not record flood-wait")
			}
			continue // try another bot
		case errors.Is(err, telegram.ErrNoAccess):
			continue // try another bot
		default:
			s.Logger.Warn().Err(err).Int64("car_id", car.ID).Str("bot", bot.Username).
				Msg("error deleting message, trying another bot")
			continue
		}
	}
	s.Logger.Warn().Int64("car_id", car.ID).
		Msg("no bot could delete the message — records kept")
	return false
}

// dropRow deletes the car row (blocks and file_ids cascade).
func (s *Service) dropRow(ctx context.Context, carID int64) bool {
	if err := s.Store.DeleteCar(ctx, carID); err != nil && !errors.Is(err, store.ErrNotFound) {
		s.Logger.Warn().Err(err).Int64("car_id", carID).Msg("could not delete car row")
		return false
	}
	return true
}
