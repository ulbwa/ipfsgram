package maintenance

import (
	"context"
	"errors"
	"time"

	"github.com/ulbwa/ipfsgram/internal/domain"
)

// carCheck is the result of probing a published car's Telegram message.
type carCheck int

const (
	checkOK       carCheck = iota // message alive, file reachable
	checkDeleted                  // message physically deleted — unrecoverable
	checkNoAccess                 // no usable bot / bot lost access — recoverable
	checkTooLarge                 // file exceeds current transport limit — recoverable
)

// probeCarMessage checks whether the car's Telegram message is alive, trying
// bots until one succeeds or none remain. Flood-waited bots are marked in the
// database and skipped; when every eligible bot is merely flood-waited, the
// probe waits for the earliest expiry and retries. The returned error is
// non-nil only when the wait was interrupted (context cancelled).
func (s *Service) probeCarMessage(
	ctx context.Context, car domain.Car, ch domain.Channel, bots []domain.Bot,
) (carCheck, error) {
	if car.MessageID == nil {
		return checkNoAccess, nil
	}
	members, err := s.Channels.MembersOf(ctx, car.ChannelID)
	if err != nil {
		s.Logger.Warn().Err(err).Int64("car_id", car.ID).Msg("could not load bot membership")
		return checkNoAccess, nil
	}
	fileIDs, err := s.Cars.FileIDs(ctx, car.ID)
	if err != nil {
		s.Logger.Warn().Err(err).Int64("car_id", car.ID).Msg("could not load file_ids")
		fileIDs = nil
	}

	remaining := append([]domain.Bot(nil), bots...)
	for {
		bot, _, err := s.Selector.PickDownloadBot(time.Now(), remaining, members, fileIDs)
		if err != nil {
			earliest := earliestRecovery(remaining, members, func(m domain.BotChannel) bool {
				return m.Member && m.CanRead
			})
			if earliest == nil {
				s.Logger.Warn().Int64("car_id", car.ID).
					Msg("no bot can check car message — treating as no_bot_access")
				return checkNoAccess, nil
			}
			if werr := s.waitForFloodWait(ctx, *earliest); werr != nil {
				return checkNoAccess, werr
			}
			continue
		}

		cerr := s.Transport.CheckMessage(ctx, bot.Token, ch.TgID, *car.MessageID)
		var fw *domain.FloodWaitError
		switch {
		case cerr == nil:
			s.Selector.Record(bot.ID)
			return checkOK, nil
		case errors.Is(cerr, domain.ErrMessageDeleted):
			return checkDeleted, nil
		case errors.Is(cerr, domain.ErrTooLarge):
			return checkTooLarge, nil
		case errors.As(cerr, &fw):
			s.Selector.RecordError(bot.ID)
			until := time.Now().Add(fw.RetryAfter)
			if err := s.Bots.SetUnavailableUntil(ctx, bot.ID, until); err != nil {
				s.Logger.Warn().Err(err).Msg("could not record flood-wait")
			}
			markUnavailable(remaining, bot.ID, until)
		case errors.Is(cerr, domain.ErrNoAccess):
			s.Selector.RecordError(bot.ID)
			remaining = removeBot(remaining, bot.ID)
		default:
			s.Selector.RecordError(bot.ID)
			s.Logger.Warn().Err(cerr).Int64("car_id", car.ID).Str("bot", bot.Username).
				Msg("error checking message, trying another bot")
			remaining = removeBot(remaining, bot.ID)
		}
	}
}

// waitForFloodWait sleeps until the given time, logging progress. It returns the
// context error when cancelled mid-wait.
func (s *Service) waitForFloodWait(ctx context.Context, until time.Time) error {
	for {
		remaining := time.Until(until)
		if remaining <= 0 {
			return nil
		}
		s.Logger.Info().Int("seconds", int(remaining.Seconds())+1).
			Msg("all bots flood-waited, waiting")
		timer := time.NewTimer(min(remaining, 10*time.Second))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// earliestRecovery returns the earliest flood-wait expiry among active bots
// whose channel membership satisfies eligible, or nil when no such bot exists.
func earliestRecovery(bots []domain.Bot, members []domain.BotChannel, eligible func(domain.BotChannel) bool) *time.Time {
	ok := make(map[int64]bool, len(members))
	for _, m := range members {
		if eligible(m) {
			ok[m.BotID] = true
		}
	}
	var earliest *time.Time
	for _, b := range bots {
		if !b.Active || !ok[b.ID] || b.UnavailableUntil == nil {
			continue
		}
		if earliest == nil || b.UnavailableUntil.Before(*earliest) {
			t := *b.UnavailableUntil
			earliest = &t
		}
	}
	return earliest
}

// markUnavailable records the flood-wait expiry on the in-memory candidate list.
func markUnavailable(bots []domain.Bot, botID int64, until time.Time) {
	for i := range bots {
		if bots[i].ID == botID {
			bots[i].UnavailableUntil = &until
			return
		}
	}
}

// removeBot returns bots without the given ID.
func removeBot(bots []domain.Bot, id int64) []domain.Bot {
	out := bots[:0]
	for _, b := range bots {
		if b.ID != id {
			out = append(out, b)
		}
	}
	return out
}
