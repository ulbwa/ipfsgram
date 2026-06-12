// probe.go — probing a published car's Telegram message, with flood-wait
// bookkeeping and bot failover. Shared by gc and doctor recovery.

// Package probe checks whether a published car's Telegram message is still
// alive and its file reachable, trying eligible bots until one succeeds or none
// remain, marking flood-waited bots and waiting out the earliest expiry when
// every candidate is merely flood-waited. The narrow store/telegram interfaces
// it needs are declared here, on the consumer side.
package probe

import (
	"context"
	"errors"
	"time"

	"github.com/rs/zerolog"

	"github.com/ulbwa/ipfsgram/internal/selector"
	"github.com/ulbwa/ipfsgram/internal/store"
	"github.com/ulbwa/ipfsgram/internal/telegram"
)

// storage is the subset of *store.Store the probe calls. *store.Store satisfies
// it implicitly.
type storage interface {
	ChannelMembers(ctx context.Context, channelID int64) ([]store.BotChannel, error)
	CarFileIDs(ctx context.Context, carID int64) (map[int64]string, error)
	SetBotUnavailable(ctx context.Context, id int64, until time.Time) error
}

// transport is the subset of the Telegram client the probe calls.
// *telegram.Client satisfies it implicitly.
type transport interface {
	CheckMessage(ctx context.Context, token string, channelTgID, messageID int64) error
}

// Check is the result of probing a published car's Telegram message.
type Check int

const (
	CheckOK       Check = iota // message alive, file reachable
	CheckDeleted               // message physically deleted — unrecoverable
	CheckNoAccess              // no usable bot / bot lost access — recoverable
	CheckTooLarge              // file exceeds current transport limit — recoverable
)

// Service probes car messages. Store is *store.Store and Transport a telegram
// client in production; Loads is the shared selector load counter.
type Service struct {
	Store     storage
	Transport transport
	Loads     *selector.LoadCounter
	Logger    zerolog.Logger
}

// CarMessage checks whether the car's Telegram message is alive, trying bots
// until one succeeds or none remain. Flood-waited bots are marked in the
// database and skipped; when every eligible bot is merely flood-waited, the
// probe waits for the earliest expiry and retries. The returned error is
// non-nil only when the wait was interrupted (context cancelled).
func (s *Service) CarMessage(
	ctx context.Context, car store.Car, ch store.Channel, bots []store.Bot,
) (Check, error) {
	if car.MessageID == nil {
		return CheckNoAccess, nil
	}
	members, err := s.Store.ChannelMembers(ctx, car.ChannelID)
	if err != nil {
		s.Logger.Warn().Err(err).Int64("car_id", car.ID).Msg("could not load bot membership")
		return CheckNoAccess, nil
	}
	fileIDs, err := s.Store.CarFileIDs(ctx, car.ID)
	if err != nil {
		s.Logger.Warn().Err(err).Int64("car_id", car.ID).Msg("could not load file_ids")
		fileIDs = nil
	}

	remaining := append([]store.Bot(nil), bots...)
	for {
		bot, _, err := selector.PickDownloadBot(time.Now(), remaining, members, fileIDs, s.Loads)
		if err != nil {
			earliest := earliestRecovery(remaining, members, func(m store.BotChannel) bool {
				return m.Member && m.CanRead
			})
			if earliest == nil {
				s.Logger.Warn().Int64("car_id", car.ID).
					Msg("no bot can check car message — treating as no_bot_access")
				return CheckNoAccess, nil
			}
			if werr := s.waitForFloodWait(ctx, *earliest); werr != nil {
				return CheckNoAccess, werr
			}
			continue
		}

		check, decided := s.checkWithBot(ctx, car, ch, bot, &remaining)
		if decided {
			return check, nil
		}
	}
}

// checkWithBot probes the car's message with one bot. It returns (check, true)
// when the bot produced a conclusive result, or (_, false) to retry with
// another bot — having marked the bot flood-waited or dropped it from remaining.
func (s *Service) checkWithBot(
	ctx context.Context, car store.Car, ch store.Channel, bot store.Bot, remaining *[]store.Bot,
) (Check, bool) {
	cerr := s.Transport.CheckMessage(ctx, bot.Token, ch.TgID, *car.MessageID)
	var fw *telegram.FloodWaitError
	switch {
	case cerr == nil:
		s.Loads.Record(bot.ID)
		return CheckOK, true
	case errors.Is(cerr, telegram.ErrMessageDeleted):
		return CheckDeleted, true
	case errors.Is(cerr, telegram.ErrTooLarge):
		return CheckTooLarge, true
	case errors.As(cerr, &fw):
		s.Loads.RecordError(bot.ID)
		until := time.Now().Add(fw.RetryAfter)
		if err := s.Store.SetBotUnavailable(ctx, bot.ID, until); err != nil {
			s.Logger.Warn().Err(err).Msg("could not record flood-wait")
		}
		markUnavailable(*remaining, bot.ID, until)
	case errors.Is(cerr, telegram.ErrNoAccess):
		s.Loads.RecordError(bot.ID)
		*remaining = removeBot(*remaining, bot.ID)
	default:
		s.Loads.RecordError(bot.ID)
		s.Logger.Warn().Err(cerr).Int64("car_id", car.ID).Str("bot", bot.Username).
			Msg("error checking message, trying another bot")
		*remaining = removeBot(*remaining, bot.ID)
	}
	return 0, false
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
func earliestRecovery(bots []store.Bot, members []store.BotChannel, eligible func(store.BotChannel) bool) *time.Time {
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
func markUnavailable(bots []store.Bot, botID int64, until time.Time) {
	for i := range bots {
		if bots[i].ID == botID {
			bots[i].UnavailableUntil = &until
			return
		}
	}
}

// removeBot returns bots without the given ID.
func removeBot(bots []store.Bot, id int64) []store.Bot {
	out := bots[:0]
	for _, b := range bots {
		if b.ID != id {
			out = append(out, b)
		}
	}
	return out
}
