// doctor.go — the doctor's diagnostics: orphaned pending cars, bot×channel
// membership revalidation with a gained/lost diff, and recovery of cars
// marked no_bot_access/too_large.

// Package doctor implements `ipfsgram doctor`: cleaning orphaned pending cars,
// revalidating the bot×channel membership matrix (reporting gained/lost
// access), and recovering cars marked no_bot_access/too_large by re-probing
// their Telegram message (via internal/probe). The narrow store/telegram
// interfaces it needs are declared here, on the consumer side.
package doctor

import (
	"context"
	"errors"
	"time"

	"github.com/rs/zerolog"

	"github.com/ulbwa/ipfsgram/internal/probe"
	"github.com/ulbwa/ipfsgram/internal/selector"
	"github.com/ulbwa/ipfsgram/internal/store"
	"github.com/ulbwa/ipfsgram/internal/telegram"
)

// OrphanPendingAge is how old a pending car must be to count as orphaned.
const OrphanPendingAge = 15 * time.Minute

// storage is the subset of *store.Store the doctor workflows call. It is a
// superset of internal/probe's storage needs so the same *store.Store backs the
// recovery probe. *store.Store satisfies it implicitly.
type storage interface {
	OrphanPendingCars(ctx context.Context, olderThan time.Duration) ([]store.Car, error)
	CarsWithStatus(ctx context.Context, statuses ...store.CarStatus) ([]store.Car, error)
	SetCarStatus(ctx context.Context, carID int64, st store.CarStatus) error
	DeleteCar(ctx context.Context, carID int64) error
	Bots(ctx context.Context) ([]store.Bot, error)
	Channels(ctx context.Context) ([]store.Channel, error)
	ChannelMembers(ctx context.Context, channelID int64) ([]store.BotChannel, error)
	UpsertBotChannel(ctx context.Context, bc store.BotChannel) error

	// Used by the recovery probe.
	CarFileIDs(ctx context.Context, carID int64) (map[int64]string, error)
	SetBotUnavailable(ctx context.Context, id int64, until time.Time) error
}

// transport is the subset of the Telegram client the doctor workflows call. It
// is a superset of internal/probe's transport needs so the same client backs
// the recovery probe. *telegram.Client satisfies it implicitly.
type transport interface {
	ProbeChannel(ctx context.Context, token string, channelTgID int64) (telegram.ChannelInfo, error)
	CheckMessage(ctx context.Context, token string, channelTgID, messageID int64) error
}

// Service performs the doctor's diagnostics. Store is *store.Store and
// Transport a telegram client in production; Loads is the shared selector load
// counter (required by the recovery probe).
type Service struct {
	Store     storage
	Transport transport
	Loads     *selector.LoadCounter
	Logger    zerolog.Logger
}

// MembershipChange describes a bot's gained or lost membership of a channel,
// detected during RevalidateMembership.
type MembershipChange struct {
	BotUsername  string
	ChannelTitle string
	Gained       bool // true: gained access; false: lost access
}

// MembershipReport is the outcome of RevalidateMembership.
type MembershipReport struct {
	Changes []MembershipChange
}

// DoctorOrphans returns pending cars older than olderThan — interrupted or
// in-flight publishes whose pending rows were never completed. It takes no lock
// so the caller can preview and confirm before calling CleanOrphans.
func (s *Service) DoctorOrphans(ctx context.Context, olderThan time.Duration) ([]store.Car, error) {
	return s.Store.OrphanPendingCars(ctx, olderThan)
}

// CleanOrphans deletes the given orphaned pending car rows (blocks and file_ids
// cascade). store.ErrNotFound on any row is tolerated (already removed).
func (s *Service) CleanOrphans(ctx context.Context, cars []store.Car) error {
	for _, c := range cars {
		if err := s.Store.DeleteCar(ctx, c.ID); err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
	}
	return nil
}

// RevalidateMembership re-probes every bot × channel pair, upserts the fresh
// permission matrix, and reports the membership diffs (gained/lost access).
func (s *Service) RevalidateMembership(ctx context.Context) (MembershipReport, error) {
	var report MembershipReport
	bots, err := s.Store.Bots(ctx)
	if err != nil {
		return report, err
	}
	channels, err := s.Store.Channels(ctx)
	if err != nil {
		return report, err
	}

	// Previous membership matrix, gathered per channel.
	wasMember := make(map[[2]int64]bool)
	for _, ch := range channels {
		members, err := s.Store.ChannelMembers(ctx, ch.ID)
		if err != nil {
			return report, err
		}
		for _, bc := range members {
			wasMember[[2]int64{bc.BotID, ch.ID}] = bc.Member
		}
	}

	for _, b := range bots {
		for _, ch := range channels {
			info, perr := s.Transport.ProbeChannel(ctx, b.Token, ch.TgID)
			if perr != nil {
				if !errors.Is(perr, telegram.ErrNoAccess) {
					s.Logger.Warn().Err(perr).Str("bot", b.Username).Int64("channel_tg_id", ch.TgID).
						Msg("could not probe access, skipping pair")
					continue
				}
				info = telegram.ChannelInfo{}
			}
			if err := s.Store.UpsertBotChannel(ctx, store.BotChannel{
				BotID: b.ID, ChannelID: ch.ID,
				CanPost: info.CanPost, CanRead: info.CanRead, CanDelete: info.CanDelete,
				Member: info.Member, VerifiedAt: time.Now(),
			}); err != nil {
				return report, err
			}
			was := wasMember[[2]int64{b.ID, ch.ID}]
			switch {
			case info.Member && !was:
				report.Changes = append(report.Changes, MembershipChange{
					BotUsername: b.Username, ChannelTitle: ch.Title, Gained: true,
				})
			case !info.Member && was:
				report.Changes = append(report.Changes, MembershipChange{
					BotUsername: b.Username, ChannelTitle: ch.Title, Gained: false,
				})
			}
		}
	}
	return report, nil
}

// Recover re-checks cars marked no_bot_access/too_large and restores them to
// published when their message is reachable again. A physically deleted message
// causes the car (and its cascading rows) to be removed. It returns the number
// of cars restored and deleted.
func (s *Service) Recover(ctx context.Context) (restored int, deleted int, err error) {
	cars, err := s.Store.CarsWithStatus(ctx, store.CarNoBotAccess, store.CarTooLarge)
	if err != nil {
		return 0, 0, err
	}
	if len(cars) == 0 {
		return 0, 0, nil
	}

	bots, err := s.Store.Bots(ctx)
	if err != nil {
		return 0, 0, err
	}
	channels, err := s.Store.Channels(ctx)
	if err != nil {
		return 0, 0, err
	}
	chByID := make(map[int64]store.Channel, len(channels))
	for _, ch := range channels {
		chByID[ch.ID] = ch
	}

	pr := &probe.Service{
		Store:     s.Store,
		Transport: s.Transport,
		Loads:     s.Loads,
		Logger:    s.Logger,
	}

	for _, car := range cars {
		ch, ok := chByID[car.ChannelID]
		if !ok {
			continue
		}
		check, err := pr.CarMessage(ctx, car, ch, bots)
		if err != nil {
			return restored, deleted, err
		}
		didRestore, didDelete, err := s.applyRecoveryCheck(ctx, car, check)
		if err != nil {
			return restored, deleted, err
		}
		if didRestore {
			restored++
		}
		if didDelete {
			deleted++
		}
	}
	return restored, deleted, nil
}

// applyRecoveryCheck persists the outcome of probing a car: restoring it to
// published, deleting it (message gone), or refreshing its unavailable status.
// It reports whether the car was restored or deleted.
func (s *Service) applyRecoveryCheck(ctx context.Context, car store.Car, check probe.Check) (restored, deleted bool, err error) {
	switch check {
	case probe.CheckOK:
		if err := s.Store.SetCarStatus(ctx, car.ID, store.CarPublished); err != nil {
			return false, false, err
		}
		return true, false, nil
	case probe.CheckDeleted:
		s.Logger.Warn().Int64("car_id", car.ID).
			Msg("car message physically deleted — removing records")
		if err := s.Store.DeleteCar(ctx, car.ID); err != nil && !errors.Is(err, store.ErrNotFound) {
			return false, false, err
		}
		return false, true, nil
	case probe.CheckTooLarge:
		if car.Status != store.CarTooLarge {
			if err := s.Store.SetCarStatus(ctx, car.ID, store.CarTooLarge); err != nil {
				return false, false, err
			}
		}
	case probe.CheckNoAccess:
		if car.Status != store.CarNoBotAccess {
			if err := s.Store.SetCarStatus(ctx, car.ID, store.CarNoBotAccess); err != nil {
				return false, false, err
			}
		}
	}
	return false, false, nil
}
