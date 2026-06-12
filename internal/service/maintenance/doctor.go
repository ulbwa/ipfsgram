package maintenance

import (
	"context"
	"errors"
	"time"

	"github.com/ulbwa/ipfsgram/internal/domain"
	"github.com/ulbwa/ipfsgram/internal/port"
)

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
func (s *Service) DoctorOrphans(ctx context.Context, olderThan time.Duration) ([]domain.Car, error) {
	return s.Cars.OrphanPending(ctx, olderThan)
}

// CleanOrphans deletes the given orphaned pending car rows (blocks and file_ids
// cascade). domain.ErrNotFound on any row is tolerated (already removed).
func (s *Service) CleanOrphans(ctx context.Context, cars []domain.Car) error {
	for _, c := range cars {
		if err := s.Cars.Delete(ctx, c.ID); err != nil && !errors.Is(err, domain.ErrNotFound) {
			return err
		}
	}
	return nil
}

// RevalidateMembership re-probes every bot × channel pair, upserts the fresh
// permission matrix, and reports the membership diffs (gained/lost access).
func (s *Service) RevalidateMembership(ctx context.Context) (MembershipReport, error) {
	var report MembershipReport
	bots, err := s.Bots.List(ctx)
	if err != nil {
		return report, err
	}
	channels, err := s.Channels.List(ctx)
	if err != nil {
		return report, err
	}

	// Previous membership matrix, gathered per channel.
	wasMember := make(map[[2]int64]bool)
	for _, ch := range channels {
		members, err := s.Channels.MembersOf(ctx, ch.ID)
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
				if !errors.Is(perr, domain.ErrNoAccess) {
					s.Logger.Warn().Err(perr).Str("bot", b.Username).Int64("channel_tg_id", ch.TgID).
						Msg("could not probe access, skipping pair")
					continue
				}
				info = port.ChannelInfo{}
			}
			if err := s.Channels.UpsertBotChannel(ctx, domain.BotChannel{
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
	cars, err := s.Cars.ByStatuses(ctx, domain.CarNoBotAccess, domain.CarTooLarge)
	if err != nil {
		return 0, 0, err
	}
	if len(cars) == 0 {
		return 0, 0, nil
	}

	bots, err := s.Bots.List(ctx)
	if err != nil {
		return 0, 0, err
	}
	channels, err := s.Channels.List(ctx)
	if err != nil {
		return 0, 0, err
	}
	chByID := make(map[int64]domain.Channel, len(channels))
	for _, ch := range channels {
		chByID[ch.ID] = ch
	}

	for _, car := range cars {
		ch, ok := chByID[car.ChannelID]
		if !ok {
			continue
		}
		check, err := s.probeCarMessage(ctx, car, ch, bots)
		if err != nil {
			return restored, deleted, err
		}
		switch check {
		case checkOK:
			if err := s.Cars.SetStatus(ctx, car.ID, domain.CarPublished); err != nil {
				return restored, deleted, err
			}
			restored++
		case checkDeleted:
			s.Logger.Warn().Int64("car_id", car.ID).
				Msg("car message physically deleted — removing records")
			if err := s.Cars.Delete(ctx, car.ID); err != nil && !errors.Is(err, domain.ErrNotFound) {
				return restored, deleted, err
			}
			deleted++
		case checkTooLarge:
			if car.Status != domain.CarTooLarge {
				if err := s.Cars.SetStatus(ctx, car.ID, domain.CarTooLarge); err != nil {
					return restored, deleted, err
				}
			}
		case checkNoAccess:
			if car.Status != domain.CarNoBotAccess {
				if err := s.Cars.SetStatus(ctx, car.ID, domain.CarNoBotAccess); err != nil {
					return restored, deleted, err
				}
			}
		}
	}
	return restored, deleted, nil
}
