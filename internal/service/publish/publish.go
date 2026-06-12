// Package publish implements the content-publishing workflow (`ipfsgram add`):
// loading a DAG from a BlockSource, deduplicating against already-stored blocks
// (re-probing their Telegram messages), packing the remaining blocks into
// size-capped CAR archives, uploading each to a channel via a bot, and finally
// recording the pin. It depends only on the port interfaces and the domain
// layer; all diagnostics go to a zerolog.Logger and user-facing text is left to
// the caller via the returned root CID.
package publish

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/rs/zerolog"

	"github.com/ulbwa/ipfsgram/internal/domain"
	"github.com/ulbwa/ipfsgram/internal/port"
)

const (
	defaultCarMaxSize    = 15 << 20
	defaultWarnThreshold = 0.9
)

// Service publishes content. Its dependencies are all ports.
type Service struct {
	Config    port.ConfigRepository
	Blocks    port.BlockRepository
	Cars      port.CarRepository
	Channels  port.ChannelRepository
	Bots      port.BotRepository
	Pins      port.PinRepository
	Transport port.Transport
	Selector  port.Selector
	Packer    port.PackerFactory
	Locker    port.Locker
	Logger    zerolog.Logger
}

// Publish loads the DAG from src, deduplicates and (re-)uploads its blocks, and
// records a pin under name. It returns the DAG root. The dedup-through-pin
// critical section runs under a shared advisory lock so it is mutually excluded
// with garbage collection but stays concurrent with other publishers.
//
// Memory note: the BlockSource loads the whole DAG into memory (~ content
// size); that is the BlockSource's concern, documented on the port.
func (s *Service) Publish(ctx context.Context, src port.BlockSource, name string) (cid.Cid, error) {
	carMaxSize, err := s.configInt64(ctx, "car_max_size", defaultCarMaxSize)
	if err != nil {
		return cid.Undef, err
	}
	warnThreshold, err := s.configFloat64(ctx, "channel_warn_threshold", defaultWarnThreshold)
	if err != nil {
		return cid.Undef, err
	}

	root, blocks, err := src.Load(ctx)
	if err != nil {
		return cid.Undef, err
	}

	var totalSize int64
	allCIDs := make([][]byte, len(blocks))
	for i, b := range blocks {
		allCIDs[i] = b.CID.Bytes()
		totalSize += int64(len(b.Data))
	}

	err = s.Locker.WithLock(ctx, false, func(ctx context.Context) error {
		existing, err := s.Blocks.Existing(ctx, allCIDs)
		if err != nil {
			return err
		}
		statuses, checks, err := s.checkExistingCars(ctx, existing)
		if err != nil {
			return err
		}
		plan := planDedup(existing, statuses, checks)

		for _, carID := range plan.DeleteCars {
			s.Logger.Warn().Int64("car_id", carID).
				Msg("car message physically deleted — removing records, blocks will be re-uploaded")
			if err := s.Cars.Delete(ctx, carID); err != nil && !errors.Is(err, domain.ErrNotFound) {
				return err
			}
		}
		for carID, st := range plan.SetStatus {
			s.Logger.Warn().Int64("car_id", carID).Str("status", string(st)).
				Msg("car unavailable — blocks will be re-uploaded")
			if err := s.Cars.SetStatus(ctx, carID, st); err != nil && !errors.Is(err, domain.ErrNotFound) {
				return err
			}
		}

		var toUpload []domain.RawBlock
		for _, b := range blocks {
			if !plan.Skip[string(b.CID.Bytes())] {
				toUpload = append(toUpload, b)
			}
		}

		if len(toUpload) > 0 {
			if err := s.packAndUpload(ctx, root, toUpload, existing, carMaxSize, warnThreshold); err != nil {
				return err
			}
		}

		return s.Pins.Create(ctx, root.Bytes(), name, totalSize, allCIDs)
	})
	if err != nil {
		return cid.Undef, err
	}
	return root, nil
}

func (s *Service) configInt64(ctx context.Context, key string, def int64) (int64, error) {
	v, err := s.Config.GetInt64(ctx, key)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return def, nil
		}
		return 0, err
	}
	return v, nil
}

func (s *Service) configFloat64(ctx context.Context, key string, def float64) (float64, error) {
	v, err := s.Config.GetFloat64(ctx, key)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return def, nil
		}
		return 0, err
	}
	return v, nil
}

// checkExistingCars loads the cars referenced by the existing blocks and, for
// published ones, probes their Telegram messages.
func (s *Service) checkExistingCars(
	ctx context.Context, existing map[string]domain.Block,
) (map[int64]domain.CarStatus, map[int64]carCheck, error) {
	carIDs := make(map[int64]bool)
	for _, ref := range existing {
		carIDs[ref.CarID] = true
	}
	statuses := make(map[int64]domain.CarStatus, len(carIDs))
	checks := make(map[int64]carCheck, len(carIDs))
	if len(carIDs) == 0 {
		return statuses, checks, nil
	}

	bots, err := s.Bots.List(ctx)
	if err != nil {
		return nil, nil, err
	}
	channels, err := s.Channels.List(ctx)
	if err != nil {
		return nil, nil, err
	}
	chByID := make(map[int64]domain.Channel, len(channels))
	for _, ch := range channels {
		chByID[ch.ID] = ch
	}

	for carID := range carIDs {
		car, err := s.Cars.Get(ctx, carID)
		if errors.Is(err, domain.ErrNotFound) {
			continue // concurrently removed; its blocks cascaded away too
		}
		if err != nil {
			return nil, nil, err
		}
		statuses[carID] = car.Status
		if car.Status != domain.CarPublished {
			continue
		}
		check, err := s.probeCarMessage(ctx, car, chByID[car.ChannelID], bots)
		if err != nil {
			return nil, nil, err
		}
		checks[carID] = check
	}
	return statuses, checks, nil
}

// probeCarMessage checks whether the car's Telegram message is alive, trying
// bots until one succeeds or none remain. Flood-waited bots are marked in the
// database and skipped; when every eligible bot is merely flood-waited, the
// probe waits for the earliest expiry and retries instead of misclassifying a
// healthy car as no_bot_access. The returned error is non-nil only when the
// wait was interrupted (context cancelled).
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

// packAndUpload packs the blocks into rotating CAR files and publishes each of
// them. Partial failure leaves pending car rows for `ipfsgram doctor`.
func (s *Service) packAndUpload(
	ctx context.Context, root cid.Cid, blocks []domain.RawBlock,
	existing map[string]domain.Block, carMaxSize int64, warnThreshold float64,
) error {
	tmpDir, err := os.MkdirTemp("", "ipfsgram-add-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	packer, err := s.Packer.New(tmpDir, carMaxSize, root)
	if err != nil {
		return err
	}
	for _, b := range blocks {
		if err := packer.Add(b.CID, b.Data); err != nil {
			return err
		}
	}
	packed, err := packer.Finish()
	if err != nil {
		return err
	}

	shortRoot := root.String()
	if len(shortRoot) > 16 {
		shortRoot = shortRoot[len(shortRoot)-16:]
	}

	for i, pc := range packed {
		name := fmt.Sprintf("%s-%06d.car", shortRoot, i+1)
		if err := s.uploadCar(ctx, pc, name, existing, warnThreshold); err != nil {
			return err
		}
	}
	return nil
}

// uploadCar publishes one packed CAR: picks a channel and bot, creates the
// pending car row, uploads, marks published and records the block rows.
func (s *Service) uploadCar(
	ctx context.Context, pc domain.PackedCar, name string,
	existing map[string]domain.Block, warnThreshold float64,
) error {
	channels, err := s.Channels.List(ctx)
	if err != nil {
		return err
	}
	ch, warn, err := s.Selector.PickChannel(channels, warnThreshold)
	if errors.Is(err, port.ErrNoChannelSpace) {
		return errors.New("no channel has free space: add a channel (ipfsgram channel add)")
	}
	if err != nil {
		return err
	}
	if warn {
		s.Logger.Warn().Str("channel", ch.Title).
			Int64("count", ch.MessageCount).Int64("limit", ch.MessageLimit).
			Msg("channel almost full")
	}
	members, err := s.Channels.MembersOf(ctx, ch.ID)
	if err != nil {
		return err
	}

	carID, err := s.Cars.CreatePending(ctx, ch.ID, pc.Size, len(pc.Blocks))
	if err != nil {
		return err
	}

	for {
		bots, err := s.Bots.List(ctx)
		if err != nil {
			return err
		}
		bot, err := s.Selector.PickUploadBot(time.Now(), bots, members)
		if errors.Is(err, port.ErrNoBotAvailable) {
			earliest := earliestRecovery(bots, members, func(m domain.BotChannel) bool {
				return m.Member && m.CanPost
			})
			if earliest == nil {
				return fmt.Errorf("no bot available for channel %q", ch.Title)
			}
			if werr := s.waitForFloodWait(ctx, *earliest); werr != nil {
				return werr
			}
			continue
		}
		if err != nil {
			return err
		}

		f, err := os.Open(pc.Path)
		if err != nil {
			return err
		}
		res, uerr := s.Transport.Upload(ctx, bot.Token, ch.TgID, name, pc.Size, f)
		f.Close()

		var fw *domain.FloodWaitError
		if errors.As(uerr, &fw) {
			s.Selector.RecordError(bot.ID)
			until := time.Now().Add(fw.RetryAfter)
			s.Logger.Warn().Str("bot", bot.Username).Dur("retry_after", fw.RetryAfter).
				Msg("flood-wait on upload, trying another bot")
			if err := s.Bots.SetUnavailableUntil(ctx, bot.ID, until); err != nil {
				s.Logger.Warn().Err(err).Msg("could not record flood-wait")
			}
			continue
		}
		if uerr != nil {
			s.Selector.RecordError(bot.ID)
			// Pending car row stays; `ipfsgram doctor` cleans it up.
			return fmt.Errorf("upload %s: %w", name, uerr)
		}
		s.Selector.Record(bot.ID)

		if err := s.Cars.MarkPublished(ctx, carID, res.MessageID); err != nil {
			return err
		}
		if res.FileID != "" {
			if err := s.Cars.UpsertFileID(ctx, carID, bot.ID, res.FileID); err != nil {
				return err
			}
		}
		if err := s.Channels.IncrementMessageCount(ctx, ch.ID); err != nil {
			return err
		}

		var newRefs, repointRefs []domain.Block
		for _, b := range pc.Blocks {
			ref := domain.Block{
				CID: b.CID.Bytes(), CarID: carID, Offset: b.Offset, Length: b.Length,
			}
			if _, ok := existing[string(ref.CID)]; ok {
				repointRefs = append(repointRefs, ref)
			} else {
				newRefs = append(newRefs, ref)
			}
		}
		if err := s.Blocks.InsertBatch(ctx, newRefs); err != nil {
			return err
		}
		if err := s.Blocks.Repoint(ctx, repointRefs); err != nil {
			return err
		}
		return nil
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
// whose channel membership satisfies eligible, or nil when no such bot exists
// (i.e. waiting would never help).
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

// markUnavailable records the flood-wait expiry on the in-memory candidate list
// so the selector skips the bot until it recovers.
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
