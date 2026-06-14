// publish.go — Publisher and the content-publishing pipeline: dedup against
// already-stored blocks, pack into size-capped CARs, upload each via a bot,
// record the pin.

// Package publish implements the content-publishing workflow (`ipfsgram add`):
// loading a DAG (block.FromFile / block.FromNetwork), deduplicating against already-stored
// blocks (re-probing their Telegram messages), packing the remaining blocks
// into size-capped CAR archives, uploading each to a channel via a bot, and
// finally recording the pin. Its dependencies are consumer-side interfaces
// declared in this package (satisfied by *store.Store and *telegram.Client);
// the bot/channel selection strategies are the pure functions of
// internal/selector. All diagnostics go to a zerolog.Logger; user-facing text
// is left to the caller via the returned root CID.
package publish

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/rs/zerolog"

	"github.com/ulbwa/ipfsgram/internal/block"
	"github.com/ulbwa/ipfsgram/internal/car"
	"github.com/ulbwa/ipfsgram/internal/selector"
	"github.com/ulbwa/ipfsgram/internal/store"
	"github.com/ulbwa/ipfsgram/internal/telegram"
)

const (
	defaultCarMaxSize    = 15 << 20
	defaultWarnThreshold = 0.9
)

// transport is the subset of *telegram.Client the publish pipeline uses.
type transport interface {
	Upload(ctx context.Context, token string, channelTgID int64, name string, size int64, r io.Reader) (telegram.UploadResult, error)
	CheckMessage(ctx context.Context, token string, channelTgID, messageID int64) error
}

// database is the subset of *store.Store the publish pipeline uses.
type database interface {
	ConfigInt64(ctx context.Context, key string) (int64, error)
	ConfigFloat64(ctx context.Context, key string) (float64, error)

	ExistingBlocks(ctx context.Context, cids [][]byte) (map[string]store.BlockRef, error)
	UpsertBlocks(ctx context.Context, blocks []store.BlockRef) error

	Car(ctx context.Context, carID int64) (store.Car, error)
	DeleteCar(ctx context.Context, carID int64) error
	SetCarStatus(ctx context.Context, carID int64, st store.CarStatus) error
	CreatePendingCar(ctx context.Context, channelID, size int64, blockCount int) (int64, error)
	MarkCarPublished(ctx context.Context, carID, messageID int64) error
	UpsertCarFileID(ctx context.Context, carID, botID int64, fileID string) error
	CarFileIDs(ctx context.Context, carID int64) (map[int64]string, error)

	Channels(ctx context.Context) ([]store.Channel, error)
	ChannelMembers(ctx context.Context, channelID int64) ([]store.BotChannel, error)
	IncrementMessageCount(ctx context.Context, id int64) error

	Bots(ctx context.Context) ([]store.Bot, error)
	SetBotUnavailable(ctx context.Context, id int64, until time.Time) error

	CreatePin(ctx context.Context, root []byte, name string, size int64, blockCIDs [][]byte) error
	NotifyNewContent(ctx context.Context, rootCID []byte) error

	WithSharedPublishLock(ctx context.Context, fn func(ctx context.Context) error) error
}

// carPacker is what the pipeline needs from car.RotatingPacker; the indirection
// exists only so tests can substitute a fake via the newPacker hook.
type carPacker interface {
	Add(c cid.Cid, data []byte) error
	Finish() ([]car.PackedCar, error)
}

// Publisher publishes content to Telegram channels.
type Publisher struct {
	db    database
	tr    transport
	loads *selector.LoadCounter
	log   zerolog.Logger

	// newPacker constructs the CAR packer; defaults to car.NewRotatingPacker.
	// Overridable in tests.
	newPacker func(dir string, maxSize int64, root cid.Cid) (carPacker, error)
}

// New returns a Publisher backed by the given store and Telegram transport.
// *store.Store satisfies db; *telegram.Client satisfies tr.
func New(db database, tr transport, log zerolog.Logger) *Publisher {
	return &Publisher{
		db:    db,
		tr:    tr,
		loads: selector.NewLoadCounter(),
		log:   log,
		newPacker: func(dir string, maxSize int64, root cid.Cid) (carPacker, error) {
			return car.NewRotatingPacker(dir, maxSize, root)
		},
	}
}

// Publish deduplicates and (re-)uploads the DAG's blocks and records a pin
// under name. It returns the DAG root. The dedup-through-pin critical section
// runs under the shared advisory lock so it is mutually excluded with garbage
// collection but stays concurrent with other publishers.
//
// Memory note: the caller (block.FromFile / block.FromNetwork) holds the whole DAG in memory
// (~ content size); documented on those functions.
func (p *Publisher) Publish(ctx context.Context, root cid.Cid, name string, blocks []block.Block) (cid.Cid, error) {
	carMaxSize, err := p.configInt64(ctx, "car_max_size", defaultCarMaxSize)
	if err != nil {
		return cid.Undef, err
	}
	warnThreshold, err := p.configFloat64(ctx, "channel_warn_threshold", defaultWarnThreshold)
	if err != nil {
		return cid.Undef, err
	}

	var totalSize int64
	allCIDs := make([][]byte, len(blocks))
	for i, b := range blocks {
		allCIDs[i] = b.CID.Bytes()
		totalSize += int64(len(b.Data))
	}

	err = p.db.WithSharedPublishLock(ctx, func(ctx context.Context) error {
		existing, err := p.db.ExistingBlocks(ctx, allCIDs)
		if err != nil {
			return err
		}
		statuses, checks, err := p.checkExistingCars(ctx, existing)
		if err != nil {
			return err
		}
		plan := planDedup(existing, statuses, checks)

		for _, carID := range plan.DeleteCars {
			p.log.Warn().Int64("car_id", carID).
				Msg("car message physically deleted — removing records, blocks will be re-uploaded")
			if err := p.db.DeleteCar(ctx, carID); err != nil && !errors.Is(err, store.ErrNotFound) {
				return err
			}
		}
		for carID, st := range plan.SetStatus {
			p.log.Warn().Int64("car_id", carID).Str("status", string(st)).
				Msg("car unavailable — blocks will be re-uploaded")
			if err := p.db.SetCarStatus(ctx, carID, st); err != nil && !errors.Is(err, store.ErrNotFound) {
				return err
			}
		}

		var toUpload []block.Block
		for _, b := range blocks {
			if !plan.Skip[string(b.CID.Bytes())] {
				toUpload = append(toUpload, b)
			}
		}

		if len(toUpload) > 0 {
			if err := p.packAndUpload(ctx, root, toUpload, carMaxSize, warnThreshold); err != nil {
				return err
			}
		}

		return p.db.CreatePin(ctx, root.Bytes(), name, totalSize, allCIDs)
	})
	if err != nil {
		return cid.Undef, err
	}

	// Tell running daemons to announce this root to the DHT now, rather than
	// waiting for their next reprovide. Best-effort: the content is already
	// committed, and the periodic reprovide is the backstop.
	if err := p.db.NotifyNewContent(ctx, root.Bytes()); err != nil {
		p.log.Warn().Err(err).Stringer("cid", root).Msg("notify daemons of new content")
	}
	return root, nil
}

func (p *Publisher) configInt64(ctx context.Context, key string, def int64) (int64, error) {
	v, err := p.db.ConfigInt64(ctx, key)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return def, nil
		}
		return 0, err
	}
	return v, nil
}

func (p *Publisher) configFloat64(ctx context.Context, key string, def float64) (float64, error) {
	v, err := p.db.ConfigFloat64(ctx, key)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return def, nil
		}
		return 0, err
	}
	return v, nil
}

// checkExistingCars loads the cars referenced by the existing blocks and, for
// published ones, probes their Telegram messages.
func (p *Publisher) checkExistingCars(
	ctx context.Context, existing map[string]store.BlockRef,
) (map[int64]store.CarStatus, map[int64]carCheck, error) {
	carIDs := make(map[int64]bool)
	for _, ref := range existing {
		carIDs[ref.CarID] = true
	}
	statuses := make(map[int64]store.CarStatus, len(carIDs))
	checks := make(map[int64]carCheck, len(carIDs))
	if len(carIDs) == 0 {
		return statuses, checks, nil
	}

	bots, err := p.db.Bots(ctx)
	if err != nil {
		return nil, nil, err
	}
	channels, err := p.db.Channels(ctx)
	if err != nil {
		return nil, nil, err
	}
	chByID := make(map[int64]store.Channel, len(channels))
	for _, ch := range channels {
		chByID[ch.ID] = ch
	}

	for carID := range carIDs {
		c, err := p.db.Car(ctx, carID)
		if errors.Is(err, store.ErrNotFound) {
			continue // concurrently removed; its blocks cascaded away too
		}
		if err != nil {
			return nil, nil, err
		}
		statuses[carID] = c.Status
		if c.Status != store.CarPublished {
			continue
		}
		check, err := p.probeCarMessage(ctx, c, chByID[c.ChannelID], bots)
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
func (p *Publisher) probeCarMessage(
	ctx context.Context, c store.Car, ch store.Channel, bots []store.Bot,
) (carCheck, error) {
	if c.MessageID == nil {
		return checkNoAccess, nil
	}
	members, err := p.db.ChannelMembers(ctx, c.ChannelID)
	if err != nil {
		p.log.Warn().Err(err).Int64("car_id", c.ID).Msg("could not load bot membership")
		return checkNoAccess, nil
	}
	fileIDs, err := p.db.CarFileIDs(ctx, c.ID)
	if err != nil {
		p.log.Warn().Err(err).Int64("car_id", c.ID).Msg("could not load file_ids")
		fileIDs = nil
	}

	remaining := append([]store.Bot(nil), bots...)
	for {
		bot, _, err := selector.PickDownloadBot(time.Now(), remaining, members, fileIDs, p.loads)
		if err != nil {
			earliest := earliestRecovery(remaining, members, func(m store.BotChannel) bool {
				return m.Member && m.CanRead
			})
			if earliest == nil {
				p.log.Warn().Int64("car_id", c.ID).
					Msg("no bot can check car message — treating as no_bot_access")
				return checkNoAccess, nil
			}
			if werr := p.waitForFloodWait(ctx, *earliest); werr != nil {
				return checkNoAccess, werr
			}
			continue
		}

		check, decided := p.checkWithBot(ctx, c, ch, bot, &remaining)
		if decided {
			return check, nil
		}
	}
}

// checkWithBot probes the car's message with one bot. It returns (check, true)
// when the bot produced a conclusive result, or (_, false) to retry with
// another bot — having marked the bot flood-waited or dropped it from remaining.
func (p *Publisher) checkWithBot(
	ctx context.Context, c store.Car, ch store.Channel, bot store.Bot, remaining *[]store.Bot,
) (carCheck, bool) {
	cerr := p.tr.CheckMessage(ctx, bot.Token, ch.TgID, *c.MessageID)
	var fw *telegram.FloodWaitError
	switch {
	case cerr == nil:
		p.loads.Record(bot.ID)
		return checkOK, true
	case errors.Is(cerr, telegram.ErrMessageDeleted):
		return checkDeleted, true
	case errors.Is(cerr, telegram.ErrTooLarge):
		return checkTooLarge, true
	case errors.As(cerr, &fw):
		p.loads.RecordError(bot.ID)
		until := time.Now().Add(fw.RetryAfter)
		if err := p.db.SetBotUnavailable(ctx, bot.ID, until); err != nil {
			p.log.Warn().Err(err).Msg("could not record flood-wait")
		}
		markUnavailable(*remaining, bot.ID, until)
	case errors.Is(cerr, telegram.ErrNoAccess):
		p.loads.RecordError(bot.ID)
		*remaining = removeBot(*remaining, bot.ID)
	default:
		p.loads.RecordError(bot.ID)
		p.log.Warn().Err(cerr).Int64("car_id", c.ID).Str("bot", bot.Username).
			Msg("error checking message, trying another bot")
		*remaining = removeBot(*remaining, bot.ID)
	}
	return 0, false
}

// packAndUpload packs the blocks into rotating CAR files and publishes each of
// them. Partial failure leaves pending car rows for `ipfsgram doctor`.
func (p *Publisher) packAndUpload(
	ctx context.Context, root cid.Cid, blocks []block.Block,
	carMaxSize int64, warnThreshold float64,
) error {
	tmpDir, err := os.MkdirTemp("", "ipfsgram-add-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	packer, err := p.newPacker(tmpDir, carMaxSize, root)
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
		if err := p.uploadCar(ctx, pc, name, warnThreshold); err != nil {
			return err
		}
	}
	return nil
}

// uploadCar publishes one packed CAR: picks a channel and bot, creates the
// pending car row, uploads, marks published and records the block rows.
func (p *Publisher) uploadCar(
	ctx context.Context, pc car.PackedCar, name string, warnThreshold float64,
) error {
	ch, err := p.pickPublishChannel(ctx, warnThreshold)
	if err != nil {
		return err
	}
	members, err := p.db.ChannelMembers(ctx, ch.ID)
	if err != nil {
		return err
	}
	carID, err := p.db.CreatePendingCar(ctx, ch.ID, pc.Size, len(pc.Blocks))
	if err != nil {
		return err
	}

	for {
		bot, err := p.pickUploadBot(ctx, ch, members)
		if err != nil {
			return err
		}
		if bot == nil {
			continue // all bots flood-waited; we waited, now retry
		}

		f, err := os.Open(pc.Path)
		if err != nil {
			return err
		}
		res, uerr := p.tr.Upload(ctx, bot.Token, ch.TgID, name, pc.Size, f)
		f.Close()

		var fw *telegram.FloodWaitError
		switch {
		case errors.As(uerr, &fw):
			p.loads.RecordError(bot.ID)
			until := time.Now().Add(fw.RetryAfter)
			p.log.Warn().Str("bot", bot.Username).Dur("retry_after", fw.RetryAfter).
				Msg("flood-wait on upload, trying another bot")
			if err := p.db.SetBotUnavailable(ctx, bot.ID, until); err != nil {
				p.log.Warn().Err(err).Msg("could not record flood-wait")
			}
			continue
		case uerr != nil:
			p.loads.RecordError(bot.ID)
			// Pending car row stays; `ipfsgram doctor` cleans it up.
			return fmt.Errorf("upload %s: %w", name, uerr)
		}
		p.loads.Record(bot.ID)
		return p.persistUpload(ctx, pc, ch, carID, *bot, res)
	}
}

// pickPublishChannel selects the fill-first channel with free space, logging a
// warning when it is near its limit. ErrNoChannelSpace is wrapped with the
// "add a channel" guidance for the caller.
func (p *Publisher) pickPublishChannel(ctx context.Context, warnThreshold float64) (store.Channel, error) {
	channels, err := p.db.Channels(ctx)
	if err != nil {
		return store.Channel{}, err
	}
	ch, warn, err := selector.PickChannel(channels, warnThreshold)
	if errors.Is(err, selector.ErrNoChannelSpace) {
		return store.Channel{}, fmt.Errorf("%w: add a channel (ipfsgram channel add)", selector.ErrNoChannelSpace)
	}
	if err != nil {
		return store.Channel{}, err
	}
	if warn {
		p.log.Warn().Str("channel", ch.Title).
			Int64("count", ch.MessageCount).Int64("limit", ch.MessageLimit).
			Msg("channel almost full")
	}
	return ch, nil
}

// pickUploadBot selects the least-loaded healthy member bot for the channel.
// When every eligible bot is merely flood-waited it waits for the earliest
// expiry and returns (nil, nil) so the caller retries; it returns an error only
// when no bot can ever serve the channel or the wait was interrupted.
func (p *Publisher) pickUploadBot(ctx context.Context, ch store.Channel, members []store.BotChannel) (*store.Bot, error) {
	bots, err := p.db.Bots(ctx)
	if err != nil {
		return nil, err
	}
	bot, err := selector.PickUploadBot(time.Now(), bots, members, p.loads)
	if errors.Is(err, selector.ErrNoBotAvailable) {
		earliest := earliestRecovery(bots, members, func(m store.BotChannel) bool {
			return m.Member && m.CanPost
		})
		if earliest == nil {
			return nil, fmt.Errorf("no bot available for channel %q", ch.Title)
		}
		if werr := p.waitForFloodWait(ctx, *earliest); werr != nil {
			return nil, werr
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &bot, nil
}

// persistUpload records a successful upload: marks the car published, stores the
// fresh file_id, increments the channel message count and upserts the block
// rows.
func (p *Publisher) persistUpload(
	ctx context.Context, pc car.PackedCar, ch store.Channel, carID int64,
	bot store.Bot, res telegram.UploadResult,
) error {
	if err := p.db.MarkCarPublished(ctx, carID, res.MessageID); err != nil {
		return err
	}
	if res.FileID != "" {
		if err := p.db.UpsertCarFileID(ctx, carID, bot.ID, res.FileID); err != nil {
			return err
		}
	}
	if err := p.db.IncrementMessageCount(ctx, ch.ID); err != nil {
		return err
	}

	// A single upsert covers every case: new blocks, blocks repointed from a
	// still-existing car, and blocks whose previous car row was deleted
	// (cascading away their block rows) after the dedup snapshot was taken.
	refs := make([]store.BlockRef, 0, len(pc.Blocks))
	for _, b := range pc.Blocks {
		refs = append(refs, store.BlockRef{
			CID: b.CID.Bytes(), CarID: carID, Offset: b.Offset, Length: b.Length,
		})
	}
	return p.db.UpsertBlocks(ctx, refs)
}

// waitForFloodWait sleeps until the given time, logging progress. It returns the
// context error when cancelled mid-wait.
func (p *Publisher) waitForFloodWait(ctx context.Context, until time.Time) error {
	for {
		remaining := time.Until(until)
		if remaining <= 0 {
			return nil
		}
		p.log.Info().Int("seconds", int(remaining.Seconds())+1).
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

// markUnavailable records the flood-wait expiry on the in-memory candidate list
// so the selector skips the bot until it recovers.
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
