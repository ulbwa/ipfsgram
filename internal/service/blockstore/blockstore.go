// Package blockstore implements a read-only boxo blockstore.Blockstore backed
// entirely by the IPFSgram ports. Lookups go through the BlockRepository;
// payloads are served from the local disk Cache, downloading the owning CAR
// from the Telegram Transport on miss. It depends only on port interfaces and
// domain value types — never on concrete repositories, the Telegram client, the
// disk cache, or the CAR packer.
package blockstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"time"

	boxoblockstore "github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/rs/zerolog"
	"golang.org/x/sync/singleflight"

	"github.com/ulbwa/ipfsgram/internal/domain"
	"github.com/ulbwa/ipfsgram/internal/port"
)

// ErrReadOnly is returned by every mutating blockstore method: the service
// serves blocks already stored in Telegram and never writes through Bitswap.
var ErrReadOnly = errors.New("blockstore: read-only")

// errCarUnavailable marks a car whose data exists but cannot be fetched right
// now (no bot access, transport size limit, deleted message). It is mapped to
// ipld.ErrNotFound at the Blockstore boundary so Bitswap simply does not serve
// the block.
var errCarUnavailable = errors.New("blockstore: car unavailable")

// maxDownloadAttempts bounds the bot-retry loop for one car download.
const maxDownloadAttempts = 8

// Compile-time assertion that Blockstore satisfies the boxo contract.
var _ boxoblockstore.Blockstore = (*Blockstore)(nil)

// Deps are the dependencies of the blockstore service, injected as port
// interfaces. Now is optional and defaults to time.Now.
type Deps struct {
	Blocks    port.BlockRepository
	Cars      port.CarRepository
	Bots      port.BotRepository
	Channels  port.ChannelRepository
	Transport port.Transport
	Cache     port.Cache
	Reader    port.BlockReader
	Selector  port.Selector

	Logger      zerolog.Logger
	CacheTmpDir string
	Now         func() time.Time
}

// Blockstore is a read-only boxo blockstore backed by CAR archives stored as
// Telegram documents. Lookups go through the BlockRepository; payloads are
// served from the local disk Cache, downloading the owning CAR from Telegram on
// miss.
type Blockstore struct {
	deps Deps

	group singleflight.Group
	now   func() time.Time
	log   zerolog.Logger
}

// New assembles the read-only blockstore from its port dependencies.
// CacheTmpDir hosts in-flight downloads and must live on the same filesystem as
// the cache directory for cheap renames (Cache.Put falls back to copying
// otherwise).
func New(deps Deps) *Blockstore {
	now := deps.Now
	if now == nil {
		now = time.Now
	}
	return &Blockstore{
		deps: deps,
		now:  now,
		log:  deps.Logger,
	}
}

// Has reports whether the block is indexed in the shared database.
func (bs *Blockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	_, err := bs.deps.Blocks.Lookup(ctx, c.Bytes())
	if errors.Is(err, domain.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// GetSize returns the block payload length from the index without touching
// Telegram.
func (bs *Blockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	b, err := bs.deps.Blocks.Lookup(ctx, c.Bytes())
	if errors.Is(err, domain.ErrNotFound) {
		return -1, ipld.ErrNotFound{Cid: c}
	}
	if err != nil {
		return -1, err
	}
	return int(b.Length), nil
}

// Get returns the block payload, fetching the owning CAR from Telegram into the
// local cache when needed.
func (bs *Blockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	b, err := bs.deps.Blocks.Lookup(ctx, c.Bytes())
	if errors.Is(err, domain.ErrNotFound) {
		return nil, ipld.ErrNotFound{Cid: c}
	}
	if err != nil {
		return nil, err
	}

	path, err := bs.carPath(ctx, b.CarID, false)
	if errors.Is(err, errCarUnavailable) || errors.Is(err, domain.ErrNotFound) {
		return nil, ipld.ErrNotFound{Cid: c}
	}
	if err != nil {
		return nil, err
	}

	data, err := bs.deps.Reader.ReadAt(path, b.Offset, b.Length)
	if errors.Is(err, fs.ErrNotExist) {
		// Raced with cache eviction: the path resolved above was deleted before
		// we could open it. Retry once, forcing a fresh download (bypassing the
		// stale cache entry).
		bs.log.Debug().Int64("car_id", b.CarID).Msg("cached car evicted mid-read, re-downloading")
		path, err = bs.carPath(ctx, b.CarID, true)
		if errors.Is(err, errCarUnavailable) || errors.Is(err, domain.ErrNotFound) {
			return nil, ipld.ErrNotFound{Cid: c}
		}
		if err != nil {
			return nil, err
		}
		data, err = bs.deps.Reader.ReadAt(path, b.Offset, b.Length)
	}
	if err != nil {
		return nil, fmt.Errorf("blockstore: read block %s from car %d: %w", c, b.CarID, err)
	}
	return blocks.NewBlockWithCid(data, c)
}

// carPath returns the local path of the cached CAR, downloading it from
// Telegram on cache miss. Concurrent requests for the same car share one
// download via singleflight. With force set, the cache lookup is bypassed and
// the car is re-downloaded (used to recover from an eviction race).
//
// The download itself runs detached from the caller's context: once a CAR
// transfer starts, cancelling one waiter must not fail the others (or waste the
// transfer). Each caller still honors its own context while waiting.
func (bs *Blockstore) carPath(ctx context.Context, carID int64, force bool) (string, error) {
	if !force {
		if path, ok := bs.deps.Cache.Get(carID); ok {
			return path, nil
		}
	}
	dctx := context.WithoutCancel(ctx)
	ch := bs.group.DoChan(fmt.Sprintf("%d", carID), func() (any, error) {
		if !force {
			// Re-check: another flight may have populated the cache while this
			// call was queued behind it.
			if path, ok := bs.deps.Cache.Get(carID); ok {
				return path, nil
			}
		}
		return bs.download(dctx, carID)
	})
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return "", res.Err
		}
		return res.Val.(string), nil
	}
}

// download fetches the CAR from Telegram, stores it in the cache and returns the
// cached path. It owns the full classified-error policy.
func (bs *Blockstore) download(ctx context.Context, carID int64) (string, error) {
	car, err := bs.deps.Cars.Get(ctx, carID)
	if err != nil {
		return "", err // includes domain.ErrNotFound: car raced with deletion
	}
	if car.MessageID == nil {
		bs.log.Warn().Int64("car_id", carID).Msg("car has no message yet (pending upload), block unavailable")
		return "", errCarUnavailable
	}
	channel, err := bs.deps.Channels.GetByID(ctx, car.ChannelID)
	if err != nil {
		return "", err
	}
	bots, err := bs.deps.Bots.List(ctx)
	if err != nil {
		return "", err
	}
	members, err := bs.deps.Channels.MembersOf(ctx, car.ChannelID)
	if err != nil {
		return "", err
	}
	fileIDs, err := bs.deps.Cars.FileIDs(ctx, carID)
	if err != nil {
		return "", err
	}

	var lastErr error
	for attempt := 0; attempt < maxDownloadAttempts; attempt++ {
		bot, fileID, err := bs.deps.Selector.PickDownloadBot(bs.now(), bots, members, fileIDs)
		if err != nil {
			if lastErr != nil {
				return "", fmt.Errorf("blockstore: download car %d: %w (last bot error: %w)", carID, err, lastErr)
			}
			return "", fmt.Errorf("blockstore: download car %d: %w", carID, err)
		}
		bs.deps.Selector.Record(bot.ID)

		rc, freshFileID, err := bs.deps.Transport.Download(ctx, bot.Token, channel.TgID, *car.MessageID, fileID)
		if err == nil {
			path, err := bs.store(ctx, car, bot, freshFileID, rc)
			if err != nil {
				return "", err
			}
			return path, nil
		}

		var flood *domain.FloodWaitError
		switch {
		case errors.Is(err, domain.ErrMessageDeleted):
			bs.log.Warn().
				Int64("car_id", carID).
				Int64("channel_tg_id", channel.TgID).
				Int64("message_id", *car.MessageID).
				Msg("telegram message physically deleted, dropping car from database")
			if derr := bs.deps.Cars.Delete(ctx, carID); derr != nil && !errors.Is(derr, domain.ErrNotFound) {
				return "", fmt.Errorf("blockstore: delete car %d after message deletion: %w", carID, derr)
			}
			return "", errCarUnavailable

		case errors.Is(err, domain.ErrNoAccess):
			bs.log.Warn().
				Int64("car_id", carID).
				Int64("channel_tg_id", channel.TgID).
				Int64("bot_id", bot.ID).
				Msg("no bot access to the car's channel, marking no_bot_access; data is intact and recoverable")
			if serr := bs.deps.Cars.SetStatus(ctx, carID, domain.CarNoBotAccess); serr != nil && !errors.Is(serr, domain.ErrNotFound) {
				return "", fmt.Errorf("blockstore: mark car %d no_bot_access: %w", carID, serr)
			}
			return "", errCarUnavailable

		case errors.Is(err, domain.ErrTooLarge):
			bs.log.Warn().
				Int64("car_id", carID).
				Int64("size", car.Size).
				Msg("car exceeds the transport download limit, marking too_large; enable MTProto to lift the Bot API 20 MB limit")
			if serr := bs.deps.Cars.SetStatus(ctx, carID, domain.CarTooLarge); serr != nil && !errors.Is(serr, domain.ErrNotFound) {
				return "", fmt.Errorf("blockstore: mark car %d too_large: %w", carID, serr)
			}
			return "", errCarUnavailable

		case errors.As(err, &flood):
			until := bs.now().Add(flood.RetryAfter)
			bs.log.Warn().
				Int64("bot_id", bot.ID).
				Dur("retry_after", flood.RetryAfter).
				Msg("bot flood-waited, trying the next eligible bot")
			bs.deps.Selector.RecordError(bot.ID)
			if serr := bs.deps.Bots.SetUnavailableUntil(ctx, bot.ID, until); serr != nil {
				bs.log.Error().Err(serr).Int64("bot_id", bot.ID).Msg("persist flood-wait")
			}
			markUnavailable(bots, bot.ID, until)
			lastErr = err

		case errors.Is(err, domain.ErrBadFileID):
			bs.log.Warn().
				Int64("car_id", carID).
				Int64("bot_id", bot.ID).
				Msg("stale file_id, dropping it and retrying")
			bs.deps.Selector.RecordError(bot.ID)
			if derr := bs.deps.Cars.DeleteFileID(ctx, carID, bot.ID); derr != nil {
				bs.log.Error().Err(derr).Int64("car_id", carID).Int64("bot_id", bot.ID).Msg("delete stale file_id")
			}
			delete(fileIDs, bot.ID)
			lastErr = err

		default:
			bs.log.Warn().Err(err).
				Int64("car_id", carID).
				Int64("bot_id", bot.ID).
				Msg("download failed, trying the next eligible bot")
			bs.deps.Selector.RecordError(bot.ID)
			bots = removeBot(bots, bot.ID)
			lastErr = err
		}
	}
	return "", fmt.Errorf("blockstore: download car %d: attempts exhausted: %w", carID, lastErr)
}

// store streams the downloaded CAR into the cache via a temp file and runs the
// post-download bookkeeping (fresh file_id, lazy status recovery).
func (bs *Blockstore) store(ctx context.Context, car domain.Car, bot domain.Bot, freshFileID string, rc io.ReadCloser) (string, error) {
	defer rc.Close()

	if err := os.MkdirAll(bs.deps.CacheTmpDir, 0o755); err != nil {
		return "", fmt.Errorf("blockstore: create tmp dir: %w", err)
	}
	tmp, err := os.CreateTemp(bs.deps.CacheTmpDir, fmt.Sprintf("car-%d-*", car.ID))
	if err != nil {
		return "", fmt.Errorf("blockstore: create temp car file: %w", err)
	}
	tmpName := tmp.Name()
	size, err := io.Copy(tmp, rc)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmpName)
		return "", fmt.Errorf("blockstore: download car %d: %w", car.ID, err)
	}

	path, err := bs.deps.Cache.Put(car.ID, tmpName, size)
	if err != nil {
		os.Remove(tmpName)
		return "", fmt.Errorf("blockstore: cache car %d: %w", car.ID, err)
	}

	if freshFileID != "" {
		if err := bs.deps.Cars.UpsertFileID(ctx, car.ID, bot.ID, freshFileID); err != nil {
			bs.log.Error().Err(err).Int64("car_id", car.ID).Int64("bot_id", bot.ID).Msg("record fresh file_id")
		}
	}
	if car.Status != domain.CarPublished {
		if err := bs.deps.Cars.SetStatus(ctx, car.ID, domain.CarPublished); err != nil && !errors.Is(err, domain.ErrNotFound) {
			bs.log.Error().Err(err).Int64("car_id", car.ID).Msg("recover car status to published")
		} else {
			bs.log.Info().
				Int64("car_id", car.ID).
				Str("previous_status", string(car.Status)).
				Msg("car downloaded successfully, status recovered to published")
		}
	}
	return path, nil
}

// Put always fails: the service never writes blocks.
func (bs *Blockstore) Put(context.Context, blocks.Block) error { return ErrReadOnly }

// PutMany always fails: the service never writes blocks.
func (bs *Blockstore) PutMany(context.Context, []blocks.Block) error { return ErrReadOnly }

// DeleteBlock always fails: the service never deletes recoverable data.
func (bs *Blockstore) DeleteBlock(context.Context, cid.Cid) error { return ErrReadOnly }

// AllKeysChan streams every CID indexed in the blocks table.
func (bs *Blockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return streamCIDs(ctx, bs.deps.Blocks)
}

// HashOnRead is a no-op: payload integrity is anchored by the CID index.
func (bs *Blockstore) HashOnRead(bool) {}

// ProvideKeys builds a CID stream for the reprovider's KeyChanFunc from the
// BlockRepository. libp2pnode.Config.ProvideKeys expects exactly this signature.
func (bs *Blockstore) ProvideKeys(ctx context.Context) (<-chan cid.Cid, error) {
	return streamCIDs(ctx, bs.deps.Blocks)
}

// ProvideKeys is the standalone form usable when only a BlockRepository is
// available (e.g. daemon wiring that has no Blockstore handle yet).
func ProvideKeys(blockRepo port.BlockRepository) func(context.Context) (<-chan cid.Cid, error) {
	return func(ctx context.Context) (<-chan cid.Cid, error) {
		return streamCIDs(ctx, blockRepo)
	}
}

// streamCIDs adapts BlockRepository.StreamAllCIDs (raw []byte CIDs + an error
// channel) into a single cid.Cid channel, parsing each entry via cid.Cast and
// respecting ctx. The channel is closed when the scan completes, fails, or ctx
// is done.
func streamCIDs(ctx context.Context, blockRepo port.BlockRepository) (<-chan cid.Cid, error) {
	raw, errCh := blockRepo.StreamAllCIDs(ctx)
	out := make(chan cid.Cid)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case b, ok := <-raw:
				if !ok {
					// Drain the terminal error (if any) so the producer is not
					// left blocked; it is logged but not propagated here, mirroring
					// the boxo KeyChanFunc contract (channel close ends the scan).
					select {
					case <-errCh:
					default:
					}
					return
				}
				c, err := cid.Cast(b)
				if err != nil {
					continue // skip invalid CID bytes, matching old behavior
				}
				select {
				case out <- c:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

// markUnavailable updates the in-memory bot list so the selector skips the
// flood-waited bot on the next iteration of the retry loop.
func markUnavailable(bots []domain.Bot, botID int64, until time.Time) {
	for i := range bots {
		if bots[i].ID == botID {
			bots[i].UnavailableUntil = &until
			return
		}
	}
}

// removeBot drops the bot from the in-memory candidate list after a
// non-classified download failure.
func removeBot(bots []domain.Bot, botID int64) []domain.Bot {
	out := bots[:0]
	for _, b := range bots {
		if b.ID != botID {
			out = append(out, b)
		}
	}
	return out
}
