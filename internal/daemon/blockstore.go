package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/singleflight"

	"github.com/ulbwa/ipfsgram/internal/cache"
	"github.com/ulbwa/ipfsgram/internal/carpack"
	"github.com/ulbwa/ipfsgram/internal/model"
	"github.com/ulbwa/ipfsgram/internal/repo"
	"github.com/ulbwa/ipfsgram/internal/selector"
	"github.com/ulbwa/ipfsgram/internal/tg"
)

// ErrReadOnly is returned by every mutating blockstore method: the daemon
// serves blocks already stored in Telegram and never writes through Bitswap.
var ErrReadOnly = errors.New("daemon: read-only blockstore")

// errCarUnavailable marks a car whose data exists but cannot be fetched right
// now (no bot access, transport size limit, deleted message). It is mapped to
// ipld.ErrNotFound at the Blockstore boundary so Bitswap simply does not
// serve the block.
var errCarUnavailable = errors.New("daemon: car unavailable")

// maxDownloadAttempts bounds the bot-retry loop for one car download.
const maxDownloadAttempts = 8

// blockIndex is the subset of repo.BlockRepo the blockstore needs.
type blockIndex interface {
	Lookup(ctx context.Context, cid []byte) (model.BlockRef, error)
}

// carStore is the subset of repo.CarRepo the blockstore needs.
type carStore interface {
	Get(ctx context.Context, carID int64) (model.Car, error)
	Delete(ctx context.Context, carID int64) error
	SetStatus(ctx context.Context, carID int64, st model.CarStatus) error
	FileIDs(ctx context.Context, carID int64) (map[int64]string, error)
	UpsertFileID(ctx context.Context, carID, botID int64, fileID string) error
	DeleteFileID(ctx context.Context, carID, botID int64) error
}

// botStore is the subset of repo.BotRepo the blockstore needs.
type botStore interface {
	List(ctx context.Context) ([]model.Bot, error)
	SetUnavailableUntil(ctx context.Context, id int64, until time.Time) error
}

// memberLister is the subset of repo.ChannelRepo the blockstore needs.
type memberLister interface {
	MembersOf(ctx context.Context, channelID int64) ([]model.BotChannel, error)
}

// channelGetter resolves a channel by its internal ID (see keys.go; the repo
// package only offers lookup by Telegram ID).
type channelGetter interface {
	ChannelByID(ctx context.Context, id int64) (model.Channel, error)
}

// keyLister streams every stored block CID (see keys.go).
type keyLister interface {
	AllCIDs(ctx context.Context) (<-chan cid.Cid, error)
}

// Blockstore is a read-only boxo blockstore backed by CAR archives stored as
// Telegram documents. Lookups go through PostgreSQL; payloads are served from
// the local disk cache, downloading the owning CAR from Telegram on miss.
type Blockstore struct {
	blocks    blockIndex
	cars      carStore
	bots      botStore
	members   memberLister
	channels  channelGetter
	keys      keyLister
	cache     cache.Cache
	tmpDir    string
	transport tg.Transport
	loads     *selector.LoadCounter

	group singleflight.Group
	now   func() time.Time
}

// NewBlockstore assembles the read-only blockstore. tmpDir hosts in-flight
// downloads and must live on the same filesystem as the cache directory for
// cheap renames (cache.Put falls back to copying otherwise).
func NewBlockstore(
	blocks blockIndex,
	cars carStore,
	bots botStore,
	members memberLister,
	channels channelGetter,
	keys keyLister,
	c cache.Cache,
	tmpDir string,
	transport tg.Transport,
) *Blockstore {
	return &Blockstore{
		blocks:    blocks,
		cars:      cars,
		bots:      bots,
		members:   members,
		channels:  channels,
		keys:      keys,
		cache:     c,
		tmpDir:    tmpDir,
		transport: transport,
		loads:     selector.NewLoadCounter(),
		now:       time.Now,
	}
}

// Has reports whether the block is indexed in the shared database.
func (bs *Blockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	_, err := bs.blocks.Lookup(ctx, c.Bytes())
	if errors.Is(err, repo.ErrNotFound) {
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
	ref, err := bs.blocks.Lookup(ctx, c.Bytes())
	if errors.Is(err, repo.ErrNotFound) {
		return -1, ipld.ErrNotFound{Cid: c}
	}
	if err != nil {
		return -1, err
	}
	return int(ref.Length), nil
}

// Get returns the block payload, fetching the owning CAR from Telegram into
// the local cache when needed.
func (bs *Blockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	ref, err := bs.blocks.Lookup(ctx, c.Bytes())
	if errors.Is(err, repo.ErrNotFound) {
		return nil, ipld.ErrNotFound{Cid: c}
	}
	if err != nil {
		return nil, err
	}

	path, err := bs.carPath(ctx, ref.CarID)
	if errors.Is(err, errCarUnavailable) || errors.Is(err, repo.ErrNotFound) {
		return nil, ipld.ErrNotFound{Cid: c}
	}
	if err != nil {
		return nil, err
	}

	data, err := carpack.ReadBlockAt(path, ref.Offset, ref.Length)
	if err != nil {
		return nil, fmt.Errorf("daemon: read block %s from car %d: %w", c, ref.CarID, err)
	}
	return blocks.NewBlockWithCid(data, c)
}

// carPath returns the local path of the cached CAR, downloading it from
// Telegram on cache miss. Concurrent requests for the same car share one
// download via singleflight.
func (bs *Blockstore) carPath(ctx context.Context, carID int64) (string, error) {
	if path, ok := bs.cache.Get(carID); ok {
		return path, nil
	}
	v, err, _ := bs.group.Do(fmt.Sprintf("%d", carID), func() (any, error) {
		// Re-check: another flight may have populated the cache while this
		// call was queued behind it.
		if path, ok := bs.cache.Get(carID); ok {
			return path, nil
		}
		return bs.download(ctx, carID)
	})
	if err != nil {
		return "", err
	}
	return v.(string), nil
}

// download fetches the CAR from Telegram, stores it in the cache and returns
// the cached path. It owns the full classified-error policy.
func (bs *Blockstore) download(ctx context.Context, carID int64) (string, error) {
	car, err := bs.cars.Get(ctx, carID)
	if err != nil {
		return "", err // includes repo.ErrNotFound: car raced with deletion
	}
	if car.MessageID == nil {
		log.Warn().Int64("car_id", carID).Msg("car has no message yet (pending upload), block unavailable")
		return "", errCarUnavailable
	}
	channel, err := bs.channels.ChannelByID(ctx, car.ChannelID)
	if err != nil {
		return "", err
	}
	bots, err := bs.bots.List(ctx)
	if err != nil {
		return "", err
	}
	members, err := bs.members.MembersOf(ctx, car.ChannelID)
	if err != nil {
		return "", err
	}
	fileIDs, err := bs.cars.FileIDs(ctx, carID)
	if err != nil {
		return "", err
	}

	var lastErr error
	for attempt := 0; attempt < maxDownloadAttempts; attempt++ {
		bot, fileID, err := selector.PickDownloadBot(bs.now(), bots, members, fileIDs, bs.loads)
		if err != nil {
			if lastErr != nil {
				return "", fmt.Errorf("daemon: download car %d: %w (last bot error: %w)", carID, err, lastErr)
			}
			return "", fmt.Errorf("daemon: download car %d: %w", carID, err)
		}
		bs.loads.Record(bot.ID)

		rc, freshFileID, err := bs.transport.Download(ctx, bot.Token, channel.TgID, *car.MessageID, fileID)
		if err == nil {
			path, err := bs.store(ctx, car, bot, freshFileID, rc)
			if err != nil {
				return "", err
			}
			return path, nil
		}

		var flood *tg.FloodWaitError
		switch {
		case errors.Is(err, tg.ErrMessageDeleted):
			log.Warn().
				Int64("car_id", carID).
				Int64("channel_tg_id", channel.TgID).
				Int64("message_id", *car.MessageID).
				Msg("telegram message physically deleted, dropping car from database")
			if derr := bs.cars.Delete(ctx, carID); derr != nil && !errors.Is(derr, repo.ErrNotFound) {
				return "", fmt.Errorf("daemon: delete car %d after message deletion: %w", carID, derr)
			}
			return "", errCarUnavailable

		case errors.Is(err, tg.ErrNoAccess):
			log.Warn().
				Int64("car_id", carID).
				Int64("channel_tg_id", channel.TgID).
				Int64("bot_id", bot.ID).
				Msg("no bot access to the car's channel, marking no_bot_access; data is intact and recoverable")
			if serr := bs.cars.SetStatus(ctx, carID, model.CarNoBotAccess); serr != nil && !errors.Is(serr, repo.ErrNotFound) {
				return "", fmt.Errorf("daemon: mark car %d no_bot_access: %w", carID, serr)
			}
			return "", errCarUnavailable

		case errors.Is(err, tg.ErrTooLarge):
			log.Warn().
				Int64("car_id", carID).
				Int64("size", car.Size).
				Msg("car exceeds the transport download limit, marking too_large; enable MTProto to lift the Bot API 20 MB limit")
			if serr := bs.cars.SetStatus(ctx, carID, model.CarTooLarge); serr != nil && !errors.Is(serr, repo.ErrNotFound) {
				return "", fmt.Errorf("daemon: mark car %d too_large: %w", carID, serr)
			}
			return "", errCarUnavailable

		case errors.As(err, &flood):
			until := bs.now().Add(flood.RetryAfter)
			log.Warn().
				Int64("bot_id", bot.ID).
				Dur("retry_after", flood.RetryAfter).
				Msg("bot flood-waited, trying the next eligible bot")
			bs.loads.RecordError(bot.ID)
			if serr := bs.bots.SetUnavailableUntil(ctx, bot.ID, until); serr != nil {
				log.Error().Err(serr).Int64("bot_id", bot.ID).Msg("persist flood-wait")
			}
			markUnavailable(bots, bot.ID, until)
			lastErr = err

		case errors.Is(err, tg.ErrBadFileID):
			log.Warn().
				Int64("car_id", carID).
				Int64("bot_id", bot.ID).
				Msg("stale file_id, dropping it and retrying")
			bs.loads.RecordError(bot.ID)
			if derr := bs.cars.DeleteFileID(ctx, carID, bot.ID); derr != nil {
				log.Error().Err(derr).Int64("car_id", carID).Int64("bot_id", bot.ID).Msg("delete stale file_id")
			}
			delete(fileIDs, bot.ID)
			lastErr = err

		default:
			log.Warn().Err(err).
				Int64("car_id", carID).
				Int64("bot_id", bot.ID).
				Msg("download failed, trying the next eligible bot")
			bs.loads.RecordError(bot.ID)
			bots = removeBot(bots, bot.ID)
			lastErr = err
		}
	}
	return "", fmt.Errorf("daemon: download car %d: attempts exhausted: %w", carID, lastErr)
}

// store streams the downloaded CAR into the cache via a temp file and runs
// the post-download bookkeeping (fresh file_id, lazy status recovery).
func (bs *Blockstore) store(ctx context.Context, car model.Car, bot model.Bot, freshFileID string, rc io.ReadCloser) (string, error) {
	defer rc.Close()

	if err := os.MkdirAll(bs.tmpDir, 0o755); err != nil {
		return "", fmt.Errorf("daemon: create tmp dir: %w", err)
	}
	tmp, err := os.CreateTemp(bs.tmpDir, fmt.Sprintf("car-%d-*", car.ID))
	if err != nil {
		return "", fmt.Errorf("daemon: create temp car file: %w", err)
	}
	tmpName := tmp.Name()
	size, err := io.Copy(tmp, rc)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmpName)
		return "", fmt.Errorf("daemon: download car %d: %w", car.ID, err)
	}

	path, err := bs.cache.Put(car.ID, tmpName, size)
	if err != nil {
		os.Remove(tmpName)
		return "", fmt.Errorf("daemon: cache car %d: %w", car.ID, err)
	}

	if freshFileID != "" {
		if err := bs.cars.UpsertFileID(ctx, car.ID, bot.ID, freshFileID); err != nil {
			log.Error().Err(err).Int64("car_id", car.ID).Int64("bot_id", bot.ID).Msg("record fresh file_id")
		}
	}
	if car.Status != model.CarPublished {
		if err := bs.cars.SetStatus(ctx, car.ID, model.CarPublished); err != nil && !errors.Is(err, repo.ErrNotFound) {
			log.Error().Err(err).Int64("car_id", car.ID).Msg("recover car status to published")
		} else {
			log.Info().
				Int64("car_id", car.ID).
				Str("previous_status", string(car.Status)).
				Msg("car downloaded successfully, status recovered to published")
		}
	}
	return path, nil
}

// Put always fails: the daemon never writes blocks.
func (bs *Blockstore) Put(context.Context, blocks.Block) error { return ErrReadOnly }

// PutMany always fails: the daemon never writes blocks.
func (bs *Blockstore) PutMany(context.Context, []blocks.Block) error { return ErrReadOnly }

// DeleteBlock always fails: the daemon never deletes recoverable data.
func (bs *Blockstore) DeleteBlock(context.Context, cid.Cid) error { return ErrReadOnly }

// AllKeysChan streams every CID indexed in the blocks table.
func (bs *Blockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return bs.keys.AllCIDs(ctx)
}

// HashOnRead is a no-op: payload integrity is anchored by the CID index.
func (bs *Blockstore) HashOnRead(bool) {}

// markUnavailable updates the in-memory bot list so the selector skips the
// flood-waited bot on the next iteration of the retry loop.
func markUnavailable(bots []model.Bot, botID int64, until time.Time) {
	for i := range bots {
		if bots[i].ID == botID {
			bots[i].UnavailableUntil = &until
			return
		}
	}
}

// removeBot drops the bot from the in-memory candidate list after a
// non-classified download failure.
func removeBot(bots []model.Bot, botID int64) []model.Bot {
	out := bots[:0]
	for _, b := range bots {
		if b.ID != botID {
			out = append(out, b)
		}
	}
	return out
}

// tmpDirFor returns the temp-download directory for a cache directory.
func tmpDirFor(cacheDir string) string { return filepath.Join(cacheDir, "tmp") }
