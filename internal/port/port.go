// Package port defines all of IPFSgram's contracts (interfaces) between the
// service layer and the infrastructure adapters: repositories, the Telegram
// Transport, the disk Cache, the CAR Packer/BlockReader, the bot/channel
// Selector, and the BlockSource. It depends only on the domain layer (plus the
// standard library and go-cid).
package port

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/ulbwa/ipfsgram/internal/domain"
)

// Selector strategy errors.
var (
	// ErrNoBotAvailable is returned when no bot passes the eligibility filters.
	ErrNoBotAvailable = errors.New("no bot available")
	// ErrNoChannelSpace is returned when no active channel has free space.
	ErrNoChannelSpace = errors.New("no channel has free space")
)

// ConfigRepository provides access to the key/value config table.
type ConfigRepository interface {
	// Get returns the value for key, or domain.ErrNotFound if absent.
	Get(ctx context.Context, key string) (string, error)
	GetInt64(ctx context.Context, key string) (int64, error)
	GetFloat64(ctx context.Context, key string) (float64, error)
	GetBool(ctx context.Context, key string) (bool, error)
	Set(ctx context.Context, key, value string) error
}

// BotRepository provides access to the bots table.
type BotRepository interface {
	// Add inserts a new bot and returns its ID, or domain.ErrBotExists on a
	// unique conflict (token or tg_id).
	Add(ctx context.Context, b domain.Bot) (int64, error)
	List(ctx context.Context) ([]domain.Bot, error)
	GetByID(ctx context.Context, id int64) (domain.Bot, error)
	// GetByUsername returns the bot with the given username, or
	// domain.ErrNotFound if none.
	GetByUsername(ctx context.Context, username string) (domain.Bot, error)
	Remove(ctx context.Context, id int64) error
	SetUnavailableUntil(ctx context.Context, id int64, until time.Time) error
	SetActive(ctx context.Context, id int64, active bool) error
}

// ChannelRepository provides access to the channels and bot_channels tables.
type ChannelRepository interface {
	Add(ctx context.Context, c domain.Channel) (int64, error)
	List(ctx context.Context) ([]domain.Channel, error)
	GetByTgID(ctx context.Context, tgID int64) (domain.Channel, error)
	GetByID(ctx context.Context, id int64) (domain.Channel, error)
	Remove(ctx context.Context, id int64) error
	IncrementMessageCount(ctx context.Context, id int64) error
	UpsertBotChannel(ctx context.Context, bc domain.BotChannel) error
	MembersOf(ctx context.Context, channelID int64) ([]domain.BotChannel, error)
	// RemoveStats reports the impact of removing the channel: how many pins have
	// at least one block stored in this channel, and the total size of the
	// channel's cars.
	RemoveStats(ctx context.Context, channelID int64) (pins int64, bytes int64, err error)
	// DeleteOrphanPins deletes pins none of whose blocks still exist (e.g. after
	// a channel cascade) and returns the number deleted.
	DeleteOrphanPins(ctx context.Context) (int64, error)
}

// CarRepository provides access to the cars and car_file_ids tables.
type CarRepository interface {
	CreatePending(ctx context.Context, channelID, size int64, blockCount int) (int64, error)
	MarkPublished(ctx context.Context, carID, messageID int64) error
	SetStatus(ctx context.Context, carID int64, st domain.CarStatus) error
	Get(ctx context.Context, carID int64) (domain.Car, error)
	Delete(ctx context.Context, carID int64) error
	UpsertFileID(ctx context.Context, carID, botID int64, fileID string) error
	DeleteFileID(ctx context.Context, carID, botID int64) error
	FileIDs(ctx context.Context, carID int64) (map[int64]string, error)
	OrphanPending(ctx context.Context, olderThan time.Duration) ([]domain.Car, error)
	UnpinnedCars(ctx context.Context) ([]domain.Car, error)
	// ByStatuses returns cars in any of the given statuses (for doctor recovery).
	ByStatuses(ctx context.Context, statuses ...domain.CarStatus) ([]domain.Car, error)
	// AccessibleOnlyVia returns cars whose channel has no OTHER active member
	// bot besides the given one — i.e. cars that become unreachable if that bot
	// is removed.
	AccessibleOnlyVia(ctx context.Context, botID int64) ([]domain.Car, error)
	// CountByStatus returns the number of cars per status (for `status`).
	CountByStatus(ctx context.Context) (map[domain.CarStatus]int64, error)
}

// BlockRepository provides access to the blocks table.
type BlockRepository interface {
	// Lookup returns the block for the given CID, or domain.ErrNotFound.
	Lookup(ctx context.Context, cid []byte) (domain.Block, error)
	// Existing returns the blocks that exist for the given CIDs, keyed by
	// string(cid).
	Existing(ctx context.Context, cids [][]byte) (map[string]domain.Block, error)
	// Upsert inserts blocks or, when a CID already exists, repoints it to the
	// new car_id/offset/length (ON CONFLICT(cid) DO UPDATE). The single write
	// primitive of the publish path: it is correct both for brand-new blocks
	// and for re-uploaded ones, including blocks whose previous car row was
	// deleted between the dedup snapshot and the write.
	Upsert(ctx context.Context, blocks []domain.Block) error
	// StreamAllCIDs streams every block CID in keyset-paginated batches. The CID
	// channel is closed when the scan completes, fails, or ctx is done; any
	// terminal error is delivered on the error channel. Used by AllKeysChan and
	// the reprovider.
	StreamAllCIDs(ctx context.Context) (<-chan []byte, <-chan error)
	CountAll(ctx context.Context) (int64, error)
}

// PinRepository provides access to the pins and pin_blocks tables.
type PinRepository interface {
	// Create records a pin and its block set in one transaction; idempotent on
	// re-creation of an existing root.
	Create(ctx context.Context, root []byte, name string, size int64, blockCIDs [][]byte) error
	// Remove deletes the pin with the given root, or domain.ErrNotFound.
	Remove(ctx context.Context, root []byte) error
	List(ctx context.Context) ([]domain.Pin, error)
	Exists(ctx context.Context, root []byte) (bool, error)
	Count(ctx context.Context) (int64, error)
}

// MTProtoRepository provides access to the mtproto_credentials table.
type MTProtoRepository interface {
	// Latest returns the most recently created credentials, or (nil, nil).
	Latest(ctx context.Context) (*domain.MTProtoCreds, error)
	// Active returns the active credentials, or (nil, nil).
	Active(ctx context.Context) (*domain.MTProtoCreds, error)
	Activate(ctx context.Context, apiID int, apiHash string) error
	Deactivate(ctx context.Context) error
}

// UploadResult is the result of publishing a CAR to a channel.
type UploadResult struct {
	MessageID int64
	FileID    string // uploader bot's file_id (Bot API); empty for pure MTProto
}

// ChannelInfo describes a bot's access to a channel.
type ChannelInfo struct {
	Title     string
	Member    bool
	CanPost   bool
	CanRead   bool
	CanDelete bool
}

// Transport is the Telegram transport contract (Bot API or MTProto).
type Transport interface {
	// ValidateToken checks a token (getMe) and returns the bot's tg_id and
	// username.
	ValidateToken(ctx context.Context, token string) (tgID int64, username string, err error)
	// ProbeChannel checks a bot's access to a channel and its permissions.
	ProbeChannel(ctx context.Context, token string, channelTgID int64) (ChannelInfo, error)
	// Upload publishes a file to a channel on behalf of a bot.
	Upload(ctx context.Context, token string, channelTgID int64, name string, size int64, r io.Reader) (UploadResult, error)
	// Download downloads a message's file. fileID is a hint (may be empty).
	Download(ctx context.Context, token string, channelTgID, messageID int64, fileID string) (rc io.ReadCloser, freshFileID string, err error)
	// CheckMessage reports whether a message is alive and its file accessible
	// (for deduplication).
	CheckMessage(ctx context.Context, token string, channelTgID, messageID int64) error
	// DeleteMessage deletes a message (for gc / channel remove).
	DeleteMessage(ctx context.Context, token string, channelTgID, messageID int64) error
}

// Cache is the daemon's local disk cache of CAR files, keyed by carID.
type Cache interface {
	// Get returns the cached file path and true when the archive is present; a
	// hit refreshes the entry's recency/deadline.
	Get(carID int64) (path string, ok bool)
	// Put moves the file at src into the cache, possibly evicting other entries,
	// and returns the cached path.
	Put(carID int64, src string, size int64) (path string, err error)
	Close() error
}

// Packer writes blocks into size-capped CARv1 archives, recording each block's
// payload offset/length. Not safe for concurrent use.
type Packer interface {
	Add(c cid.Cid, data []byte) error
	Finish() ([]domain.PackedCar, error)
}

// PackerFactory constructs Packers writing into dir, capped at maxSize, with the
// given DAG root in each CAR header.
type PackerFactory interface {
	New(dir string, maxSize int64, root cid.Cid) (Packer, error)
}

// BlockReader serves a block payload from a CAR file by absolute offset/length
// without parsing the CAR.
type BlockReader interface {
	ReadAt(path string, off int64, length int32) ([]byte, error)
}

// Selector implements the fixed strategies for picking bots and channels.
type Selector interface {
	// PickUploadBot picks the least-loaded healthy member bot that can post.
	PickUploadBot(now time.Time, bots []domain.Bot, members []domain.BotChannel) (domain.Bot, error)
	// PickDownloadBot picks a bot to download with (file_id owner first, else
	// least-loaded reader); returns the picked bot's file_id (or "").
	PickDownloadBot(now time.Time, bots []domain.Bot, members []domain.BotChannel, fileIDs map[int64]string) (domain.Bot, string, error)
	// PickChannel implements fill-first; warn is true at or above warnThreshold.
	PickChannel(channels []domain.Channel, warnThreshold float64) (domain.Channel, bool, error)
	// Record notes a successful operation by a bot (for load balancing).
	Record(botID int64)
	// RecordError notes an errored operation by a bot (for tie-breaking).
	RecordError(botID int64)
}

// BlockSource loads a DAG from some origin (local file via UnixFS chunking, or
// the IPFS network by CID) into a root and an ordered set of raw blocks.
type BlockSource interface {
	// Load returns the DAG root and all blocks in traversal order. Memory use is
	// ~ content size (v1, documented).
	Load(ctx context.Context) (root cid.Cid, blocks []domain.RawBlock, err error)
}
