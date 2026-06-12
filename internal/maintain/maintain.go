// maintain.go — package doc, the Service type with its consumer-side
// dependency interfaces, the Unpin entry point and shared constants.

// Package maintain implements IPFSgram's housekeeping workflows: unpinning
// roots (`ipfsgram rm`), garbage-collecting unpinned CARs (`ipfsgram gc`),
// the doctor's diagnostics — orphaned pending cars, bot×channel membership
// revalidation, and recovery of cars marked unavailable — and the impact
// computations behind the `channel remove`/`bot remove` confirmations.
// Garbage collection is split into a lock-free GCCandidates step (so the
// caller can preview and confirm) and a GC step that re-fetches and deletes
// under the exclusive advisory lock. All diagnostics go to a zerolog.Logger;
// user-facing text is the caller's job.
package maintain

import (
	"context"
	"time"

	"github.com/rs/zerolog"

	"github.com/ulbwa/ipfsgram/internal/selector"
	"github.com/ulbwa/ipfsgram/internal/store"
	"github.com/ulbwa/ipfsgram/internal/telegram"
)

// OrphanPendingAge is how old a pending car must be to count as orphaned.
const OrphanPendingAge = 15 * time.Minute

// storage is the subset of *store.Store the maintenance workflows call.
// *store.Store satisfies it implicitly; tests substitute a fake.
type storage interface {
	// Cars.
	UnpinnedCars(ctx context.Context) ([]store.Car, error)
	OrphanPendingCars(ctx context.Context, olderThan time.Duration) ([]store.Car, error)
	CarsWithStatus(ctx context.Context, statuses ...store.CarStatus) ([]store.Car, error)
	CarsAccessibleOnlyVia(ctx context.Context, botID int64) ([]store.Car, error)
	SetCarStatus(ctx context.Context, carID int64, st store.CarStatus) error
	DeleteCar(ctx context.Context, carID int64) error
	CarFileIDs(ctx context.Context, carID int64) (map[int64]string, error)

	// Bots.
	Bots(ctx context.Context) ([]store.Bot, error)
	SetBotUnavailable(ctx context.Context, id int64, until time.Time) error
	RemoveBot(ctx context.Context, id int64) error

	// Channels.
	Channels(ctx context.Context) ([]store.Channel, error)
	ChannelMembers(ctx context.Context, channelID int64) ([]store.BotChannel, error)
	UpsertBotChannel(ctx context.Context, bc store.BotChannel) error
	ChannelRemoveStats(ctx context.Context, channelID int64) (pins int64, bytes int64, err error)
	RemoveChannel(ctx context.Context, id int64) error

	// Pins.
	RemovePin(ctx context.Context, root []byte) error
	DeleteOrphanPins(ctx context.Context) (int64, error)

	// Advisory locking.
	WithExclusiveGCLock(ctx context.Context, fn func(ctx context.Context) error) error
}

// transport is the subset of the Telegram client the maintenance workflows
// call. telegram.Client satisfies it implicitly; tests substitute a fake.
type transport interface {
	ProbeChannel(ctx context.Context, token string, channelTgID int64) (telegram.ChannelInfo, error)
	CheckMessage(ctx context.Context, token string, channelTgID, messageID int64) error
	DeleteMessage(ctx context.Context, token string, channelTgID, messageID int64) error
}

// Service performs maintenance. Store is *store.Store and Transport a
// telegram client in production; Loads is the shared selector load counter
// (required by the doctor's recovery probe).
type Service struct {
	Store     storage
	Transport transport
	Loads     *selector.LoadCounter
	Logger    zerolog.Logger
}

// Unpin removes the pin with the given root. store.ErrNotFound passes through
// so the caller can report "pin not found".
func (s *Service) Unpin(ctx context.Context, root []byte) error {
	return s.Store.RemovePin(ctx, root)
}
