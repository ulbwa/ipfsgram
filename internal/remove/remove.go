// remove.go — unpinning roots and the impact computations and execution for
// `channel remove` and `bot remove`: removal stats for confirmation prompts,
// the cascade plus orphan-pin cleanup, and the optional purge of
// now-inaccessible car records.

// Package remove implements IPFSgram's explicit removal workflows: unpinning a
// root (`ipfsgram rm`), and the plan/execute split behind the `channel remove`
// and `bot remove` confirmations (impact stats, the schema cascade with
// orphan-pin cleanup, and the optional purge of now-inaccessible car records).
// The narrow store interface it needs is declared here, on the consumer side.
package remove

import (
	"context"
	"errors"

	"github.com/ulbwa/ipfsgram/internal/store"
)

// storage is the subset of *store.Store the removal workflows call. *store.Store
// satisfies it implicitly.
type storage interface {
	RemovePin(ctx context.Context, root []byte) error
	ChannelRemoveStats(ctx context.Context, channelID int64) (pins int64, bytes int64, err error)
	RemoveChannel(ctx context.Context, id int64) error
	DeleteOrphanPins(ctx context.Context) (int64, error)
	CarsAccessibleOnlyVia(ctx context.Context, botID int64) ([]store.Car, error)
	RemoveBot(ctx context.Context, id int64) error
	DeleteCar(ctx context.Context, carID int64) error
}

// Service performs explicit removals. Store is *store.Store in production.
type Service struct {
	Store storage
}

// Unpin removes the pin with the given root. store.ErrNotFound passes through
// so the caller can report "pin not found".
func (s *Service) Unpin(ctx context.Context, root []byte) error {
	return s.Store.RemovePin(ctx, root)
}

// ChannelRemovePlan reports the impact of removing the channel, for the
// caller's confirmation prompt: how many pins have at least one block stored
// in this channel, and the total size of the channel's cars.
func (s *Service) ChannelRemovePlan(ctx context.Context, channelID int64) (pins int64, bytes int64, err error) {
	return s.Store.ChannelRemoveStats(ctx, channelID)
}

// ChannelRemoveExecute deletes the channel row (the schema cascades
// cars/blocks/car_file_ids) and then drops the pins whose blocks all lived in
// this channel and are now empty. Telegram messages are not deleted.
// store.ErrNotFound passes through when the channel does not exist.
func (s *Service) ChannelRemoveExecute(ctx context.Context, channelID int64) error {
	if err := s.Store.RemoveChannel(ctx, channelID); err != nil {
		return err
	}
	// The schema cascades cars/blocks/car_file_ids; pins whose blocks all
	// lived in this channel are now empty — drop them.
	_, err := s.Store.DeleteOrphanPins(ctx)
	return err
}

// BotRemovePlan returns the cars that become unreachable if the bot is
// removed — the bot is the last active member of their channel — and their
// total size, for the caller's confirmation prompt.
func (s *Service) BotRemovePlan(ctx context.Context, botID int64) (affected []store.Car, bytes int64, err error) {
	affected, err = s.Store.CarsAccessibleOnlyVia(ctx, botID)
	if err != nil {
		return nil, 0, err
	}
	for _, c := range affected {
		bytes += c.Size
	}
	return affected, bytes, nil
}

// BotRemoveExecute deletes the bot row. store.ErrNotFound passes through when
// the bot does not exist. The affected cars from BotRemovePlan are kept (their
// statuses lazily become no_bot_access) unless the caller purges them with
// PurgeCars.
func (s *Service) BotRemoveExecute(ctx context.Context, botID int64) error {
	return s.Store.RemoveBot(ctx, botID)
}

// PurgeCars deletes the given car rows (blocks and file_ids cascade) — used to
// drop the now-inaccessible records after a bot removal. store.ErrNotFound on
// any row is tolerated (already removed).
func (s *Service) PurgeCars(ctx context.Context, cars []store.Car) error {
	for _, c := range cars {
		if err := s.Store.DeleteCar(ctx, c.ID); err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
	}
	return nil
}
