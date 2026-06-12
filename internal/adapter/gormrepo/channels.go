package gormrepo

import (
	"context"
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/ulbwa/ipfsgram/internal/domain"
	"github.com/ulbwa/ipfsgram/internal/port"
)

// ChannelRepository is a GORM-backed port.ChannelRepository.
type ChannelRepository struct {
	gdb *gorm.DB
}

var _ port.ChannelRepository = (*ChannelRepository)(nil)

// NewChannelRepository returns a ChannelRepository over gdb.
func NewChannelRepository(gdb *gorm.DB) *ChannelRepository {
	return &ChannelRepository{gdb: gdb}
}

// Add inserts a new channel and returns its ID.
func (r *ChannelRepository) Add(ctx context.Context, c domain.Channel) (int64, error) {
	c.ID = 0 // generated-identity primary key
	if err := r.gdb.WithContext(ctx).Create(&c).Error; err != nil {
		return 0, fmt.Errorf("add channel: %w", err)
	}
	return c.ID, nil
}

// List returns all channels ordered by ID.
func (r *ChannelRepository) List(ctx context.Context) ([]domain.Channel, error) {
	var channels []domain.Channel
	if err := r.gdb.WithContext(ctx).Order("id").Find(&channels).Error; err != nil {
		return nil, fmt.Errorf("list channels: %w", err)
	}
	return channels, nil
}

// GetByTgID returns the channel with the given Telegram ID, or
// domain.ErrNotFound.
func (r *ChannelRepository) GetByTgID(ctx context.Context, tgID int64) (domain.Channel, error) {
	var c domain.Channel
	err := r.gdb.WithContext(ctx).Where("tg_id = ?", tgID).Take(&c).Error
	if err != nil {
		return domain.Channel{}, notFound(fmt.Sprintf("get channel tg_id %d", tgID), err, domain.ErrNotFound)
	}
	return c, nil
}

// GetByID returns the channel with the given ID, or domain.ErrNotFound.
func (r *ChannelRepository) GetByID(ctx context.Context, id int64) (domain.Channel, error) {
	var c domain.Channel
	err := r.gdb.WithContext(ctx).Where("id = ?", id).Take(&c).Error
	if err != nil {
		return domain.Channel{}, notFound(fmt.Sprintf("get channel %d", id), err, domain.ErrNotFound)
	}
	return c, nil
}

// Remove deletes the channel with the given ID; the schema cascades the
// deletion to its cars, blocks and car_file_ids. Returns domain.ErrNotFound if
// the channel does not exist.
func (r *ChannelRepository) Remove(ctx context.Context, id int64) error {
	res := r.gdb.WithContext(ctx).Where("id = ?", id).Delete(&domain.Channel{})
	return requireAffected(res, fmt.Sprintf("remove channel %d", id), domain.ErrNotFound)
}

// IncrementMessageCount atomically increments the channel's message counter.
func (r *ChannelRepository) IncrementMessageCount(ctx context.Context, id int64) error {
	res := r.gdb.WithContext(ctx).
		Model(&domain.Channel{}).
		Where("id = ?", id).
		Update("message_count", gorm.Expr("message_count + 1"))
	return requireAffected(res, fmt.Sprintf("increment channel %d message_count", id), domain.ErrNotFound)
}

// UpsertBotChannel inserts or updates the bot↔channel membership row.
func (r *ChannelRepository) UpsertBotChannel(ctx context.Context, bc domain.BotChannel) error {
	err := r.gdb.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "bot_id"}, {Name: "channel_id"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"can_post", "can_read", "can_delete", "member", "verified_at",
			}),
		}).
		Create(&bc).Error
	if err != nil {
		return fmt.Errorf("upsert bot_channel (%d, %d): %w", bc.BotID, bc.ChannelID, err)
	}
	return nil
}

// MembersOf returns the bot membership rows for the given channel.
func (r *ChannelRepository) MembersOf(ctx context.Context, channelID int64) ([]domain.BotChannel, error) {
	var members []domain.BotChannel
	err := r.gdb.WithContext(ctx).
		Where("channel_id = ?", channelID).
		Order("bot_id").
		Find(&members).Error
	if err != nil {
		return nil, fmt.Errorf("members of channel %d: %w", channelID, err)
	}
	return members, nil
}

// RemoveStats reports the impact of removing the channel: how many pins have at
// least one block stored in this channel, and the total size of the channel's
// cars.
func (r *ChannelRepository) RemoveStats(ctx context.Context, channelID int64) (pins int64, bytes int64, err error) {
	var row struct {
		Pins  int64
		Bytes int64
	}
	err = r.gdb.WithContext(ctx).Raw(`
		SELECT
			(SELECT count(DISTINCT pb.root_cid)
			 FROM pin_blocks pb
			 JOIN blocks b ON b.cid = pb.cid
			 JOIN cars c ON c.id = b.car_id
			 WHERE c.channel_id = ?) AS pins,
			(SELECT coalesce(sum(size), 0) FROM cars WHERE channel_id = ?) AS bytes`,
		channelID, channelID).Scan(&row).Error
	if err != nil {
		return 0, 0, fmt.Errorf("remove stats for channel %d: %w", channelID, err)
	}
	return row.Pins, row.Bytes, nil
}

// DeleteOrphanPins deletes pins none of whose blocks still exist — e.g. after a
// channel removal cascades away its cars/blocks. A pin is orphaned when no
// pin_blocks row references a still-present block. Returns the number deleted.
func (r *ChannelRepository) DeleteOrphanPins(ctx context.Context) (int64, error) {
	res := r.gdb.WithContext(ctx).Exec(`
		DELETE FROM pins p WHERE NOT EXISTS (
			SELECT 1 FROM pin_blocks pb
			JOIN blocks b ON b.cid = pb.cid
			WHERE pb.root_cid = p.root_cid
		)`)
	if res.Error != nil {
		return 0, fmt.Errorf("delete orphan pins: %w", res.Error)
	}
	return res.RowsAffected, nil
}
