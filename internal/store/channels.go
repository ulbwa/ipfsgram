// Store methods for the channels and bot_channels tables, including the
// cross-entity removal-impact and orphan-pin cleanup queries used by
// `channel remove`.
package store

import (
	"context"
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// AddChannel inserts a new channel and returns its ID.
func (s *Store) AddChannel(ctx context.Context, c Channel) (int64, error) {
	c.ID = 0 // generated-identity primary key
	if err := s.db.WithContext(ctx).Create(&c).Error; err != nil {
		return 0, fmt.Errorf("add channel: %w", err)
	}
	return c.ID, nil
}

// Channels returns all channels ordered by ID.
func (s *Store) Channels(ctx context.Context) ([]Channel, error) {
	var channels []Channel
	if err := s.db.WithContext(ctx).Order("id").Find(&channels).Error; err != nil {
		return nil, fmt.Errorf("list channels: %w", err)
	}
	return channels, nil
}

// ChannelByTgID returns the channel with the given Telegram ID, or ErrNotFound.
func (s *Store) ChannelByTgID(ctx context.Context, tgID int64) (Channel, error) {
	var c Channel
	err := s.db.WithContext(ctx).Where("tg_id = ?", tgID).Take(&c).Error
	if err != nil {
		return Channel{}, notFound(fmt.Sprintf("get channel tg_id %d", tgID), err)
	}
	return c, nil
}

// ChannelByID returns the channel with the given ID, or ErrNotFound.
func (s *Store) ChannelByID(ctx context.Context, id int64) (Channel, error) {
	var c Channel
	err := s.db.WithContext(ctx).Where("id = ?", id).Take(&c).Error
	if err != nil {
		return Channel{}, notFound(fmt.Sprintf("get channel %d", id), err)
	}
	return c, nil
}

// RemoveChannel deletes the channel with the given ID; the schema cascades the
// deletion to its cars, blocks and car_file_ids. Returns ErrNotFound if the
// channel does not exist.
func (s *Store) RemoveChannel(ctx context.Context, id int64) error {
	res := s.db.WithContext(ctx).Where("id = ?", id).Delete(&Channel{})
	return requireAffected(res, fmt.Sprintf("remove channel %d", id))
}

// IncrementMessageCount atomically increments the channel's message counter.
func (s *Store) IncrementMessageCount(ctx context.Context, id int64) error {
	res := s.db.WithContext(ctx).
		Model(&Channel{}).
		Where("id = ?", id).
		Update("message_count", gorm.Expr("message_count + 1"))
	return requireAffected(res, fmt.Sprintf("increment channel %d message_count", id))
}

// UpsertBotChannel inserts or updates the bot↔channel membership row.
func (s *Store) UpsertBotChannel(ctx context.Context, bc BotChannel) error {
	err := s.db.WithContext(ctx).
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

// ChannelMembers returns the bot membership rows for the given channel.
func (s *Store) ChannelMembers(ctx context.Context, channelID int64) ([]BotChannel, error) {
	var members []BotChannel
	err := s.db.WithContext(ctx).
		Where("channel_id = ?", channelID).
		Order("bot_id").
		Find(&members).Error
	if err != nil {
		return nil, fmt.Errorf("members of channel %d: %w", channelID, err)
	}
	return members, nil
}

// ChannelRemoveStats reports the impact of removing the channel: how many pins
// have at least one block stored in this channel, and the total size of the
// channel's cars.
func (s *Store) ChannelRemoveStats(ctx context.Context, channelID int64) (pins int64, bytes int64, err error) {
	var row struct {
		Pins  int64
		Bytes int64
	}
	err = s.db.WithContext(ctx).Raw(`
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
