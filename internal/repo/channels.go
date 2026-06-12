package repo

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/ulbwa/ipfsgram/internal/model"
)

// ChannelRepo provides access to the channels and bot_channels tables.
type ChannelRepo struct {
	db *sqlx.DB
}

// NewChannelRepo returns a ChannelRepo over db.
func NewChannelRepo(db *sqlx.DB) *ChannelRepo {
	return &ChannelRepo{db: db}
}

// Add inserts a new channel and returns its ID.
func (r *ChannelRepo) Add(ctx context.Context, c model.Channel) (int64, error) {
	var id int64
	err := r.db.GetContext(ctx, &id, `
		INSERT INTO channels (tg_id, title, message_count, message_limit, active)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id`,
		c.TgID, c.Title, c.MessageCount, c.MessageLimit, c.Active)
	if err != nil {
		return 0, fmt.Errorf("add channel: %w", err)
	}
	return id, nil
}

// List returns all channels ordered by ID.
func (r *ChannelRepo) List(ctx context.Context) ([]model.Channel, error) {
	var channels []model.Channel
	err := r.db.SelectContext(ctx, &channels, `
		SELECT id, tg_id, title, message_count, message_limit, active
		FROM channels ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list channels: %w", err)
	}
	return channels, nil
}

// GetByTgID returns the channel with the given Telegram ID, or ErrNotFound.
func (r *ChannelRepo) GetByTgID(ctx context.Context, tgID int64) (model.Channel, error) {
	var c model.Channel
	err := r.db.GetContext(ctx, &c, `
		SELECT id, tg_id, title, message_count, message_limit, active
		FROM channels WHERE tg_id = $1`, tgID)
	if err != nil {
		return model.Channel{}, notFound(fmt.Sprintf("get channel tg_id %d", tgID), err)
	}
	return c, nil
}

// Remove deletes the channel with the given ID; the schema cascades the
// deletion to its cars, blocks and car_file_ids. It returns ErrNotFound if
// the channel does not exist.
func (r *ChannelRepo) Remove(ctx context.Context, id int64) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM channels WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("remove channel %d: %w", id, err)
	}
	return requireAffected(res, fmt.Sprintf("remove channel %d", id))
}

// IncrementMessageCount atomically increments the channel's message counter.
func (r *ChannelRepo) IncrementMessageCount(ctx context.Context, id int64) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE channels SET message_count = message_count + 1 WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("increment channel %d message_count: %w", id, err)
	}
	return requireAffected(res, fmt.Sprintf("increment channel %d message_count", id))
}

// UpsertBotChannel inserts or updates the bot↔channel membership row.
func (r *ChannelRepo) UpsertBotChannel(ctx context.Context, bc model.BotChannel) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO bot_channels (bot_id, channel_id, can_post, can_read, can_delete, member, verified_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (bot_id, channel_id) DO UPDATE SET
			can_post    = EXCLUDED.can_post,
			can_read    = EXCLUDED.can_read,
			can_delete  = EXCLUDED.can_delete,
			member      = EXCLUDED.member,
			verified_at = EXCLUDED.verified_at`,
		bc.BotID, bc.ChannelID, bc.CanPost, bc.CanRead, bc.CanDelete, bc.Member, bc.VerifiedAt)
	if err != nil {
		return fmt.Errorf("upsert bot_channel (%d, %d): %w", bc.BotID, bc.ChannelID, err)
	}
	return nil
}

// MembersOf returns the bot membership rows for the given channel.
func (r *ChannelRepo) MembersOf(ctx context.Context, channelID int64) ([]model.BotChannel, error) {
	var members []model.BotChannel
	err := r.db.SelectContext(ctx, &members, `
		SELECT bot_id, channel_id, can_post, can_read, can_delete, member, verified_at
		FROM bot_channels WHERE channel_id = $1 ORDER BY bot_id`, channelID)
	if err != nil {
		return nil, fmt.Errorf("members of channel %d: %w", channelID, err)
	}
	return members, nil
}

// RemoveStats reports the impact of removing the channel: how many pins have
// at least one block stored in this channel, and the total size of the
// channel's cars. Intended for deletion confirmation prompts.
func (r *ChannelRepo) RemoveStats(ctx context.Context, channelID int64) (pins int64, bytes int64, err error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT
			(SELECT count(DISTINCT pb.root_cid)
			 FROM pin_blocks pb
			 JOIN blocks b ON b.cid = pb.cid
			 JOIN cars c ON c.id = b.car_id
			 WHERE c.channel_id = $1),
			(SELECT coalesce(sum(size), 0) FROM cars WHERE channel_id = $1)`,
		channelID)
	if err := row.Scan(&pins, &bytes); err != nil {
		return 0, 0, fmt.Errorf("remove stats for channel %d: %w", channelID, err)
	}
	return pins, bytes, nil
}
