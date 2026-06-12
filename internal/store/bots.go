// Store methods for the bots table.
package store

import (
	"context"
	"fmt"
	"time"
)

// AddBot inserts a new bot and returns its ID. It returns ErrBotExists if a
// bot with the same token or Telegram ID already exists.
func (s *Store) AddBot(ctx context.Context, b Bot) (int64, error) {
	// Clear the generated-identity primary key so GORM omits it from the INSERT.
	b.ID = 0
	err := s.db.WithContext(ctx).Create(&b).Error
	if err != nil {
		if isUniqueViolation(err) {
			return 0, ErrBotExists
		}
		return 0, fmt.Errorf("add bot: %w", err)
	}
	return b.ID, nil
}

// Bots returns all bots ordered by ID.
func (s *Store) Bots(ctx context.Context) ([]Bot, error) {
	var bots []Bot
	if err := s.db.WithContext(ctx).Order("id").Find(&bots).Error; err != nil {
		return nil, fmt.Errorf("list bots: %w", err)
	}
	return bots, nil
}

// BotByID returns the bot with the given ID, or ErrNotFound.
func (s *Store) BotByID(ctx context.Context, id int64) (Bot, error) {
	var b Bot
	err := s.db.WithContext(ctx).Where("id = ?", id).Take(&b).Error
	if err != nil {
		return Bot{}, notFound(fmt.Sprintf("get bot %d", id), err)
	}
	return b, nil
}

// BotByUsername returns the bot with the given username, or ErrNotFound.
func (s *Store) BotByUsername(ctx context.Context, username string) (Bot, error) {
	var b Bot
	err := s.db.WithContext(ctx).Where("username = ?", username).Take(&b).Error
	if err != nil {
		return Bot{}, notFound(fmt.Sprintf("get bot @%s", username), err)
	}
	return b, nil
}

// RemoveBot deletes the bot with the given ID, or ErrNotFound.
func (s *Store) RemoveBot(ctx context.Context, id int64) error {
	res := s.db.WithContext(ctx).Where("id = ?", id).Delete(&Bot{})
	return requireAffected(res, fmt.Sprintf("remove bot %d", id))
}

// SetBotUnavailable marks the bot as unavailable until the given time.
func (s *Store) SetBotUnavailable(ctx context.Context, id int64, until time.Time) error {
	res := s.db.WithContext(ctx).
		Model(&Bot{}).
		Where("id = ?", id).
		Update("unavailable_until", until)
	return requireAffected(res, fmt.Sprintf("set bot %d unavailable_until", id))
}

// SetBotActive sets the bot's active flag.
func (s *Store) SetBotActive(ctx context.Context, id int64, active bool) error {
	res := s.db.WithContext(ctx).
		Model(&Bot{}).
		Where("id = ?", id).
		Update("active", active)
	return requireAffected(res, fmt.Sprintf("set bot %d active", id))
}
