package gormrepo

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/ulbwa/ipfsgram/internal/domain"
	"github.com/ulbwa/ipfsgram/internal/port"
)

// BotRepository is a GORM-backed port.BotRepository.
type BotRepository struct {
	gdb *gorm.DB
}

var _ port.BotRepository = (*BotRepository)(nil)

// NewBotRepository returns a BotRepository over gdb.
func NewBotRepository(gdb *gorm.DB) *BotRepository {
	return &BotRepository{gdb: gdb}
}

// Add inserts a new bot and returns its ID. It returns domain.ErrBotExists if a
// bot with the same token or Telegram ID already exists.
func (r *BotRepository) Add(ctx context.Context, b domain.Bot) (int64, error) {
	// Clear the generated-identity primary key so GORM omits it from the INSERT.
	b.ID = 0
	err := r.gdb.WithContext(ctx).Create(&b).Error
	if err != nil {
		if isUniqueViolation(err) {
			return 0, domain.ErrBotExists
		}
		return 0, fmt.Errorf("add bot: %w", err)
	}
	return b.ID, nil
}

// List returns all bots ordered by ID.
func (r *BotRepository) List(ctx context.Context) ([]domain.Bot, error) {
	var bots []domain.Bot
	if err := r.gdb.WithContext(ctx).Order("id").Find(&bots).Error; err != nil {
		return nil, fmt.Errorf("list bots: %w", err)
	}
	return bots, nil
}

// GetByID returns the bot with the given ID, or domain.ErrNotFound.
func (r *BotRepository) GetByID(ctx context.Context, id int64) (domain.Bot, error) {
	var b domain.Bot
	err := r.gdb.WithContext(ctx).Where("id = ?", id).Take(&b).Error
	if err != nil {
		return domain.Bot{}, notFound(fmt.Sprintf("get bot %d", id), err, domain.ErrNotFound)
	}
	return b, nil
}

// GetByUsername returns the bot with the given username, or domain.ErrNotFound.
func (r *BotRepository) GetByUsername(ctx context.Context, username string) (domain.Bot, error) {
	var b domain.Bot
	err := r.gdb.WithContext(ctx).Where("username = ?", username).Take(&b).Error
	if err != nil {
		return domain.Bot{}, notFound(fmt.Sprintf("get bot @%s", username), err, domain.ErrNotFound)
	}
	return b, nil
}

// Remove deletes the bot with the given ID, or domain.ErrNotFound.
func (r *BotRepository) Remove(ctx context.Context, id int64) error {
	res := r.gdb.WithContext(ctx).Where("id = ?", id).Delete(&domain.Bot{})
	return requireAffected(res, fmt.Sprintf("remove bot %d", id), domain.ErrNotFound)
}

// SetUnavailableUntil marks the bot as unavailable until the given time.
func (r *BotRepository) SetUnavailableUntil(ctx context.Context, id int64, until time.Time) error {
	res := r.gdb.WithContext(ctx).
		Model(&domain.Bot{}).
		Where("id = ?", id).
		Update("unavailable_until", until)
	return requireAffected(res, fmt.Sprintf("set bot %d unavailable_until", id), domain.ErrNotFound)
}

// SetActive sets the bot's active flag.
func (r *BotRepository) SetActive(ctx context.Context, id int64, active bool) error {
	res := r.gdb.WithContext(ctx).
		Model(&domain.Bot{}).
		Where("id = ?", id).
		Update("active", active)
	return requireAffected(res, fmt.Sprintf("set bot %d active", id), domain.ErrNotFound)
}
