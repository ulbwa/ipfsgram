package repo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jmoiron/sqlx"

	"github.com/ulbwa/ipfsgram/internal/model"
)

// ErrBotExists is returned by BotRepo.Add when a bot with the same token or
// Telegram ID is already registered.
var ErrBotExists = errors.New("repo: bot already exists")

// BotRepo provides access to the bots table.
type BotRepo struct {
	db *sqlx.DB
}

// NewBotRepo returns a BotRepo over db.
func NewBotRepo(db *sqlx.DB) *BotRepo {
	return &BotRepo{db: db}
}

// Add inserts a new bot and returns its ID. It returns ErrBotExists if a bot
// with the same token or Telegram ID already exists.
func (r *BotRepo) Add(ctx context.Context, b model.Bot) (int64, error) {
	var id int64
	err := r.db.GetContext(ctx, &id, `
		INSERT INTO bots (tg_id, username, token, active, unavailable_until)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id`,
		b.TgID, b.Username, b.Token, b.Active, b.UnavailableUntil)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
			return 0, ErrBotExists
		}
		return 0, fmt.Errorf("add bot: %w", err)
	}
	return id, nil
}

// List returns all bots ordered by ID.
func (r *BotRepo) List(ctx context.Context) ([]model.Bot, error) {
	var bots []model.Bot
	err := r.db.SelectContext(ctx, &bots, `
		SELECT id, tg_id, username, token, active, unavailable_until
		FROM bots ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list bots: %w", err)
	}
	return bots, nil
}

// GetByID returns the bot with the given ID, or ErrNotFound.
func (r *BotRepo) GetByID(ctx context.Context, id int64) (model.Bot, error) {
	var b model.Bot
	err := r.db.GetContext(ctx, &b, `
		SELECT id, tg_id, username, token, active, unavailable_until
		FROM bots WHERE id = $1`, id)
	if err != nil {
		return model.Bot{}, notFound(fmt.Sprintf("get bot %d", id), err)
	}
	return b, nil
}

// Remove deletes the bot with the given ID. It returns ErrNotFound if the bot
// does not exist.
func (r *BotRepo) Remove(ctx context.Context, id int64) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM bots WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("remove bot %d: %w", id, err)
	}
	return requireAffected(res, fmt.Sprintf("remove bot %d", id))
}

// SetUnavailableUntil marks the bot as unavailable until the given time.
func (r *BotRepo) SetUnavailableUntil(ctx context.Context, id int64, until time.Time) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE bots SET unavailable_until = $2 WHERE id = $1`, id, until)
	if err != nil {
		return fmt.Errorf("set bot %d unavailable_until: %w", id, err)
	}
	return requireAffected(res, fmt.Sprintf("set bot %d unavailable_until", id))
}

// SetActive sets the bot's active flag.
func (r *BotRepo) SetActive(ctx context.Context, id int64, active bool) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE bots SET active = $2 WHERE id = $1`, id, active)
	if err != nil {
		return fmt.Errorf("set bot %d active: %w", id, err)
	}
	return requireAffected(res, fmt.Sprintf("set bot %d active", id))
}
