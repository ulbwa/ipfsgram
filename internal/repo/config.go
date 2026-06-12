package repo

import (
	"context"
	"fmt"
	"strconv"

	"github.com/jmoiron/sqlx"
)

// ConfigRepo provides access to the key/value config table.
type ConfigRepo struct {
	db *sqlx.DB
}

// NewConfigRepo returns a ConfigRepo over db.
func NewConfigRepo(db *sqlx.DB) *ConfigRepo {
	return &ConfigRepo{db: db}
}

// Get returns the value for key, or ErrNotFound if the key does not exist.
func (r *ConfigRepo) Get(ctx context.Context, key string) (string, error) {
	var value string
	err := r.db.GetContext(ctx, &value, `SELECT value FROM config WHERE key = $1`, key)
	if err != nil {
		return "", notFound(fmt.Sprintf("get config %q", key), err)
	}
	return value, nil
}

// GetInt64 returns the value for key parsed as int64.
func (r *ConfigRepo) GetInt64(ctx context.Context, key string) (int64, error) {
	raw, err := r.Get(ctx, key)
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse config %q as int64: %w", key, err)
	}
	return v, nil
}

// GetFloat64 returns the value for key parsed as float64.
func (r *ConfigRepo) GetFloat64(ctx context.Context, key string) (float64, error) {
	raw, err := r.Get(ctx, key)
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("parse config %q as float64: %w", key, err)
	}
	return v, nil
}

// GetBool returns the value for key parsed as bool.
func (r *ConfigRepo) GetBool(ctx context.Context, key string) (bool, error) {
	raw, err := r.Get(ctx, key)
	if err != nil {
		return false, err
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("parse config %q as bool: %w", key, err)
	}
	return v, nil
}

// Set upserts the value for key.
func (r *ConfigRepo) Set(ctx context.Context, key, value string) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO config (key, value) VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
		key, value)
	if err != nil {
		return fmt.Errorf("set config %q: %w", key, err)
	}
	return nil
}
