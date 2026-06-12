package gormrepo

import (
	"context"
	"fmt"
	"strconv"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/ulbwa/ipfsgram/internal/domain"
	"github.com/ulbwa/ipfsgram/internal/port"
)

// configRow maps the config key/value table.
type configRow struct {
	Key   string `gorm:"column:key;primaryKey"`
	Value string `gorm:"column:value"`
}

func (configRow) TableName() string { return "config" }

// ConfigRepository is a GORM-backed port.ConfigRepository.
type ConfigRepository struct {
	gdb *gorm.DB
}

var _ port.ConfigRepository = (*ConfigRepository)(nil)

// NewConfigRepository returns a ConfigRepository over gdb.
func NewConfigRepository(gdb *gorm.DB) *ConfigRepository {
	return &ConfigRepository{gdb: gdb}
}

// Get returns the value for key, or domain.ErrNotFound if the key is absent.
func (r *ConfigRepository) Get(ctx context.Context, key string) (string, error) {
	var row configRow
	err := r.gdb.WithContext(ctx).
		Select("value").
		Where("key = ?", key).
		Take(&row).Error
	if err != nil {
		return "", notFound(fmt.Sprintf("get config %q", key), err, domain.ErrNotFound)
	}
	return row.Value, nil
}

// GetInt64 returns the value for key parsed as int64.
func (r *ConfigRepository) GetInt64(ctx context.Context, key string) (int64, error) {
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
func (r *ConfigRepository) GetFloat64(ctx context.Context, key string) (float64, error) {
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
func (r *ConfigRepository) GetBool(ctx context.Context, key string) (bool, error) {
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
func (r *ConfigRepository) Set(ctx context.Context, key, value string) error {
	err := r.gdb.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "key"}},
			DoUpdates: clause.AssignmentColumns([]string{"value"}),
		}).
		Create(&configRow{Key: key, Value: value}).Error
	if err != nil {
		return fmt.Errorf("set config %q: %w", key, err)
	}
	return nil
}
