// Store methods for the key/value config table, with typed getters that parse
// the stored string values.
package store

import (
	"context"
	"fmt"
	"strconv"

	"gorm.io/gorm/clause"
)

// configRow maps the config key/value table.
type configRow struct {
	Key   string `gorm:"column:key;primaryKey"`
	Value string `gorm:"column:value"`
}

func (configRow) TableName() string { return "config" }

// ConfigValue returns the value for key, or ErrNotFound if the key is absent.
func (s *Store) ConfigValue(ctx context.Context, key string) (string, error) {
	var row configRow
	err := s.db.WithContext(ctx).
		Select("value").
		Where("key = ?", key).
		Take(&row).Error
	if err != nil {
		return "", notFound(fmt.Sprintf("get config %q", key), err)
	}
	return row.Value, nil
}

// ConfigInt64 returns the value for key parsed as int64.
func (s *Store) ConfigInt64(ctx context.Context, key string) (int64, error) {
	raw, err := s.ConfigValue(ctx, key)
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse config %q as int64: %w", key, err)
	}
	return v, nil
}

// ConfigFloat64 returns the value for key parsed as float64.
func (s *Store) ConfigFloat64(ctx context.Context, key string) (float64, error) {
	raw, err := s.ConfigValue(ctx, key)
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("parse config %q as float64: %w", key, err)
	}
	return v, nil
}

// ConfigBool returns the value for key parsed as bool.
func (s *Store) ConfigBool(ctx context.Context, key string) (bool, error) {
	raw, err := s.ConfigValue(ctx, key)
	if err != nil {
		return false, err
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("parse config %q as bool: %w", key, err)
	}
	return v, nil
}

// SetConfig upserts the value for key.
func (s *Store) SetConfig(ctx context.Context, key, value string) error {
	err := s.db.WithContext(ctx).
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
