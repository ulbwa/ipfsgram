package gormrepo

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"

	"github.com/ulbwa/ipfsgram/internal/domain"
	"github.com/ulbwa/ipfsgram/internal/port"
)

// MTProtoRepository is a GORM-backed port.MTProtoRepository.
type MTProtoRepository struct {
	gdb *gorm.DB
}

var _ port.MTProtoRepository = (*MTProtoRepository)(nil)

// NewMTProtoRepository returns an MTProtoRepository over gdb.
func NewMTProtoRepository(gdb *gorm.DB) *MTProtoRepository {
	return &MTProtoRepository{gdb: gdb}
}

// Latest returns the most recently created credentials, or (nil, nil) if none
// exist.
func (r *MTProtoRepository) Latest(ctx context.Context) (*domain.MTProtoCreds, error) {
	var c domain.MTProtoCreds
	err := r.gdb.WithContext(ctx).
		Order("created_at DESC, id DESC").
		Take(&c).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("latest mtproto credentials: %w", err)
	}
	return &c, nil
}

// Active returns the active credentials, or (nil, nil) if none are active.
func (r *MTProtoRepository) Active(ctx context.Context) (*domain.MTProtoCreds, error) {
	var c domain.MTProtoCreds
	err := r.gdb.WithContext(ctx).Where("active").Take(&c).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("active mtproto credentials: %w", err)
	}
	return &c, nil
}

// Activate makes the given credentials the single active pair: it deactivates
// all rows, then reactivates the newest row with the same api_id+api_hash or
// inserts a new active row, all in one transaction. This respects the partial
// unique index that allows only one active row.
func (r *MTProtoRepository) Activate(ctx context.Context, apiID int, apiHash string) error {
	err := r.gdb.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&domain.MTProtoCreds{}).
			Where("active").
			Update("active", false).Error; err != nil {
			return fmt.Errorf("deactivate: %w", err)
		}

		res := tx.Exec(`
			UPDATE mtproto_credentials SET active = true
			WHERE id = (
				SELECT id FROM mtproto_credentials
				WHERE api_id = ? AND api_hash = ?
				ORDER BY created_at DESC, id DESC
				LIMIT 1
			)`, apiID, apiHash)
		if res.Error != nil {
			return fmt.Errorf("reactivate: %w", res.Error)
		}

		if res.RowsAffected == 0 {
			creds := domain.MTProtoCreds{APIID: apiID, APIHash: apiHash, Active: true}
			if err := tx.
				Select("api_id", "api_hash", "active").
				Create(&creds).Error; err != nil {
				return fmt.Errorf("insert: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("activate mtproto credentials: %w", err)
	}
	return nil
}

// Deactivate clears the active flag on all credentials.
func (r *MTProtoRepository) Deactivate(ctx context.Context) error {
	err := r.gdb.WithContext(ctx).
		Model(&domain.MTProtoCreds{}).
		Where("active").
		Update("active", false).Error
	if err != nil {
		return fmt.Errorf("deactivate mtproto credentials: %w", err)
	}
	return nil
}
