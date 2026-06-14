// Store methods for the mtproto_credentials table: credential history plus
// the single-active-row activation protocol.
package store

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"
)

// LatestMTProtoCreds returns the most recently created credentials, or
// (nil, nil) if none exist.
func (s *Store) LatestMTProtoCreds(ctx context.Context) (*MTProtoCreds, error) {
	var c MTProtoCreds
	err := s.db.WithContext(ctx).
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

// ActiveMTProtoCreds returns the active credentials, or (nil, nil) if none are
// active.
func (s *Store) ActiveMTProtoCreds(ctx context.Context) (*MTProtoCreds, error) {
	var c MTProtoCreds
	err := s.db.WithContext(ctx).Where("active").Take(&c).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("active mtproto credentials: %w", err)
	}
	return &c, nil
}

// ActivateMTProto makes the given credentials the single active pair: it
// deactivates all rows, then reactivates the newest row with the same
// api_id+api_hash or inserts a new active row, all in one transaction. This
// respects the partial unique index that allows only one active row.
func (s *Store) ActivateMTProto(ctx context.Context, apiID int, apiHash string) error {
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&MTProtoCreds{}).
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
			creds := MTProtoCreds{APIID: apiID, APIHash: apiHash, Active: true}
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

// DeactivateMTProto clears the active flag on all credentials.
func (s *Store) DeactivateMTProto(ctx context.Context) error {
	err := s.db.WithContext(ctx).
		Model(&MTProtoCreds{}).
		Where("active").
		Update("active", false).Error
	if err != nil {
		return fmt.Errorf("deactivate mtproto credentials: %w", err)
	}
	return nil
}
