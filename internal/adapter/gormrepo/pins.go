package gormrepo

import (
	"context"
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/ulbwa/ipfsgram/internal/domain"
	"github.com/ulbwa/ipfsgram/internal/port"
)

// PinRepository is a GORM-backed port.PinRepository.
type PinRepository struct {
	gdb *gorm.DB
}

var _ port.PinRepository = (*PinRepository)(nil)

// NewPinRepository returns a PinRepository over gdb.
func NewPinRepository(gdb *gorm.DB) *PinRepository {
	return &PinRepository{gdb: gdb}
}

// Create records a pin and its block set in one transaction. Re-creating an
// existing root is idempotent: the pin row is upserted (name/size only, leaving
// created_at intact) and the block set is replaced.
func (r *PinRepository) Create(ctx context.Context, root []byte, name string, size int64, blockCIDs [][]byte) error {
	err := r.gdb.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		pin := domain.Pin{RootCID: root, Name: name, Size: size}
		if err := tx.
			Select("root_cid", "name", "size").
			Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "root_cid"}},
				DoUpdates: clause.AssignmentColumns([]string{"name", "size"}),
			}).
			Create(&pin).Error; err != nil {
			return fmt.Errorf("upsert pin: %w", err)
		}

		if err := tx.Where("root_cid = ?", root).Delete(&domain.PinBlock{}).Error; err != nil {
			return fmt.Errorf("clear pin_blocks: %w", err)
		}

		return chunk(blockCIDs, func(batch [][]byte) error {
			rows := make([]domain.PinBlock, len(batch))
			for i, cid := range batch {
				rows[i] = domain.PinBlock{RootCID: root, CID: cid}
			}
			if err := tx.
				Clauses(clause.OnConflict{DoNothing: true}).
				Create(&rows).Error; err != nil {
				return fmt.Errorf("insert pin_blocks: %w", err)
			}
			return nil
		})
	})
	if err != nil {
		return fmt.Errorf("create pin: %w", err)
	}
	return nil
}

// Remove deletes the pin with the given root CID; pin_blocks rows cascade.
// Returns domain.ErrNotFound if no such pin exists.
func (r *PinRepository) Remove(ctx context.Context, root []byte) error {
	res := r.gdb.WithContext(ctx).Where("root_cid = ?", root).Delete(&domain.Pin{})
	return requireAffected(res, "remove pin", domain.ErrNotFound)
}

// List returns all pins ordered by creation time.
func (r *PinRepository) List(ctx context.Context) ([]domain.Pin, error) {
	var pins []domain.Pin
	err := r.gdb.WithContext(ctx).Order("created_at, root_cid").Find(&pins).Error
	if err != nil {
		return nil, fmt.Errorf("list pins: %w", err)
	}
	return pins, nil
}

// Exists reports whether a pin with the given root CID exists.
func (r *PinRepository) Exists(ctx context.Context, root []byte) (bool, error) {
	var exists bool
	err := r.gdb.WithContext(ctx).
		Raw(`SELECT EXISTS (SELECT 1 FROM pins WHERE root_cid = ?)`, root).
		Scan(&exists).Error
	if err != nil {
		return false, fmt.Errorf("pin exists: %w", err)
	}
	return exists, nil
}

// Count returns the total number of pins.
func (r *PinRepository) Count(ctx context.Context) (int64, error) {
	var n int64
	if err := r.gdb.WithContext(ctx).Model(&domain.Pin{}).Count(&n).Error; err != nil {
		return 0, fmt.Errorf("count pins: %w", err)
	}
	return n, nil
}
