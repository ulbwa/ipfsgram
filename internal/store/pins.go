// Store methods for the pins and pin_blocks tables, including the orphan-pin
// cleanup that runs after cascading deletions remove a pin's last blocks.
package store

import (
	"context"
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// CreatePin records a pin and its block set in one transaction. Re-creating an
// existing root is idempotent: the pin row is upserted (name/size only, leaving
// created_at intact) and the block set is replaced.
func (s *Store) CreatePin(ctx context.Context, root []byte, name string, size int64, blockCIDs [][]byte) error {
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		pin := Pin{RootCID: root, Name: name, Size: size}
		if err := tx.
			Select("root_cid", "name", "size").
			Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "root_cid"}},
				DoUpdates: clause.AssignmentColumns([]string{"name", "size"}),
			}).
			Create(&pin).Error; err != nil {
			return fmt.Errorf("upsert pin: %w", err)
		}

		if err := tx.Where("root_cid = ?", root).Delete(&PinBlock{}).Error; err != nil {
			return fmt.Errorf("clear pin_blocks: %w", err)
		}

		return chunk(blockCIDs, func(batch [][]byte) error {
			rows := make([]PinBlock, len(batch))
			for i, cid := range batch {
				rows[i] = PinBlock{RootCID: root, CID: cid}
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

// RemovePin deletes the pin with the given root CID; pin_blocks rows cascade.
// Returns ErrNotFound if no such pin exists.
func (s *Store) RemovePin(ctx context.Context, root []byte) error {
	res := s.db.WithContext(ctx).Where("root_cid = ?", root).Delete(&Pin{})
	return requireAffected(res, "remove pin")
}

// Pins returns all pins ordered by creation time.
func (s *Store) Pins(ctx context.Context) ([]Pin, error) {
	var pins []Pin
	err := s.db.WithContext(ctx).Order("created_at, root_cid").Find(&pins).Error
	if err != nil {
		return nil, fmt.Errorf("list pins: %w", err)
	}
	return pins, nil
}

// PinExists reports whether a pin with the given root CID exists.
func (s *Store) PinExists(ctx context.Context, root []byte) (bool, error) {
	var exists bool
	err := s.db.WithContext(ctx).
		Raw(`SELECT EXISTS (SELECT 1 FROM pins WHERE root_cid = ?)`, root).
		Scan(&exists).Error
	if err != nil {
		return false, fmt.Errorf("pin exists: %w", err)
	}
	return exists, nil
}

// CountPins returns the total number of pins.
func (s *Store) CountPins(ctx context.Context) (int64, error) {
	var n int64
	if err := s.db.WithContext(ctx).Model(&Pin{}).Count(&n).Error; err != nil {
		return 0, fmt.Errorf("count pins: %w", err)
	}
	return n, nil
}

// DeleteOrphanPins deletes pins none of whose blocks still exist — e.g. after a
// channel removal cascades away its cars/blocks. A pin is orphaned when no
// pin_blocks row references a still-present block. Returns the number deleted.
func (s *Store) DeleteOrphanPins(ctx context.Context) (int64, error) {
	res := s.db.WithContext(ctx).Exec(`
		DELETE FROM pins p WHERE NOT EXISTS (
			SELECT 1 FROM pin_blocks pb
			JOIN blocks b ON b.cid = pb.cid
			WHERE pb.root_cid = p.root_cid
		)`)
	if res.Error != nil {
		return 0, fmt.Errorf("delete orphan pins: %w", res.Error)
	}
	return res.RowsAffected, nil
}
