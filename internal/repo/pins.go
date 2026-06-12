package repo

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/ulbwa/ipfsgram/internal/model"
)

// PinRepo provides access to the pins and pin_blocks tables.
type PinRepo struct {
	db *sqlx.DB
}

// NewPinRepo returns a PinRepo over db.
func NewPinRepo(db *sqlx.DB) *PinRepo {
	return &PinRepo{db: db}
}

// Create records a pin and its block set in one transaction. Re-creating an
// existing root is idempotent: the pin row is upserted and the block set is
// replaced.
func (r *PinRepo) Create(ctx context.Context, root []byte, name string, size int64, blockCIDs [][]byte) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("create pin: begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit

	_, err = tx.ExecContext(ctx, `
		INSERT INTO pins (root_cid, name, size)
		VALUES ($1, $2, $3)
		ON CONFLICT (root_cid) DO UPDATE SET
			name = EXCLUDED.name,
			size = EXCLUDED.size`,
		root, name, size)
	if err != nil {
		return fmt.Errorf("create pin: upsert pin: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM pin_blocks WHERE root_cid = $1`, root); err != nil {
		return fmt.Errorf("create pin: clear pin_blocks: %w", err)
	}

	err = chunk(blockCIDs, func(batch [][]byte) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO pin_blocks (root_cid, cid)
			SELECT $1, c FROM unnest($2::bytea[]) AS u(c)
			ON CONFLICT (root_cid, cid) DO NOTHING`,
			root, batch)
		if err != nil {
			return fmt.Errorf("create pin: insert pin_blocks: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("create pin: commit: %w", err)
	}
	return nil
}

// Remove deletes the pin with the given root CID; pin_blocks rows cascade.
// It returns ErrNotFound if no such pin exists.
func (r *PinRepo) Remove(ctx context.Context, root []byte) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM pins WHERE root_cid = $1`, root)
	if err != nil {
		return fmt.Errorf("remove pin: %w", err)
	}
	return requireAffected(res, "remove pin")
}

// List returns all pins ordered by creation time.
func (r *PinRepo) List(ctx context.Context) ([]model.Pin, error) {
	var pins []model.Pin
	err := r.db.SelectContext(ctx, &pins, `
		SELECT root_cid, name, size, created_at
		FROM pins ORDER BY created_at, root_cid`)
	if err != nil {
		return nil, fmt.Errorf("list pins: %w", err)
	}
	return pins, nil
}

// Exists reports whether a pin with the given root CID exists.
func (r *PinRepo) Exists(ctx context.Context, root []byte) (bool, error) {
	var exists bool
	err := r.db.GetContext(ctx, &exists,
		`SELECT EXISTS (SELECT 1 FROM pins WHERE root_cid = $1)`, root)
	if err != nil {
		return false, fmt.Errorf("pin exists: %w", err)
	}
	return exists, nil
}
