package repo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/ulbwa/ipfsgram/internal/model"
)

// MTProtoCredsRepo provides access to the mtproto_credentials table.
type MTProtoCredsRepo struct {
	db *sqlx.DB
}

// NewMTProtoCredsRepo returns an MTProtoCredsRepo over db.
func NewMTProtoCredsRepo(db *sqlx.DB) *MTProtoCredsRepo {
	return &MTProtoCredsRepo{db: db}
}

// Latest returns the most recently created credentials, or (nil, nil) if
// none exist.
func (r *MTProtoCredsRepo) Latest(ctx context.Context) (*model.MTProtoCreds, error) {
	var c model.MTProtoCreds
	err := r.db.GetContext(ctx, &c, `
		SELECT id, api_id, api_hash, active, created_at
		FROM mtproto_credentials
		ORDER BY created_at DESC, id DESC
		LIMIT 1`)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("latest mtproto credentials: %w", err)
	}
	return &c, nil
}

// Active returns the active credentials, or (nil, nil) if none are active.
func (r *MTProtoCredsRepo) Active(ctx context.Context) (*model.MTProtoCreds, error) {
	var c model.MTProtoCreds
	err := r.db.GetContext(ctx, &c, `
		SELECT id, api_id, api_hash, active, created_at
		FROM mtproto_credentials
		WHERE active`)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("active mtproto credentials: %w", err)
	}
	return &c, nil
}

// Activate makes the given credentials the single active pair: it deactivates
// all existing rows, then reactivates an existing row with the same
// api_id+api_hash or inserts a new one, all in one transaction.
func (r *MTProtoCredsRepo) Activate(ctx context.Context, apiID int, apiHash string) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("activate mtproto credentials: begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit

	if _, err := tx.ExecContext(ctx,
		`UPDATE mtproto_credentials SET active = false WHERE active`); err != nil {
		return fmt.Errorf("activate mtproto credentials: deactivate: %w", err)
	}

	res, err := tx.ExecContext(ctx, `
		UPDATE mtproto_credentials SET active = true
		WHERE id = (
			SELECT id FROM mtproto_credentials
			WHERE api_id = $1 AND api_hash = $2
			ORDER BY created_at DESC, id DESC
			LIMIT 1
		)`,
		apiID, apiHash)
	if err != nil {
		return fmt.Errorf("activate mtproto credentials: reactivate: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("activate mtproto credentials: rows affected: %w", err)
	}
	if n == 0 {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO mtproto_credentials (api_id, api_hash, active)
			VALUES ($1, $2, true)`,
			apiID, apiHash)
		if err != nil {
			return fmt.Errorf("activate mtproto credentials: insert: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("activate mtproto credentials: commit: %w", err)
	}
	return nil
}

// Deactivate clears the active flag on all credentials.
func (r *MTProtoCredsRepo) Deactivate(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE mtproto_credentials SET active = false WHERE active`)
	if err != nil {
		return fmt.Errorf("deactivate mtproto credentials: %w", err)
	}
	return nil
}
