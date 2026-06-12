// Package store is IPFSgram's data-access layer: the GORM models for the
// PostgreSQL schema and a single Store type whose methods (one file per
// entity) carry all SQL/GORM queries. It maps infrastructure errors to the
// package sentinels ErrNotFound and ErrBotExists.
package store

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
)

// Package sentinels.
var (
	// ErrNotFound is returned when a requested row does not exist.
	ErrNotFound = errors.New("store: not found")

	// ErrBotExists is returned when a bot with the same token or Telegram ID is
	// already registered.
	ErrBotExists = errors.New("store: bot already exists")
)

// Store provides all database access over a single *gorm.DB. Construct it
// with New; the connection comes from db.Connect.
type Store struct {
	db *gorm.DB
}

// New returns a Store over db.
func New(db *gorm.DB) *Store {
	return &Store{db: db}
}

// maxBatch caps the number of rows sent in a single batched statement, matching
// the streaming page size used by StreamAllCIDs.
const maxBatch = 1000

// notFound maps gorm.ErrRecordNotFound to ErrNotFound, wrapping any other
// error with the given operation description.
func notFound(op string, err error) error {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrNotFound
	}
	return fmt.Errorf("%s: %w", op, err)
}

// isUniqueViolation reports whether err is a PostgreSQL unique_violation
// (SQLSTATE 23505).
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// requireAffected returns ErrNotFound when the statement touched no rows.
func requireAffected(res *gorm.DB, op string) error {
	if res.Error != nil {
		return fmt.Errorf("%s: %w", op, res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

// chunk calls fn for consecutive sub-slices of items of at most maxBatch
// elements.
func chunk[T any](items []T, fn func(batch []T) error) error {
	for len(items) > 0 {
		n := min(len(items), maxBatch)
		if err := fn(items[:n]); err != nil {
			return err
		}
		items = items[n:]
	}
	return nil
}
