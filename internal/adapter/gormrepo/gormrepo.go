// Package gormrepo holds GORM-backed implementations of the repository ports
// defined in internal/port. Each repository is a thin data-access type carrying
// no business logic: it translates between the domain models in
// internal/domain and PostgreSQL via GORM. Errors are mapped to the domain
// sentinels (domain.ErrNotFound, domain.ErrBotExists) so the service layer
// never sees infrastructure-specific error types.
package gormrepo

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
)

// maxBatch caps the number of rows sent in a single batched statement, matching
// the streaming page size used by StreamAllCIDs.
const maxBatch = 1000

// notFound maps gorm.ErrRecordNotFound to domain.ErrNotFound, wrapping any
// other error with the given operation description.
func notFound(op string, err error, notFoundErr error) error {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return notFoundErr
	}
	return fmt.Errorf("%s: %w", op, err)
}

// isUniqueViolation reports whether err is a PostgreSQL unique_violation
// (SQLSTATE 23505).
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// requireAffected returns notFoundErr when the statement touched no rows.
func requireAffected(res *gorm.DB, op string, notFoundErr error) error {
	if res.Error != nil {
		return fmt.Errorf("%s: %w", op, res.Error)
	}
	if res.RowsAffected == 0 {
		return notFoundErr
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
