// Package repo contains thin sqlx-based data-access repositories over the
// shared IPFSgram PostgreSQL database. Repositories carry no business logic:
// they translate between domain types in internal/model and SQL.
package repo

import (
	"database/sql"
	"errors"
	"fmt"
)

// ErrNotFound is returned when a requested row does not exist.
var ErrNotFound = errors.New("repo: not found")

// maxBatch caps the number of rows sent in a single batched statement.
const maxBatch = 1000

// notFound maps sql.ErrNoRows to ErrNotFound, wrapping any other error with
// the given operation description.
func notFound(op string, err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	return fmt.Errorf("%s: %w", op, err)
}

// requireAffected returns ErrNotFound when the statement touched no rows.
func requireAffected(res sql.Result, op string) error {
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: rows affected: %w", op, err)
	}
	if n == 0 {
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
