package gormrepo

import (
	"context"
	"fmt"

	"gorm.io/gorm"

	"github.com/ulbwa/ipfsgram/internal/port"
)

// advisoryLockID is the fixed key serializing publish (shared) against
// garbage-collection (exclusive). It matches the value used by the legacy
// `ipfsgram gc`/`add` advisory-lock protocol so a mixed-version fleet stays
// mutually exclusive during a rollout.
const advisoryLockID = 496818465

// Locker is a GORM-backed port.Locker using PostgreSQL transaction-scoped
// advisory locks.
type Locker struct {
	gdb *gorm.DB
}

var _ port.Locker = (*Locker)(nil)

// NewLocker returns a Locker over gdb.
func NewLocker(gdb *gorm.DB) *Locker {
	return &Locker{gdb: gdb}
}

// WithLock opens a transaction, acquires the advisory lock (exclusive or
// shared), runs fn with a context bound to that transaction, and commits on
// success or rolls back on error. The advisory lock is released automatically
// when the transaction ends.
func (l *Locker) WithLock(ctx context.Context, exclusive bool, fn func(ctx context.Context) error) error {
	return l.gdb.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		stmt := `SELECT pg_advisory_xact_lock_shared(?)`
		if exclusive {
			stmt = `SELECT pg_advisory_xact_lock(?)`
		}
		if err := tx.Exec(stmt, advisoryLockID).Error; err != nil {
			return fmt.Errorf("advisory lock: %w", err)
		}
		return fn(ctx)
	})
}
