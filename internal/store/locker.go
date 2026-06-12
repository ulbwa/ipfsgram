// PostgreSQL transaction-scoped advisory locking serializing publish (shared)
// against garbage collection (exclusive).
package store

import (
	"context"
	"fmt"

	"gorm.io/gorm"
)

// advisoryLockID is the fixed key serializing publish (shared) against
// garbage-collection (exclusive). It matches the value used by the legacy
// `ipfsgram gc`/`add` advisory-lock protocol so a mixed-version fleet stays
// mutually exclusive during a rollout.
const advisoryLockID = 496818465

// WithSharedPublishLock holds the publish/gc advisory lock in shared mode for
// fn's duration: parallel publishers may hold it concurrently, while gc is
// excluded. The lock is transaction-scoped — acquired inside a dedicated
// transaction, held while fn runs, and released when that transaction ends.
func (s *Store) WithSharedPublishLock(ctx context.Context, fn func(ctx context.Context) error) error {
	return s.withAdvisoryLock(ctx, false, fn)
}

// WithExclusiveGCLock holds the publish/gc advisory lock in exclusive mode for
// fn's duration, mutually excluding gc from every publisher. The lock is
// transaction-scoped, exactly as in WithSharedPublishLock.
func (s *Store) WithExclusiveGCLock(ctx context.Context, fn func(ctx context.Context) error) error {
	return s.withAdvisoryLock(ctx, true, fn)
}

// withAdvisoryLock opens a transaction, acquires the advisory lock (exclusive
// or shared), runs fn with the caller's context, and commits on success or
// rolls back on error. The advisory lock is released automatically when the
// transaction ends.
func (s *Store) withAdvisoryLock(ctx context.Context, exclusive bool, fn func(ctx context.Context) error) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
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
