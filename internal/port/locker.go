package port

import "context"

// Locker holds a PostgreSQL advisory lock for the duration of a callback. It
// serializes the publish (shared) and garbage-collection (exclusive) critical
// sections against each other: parallel publishers may hold the shared lock
// concurrently, while gc takes the exclusive lock and is thus mutually excluded
// with every publisher.
type Locker interface {
	// WithLock holds a Postgres advisory lock for fn's duration. When exclusive
	// is true it uses pg_advisory_xact_lock; otherwise pg_advisory_xact_lock_shared.
	// The lock is transaction-scoped: it is acquired inside a dedicated
	// transaction, held while fn runs, and released when that transaction ends.
	WithLock(ctx context.Context, exclusive bool, fn func(ctx context.Context) error) error
}
