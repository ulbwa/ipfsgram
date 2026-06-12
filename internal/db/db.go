// Package db provides the shared PostgreSQL connection and embedded schema
// migrations for IPFSgram. PostgreSQL is the single coordination point shared
// by all daemons and CLIs, so every binary pins the schema version it was
// built against (see CheckSchemaVersion).
package db

import (
	"context"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib" // register the pgx database/sql driver
	"github.com/jmoiron/sqlx"
)

// Connect opens a PostgreSQL connection pool for the given DSN and verifies
// it with a ping.
func Connect(ctx context.Context, dsn string) (*sqlx.DB, error) {
	conn, err := sqlx.ConnectContext(ctx, "pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	conn.SetMaxOpenConns(10)
	return conn, nil
}
