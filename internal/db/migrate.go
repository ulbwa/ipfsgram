package db

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"strings"

	"github.com/amacneil/dbmate/v2/pkg/dbmate"
	_ "github.com/amacneil/dbmate/v2/pkg/driver/postgres" // register the dbmate postgres driver
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jmoiron/sqlx"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Schema version pinning errors. Both wrap actionable advice for the
// operator, since multiple binaries of different ages share one database.
var (
	// ErrSchemaBehind indicates the database schema is older than the one
	// this binary was built against.
	ErrSchemaBehind = errors.New("database schema is older than this binary expects: run `ipfsgram db migrate`")

	// ErrSchemaAhead indicates the database schema is newer than the one
	// this binary was built against.
	ErrSchemaAhead = errors.New("database schema is newer than this binary expects: update the binary")
)

// Migrate applies all embedded migrations to the database at dsn,
// creating the database first if it does not exist.
func Migrate(dsn string) error {
	u, err := url.Parse(dsn)
	if err != nil {
		return fmt.Errorf("parse dsn: %w", err)
	}

	m := dbmate.New(u)
	m.FS = migrationsFS
	m.MigrationsDir = []string{"migrations"}
	m.AutoDumpSchema = false

	if err := m.CreateAndMigrate(); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}

// BinarySchemaVersion returns the highest migration version embedded in this
// binary, i.e. the schema version the binary expects the database to be at.
func BinarySchemaVersion() string {
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		// The directory is embedded at compile time; failure to read it is a
		// programming error.
		panic(fmt.Sprintf("db: read embedded migrations: %v", err))
	}

	var max string
	for _, e := range entries {
		version, _, ok := strings.Cut(e.Name(), "_")
		if !ok {
			continue
		}
		// Versions are fixed-width numeric timestamps, so string comparison
		// matches numeric ordering.
		if version > max {
			max = version
		}
	}
	return max
}

// CheckSchemaVersion compares the database schema version against the version
// this binary was built with. It returns ErrSchemaBehind if the database
// needs migrating (or has never been migrated), and ErrSchemaAhead if the
// database was migrated by a newer binary.
func CheckSchemaVersion(ctx context.Context, conn *sqlx.DB) error {
	var dbVersion sql.NullString
	err := conn.GetContext(ctx, &dbVersion, `SELECT max(version) FROM schema_migrations`)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" { // undefined_table
			return ErrSchemaBehind
		}
		return fmt.Errorf("query schema_migrations: %w", err)
	}
	if !dbVersion.Valid {
		return ErrSchemaBehind
	}

	binVersion := BinarySchemaVersion()
	switch {
	case dbVersion.String < binVersion:
		return ErrSchemaBehind
	case dbVersion.String > binVersion:
		return ErrSchemaAhead
	default:
		return nil
	}
}
