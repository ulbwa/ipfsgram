// Package db holds the IPFSgram PostgreSQL bootstrap: the embedded dbmate
// migrations (the only place SQL files live — /internal is code-only), the
// GORM connection helper, and the schema-version pinning logic.
//
// PostgreSQL is the single coordination point shared by all daemons and CLIs,
// so every binary pins the schema version it was built against (see
// CheckSchemaVersion) and refuses to run against an out-of-date or too-new
// database.
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
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// maxOpenConns caps the pool size so many daemons/CLIs sharing one PostgreSQL
// instance do not exhaust its connection limit.
const maxOpenConns = 10

// Schema version pinning errors. Both wrap actionable advice for the operator,
// since multiple binaries of different ages share one database.
var (
	// ErrSchemaBehind indicates the database schema is older than the one this
	// binary was built against.
	ErrSchemaBehind = errors.New("database schema is out of date; run `ipfsgram db migrate`")

	// ErrSchemaAhead indicates the database schema is newer than the one this
	// binary was built against.
	ErrSchemaAhead = errors.New("database schema is newer than this binary; update the binary")
)

// Connect opens a GORM connection to the PostgreSQL database at dsn and
// configures the underlying connection pool. The GORM logger is kept at Warn
// level to avoid noisy per-statement logging.
func Connect(ctx context.Context, dsn string) (*gorm.DB, error) {
	gdb, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Warn),
	})
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	sqlDB, err := gdb.DB()
	if err != nil {
		return nil, fmt.Errorf("access sql.DB: %w", err)
	}
	sqlDB.SetMaxOpenConns(maxOpenConns)
	return gdb, nil
}

// Migrate applies all embedded migrations to the database at dsn, creating the
// database first if it does not exist.
func Migrate(dsn string) error {
	u, err := url.Parse(dsn)
	if err != nil {
		// url.Parse embeds the raw DSN (including the password) in its error
		// text; surface only the underlying reason so the connection string
		// never reaches logs.
		var ue *url.Error
		if errors.As(err, &ue) {
			return fmt.Errorf("parse dsn: %w", ue.Err)
		}
		return errors.New("parse dsn: invalid connection string")
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
// this binary was built with. It returns ErrSchemaBehind if the database needs
// migrating (or has never been migrated), and ErrSchemaAhead if the database
// was migrated by a newer binary.
func CheckSchemaVersion(ctx context.Context, gdb *gorm.DB) error {
	var dbVersion sql.NullString
	err := gdb.WithContext(ctx).
		Raw(`SELECT max(version) FROM schema_migrations`).
		Scan(&dbVersion).Error
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
