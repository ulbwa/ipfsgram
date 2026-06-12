// app.go — shared command wiring: DSN resolution, database connection with the
// schema-version check, the Store, and the Telegram transport assembled from
// the database config.

package main

import (
	"context"
	"errors"
	"os"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"gorm.io/gorm"

	"github.com/ulbwa/ipfsgram/db"
	"github.com/ulbwa/ipfsgram/internal/store"
	"github.com/ulbwa/ipfsgram/internal/telegram"
)

// errNoDSN is returned when neither --dsn nor IPFSGRAM_DSN is provided.
var errNoDSN = errors.New("set --dsn or IPFSGRAM_DSN")

// resolveDSN returns the PostgreSQL DSN: the --dsn persistent flag if set,
// otherwise the IPFSGRAM_DSN environment variable.
func resolveDSN(cmd *cobra.Command) string {
	if dsn, _ := cmd.Flags().GetString("dsn"); dsn != "" {
		return dsn
	}
	return os.Getenv(dsnEnv)
}

// app bundles the database handle, the Store and the Telegram transport that
// the commands wire into the workflow packages. The only place that touches a
// *gorm.DB is this wiring; commands use the Store exclusively.
type app struct {
	db *gorm.DB

	Store     *store.Store
	Transport *telegram.Client
}

// openStore resolves the DSN, connects to PostgreSQL, verifies the schema
// version, and returns the gorm handle plus the Store over it. The caller must
// close the handle via closeDB (or app.Close).
func openStore(ctx context.Context, cmd *cobra.Command) (*gorm.DB, *store.Store, error) {
	dsn := resolveDSN(cmd)
	if dsn == "" {
		return nil, nil, errNoDSN
	}
	gdb, err := db.Connect(ctx, dsn)
	if err != nil {
		return nil, nil, err
	}
	if err := db.CheckSchemaVersion(ctx, gdb); err != nil {
		closeDB(gdb)
		return nil, nil, err
	}
	return gdb, store.New(gdb), nil
}

// openApp opens the store and constructs the Telegram transport from the
// database config. sessionDir scopes the MTProto session storage (empty for
// the CLI). The caller must Close the returned app.
func openApp(ctx context.Context, cmd *cobra.Command, sessionDir string) (*app, error) {
	gdb, st, err := openStore(ctx, cmd)
	if err != nil {
		return nil, err
	}

	a := &app{db: gdb, Store: st}
	tr, err := a.buildTransport(ctx, sessionDir)
	if err != nil {
		closeDB(gdb)
		return nil, err
	}
	a.Transport = tr
	return a, nil
}

// buildTransport assembles the Telegram transport from config: bot_api_url plus,
// when mtproto_enabled, the active MTProto credentials.
func (a *app) buildTransport(ctx context.Context, sessionDir string) (*telegram.Client, error) {
	apiURL, err := a.Store.ConfigValue(ctx, "bot_api_url")
	if errors.Is(err, store.ErrNotFound) {
		apiURL = "https://api.telegram.org"
	} else if err != nil {
		return nil, err
	}

	enabled, err := a.Store.ConfigBool(ctx, "mtproto_enabled")
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}

	apiID, apiHash := 0, ""
	if enabled {
		creds, err := a.Store.ActiveMTProtoCreds(ctx)
		if err != nil {
			return nil, err
		}
		if creds == nil {
			log.Warn().Msg("mtproto_enabled is true but no active credentials exist, falling back to Bot API only")
		} else {
			apiID, apiHash = creds.APIID, creds.APIHash
		}
	}
	return telegram.New(apiURL, apiID, apiHash, sessionDir), nil
}

// Close releases the database connection pool.
func (a *app) Close() { closeDB(a.db) }

// closeDB closes the underlying *sql.DB behind a gorm handle, logging any error.
func closeDB(gdb *gorm.DB) {
	sqlDB, err := gdb.DB()
	if err != nil {
		log.Warn().Err(err).Msg("could not access sql.DB for close")
		return
	}
	if err := sqlDB.Close(); err != nil {
		log.Warn().Err(err).Msg("could not close database")
	}
}
