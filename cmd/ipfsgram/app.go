package main

import (
	"context"
	"errors"
	"os"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"gorm.io/gorm"

	"github.com/ulbwa/ipfsgram/db"
	"github.com/ulbwa/ipfsgram/internal/adapter/carpack"
	"github.com/ulbwa/ipfsgram/internal/adapter/gormrepo"
	"github.com/ulbwa/ipfsgram/internal/adapter/selector"
	"github.com/ulbwa/ipfsgram/internal/adapter/telegram"
	"github.com/ulbwa/ipfsgram/internal/domain"
	"github.com/ulbwa/ipfsgram/internal/port"
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

// app bundles the database handle, every repository, the Telegram transport and
// the stateless helpers (selector, packer factory, block reader) that the
// commands wire into the service layer. The only place that touches a *gorm.DB
// is this wiring; commands use the ports exclusively.
type app struct {
	db *gorm.DB

	Config   port.ConfigRepository
	Bots     port.BotRepository
	Channels port.ChannelRepository
	Cars     port.CarRepository
	Blocks   port.BlockRepository
	Pins     port.PinRepository
	MTProto  port.MTProtoRepository
	Locker   port.Locker

	Transport port.Transport
	Selector  port.Selector
	Packer    port.PackerFactory
	Reader    port.BlockReader
}

// openApp resolves the DSN, connects to PostgreSQL, verifies the schema version,
// and constructs every repository and adapter. sessionDir scopes the MTProto
// session storage (empty for the CLI, a daemon data subdir for the daemon). The
// caller must Close the returned app.
func openApp(ctx context.Context, cmd *cobra.Command, sessionDir string) (*app, error) {
	dsn := resolveDSN(cmd)
	if dsn == "" {
		return nil, errNoDSN
	}
	gdb, err := db.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := db.CheckSchemaVersion(ctx, gdb); err != nil {
		closeDB(gdb)
		return nil, err
	}

	a := &app{
		db:       gdb,
		Config:   gormrepo.NewConfigRepository(gdb),
		Bots:     gormrepo.NewBotRepository(gdb),
		Channels: gormrepo.NewChannelRepository(gdb),
		Cars:     gormrepo.NewCarRepository(gdb),
		Blocks:   gormrepo.NewBlockRepository(gdb),
		Pins:     gormrepo.NewPinRepository(gdb),
		MTProto:  gormrepo.NewMTProtoRepository(gdb),
		Locker:   gormrepo.NewLocker(gdb),
		Selector: selector.New(),
		Packer:   carpack.NewFactory(),
		Reader:   carpack.NewReader(),
	}

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
func (a *app) buildTransport(ctx context.Context, sessionDir string) (port.Transport, error) {
	apiURL, err := a.Config.Get(ctx, "bot_api_url")
	if errors.Is(err, domain.ErrNotFound) {
		apiURL = "https://api.telegram.org"
	} else if err != nil {
		return nil, err
	}

	enabled, err := a.Config.GetBool(ctx, "mtproto_enabled")
	if err != nil && !errors.Is(err, domain.ErrNotFound) {
		return nil, err
	}

	var creds *domain.MTProtoCreds
	if enabled {
		creds, err = a.MTProto.Active(ctx)
		if err != nil {
			return nil, err
		}
		if creds == nil {
			log.Warn().Msg("mtproto_enabled is true but no active credentials exist, falling back to Bot API only")
		}
	}
	return telegram.New(apiURL, creds, sessionDir), nil
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
