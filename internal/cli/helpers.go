package cli

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/spf13/cobra"

	"github.com/ulbwa/ipfsgram/internal/db"
	"github.com/ulbwa/ipfsgram/internal/model"
	"github.com/ulbwa/ipfsgram/internal/repo"
	"github.com/ulbwa/ipfsgram/internal/tg"
)

// errNoDSN is returned when neither --dsn nor IPFSGRAM_DSN is provided.
var errNoDSN = errors.New("set --dsn or IPFSGRAM_DSN")

// env bundles the database connection and repositories every command needs.
type env struct {
	db       *sqlx.DB
	Bots     *repo.BotRepo
	Channels *repo.ChannelRepo
	Cars     *repo.CarRepo
	Blocks   *repo.BlockRepo
	Pins     *repo.PinRepo
	Config   *repo.ConfigRepo
	Creds    *repo.MTProtoCredsRepo
}

// openEnv resolves the DSN, connects to PostgreSQL and verifies the schema
// version. The caller must Close the returned env.
func openEnv(ctx context.Context, cmd *cobra.Command) (*env, error) {
	dsn := ResolveDSN(cmd)
	if dsn == "" {
		return nil, errNoDSN
	}
	conn, err := db.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := db.CheckSchemaVersion(ctx, conn); err != nil {
		conn.Close()
		return nil, err
	}
	return &env{
		db:       conn,
		Bots:     repo.NewBotRepo(conn),
		Channels: repo.NewChannelRepo(conn),
		Cars:     repo.NewCarRepo(conn),
		Blocks:   repo.NewBlockRepo(conn),
		Pins:     repo.NewPinRepo(conn),
		Config:   repo.NewConfigRepo(conn),
		Creds:    repo.NewMTProtoCredsRepo(conn),
	}, nil
}

// Close releases the database connection pool.
func (e *env) Close() { e.db.Close() }

// transport builds the Telegram transport from the shared config:
// bot_api_url plus, when mtproto_enabled, the active MTProto credentials.
func (e *env) transport(ctx context.Context) (tg.Transport, error) {
	apiURL, err := e.Config.Get(ctx, "bot_api_url")
	if errors.Is(err, repo.ErrNotFound) {
		apiURL = "https://api.telegram.org"
	} else if err != nil {
		return nil, err
	}

	enabled, err := e.Config.GetBool(ctx, "mtproto_enabled")
	if err != nil && !errors.Is(err, repo.ErrNotFound) {
		return nil, err
	}

	var creds *model.MTProtoCreds
	if enabled {
		creds, err = e.Creds.Active(ctx)
		if err != nil {
			return nil, err
		}
	}
	return tg.New(apiURL, creds, ""), nil
}

// findBot resolves a bot by numeric ID or @username.
func (e *env) findBot(ctx context.Context, arg string) (model.Bot, error) {
	if id, err := strconv.ParseInt(arg, 10, 64); err == nil {
		return e.Bots.GetByID(ctx, id)
	}
	username := strings.TrimPrefix(arg, "@")
	var b model.Bot
	err := e.db.GetContext(ctx, &b, `
		SELECT id, tg_id, username, token, active, unavailable_until
		FROM bots WHERE username = $1`, username)
	if errors.Is(err, sql.ErrNoRows) {
		return model.Bot{}, fmt.Errorf("бот %q не найден", arg)
	}
	if err != nil {
		return model.Bot{}, fmt.Errorf("поиск бота %q: %w", arg, err)
	}
	return b, nil
}

// humanBytes formats a byte count with binary prefixes.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d Б", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	suffixes := []string{"КиБ", "МиБ", "ГиБ", "ТиБ", "ПиБ"}
	return fmt.Sprintf("%.1f %s", float64(n)/float64(div), suffixes[exp])
}
