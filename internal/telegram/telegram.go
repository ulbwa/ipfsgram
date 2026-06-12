// telegram.go — package doc, the Client interface and the hybrid factory New
// that routes Download/CheckMessage through MTProto when it is enabled.

// Package telegram provides concrete Telegram clients used to store and fetch
// CAR archives in channels: BotAPI (net/http over the Bot API, official or
// self-hosted via BaseURL) and MTProto (gotd/td, bot-token auth, no 20 MB
// download limit), plus a hybrid combining them. All transport errors are
// mapped to this package's classified sentinels (ErrMessageDeleted,
// ErrNoAccess, ErrTooLarge, ErrBadFileID, *FloodWaitError) and bot tokens are
// redacted from error texts.
package telegram

import (
	"context"
	"io"
)

// Client is the set of Telegram operations the rest of the system uses. The
// package ships three implementations: *BotAPI, *MTProto and the hybrid
// returned by New.
type Client interface {
	// ValidateToken checks a token (getMe) and returns the bot's tg_id and
	// username.
	ValidateToken(ctx context.Context, token string) (tgID int64, username string, err error)
	// ProbeChannel checks a bot's access to a channel and its permissions.
	ProbeChannel(ctx context.Context, token string, channelTgID int64) (ChannelInfo, error)
	// Upload publishes a file to a channel on behalf of a bot.
	Upload(ctx context.Context, token string, channelTgID int64, name string, size int64, r io.Reader) (UploadResult, error)
	// Download downloads a message's file. fileID is a hint (may be empty).
	Download(ctx context.Context, token string, channelTgID, messageID int64, fileID string) (rc io.ReadCloser, freshFileID string, err error)
	// CheckMessage reports whether a message is alive and its file accessible
	// (for deduplication).
	CheckMessage(ctx context.Context, token string, channelTgID, messageID int64) error
	// DeleteMessage deletes a message (for gc / channel remove).
	DeleteMessage(ctx context.Context, token string, channelTgID, messageID int64) error
}

// New returns a client: pure Bot API (mtAPIID == 0 or mtAPIHash == ""), or a
// hybrid where Download/CheckMessage go through MTProto (without the 20 MB
// download limit) and the other operations go through Bot API.
func New(botAPIURL string, mtAPIID int, mtAPIHash string, sessionDir string) Client {
	bot := NewBotAPI(botAPIURL)
	if mtAPIID == 0 || mtAPIHash == "" {
		return bot
	}
	return &hybrid{
		bot: bot,
		mt:  NewMTProto(mtAPIID, mtAPIHash, sessionDir),
	}
}

// hybrid uses Bot API for control and upload, MTProto for reads.
type hybrid struct {
	bot *BotAPI
	mt  *MTProto
}

func (h *hybrid) ValidateToken(ctx context.Context, token string) (int64, string, error) {
	return h.bot.ValidateToken(ctx, token)
}

func (h *hybrid) ProbeChannel(ctx context.Context, token string, channelTgID int64) (ChannelInfo, error) {
	return h.bot.ProbeChannel(ctx, token, channelTgID)
}

func (h *hybrid) Upload(ctx context.Context, token string, channelTgID int64, name string, size int64, r io.Reader) (UploadResult, error) {
	return h.bot.Upload(ctx, token, channelTgID, name, size, r)
}

func (h *hybrid) Download(ctx context.Context, token string, channelTgID, messageID int64, fileID string) (io.ReadCloser, string, error) {
	return h.mt.Download(ctx, token, channelTgID, messageID, fileID)
}

func (h *hybrid) CheckMessage(ctx context.Context, token string, channelTgID, messageID int64) error {
	return h.mt.CheckMessage(ctx, token, channelTgID, messageID)
}

func (h *hybrid) DeleteMessage(ctx context.Context, token string, channelTgID, messageID int64) error {
	return h.bot.DeleteMessage(ctx, token, channelTgID, messageID)
}
