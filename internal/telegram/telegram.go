// telegram.go — package doc, the concrete Client type and the factory New
// that routes Download/CheckMessage through MTProto when it is enabled, falling
// back to the Bot API otherwise.

// Package telegram provides concrete Telegram clients used to store and fetch
// CAR archives in channels: BotAPI (net/http over the Bot API, official or
// self-hosted via BaseURL) and MTProto (gotd/td, bot-token auth, no 20 MB
// download limit), plus Client combining them. All transport errors are
// mapped to this package's classified sentinels (ErrMessageDeleted,
// ErrNoAccess, ErrTooLarge, ErrBadFileID, *FloodWaitError) and bot tokens are
// redacted from error texts. Consumers declare their own narrow interfaces;
// *Client satisfies them implicitly.
package telegram

import (
	"context"
	"io"
)

// Client is the concrete Telegram client the rest of the system uses. It uses
// Bot API for control and upload; when MTProto is enabled it routes
// Download/CheckMessage through MTProto (without the 20 MB download limit),
// otherwise those too go through the Bot API.
type Client struct {
	bot *BotAPI
	mt  *MTProto // nil when MTProto is not enabled
}

// New returns a Client: pure Bot API (mtAPIID == 0 or mtAPIHash == ""), or a
// hybrid where Download/CheckMessage go through MTProto (without the 20 MB
// download limit) and the other operations go through Bot API.
func New(botAPIURL string, mtAPIID int, mtAPIHash string, sessionDir string) *Client {
	c := &Client{bot: NewBotAPI(botAPIURL)}
	if mtAPIID != 0 && mtAPIHash != "" {
		c.mt = NewMTProto(mtAPIID, mtAPIHash, sessionDir)
	}
	return c
}

// ValidateToken checks a token (getMe) and returns the bot's tg_id and username.
func (c *Client) ValidateToken(ctx context.Context, token string) (int64, string, error) {
	return c.bot.ValidateToken(ctx, token)
}

// ProbeChannel checks a bot's access to a channel and its permissions.
func (c *Client) ProbeChannel(ctx context.Context, token string, channelTgID int64) (ChannelInfo, error) {
	return c.bot.ProbeChannel(ctx, token, channelTgID)
}

// Upload publishes a file to a channel on behalf of a bot.
func (c *Client) Upload(ctx context.Context, token string, channelTgID int64, name string, size int64, r io.Reader) (UploadResult, error) {
	return c.bot.Upload(ctx, token, channelTgID, name, size, r)
}

// Download downloads a message's file (fileID is a hint, may be empty), via
// MTProto when enabled and via the Bot API otherwise.
func (c *Client) Download(ctx context.Context, token string, channelTgID, messageID int64, fileID string) (io.ReadCloser, string, error) {
	if c.mt != nil {
		return c.mt.Download(ctx, token, channelTgID, messageID, fileID)
	}
	return c.bot.Download(ctx, token, channelTgID, messageID, fileID)
}

// CheckMessage reports whether a message is alive and its file accessible (for
// deduplication), via MTProto when enabled and via the Bot API otherwise.
func (c *Client) CheckMessage(ctx context.Context, token string, channelTgID, messageID int64) error {
	if c.mt != nil {
		return c.mt.CheckMessage(ctx, token, channelTgID, messageID)
	}
	return c.bot.CheckMessage(ctx, token, channelTgID, messageID)
}

// DeleteMessage deletes a message (for gc / channel remove).
func (c *Client) DeleteMessage(ctx context.Context, token string, channelTgID, messageID int64) error {
	return c.bot.DeleteMessage(ctx, token, channelTgID, messageID)
}
