package telegram

import (
	"context"
	"io"

	"github.com/ulbwa/ipfsgram/internal/domain"
	"github.com/ulbwa/ipfsgram/internal/port"
)

// New returns a transport: pure Bot API (creds == nil), or a hybrid where
// Download/CheckMessage go through MTProto (without the 20 MB download limit)
// and the other operations go through Bot API.
func New(botAPIURL string, creds *domain.MTProtoCreds, sessionDir string) port.Transport {
	bot := NewBotAPI(botAPIURL)
	if creds == nil {
		return bot
	}
	return &hybrid{
		bot: bot,
		mt:  NewMTProto(creds.APIID, creds.APIHash, sessionDir),
	}
}

// hybrid uses Bot API for control and upload, MTProto for reads.
type hybrid struct {
	bot *BotAPI
	mt  *MTProto
}

var _ port.Transport = (*hybrid)(nil)

func (h *hybrid) ValidateToken(ctx context.Context, token string) (int64, string, error) {
	return h.bot.ValidateToken(ctx, token)
}

func (h *hybrid) ProbeChannel(ctx context.Context, token string, channelTgID int64) (port.ChannelInfo, error) {
	return h.bot.ProbeChannel(ctx, token, channelTgID)
}

func (h *hybrid) Upload(ctx context.Context, token string, channelTgID int64, name string, size int64, r io.Reader) (port.UploadResult, error) {
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
