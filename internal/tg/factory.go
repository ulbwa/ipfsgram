package tg

import (
	"context"
	"io"

	"github.com/ulbwa/ipfsgram/internal/model"
)

// New возвращает транспорт: чистый Bot API (creds == nil), либо гибрид,
// где Download/CheckMessage идут через MTProto (без лимита 20 МБ на
// скачивание), а остальные операции — через Bot API.
func New(botAPIURL string, creds *model.MTProtoCreds, sessionDir string) Transport {
	bot := NewBotAPI(botAPIURL)
	if creds == nil {
		return bot
	}
	return &hybrid{
		bot: bot,
		mt:  NewMTProto(creds.APIID, creds.APIHash, sessionDir),
	}
}

// hybrid — Bot API для управления и загрузки, MTProto для чтения.
type hybrid struct {
	bot *BotAPI
	mt  *MTProto
}

var _ Transport = (*hybrid)(nil)

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
