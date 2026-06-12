package tg

import (
	"context"
	"io"
)

// UploadResult — результат публикации CAR в канал.
type UploadResult struct {
	MessageID int64
	FileID    string // file_id бота-аплоадера (Bot API); пустой для чистого MTProto
}

type ChannelInfo struct {
	Title     string
	Member    bool
	CanPost   bool
	CanRead   bool
	CanDelete bool
}

type Transport interface {
	// ValidateToken проверяет токен (getMe) и возвращает tg_id и username бота.
	ValidateToken(ctx context.Context, token string) (tgID int64, username string, err error)
	// ProbeChannel проверяет доступ бота к каналу и его права.
	ProbeChannel(ctx context.Context, token string, channelTgID int64) (ChannelInfo, error)
	// Upload публикует файл в канал от имени бота.
	Upload(ctx context.Context, token string, channelTgID int64, name string, size int64, r io.Reader) (UploadResult, error)
	// Download скачивает файл сообщения. fileID — подсказка (может быть пустой).
	Download(ctx context.Context, token string, channelTgID, messageID int64, fileID string) (rc io.ReadCloser, freshFileID string, err error)
	// CheckMessage проверяет, живо ли сообщение и доступен ли файл (для дедупликации).
	CheckMessage(ctx context.Context, token string, channelTgID, messageID int64) error
	// DeleteMessage удаляет сообщение (для gc / channel remove).
	DeleteMessage(ctx context.Context, token string, channelTgID, messageID int64) error
}
