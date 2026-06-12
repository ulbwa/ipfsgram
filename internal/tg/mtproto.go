package tg

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/downloader"
	tdtg "github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

// MTProto — транспорт поверх gotd/td с авторизацией по bot-токену.
// Снимает лимит Bot API в 20 МБ на скачивание.
//
// Клиент создаётся и подключается на каждый вызов (client.Run): это просто
// и не требует управления жизненным циклом соединения, но добавляет
// ~секунды на хендшейк/авторизацию на вызов. Персистентный клиент —
// оптимизация на потом; файловая сессия в SessionDir уже сейчас убирает
// повторный полный bot-login.
type MTProto struct {
	APIID      int
	APIHash    string
	SessionDir string // пусто — сессии в памяти (одноразовые)
}

// NewMTProto создаёт MTProto-транспорт.
func NewMTProto(apiID int, apiHash string, sessionDir string) *MTProto {
	return &MTProto{APIID: apiID, APIHash: apiHash, SessionDir: sessionDir}
}

// sessionStorage возвращает хранилище сессии: файл на бот-токен (по хэшу
// токена, сам токен на диск не попадает) либо память.
func (m *MTProto) sessionStorage(token string) (telegram.SessionStorage, error) {
	if m.SessionDir == "" {
		return &session.StorageMemory{}, nil
	}
	if err := os.MkdirAll(m.SessionDir, 0o700); err != nil {
		return nil, fmt.Errorf("tg: mtproto: create session dir: %w", err)
	}
	sum := sha256.Sum256([]byte(token))
	return &session.FileStorage{
		Path: filepath.Join(m.SessionDir, fmt.Sprintf("bot-%x.session", sum[:8])),
	}, nil
}

// run подключает клиента, авторизует бота и вызывает f с API.
func (m *MTProto) run(ctx context.Context, token string, f func(ctx context.Context, api *tdtg.Client) error) error {
	storage, err := m.sessionStorage(token)
	if err != nil {
		return err
	}
	client := telegram.NewClient(m.APIID, m.APIHash, telegram.Options{
		SessionStorage: storage,
		NoUpdates:      true,
	})
	err = client.Run(ctx, func(ctx context.Context) error {
		status, err := client.Auth().Status(ctx)
		if err != nil {
			return fmt.Errorf("auth status: %w", err)
		}
		if !status.Authorized {
			if _, err := client.Auth().Bot(ctx, token); err != nil {
				return fmt.Errorf("bot auth: %w", err)
			}
		}
		return f(ctx, client.API())
	})
	if err != nil {
		return classifyMTProtoError(err)
	}
	return nil
}

// ValidateCreds выполняет пробное подключение (хендшейк с DC) без
// авторизации бота — проверяет, что api_id/api_hash рабочие.
func (m *MTProto) ValidateCreds(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	client := telegram.NewClient(m.APIID, m.APIHash, telegram.Options{
		SessionStorage: &session.StorageMemory{},
		NoUpdates:      true,
	})
	err := client.Run(ctx, func(ctx context.Context) error {
		// Пинг после успешного хендшейка; невалидный api_id/api_hash
		// проявится как API_ID_INVALID на первом же запросе.
		if err := client.Ping(ctx); err != nil {
			return err
		}
		_, err := client.API().HelpGetConfig(ctx)
		return err
	})
	if err != nil {
		return fmt.Errorf("tg: mtproto: validate creds: %w", classifyMTProtoError(err))
	}
	return nil
}

// classifyMTProtoError переводит ошибки gotd в классифицированные ошибки пакета.
func classifyMTProtoError(err error) error {
	if err == nil {
		return nil
	}
	// Уже классифицированные ошибки не переворачиваем.
	var fw *FloodWaitError
	if errors.As(err, &fw) || errors.Is(err, ErrMessageDeleted) ||
		errors.Is(err, ErrNoAccess) || errors.Is(err, ErrTooLarge) || errors.Is(err, ErrBadFileID) {
		return err
	}
	if d, ok := tgerr.AsFloodWait(err); ok {
		return fmt.Errorf("tg: mtproto: %v: %w", err, &FloodWaitError{RetryAfter: d})
	}
	if tgerr.Is(err, "MESSAGE_ID_INVALID", "MESSAGE_IDS_EMPTY") {
		return fmt.Errorf("tg: mtproto: %v: %w", err, ErrMessageDeleted)
	}
	if tgerr.Is(err,
		"CHANNEL_PRIVATE", "CHANNEL_INVALID", "CHAT_ADMIN_REQUIRED",
		"AUTH_KEY_UNREGISTERED", "USER_DEACTIVATED", "ACCESS_TOKEN_INVALID",
		"ACCESS_TOKEN_EXPIRED", "BOT_METHOD_INVALID",
	) {
		return fmt.Errorf("tg: mtproto: %v: %w", err, ErrNoAccess)
	}
	return err
}

// resolveChannel получает InputChannel по tg_id канала (без префикса -100).
// Бот-участник канала обычно может обращаться с access_hash = 0; если
// сервер отверг — у бота нет доступа.
func resolveChannel(ctx context.Context, api *tdtg.Client, channelTgID int64) (*tdtg.InputChannel, error) {
	chats, err := api.ChannelsGetChannels(ctx, []tdtg.InputChannelClass{
		&tdtg.InputChannel{ChannelID: channelTgID, AccessHash: 0},
	})
	if err != nil {
		if cl := classifyMTProtoError(err); !errors.Is(cl, err) {
			return nil, cl
		}
		return nil, fmt.Errorf("tg: mtproto: resolve channel %d: %v: %w", channelTgID, err, ErrNoAccess)
	}
	for _, chat := range chats.GetChats() {
		ch, ok := chat.(*tdtg.Channel)
		if !ok || ch.ID != channelTgID {
			continue
		}
		hash, _ := ch.GetAccessHash()
		return &tdtg.InputChannel{ChannelID: ch.ID, AccessHash: hash}, nil
	}
	return nil, fmt.Errorf("tg: mtproto: channel %d not returned by getChannels: %w", channelTgID, ErrNoAccess)
}

// channelDocument достаёт документ из сообщения канала.
func channelDocument(ctx context.Context, api *tdtg.Client, ch *tdtg.InputChannel, messageID int64) (*tdtg.Document, error) {
	res, err := api.ChannelsGetMessages(ctx, &tdtg.ChannelsGetMessagesRequest{
		Channel: ch,
		ID:      []tdtg.InputMessageClass{&tdtg.InputMessageID{ID: int(messageID)}},
	})
	if err != nil {
		return nil, err
	}
	msgs, ok := res.(*tdtg.MessagesChannelMessages)
	if !ok {
		return nil, fmt.Errorf("tg: mtproto: unexpected getMessages result %T: %w", res, ErrNoAccess)
	}
	if len(msgs.Messages) == 0 {
		return nil, fmt.Errorf("tg: mtproto: message %d not found: %w", messageID, ErrMessageDeleted)
	}
	msg, ok := msgs.Messages[0].(*tdtg.Message)
	if !ok {
		// messageEmpty — сообщение удалено.
		return nil, fmt.Errorf("tg: mtproto: message %d is empty: %w", messageID, ErrMessageDeleted)
	}
	media, ok := msg.Media.(*tdtg.MessageMediaDocument)
	if !ok {
		return nil, fmt.Errorf("tg: mtproto: message %d has no document media: %w", messageID, ErrMessageDeleted)
	}
	doc, ok := media.Document.AsNotEmpty()
	if !ok {
		return nil, fmt.Errorf("tg: mtproto: message %d document is empty: %w", messageID, ErrMessageDeleted)
	}
	return doc, nil
}

// Download скачивает документ сообщения через MTProto, стримя его в pipe.
// freshFileID всегда пустой: file_id — сущность Bot API.
//
// Соединение живёт до конца чтения rc; закрытие rc до конца файла
// останавливает скачивание.
func (m *MTProto) Download(ctx context.Context, token string, channelTgID, messageID int64, _ string) (io.ReadCloser, string, error) {
	pr, pw := io.Pipe()
	runCtx, cancel := context.WithCancel(ctx)
	setup := make(chan error, 1)

	go func() {
		defer cancel()
		err := m.run(runCtx, token, func(ctx context.Context, api *tdtg.Client) error {
			ch, err := resolveChannel(ctx, api, channelTgID)
			if err != nil {
				return err
			}
			doc, err := channelDocument(ctx, api, ch, messageID)
			if err != nil {
				return err
			}
			setup <- nil
			_, err = downloader.NewDownloader().
				Download(api, doc.AsInputDocumentFileLocation()).
				Stream(ctx, pw)
			return err
		})
		err = classifyMTProtoError(err)
		select {
		case setup <- err:
			// Ошибка до начала стрима — отдаём её напрямую.
		default:
			// Стрим уже начался: ошибку (или nil) получит читатель pipe.
		}
		pw.CloseWithError(err)
	}()

	if err := <-setup; err != nil {
		cancel()
		pr.Close()
		return nil, "", err
	}
	return &cancelReadCloser{ReadCloser: pr, cancel: cancel}, "", nil
}

// cancelReadCloser отменяет контекст клиента при закрытии читателя.
type cancelReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelReadCloser) Close() error {
	c.cancel()
	return c.ReadCloser.Close()
}

// CheckMessage проверяет через channels.getMessages, что сообщение живо и
// содержит документ.
func (m *MTProto) CheckMessage(ctx context.Context, token string, channelTgID, messageID int64) error {
	return m.run(ctx, token, func(ctx context.Context, api *tdtg.Client) error {
		ch, err := resolveChannel(ctx, api, channelTgID)
		if err != nil {
			return err
		}
		_, err = channelDocument(ctx, api, ch, messageID)
		return err
	})
}

// Upload через MTProto не реализован: фабрика всегда маршрутизирует
// загрузку через Bot API (sendDocument даёт file_id, который нужен для
// дальнейших скачиваний другими ботами).
func (m *MTProto) Upload(context.Context, string, int64, string, int64, io.Reader) (UploadResult, error) {
	return UploadResult{}, errors.New("tg: mtproto: upload not supported, use bot api transport")
}
