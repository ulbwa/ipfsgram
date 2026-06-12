// mtproto.go — MTProto, a client over gotd/td authorized with a bot token:
// credential validation, streaming download and message liveness checks via
// channels.getMessages, plus the gotd error classifier. Removes the Bot API
// 20 MB download limit.

package telegram

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

// MTProto is a client over gotd/td authorized with a bot token. It removes
// the Bot API 20 MB download limit.
//
// The client is created and connected on every call (client.Run): this is
// simple and requires no connection lifecycle management, but adds ~seconds of
// handshake/auth per call. A persistent client is a later optimization; the
// file-based session in SessionDir already removes the repeated full bot login.
type MTProto struct {
	APIID      int
	APIHash    string
	SessionDir string // empty -> in-memory (one-shot) sessions
}

// NewMTProto creates an MTProto client.
func NewMTProto(apiID int, apiHash string, sessionDir string) *MTProto {
	return &MTProto{APIID: apiID, APIHash: apiHash, SessionDir: sessionDir}
}

// sessionStorage returns the session storage: a file per bot token (by token
// hash, so the token itself never reaches disk) or memory.
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

// run connects the client, authorizes the bot and calls f with the API.
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

// ValidateCreds performs a trial connection (DC handshake) without authorizing
// the bot — it checks that api_id/api_hash work.
func (m *MTProto) ValidateCreds(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	client := telegram.NewClient(m.APIID, m.APIHash, telegram.Options{
		SessionStorage: &session.StorageMemory{},
		NoUpdates:      true,
	})
	err := client.Run(ctx, func(ctx context.Context) error {
		// Ping after a successful handshake; an invalid api_id/api_hash shows up
		// as API_ID_INVALID on the very first request.
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

// classifyMTProtoError translates gotd errors into the package's classified
// errors.
func classifyMTProtoError(err error) error {
	if err == nil {
		return nil
	}
	// Do not re-wrap already-classified errors.
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

// resolveChannel obtains an InputChannel by channel tg_id (without the -100
// prefix). A bot that is a channel member can usually access it with
// access_hash = 0; if the server rejects it, the bot has no access.
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

// channelDocument extracts the document from a channel message.
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
		// messageEmpty -> the message was deleted.
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

// Download downloads a message's document via MTProto, streaming it into a pipe.
// freshFileID is always empty: file_id is a Bot API entity.
//
// The connection lives until rc is fully read; closing rc before the end of the
// file stops the download.
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
			// Error before the stream started — deliver it directly.
		default:
			// The stream already started: the pipe reader gets the error (or nil).
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

// cancelReadCloser cancels the client's context when the reader is closed.
type cancelReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelReadCloser) Close() error {
	c.cancel()
	return c.ReadCloser.Close()
}

// CheckMessage checks via channels.getMessages that the message is alive and
// contains a document.
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

// Upload via MTProto is not implemented: the hybrid always routes uploads
// through Bot API (sendDocument yields a file_id, which is needed for
// subsequent downloads by other bots).
func (m *MTProto) Upload(context.Context, string, int64, string, int64, io.Reader) (UploadResult, error) {
	return UploadResult{}, errors.New("tg: mtproto: upload not supported, use bot api client")
}
