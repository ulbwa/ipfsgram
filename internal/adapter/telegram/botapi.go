// Package telegram is the Telegram transport adapter implementing
// port.Transport. It offers a Bot API transport over net/http and an MTProto
// transport over gotd/td, plus a factory that combines them into a hybrid
// transport. All transport errors are mapped to the domain sentinels
// (domain.ErrMessageDeleted, domain.ErrNoAccess, domain.ErrTooLarge,
// domain.ErrBadFileID, *domain.FloodWaitError).
package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ulbwa/ipfsgram/internal/domain"
	"github.com/ulbwa/ipfsgram/internal/port"
)

const defaultBotAPIURL = "https://api.telegram.org"

// apiCallTimeout bounds short control calls (getMe, getChat, deleteMessage,
// etc.). Upload/Download are not bounded by a single hard deadline (files can
// be large), but are protected by the transport's ResponseHeaderTimeout and by
// the caller's context.
const apiCallTimeout = 30 * time.Second

// BotAPI is a transport over the Telegram Bot API (api.telegram.org or a
// self-hosted bot api server via BaseURL).
type BotAPI struct {
	BaseURL string
	HTTP    *http.Client

	mu      sync.Mutex
	selfIDs map[string]int64 // token -> bot id (getMe cache for getChatMember)
}

var _ port.Transport = (*BotAPI)(nil)

// NewBotAPI creates a Bot API transport. An empty baseURL means the official
// https://api.telegram.org.
func NewBotAPI(baseURL string) *BotAPI {
	if baseURL == "" {
		baseURL = defaultBotAPIURL
	}
	return &BotAPI{
		BaseURL: strings.TrimRight(baseURL, "/"),
		HTTP: &http.Client{
			// No global Timeout: uploads/downloads of large files can take a
			// long time; the transport's timeouts cut off stalled connections.
			Transport: &http.Transport{
				ResponseHeaderTimeout: 2 * time.Minute,
				IdleConnTimeout:       90 * time.Second,
			},
		},
		selfIDs: make(map[string]int64),
	}
}

// apiResponse is the standard Bot API response envelope.
type apiResponse struct {
	OK          bool            `json:"ok"`
	Result      json.RawMessage `json:"result"`
	Description string          `json:"description"`
	ErrorCode   int             `json:"error_code"`
	Parameters  *struct {
		RetryAfter int `json:"retry_after"`
	} `json:"parameters"`
}

// chatID builds a channel chat_id from a tg_id without the -100 prefix:
// -(1000000000000 + tgID).
func chatID(channelTgID int64) string {
	return strconv.FormatInt(-(1_000_000_000_000 + channelTgID), 10)
}

// classifyBotAPIError translates a Bot API error into the package's classified
// errors. It always wraps a domain sentinel via %w.
func classifyBotAPIError(method string, resp *apiResponse) error {
	desc := resp.Description
	d := strings.ToLower(desc)

	if resp.ErrorCode == http.StatusTooManyRequests || (resp.Parameters != nil && resp.Parameters.RetryAfter > 0) {
		retry := 1
		if resp.Parameters != nil && resp.Parameters.RetryAfter > 0 {
			retry = resp.Parameters.RetryAfter
		}
		return fmt.Errorf("tg: %s: %s: %w", method, desc, &domain.FloodWaitError{RetryAfter: time.Duration(retry) * time.Second})
	}
	switch {
	case strings.Contains(d, "file is too big"):
		return fmt.Errorf("tg: %s: %s: %w", method, desc, domain.ErrTooLarge)
	case strings.Contains(d, "message to forward not found"),
		strings.Contains(d, "message to delete not found"),
		strings.Contains(d, "message not found"),
		strings.Contains(d, "message_id_invalid"):
		return fmt.Errorf("tg: %s: %s: %w", method, desc, domain.ErrMessageDeleted)
	case strings.Contains(d, "wrong file_id"),
		strings.Contains(d, "invalid file_id"),
		strings.Contains(d, "wrong remote file identifier"):
		return fmt.Errorf("tg: %s: %s: %w", method, desc, domain.ErrBadFileID)
	case resp.ErrorCode == http.StatusForbidden,
		strings.Contains(d, "bot is not a member"),
		strings.Contains(d, "chat not found"),
		strings.Contains(d, "chat_admin_required"),
		strings.Contains(d, "not enough rights"):
		return fmt.Errorf("tg: %s: %s: %w", method, desc, domain.ErrNoAccess)
	}
	return fmt.Errorf("tg: %s: bot api error %d: %s", method, resp.ErrorCode, desc)
}

// redactToken scrubs the bot token from an error's text: transport errors
// (*url.Error and URL parsing errors) carry the full request URL, including
// "/bot<token>/". If the token does not appear in the text, the error is
// returned unchanged. For a *url.Error where the token appears only in the URL
// field, the token is masked in that field directly, preserving the error chain
// (errors.Is(err, context.Canceled) and the like keep working). Otherwise a
// flat error with the masked token is returned.
func redactToken(token string, err error) error {
	if err == nil || token == "" {
		return err
	}
	if !strings.Contains(err.Error(), token) {
		return err
	}
	var ue *url.Error
	if errors.As(err, &ue) && strings.Contains(ue.URL, token) {
		ue.URL = strings.ReplaceAll(ue.URL, token, "<redacted>")
		if !strings.Contains(err.Error(), token) {
			return err
		}
	}
	return errors.New(strings.ReplaceAll(err.Error(), token, "<redacted>"))
}

// do performs the POST request and decodes the response envelope; on ok=false
// it returns a classified error. Transport errors are scrubbed of the token
// (the request URL contains "/bot<token>/").
func (b *BotAPI) do(req *http.Request, token, method string) (*apiResponse, error) {
	httpResp, err := b.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("tg: %s: %w", method, redactToken(token, err))
	}
	defer httpResp.Body.Close()

	var resp apiResponse
	if err := json.NewDecoder(io.LimitReader(httpResp.Body, 1<<20)).Decode(&resp); err != nil {
		return nil, fmt.Errorf("tg: %s: decode response (http %d): %w", method, httpResp.StatusCode, err)
	}
	if !resp.OK {
		if resp.ErrorCode == 0 {
			resp.ErrorCode = httpResp.StatusCode
		}
		return nil, classifyBotAPIError(method, &resp)
	}
	return &resp, nil
}

// call performs a form-encoded Bot API method call with a short timeout and
// decodes result into out (if out != nil).
func (b *BotAPI) call(ctx context.Context, token, method string, params url.Values, out any) error {
	ctx, cancel := context.WithTimeout(ctx, apiCallTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		b.BaseURL+"/bot"+token+"/"+method, strings.NewReader(params.Encode()))
	if err != nil {
		return fmt.Errorf("tg: %s: %w", method, redactToken(token, err))
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := b.do(req, token, method)
	if err != nil {
		return err
	}
	if out != nil {
		if err := json.Unmarshal(resp.Result, out); err != nil {
			return fmt.Errorf("tg: %s: decode result: %w", method, err)
		}
	}
	return nil
}

type botUser struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
}

// ValidateToken validates a token via getMe.
func (b *BotAPI) ValidateToken(ctx context.Context, token string) (int64, string, error) {
	var me botUser
	if err := b.call(ctx, token, "getMe", url.Values{}, &me); err != nil {
		return 0, "", err
	}
	b.mu.Lock()
	b.selfIDs[token] = me.ID
	b.mu.Unlock()
	return me.ID, me.Username, nil
}

// selfID returns the bot id for a token (per-token cache; otherwise getMe).
func (b *BotAPI) selfID(ctx context.Context, token string) (int64, error) {
	b.mu.Lock()
	id, ok := b.selfIDs[token]
	b.mu.Unlock()
	if ok {
		return id, nil
	}
	id, _, err := b.ValidateToken(ctx, token)
	return id, err
}

// ProbeChannel checks the bot's access to a channel and its permissions via
// getChat + getChatMember.
func (b *BotAPI) ProbeChannel(ctx context.Context, token string, channelTgID int64) (port.ChannelInfo, error) {
	cid := chatID(channelTgID)

	var chat struct {
		Title string `json:"title"`
	}
	if err := b.call(ctx, token, "getChat", url.Values{"chat_id": {cid}}, &chat); err != nil {
		return port.ChannelInfo{}, err
	}

	botID, err := b.selfID(ctx, token)
	if err != nil {
		return port.ChannelInfo{}, err
	}

	var member struct {
		Status            string `json:"status"`
		CanPostMessages   bool   `json:"can_post_messages"`
		CanDeleteMessages bool   `json:"can_delete_messages"`
	}
	if err := b.call(ctx, token, "getChatMember", url.Values{
		"chat_id": {cid},
		"user_id": {strconv.FormatInt(botID, 10)},
	}, &member); err != nil {
		return port.ChannelInfo{}, err
	}

	admin := member.Status == "administrator" || member.Status == "creator"
	isMember := admin || member.Status == "member"
	return port.ChannelInfo{
		Title:  chat.Title,
		Member: isMember,
		// Posting to a channel is only available to admins with
		// can_post_messages.
		CanPost: admin && member.CanPostMessages,
		// Reading the channel by the bot (via MTProto) is possible for an admin
		// or a member.
		CanRead:   isMember,
		CanDelete: admin && member.CanDeleteMessages,
	}, nil
}

// Upload publishes a document via sendDocument, streaming the body from r
// (multipart via io.Pipe, without buffering the file in memory). The size
// parameter is unused: Bot API multipart streaming does not require knowing the
// file size in advance (unlike MTProto).
func (b *BotAPI) Upload(ctx context.Context, token string, channelTgID int64, name string, _ int64, r io.Reader) (port.UploadResult, error) {
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)

	// The request is constructed before starting the writer goroutine: if
	// request creation fails, the reader pr would never appear and the
	// goroutine would block forever on pw.Write.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		b.BaseURL+"/bot"+token+"/sendDocument", pr)
	if err != nil {
		return port.UploadResult{}, fmt.Errorf("tg: sendDocument: %w", redactToken(token, err))
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

	go func() {
		err := func() error {
			if err := mw.WriteField("chat_id", chatID(channelTgID)); err != nil {
				return err
			}
			if err := mw.WriteField("disable_notification", "true"); err != nil {
				return err
			}
			part, err := mw.CreateFormFile("document", name)
			if err != nil {
				return err
			}
			if _, err := io.Copy(part, r); err != nil {
				return err
			}
			return mw.Close()
		}()
		pw.CloseWithError(err)
	}()

	resp, err := b.do(req, token, "sendDocument")
	if err != nil {
		return port.UploadResult{}, err
	}

	var msg struct {
		MessageID int64 `json:"message_id"`
		Document  struct {
			FileID string `json:"file_id"`
		} `json:"document"`
	}
	if err := json.Unmarshal(resp.Result, &msg); err != nil {
		return port.UploadResult{}, fmt.Errorf("tg: sendDocument: decode result: %w", err)
	}
	if msg.MessageID == 0 || msg.Document.FileID == "" {
		return port.UploadResult{}, fmt.Errorf("tg: sendDocument: message has no document (message_id=%d)", msg.MessageID)
	}
	return port.UploadResult{MessageID: msg.MessageID, FileID: msg.Document.FileID}, nil
}

// Download downloads a file by file_id: getFile -> GET /file/bot<token>/<path>.
//
// Bot API cannot fetch a file by message_id without a file_id hint; in that
// case it returns domain.ErrNoAccess, and the daemon tries other bots / other
// file_ids or MTProto.
func (b *BotAPI) Download(ctx context.Context, token string, channelTgID, messageID int64, fileID string) (io.ReadCloser, string, error) {
	if fileID == "" {
		return nil, "", fmt.Errorf(
			"tg: bot api cannot fetch message %d by id without a file_id hint (mtproto required): %w",
			messageID, domain.ErrNoAccess)
	}

	var file struct {
		FileID   string `json:"file_id"`
		FilePath string `json:"file_path"`
	}
	if err := b.call(ctx, token, "getFile", url.Values{"file_id": {fileID}}, &file); err != nil {
		return nil, "", err
	}
	if file.FilePath == "" {
		return nil, "", fmt.Errorf("tg: getFile: empty file_path for file_id %q: %w", fileID, domain.ErrBadFileID)
	}
	if file.FileID == "" {
		file.FileID = fileID
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		b.BaseURL+"/file/bot"+token+"/"+file.FilePath, nil)
	if err != nil {
		return nil, "", fmt.Errorf("tg: download file: %w", redactToken(token, err))
	}
	httpResp, err := b.HTTP.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("tg: download file: %w", redactToken(token, err))
	}
	if httpResp.StatusCode != http.StatusOK {
		httpResp.Body.Close()
		if httpResp.StatusCode == http.StatusNotFound {
			return nil, "", fmt.Errorf("tg: download file: http 404 for path %q: %w", file.FilePath, domain.ErrBadFileID)
		}
		return nil, "", fmt.Errorf("tg: download file: unexpected http %d", httpResp.StatusCode)
	}
	return httpResp.Body, file.FileID, nil
}

// CheckMessage checks whether a message is alive.
//
// Bot API has no getMessage method, so this uses a trick: forwardMessage into
// the same channel (with disable_notification) followed by an immediate
// deleteMessage of the forward. "message to forward not found" maps to
// domain.ErrMessageDeleted. This creates a short junk post in the channel; the
// caller uses this method only as a best-effort check during deduplication.
func (b *BotAPI) CheckMessage(ctx context.Context, token string, channelTgID, messageID int64) error {
	cid := chatID(channelTgID)
	var fwd struct {
		MessageID int64 `json:"message_id"`
	}
	err := b.call(ctx, token, "forwardMessage", url.Values{
		"chat_id":              {cid},
		"from_chat_id":         {cid},
		"message_id":           {strconv.FormatInt(messageID, 10)},
		"disable_notification": {"true"},
	}, &fwd)
	if err != nil {
		return err
	}
	// Clean up the forward best-effort: the message is alive, a delete error is
	// not critical.
	_ = b.DeleteMessage(ctx, token, channelTgID, fwd.MessageID)
	return nil
}

// DeleteMessage deletes a message from a channel.
func (b *BotAPI) DeleteMessage(ctx context.Context, token string, channelTgID, messageID int64) error {
	return b.call(ctx, token, "deleteMessage", url.Values{
		"chat_id":    {chatID(channelTgID)},
		"message_id": {strconv.FormatInt(messageID, 10)},
	}, nil)
}
