package tg

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
)

const defaultBotAPIURL = "https://api.telegram.org"

// apiCallTimeout ограничивает короткие управляющие вызовы (getMe, getChat,
// deleteMessage и т.п.). Upload/Download не ограничиваются жёстким дедлайном
// целиком (файлы бывают большими), но защищены ResponseHeaderTimeout
// транспорта и контекстом вызывающего.
const apiCallTimeout = 30 * time.Second

// BotAPI — транспорт поверх Telegram Bot API (api.telegram.org или
// self-hosted bot api server через BaseURL).
type BotAPI struct {
	BaseURL string
	HTTP    *http.Client

	mu      sync.Mutex
	selfIDs map[string]int64 // токен → id бота (кэш getMe для getChatMember)
}

var _ Transport = (*BotAPI)(nil)

// NewBotAPI создаёт транспорт Bot API. Пустой baseURL — официальный
// https://api.telegram.org.
func NewBotAPI(baseURL string) *BotAPI {
	if baseURL == "" {
		baseURL = defaultBotAPIURL
	}
	return &BotAPI{
		BaseURL: strings.TrimRight(baseURL, "/"),
		HTTP: &http.Client{
			// Без общего Timeout: upload/download больших файлов могут идти
			// долго; зависание соединения отсекают таймауты транспорта.
			Transport: &http.Transport{
				ResponseHeaderTimeout: 2 * time.Minute,
				IdleConnTimeout:       90 * time.Second,
			},
		},
		selfIDs: make(map[string]int64),
	}
}

// apiResponse — стандартный конверт ответа Bot API.
type apiResponse struct {
	OK          bool            `json:"ok"`
	Result      json.RawMessage `json:"result"`
	Description string          `json:"description"`
	ErrorCode   int             `json:"error_code"`
	Parameters  *struct {
		RetryAfter int `json:"retry_after"`
	} `json:"parameters"`
}

// chatID собирает chat_id канала из tg_id без префикса -100:
// -(1000000000000 + tgID).
func chatID(channelTgID int64) string {
	return strconv.FormatInt(-(1_000_000_000_000 + channelTgID), 10)
}

// classifyBotAPIError переводит ошибку Bot API в классифицированные ошибки
// пакета. Всегда оборачивает sentinel через %w.
func classifyBotAPIError(method string, resp *apiResponse) error {
	desc := resp.Description
	d := strings.ToLower(desc)

	if resp.ErrorCode == http.StatusTooManyRequests || (resp.Parameters != nil && resp.Parameters.RetryAfter > 0) {
		retry := 1
		if resp.Parameters != nil && resp.Parameters.RetryAfter > 0 {
			retry = resp.Parameters.RetryAfter
		}
		return fmt.Errorf("tg: %s: %s: %w", method, desc, &FloodWaitError{RetryAfter: time.Duration(retry) * time.Second})
	}
	switch {
	case strings.Contains(d, "file is too big"):
		return fmt.Errorf("tg: %s: %s: %w", method, desc, ErrTooLarge)
	case strings.Contains(d, "message to forward not found"),
		strings.Contains(d, "message to delete not found"),
		strings.Contains(d, "message not found"),
		strings.Contains(d, "message_id_invalid"):
		return fmt.Errorf("tg: %s: %s: %w", method, desc, ErrMessageDeleted)
	case strings.Contains(d, "wrong file_id"),
		strings.Contains(d, "invalid file_id"),
		strings.Contains(d, "wrong remote file identifier"):
		return fmt.Errorf("tg: %s: %s: %w", method, desc, ErrBadFileID)
	case resp.ErrorCode == http.StatusForbidden,
		strings.Contains(d, "bot is not a member"),
		strings.Contains(d, "chat not found"),
		strings.Contains(d, "chat_admin_required"),
		strings.Contains(d, "not enough rights"):
		return fmt.Errorf("tg: %s: %s: %w", method, desc, ErrNoAccess)
	}
	return fmt.Errorf("tg: %s: bot api error %d: %s", method, resp.ErrorCode, desc)
}

// redactToken вычищает токен бота из текста ошибки: транспортные ошибки
// (*url.Error и ошибки парсинга URL) содержат полный URL запроса с
// "/bot<token>/". Если токен в тексте не встречается, ошибка возвращается
// как есть. Для *url.Error, у которого токен встречается только в поле URL,
// токен маскируется прямо в этом поле — цепочка ошибок сохраняется
// (errors.Is(err, context.Canceled) и т.п. продолжают работать). В остальных
// случаях возвращается плоская ошибка с замаскированным токеном.
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

// do выполняет POST-запрос и декодирует конверт ответа; при ok=false
// возвращает классифицированную ошибку. Транспортные ошибки очищаются от
// токена (URL запроса содержит "/bot<token>/").
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

// call выполняет form-encoded вызов метода Bot API с коротким таймаутом и
// декодирует result в out (если out != nil).
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

// ValidateToken проверяет токен через getMe.
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

// selfID возвращает id бота для токена (кэш на токен; иначе getMe).
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

// ProbeChannel проверяет доступ и права бота в канале через
// getChat + getChatMember.
func (b *BotAPI) ProbeChannel(ctx context.Context, token string, channelTgID int64) (ChannelInfo, error) {
	cid := chatID(channelTgID)

	var chat struct {
		Title string `json:"title"`
	}
	if err := b.call(ctx, token, "getChat", url.Values{"chat_id": {cid}}, &chat); err != nil {
		return ChannelInfo{}, err
	}

	botID, err := b.selfID(ctx, token)
	if err != nil {
		return ChannelInfo{}, err
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
		return ChannelInfo{}, err
	}

	admin := member.Status == "administrator" || member.Status == "creator"
	isMember := admin || member.Status == "member"
	return ChannelInfo{
		Title:  chat.Title,
		Member: isMember,
		// Постинг в канал доступен только администраторам с can_post_messages.
		CanPost: admin && member.CanPostMessages,
		// Чтение канала ботом (через MTProto) возможно для админа или member.
		CanRead:   isMember,
		CanDelete: admin && member.CanDeleteMessages,
	}, nil
}

// Upload публикует документ через sendDocument, стримя тело из r
// (multipart через io.Pipe, без буферизации файла в памяти).
// Параметр size не используется: multipart-стриминг Bot API не требует
// знать размер файла заранее (в отличие от MTProto).
func (b *BotAPI) Upload(ctx context.Context, token string, channelTgID int64, name string, _ int64, r io.Reader) (UploadResult, error) {
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)

	// Запрос конструируется до старта пишущей горутины: если создание запроса
	// не удалось, читатель pr никогда не появится и горутина навсегда зависла
	// бы на pw.Write.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		b.BaseURL+"/bot"+token+"/sendDocument", pr)
	if err != nil {
		return UploadResult{}, fmt.Errorf("tg: sendDocument: %w", redactToken(token, err))
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
		return UploadResult{}, err
	}

	var msg struct {
		MessageID int64 `json:"message_id"`
		Document  struct {
			FileID string `json:"file_id"`
		} `json:"document"`
	}
	if err := json.Unmarshal(resp.Result, &msg); err != nil {
		return UploadResult{}, fmt.Errorf("tg: sendDocument: decode result: %w", err)
	}
	if msg.MessageID == 0 || msg.Document.FileID == "" {
		return UploadResult{}, fmt.Errorf("tg: sendDocument: message has no document (message_id=%d)", msg.MessageID)
	}
	return UploadResult{MessageID: msg.MessageID, FileID: msg.Document.FileID}, nil
}

// Download скачивает файл по file_id: getFile → GET /file/bot<token>/<path>.
//
// Bot API не умеет получать файл по message_id без подсказки file_id —
// в этом случае возвращается ErrNoAccess, и демон пробует других ботов /
// другие file_id либо MTProto.
func (b *BotAPI) Download(ctx context.Context, token string, channelTgID, messageID int64, fileID string) (io.ReadCloser, string, error) {
	if fileID == "" {
		return nil, "", fmt.Errorf(
			"tg: bot api cannot fetch message %d by id without a file_id hint (mtproto required): %w",
			messageID, ErrNoAccess)
	}

	var file struct {
		FileID   string `json:"file_id"`
		FilePath string `json:"file_path"`
	}
	if err := b.call(ctx, token, "getFile", url.Values{"file_id": {fileID}}, &file); err != nil {
		return nil, "", err
	}
	if file.FilePath == "" {
		return nil, "", fmt.Errorf("tg: getFile: empty file_path for file_id %q: %w", fileID, ErrBadFileID)
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
			return nil, "", fmt.Errorf("tg: download file: http 404 for path %q: %w", file.FilePath, ErrBadFileID)
		}
		return nil, "", fmt.Errorf("tg: download file: unexpected http %d", httpResp.StatusCode)
	}
	return httpResp.Body, file.FileID, nil
}

// CheckMessage проверяет, живо ли сообщение.
//
// Bot API не имеет метода getMessage, поэтому используется хак: forwardMessage
// в тот же канал (с disable_notification) и немедленный deleteMessage форварда.
// "message to forward not found" → ErrMessageDeleted. Это создаёт короткий
// мусорный пост в канале; вызывающий использует метод только как best-effort
// проверку при дедупликации.
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
	// Чистим форвард best-effort: сообщение живо, ошибка удаления не критична.
	_ = b.DeleteMessage(ctx, token, channelTgID, fwd.MessageID)
	return nil
}

// DeleteMessage удаляет сообщение из канала.
func (b *BotAPI) DeleteMessage(ctx context.Context, token string, channelTgID, messageID int64) error {
	return b.call(ctx, token, "deleteMessage", url.Values{
		"chat_id":    {chatID(channelTgID)},
		"message_id": {strconv.FormatInt(messageID, 10)},
	}, nil)
}
