package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ulbwa/ipfsgram/internal/domain"
	"github.com/ulbwa/ipfsgram/internal/port"
)

const testToken = "12345:TEST_TOKEN"

func writeJSON(t *testing.T, w http.ResponseWriter, status int, body string) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := io.WriteString(w, body); err != nil {
		t.Errorf("write response: %v", err)
	}
}

func TestValidateToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bot"+testToken+"/getMe" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		writeJSON(t, w, http.StatusOK, `{"ok":true,"result":{"id":12345,"is_bot":true,"username":"ipfsgram_bot"}}`)
	}))
	defer srv.Close()

	api := NewBotAPI(srv.URL)
	tgID, username, err := api.ValidateToken(context.Background(), testToken)
	if err != nil {
		t.Fatalf("ValidateToken: %v", err)
	}
	if tgID != 12345 {
		t.Errorf("tgID = %d, want 12345", tgID)
	}
	if username != "ipfsgram_bot" {
		t.Errorf("username = %q, want %q", username, "ipfsgram_bot")
	}
}

func TestUpload(t *testing.T) {
	const channelTgID = 1234567890

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bot"+testToken+"/sendDocument" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Fatalf("parse multipart: %v", err)
		}
		if got := r.FormValue("chat_id"); got != "-1001234567890" {
			t.Errorf("chat_id = %q, want %q", got, "-1001234567890")
		}
		if got := r.FormValue("disable_notification"); got != "true" {
			t.Errorf("disable_notification = %q, want %q", got, "true")
		}
		f, hdr, err := r.FormFile("document")
		if err != nil {
			t.Fatalf("form file document: %v", err)
		}
		defer f.Close()
		if hdr.Filename != "bafy.car" {
			t.Errorf("filename = %q, want %q", hdr.Filename, "bafy.car")
		}
		data, err := io.ReadAll(f)
		if err != nil {
			t.Fatalf("read document: %v", err)
		}
		if string(data) != "car-bytes" {
			t.Errorf("document body = %q, want %q", data, "car-bytes")
		}
		writeJSON(t, w, http.StatusOK, `{"ok":true,"result":{"message_id":42,"document":{"file_id":"FILE_ID_1"}}}`)
	}))
	defer srv.Close()

	api := NewBotAPI(srv.URL)
	body := strings.NewReader("car-bytes")
	res, err := api.Upload(context.Background(), testToken, channelTgID, "bafy.car", int64(body.Len()), body)
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if res.MessageID != 42 {
		t.Errorf("MessageID = %d, want 42", res.MessageID)
	}
	if res.FileID != "FILE_ID_1" {
		t.Errorf("FileID = %q, want %q", res.FileID, "FILE_ID_1")
	}
}

func TestUploadFloodWait(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, http.StatusTooManyRequests,
			`{"ok":false,"error_code":429,"description":"Too Many Requests: retry after 7","parameters":{"retry_after":7}}`)
	}))
	defer srv.Close()

	api := NewBotAPI(srv.URL)
	_, err := api.Upload(context.Background(), testToken, 1, "x.car", 1, strings.NewReader("x"))
	var fw *domain.FloodWaitError
	if !errors.As(err, &fw) {
		t.Fatalf("err = %v, want *domain.FloodWaitError", err)
	}
	if fw.RetryAfter != 7*time.Second {
		t.Errorf("RetryAfter = %s, want 7s", fw.RetryAfter)
	}
}

func TestUploadForbidden(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, http.StatusForbidden,
			`{"ok":false,"error_code":403,"description":"Forbidden: bot is not a member of the channel chat"}`)
	}))
	defer srv.Close()

	api := NewBotAPI(srv.URL)
	_, err := api.Upload(context.Background(), testToken, 1, "x.car", 1, strings.NewReader("x"))
	if !errors.Is(err, domain.ErrNoAccess) {
		t.Fatalf("err = %v, want domain.ErrNoAccess", err)
	}
}

func TestDownloadTooBig(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, http.StatusBadRequest,
			`{"ok":false,"error_code":400,"description":"Bad Request: file is too big"}`)
	}))
	defer srv.Close()

	api := NewBotAPI(srv.URL)
	_, _, err := api.Download(context.Background(), testToken, 1, 2, "FILE_ID_BIG")
	if !errors.Is(err, domain.ErrTooLarge) {
		t.Fatalf("err = %v, want domain.ErrTooLarge", err)
	}
}

func TestDownload(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/bot"+testToken+"/getFile", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		if got := r.FormValue("file_id"); got != "FILE_ID_1" {
			t.Errorf("file_id = %q, want %q", got, "FILE_ID_1")
		}
		writeJSON(t, w, http.StatusOK,
			`{"ok":true,"result":{"file_id":"FILE_ID_FRESH","file_path":"documents/file_1.car"}}`)
	})
	mux.HandleFunc("/file/bot"+testToken+"/documents/file_1.car", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "car-bytes")
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	api := NewBotAPI(srv.URL)
	rc, fresh, err := api.Download(context.Background(), testToken, 1, 2, "FILE_ID_1")
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	defer rc.Close()
	if fresh != "FILE_ID_FRESH" {
		t.Errorf("freshFileID = %q, want %q", fresh, "FILE_ID_FRESH")
	}
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(data) != "car-bytes" {
		t.Errorf("body = %q, want %q", data, "car-bytes")
	}
}

func TestDownloadStaleFileID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, http.StatusBadRequest,
			`{"ok":false,"error_code":400,"description":"Bad Request: wrong file_id or the file is temporarily unavailable"}`)
	}))
	defer srv.Close()

	api := NewBotAPI(srv.URL)
	_, _, err := api.Download(context.Background(), testToken, 1, 2, "STALE")
	if !errors.Is(err, domain.ErrBadFileID) {
		t.Fatalf("err = %v, want domain.ErrBadFileID", err)
	}
}

func TestDownloadWithoutFileID(t *testing.T) {
	api := NewBotAPI("http://127.0.0.1:0") // must not be contacted
	_, _, err := api.Download(context.Background(), testToken, 1, 2, "")
	if !errors.Is(err, domain.ErrNoAccess) {
		t.Fatalf("err = %v, want domain.ErrNoAccess", err)
	}
}

func TestCheckMessageDeleted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/forwardMessage") {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		writeJSON(t, w, http.StatusBadRequest,
			`{"ok":false,"error_code":400,"description":"Bad Request: message to forward not found"}`)
	}))
	defer srv.Close()

	api := NewBotAPI(srv.URL)
	err := api.CheckMessage(context.Background(), testToken, 1, 2)
	if !errors.Is(err, domain.ErrMessageDeleted) {
		t.Fatalf("err = %v, want domain.ErrMessageDeleted", err)
	}
}

func TestCheckMessageAlive(t *testing.T) {
	var deleted bool
	mux := http.NewServeMux()
	mux.HandleFunc("/bot"+testToken+"/forwardMessage", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		if got := r.FormValue("chat_id"); got != "-1000000000001" {
			t.Errorf("chat_id = %q, want %q", got, "-1000000000001")
		}
		if got := r.FormValue("from_chat_id"); got != "-1000000000001" {
			t.Errorf("from_chat_id = %q, want %q", got, "-1000000000001")
		}
		writeJSON(t, w, http.StatusOK, `{"ok":true,"result":{"message_id":99}}`)
	})
	mux.HandleFunc("/bot"+testToken+"/deleteMessage", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		if got := r.FormValue("message_id"); got != "99" {
			t.Errorf("message_id = %q, want %q (forward cleanup)", got, "99")
		}
		deleted = true
		writeJSON(t, w, http.StatusOK, `{"ok":true,"result":true}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	api := NewBotAPI(srv.URL)
	if err := api.CheckMessage(context.Background(), testToken, 1, 2); err != nil {
		t.Fatalf("CheckMessage: %v", err)
	}
	if !deleted {
		t.Error("forwarded probe message was not deleted")
	}
}

func TestProbeChannel(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/bot"+testToken+"/getMe", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, http.StatusOK, `{"ok":true,"result":{"id":12345,"username":"ipfsgram_bot"}}`)
	})
	mux.HandleFunc("/bot"+testToken+"/getChat", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, http.StatusOK, `{"ok":true,"result":{"id":-1001234567890,"title":"Storage","type":"channel"}}`)
	})
	mux.HandleFunc("/bot"+testToken+"/getChatMember", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		if got := r.FormValue("user_id"); got != "12345" {
			t.Errorf("user_id = %q, want %q", got, "12345")
		}
		writeJSON(t, w, http.StatusOK,
			`{"ok":true,"result":{"status":"administrator","can_post_messages":true,"can_delete_messages":false}}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	api := NewBotAPI(srv.URL)
	info, err := api.ProbeChannel(context.Background(), testToken, 1234567890)
	if err != nil {
		t.Fatalf("ProbeChannel: %v", err)
	}
	want := port.ChannelInfo{Title: "Storage", Member: true, CanPost: true, CanRead: true, CanDelete: false}
	if info != want {
		t.Errorf("info = %+v, want %+v", info, want)
	}
}

func TestDeleteMessage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/deleteMessage") {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		if got := r.FormValue("message_id"); got != "7" {
			t.Errorf("message_id = %q, want %q", got, "7")
		}
		writeJSON(t, w, http.StatusOK, `{"ok":true,"result":true}`)
	}))
	defer srv.Close()

	api := NewBotAPI(srv.URL)
	if err := api.DeleteMessage(context.Background(), testToken, 1, 7); err != nil {
		t.Fatalf("DeleteMessage: %v", err)
	}
}

// Transport-level errors embed the request URL (which contains the bot
// token); they must be sanitized before being returned to callers.
func TestTransportErrorRedactsToken(t *testing.T) {
	const secret = "12345:SECRET_TOKEN_VALUE"
	// Port 0 is never connectable: dialing fails with *url.Error embedding
	// the full request URL, including "/bot<token>/".
	api := NewBotAPI("http://127.0.0.1:0")
	_, _, err := api.ValidateToken(context.Background(), secret)
	if err == nil {
		t.Fatal("ValidateToken: expected error, got nil")
	}
	if strings.Contains(err.Error(), secret) {
		t.Errorf("error leaks bot token: %v", err)
	}
	if !strings.Contains(err.Error(), "<redacted>") {
		t.Errorf("error does not contain redaction marker: %v", err)
	}
}

func TestDownloadFileFetchErrorRedactsToken(t *testing.T) {
	const secret = "12345:SECRET_TOKEN_VALUE"
	mux := http.NewServeMux()
	mux.HandleFunc("/bot"+secret+"/getFile", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, http.StatusOK,
			`{"ok":true,"result":{"file_id":"F","file_path":"documents/f.car"}}`)
	})
	mux.HandleFunc("/file/bot"+secret+"/documents/f.car", func(w http.ResponseWriter, r *http.Request) {
		// Drop the connection so the client sees a transport error whose
		// *url.Error embeds the file URL with the token.
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Fatalf("hijack: %v", err)
		}
		conn.Close()
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	api := NewBotAPI(srv.URL)
	rc, _, err := api.Download(context.Background(), secret, 1, 2, "F")
	if err == nil {
		rc.Close()
		t.Fatal("Download: expected error, got nil")
	}
	if strings.Contains(err.Error(), secret) {
		t.Errorf("error leaks bot token: %v", err)
	}
}

// readRecorder records whether its Read was ever called.
type readRecorder struct {
	read bool
}

func (r *readRecorder) Read(p []byte) (int, error) {
	r.read = true
	return 0, io.EOF
}

// When request construction fails, Upload must return promptly without
// starting the multipart writer goroutine (which would block forever on the
// pipe with no reader).
func TestUploadBadURLNoGoroutineLeak(t *testing.T) {
	api := NewBotAPI("http://invalid host with spaces")
	rr := &readRecorder{}
	done := make(chan error, 1)
	go func() {
		_, err := api.Upload(context.Background(), testToken, 1, "x.car", 1, rr)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Upload: expected error, got nil")
		}
		if strings.Contains(err.Error(), testToken) {
			t.Errorf("error leaks bot token: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Upload did not return promptly on bad URL")
	}
	// The writer goroutine must not have been started: nothing should have
	// touched the source reader.
	time.Sleep(50 * time.Millisecond)
	if rr.read {
		t.Error("source reader was read despite request construction failure")
	}
}

// Sanity check on raw JSON decoding of the Bot API envelope.
func TestAPIResponseDecoding(t *testing.T) {
	var resp apiResponse
	raw := `{"ok":false,"error_code":429,"description":"flood","parameters":{"retry_after":3}}`
	if err := json.Unmarshal([]byte(raw), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.OK || resp.ErrorCode != 429 || resp.Parameters == nil || resp.Parameters.RetryAfter != 3 {
		t.Errorf("unexpected decode result: %+v", resp)
	}
}
