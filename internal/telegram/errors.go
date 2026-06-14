// errors.go — the package's classified error sentinels, FloodWaitError and the
// bot-token redaction helper shared by all clients.

package telegram

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// Classified transport sentinels. Callers branch on them to decide whether
// records are recoverable.
var (
	// ErrMessageDeleted: the message was physically deleted; its records are
	// unrecoverable and must be removed from the database.
	ErrMessageDeleted = errors.New("message deleted")

	// ErrNoAccess: the bot has no access to the channel/message (kicked, token
	// changed). Data is alive; access can be restored.
	ErrNoAccess = errors.New("bot has no access")

	// ErrTooLarge: the file exceeds the current transport's download limit.
	ErrTooLarge = errors.New("file exceeds transport download limit")

	// ErrBadFileID: the file_id is stale or invalid (Bot API "wrong file_id").
	ErrBadFileID = errors.New("stale file_id")
)

// FloodWaitError reports that Telegram asked the caller to wait before
// retrying (retry_after / FLOOD_WAIT).
type FloodWaitError struct{ RetryAfter time.Duration }

func (e *FloodWaitError) Error() string {
	return fmt.Sprintf("flood wait %s", e.RetryAfter)
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
		// Copy before modifying so the shared error object net/http (and the
		// caller) may still reference stays untouched. The copy keeps ue.Err, so
		// the errors.Is/As chain is preserved.
		ue2 := *ue
		ue2.URL = strings.ReplaceAll(ue.URL, token, "<redacted>")
		if !strings.Contains(ue2.Error(), token) {
			return &ue2
		}
	}
	return errors.New(strings.ReplaceAll(err.Error(), token, "<redacted>"))
}
