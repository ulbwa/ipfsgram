package domain

import (
	"errors"
	"fmt"
	"time"
)

// Repository-level sentinels.
var (
	// ErrNotFound is returned when a requested row does not exist.
	ErrNotFound = errors.New("not found")

	// ErrBotExists is returned when a bot with the same token or Telegram ID is
	// already registered.
	ErrBotExists = errors.New("bot already exists")
)

// Transport-contract sentinels. They live here so the Transport port and all
// of its callers share one home. The wrapping transport adapter is responsible
// for any package-specific prefix; the messages here carry none.
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
