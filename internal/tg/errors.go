package tg

import (
	"errors"
	"fmt"
	"time"
)

// FloodWaitError — Telegram попросил подождать (retry_after / FLOOD_WAIT).
type FloodWaitError struct{ RetryAfter time.Duration }

func (e *FloodWaitError) Error() string {
	return fmt.Sprintf("tg: flood wait %s", e.RetryAfter)
}

var (
	ErrMessageDeleted = errors.New("tg: message deleted")
	ErrNoAccess       = errors.New("tg: bot has no access")
	ErrTooLarge       = errors.New("tg: file exceeds transport download limit")
)
