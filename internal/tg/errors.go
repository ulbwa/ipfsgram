package tg

import (
	"errors"
	"fmt"
	"time"
)

// FloodWaitError — Telegram попросил подождать (retry_after / FLOOD_WAIT).
type FloodWaitError struct{ RetryAfter time.Duration }

func (e *FloodWaitError) Error() string {
	return fmt.Sprintf("flood wait %s", e.RetryAfter)
}

// Сообщения sentinel-ошибок без префикса "tg: ": оборачивающий код пакета
// (classifyBotAPIError и т.п.) добавляет его сам, иначе префикс дублируется.
var (
	ErrMessageDeleted = errors.New("message deleted")
	ErrNoAccess       = errors.New("bot has no access")
	ErrTooLarge       = errors.New("file exceeds transport download limit")
	// ErrBadFileID — file_id протух или невалиден (Bot API: "wrong file_id").
	ErrBadFileID = errors.New("stale file_id")
)
