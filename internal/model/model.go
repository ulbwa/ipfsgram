package model

import "time"

type CarStatus string

const (
	CarPending     CarStatus = "pending"
	CarPublished   CarStatus = "published"
	CarNoBotAccess CarStatus = "no_bot_access" // данные живы, доступ можно вернуть
	CarTooLarge    CarStatus = "too_large"     // больше лимита текущего транспорта
	// Физически удалённое сообщение статуса не имеет: его записи (cars +
	// каскадом blocks/car_file_ids) удаляются из БД сразу при обнаружении.
)

type Bot struct {
	ID               int64      `db:"id"`
	TgID             int64      `db:"tg_id"`
	Username         string     `db:"username"`
	Token            string     `db:"token"`
	Active           bool       `db:"active"`
	UnavailableUntil *time.Time `db:"unavailable_until"`
}

type Channel struct {
	ID           int64  `db:"id"`
	TgID         int64  `db:"tg_id"`
	Title        string `db:"title"`
	MessageCount int64  `db:"message_count"`
	MessageLimit int64  `db:"message_limit"`
	Active       bool   `db:"active"`
}

type BotChannel struct {
	BotID      int64     `db:"bot_id"`
	ChannelID  int64     `db:"channel_id"`
	CanPost    bool      `db:"can_post"`
	CanRead    bool      `db:"can_read"`
	CanDelete  bool      `db:"can_delete"`
	Member     bool      `db:"member"`
	VerifiedAt time.Time `db:"verified_at"`
}

type Car struct {
	ID         int64     `db:"id"`
	ChannelID  int64     `db:"channel_id"`
	MessageID  *int64    `db:"message_id"`
	Size       int64     `db:"size"`
	BlockCount int       `db:"block_count"`
	Status     CarStatus `db:"status"`
}

type BlockRef struct {
	CID    []byte `db:"cid"` // бинарное представление CID (cid.Cid.Bytes())
	CarID  int64  `db:"car_id"`
	Offset int64  `db:"offset"`
	Length int32  `db:"length"`
}

type MTProtoCreds struct {
	ID        int64     `db:"id"`
	APIID     int       `db:"api_id"`
	APIHash   string    `db:"api_hash"`
	Active    bool      `db:"active"`
	CreatedAt time.Time `db:"created_at"`
}

type Pin struct {
	RootCID   []byte    `db:"root_cid"`
	Name      string    `db:"name"`
	Size      int64     `db:"size"`
	CreatedAt time.Time `db:"created_at"`
}
