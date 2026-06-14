// GORM-mapped models for every table in the schema, plus the CarStatus
// lifecycle constants. The schema itself is created only by the dbmate
// migrations in /db/migrations; these structs merely map onto it.
package store

import "time"

// CarStatus is the lifecycle status of a CAR archive. A physically deleted
// message has no status: its records (cars, cascading to blocks/car_file_ids)
// are removed from the database as soon as the deletion is detected.
type CarStatus string

const (
	CarPending     CarStatus = "pending"
	CarPublished   CarStatus = "published"
	CarNoBotAccess CarStatus = "no_bot_access" // data is alive, access can be restored
	CarTooLarge    CarStatus = "too_large"     // larger than the current transport's limit
)

// Bot is a Telegram bot that uploads and downloads CARs. Maps to "bots".
type Bot struct {
	ID               int64      `gorm:"column:id;primaryKey"`
	TgID             int64      `gorm:"column:tg_id"`
	Username         string     `gorm:"column:username"`
	Token            string     `gorm:"column:token"`
	Active           bool       `gorm:"column:active"`
	UnavailableUntil *time.Time `gorm:"column:unavailable_until"`
	CreatedAt        time.Time  `gorm:"column:created_at"`
}

// TableName returns the table name for Bot.
func (Bot) TableName() string { return "bots" }

// Channel is a Telegram channel that stores CARs as message files. Maps to
// "channels".
type Channel struct {
	ID           int64  `gorm:"column:id;primaryKey"`
	TgID         int64  `gorm:"column:tg_id"`
	Title        string `gorm:"column:title"`
	MessageCount int64  `gorm:"column:message_count"`
	MessageLimit int64  `gorm:"column:message_limit"`
	Active       bool   `gorm:"column:active"`
}

// TableName returns the table name for Channel.
func (Channel) TableName() string { return "channels" }

// BotChannel is the membership/permission matrix row linking a bot to a
// channel. Maps to "bot_channels".
type BotChannel struct {
	BotID      int64     `gorm:"column:bot_id;primaryKey"`
	ChannelID  int64     `gorm:"column:channel_id;primaryKey"`
	CanPost    bool      `gorm:"column:can_post"`
	CanRead    bool      `gorm:"column:can_read"`
	CanDelete  bool      `gorm:"column:can_delete"`
	Member     bool      `gorm:"column:member"`
	VerifiedAt time.Time `gorm:"column:verified_at"`
}

// TableName returns the table name for BotChannel.
func (BotChannel) TableName() string { return "bot_channels" }

// Car is a CARv1 archive stored as a message file in a channel. Maps to "cars".
type Car struct {
	ID         int64     `gorm:"column:id;primaryKey"`
	ChannelID  int64     `gorm:"column:channel_id"`
	MessageID  *int64    `gorm:"column:message_id"`
	Size       int64     `gorm:"column:size"`
	BlockCount int       `gorm:"column:block_count"`
	Status     CarStatus `gorm:"column:status"`
}

// TableName returns the table name for Car.
func (Car) TableName() string { return "cars" }

// CarFileID is the Telegram file_id a given bot holds for a given CAR. Maps to
// "car_file_ids".
type CarFileID struct {
	CarID     int64     `gorm:"column:car_id;primaryKey"`
	BotID     int64     `gorm:"column:bot_id;primaryKey"`
	FileID    string    `gorm:"column:file_id"`
	UpdatedAt time.Time `gorm:"column:updated_at"`
}

// TableName returns the table name for CarFileID.
func (CarFileID) TableName() string { return "car_file_ids" }

// BlockRef is a stored block-location row: the binary CID and where its payload
// lives inside a CAR. Maps to "blocks".
type BlockRef struct {
	CID    []byte `gorm:"column:cid;primaryKey"` // binary CID (cid.Cid.Bytes())
	CarID  int64  `gorm:"column:car_id"`
	Offset int64  `gorm:"column:offset"` // "offset" is a reserved word
	Length int32  `gorm:"column:length"`
}

// TableName returns the table name for BlockRef.
func (BlockRef) TableName() string { return "blocks" }

// Pin is a pinned DAG root. Maps to "pins".
type Pin struct {
	RootCID   []byte    `gorm:"column:root_cid;primaryKey"`
	Name      string    `gorm:"column:name"`
	Size      int64     `gorm:"column:size"`
	CreatedAt time.Time `gorm:"column:created_at"`
}

// TableName returns the table name for Pin.
func (Pin) TableName() string { return "pins" }

// PinBlock links a pin root to one of the block CIDs it transitively
// references. Maps to "pin_blocks".
type PinBlock struct {
	RootCID []byte `gorm:"column:root_cid;primaryKey"`
	CID     []byte `gorm:"column:cid;primaryKey"`
}

// TableName returns the table name for PinBlock.
func (PinBlock) TableName() string { return "pin_blocks" }

// MTProtoCreds is a stored MTProto api_id/api_hash credential pair, with
// history. Maps to "mtproto_credentials".
type MTProtoCreds struct {
	ID        int64     `gorm:"column:id;primaryKey"`
	APIID     int       `gorm:"column:api_id"`
	APIHash   string    `gorm:"column:api_hash"`
	Active    bool      `gorm:"column:active"`
	CreatedAt time.Time `gorm:"column:created_at"`
}

// TableName returns the table name for MTProtoCreds.
func (MTProtoCreds) TableName() string { return "mtproto_credentials" }
