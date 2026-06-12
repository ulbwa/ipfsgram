package repo

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/ulbwa/ipfsgram/internal/model"
)

// CarRepo provides access to the cars and car_file_ids tables.
type CarRepo struct {
	db *sqlx.DB
}

// NewCarRepo returns a CarRepo over db.
func NewCarRepo(db *sqlx.DB) *CarRepo {
	return &CarRepo{db: db}
}

// CreatePending inserts a new car in the pending status and returns its ID.
func (r *CarRepo) CreatePending(ctx context.Context, channelID, size int64, blockCount int) (int64, error) {
	var id int64
	err := r.db.GetContext(ctx, &id, `
		INSERT INTO cars (channel_id, size, block_count, status)
		VALUES ($1, $2, $3, 'pending')
		RETURNING id`,
		channelID, size, blockCount)
	if err != nil {
		return 0, fmt.Errorf("create pending car: %w", err)
	}
	return id, nil
}

// MarkPublished sets the car's message ID and moves it to the published
// status.
func (r *CarRepo) MarkPublished(ctx context.Context, carID, messageID int64) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE cars SET message_id = $2, status = 'published' WHERE id = $1`,
		carID, messageID)
	if err != nil {
		return fmt.Errorf("mark car %d published: %w", carID, err)
	}
	return requireAffected(res, fmt.Sprintf("mark car %d published", carID))
}

// SetStatus sets the car's status.
func (r *CarRepo) SetStatus(ctx context.Context, carID int64, st model.CarStatus) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE cars SET status = $2 WHERE id = $1`, carID, string(st))
	if err != nil {
		return fmt.Errorf("set car %d status: %w", carID, err)
	}
	return requireAffected(res, fmt.Sprintf("set car %d status", carID))
}

// Get returns the car with the given ID, or ErrNotFound.
func (r *CarRepo) Get(ctx context.Context, carID int64) (model.Car, error) {
	var c model.Car
	err := r.db.GetContext(ctx, &c, `
		SELECT id, channel_id, message_id, size, block_count, status
		FROM cars WHERE id = $1`, carID)
	if err != nil {
		return model.Car{}, notFound(fmt.Sprintf("get car %d", carID), err)
	}
	return c, nil
}

// Delete removes the car row; the schema cascades the deletion to its blocks
// and car_file_ids. Used when the underlying message is found physically
// deleted.
func (r *CarRepo) Delete(ctx context.Context, carID int64) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM cars WHERE id = $1`, carID)
	if err != nil {
		return fmt.Errorf("delete car %d: %w", carID, err)
	}
	return requireAffected(res, fmt.Sprintf("delete car %d", carID))
}

// UpsertFileID inserts or updates the Telegram file_id for the (car, bot)
// pair.
func (r *CarRepo) UpsertFileID(ctx context.Context, carID, botID int64, fileID string) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO car_file_ids (car_id, bot_id, file_id, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (car_id, bot_id) DO UPDATE SET
			file_id    = EXCLUDED.file_id,
			updated_at = now()`,
		carID, botID, fileID)
	if err != nil {
		return fmt.Errorf("upsert file_id for car %d bot %d: %w", carID, botID, err)
	}
	return nil
}

// DeleteFileID removes the file_id for the (car, bot) pair.
func (r *CarRepo) DeleteFileID(ctx context.Context, carID, botID int64) error {
	_, err := r.db.ExecContext(ctx,
		`DELETE FROM car_file_ids WHERE car_id = $1 AND bot_id = $2`, carID, botID)
	if err != nil {
		return fmt.Errorf("delete file_id for car %d bot %d: %w", carID, botID, err)
	}
	return nil
}

// FileIDs returns the bot ID → file_id mapping for the given car.
func (r *CarRepo) FileIDs(ctx context.Context, carID int64) (map[int64]string, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT bot_id, file_id FROM car_file_ids WHERE car_id = $1`, carID)
	if err != nil {
		return nil, fmt.Errorf("file_ids for car %d: %w", carID, err)
	}
	defer rows.Close()

	out := make(map[int64]string)
	for rows.Next() {
		var botID int64
		var fileID string
		if err := rows.Scan(&botID, &fileID); err != nil {
			return nil, fmt.Errorf("file_ids for car %d: scan: %w", carID, err)
		}
		out[botID] = fileID
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("file_ids for car %d: %w", carID, err)
	}
	return out, nil
}

// OrphanPending returns cars stuck in the pending status that were created
// more than olderThan ago.
func (r *CarRepo) OrphanPending(ctx context.Context, olderThan time.Duration) ([]model.Car, error) {
	var cars []model.Car
	err := r.db.SelectContext(ctx, &cars, `
		SELECT id, channel_id, message_id, size, block_count, status
		FROM cars
		WHERE status = 'pending' AND created_at < now() - $1::interval
		ORDER BY id`,
		fmt.Sprintf("%f seconds", olderThan.Seconds()))
	if err != nil {
		return nil, fmt.Errorf("orphan pending cars: %w", err)
	}
	return cars, nil
}

// UnpinnedCars returns cars none of whose blocks are referenced by any pin,
// i.e. candidates for garbage collection.
func (r *CarRepo) UnpinnedCars(ctx context.Context) ([]model.Car, error) {
	var cars []model.Car
	err := r.db.SelectContext(ctx, &cars, `
		SELECT c.id, c.channel_id, c.message_id, c.size, c.block_count, c.status
		FROM cars c
		WHERE NOT EXISTS (
			SELECT 1
			FROM blocks b
			JOIN pin_blocks pb ON pb.cid = b.cid
			WHERE b.car_id = c.id
		)
		ORDER BY c.id`)
	if err != nil {
		return nil, fmt.Errorf("unpinned cars: %w", err)
	}
	return cars, nil
}
