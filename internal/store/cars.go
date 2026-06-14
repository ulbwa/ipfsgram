// Store methods for the cars and car_file_ids tables, including the
// cross-entity GC/doctor/bot-remove candidate queries.
package store

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm/clause"
)

// CreatePendingCar inserts a new car in the pending status and returns its ID.
func (s *Store) CreatePendingCar(ctx context.Context, channelID, size int64, blockCount int) (int64, error) {
	c := Car{
		ChannelID:  channelID,
		Size:       size,
		BlockCount: blockCount,
		Status:     CarPending,
	}
	if err := s.db.WithContext(ctx).Create(&c).Error; err != nil {
		return 0, fmt.Errorf("create pending car: %w", err)
	}
	return c.ID, nil
}

// MarkCarPublished sets the car's message ID and moves it to the published
// status.
func (s *Store) MarkCarPublished(ctx context.Context, carID, messageID int64) error {
	res := s.db.WithContext(ctx).
		Model(&Car{}).
		Where("id = ?", carID).
		Updates(map[string]any{
			"message_id": messageID,
			"status":     CarPublished,
		})
	return requireAffected(res, fmt.Sprintf("mark car %d published", carID))
}

// SetCarStatus sets the car's status.
func (s *Store) SetCarStatus(ctx context.Context, carID int64, st CarStatus) error {
	res := s.db.WithContext(ctx).
		Model(&Car{}).
		Where("id = ?", carID).
		Update("status", st)
	return requireAffected(res, fmt.Sprintf("set car %d status", carID))
}

// Car returns the car with the given ID, or ErrNotFound.
func (s *Store) Car(ctx context.Context, carID int64) (Car, error) {
	var c Car
	err := s.db.WithContext(ctx).Where("id = ?", carID).Take(&c).Error
	if err != nil {
		return Car{}, notFound(fmt.Sprintf("get car %d", carID), err)
	}
	return c, nil
}

// DeleteCar removes the car row; the schema cascades the deletion to its
// blocks and car_file_ids.
func (s *Store) DeleteCar(ctx context.Context, carID int64) error {
	res := s.db.WithContext(ctx).Where("id = ?", carID).Delete(&Car{})
	return requireAffected(res, fmt.Sprintf("delete car %d", carID))
}

// UpsertCarFileID inserts or updates the Telegram file_id for the (car, bot)
// pair.
func (s *Store) UpsertCarFileID(ctx context.Context, carID, botID int64, fileID string) error {
	row := CarFileID{
		CarID:     carID,
		BotID:     botID,
		FileID:    fileID,
		UpdatedAt: time.Now(),
	}
	err := s.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "car_id"}, {Name: "bot_id"}},
			DoUpdates: clause.AssignmentColumns([]string{"file_id", "updated_at"}),
		}).
		Create(&row).Error
	if err != nil {
		return fmt.Errorf("upsert file_id for car %d bot %d: %w", carID, botID, err)
	}
	return nil
}

// DeleteCarFileID removes the file_id for the (car, bot) pair.
func (s *Store) DeleteCarFileID(ctx context.Context, carID, botID int64) error {
	err := s.db.WithContext(ctx).
		Where("car_id = ? AND bot_id = ?", carID, botID).
		Delete(&CarFileID{}).Error
	if err != nil {
		return fmt.Errorf("delete file_id for car %d bot %d: %w", carID, botID, err)
	}
	return nil
}

// CarFileIDs returns the bot ID → file_id mapping for the given car.
func (s *Store) CarFileIDs(ctx context.Context, carID int64) (map[int64]string, error) {
	var rows []CarFileID
	err := s.db.WithContext(ctx).
		Where("car_id = ?", carID).
		Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("file_ids for car %d: %w", carID, err)
	}
	out := make(map[int64]string, len(rows))
	for _, row := range rows {
		out[row.BotID] = row.FileID
	}
	return out, nil
}

// OrphanPendingCars returns cars stuck in the pending status that were created
// more than olderThan ago.
func (s *Store) OrphanPendingCars(ctx context.Context, olderThan time.Duration) ([]Car, error) {
	var cars []Car
	err := s.db.WithContext(ctx).
		Where("status = ? AND created_at < now() - make_interval(secs => ?)",
			CarPending, olderThan.Seconds()).
		Order("id").
		Find(&cars).Error
	if err != nil {
		return nil, fmt.Errorf("orphan pending cars: %w", err)
	}
	return cars, nil
}

// UnpinnedCars returns cars none of whose blocks are referenced by any pin —
// i.e. candidates for garbage collection.
func (s *Store) UnpinnedCars(ctx context.Context) ([]Car, error) {
	var cars []Car
	err := s.db.WithContext(ctx).
		Where(`NOT EXISTS (
			SELECT 1
			FROM blocks b
			JOIN pin_blocks pb ON pb.cid = b.cid
			WHERE b.car_id = cars.id
		)`).
		Order("id").
		Find(&cars).Error
	if err != nil {
		return nil, fmt.Errorf("unpinned cars: %w", err)
	}
	return cars, nil
}

// CarsWithStatus returns cars in any of the given statuses (for doctor
// recovery).
func (s *Store) CarsWithStatus(ctx context.Context, statuses ...CarStatus) ([]Car, error) {
	var cars []Car
	if len(statuses) == 0 {
		return cars, nil
	}
	err := s.db.WithContext(ctx).
		Where("status IN ?", statuses).
		Order("id").
		Find(&cars).Error
	if err != nil {
		return nil, fmt.Errorf("cars by statuses: %w", err)
	}
	return cars, nil
}

// CarsAccessibleOnlyVia returns cars whose channel has no OTHER active member
// bot besides the given one — i.e. cars that become unreachable if that bot is
// removed. Mirrors the inline query in `ipfsgram bot remove`.
func (s *Store) CarsAccessibleOnlyVia(ctx context.Context, botID int64) ([]Car, error) {
	var cars []Car
	err := s.db.WithContext(ctx).
		Where(`EXISTS (
			SELECT 1 FROM bot_channels bc
			WHERE bc.channel_id = cars.channel_id AND bc.bot_id = ? AND bc.member
		)
		AND NOT EXISTS (
			SELECT 1 FROM bot_channels bc2
			JOIN bots b ON b.id = bc2.bot_id
			WHERE bc2.channel_id = cars.channel_id
			  AND bc2.bot_id <> ? AND bc2.member AND b.active
		)`, botID, botID).
		Order("id").
		Find(&cars).Error
	if err != nil {
		return nil, fmt.Errorf("cars accessible only via bot %d: %w", botID, err)
	}
	return cars, nil
}

// CountCarsByStatus returns the number of cars per status.
func (s *Store) CountCarsByStatus(ctx context.Context) (map[CarStatus]int64, error) {
	var rows []struct {
		Status CarStatus
		Count  int64
	}
	err := s.db.WithContext(ctx).
		Model(&Car{}).
		Select("status, count(*) AS count").
		Group("status").
		Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("count cars by status: %w", err)
	}
	out := make(map[CarStatus]int64, len(rows))
	for _, row := range rows {
		out[row.Status] = row.Count
	}
	return out, nil
}
