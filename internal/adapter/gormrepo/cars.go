package gormrepo

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/ulbwa/ipfsgram/internal/domain"
	"github.com/ulbwa/ipfsgram/internal/port"
)

// CarRepository is a GORM-backed port.CarRepository.
type CarRepository struct {
	gdb *gorm.DB
}

var _ port.CarRepository = (*CarRepository)(nil)

// NewCarRepository returns a CarRepository over gdb.
func NewCarRepository(gdb *gorm.DB) *CarRepository {
	return &CarRepository{gdb: gdb}
}

// CreatePending inserts a new car in the pending status and returns its ID.
func (r *CarRepository) CreatePending(ctx context.Context, channelID, size int64, blockCount int) (int64, error) {
	c := domain.Car{
		ChannelID:  channelID,
		Size:       size,
		BlockCount: blockCount,
		Status:     domain.CarPending,
	}
	if err := r.gdb.WithContext(ctx).Create(&c).Error; err != nil {
		return 0, fmt.Errorf("create pending car: %w", err)
	}
	return c.ID, nil
}

// MarkPublished sets the car's message ID and moves it to the published status.
func (r *CarRepository) MarkPublished(ctx context.Context, carID, messageID int64) error {
	res := r.gdb.WithContext(ctx).
		Model(&domain.Car{}).
		Where("id = ?", carID).
		Updates(map[string]any{
			"message_id": messageID,
			"status":     domain.CarPublished,
		})
	return requireAffected(res, fmt.Sprintf("mark car %d published", carID), domain.ErrNotFound)
}

// SetStatus sets the car's status.
func (r *CarRepository) SetStatus(ctx context.Context, carID int64, st domain.CarStatus) error {
	res := r.gdb.WithContext(ctx).
		Model(&domain.Car{}).
		Where("id = ?", carID).
		Update("status", st)
	return requireAffected(res, fmt.Sprintf("set car %d status", carID), domain.ErrNotFound)
}

// Get returns the car with the given ID, or domain.ErrNotFound.
func (r *CarRepository) Get(ctx context.Context, carID int64) (domain.Car, error) {
	var c domain.Car
	err := r.gdb.WithContext(ctx).Where("id = ?", carID).Take(&c).Error
	if err != nil {
		return domain.Car{}, notFound(fmt.Sprintf("get car %d", carID), err, domain.ErrNotFound)
	}
	return c, nil
}

// Delete removes the car row; the schema cascades the deletion to its blocks
// and car_file_ids.
func (r *CarRepository) Delete(ctx context.Context, carID int64) error {
	res := r.gdb.WithContext(ctx).Where("id = ?", carID).Delete(&domain.Car{})
	return requireAffected(res, fmt.Sprintf("delete car %d", carID), domain.ErrNotFound)
}

// UpsertFileID inserts or updates the Telegram file_id for the (car, bot) pair.
func (r *CarRepository) UpsertFileID(ctx context.Context, carID, botID int64, fileID string) error {
	row := domain.CarFileID{
		CarID:     carID,
		BotID:     botID,
		FileID:    fileID,
		UpdatedAt: time.Now(),
	}
	err := r.gdb.WithContext(ctx).
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

// DeleteFileID removes the file_id for the (car, bot) pair.
func (r *CarRepository) DeleteFileID(ctx context.Context, carID, botID int64) error {
	err := r.gdb.WithContext(ctx).
		Where("car_id = ? AND bot_id = ?", carID, botID).
		Delete(&domain.CarFileID{}).Error
	if err != nil {
		return fmt.Errorf("delete file_id for car %d bot %d: %w", carID, botID, err)
	}
	return nil
}

// FileIDs returns the bot ID → file_id mapping for the given car.
func (r *CarRepository) FileIDs(ctx context.Context, carID int64) (map[int64]string, error) {
	var rows []domain.CarFileID
	err := r.gdb.WithContext(ctx).
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

// OrphanPending returns cars stuck in the pending status that were created more
// than olderThan ago.
func (r *CarRepository) OrphanPending(ctx context.Context, olderThan time.Duration) ([]domain.Car, error) {
	var cars []domain.Car
	err := r.gdb.WithContext(ctx).
		Where("status = ? AND created_at < now() - make_interval(secs => ?)",
			domain.CarPending, olderThan.Seconds()).
		Order("id").
		Find(&cars).Error
	if err != nil {
		return nil, fmt.Errorf("orphan pending cars: %w", err)
	}
	return cars, nil
}

// UnpinnedCars returns cars none of whose blocks are referenced by any pin —
// i.e. candidates for garbage collection.
func (r *CarRepository) UnpinnedCars(ctx context.Context) ([]domain.Car, error) {
	var cars []domain.Car
	err := r.gdb.WithContext(ctx).
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

// ByStatuses returns cars in any of the given statuses (for doctor recovery).
func (r *CarRepository) ByStatuses(ctx context.Context, statuses ...domain.CarStatus) ([]domain.Car, error) {
	var cars []domain.Car
	if len(statuses) == 0 {
		return cars, nil
	}
	err := r.gdb.WithContext(ctx).
		Where("status IN ?", statuses).
		Order("id").
		Find(&cars).Error
	if err != nil {
		return nil, fmt.Errorf("cars by statuses: %w", err)
	}
	return cars, nil
}

// AccessibleOnlyVia returns cars whose channel has no OTHER active member bot
// besides the given one — i.e. cars that become unreachable if that bot is
// removed. Mirrors the inline query in `ipfsgram bot remove`.
func (r *CarRepository) AccessibleOnlyVia(ctx context.Context, botID int64) ([]domain.Car, error) {
	var cars []domain.Car
	err := r.gdb.WithContext(ctx).
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

// CountByStatus returns the number of cars per status.
func (r *CarRepository) CountByStatus(ctx context.Context) (map[domain.CarStatus]int64, error) {
	var rows []struct {
		Status domain.CarStatus
		Count  int64
	}
	err := r.gdb.WithContext(ctx).
		Model(&domain.Car{}).
		Select("status, count(*) AS count").
		Group("status").
		Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("count cars by status: %w", err)
	}
	out := make(map[domain.CarStatus]int64, len(rows))
	for _, row := range rows {
		out[row.Status] = row.Count
	}
	return out, nil
}
