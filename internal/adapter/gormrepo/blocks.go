package gormrepo

import (
	"context"
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/ulbwa/ipfsgram/internal/domain"
	"github.com/ulbwa/ipfsgram/internal/port"
)

// BlockRepository is a GORM-backed port.BlockRepository.
type BlockRepository struct {
	gdb *gorm.DB
}

var _ port.BlockRepository = (*BlockRepository)(nil)

// NewBlockRepository returns a BlockRepository over gdb.
func NewBlockRepository(gdb *gorm.DB) *BlockRepository {
	return &BlockRepository{gdb: gdb}
}

// Lookup returns the block for the given CID, or domain.ErrNotFound.
func (r *BlockRepository) Lookup(ctx context.Context, cid []byte) (domain.Block, error) {
	var b domain.Block
	err := r.gdb.WithContext(ctx).Where("cid = ?", cid).Take(&b).Error
	if err != nil {
		return domain.Block{}, notFound("lookup block", err, domain.ErrNotFound)
	}
	return b, nil
}

// Existing returns the blocks that exist for the given CIDs, keyed by
// string(cid). CIDs are looked up in batches.
func (r *BlockRepository) Existing(ctx context.Context, cids [][]byte) (map[string]domain.Block, error) {
	out := make(map[string]domain.Block, len(cids))
	err := chunk(cids, func(batch [][]byte) error {
		var blocks []domain.Block
		if err := r.gdb.WithContext(ctx).Where("cid IN ?", batch).Find(&blocks).Error; err != nil {
			return fmt.Errorf("existing blocks: %w", err)
		}
		for _, b := range blocks {
			out[string(b.CID)] = b
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// InsertBatch inserts the given blocks, silently skipping CIDs that already
// exist (ON CONFLICT(cid) DO NOTHING).
func (r *BlockRepository) InsertBatch(ctx context.Context, blocks []domain.Block) error {
	return chunk(blocks, func(batch []domain.Block) error {
		err := r.gdb.WithContext(ctx).
			Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "cid"}},
				DoNothing: true,
			}).
			Create(&batch).Error
		if err != nil {
			return fmt.Errorf("insert blocks: %w", err)
		}
		return nil
	})
}

// Repoint updates the car_id/offset/length of existing blocks by CID, used when
// a car is re-uploaded and its blocks move. Each batch runs as one transaction
// of per-row updates keyed on cid.
func (r *BlockRepository) Repoint(ctx context.Context, blocks []domain.Block) error {
	return chunk(blocks, func(batch []domain.Block) error {
		err := r.gdb.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			for _, b := range batch {
				res := tx.Model(&domain.Block{}).
					Where("cid = ?", b.CID).
					Updates(map[string]any{
						"car_id": b.CarID,
						"offset": b.Offset,
						"length": b.Length,
					})
				if res.Error != nil {
					return res.Error
				}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("repoint blocks: %w", err)
		}
		return nil
	})
}

// StreamAllCIDs streams every block CID in keyset-paginated batches ordered by
// cid. A goroutine pages the table (WHERE cid > last ORDER BY cid LIMIT N) and
// sends each CID on the returned channel, closing it when the scan completes,
// fails, or ctx is done; any terminal error is delivered on the error channel.
func (r *BlockRepository) StreamAllCIDs(ctx context.Context) (<-chan []byte, <-chan error) {
	out := make(chan []byte)
	errc := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errc)

		var last []byte
		for {
			if err := ctx.Err(); err != nil {
				errc <- err
				return
			}

			q := r.gdb.WithContext(ctx).
				Model(&domain.Block{}).
				Select("cid").
				Order("cid").
				Limit(maxBatch)
			if last != nil {
				q = q.Where("cid > ?", last)
			}

			var page [][]byte
			if err := q.Pluck("cid", &page).Error; err != nil {
				errc <- fmt.Errorf("stream cids: %w", err)
				return
			}
			if len(page) == 0 {
				return
			}

			for _, cid := range page {
				select {
				case out <- cid:
				case <-ctx.Done():
					errc <- ctx.Err()
					return
				}
			}
			last = page[len(page)-1]

			if len(page) < maxBatch {
				return
			}
		}
	}()

	return out, errc
}

// CountAll returns the total number of stored blocks.
func (r *BlockRepository) CountAll(ctx context.Context) (int64, error) {
	var n int64
	if err := r.gdb.WithContext(ctx).Model(&domain.Block{}).Count(&n).Error; err != nil {
		return 0, fmt.Errorf("count blocks: %w", err)
	}
	return n, nil
}
