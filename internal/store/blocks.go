// Store methods for the blocks table: lookup, batched dedup, the ON CONFLICT
// upsert that is the publish path's single write primitive, and the
// keyset-paginated CID stream for the reprovider.
package store

import (
	"context"
	"fmt"

	"gorm.io/gorm/clause"
)

// LookupBlock returns the block for the given CID, or ErrNotFound.
func (s *Store) LookupBlock(ctx context.Context, cid []byte) (BlockRef, error) {
	var b BlockRef
	err := s.db.WithContext(ctx).Where("cid = ?", cid).Take(&b).Error
	if err != nil {
		return BlockRef{}, notFound("lookup block", err)
	}
	return b, nil
}

// ExistingBlocks returns the blocks that exist for the given CIDs, keyed by
// string(cid). CIDs are looked up in batches.
func (s *Store) ExistingBlocks(ctx context.Context, cids [][]byte) (map[string]BlockRef, error) {
	out := make(map[string]BlockRef, len(cids))
	err := chunk(cids, func(batch [][]byte) error {
		var blocks []BlockRef
		if err := s.db.WithContext(ctx).Where("cid IN ?", batch).Find(&blocks).Error; err != nil {
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

// UpsertBlocks inserts the given blocks or, when a CID already exists,
// repoints it to the new car_id/offset/length (ON CONFLICT(cid) DO UPDATE).
// This is the single write primitive of the publish path: correct for
// brand-new blocks, for blocks moved during a re-upload, and for blocks whose
// previous car row (and thus their block rows, via cascade) was deleted
// between the dedup snapshot and this write.
func (s *Store) UpsertBlocks(ctx context.Context, blocks []BlockRef) error {
	return chunk(blocks, func(batch []BlockRef) error {
		err := s.db.WithContext(ctx).
			Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "cid"}},
				DoUpdates: clause.AssignmentColumns([]string{"car_id", "offset", "length"}),
			}).
			Create(&batch).Error
		if err != nil {
			return fmt.Errorf("upsert blocks: %w", err)
		}
		return nil
	})
}

// StreamAllCIDs streams every block CID in keyset-paginated batches ordered by
// cid. A goroutine pages the table (WHERE cid > last ORDER BY cid LIMIT N) and
// sends each CID on the returned channel, closing it when the scan completes,
// fails, or ctx is done; any terminal error is delivered on the error channel.
func (s *Store) StreamAllCIDs(ctx context.Context) (<-chan []byte, <-chan error) {
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

			q := s.db.WithContext(ctx).
				Model(&BlockRef{}).
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

// CountBlocks returns the total number of stored blocks.
func (s *Store) CountBlocks(ctx context.Context) (int64, error) {
	var n int64
	if err := s.db.WithContext(ctx).Model(&BlockRef{}).Count(&n).Error; err != nil {
		return 0, fmt.Errorf("count blocks: %w", err)
	}
	return n, nil
}
