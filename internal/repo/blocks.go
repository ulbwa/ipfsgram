package repo

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/ulbwa/ipfsgram/internal/model"
)

// BlockRepo provides access to the blocks table.
type BlockRepo struct {
	db *sqlx.DB
}

// NewBlockRepo returns a BlockRepo over db.
func NewBlockRepo(db *sqlx.DB) *BlockRepo {
	return &BlockRepo{db: db}
}

// Lookup returns the block reference for the given CID, or ErrNotFound.
func (r *BlockRepo) Lookup(ctx context.Context, cid []byte) (model.BlockRef, error) {
	var ref model.BlockRef
	err := r.db.GetContext(ctx, &ref,
		`SELECT cid, car_id, "offset", length FROM blocks WHERE cid = $1`, cid)
	if err != nil {
		return model.BlockRef{}, notFound("lookup block", err)
	}
	return ref, nil
}

// Existing returns the block references that exist for the given CIDs, keyed
// by string(cid). CIDs are looked up in batches.
func (r *BlockRepo) Existing(ctx context.Context, cids [][]byte) (map[string]model.BlockRef, error) {
	out := make(map[string]model.BlockRef, len(cids))
	err := chunk(cids, func(batch [][]byte) error {
		var refs []model.BlockRef
		err := r.db.SelectContext(ctx, &refs,
			`SELECT cid, car_id, "offset", length FROM blocks WHERE cid = ANY($1)`,
			batch)
		if err != nil {
			return fmt.Errorf("existing blocks: %w", err)
		}
		for _, ref := range refs {
			out[string(ref.CID)] = ref
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// InsertBatch inserts the given block references, silently skipping CIDs that
// already exist (ON CONFLICT DO NOTHING).
func (r *BlockRepo) InsertBatch(ctx context.Context, refs []model.BlockRef) error {
	return chunk(refs, func(batch []model.BlockRef) error {
		cids := make([][]byte, len(batch))
		carIDs := make([]int64, len(batch))
		offsets := make([]int64, len(batch))
		lengths := make([]int32, len(batch))
		for i, ref := range batch {
			cids[i] = ref.CID
			carIDs[i] = ref.CarID
			offsets[i] = ref.Offset
			lengths[i] = ref.Length
		}
		_, err := r.db.ExecContext(ctx, `
			INSERT INTO blocks (cid, car_id, "offset", length)
			SELECT * FROM unnest($1::bytea[], $2::bigint[], $3::bigint[], $4::integer[])
			ON CONFLICT (cid) DO NOTHING`,
			cids, carIDs, offsets, lengths)
		if err != nil {
			return fmt.Errorf("insert blocks: %w", err)
		}
		return nil
	})
}

// Repoint updates the car_id/offset/length of existing blocks by CID, used
// when a car is re-uploaded and its blocks move.
func (r *BlockRepo) Repoint(ctx context.Context, refs []model.BlockRef) error {
	return chunk(refs, func(batch []model.BlockRef) error {
		cids := make([][]byte, len(batch))
		carIDs := make([]int64, len(batch))
		offsets := make([]int64, len(batch))
		lengths := make([]int32, len(batch))
		for i, ref := range batch {
			cids[i] = ref.CID
			carIDs[i] = ref.CarID
			offsets[i] = ref.Offset
			lengths[i] = ref.Length
		}
		_, err := r.db.ExecContext(ctx, `
			UPDATE blocks b SET
				car_id   = u.car_id,
				"offset" = u.off,
				length   = u.len
			FROM unnest($1::bytea[], $2::bigint[], $3::bigint[], $4::integer[])
				AS u(cid, car_id, off, len)
			WHERE b.cid = u.cid`,
			cids, carIDs, offsets, lengths)
		if err != nil {
			return fmt.Errorf("repoint blocks: %w", err)
		}
		return nil
	})
}
