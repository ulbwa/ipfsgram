// Local SQL queries the daemon needs that the repo package does not expose:
// channel lookup by internal ID and a cursor scan over all block CIDs.
package daemon

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"

	"github.com/ulbwa/ipfsgram/internal/model"
)

// keyBatchSize is the page size of the keyset scan over the blocks table.
const keyBatchSize = 1000

// dbQueries runs the daemon-local SQL queries over the shared database.
type dbQueries struct {
	db *sqlx.DB
}

// newDBQueries returns daemon-local queries over db.
func newDBQueries(db *sqlx.DB) *dbQueries { return &dbQueries{db: db} }

// ChannelByID returns the channel with the given internal ID.
func (q *dbQueries) ChannelByID(ctx context.Context, id int64) (model.Channel, error) {
	var c model.Channel
	err := q.db.GetContext(ctx, &c, `
		SELECT id, tg_id, title, message_count, message_limit, active
		FROM channels WHERE id = $1`, id)
	if err != nil {
		return model.Channel{}, fmt.Errorf("daemon: get channel %d: %w", id, err)
	}
	return c, nil
}

// AllCIDs streams every CID from the blocks table in keyset-paginated
// batches. The channel is closed when the scan completes, fails, or ctx is
// done. Used by AllKeysChan and the reprovider's KeyChanFunc.
func (q *dbQueries) AllCIDs(ctx context.Context) (<-chan cid.Cid, error) {
	out := make(chan cid.Cid)
	go func() {
		defer close(out)
		after := []byte{} // empty bytea sorts before every cid
		for {
			var page [][]byte
			err := q.db.SelectContext(ctx, &page, `
				SELECT cid FROM blocks
				WHERE cid > $1
				ORDER BY cid
				LIMIT $2`, after, keyBatchSize)
			if err != nil {
				if ctx.Err() == nil {
					log.Error().Err(err).Msg("stream block cids")
				}
				return
			}
			if len(page) == 0 {
				return
			}
			for _, raw := range page {
				c, err := cid.Cast(raw)
				if err != nil {
					log.Error().Err(err).Hex("cid", raw).Msg("invalid cid bytes in blocks table")
					continue
				}
				select {
				case out <- c:
				case <-ctx.Done():
					return
				}
			}
			after = page[len(page)-1]
		}
	}()
	return out, nil
}
