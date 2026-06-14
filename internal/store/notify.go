// notify.go — PostgreSQL LISTEN/NOTIFY for newly published content. The CLI
// signals new pin roots over a notification channel so running daemons announce
// them to the DHT immediately, instead of waiting for the next reprovide.
package store

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// contentChannel is the PostgreSQL NOTIFY channel carrying hex-encoded root CIDs
// of newly published pins.
const contentChannel = "ipfsgram_content"

// NotifyNewContent announces, over LISTEN/NOTIFY, that a pin root was published,
// so daemons can provide it to the DHT right away. The payload is the
// hex-encoded root CID (well within the 8 KB NOTIFY payload limit).
func (s *Store) NotifyNewContent(ctx context.Context, rootCID []byte) error {
	payload := hex.EncodeToString(rootCID)
	if err := s.db.WithContext(ctx).Exec("SELECT pg_notify(?, ?)", contentChannel, payload).Error; err != nil {
		return fmt.Errorf("notify new content: %w", err)
	}
	return nil
}

// ListenContent opens a dedicated connection, LISTENs on the content channel and
// delivers the root CID bytes of each newly published pin. The returned channel
// is closed when ctx is cancelled or the connection fails; callers that want to
// keep listening should reconnect. A dedicated connection is required because
// LISTEN binds to a single session, which GORM's pooled connections cannot
// guarantee.
func ListenContent(ctx context.Context, dsn string) (<-chan []byte, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("listen connect: %w", err)
	}
	if _, err := conn.Exec(ctx, "LISTEN "+contentChannel); err != nil {
		_ = conn.Close(ctx)
		return nil, fmt.Errorf("listen: %w", err)
	}

	out := make(chan []byte)
	go func() {
		defer close(out)
		defer conn.Close(context.Background())
		for {
			n, err := conn.WaitForNotification(ctx)
			if err != nil {
				return // ctx cancelled or connection lost
			}
			raw, err := hex.DecodeString(n.Payload)
			if err != nil {
				continue
			}
			select {
			case out <- raw:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}
