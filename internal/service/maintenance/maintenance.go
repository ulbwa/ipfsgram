// Package maintenance implements IPFSgram's housekeeping workflows: unpinning
// roots (`ipfsgram rm`), garbage-collecting unpinned CARs (`ipfsgram gc`), and
// the doctor's diagnostics — orphaned pending cars, bot×channel membership
// revalidation, and recovery of cars marked unavailable. It depends only on the
// port interfaces and the domain layer. Garbage collection is split into a
// lock-free GCCandidates step (so the caller can preview and confirm) and a
// GC step that re-fetches and deletes under the exclusive advisory lock. All
// diagnostics go to a zerolog.Logger; user-facing text is the caller's job.
package maintenance

import (
	"context"
	"time"

	"github.com/rs/zerolog"

	"github.com/ulbwa/ipfsgram/internal/port"
)

// OrphanPendingAge is how old a pending car must be to count as orphaned.
const OrphanPendingAge = 15 * time.Minute

// Service performs maintenance. Its dependencies are all ports.
type Service struct {
	Cars      port.CarRepository
	Channels  port.ChannelRepository
	Bots      port.BotRepository
	Pins      port.PinRepository
	Transport port.Transport
	Selector  port.Selector
	Locker    port.Locker
	Logger    zerolog.Logger
}

// Unpin removes the pin with the given root. domain.ErrNotFound passes through
// so the caller can report "pin not found".
func (s *Service) Unpin(ctx context.Context, root []byte) error {
	return s.Pins.Remove(ctx, root)
}
