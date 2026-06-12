// Package selector is the selection adapter: it implements the fixed
// (non-configurable) strategies for picking bots and channels behind the
// port.Selector contract: least-loaded healthy member for uploads, file_id
// owner first for downloads, fill-first for channels. Per-bot load and error
// counts are kept as internal state.
package selector

import (
	"time"

	"github.com/ulbwa/ipfsgram/internal/domain"
	"github.com/ulbwa/ipfsgram/internal/port"
)

// Compile-time assertion that Selector satisfies its port.
var _ port.Selector = (*Selector)(nil)

const (
	loadWindow  = 5 * time.Minute
	errorWindow = time.Hour
)

// Selector implements port.Selector. It owns an internal load counter used for
// least-loaded tie-breaking across uploads and downloads.
type Selector struct {
	lc *loadCounter
}

// New returns a Selector with an empty internal load counter.
func New() *Selector {
	return &Selector{lc: newLoadCounter()}
}

// PickUploadBot picks the least-loaded active bot that is a member of the
// channel with can_post and is not in flood-wait. Ties are broken by fewer
// errors over the last hour, then by smaller bot ID.
func (s *Selector) PickUploadBot(now time.Time, bots []domain.Bot, members []domain.BotChannel) (domain.Bot, error) {
	eligible := eligibleBots(now, bots, members, func(m domain.BotChannel) bool { return m.CanPost })
	if len(eligible) == 0 {
		return domain.Bot{}, port.ErrNoBotAvailable
	}
	return s.pickLeastLoaded(eligible), nil
}

// PickDownloadBot picks a bot to download with. Bots owning a file_id for the
// message are preferred (least-loaded among owners); otherwise the least-loaded
// active member with can_read is picked. The returned string is the picked
// bot's file_id, or "" if it owns none.
func (s *Selector) PickDownloadBot(now time.Time, bots []domain.Bot, members []domain.BotChannel, fileIDs map[int64]string) (domain.Bot, string, error) {
	eligible := eligibleBots(now, bots, members, func(m domain.BotChannel) bool { return m.CanRead })
	if len(eligible) == 0 {
		return domain.Bot{}, "", port.ErrNoBotAvailable
	}

	var owners []domain.Bot
	for _, b := range eligible {
		if fileIDs[b.ID] != "" {
			owners = append(owners, b)
		}
	}
	if len(owners) > 0 {
		picked := s.pickLeastLoaded(owners)
		return picked, fileIDs[picked.ID], nil
	}
	return s.pickLeastLoaded(eligible), "", nil
}

// PickChannel implements fill-first: among active channels with
// message_count < message_limit it picks the one with the highest
// message_count (smaller ID on ties). warn is true when the picked channel's
// fill ratio is at or above warnThreshold.
func (s *Selector) PickChannel(channels []domain.Channel, warnThreshold float64) (domain.Channel, bool, error) {
	var (
		best  domain.Channel
		found bool
	)
	for _, ch := range channels {
		if !ch.Active || ch.MessageCount >= ch.MessageLimit {
			continue
		}
		if !found || ch.MessageCount > best.MessageCount ||
			(ch.MessageCount == best.MessageCount && ch.ID < best.ID) {
			best = ch
			found = true
		}
	}
	if !found {
		return domain.Channel{}, false, port.ErrNoChannelSpace
	}
	warn := float64(best.MessageCount)/float64(best.MessageLimit) >= warnThreshold
	return best, warn, nil
}

// Record notes a successful operation by a bot (for load balancing).
func (s *Selector) Record(botID int64) { s.lc.Record(botID) }

// RecordError notes an errored operation by a bot (for tie-breaking).
func (s *Selector) RecordError(botID int64) { s.lc.RecordError(botID) }

// eligibleBots returns active bots that are members of the channel (per the
// membership rows), satisfy the permission predicate, and are not in flood-wait
// at the given time.
func eligibleBots(now time.Time, bots []domain.Bot, members []domain.BotChannel, perm func(domain.BotChannel) bool) []domain.Bot {
	allowed := make(map[int64]bool, len(members))
	for _, m := range members {
		if m.Member && perm(m) {
			allowed[m.BotID] = true
		}
	}
	var out []domain.Bot
	for _, b := range bots {
		if !b.Active || !allowed[b.ID] || inFloodWait(now, b) {
			continue
		}
		out = append(out, b)
	}
	return out
}

func inFloodWait(now time.Time, b domain.Bot) bool {
	return b.UnavailableUntil != nil && now.Before(*b.UnavailableUntil)
}

// pickLeastLoaded picks the bot with the fewest operations over loadWindow,
// breaking ties by fewer errors over errorWindow, then by smaller ID.
// candidates must be non-empty.
func (s *Selector) pickLeastLoaded(candidates []domain.Bot) domain.Bot {
	best := candidates[0]
	bestLoad := s.lc.CountSince(best.ID, loadWindow)
	bestErrs := s.lc.ErrorsSince(best.ID, errorWindow)
	for _, b := range candidates[1:] {
		load := s.lc.CountSince(b.ID, loadWindow)
		errs := s.lc.ErrorsSince(b.ID, errorWindow)
		if load < bestLoad ||
			(load == bestLoad && errs < bestErrs) ||
			(load == bestLoad && errs == bestErrs && b.ID < best.ID) {
			best, bestLoad, bestErrs = b, load, errs
		}
	}
	return best
}
