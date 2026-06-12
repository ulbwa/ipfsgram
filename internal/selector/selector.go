// Package selector implements the fixed (non-configurable) strategies for
// picking bots and channels: least-loaded healthy member for uploads,
// file_id owner first for downloads, fill-first for channels.
package selector

import (
	"errors"
	"time"

	"github.com/ulbwa/ipfsgram/internal/model"
)

var (
	// ErrNoBotAvailable is returned when no bot passes the eligibility filters.
	ErrNoBotAvailable = errors.New("selector: no bot available")
	// ErrNoChannelSpace is returned when no active channel has free space.
	ErrNoChannelSpace = errors.New("selector: no channel has free space")
)

const (
	loadWindow  = 5 * time.Minute
	errorWindow = time.Hour
)

// PickUploadBot picks the least-loaded active bot that is a member of the
// channel with can_post and is not in flood-wait. Ties are broken by fewer
// errors over the last hour, then by smaller bot ID.
func PickUploadBot(now time.Time, bots []model.Bot, members []model.BotChannel, lc *LoadCounter) (model.Bot, error) {
	eligible := eligibleBots(now, bots, members, func(m model.BotChannel) bool { return m.CanPost })
	if len(eligible) == 0 {
		return model.Bot{}, ErrNoBotAvailable
	}
	return pickLeastLoaded(eligible, lc), nil
}

// PickDownloadBot picks a bot to download with. Bots owning a file_id for the
// message are preferred (least-loaded among owners); otherwise the
// least-loaded active member with can_read is picked. The returned string is
// the picked bot's file_id, or "" if it owns none.
func PickDownloadBot(now time.Time, bots []model.Bot, members []model.BotChannel, fileIDs map[int64]string, lc *LoadCounter) (model.Bot, string, error) {
	eligible := eligibleBots(now, bots, members, func(m model.BotChannel) bool { return m.CanRead })
	if len(eligible) == 0 {
		return model.Bot{}, "", ErrNoBotAvailable
	}

	var owners []model.Bot
	for _, b := range eligible {
		if fileIDs[b.ID] != "" {
			owners = append(owners, b)
		}
	}
	if len(owners) > 0 {
		picked := pickLeastLoaded(owners, lc)
		return picked, fileIDs[picked.ID], nil
	}
	return pickLeastLoaded(eligible, lc), "", nil
}

// PickChannel implements fill-first: among active channels with
// message_count < message_limit it picks the one with the highest
// message_count (smaller ID on ties). warn is true when the picked channel's
// fill ratio is at or above warnThreshold.
func PickChannel(channels []model.Channel, warnThreshold float64) (model.Channel, bool, error) {
	var (
		best  model.Channel
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
		return model.Channel{}, false, ErrNoChannelSpace
	}
	warn := float64(best.MessageCount)/float64(best.MessageLimit) >= warnThreshold
	return best, warn, nil
}

// eligibleBots returns active bots that are members of the channel (per the
// membership rows), satisfy the permission predicate, and are not in
// flood-wait at the given time.
func eligibleBots(now time.Time, bots []model.Bot, members []model.BotChannel, perm func(model.BotChannel) bool) []model.Bot {
	allowed := make(map[int64]bool, len(members))
	for _, m := range members {
		if m.Member && perm(m) {
			allowed[m.BotID] = true
		}
	}
	var out []model.Bot
	for _, b := range bots {
		if !b.Active || !allowed[b.ID] || inFloodWait(now, b) {
			continue
		}
		out = append(out, b)
	}
	return out
}

func inFloodWait(now time.Time, b model.Bot) bool {
	return b.UnavailableUntil != nil && now.Before(*b.UnavailableUntil)
}

// pickLeastLoaded picks the bot with the fewest operations over loadWindow,
// breaking ties by fewer errors over errorWindow, then by smaller ID.
// candidates must be non-empty.
func pickLeastLoaded(candidates []model.Bot, lc *LoadCounter) model.Bot {
	best := candidates[0]
	bestLoad := lc.CountSince(best.ID, loadWindow)
	bestErrs := lc.ErrorsSince(best.ID, errorWindow)
	for _, b := range candidates[1:] {
		load := lc.CountSince(b.ID, loadWindow)
		errs := lc.ErrorsSince(b.ID, errorWindow)
		if load < bestLoad ||
			(load == bestLoad && errs < bestErrs) ||
			(load == bestLoad && errs == bestErrs && b.ID < best.ID) {
			best, bestLoad, bestErrs = b, load, errs
		}
	}
	return best
}
