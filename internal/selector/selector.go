// Package selector implements the fixed (non-configurable) strategies for
// picking bots and channels: least-loaded healthy member for uploads, file_id
// owner first for downloads, fill-first for channels. Per-bot load and error
// counts are kept in a LoadCounter passed by the caller.
package selector

import (
	"errors"
	"time"

	"github.com/ulbwa/ipfsgram/internal/store"
)

// ErrNoBotAvailable is returned when no active, permitted, non-flood-wait bot
// exists for the requested operation.
var ErrNoBotAvailable = errors.New("no bot available")

// ErrNoChannelSpace is returned when no active channel has free message slots.
var ErrNoChannelSpace = errors.New("no channel has free space")

const (
	loadWindow  = 5 * time.Minute
	errorWindow = time.Hour
)

// PickUploadBot picks the least-loaded active bot that is a member of the
// channel with can_post and is not in flood-wait. Ties are broken by fewer
// errors over the last hour, then by smaller bot ID.
func PickUploadBot(now time.Time, bots []store.Bot, members []store.BotChannel, lc *LoadCounter) (store.Bot, error) {
	eligible := eligibleBots(now, bots, members, func(m store.BotChannel) bool { return m.CanPost })
	if len(eligible) == 0 {
		return store.Bot{}, ErrNoBotAvailable
	}
	return pickLeastLoaded(eligible, lc), nil
}

// PickDownloadBot picks a bot to download with. Bots owning a file_id for the
// message are preferred (least-loaded among owners); otherwise the least-loaded
// active member with can_read is picked. The returned string is the picked
// bot's file_id, or "" if it owns none.
func PickDownloadBot(now time.Time, bots []store.Bot, members []store.BotChannel, fileIDs map[int64]string, lc *LoadCounter) (store.Bot, string, error) {
	eligible := eligibleBots(now, bots, members, func(m store.BotChannel) bool { return m.CanRead })
	if len(eligible) == 0 {
		return store.Bot{}, "", ErrNoBotAvailable
	}

	var owners []store.Bot
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
func PickChannel(channels []store.Channel, warnThreshold float64) (store.Channel, bool, error) {
	var (
		best  store.Channel
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
		return store.Channel{}, false, ErrNoChannelSpace
	}
	warn := float64(best.MessageCount)/float64(best.MessageLimit) >= warnThreshold
	return best, warn, nil
}

// eligibleBots returns active bots that are members of the channel (per the
// membership rows), satisfy the permission predicate, and are not in flood-wait
// at the given time.
func eligibleBots(now time.Time, bots []store.Bot, members []store.BotChannel, perm func(store.BotChannel) bool) []store.Bot {
	allowed := make(map[int64]bool, len(members))
	for _, m := range members {
		if m.Member && perm(m) {
			allowed[m.BotID] = true
		}
	}
	var out []store.Bot
	for _, b := range bots {
		if !b.Active || !allowed[b.ID] || inFloodWait(now, b) {
			continue
		}
		out = append(out, b)
	}
	return out
}

func inFloodWait(now time.Time, b store.Bot) bool {
	return b.UnavailableUntil != nil && now.Before(*b.UnavailableUntil)
}

// pickLeastLoaded picks the bot with the fewest operations over loadWindow,
// breaking ties by fewer errors over errorWindow, then by smaller ID.
// candidates must be non-empty.
func pickLeastLoaded(candidates []store.Bot, lc *LoadCounter) store.Bot {
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
