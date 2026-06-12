package selector

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ulbwa/ipfsgram/internal/model"
)

func bot(id int64, active bool, unavailableUntil *time.Time) model.Bot {
	return model.Bot{ID: id, Active: active, UnavailableUntil: unavailableUntil}
}

func member(botID int64, canPost, canRead, isMember bool) model.BotChannel {
	return model.BotChannel{BotID: botID, ChannelID: 1, CanPost: canPost, CanRead: canRead, Member: isMember}
}

func timePtr(t time.Time) *time.Time { return &t }

// --- LoadCounter ---

func TestLoadCounterWindow(t *testing.T) {
	lc := NewLoadCounter()
	base := time.Now()
	lc.now = func() time.Time { return base.Add(-10 * time.Minute) }
	lc.Record(1)
	lc.RecordError(1)
	lc.now = func() time.Time { return base }
	lc.Record(1)
	lc.Record(1)
	lc.RecordError(1)

	if got := lc.CountSince(1, 5*time.Minute); got != 2 {
		t.Errorf("CountSince(5m) = %d, want 2", got)
	}
	if got := lc.CountSince(1, time.Hour); got != 3 {
		t.Errorf("CountSince(1h) = %d, want 3", got)
	}
	if got := lc.ErrorsSince(1, 5*time.Minute); got != 1 {
		t.Errorf("ErrorsSince(5m) = %d, want 1", got)
	}
	if got := lc.ErrorsSince(1, time.Hour); got != 2 {
		t.Errorf("ErrorsSince(1h) = %d, want 2", got)
	}
	if got := lc.CountSince(42, time.Hour); got != 0 {
		t.Errorf("CountSince(unknown bot) = %d, want 0", got)
	}
}

func TestLoadCounterPrunesOldEntries(t *testing.T) {
	lc := NewLoadCounter()
	base := time.Now()
	lc.now = func() time.Time { return base.Add(-2 * time.Hour) }
	lc.Record(1)
	lc.RecordError(1)
	lc.now = func() time.Time { return base }
	lc.Record(1) // write triggers prune of >1h-old entries
	lc.RecordError(1)

	lc.mu.Lock()
	defer lc.mu.Unlock()
	if got := len(lc.ops[1]); got != 1 {
		t.Errorf("ops kept = %d, want 1 (old entries pruned)", got)
	}
	if got := len(lc.errs[1]); got != 1 {
		t.Errorf("errs kept = %d, want 1 (old entries pruned)", got)
	}
}

func TestLoadCounterConcurrent(t *testing.T) {
	lc := NewLoadCounter()
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			for range 100 {
				lc.Record(id % 2)
				lc.RecordError(id % 2)
				lc.CountSince(id%2, time.Minute)
				lc.ErrorsSince(id%2, time.Minute)
			}
		}(int64(i))
	}
	wg.Wait()
	total := lc.CountSince(0, time.Hour) + lc.CountSince(1, time.Hour)
	if total != 800 {
		t.Errorf("total records = %d, want 800", total)
	}
}

// --- PickUploadBot ---

func TestPickUploadBotFilters(t *testing.T) {
	now := time.Now()
	bots := []model.Bot{
		bot(1, true, nil),                            // not a member at all
		bot(2, true, nil),                            // Member=false row
		bot(3, true, nil),                            // no can_post
		bot(4, false, nil),                           // inactive
		bot(5, true, timePtr(now.Add(time.Minute))),  // flood-wait
		bot(6, true, timePtr(now.Add(-time.Minute))), // expired flood-wait -> eligible
	}
	members := []model.BotChannel{
		member(2, true, true, false),
		member(3, false, true, true),
		member(4, true, true, true),
		member(5, true, true, true),
		member(6, true, true, true),
	}
	got, err := PickUploadBot(now, bots, members, NewLoadCounter())
	if err != nil {
		t.Fatalf("PickUploadBot: %v", err)
	}
	if got.ID != 6 {
		t.Errorf("picked bot %d, want 6", got.ID)
	}
}

func TestPickUploadBotLeastLoaded(t *testing.T) {
	now := time.Now()
	bots := []model.Bot{bot(1, true, nil), bot(2, true, nil)}
	members := []model.BotChannel{member(1, true, true, true), member(2, true, true, true)}
	lc := NewLoadCounter()
	lc.Record(1)
	lc.Record(1)
	lc.Record(2)
	got, err := PickUploadBot(now, bots, members, lc)
	if err != nil {
		t.Fatalf("PickUploadBot: %v", err)
	}
	if got.ID != 2 {
		t.Errorf("picked bot %d, want least-loaded 2", got.ID)
	}
}

func TestPickUploadBotTieBreakByErrorsThenID(t *testing.T) {
	now := time.Now()
	bots := []model.Bot{bot(3, true, nil), bot(1, true, nil), bot(2, true, nil)}
	members := []model.BotChannel{
		member(1, true, true, true),
		member(2, true, true, true),
		member(3, true, true, true),
	}
	lc := NewLoadCounter()
	lc.RecordError(1) // same load, more errors -> lose tie-break

	got, err := PickUploadBot(now, bots, members, lc)
	if err != nil {
		t.Fatalf("PickUploadBot: %v", err)
	}
	if got.ID != 2 {
		t.Errorf("picked bot %d, want 2 (fewer errors, then smaller ID)", got.ID)
	}

	// All equal -> smallest ID wins.
	got, err = PickUploadBot(now, bots, members, NewLoadCounter())
	if err != nil {
		t.Fatalf("PickUploadBot: %v", err)
	}
	if got.ID != 1 {
		t.Errorf("picked bot %d, want 1 (smallest ID)", got.ID)
	}
}

func TestPickUploadBotNoneAvailable(t *testing.T) {
	now := time.Now()
	bots := []model.Bot{
		bot(1, false, nil),
		bot(2, true, timePtr(now.Add(time.Minute))),
	}
	members := []model.BotChannel{member(1, true, true, true), member(2, true, true, true)}
	_, err := PickUploadBot(now, bots, members, NewLoadCounter())
	if !errors.Is(err, ErrNoBotAvailable) {
		t.Errorf("err = %v, want ErrNoBotAvailable", err)
	}
}

// --- PickDownloadBot ---

func TestPickDownloadBotPrefersFileIDOwner(t *testing.T) {
	now := time.Now()
	bots := []model.Bot{bot(1, true, nil), bot(2, true, nil)}
	members := []model.BotChannel{member(1, false, true, true), member(2, false, true, true)}
	lc := NewLoadCounter()
	lc.Record(1) // owner more loaded than non-owner
	lc.Record(1)
	got, fileID, err := PickDownloadBot(now, bots, members, map[int64]string{1: "file-1"}, lc)
	if err != nil {
		t.Fatalf("PickDownloadBot: %v", err)
	}
	if got.ID != 1 || fileID != "file-1" {
		t.Errorf("picked (%d, %q), want (1, file-1)", got.ID, fileID)
	}
}

func TestPickDownloadBotLeastLoadedOwner(t *testing.T) {
	now := time.Now()
	bots := []model.Bot{bot(1, true, nil), bot(2, true, nil), bot(3, true, nil)}
	members := []model.BotChannel{
		member(1, false, true, true),
		member(2, false, true, true),
		member(3, false, true, true),
	}
	fileIDs := map[int64]string{1: "file-1", 2: "file-2"}
	lc := NewLoadCounter()
	lc.Record(1)
	got, fileID, err := PickDownloadBot(now, bots, members, fileIDs, lc)
	if err != nil {
		t.Fatalf("PickDownloadBot: %v", err)
	}
	if got.ID != 2 || fileID != "file-2" {
		t.Errorf("picked (%d, %q), want (2, file-2)", got.ID, fileID)
	}
}

func TestPickDownloadBotOwnerFloodWaitFallsBack(t *testing.T) {
	now := time.Now()
	bots := []model.Bot{
		bot(1, true, timePtr(now.Add(time.Minute))), // owner in flood-wait
		bot(2, true, nil),
	}
	members := []model.BotChannel{member(1, false, true, true), member(2, false, true, true)}
	got, fileID, err := PickDownloadBot(now, bots, members, map[int64]string{1: "file-1"}, NewLoadCounter())
	if err != nil {
		t.Fatalf("PickDownloadBot: %v", err)
	}
	if got.ID != 2 || fileID != "" {
		t.Errorf("picked (%d, %q), want (2, \"\")", got.ID, fileID)
	}
}

func TestPickDownloadBotRequiresCanRead(t *testing.T) {
	now := time.Now()
	bots := []model.Bot{bot(1, true, nil), bot(2, true, nil)}
	members := []model.BotChannel{
		member(1, true, false, true), // can_post but not can_read
		member(2, false, true, true),
	}
	got, _, err := PickDownloadBot(now, bots, members, nil, NewLoadCounter())
	if err != nil {
		t.Fatalf("PickDownloadBot: %v", err)
	}
	if got.ID != 2 {
		t.Errorf("picked bot %d, want 2", got.ID)
	}
}

func TestPickDownloadBotNoneAvailable(t *testing.T) {
	now := time.Now()
	bots := []model.Bot{bot(1, false, nil)}
	members := []model.BotChannel{member(1, false, true, true)}
	_, _, err := PickDownloadBot(now, bots, members, map[int64]string{1: "file-1"}, NewLoadCounter())
	if !errors.Is(err, ErrNoBotAvailable) {
		t.Errorf("err = %v, want ErrNoBotAvailable", err)
	}
}

// --- PickChannel ---

func TestPickChannelFillFirst(t *testing.T) {
	channels := []model.Channel{
		{ID: 1, MessageCount: 10, MessageLimit: 100, Active: true},
		{ID: 2, MessageCount: 90, MessageLimit: 100, Active: true},
		{ID: 3, MessageCount: 100, MessageLimit: 100, Active: true}, // full
		{ID: 4, MessageCount: 95, MessageLimit: 100, Active: false}, // inactive
	}
	ch, warn, err := PickChannel(channels, 0.95)
	if err != nil {
		t.Fatalf("PickChannel: %v", err)
	}
	if ch.ID != 2 {
		t.Errorf("picked channel %d, want fullest non-full 2", ch.ID)
	}
	if warn {
		t.Errorf("warn = true at 0.90 ratio with 0.95 threshold")
	}
}

func TestPickChannelWarnThreshold(t *testing.T) {
	channels := []model.Channel{
		{ID: 1, MessageCount: 95, MessageLimit: 100, Active: true},
	}
	ch, warn, err := PickChannel(channels, 0.95)
	if err != nil {
		t.Fatalf("PickChannel: %v", err)
	}
	if ch.ID != 1 || !warn {
		t.Errorf("got (ch=%d, warn=%v), want (1, true)", ch.ID, warn)
	}
}

func TestPickChannelTieBreakByID(t *testing.T) {
	channels := []model.Channel{
		{ID: 2, MessageCount: 50, MessageLimit: 100, Active: true},
		{ID: 1, MessageCount: 50, MessageLimit: 100, Active: true},
	}
	ch, _, err := PickChannel(channels, 0.95)
	if err != nil {
		t.Fatalf("PickChannel: %v", err)
	}
	if ch.ID != 1 {
		t.Errorf("picked channel %d, want 1 (smaller ID on tie)", ch.ID)
	}
}

func TestPickChannelNoSpace(t *testing.T) {
	full := []model.Channel{
		{ID: 1, MessageCount: 100, MessageLimit: 100, Active: true},
		{ID: 2, MessageCount: 10, MessageLimit: 100, Active: false},
	}
	if _, _, err := PickChannel(full, 0.95); !errors.Is(err, ErrNoChannelSpace) {
		t.Errorf("err = %v, want ErrNoChannelSpace", err)
	}
	if _, _, err := PickChannel(nil, 0.95); !errors.Is(err, ErrNoChannelSpace) {
		t.Errorf("empty input: err = %v, want ErrNoChannelSpace", err)
	}
}
