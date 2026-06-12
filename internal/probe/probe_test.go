// probe_test.go — unit tests for the CAR-message check and its error
// classification, against fakes of the store/transport interfaces. No real
// network or database.

package probe

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/ulbwa/ipfsgram/internal/selector"
	"github.com/ulbwa/ipfsgram/internal/store"
	"github.com/ulbwa/ipfsgram/internal/telegram"
)

// fakeStore serves fixed membership/file_id data and records flood-wait writes.
// The embedded storage interface panics on any unstubbed method.
type fakeStore struct {
	storage
	members     []store.BotChannel
	membersErr  error
	fileIDs     map[int64]string
	fileIDsErr  error
	unavailable map[int64]time.Time
}

func (f *fakeStore) ChannelMembers(context.Context, int64) ([]store.BotChannel, error) {
	return f.members, f.membersErr
}

func (f *fakeStore) CarFileIDs(context.Context, int64) (map[int64]string, error) {
	return f.fileIDs, f.fileIDsErr
}

func (f *fakeStore) SetBotUnavailable(_ context.Context, id int64, until time.Time) error {
	if f.unavailable == nil {
		f.unavailable = make(map[int64]time.Time)
	}
	f.unavailable[id] = until
	return nil
}

// fakeTransport returns a scripted error per bot token and records which token
// was used on each call.
type fakeTransport struct {
	transport
	errByToken map[string]error
	usedTokens []string
}

func (t *fakeTransport) CheckMessage(_ context.Context, token string, _, _ int64) error {
	t.usedTokens = append(t.usedTokens, token)
	if t.errByToken == nil {
		return nil
	}
	return t.errByToken[token]
}

func newService(st storage, tr transport) *Service {
	return &Service{
		Store:     st,
		Transport: tr,
		Loads:     selector.NewLoadCounter(),
		Logger:    zerolog.New(io.Discard),
	}
}

func member(botID int64) store.BotChannel {
	return store.BotChannel{BotID: botID, Member: true, CanRead: true}
}

func bot(id int64, token string) store.Bot {
	return store.Bot{ID: id, Active: true, Token: token, Username: token}
}

func publishedCar() store.Car {
	msgID := int64(99)
	return store.Car{ID: 1, ChannelID: 10, MessageID: &msgID, Status: store.CarPublished}
}

func TestCarMessageNoMessageID(t *testing.T) {
	st := &fakeStore{}
	s := newService(st, &fakeTransport{})
	car := store.Car{ID: 1, ChannelID: 10} // MessageID nil
	check, err := s.CarMessage(context.Background(), car, store.Channel{ID: 10}, nil)
	if err != nil {
		t.Fatalf("CarMessage: %v", err)
	}
	if check != CheckNoAccess {
		t.Errorf("check = %v, want CheckNoAccess", check)
	}
}

func TestCarMessageOK(t *testing.T) {
	st := &fakeStore{members: []store.BotChannel{member(1)}}
	tr := &fakeTransport{} // nil error => OK
	s := newService(st, tr)
	check, err := s.CarMessage(context.Background(), publishedCar(), store.Channel{ID: 10, TgID: -100}, []store.Bot{bot(1, "t1")})
	if err != nil {
		t.Fatalf("CarMessage: %v", err)
	}
	if check != CheckOK {
		t.Errorf("check = %v, want CheckOK", check)
	}
}

func TestCarMessageDeleted(t *testing.T) {
	st := &fakeStore{members: []store.BotChannel{member(1)}}
	tr := &fakeTransport{errByToken: map[string]error{"t1": telegram.ErrMessageDeleted}}
	s := newService(st, tr)
	check, err := s.CarMessage(context.Background(), publishedCar(), store.Channel{ID: 10}, []store.Bot{bot(1, "t1")})
	if err != nil {
		t.Fatalf("CarMessage: %v", err)
	}
	if check != CheckDeleted {
		t.Errorf("check = %v, want CheckDeleted", check)
	}
}

func TestCarMessageTooLarge(t *testing.T) {
	st := &fakeStore{members: []store.BotChannel{member(1)}}
	tr := &fakeTransport{errByToken: map[string]error{"t1": telegram.ErrTooLarge}}
	s := newService(st, tr)
	check, err := s.CarMessage(context.Background(), publishedCar(), store.Channel{ID: 10}, []store.Bot{bot(1, "t1")})
	if err != nil {
		t.Fatalf("CarMessage: %v", err)
	}
	if check != CheckTooLarge {
		t.Errorf("check = %v, want CheckTooLarge", check)
	}
}

// ErrNoAccess drops the bot from the candidate list; with only one bot the probe
// then has no candidates and reports CheckNoAccess.
func TestCarMessageNoAccessDropsBot(t *testing.T) {
	st := &fakeStore{members: []store.BotChannel{member(1)}}
	tr := &fakeTransport{errByToken: map[string]error{"t1": telegram.ErrNoAccess}}
	s := newService(st, tr)
	check, err := s.CarMessage(context.Background(), publishedCar(), store.Channel{ID: 10}, []store.Bot{bot(1, "t1")})
	if err != nil {
		t.Fatalf("CarMessage: %v", err)
	}
	if check != CheckNoAccess {
		t.Errorf("check = %v, want CheckNoAccess", check)
	}
}

// No eligible bot at all (no members) => no_bot_access.
func TestCarMessageNoEligibleBot(t *testing.T) {
	st := &fakeStore{members: nil}
	s := newService(st, &fakeTransport{})
	check, err := s.CarMessage(context.Background(), publishedCar(), store.Channel{ID: 10}, []store.Bot{bot(1, "t1")})
	if err != nil {
		t.Fatalf("CarMessage: %v", err)
	}
	if check != CheckNoAccess {
		t.Errorf("check = %v, want CheckNoAccess", check)
	}
}

// A flood-wait on the first bot marks it unavailable (SetBotUnavailable) and the
// probe falls through to the next bot, which succeeds.
func TestCarMessageFloodWaitTriesNextBot(t *testing.T) {
	st := &fakeStore{members: []store.BotChannel{member(1), member(2)}}
	tr := &fakeTransport{errByToken: map[string]error{
		"t1": &telegram.FloodWaitError{RetryAfter: time.Hour},
		// t2 returns nil => OK
	}}
	s := newService(st, tr)
	check, err := s.CarMessage(context.Background(), publishedCar(),
		store.Channel{ID: 10, TgID: -100}, []store.Bot{bot(1, "t1"), bot(2, "t2")})
	if err != nil {
		t.Fatalf("CarMessage: %v", err)
	}
	if check != CheckOK {
		t.Errorf("check = %v, want CheckOK", check)
	}
	if _, ok := st.unavailable[1]; !ok {
		t.Errorf("SetBotUnavailable must be recorded for bot 1; got %v", st.unavailable)
	}
	// Both bots should have been tried, t1 first then t2.
	if len(tr.usedTokens) != 2 || tr.usedTokens[0] != "t1" || tr.usedTokens[1] != "t2" {
		t.Errorf("usedTokens = %v, want [t1 t2]", tr.usedTokens)
	}
}

// Bot selection prefers the file_id owner. Two eligible bots, only bot 2 owns a
// file_id, so it is probed first.
func TestCarMessagePrefersFileIDOwner(t *testing.T) {
	st := &fakeStore{
		members: []store.BotChannel{member(1), member(2)},
		fileIDs: map[int64]string{2: "file-2"},
	}
	tr := &fakeTransport{} // both return OK
	s := newService(st, tr)
	check, err := s.CarMessage(context.Background(), publishedCar(),
		store.Channel{ID: 10, TgID: -100}, []store.Bot{bot(1, "t1"), bot(2, "t2")})
	if err != nil {
		t.Fatalf("CarMessage: %v", err)
	}
	if check != CheckOK {
		t.Errorf("check = %v, want CheckOK", check)
	}
	if len(tr.usedTokens) != 1 || tr.usedTokens[0] != "t2" {
		t.Errorf("usedTokens = %v, want [t2] (file_id owner preferred)", tr.usedTokens)
	}
}

// When every eligible bot is merely flood-waited, the probe enters the
// wait-loop. A cancelled context must make it return promptly with ctx.Err().
func TestCarMessageFloodWaitWaitLoopRespectsContext(t *testing.T) {
	until := time.Now().Add(time.Hour)
	b := bot(1, "t1")
	b.UnavailableUntil = &until
	st := &fakeStore{members: []store.BotChannel{member(1)}}
	tr := &fakeTransport{} // never reached: bot already flood-waited
	s := newService(st, tr)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before the call so the wait-loop returns immediately

	done := make(chan struct{})
	var check Check
	var err error
	go func() {
		check, err = s.CarMessage(ctx, publishedCar(), store.Channel{ID: 10, TgID: -100}, []store.Bot{b})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("CarMessage did not return promptly on context cancel")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if check != CheckNoAccess {
		t.Errorf("check = %v, want CheckNoAccess", check)
	}
}
