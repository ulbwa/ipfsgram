// doctor_test.go — unit tests for the doctor diagnostics against fakes of the
// store/transport interfaces. No real database or network.

package doctor

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

// fakeStore serves fixed data and records mutations. The embedded storage
// interface panics on any unstubbed method.
type fakeStore struct {
	storage

	orphans       []store.Car
	orphansErr    error
	withStatus    []store.Car
	withStatusErr error
	bots          []store.Bot
	channels      []store.Channel
	members       map[int64][]store.BotChannel // by channel ID
	fileIDs       map[int64]string

	deleteCarErrs map[int64]error

	// recorded mutations
	deletedCars []int64
	setStatus   []statusCall
	upserts     []store.BotChannel
	unavailable map[int64]time.Time
}

type statusCall struct {
	carID int64
	st    store.CarStatus
}

func (f *fakeStore) OrphanPendingCars(_ context.Context, _ time.Duration) ([]store.Car, error) {
	return f.orphans, f.orphansErr
}

func (f *fakeStore) CarsWithStatus(_ context.Context, _ ...store.CarStatus) ([]store.Car, error) {
	return f.withStatus, f.withStatusErr
}

func (f *fakeStore) SetCarStatus(_ context.Context, carID int64, st store.CarStatus) error {
	f.setStatus = append(f.setStatus, statusCall{carID, st})
	return nil
}

func (f *fakeStore) DeleteCar(_ context.Context, carID int64) error {
	f.deletedCars = append(f.deletedCars, carID)
	if f.deleteCarErrs != nil {
		return f.deleteCarErrs[carID]
	}
	return nil
}

func (f *fakeStore) Bots(context.Context) ([]store.Bot, error)         { return f.bots, nil }
func (f *fakeStore) Channels(context.Context) ([]store.Channel, error) { return f.channels, nil }

func (f *fakeStore) ChannelMembers(_ context.Context, channelID int64) ([]store.BotChannel, error) {
	return f.members[channelID], nil
}

func (f *fakeStore) UpsertBotChannel(_ context.Context, bc store.BotChannel) error {
	f.upserts = append(f.upserts, bc)
	return nil
}

func (f *fakeStore) CarFileIDs(_ context.Context, _ int64) (map[int64]string, error) {
	return f.fileIDs, nil
}

func (f *fakeStore) SetBotUnavailable(_ context.Context, id int64, until time.Time) error {
	if f.unavailable == nil {
		f.unavailable = make(map[int64]time.Time)
	}
	f.unavailable[id] = until
	return nil
}

// fakeTransport scripts ProbeChannel and CheckMessage results per bot token.
type fakeTransport struct {
	transport
	probeByToken map[string]probeResult
	checkByToken map[string]error
}

type probeResult struct {
	info telegram.ChannelInfo
	err  error
}

func (t *fakeTransport) ProbeChannel(_ context.Context, token string, _ int64) (telegram.ChannelInfo, error) {
	r := t.probeByToken[token]
	return r.info, r.err
}

func (t *fakeTransport) CheckMessage(_ context.Context, token string, _, _ int64) error {
	if t.checkByToken == nil {
		return nil
	}
	return t.checkByToken[token]
}

func newService(st storage, tr transport) *Service {
	return &Service{
		Store:     st,
		Transport: tr,
		Loads:     selector.NewLoadCounter(),
		Logger:    zerolog.New(io.Discard),
	}
}

func TestDoctorOrphans(t *testing.T) {
	want := []store.Car{{ID: 1, Status: store.CarPending}}
	st := &fakeStore{orphans: want}
	s := newService(st, &fakeTransport{})
	got, err := s.DoctorOrphans(context.Background(), OrphanPendingAge)
	if err != nil {
		t.Fatalf("DoctorOrphans: %v", err)
	}
	if len(got) != 1 || got[0].ID != 1 {
		t.Errorf("orphans = %v, want car 1", got)
	}
}

func TestCleanOrphans(t *testing.T) {
	st := &fakeStore{}
	s := newService(st, &fakeTransport{})
	cars := []store.Car{{ID: 1}, {ID: 2}}
	if err := s.CleanOrphans(context.Background(), cars); err != nil {
		t.Fatalf("CleanOrphans: %v", err)
	}
	if len(st.deletedCars) != 2 {
		t.Errorf("DeleteCar calls = %v, want 2", st.deletedCars)
	}
}

func TestCleanOrphansToleratesNotFound(t *testing.T) {
	st := &fakeStore{deleteCarErrs: map[int64]error{1: store.ErrNotFound}}
	s := newService(st, &fakeTransport{})
	cars := []store.Car{{ID: 1}, {ID: 2}}
	if err := s.CleanOrphans(context.Background(), cars); err != nil {
		t.Fatalf("CleanOrphans: %v, want nil (ErrNotFound tolerated)", err)
	}
}

func TestCleanOrphansSurfacesOtherErrors(t *testing.T) {
	boom := errors.New("boom")
	st := &fakeStore{deleteCarErrs: map[int64]error{1: boom}}
	s := newService(st, &fakeTransport{})
	if err := s.CleanOrphans(context.Background(), []store.Car{{ID: 1}}); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}

// RevalidateMembership: bot 1 gains access (was not member, now member), bot 2
// loses access (was member, now ErrNoAccess => zero ChannelInfo).
func TestRevalidateMembershipDiff(t *testing.T) {
	st := &fakeStore{
		bots: []store.Bot{
			{ID: 1, Username: "gainer", Token: "tg"},
			{ID: 2, Username: "loser", Token: "tl"},
		},
		channels: []store.Channel{{ID: 10, TgID: -100, Title: "Storage"}},
		members: map[int64][]store.BotChannel{
			10: {
				{BotID: 1, ChannelID: 10, Member: false},
				{BotID: 2, ChannelID: 10, Member: true},
			},
		},
	}
	tr := &fakeTransport{probeByToken: map[string]probeResult{
		"tg": {info: telegram.ChannelInfo{Member: true, CanRead: true}},
		"tl": {err: telegram.ErrNoAccess},
	}}
	s := newService(st, tr)

	report, err := s.RevalidateMembership(context.Background())
	if err != nil {
		t.Fatalf("RevalidateMembership: %v", err)
	}
	if len(report.Changes) != 2 {
		t.Fatalf("changes = %v, want 2", report.Changes)
	}
	var gained, lost *MembershipChange
	for i := range report.Changes {
		if report.Changes[i].Gained {
			gained = &report.Changes[i]
		} else {
			lost = &report.Changes[i]
		}
	}
	if gained == nil || gained.BotUsername != "gainer" || gained.ChannelTitle != "Storage" {
		t.Errorf("gained = %v, want gainer/Storage", gained)
	}
	if lost == nil || lost.BotUsername != "loser" {
		t.Errorf("lost = %v, want loser", lost)
	}
	// The fresh matrix is upserted for both bot×channel pairs.
	if len(st.upserts) != 2 {
		t.Errorf("upserts = %d, want 2", len(st.upserts))
	}
}

// No diff when current membership matches the stored matrix.
func TestRevalidateMembershipNoChange(t *testing.T) {
	st := &fakeStore{
		bots:     []store.Bot{{ID: 1, Username: "b", Token: "t"}},
		channels: []store.Channel{{ID: 10, TgID: -100, Title: "C"}},
		members: map[int64][]store.BotChannel{
			10: {{BotID: 1, ChannelID: 10, Member: true}},
		},
	}
	tr := &fakeTransport{probeByToken: map[string]probeResult{
		"t": {info: telegram.ChannelInfo{Member: true}},
	}}
	s := newService(st, tr)
	report, err := s.RevalidateMembership(context.Background())
	if err != nil {
		t.Fatalf("RevalidateMembership: %v", err)
	}
	if len(report.Changes) != 0 {
		t.Errorf("changes = %v, want none", report.Changes)
	}
	if len(st.upserts) != 1 {
		t.Errorf("upserts = %d, want 1", len(st.upserts))
	}
}

func recoverFixture(carStatus store.CarStatus) *fakeStore {
	return &fakeStore{
		withStatus: []store.Car{{ID: 1, ChannelID: 10, MessageID: ptr(int64(99)), Status: carStatus}},
		bots:       []store.Bot{{ID: 1, Active: true, Token: "t", Username: "b"}},
		channels:   []store.Channel{{ID: 10, TgID: -100, Title: "C"}},
		members: map[int64][]store.BotChannel{
			10: {{BotID: 1, ChannelID: 10, Member: true, CanRead: true}},
		},
	}
}

func ptr[T any](v T) *T { return &v }

// Recover: a no_bot_access car whose message is reachable again => restored to
// published.
func TestRecoverRestores(t *testing.T) {
	st := recoverFixture(store.CarNoBotAccess)
	tr := &fakeTransport{} // CheckMessage returns nil => CheckOK
	s := newService(st, tr)
	restored, deleted, err := s.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if restored != 1 || deleted != 0 {
		t.Errorf("restored, deleted = %d, %d, want 1, 0", restored, deleted)
	}
	if len(st.setStatus) != 1 || st.setStatus[0].st != store.CarPublished {
		t.Errorf("SetCarStatus = %v, want published", st.setStatus)
	}
}

// Recover: a deleted message => the car is removed.
func TestRecoverDeletes(t *testing.T) {
	st := recoverFixture(store.CarNoBotAccess)
	tr := &fakeTransport{checkByToken: map[string]error{"t": telegram.ErrMessageDeleted}}
	s := newService(st, tr)
	restored, deleted, err := s.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if restored != 0 || deleted != 1 {
		t.Errorf("restored, deleted = %d, %d, want 0, 1", restored, deleted)
	}
	if len(st.deletedCars) != 1 || st.deletedCars[0] != 1 {
		t.Errorf("DeleteCar = %v, want [1]", st.deletedCars)
	}
}

// Recover: still no access (bot lost access) and car already no_bot_access =>
// unchanged (no status write, no delete).
func TestRecoverStillUnavailableUnchanged(t *testing.T) {
	st := recoverFixture(store.CarNoBotAccess)
	tr := &fakeTransport{checkByToken: map[string]error{"t": telegram.ErrNoAccess}}
	s := newService(st, tr)
	restored, deleted, err := s.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if restored != 0 || deleted != 0 {
		t.Errorf("restored, deleted = %d, %d, want 0, 0", restored, deleted)
	}
	if len(st.setStatus) != 0 {
		t.Errorf("SetCarStatus should not run when status already no_bot_access: %v", st.setStatus)
	}
	if len(st.deletedCars) != 0 {
		t.Errorf("DeleteCar should not run: %v", st.deletedCars)
	}
}

// Recover: a too_large car that is still too large and already too_large =>
// unchanged.
func TestRecoverTooLargeUnchanged(t *testing.T) {
	st := recoverFixture(store.CarTooLarge)
	tr := &fakeTransport{checkByToken: map[string]error{"t": telegram.ErrTooLarge}}
	s := newService(st, tr)
	restored, deleted, err := s.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if restored != 0 || deleted != 0 {
		t.Errorf("restored, deleted = %d, %d, want 0, 0", restored, deleted)
	}
	if len(st.setStatus) != 0 {
		t.Errorf("SetCarStatus should not run: %v", st.setStatus)
	}
}

func TestRecoverNoCars(t *testing.T) {
	st := &fakeStore{withStatus: nil}
	s := newService(st, &fakeTransport{})
	restored, deleted, err := s.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if restored != 0 || deleted != 0 {
		t.Errorf("restored, deleted = %d, %d, want 0, 0", restored, deleted)
	}
}
