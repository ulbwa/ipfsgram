package maintenance

import (
	"context"
	"io"
	"testing"

	"github.com/rs/zerolog"

	"github.com/ulbwa/ipfsgram/internal/domain"
	"github.com/ulbwa/ipfsgram/internal/port"
)

// passThroughLocker runs fn directly without any real database transaction.
type passThroughLocker struct{ lastExclusive bool }

func (l *passThroughLocker) WithLock(ctx context.Context, exclusive bool, fn func(ctx context.Context) error) error {
	l.lastExclusive = exclusive
	return fn(ctx)
}

// fakeCarRepo records Delete calls and serves a fixed unpinned set.
type fakeCarRepo struct {
	port.CarRepository
	unpinned []domain.Car
	deleted  []int64
}

func (r *fakeCarRepo) UnpinnedCars(context.Context) ([]domain.Car, error) {
	return r.unpinned, nil
}

func (r *fakeCarRepo) Delete(_ context.Context, carID int64) error {
	r.deleted = append(r.deleted, carID)
	return nil
}

type fakeChannelRepo struct {
	port.ChannelRepository
	channels []domain.Channel
	members  map[int64][]domain.BotChannel
}

func (r *fakeChannelRepo) List(context.Context) ([]domain.Channel, error) {
	return r.channels, nil
}

func (r *fakeChannelRepo) MembersOf(_ context.Context, channelID int64) ([]domain.BotChannel, error) {
	return r.members[channelID], nil
}

type fakeBotRepo struct {
	port.BotRepository
	bots []domain.Bot
}

func (r *fakeBotRepo) List(context.Context) ([]domain.Bot, error) { return r.bots, nil }

type fakeTransport struct {
	port.Transport
	deletedMsgs []int64
}

func (t *fakeTransport) DeleteMessage(_ context.Context, _ string, _ int64, messageID int64) error {
	t.deletedMsgs = append(t.deletedMsgs, messageID)
	return nil
}

func TestGCDeletesUnpinnedCar(t *testing.T) {
	msgID := int64(42)
	cars := &fakeCarRepo{
		unpinned: []domain.Car{
			{ID: 1, ChannelID: 10, MessageID: &msgID, Size: 100, Status: domain.CarPublished},
			{ID: 2, ChannelID: 10, Status: domain.CarPending}, // skipped: pending
		},
	}
	channels := &fakeChannelRepo{
		channels: []domain.Channel{{ID: 10, TgID: -100, Title: "ch"}},
		members: map[int64][]domain.BotChannel{
			10: {{BotID: 5, ChannelID: 10, Member: true, CanDelete: true}},
		},
	}
	bots := &fakeBotRepo{bots: []domain.Bot{{ID: 5, Active: true, Token: "t", Username: "b"}}}
	tr := &fakeTransport{}
	locker := &passThroughLocker{}

	s := &Service{
		Cars:      cars,
		Channels:  channels,
		Bots:      bots,
		Transport: tr,
		Locker:    locker,
		Logger:    zerolog.New(io.Discard),
	}

	deleted, err := s.GC(context.Background())
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if deleted != 1 {
		t.Errorf("deleted = %d, want 1", deleted)
	}
	if !locker.lastExclusive {
		t.Errorf("GC must take the lock in exclusive mode")
	}
	if len(tr.deletedMsgs) != 1 || tr.deletedMsgs[0] != msgID {
		t.Errorf("DeleteMessage calls = %v, want [%d]", tr.deletedMsgs, msgID)
	}
	if len(cars.deleted) != 1 || cars.deleted[0] != 1 {
		t.Errorf("Car.Delete calls = %v, want [1]", cars.deleted)
	}
}

func TestGCCandidatesExcludesPending(t *testing.T) {
	msgID := int64(7)
	cars := &fakeCarRepo{
		unpinned: []domain.Car{
			{ID: 1, MessageID: &msgID, Size: 100, Status: domain.CarPublished},
			{ID: 2, Status: domain.CarPending, Size: 999},
		},
	}
	s := &Service{Cars: cars, Logger: zerolog.New(io.Discard)}
	got, total, err := s.GCCandidates(context.Background())
	if err != nil {
		t.Fatalf("GCCandidates: %v", err)
	}
	if len(got) != 1 || got[0].ID != 1 {
		t.Errorf("candidates = %v, want [car 1]", got)
	}
	if total != 100 {
		t.Errorf("total = %d, want 100", total)
	}
}
