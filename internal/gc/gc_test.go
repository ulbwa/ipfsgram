// gc_test.go — unit tests for garbage collection against fakes of the local
// storage and transport interfaces.

package gc

import (
	"context"
	"io"
	"testing"

	"github.com/rs/zerolog"

	"github.com/ulbwa/ipfsgram/internal/store"
)

// fakeStore serves fixed data, records DeleteCar calls and runs the GC lock
// callback directly without any real database transaction. The embedded
// storage interface panics on any method the test does not stub.
type fakeStore struct {
	storage
	unpinned []store.Car
	channels []store.Channel
	members  map[int64][]store.BotChannel
	bots     []store.Bot

	deleted  []int64
	gcLocked bool
}

func (f *fakeStore) WithExclusiveGCLock(ctx context.Context, fn func(ctx context.Context) error) error {
	f.gcLocked = true
	return fn(ctx)
}

func (f *fakeStore) UnpinnedCars(context.Context) ([]store.Car, error) {
	return f.unpinned, nil
}

func (f *fakeStore) DeleteCar(_ context.Context, carID int64) error {
	f.deleted = append(f.deleted, carID)
	return nil
}

func (f *fakeStore) Channels(context.Context) ([]store.Channel, error) {
	return f.channels, nil
}

func (f *fakeStore) ChannelMembers(_ context.Context, channelID int64) ([]store.BotChannel, error) {
	return f.members[channelID], nil
}

func (f *fakeStore) Bots(context.Context) ([]store.Bot, error) { return f.bots, nil }

type fakeTransport struct {
	transport
	deletedMsgs []int64
}

func (t *fakeTransport) DeleteMessage(_ context.Context, _ string, _ int64, messageID int64) error {
	t.deletedMsgs = append(t.deletedMsgs, messageID)
	return nil
}

func TestGCDeletesUnpinnedCar(t *testing.T) {
	msgID := int64(42)
	st := &fakeStore{
		unpinned: []store.Car{
			{ID: 1, ChannelID: 10, MessageID: &msgID, Size: 100, Status: store.CarPublished},
			{ID: 2, ChannelID: 10, Status: store.CarPending}, // skipped: pending
		},
		channels: []store.Channel{{ID: 10, TgID: -100, Title: "ch"}},
		members: map[int64][]store.BotChannel{
			10: {{BotID: 5, ChannelID: 10, Member: true, CanDelete: true}},
		},
		bots: []store.Bot{{ID: 5, Active: true, Token: "t", Username: "b"}},
	}
	tr := &fakeTransport{}

	s := &Service{
		Store:     st,
		Transport: tr,
		Logger:    zerolog.New(io.Discard),
	}

	deleted, err := s.GC(context.Background())
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if deleted != 1 {
		t.Errorf("deleted = %d, want 1", deleted)
	}
	if !st.gcLocked {
		t.Errorf("GC must take the lock in exclusive mode")
	}
	if len(tr.deletedMsgs) != 1 || tr.deletedMsgs[0] != msgID {
		t.Errorf("DeleteMessage calls = %v, want [%d]", tr.deletedMsgs, msgID)
	}
	if len(st.deleted) != 1 || st.deleted[0] != 1 {
		t.Errorf("DeleteCar calls = %v, want [1]", st.deleted)
	}
}

func TestCandidatesExcludesPending(t *testing.T) {
	msgID := int64(7)
	st := &fakeStore{
		unpinned: []store.Car{
			{ID: 1, MessageID: &msgID, Size: 100, Status: store.CarPublished},
			{ID: 2, Status: store.CarPending, Size: 999},
		},
	}
	s := &Service{Store: st, Logger: zerolog.New(io.Discard)}
	got, total, err := s.Candidates(context.Background())
	if err != nil {
		t.Fatalf("Candidates: %v", err)
	}
	if len(got) != 1 || got[0].ID != 1 {
		t.Errorf("candidates = %v, want [car 1]", got)
	}
	if total != 100 {
		t.Errorf("total = %d, want 100", total)
	}
}
