// remove_test.go — unit tests for the removal workflows against a spy fake of
// the store interface. No real database.

package remove

import (
	"context"
	"errors"
	"testing"

	"github.com/ulbwa/ipfsgram/internal/store"
)

// fakeStore records calls and serves fixed data. The embedded storage interface
// panics on any method a test does not stub.
type fakeStore struct {
	storage

	// canned returns
	statsPins  int64
	statsBytes int64
	statsErr   error

	orphanPins    int64
	orphanPinsErr error

	accessibleCars []store.Car
	accessibleErr  error

	removeChannelErr error
	removeBotErr     error
	removePinErr     error
	deleteCarErrs    map[int64]error

	// recorded calls
	removePinRoots     [][]byte
	channelStatsCalls  []int64
	removeChannelCalls []int64
	deleteOrphanCalls  int
	accessibleCalls    []int64
	removeBotCalls     []int64
	deleteCarCalls     []int64
}

func (f *fakeStore) RemovePin(_ context.Context, root []byte) error {
	f.removePinRoots = append(f.removePinRoots, root)
	return f.removePinErr
}

func (f *fakeStore) ChannelRemoveStats(_ context.Context, channelID int64) (int64, int64, error) {
	f.channelStatsCalls = append(f.channelStatsCalls, channelID)
	return f.statsPins, f.statsBytes, f.statsErr
}

func (f *fakeStore) RemoveChannel(_ context.Context, id int64) error {
	f.removeChannelCalls = append(f.removeChannelCalls, id)
	return f.removeChannelErr
}

func (f *fakeStore) DeleteOrphanPins(_ context.Context) (int64, error) {
	f.deleteOrphanCalls++
	return f.orphanPins, f.orphanPinsErr
}

func (f *fakeStore) CarsAccessibleOnlyVia(_ context.Context, botID int64) ([]store.Car, error) {
	f.accessibleCalls = append(f.accessibleCalls, botID)
	return f.accessibleCars, f.accessibleErr
}

func (f *fakeStore) RemoveBot(_ context.Context, id int64) error {
	f.removeBotCalls = append(f.removeBotCalls, id)
	return f.removeBotErr
}

func (f *fakeStore) DeleteCar(_ context.Context, carID int64) error {
	f.deleteCarCalls = append(f.deleteCarCalls, carID)
	if f.deleteCarErrs != nil {
		return f.deleteCarErrs[carID]
	}
	return nil
}

func TestUnpin(t *testing.T) {
	st := &fakeStore{}
	s := &Service{Store: st}
	root := []byte("root-cid")
	if err := s.Unpin(context.Background(), root); err != nil {
		t.Fatalf("Unpin: %v", err)
	}
	if len(st.removePinRoots) != 1 || string(st.removePinRoots[0]) != "root-cid" {
		t.Errorf("RemovePin calls = %v", st.removePinRoots)
	}
}

func TestUnpinSurfacesNotFound(t *testing.T) {
	st := &fakeStore{removePinErr: store.ErrNotFound}
	s := &Service{Store: st}
	if err := s.Unpin(context.Background(), []byte("x")); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Unpin err = %v, want ErrNotFound", err)
	}
}

func TestChannelRemovePlan(t *testing.T) {
	st := &fakeStore{statsPins: 3, statsBytes: 4096}
	s := &Service{Store: st}
	pins, bytes, err := s.ChannelRemovePlan(context.Background(), 42)
	if err != nil {
		t.Fatalf("ChannelRemovePlan: %v", err)
	}
	if pins != 3 || bytes != 4096 {
		t.Errorf("pins, bytes = %d, %d, want 3, 4096", pins, bytes)
	}
	if len(st.channelStatsCalls) != 1 || st.channelStatsCalls[0] != 42 {
		t.Errorf("ChannelRemoveStats calls = %v, want [42]", st.channelStatsCalls)
	}
}

func TestChannelRemoveExecute(t *testing.T) {
	st := &fakeStore{}
	s := &Service{Store: st}
	if err := s.ChannelRemoveExecute(context.Background(), 7); err != nil {
		t.Fatalf("ChannelRemoveExecute: %v", err)
	}
	if len(st.removeChannelCalls) != 1 || st.removeChannelCalls[0] != 7 {
		t.Errorf("RemoveChannel calls = %v, want [7]", st.removeChannelCalls)
	}
	if st.deleteOrphanCalls != 1 {
		t.Errorf("DeleteOrphanPins calls = %d, want 1", st.deleteOrphanCalls)
	}
}

func TestChannelRemoveExecuteSkipsOrphanCleanupOnError(t *testing.T) {
	st := &fakeStore{removeChannelErr: store.ErrNotFound}
	s := &Service{Store: st}
	if err := s.ChannelRemoveExecute(context.Background(), 7); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	if st.deleteOrphanCalls != 0 {
		t.Errorf("DeleteOrphanPins must not run after RemoveChannel error")
	}
}

func TestBotRemovePlan(t *testing.T) {
	st := &fakeStore{accessibleCars: []store.Car{
		{ID: 1, Size: 100},
		{ID: 2, Size: 250},
	}}
	s := &Service{Store: st}
	affected, bytes, err := s.BotRemovePlan(context.Background(), 9)
	if err != nil {
		t.Fatalf("BotRemovePlan: %v", err)
	}
	if len(affected) != 2 {
		t.Errorf("affected = %d cars, want 2", len(affected))
	}
	if bytes != 350 {
		t.Errorf("bytes = %d, want 350", bytes)
	}
	if len(st.accessibleCalls) != 1 || st.accessibleCalls[0] != 9 {
		t.Errorf("CarsAccessibleOnlyVia calls = %v, want [9]", st.accessibleCalls)
	}
}

func TestBotRemovePlanError(t *testing.T) {
	st := &fakeStore{accessibleErr: errors.New("boom")}
	s := &Service{Store: st}
	if _, _, err := s.BotRemovePlan(context.Background(), 1); err == nil {
		t.Fatal("BotRemovePlan: want error")
	}
}

func TestBotRemoveExecute(t *testing.T) {
	st := &fakeStore{}
	s := &Service{Store: st}
	if err := s.BotRemoveExecute(context.Background(), 5); err != nil {
		t.Fatalf("BotRemoveExecute: %v", err)
	}
	if len(st.removeBotCalls) != 1 || st.removeBotCalls[0] != 5 {
		t.Errorf("RemoveBot calls = %v, want [5]", st.removeBotCalls)
	}
}

func TestPurgeCars(t *testing.T) {
	st := &fakeStore{}
	s := &Service{Store: st}
	cars := []store.Car{{ID: 1}, {ID: 2}, {ID: 3}}
	if err := s.PurgeCars(context.Background(), cars); err != nil {
		t.Fatalf("PurgeCars: %v", err)
	}
	if len(st.deleteCarCalls) != 3 {
		t.Errorf("DeleteCar calls = %v, want 3", st.deleteCarCalls)
	}
}

func TestPurgeCarsToleratesNotFound(t *testing.T) {
	st := &fakeStore{deleteCarErrs: map[int64]error{2: store.ErrNotFound}}
	s := &Service{Store: st}
	cars := []store.Car{{ID: 1}, {ID: 2}, {ID: 3}}
	if err := s.PurgeCars(context.Background(), cars); err != nil {
		t.Fatalf("PurgeCars: %v, want nil (ErrNotFound tolerated)", err)
	}
	if len(st.deleteCarCalls) != 3 {
		t.Errorf("DeleteCar must be called for all cars, got %v", st.deleteCarCalls)
	}
}

func TestPurgeCarsSurfacesOtherErrors(t *testing.T) {
	boom := errors.New("disk full")
	st := &fakeStore{deleteCarErrs: map[int64]error{2: boom}}
	s := &Service{Store: st}
	cars := []store.Car{{ID: 1}, {ID: 2}, {ID: 3}}
	err := s.PurgeCars(context.Background(), cars)
	if !errors.Is(err, boom) {
		t.Fatalf("PurgeCars err = %v, want %v", err, boom)
	}
	// Should stop at the failing car.
	if len(st.deleteCarCalls) != 2 {
		t.Errorf("DeleteCar calls = %v, want stop after car 2", st.deleteCarCalls)
	}
}
