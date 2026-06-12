// Integration tests for the Store, run against the PostgreSQL instance at
// IPFSGRAM_TEST_DSN (skipped when unset). The schema is reset and re-migrated
// in a per-package database so packages can run in parallel.
package store

import (
	"context"
	"errors"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ulbwa/ipfsgram/db"
)

// testStore connects to the database at IPFSGRAM_TEST_DSN, resets the public
// schema and applies all migrations. Tests are skipped when the variable is
// unset. The database name from the DSN gets a per-package "_store" suffix so
// this package and others that reset the public schema can run in parallel
// under `go test ./...` without racing on a shared database.
func testStore(t *testing.T) *Store {
	t.Helper()

	dsn := os.Getenv("IPFSGRAM_TEST_DSN")
	if dsn == "" {
		t.Skip("IPFSGRAM_TEST_DSN not set; skipping store integration tests")
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse IPFSGRAM_TEST_DSN: %v", err)
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = "/ipfsgram_test"
	}
	u.Path += "_store"
	dsn = u.String()

	// Migrate first: dbmate creates the database if it does not exist yet.
	if err := db.Migrate(dsn); err != nil {
		t.Fatalf("initial migrate: %v", err)
	}

	ctx := context.Background()
	gdb, err := db.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		if sqlDB, err := gdb.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})

	if err := gdb.Exec(`DROP SCHEMA public CASCADE; CREATE SCHEMA public`).Error; err != nil {
		t.Fatalf("reset schema: %v", err)
	}
	if err := db.Migrate(dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return New(gdb)
}

// newCar creates a channel (with unique tg_id) and one pending car in it,
// returning their IDs.
func newCar(t *testing.T, s *Store, tgID int64) (channelID, carID int64) {
	t.Helper()
	ctx := context.Background()

	channelID, err := s.AddChannel(ctx, Channel{
		TgID: tgID, Title: "test", MessageLimit: 1000, Active: true,
	})
	if err != nil {
		t.Fatalf("add channel: %v", err)
	}
	carID, err = s.CreatePendingCar(ctx, channelID, 1024, 1)
	if err != nil {
		t.Fatalf("create pending car: %v", err)
	}
	return channelID, carID
}

func TestConfig(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	if _, err := s.ConfigValue(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ConfigValue(missing) = %v, want ErrNotFound", err)
	}
	if err := s.SetConfig(ctx, "k", "1"); err != nil {
		t.Fatalf("SetConfig: %v", err)
	}
	if err := s.SetConfig(ctx, "k", "2"); err != nil { // upsert
		t.Fatalf("SetConfig (update): %v", err)
	}
	if v, err := s.ConfigInt64(ctx, "k"); err != nil || v != 2 {
		t.Fatalf("ConfigInt64 = %d, %v; want 2", v, err)
	}
}

func TestBotAddExists(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	b := Bot{TgID: 1, Username: "a", Token: "tok", Active: true}
	id, err := s.AddBot(ctx, b)
	if err != nil || id == 0 {
		t.Fatalf("AddBot = %d, %v", id, err)
	}
	if _, err := s.AddBot(ctx, b); !errors.Is(err, ErrBotExists) {
		t.Fatalf("AddBot(dup) = %v, want ErrBotExists", err)
	}

	got, err := s.BotByUsername(ctx, "a")
	if err != nil || got.ID != id {
		t.Fatalf("BotByUsername = %+v, %v", got, err)
	}
	if _, err := s.BotByUsername(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("BotByUsername(nope) = %v, want ErrNotFound", err)
	}
}

func TestBlockDedup(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	_, carID := newCar(t, s, 100)

	blocks := []BlockRef{
		{CID: []byte("cid-a"), CarID: carID, Offset: 0, Length: 10},
		{CID: []byte("cid-b"), CarID: carID, Offset: 10, Length: 20},
	}
	if err := s.UpsertBlocks(ctx, blocks); err != nil {
		t.Fatalf("UpsertBlocks[1]: %v", err)
	}
	// A conflicting upsert must not duplicate the row and must repoint it
	// (ON CONFLICT DO UPDATE) — the publish path relies on this to recover
	// blocks whose previous car row was deleted after the dedup snapshot.
	dup := []BlockRef{{CID: []byte("cid-a"), CarID: carID, Offset: 999, Length: 999}}
	if err := s.UpsertBlocks(ctx, dup); err != nil {
		t.Fatalf("UpsertBlocks[2]: %v", err)
	}
	n, err := s.CountBlocks(ctx)
	if err != nil || n != 2 {
		t.Fatalf("CountBlocks = %d, %v; want 2", n, err)
	}
	got, err := s.LookupBlock(ctx, []byte("cid-a"))
	if err != nil || got.Offset != 999 {
		t.Fatalf("LookupBlock cid-a = %+v, %v; want offset 999 (repointed)", got, err)
	}
	if _, err := s.LookupBlock(ctx, []byte("nope")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("LookupBlock(nope) = %v, want ErrNotFound", err)
	}

	existing, err := s.ExistingBlocks(ctx, [][]byte{[]byte("cid-a"), []byte("cid-x")})
	if err != nil {
		t.Fatalf("ExistingBlocks: %v", err)
	}
	if len(existing) != 1 {
		t.Fatalf("ExistingBlocks len = %d, want 1", len(existing))
	}
	if _, ok := existing[string([]byte("cid-a"))]; !ok {
		t.Fatalf("ExistingBlocks missing cid-a")
	}
}

func TestRepoint(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	_, carID := newCar(t, s, 101)
	_, carID2 := newCar(t, s, 102)

	if err := s.UpsertBlocks(ctx, []BlockRef{
		{CID: []byte("rc-a"), CarID: carID, Offset: 1, Length: 2},
	}); err != nil {
		t.Fatalf("UpsertBlocks: %v", err)
	}
	if err := s.UpsertBlocks(ctx, []BlockRef{
		{CID: []byte("rc-a"), CarID: carID2, Offset: 7, Length: 8},
	}); err != nil {
		t.Fatalf("UpsertBlocks: %v", err)
	}
	got, err := s.LookupBlock(ctx, []byte("rc-a"))
	if err != nil {
		t.Fatalf("LookupBlock: %v", err)
	}
	if got.CarID != carID2 || got.Offset != 7 || got.Length != 8 {
		t.Fatalf("UpsertBlocks result = %+v; want car=%d off=7 len=8", got, carID2)
	}
}

func TestStreamAllCIDs(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	_, carID := newCar(t, s, 103)

	want := map[string]bool{}
	var blocks []BlockRef
	for i := 0; i < 2500; i++ {
		cid := []byte("scid-" + string(rune('A'+i%26)) + "-" + itoa(i))
		blocks = append(blocks, BlockRef{CID: cid, CarID: carID, Offset: int64(i), Length: 1})
		want[string(cid)] = true
	}
	if err := s.UpsertBlocks(ctx, blocks); err != nil {
		t.Fatalf("UpsertBlocks: %v", err)
	}

	cids, errc := s.StreamAllCIDs(ctx)
	got := map[string]bool{}
	for cid := range cids {
		got[string(cid)] = true
	}
	if err := <-errc; err != nil {
		t.Fatalf("StreamAllCIDs err: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("streamed %d cids, want %d", len(got), len(want))
	}
	for k := range want {
		if !got[k] {
			t.Fatalf("StreamAllCIDs missing cid %q", k)
		}
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

func TestIncrementMessageCountConcurrent(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	channelID, _ := newCar(t, s, 104)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.IncrementMessageCount(ctx, channelID); err != nil {
				t.Errorf("IncrementMessageCount: %v", err)
			}
		}()
	}
	wg.Wait()

	ch, err := s.ChannelByID(ctx, channelID)
	if err != nil {
		t.Fatalf("ChannelByID: %v", err)
	}
	if ch.MessageCount != 10 {
		t.Fatalf("MessageCount = %d, want 10", ch.MessageCount)
	}
}

func TestUnpinnedCars(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	_, carID := newCar(t, s, 105)

	root := []byte("root-1")
	cid := []byte("blk-1")
	if err := s.UpsertBlocks(ctx, []BlockRef{
		{CID: cid, CarID: carID, Offset: 0, Length: 1},
	}); err != nil {
		t.Fatalf("UpsertBlocks: %v", err)
	}

	// Before pinning: the car is unpinned.
	up, err := s.UnpinnedCars(ctx)
	if err != nil {
		t.Fatalf("UnpinnedCars: %v", err)
	}
	if !containsCar(up, carID) {
		t.Fatalf("car %d should be unpinned before pinning", carID)
	}

	if err := s.CreatePin(ctx, root, "n", 100, [][]byte{cid}); err != nil {
		t.Fatalf("CreatePin: %v", err)
	}
	up, err = s.UnpinnedCars(ctx)
	if err != nil {
		t.Fatalf("UnpinnedCars: %v", err)
	}
	if containsCar(up, carID) {
		t.Fatalf("car %d should NOT be unpinned after pinning", carID)
	}

	// Idempotent re-create on the same root.
	if err := s.CreatePin(ctx, root, "n2", 200, [][]byte{cid}); err != nil {
		t.Fatalf("CreatePin (idempotent): %v", err)
	}
	if c, _ := s.CountPins(ctx); c != 1 {
		t.Fatalf("CountPins = %d, want 1", c)
	}

	// After removing the pin: unpinned again.
	if err := s.RemovePin(ctx, root); err != nil {
		t.Fatalf("RemovePin: %v", err)
	}
	if err := s.RemovePin(ctx, root); !errors.Is(err, ErrNotFound) {
		t.Fatalf("RemovePin(again) = %v, want ErrNotFound", err)
	}
	up, err = s.UnpinnedCars(ctx)
	if err != nil {
		t.Fatalf("UnpinnedCars: %v", err)
	}
	if !containsCar(up, carID) {
		t.Fatalf("car %d should be unpinned after pin removal", carID)
	}
}

func containsCar(cars []Car, id int64) bool {
	for _, c := range cars {
		if c.ID == id {
			return true
		}
	}
	return false
}

func TestCountCarsByStatus(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	channelID, carID := newCar(t, s, 106)

	c2, err := s.CreatePendingCar(ctx, channelID, 1, 1)
	if err != nil {
		t.Fatalf("CreatePendingCar: %v", err)
	}
	if err := s.MarkCarPublished(ctx, c2, 555); err != nil {
		t.Fatalf("MarkCarPublished: %v", err)
	}
	if err := s.SetCarStatus(ctx, carID, CarNoBotAccess); err != nil {
		t.Fatalf("SetCarStatus: %v", err)
	}

	counts, err := s.CountCarsByStatus(ctx)
	if err != nil {
		t.Fatalf("CountCarsByStatus: %v", err)
	}
	if counts[CarPublished] != 1 || counts[CarNoBotAccess] != 1 {
		t.Fatalf("CountCarsByStatus = %v", counts)
	}

	byStatus, err := s.CarsWithStatus(ctx, CarNoBotAccess, CarTooLarge)
	if err != nil {
		t.Fatalf("CarsWithStatus: %v", err)
	}
	if len(byStatus) != 1 || byStatus[0].ID != carID {
		t.Fatalf("CarsWithStatus = %+v, want [car %d]", byStatus, carID)
	}
}

func TestCarsAccessibleOnlyVia(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	channelID, carID := newCar(t, s, 107)

	bot1, err := s.AddBot(ctx, Bot{TgID: 11, Username: "b1", Token: "t1", Active: true})
	if err != nil {
		t.Fatalf("AddBot bot1: %v", err)
	}
	bot2, err := s.AddBot(ctx, Bot{TgID: 12, Username: "b2", Token: "t2", Active: true})
	if err != nil {
		t.Fatalf("AddBot bot2: %v", err)
	}

	// Only bot1 is a member: the car is accessible only via bot1.
	if err := s.UpsertBotChannel(ctx, BotChannel{
		BotID: bot1, ChannelID: channelID, Member: true, CanRead: true, VerifiedAt: time.Now(),
	}); err != nil {
		t.Fatalf("UpsertBotChannel bot1: %v", err)
	}
	got, err := s.CarsAccessibleOnlyVia(ctx, bot1)
	if err != nil {
		t.Fatalf("CarsAccessibleOnlyVia: %v", err)
	}
	if !containsCar(got, carID) {
		t.Fatalf("car %d should be accessible only via bot1", carID)
	}

	// Add bot2 as an active member: bot1 is no longer the sole access path.
	if err := s.UpsertBotChannel(ctx, BotChannel{
		BotID: bot2, ChannelID: channelID, Member: true, CanRead: true, VerifiedAt: time.Now(),
	}); err != nil {
		t.Fatalf("UpsertBotChannel bot2: %v", err)
	}
	got, err = s.CarsAccessibleOnlyVia(ctx, bot1)
	if err != nil {
		t.Fatalf("CarsAccessibleOnlyVia: %v", err)
	}
	if containsCar(got, carID) {
		t.Fatalf("car %d should NOT be accessible only via bot1 once bot2 is a member", carID)
	}

	// Deactivating bot2 makes bot1 the sole access path again.
	if err := s.SetBotActive(ctx, bot2, false); err != nil {
		t.Fatalf("SetBotActive bot2 false: %v", err)
	}
	got, err = s.CarsAccessibleOnlyVia(ctx, bot1)
	if err != nil {
		t.Fatalf("CarsAccessibleOnlyVia: %v", err)
	}
	if !containsCar(got, carID) {
		t.Fatalf("car %d should be accessible only via bot1 once bot2 is inactive", carID)
	}
}

func TestRemoveStatsAndOrphanPins(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	channelID, carID := newCar(t, s, 108)

	cid := []byte("os-blk")
	if err := s.UpsertBlocks(ctx, []BlockRef{
		{CID: cid, CarID: carID, Offset: 0, Length: 1},
	}); err != nil {
		t.Fatalf("UpsertBlocks: %v", err)
	}
	if err := s.CreatePin(ctx, []byte("os-root"), "n", 7, [][]byte{cid}); err != nil {
		t.Fatalf("CreatePin: %v", err)
	}

	p, b, err := s.ChannelRemoveStats(ctx, channelID)
	if err != nil {
		t.Fatalf("ChannelRemoveStats: %v", err)
	}
	if p != 1 || b != 1024 {
		t.Fatalf("ChannelRemoveStats = pins %d bytes %d; want 1, 1024", p, b)
	}

	// Removing the channel cascades cars+blocks; the pin is now orphaned.
	if err := s.RemoveChannel(ctx, channelID); err != nil {
		t.Fatalf("RemoveChannel: %v", err)
	}
	deleted, err := s.DeleteOrphanPins(ctx)
	if err != nil {
		t.Fatalf("DeleteOrphanPins: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("DeleteOrphanPins = %d, want 1", deleted)
	}
	if c, _ := s.CountPins(ctx); c != 0 {
		t.Fatalf("CountPins after orphan cleanup = %d, want 0", c)
	}
}

func TestMTProto(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	if c, err := s.LatestMTProtoCreds(ctx); err != nil || c != nil {
		t.Fatalf("LatestMTProtoCreds(empty) = %+v, %v; want nil, nil", c, err)
	}
	if c, err := s.ActiveMTProtoCreds(ctx); err != nil || c != nil {
		t.Fatalf("ActiveMTProtoCreds(empty) = %+v, %v; want nil, nil", c, err)
	}

	if err := s.ActivateMTProto(ctx, 100, "hashA"); err != nil {
		t.Fatalf("ActivateMTProto A: %v", err)
	}
	active, err := s.ActiveMTProtoCreds(ctx)
	if err != nil || active == nil || active.APIID != 100 {
		t.Fatalf("ActiveMTProtoCreds = %+v, %v; want api_id 100", active, err)
	}

	// Activating different creds keeps only one active row.
	if err := s.ActivateMTProto(ctx, 200, "hashB"); err != nil {
		t.Fatalf("ActivateMTProto B: %v", err)
	}
	active, err = s.ActiveMTProtoCreds(ctx)
	if err != nil || active == nil || active.APIID != 200 {
		t.Fatalf("ActiveMTProtoCreds = %+v, %v; want api_id 200", active, err)
	}

	// Reactivating the original creds reuses the existing row (no new insert).
	if err := s.ActivateMTProto(ctx, 100, "hashA"); err != nil {
		t.Fatalf("Reactivate A: %v", err)
	}
	active, err = s.ActiveMTProtoCreds(ctx)
	if err != nil || active == nil || active.APIID != 100 {
		t.Fatalf("ActiveMTProtoCreds = %+v, %v; want api_id 100 again", active, err)
	}

	latest, err := s.LatestMTProtoCreds(ctx)
	if err != nil || latest == nil || latest.APIID != 200 {
		t.Fatalf("LatestMTProtoCreds = %+v, %v; want newest api_id 200", latest, err)
	}

	if err := s.DeactivateMTProto(ctx); err != nil {
		t.Fatalf("DeactivateMTProto: %v", err)
	}
	if c, err := s.ActiveMTProtoCreds(ctx); err != nil || c != nil {
		t.Fatalf("ActiveMTProtoCreds after DeactivateMTProto = %+v, %v; want nil, nil", c, err)
	}
}

func TestCarFileIDs(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	_, carID := newCar(t, s, 109)

	botID, err := s.AddBot(ctx, Bot{TgID: 21, Username: "fb", Token: "ftok", Active: true})
	if err != nil {
		t.Fatalf("AddBot: %v", err)
	}
	if err := s.UpsertCarFileID(ctx, carID, botID, "file-1"); err != nil {
		t.Fatalf("UpsertCarFileID: %v", err)
	}
	if err := s.UpsertCarFileID(ctx, carID, botID, "file-2"); err != nil {
		t.Fatalf("UpsertCarFileID (update): %v", err)
	}
	ids, err := s.CarFileIDs(ctx, carID)
	if err != nil {
		t.Fatalf("CarFileIDs: %v", err)
	}
	if ids[botID] != "file-2" {
		t.Fatalf("CarFileIDs[%d] = %q, want file-2", botID, ids[botID])
	}
	if err := s.DeleteCarFileID(ctx, carID, botID); err != nil {
		t.Fatalf("DeleteCarFileID: %v", err)
	}
	if ids, _ := s.CarFileIDs(ctx, carID); len(ids) != 0 {
		t.Fatalf("CarFileIDs after delete = %v, want empty", ids)
	}
}

func TestOrphanPendingCars(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	channelID, carID := newCar(t, s, 110)

	// Freshly created: not yet orphaned.
	got, err := s.OrphanPendingCars(ctx, time.Hour)
	if err != nil {
		t.Fatalf("OrphanPendingCars: %v", err)
	}
	if containsCar(got, carID) {
		t.Fatalf("fresh car %d should not be orphaned", carID)
	}

	// Backdate created_at so it qualifies.
	if err := s.db.Exec(
		`UPDATE cars SET created_at = now() - interval '1 hour' WHERE id = ?`, carID,
	).Error; err != nil {
		t.Fatalf("backdate: %v", err)
	}
	got, err = s.OrphanPendingCars(ctx, time.Minute)
	if err != nil {
		t.Fatalf("OrphanPendingCars: %v", err)
	}
	if !containsCar(got, carID) {
		t.Fatalf("backdated car %d should be orphaned", carID)
	}
	_ = channelID
}

func TestAdvisoryLocks(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	ran := false
	if err := s.WithSharedPublishLock(ctx, func(ctx context.Context) error {
		ran = true
		return nil
	}); err != nil {
		t.Fatalf("WithSharedPublishLock: %v", err)
	}
	if !ran {
		t.Fatal("WithSharedPublishLock did not run fn")
	}

	wantErr := errors.New("boom")
	if err := s.WithExclusiveGCLock(ctx, func(ctx context.Context) error {
		return wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("WithExclusiveGCLock = %v, want boom", err)
	}

	// Two shared holders may overlap; verified by acquiring the shared lock
	// while another shared holder is still inside its callback.
	inner := false
	err := s.WithSharedPublishLock(ctx, func(ctx context.Context) error {
		return s.WithSharedPublishLock(ctx, func(ctx context.Context) error {
			inner = true
			return nil
		})
	})
	if err != nil || !inner {
		t.Fatalf("nested shared locks = %v, inner ran %v; want nil, true", err, inner)
	}
}
