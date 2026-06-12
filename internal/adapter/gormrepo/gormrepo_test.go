package gormrepo

import (
	"context"
	"errors"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"

	"github.com/ulbwa/ipfsgram/db"
	"github.com/ulbwa/ipfsgram/internal/domain"
)

// testDB connects to the database at IPFSGRAM_TEST_DSN, resets the public schema
// and applies all migrations. Tests are skipped when the variable is unset. The
// database name from the DSN gets a per-package "_gormrepo" suffix so this
// package and others that reset the public schema can run in parallel under
// `go test ./...` without racing on a shared database.
func testDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := os.Getenv("IPFSGRAM_TEST_DSN")
	if dsn == "" {
		t.Skip("IPFSGRAM_TEST_DSN not set; skipping gormrepo integration tests")
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse IPFSGRAM_TEST_DSN: %v", err)
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = "/ipfsgram_test"
	}
	u.Path += "_gormrepo"
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
	return gdb
}

// newCar creates a channel (with unique tg_id) and one pending car in it,
// returning their IDs.
func newCar(t *testing.T, gdb *gorm.DB, tgID int64) (channelID, carID int64) {
	t.Helper()
	ctx := context.Background()

	channelID, err := NewChannelRepository(gdb).Add(ctx, domain.Channel{
		TgID: tgID, Title: "test", MessageLimit: 1000, Active: true,
	})
	if err != nil {
		t.Fatalf("add channel: %v", err)
	}
	carID, err = NewCarRepository(gdb).CreatePending(ctx, channelID, 1024, 1)
	if err != nil {
		t.Fatalf("create pending car: %v", err)
	}
	return channelID, carID
}

func TestConfig(t *testing.T) {
	gdb := testDB(t)
	ctx := context.Background()
	r := NewConfigRepository(gdb)

	if _, err := r.Get(ctx, "missing"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("Get(missing) = %v, want ErrNotFound", err)
	}
	if err := r.Set(ctx, "k", "1"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := r.Set(ctx, "k", "2"); err != nil { // upsert
		t.Fatalf("Set (update): %v", err)
	}
	if v, err := r.GetInt64(ctx, "k"); err != nil || v != 2 {
		t.Fatalf("GetInt64 = %d, %v; want 2", v, err)
	}
}

func TestBotAddExists(t *testing.T) {
	gdb := testDB(t)
	ctx := context.Background()
	r := NewBotRepository(gdb)

	b := domain.Bot{TgID: 1, Username: "a", Token: "tok", Active: true}
	id, err := r.Add(ctx, b)
	if err != nil || id == 0 {
		t.Fatalf("Add = %d, %v", id, err)
	}
	if _, err := r.Add(ctx, b); !errors.Is(err, domain.ErrBotExists) {
		t.Fatalf("Add(dup) = %v, want ErrBotExists", err)
	}

	got, err := r.GetByUsername(ctx, "a")
	if err != nil || got.ID != id {
		t.Fatalf("GetByUsername = %+v, %v", got, err)
	}
	if _, err := r.GetByUsername(ctx, "nope"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("GetByUsername(nope) = %v, want ErrNotFound", err)
	}
}

func TestBlockDedup(t *testing.T) {
	gdb := testDB(t)
	ctx := context.Background()
	_, carID := newCar(t, gdb, 100)
	r := NewBlockRepository(gdb)

	blocks := []domain.Block{
		{CID: []byte("cid-a"), CarID: carID, Offset: 0, Length: 10},
		{CID: []byte("cid-b"), CarID: carID, Offset: 10, Length: 20},
	}
	if err := r.InsertBatch(ctx, blocks); err != nil {
		t.Fatalf("InsertBatch[1]: %v", err)
	}
	// Second insert with a conflicting offset must be a no-op (DO NOTHING).
	dup := []domain.Block{{CID: []byte("cid-a"), CarID: carID, Offset: 999, Length: 999}}
	if err := r.InsertBatch(ctx, dup); err != nil {
		t.Fatalf("InsertBatch[2]: %v", err)
	}
	n, err := r.CountAll(ctx)
	if err != nil || n != 2 {
		t.Fatalf("CountAll = %d, %v; want 2", n, err)
	}
	got, err := r.Lookup(ctx, []byte("cid-a"))
	if err != nil || got.Offset != 0 {
		t.Fatalf("Lookup cid-a = %+v, %v; want offset 0 (unchanged)", got, err)
	}
	if _, err := r.Lookup(ctx, []byte("nope")); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("Lookup(nope) = %v, want ErrNotFound", err)
	}

	existing, err := r.Existing(ctx, [][]byte{[]byte("cid-a"), []byte("cid-x")})
	if err != nil {
		t.Fatalf("Existing: %v", err)
	}
	if len(existing) != 1 {
		t.Fatalf("Existing len = %d, want 1", len(existing))
	}
	if _, ok := existing[string([]byte("cid-a"))]; !ok {
		t.Fatalf("Existing missing cid-a")
	}
}

func TestRepoint(t *testing.T) {
	gdb := testDB(t)
	ctx := context.Background()
	_, carID := newCar(t, gdb, 101)
	_, carID2 := newCar(t, gdb, 102)
	r := NewBlockRepository(gdb)

	if err := r.InsertBatch(ctx, []domain.Block{
		{CID: []byte("rc-a"), CarID: carID, Offset: 1, Length: 2},
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if err := r.Repoint(ctx, []domain.Block{
		{CID: []byte("rc-a"), CarID: carID2, Offset: 7, Length: 8},
	}); err != nil {
		t.Fatalf("Repoint: %v", err)
	}
	got, err := r.Lookup(ctx, []byte("rc-a"))
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if got.CarID != carID2 || got.Offset != 7 || got.Length != 8 {
		t.Fatalf("Repoint result = %+v; want car=%d off=7 len=8", got, carID2)
	}
}

func TestStreamAllCIDs(t *testing.T) {
	gdb := testDB(t)
	ctx := context.Background()
	_, carID := newCar(t, gdb, 103)
	r := NewBlockRepository(gdb)

	want := map[string]bool{}
	var blocks []domain.Block
	for i := 0; i < 2500; i++ {
		cid := []byte("scid-" + string(rune('A'+i%26)) + "-" + itoa(i))
		blocks = append(blocks, domain.Block{CID: cid, CarID: carID, Offset: int64(i), Length: 1})
		want[string(cid)] = true
	}
	if err := r.InsertBatch(ctx, blocks); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	cids, errc := r.StreamAllCIDs(ctx)
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
	gdb := testDB(t)
	ctx := context.Background()
	channelID, _ := newCar(t, gdb, 104)
	r := NewChannelRepository(gdb)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.IncrementMessageCount(ctx, channelID); err != nil {
				t.Errorf("IncrementMessageCount: %v", err)
			}
		}()
	}
	wg.Wait()

	ch, err := r.GetByID(ctx, channelID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if ch.MessageCount != 10 {
		t.Fatalf("MessageCount = %d, want 10", ch.MessageCount)
	}
}

func TestUnpinnedCars(t *testing.T) {
	gdb := testDB(t)
	ctx := context.Background()
	_, carID := newCar(t, gdb, 105)
	blocks := NewBlockRepository(gdb)
	pins := NewPinRepository(gdb)
	cars := NewCarRepository(gdb)

	root := []byte("root-1")
	cid := []byte("blk-1")
	if err := blocks.InsertBatch(ctx, []domain.Block{
		{CID: cid, CarID: carID, Offset: 0, Length: 1},
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	// Before pinning: the car is unpinned.
	up, err := cars.UnpinnedCars(ctx)
	if err != nil {
		t.Fatalf("UnpinnedCars: %v", err)
	}
	if !containsCar(up, carID) {
		t.Fatalf("car %d should be unpinned before pinning", carID)
	}

	if err := pins.Create(ctx, root, "n", 100, [][]byte{cid}); err != nil {
		t.Fatalf("Create pin: %v", err)
	}
	up, err = cars.UnpinnedCars(ctx)
	if err != nil {
		t.Fatalf("UnpinnedCars: %v", err)
	}
	if containsCar(up, carID) {
		t.Fatalf("car %d should NOT be unpinned after pinning", carID)
	}

	// Idempotent re-create on the same root.
	if err := pins.Create(ctx, root, "n2", 200, [][]byte{cid}); err != nil {
		t.Fatalf("Create pin (idempotent): %v", err)
	}
	if c, _ := pins.Count(ctx); c != 1 {
		t.Fatalf("pin Count = %d, want 1", c)
	}

	// After removing the pin: unpinned again.
	if err := pins.Remove(ctx, root); err != nil {
		t.Fatalf("Remove pin: %v", err)
	}
	if err := pins.Remove(ctx, root); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("Remove(again) = %v, want ErrNotFound", err)
	}
	up, err = cars.UnpinnedCars(ctx)
	if err != nil {
		t.Fatalf("UnpinnedCars: %v", err)
	}
	if !containsCar(up, carID) {
		t.Fatalf("car %d should be unpinned after pin removal", carID)
	}
}

func containsCar(cars []domain.Car, id int64) bool {
	for _, c := range cars {
		if c.ID == id {
			return true
		}
	}
	return false
}

func TestCountByStatus(t *testing.T) {
	gdb := testDB(t)
	ctx := context.Background()
	channelID, carID := newCar(t, gdb, 106)
	cars := NewCarRepository(gdb)

	c2, err := cars.CreatePending(ctx, channelID, 1, 1)
	if err != nil {
		t.Fatalf("CreatePending: %v", err)
	}
	if err := cars.MarkPublished(ctx, c2, 555); err != nil {
		t.Fatalf("MarkPublished: %v", err)
	}
	if err := cars.SetStatus(ctx, carID, domain.CarNoBotAccess); err != nil {
		t.Fatalf("SetStatus: %v", err)
	}

	counts, err := cars.CountByStatus(ctx)
	if err != nil {
		t.Fatalf("CountByStatus: %v", err)
	}
	if counts[domain.CarPublished] != 1 || counts[domain.CarNoBotAccess] != 1 {
		t.Fatalf("CountByStatus = %v", counts)
	}

	byStatus, err := cars.ByStatuses(ctx, domain.CarNoBotAccess, domain.CarTooLarge)
	if err != nil {
		t.Fatalf("ByStatuses: %v", err)
	}
	if len(byStatus) != 1 || byStatus[0].ID != carID {
		t.Fatalf("ByStatuses = %+v, want [car %d]", byStatus, carID)
	}
}

func TestAccessibleOnlyVia(t *testing.T) {
	gdb := testDB(t)
	ctx := context.Background()
	channelID, carID := newCar(t, gdb, 107)
	bots := NewBotRepository(gdb)
	chans := NewChannelRepository(gdb)
	cars := NewCarRepository(gdb)

	bot1, err := bots.Add(ctx, domain.Bot{TgID: 11, Username: "b1", Token: "t1", Active: true})
	if err != nil {
		t.Fatalf("Add bot1: %v", err)
	}
	bot2, err := bots.Add(ctx, domain.Bot{TgID: 12, Username: "b2", Token: "t2", Active: true})
	if err != nil {
		t.Fatalf("Add bot2: %v", err)
	}

	// Only bot1 is a member: the car is accessible only via bot1.
	if err := chans.UpsertBotChannel(ctx, domain.BotChannel{
		BotID: bot1, ChannelID: channelID, Member: true, CanRead: true, VerifiedAt: time.Now(),
	}); err != nil {
		t.Fatalf("UpsertBotChannel bot1: %v", err)
	}
	got, err := cars.AccessibleOnlyVia(ctx, bot1)
	if err != nil {
		t.Fatalf("AccessibleOnlyVia: %v", err)
	}
	if !containsCar(got, carID) {
		t.Fatalf("car %d should be accessible only via bot1", carID)
	}

	// Add bot2 as an active member: bot1 is no longer the sole access path.
	if err := chans.UpsertBotChannel(ctx, domain.BotChannel{
		BotID: bot2, ChannelID: channelID, Member: true, CanRead: true, VerifiedAt: time.Now(),
	}); err != nil {
		t.Fatalf("UpsertBotChannel bot2: %v", err)
	}
	got, err = cars.AccessibleOnlyVia(ctx, bot1)
	if err != nil {
		t.Fatalf("AccessibleOnlyVia: %v", err)
	}
	if containsCar(got, carID) {
		t.Fatalf("car %d should NOT be accessible only via bot1 once bot2 is a member", carID)
	}

	// Deactivating bot2 makes bot1 the sole access path again.
	if err := bots.SetActive(ctx, bot2, false); err != nil {
		t.Fatalf("SetActive bot2 false: %v", err)
	}
	got, err = cars.AccessibleOnlyVia(ctx, bot1)
	if err != nil {
		t.Fatalf("AccessibleOnlyVia: %v", err)
	}
	if !containsCar(got, carID) {
		t.Fatalf("car %d should be accessible only via bot1 once bot2 is inactive", carID)
	}
}

func TestRemoveStatsAndOrphanPins(t *testing.T) {
	gdb := testDB(t)
	ctx := context.Background()
	channelID, carID := newCar(t, gdb, 108)
	blocks := NewBlockRepository(gdb)
	pins := NewPinRepository(gdb)
	chans := NewChannelRepository(gdb)

	cid := []byte("os-blk")
	if err := blocks.InsertBatch(ctx, []domain.Block{
		{CID: cid, CarID: carID, Offset: 0, Length: 1},
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if err := pins.Create(ctx, []byte("os-root"), "n", 7, [][]byte{cid}); err != nil {
		t.Fatalf("Create pin: %v", err)
	}

	p, b, err := chans.RemoveStats(ctx, channelID)
	if err != nil {
		t.Fatalf("RemoveStats: %v", err)
	}
	if p != 1 || b != 1024 {
		t.Fatalf("RemoveStats = pins %d bytes %d; want 1, 1024", p, b)
	}

	// Removing the channel cascades cars+blocks; the pin is now orphaned.
	if err := chans.Remove(ctx, channelID); err != nil {
		t.Fatalf("Remove channel: %v", err)
	}
	deleted, err := chans.DeleteOrphanPins(ctx)
	if err != nil {
		t.Fatalf("DeleteOrphanPins: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("DeleteOrphanPins = %d, want 1", deleted)
	}
	if c, _ := pins.Count(ctx); c != 0 {
		t.Fatalf("pin Count after orphan cleanup = %d, want 0", c)
	}
}

func TestMTProto(t *testing.T) {
	gdb := testDB(t)
	ctx := context.Background()
	r := NewMTProtoRepository(gdb)

	if c, err := r.Latest(ctx); err != nil || c != nil {
		t.Fatalf("Latest(empty) = %+v, %v; want nil, nil", c, err)
	}
	if c, err := r.Active(ctx); err != nil || c != nil {
		t.Fatalf("Active(empty) = %+v, %v; want nil, nil", c, err)
	}

	if err := r.Activate(ctx, 100, "hashA"); err != nil {
		t.Fatalf("Activate A: %v", err)
	}
	active, err := r.Active(ctx)
	if err != nil || active == nil || active.APIID != 100 {
		t.Fatalf("Active = %+v, %v; want api_id 100", active, err)
	}

	// Activating different creds keeps only one active row.
	if err := r.Activate(ctx, 200, "hashB"); err != nil {
		t.Fatalf("Activate B: %v", err)
	}
	active, err = r.Active(ctx)
	if err != nil || active == nil || active.APIID != 200 {
		t.Fatalf("Active = %+v, %v; want api_id 200", active, err)
	}

	// Reactivating the original creds reuses the existing row (no new insert).
	if err := r.Activate(ctx, 100, "hashA"); err != nil {
		t.Fatalf("Reactivate A: %v", err)
	}
	active, err = r.Active(ctx)
	if err != nil || active == nil || active.APIID != 100 {
		t.Fatalf("Active = %+v, %v; want api_id 100 again", active, err)
	}

	latest, err := r.Latest(ctx)
	if err != nil || latest == nil || latest.APIID != 200 {
		t.Fatalf("Latest = %+v, %v; want newest api_id 200", latest, err)
	}

	if err := r.Deactivate(ctx); err != nil {
		t.Fatalf("Deactivate: %v", err)
	}
	if c, err := r.Active(ctx); err != nil || c != nil {
		t.Fatalf("Active after Deactivate = %+v, %v; want nil, nil", c, err)
	}
}

func TestCarFileIDs(t *testing.T) {
	gdb := testDB(t)
	ctx := context.Background()
	_, carID := newCar(t, gdb, 109)
	bots := NewBotRepository(gdb)
	cars := NewCarRepository(gdb)

	botID, err := bots.Add(ctx, domain.Bot{TgID: 21, Username: "fb", Token: "ftok", Active: true})
	if err != nil {
		t.Fatalf("Add bot: %v", err)
	}
	if err := cars.UpsertFileID(ctx, carID, botID, "file-1"); err != nil {
		t.Fatalf("UpsertFileID: %v", err)
	}
	if err := cars.UpsertFileID(ctx, carID, botID, "file-2"); err != nil {
		t.Fatalf("UpsertFileID (update): %v", err)
	}
	ids, err := cars.FileIDs(ctx, carID)
	if err != nil {
		t.Fatalf("FileIDs: %v", err)
	}
	if ids[botID] != "file-2" {
		t.Fatalf("FileIDs[%d] = %q, want file-2", botID, ids[botID])
	}
	if err := cars.DeleteFileID(ctx, carID, botID); err != nil {
		t.Fatalf("DeleteFileID: %v", err)
	}
	if ids, _ := cars.FileIDs(ctx, carID); len(ids) != 0 {
		t.Fatalf("FileIDs after delete = %v, want empty", ids)
	}
}

func TestOrphanPending(t *testing.T) {
	gdb := testDB(t)
	ctx := context.Background()
	channelID, carID := newCar(t, gdb, 110)
	cars := NewCarRepository(gdb)

	// Freshly created: not yet orphaned.
	got, err := cars.OrphanPending(ctx, time.Hour)
	if err != nil {
		t.Fatalf("OrphanPending: %v", err)
	}
	if containsCar(got, carID) {
		t.Fatalf("fresh car %d should not be orphaned", carID)
	}

	// Backdate created_at so it qualifies.
	if err := gdb.Exec(
		`UPDATE cars SET created_at = now() - interval '1 hour' WHERE id = ?`, carID,
	).Error; err != nil {
		t.Fatalf("backdate: %v", err)
	}
	got, err = cars.OrphanPending(ctx, time.Minute)
	if err != nil {
		t.Fatalf("OrphanPending: %v", err)
	}
	if !containsCar(got, carID) {
		t.Fatalf("backdated car %d should be orphaned", carID)
	}
	_ = channelID
}
