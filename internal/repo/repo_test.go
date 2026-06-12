package repo

import (
	"bytes"
	"context"
	"errors"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/ulbwa/ipfsgram/internal/db"
	"github.com/ulbwa/ipfsgram/internal/model"
)

// testDB connects to the database at IPFSGRAM_TEST_DSN, resets the public
// schema and applies all migrations. Tests are skipped when the variable is
// unset. The database name from the DSN gets a per-package "_repo" suffix so
// this package and internal/db (which both reset the public schema) can run
// in parallel under `go test ./...` without racing on a shared database.
func testDB(t *testing.T) *sqlx.DB {
	t.Helper()

	dsn := os.Getenv("IPFSGRAM_TEST_DSN")
	if dsn == "" {
		t.Skip("IPFSGRAM_TEST_DSN not set; skipping repo integration tests")
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse IPFSGRAM_TEST_DSN: %v", err)
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = "/ipfsgram_test"
	}
	u.Path += "_repo"
	dsn = u.String()

	// Migrate first: dbmate creates the database if it does not exist yet.
	if err := db.Migrate(dsn); err != nil {
		t.Fatalf("initial migrate: %v", err)
	}

	ctx := context.Background()
	conn, err := db.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	if _, err := conn.ExecContext(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public`); err != nil {
		t.Fatalf("reset schema: %v", err)
	}
	if err := db.Migrate(dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return conn
}

// newCar creates a channel (with unique tg_id) and one pending car in it,
// returning their IDs.
func newCar(t *testing.T, conn *sqlx.DB, tgID int64) (channelID, carID int64) {
	t.Helper()
	ctx := context.Background()

	channelID, err := NewChannelRepo(conn).Add(ctx, model.Channel{
		TgID: tgID, Title: "test", MessageLimit: 1000, Active: true,
	})
	if err != nil {
		t.Fatalf("add channel: %v", err)
	}
	carID, err = NewCarRepo(conn).CreatePending(ctx, channelID, 1024, 1)
	if err != nil {
		t.Fatalf("create car: %v", err)
	}
	return channelID, carID
}

func TestBlockDedup(t *testing.T) {
	conn := testDB(t)
	ctx := context.Background()
	blocks := NewBlockRepo(conn)
	_, carID := newCar(t, conn, 100)

	cid := []byte("block-cid-1")
	refs := []model.BlockRef{{CID: cid, CarID: carID, Offset: 0, Length: 64}}

	if err := blocks.InsertBatch(ctx, refs); err != nil {
		t.Fatalf("first InsertBatch: %v", err)
	}
	if err := blocks.InsertBatch(ctx, refs); err != nil {
		t.Fatalf("second InsertBatch (dedup): %v", err)
	}

	existing, err := blocks.Existing(ctx, [][]byte{cid, []byte("missing")})
	if err != nil {
		t.Fatalf("Existing: %v", err)
	}
	if len(existing) != 1 {
		t.Fatalf("Existing returned %d entries, want 1", len(existing))
	}
	if ref, ok := existing[string(cid)]; !ok || ref.CarID != carID {
		t.Fatalf("Existing[cid] = %+v, ok=%v; want car %d", ref, ok, carID)
	}

	if _, err := blocks.Lookup(ctx, []byte("missing")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Lookup(missing) = %v, want ErrNotFound", err)
	}
}

func TestIncrementMessageCountConcurrent(t *testing.T) {
	conn := testDB(t)
	ctx := context.Background()
	channels := NewChannelRepo(conn)

	channelID, err := channels.Add(ctx, model.Channel{TgID: 200, MessageLimit: 1000, Active: true})
	if err != nil {
		t.Fatalf("add channel: %v", err)
	}

	const n = 10
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- channels.IncrementMessageCount(ctx, channelID)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("IncrementMessageCount: %v", err)
		}
	}

	list, err := channels.List(ctx)
	if err != nil {
		t.Fatalf("list channels: %v", err)
	}
	if len(list) != 1 || list[0].MessageCount != n {
		t.Fatalf("message_count = %d, want %d", list[0].MessageCount, n)
	}
}

func TestUnpinnedCars(t *testing.T) {
	conn := testDB(t)
	ctx := context.Background()
	blocks := NewBlockRepo(conn)
	cars := NewCarRepo(conn)
	pins := NewPinRepo(conn)

	channelID, pinnedCar := newCar(t, conn, 300)
	unpinnedCar, err := cars.CreatePending(ctx, channelID, 2048, 1)
	if err != nil {
		t.Fatalf("create car: %v", err)
	}

	pinnedCID := []byte("cid-pinned")
	unpinnedCID := []byte("cid-unpinned")
	err = blocks.InsertBatch(ctx, []model.BlockRef{
		{CID: pinnedCID, CarID: pinnedCar, Offset: 0, Length: 10},
		{CID: unpinnedCID, CarID: unpinnedCar, Offset: 0, Length: 10},
	})
	if err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	root := []byte("root-1")
	if err := pins.Create(ctx, root, "pin", 10, [][]byte{pinnedCID}); err != nil {
		t.Fatalf("pin create: %v", err)
	}

	got, err := cars.UnpinnedCars(ctx)
	if err != nil {
		t.Fatalf("UnpinnedCars: %v", err)
	}
	if len(got) != 1 || got[0].ID != unpinnedCar {
		t.Fatalf("UnpinnedCars = %+v, want only car %d", got, unpinnedCar)
	}

	if err := pins.Remove(ctx, root); err != nil {
		t.Fatalf("pin remove: %v", err)
	}
	got, err = cars.UnpinnedCars(ctx)
	if err != nil {
		t.Fatalf("UnpinnedCars after unpin: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("UnpinnedCars after unpin returned %d cars, want 2", len(got))
	}
}

func TestRepoint(t *testing.T) {
	conn := testDB(t)
	ctx := context.Background()
	blocks := NewBlockRepo(conn)
	cars := NewCarRepo(conn)

	channelID, oldCar := newCar(t, conn, 400)
	newCarID, err := cars.CreatePending(ctx, channelID, 4096, 1)
	if err != nil {
		t.Fatalf("create car: %v", err)
	}

	cid := []byte("cid-repoint")
	if err := blocks.InsertBatch(ctx, []model.BlockRef{{CID: cid, CarID: oldCar, Offset: 0, Length: 10}}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	if err := blocks.Repoint(ctx, []model.BlockRef{{CID: cid, CarID: newCarID, Offset: 128, Length: 256}}); err != nil {
		t.Fatalf("Repoint: %v", err)
	}

	ref, err := blocks.Lookup(ctx, cid)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if ref.CarID != newCarID || ref.Offset != 128 || ref.Length != 256 {
		t.Fatalf("after Repoint ref = %+v, want car %d offset 128 length 256", ref, newCarID)
	}
}

func TestMTProtoFlow(t *testing.T) {
	conn := testDB(t)
	ctx := context.Background()
	creds := NewMTProtoCredsRepo(conn)

	if got, err := creds.Latest(ctx); err != nil || got != nil {
		t.Fatalf("Latest on empty = (%+v, %v), want (nil, nil)", got, err)
	}
	if got, err := creds.Active(ctx); err != nil || got != nil {
		t.Fatalf("Active on empty = (%+v, %v), want (nil, nil)", got, err)
	}

	if err := creds.Activate(ctx, 111, "hash-a"); err != nil {
		t.Fatalf("Activate a: %v", err)
	}
	active, err := creds.Active(ctx)
	if err != nil || active == nil || active.APIID != 111 || active.APIHash != "hash-a" {
		t.Fatalf("Active = (%+v, %v), want api_id 111", active, err)
	}
	firstID := active.ID

	if err := creds.Activate(ctx, 222, "hash-b"); err != nil {
		t.Fatalf("Activate b: %v", err)
	}
	active, err = creds.Active(ctx)
	if err != nil || active == nil || active.APIID != 222 {
		t.Fatalf("Active = (%+v, %v), want api_id 222", active, err)
	}

	// Reactivating the first creds must reuse the existing row.
	if err := creds.Activate(ctx, 111, "hash-a"); err != nil {
		t.Fatalf("re-Activate a: %v", err)
	}
	active, err = creds.Active(ctx)
	if err != nil || active == nil || active.ID != firstID {
		t.Fatalf("Active after reactivation = (%+v, %v), want row %d reused", active, err, firstID)
	}

	var count int
	if err := conn.GetContext(ctx, &count, `SELECT count(*) FROM mtproto_credentials`); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if count != 2 {
		t.Fatalf("credentials rows = %d, want 2 (reactivation must not insert)", count)
	}

	if err := creds.Deactivate(ctx); err != nil {
		t.Fatalf("Deactivate: %v", err)
	}
	if got, err := creds.Active(ctx); err != nil || got != nil {
		t.Fatalf("Active after Deactivate = (%+v, %v), want (nil, nil)", got, err)
	}
	latest, err := creds.Latest(ctx)
	if err != nil || latest == nil {
		t.Fatalf("Latest = (%+v, %v), want newest row", latest, err)
	}
	if latest.APIID != 222 {
		t.Fatalf("Latest api_id = %d, want 222", latest.APIID)
	}
}

func TestPinCreateIdempotent(t *testing.T) {
	conn := testDB(t)
	ctx := context.Background()
	pins := NewPinRepo(conn)

	root := []byte("root-idem")
	cids := [][]byte{[]byte("c1"), []byte("c2")}

	if err := pins.Create(ctx, root, "first", 10, cids); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	if err := pins.Create(ctx, root, "second", 20, cids); err != nil {
		t.Fatalf("second Create (idempotent): %v", err)
	}

	list, err := pins.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("List returned %d pins, want 1", len(list))
	}
	if !bytes.Equal(list[0].RootCID, root) || list[0].Name != "second" || list[0].Size != 20 {
		t.Fatalf("pin = %+v, want updated name/size", list[0])
	}

	exists, err := pins.Exists(ctx, root)
	if err != nil || !exists {
		t.Fatalf("Exists = (%v, %v), want true", exists, err)
	}

	if err := pins.Remove(ctx, root); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if err := pins.Remove(ctx, root); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second Remove = %v, want ErrNotFound", err)
	}
}

func TestRemoveStats(t *testing.T) {
	conn := testDB(t)
	ctx := context.Background()
	blocks := NewBlockRepo(conn)
	cars := NewCarRepo(conn)
	pins := NewPinRepo(conn)
	channels := NewChannelRepo(conn)

	channelID, car1 := newCar(t, conn, 500) // size 1024
	car2, err := cars.CreatePending(ctx, channelID, 4096, 1)
	if err != nil {
		t.Fatalf("create car: %v", err)
	}

	// An unrelated channel whose data must not affect the stats.
	otherChannel, otherCar := newCar(t, conn, 501)
	_ = otherChannel

	cid1, cid2, cid3 := []byte("rs-1"), []byte("rs-2"), []byte("rs-3")
	err = blocks.InsertBatch(ctx, []model.BlockRef{
		{CID: cid1, CarID: car1, Offset: 0, Length: 10},
		{CID: cid2, CarID: car2, Offset: 0, Length: 10},
		{CID: cid3, CarID: otherCar, Offset: 0, Length: 10},
	})
	if err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	// Two pins touch the channel (one of them via both cars), a third pin
	// lives entirely in the other channel.
	if err := pins.Create(ctx, []byte("rs-root-1"), "", 0, [][]byte{cid1, cid2}); err != nil {
		t.Fatalf("pin 1: %v", err)
	}
	if err := pins.Create(ctx, []byte("rs-root-2"), "", 0, [][]byte{cid2}); err != nil {
		t.Fatalf("pin 2: %v", err)
	}
	if err := pins.Create(ctx, []byte("rs-root-3"), "", 0, [][]byte{cid3}); err != nil {
		t.Fatalf("pin 3: %v", err)
	}

	pinCount, byteCount, err := channels.RemoveStats(ctx, channelID)
	if err != nil {
		t.Fatalf("RemoveStats: %v", err)
	}
	if pinCount != 2 {
		t.Fatalf("RemoveStats pins = %d, want 2", pinCount)
	}
	if byteCount != 1024+4096 {
		t.Fatalf("RemoveStats bytes = %d, want %d", byteCount, 1024+4096)
	}

	// A channel with no cars yields zeros.
	emptyChannel, err := channels.Add(ctx, model.Channel{TgID: 502, MessageLimit: 1000, Active: true})
	if err != nil {
		t.Fatalf("add empty channel: %v", err)
	}
	pinCount, byteCount, err = channels.RemoveStats(ctx, emptyChannel)
	if err != nil || pinCount != 0 || byteCount != 0 {
		t.Fatalf("RemoveStats(empty) = (%d, %d, %v), want (0, 0, nil)", pinCount, byteCount, err)
	}
}

func TestConfigRepo(t *testing.T) {
	conn := testDB(t)
	ctx := context.Background()
	cfg := NewConfigRepo(conn)

	// Seeded by the migration.
	if v, err := cfg.GetInt64(ctx, "car_max_size"); err != nil || v != 15728640 {
		t.Fatalf("GetInt64(car_max_size) = (%d, %v)", v, err)
	}
	if v, err := cfg.GetFloat64(ctx, "channel_warn_threshold"); err != nil || v != 0.9 {
		t.Fatalf("GetFloat64(channel_warn_threshold) = (%v, %v)", v, err)
	}
	if v, err := cfg.GetBool(ctx, "mtproto_enabled"); err != nil || v {
		t.Fatalf("GetBool(mtproto_enabled) = (%v, %v)", v, err)
	}
	if _, err := cfg.Get(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(nope) = %v, want ErrNotFound", err)
	}

	if err := cfg.Set(ctx, "mtproto_enabled", "true"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if v, err := cfg.GetBool(ctx, "mtproto_enabled"); err != nil || !v {
		t.Fatalf("GetBool after Set = (%v, %v), want true", v, err)
	}
	if err := cfg.Set(ctx, "new_key", "abc"); err != nil {
		t.Fatalf("Set new key: %v", err)
	}
	if v, err := cfg.Get(ctx, "new_key"); err != nil || v != "abc" {
		t.Fatalf("Get(new_key) = (%q, %v)", v, err)
	}
}

func TestBotRepo(t *testing.T) {
	conn := testDB(t)
	ctx := context.Background()
	bots := NewBotRepo(conn)

	id, err := bots.Add(ctx, model.Bot{TgID: 1, Username: "a_bot", Token: "tok-a", Active: true})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := bots.Add(ctx, model.Bot{TgID: 1, Username: "dup", Token: "tok-b", Active: true}); !errors.Is(err, ErrBotExists) {
		t.Fatalf("Add duplicate tg_id = %v, want ErrBotExists", err)
	}
	if _, err := bots.Add(ctx, model.Bot{TgID: 2, Username: "dup", Token: "tok-a", Active: true}); !errors.Is(err, ErrBotExists) {
		t.Fatalf("Add duplicate token = %v, want ErrBotExists", err)
	}

	until := time.Now().Add(time.Hour).UTC().Truncate(time.Millisecond)
	if err := bots.SetUnavailableUntil(ctx, id, until); err != nil {
		t.Fatalf("SetUnavailableUntil: %v", err)
	}
	if err := bots.SetActive(ctx, id, false); err != nil {
		t.Fatalf("SetActive: %v", err)
	}

	b, err := bots.GetByID(ctx, id)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if b.Active || b.UnavailableUntil == nil || !b.UnavailableUntil.Equal(until) {
		t.Fatalf("bot = %+v, want inactive with unavailable_until %v", b, until)
	}

	if err := bots.Remove(ctx, id); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := bots.GetByID(ctx, id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetByID after Remove = %v, want ErrNotFound", err)
	}
}

func TestCarFileIDsAndOrphans(t *testing.T) {
	conn := testDB(t)
	ctx := context.Background()
	cars := NewCarRepo(conn)
	bots := NewBotRepo(conn)

	_, carID := newCar(t, conn, 600)
	botID, err := bots.Add(ctx, model.Bot{TgID: 10, Username: "b", Token: "t", Active: true})
	if err != nil {
		t.Fatalf("add bot: %v", err)
	}

	if err := cars.UpsertFileID(ctx, carID, botID, "file-1"); err != nil {
		t.Fatalf("UpsertFileID: %v", err)
	}
	if err := cars.UpsertFileID(ctx, carID, botID, "file-2"); err != nil {
		t.Fatalf("UpsertFileID update: %v", err)
	}
	ids, err := cars.FileIDs(ctx, carID)
	if err != nil {
		t.Fatalf("FileIDs: %v", err)
	}
	if len(ids) != 1 || ids[botID] != "file-2" {
		t.Fatalf("FileIDs = %v, want {%d: file-2}", ids, botID)
	}
	if err := cars.DeleteFileID(ctx, carID, botID); err != nil {
		t.Fatalf("DeleteFileID: %v", err)
	}
	ids, err = cars.FileIDs(ctx, carID)
	if err != nil || len(ids) != 0 {
		t.Fatalf("FileIDs after delete = (%v, %v), want empty", ids, err)
	}

	// The pending car is younger than an hour: not an orphan yet.
	orphans, err := cars.OrphanPending(ctx, time.Hour)
	if err != nil || len(orphans) != 0 {
		t.Fatalf("OrphanPending(1h) = (%v, %v), want none", orphans, err)
	}
	orphans, err = cars.OrphanPending(ctx, 0)
	if err != nil || len(orphans) != 1 || orphans[0].ID != carID {
		t.Fatalf("OrphanPending(0) = (%v, %v), want car %d", orphans, err, carID)
	}

	if err := cars.MarkPublished(ctx, carID, 777); err != nil {
		t.Fatalf("MarkPublished: %v", err)
	}
	car, err := cars.Get(ctx, carID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if car.Status != model.CarPublished || car.MessageID == nil || *car.MessageID != 777 {
		t.Fatalf("car = %+v, want published with message_id 777", car)
	}
	orphans, err = cars.OrphanPending(ctx, 0)
	if err != nil || len(orphans) != 0 {
		t.Fatalf("OrphanPending after publish = (%v, %v), want none", orphans, err)
	}

	if err := cars.SetStatus(ctx, carID, model.CarNoBotAccess); err != nil {
		t.Fatalf("SetStatus: %v", err)
	}
	if err := cars.Delete(ctx, carID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := cars.Get(ctx, carID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after Delete = %v, want ErrNotFound", err)
	}
}

func TestChannelMembership(t *testing.T) {
	conn := testDB(t)
	ctx := context.Background()
	channels := NewChannelRepo(conn)
	bots := NewBotRepo(conn)

	channelID, err := channels.Add(ctx, model.Channel{TgID: 700, MessageLimit: 1000, Active: true})
	if err != nil {
		t.Fatalf("add channel: %v", err)
	}
	if _, err := channels.GetByTgID(ctx, 700); err != nil {
		t.Fatalf("GetByTgID: %v", err)
	}
	if _, err := channels.GetByTgID(ctx, 999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetByTgID(999) = %v, want ErrNotFound", err)
	}

	botID, err := bots.Add(ctx, model.Bot{TgID: 20, Username: "m", Token: "tm", Active: true})
	if err != nil {
		t.Fatalf("add bot: %v", err)
	}

	bc := model.BotChannel{
		BotID: botID, ChannelID: channelID,
		CanPost: true, CanRead: true, Member: true,
		VerifiedAt: time.Now().UTC().Truncate(time.Millisecond),
	}
	if err := channels.UpsertBotChannel(ctx, bc); err != nil {
		t.Fatalf("UpsertBotChannel: %v", err)
	}
	bc.CanDelete = true
	if err := channels.UpsertBotChannel(ctx, bc); err != nil {
		t.Fatalf("UpsertBotChannel update: %v", err)
	}

	members, err := channels.MembersOf(ctx, channelID)
	if err != nil {
		t.Fatalf("MembersOf: %v", err)
	}
	if len(members) != 1 || !members[0].CanDelete {
		t.Fatalf("MembersOf = %+v, want one member with can_delete", members)
	}

	if err := channels.Remove(ctx, channelID); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if err := channels.Remove(ctx, channelID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second Remove = %v, want ErrNotFound", err)
	}
}
