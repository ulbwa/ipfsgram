// publish_test.go — happy-path pipeline test with an in-memory fake database,
// fake transport and a fake packer substituted via the newPacker hook.

package publish

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/rs/zerolog"

	"github.com/ulbwa/ipfsgram/internal/block"
	"github.com/ulbwa/ipfsgram/internal/car"
	"github.com/ulbwa/ipfsgram/internal/selector"
	"github.com/ulbwa/ipfsgram/internal/store"
	"github.com/ulbwa/ipfsgram/internal/telegram"
)

func testCID(t *testing.T, s string) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum([]byte(s), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	return cid.NewCidV1(cid.Raw, mh)
}

// --- fakes ---

// fakeDB is an in-memory implementation of the package's database interface.
type fakeDB struct {
	channels []store.Channel
	members  []store.BotChannel
	bots     []store.Bot

	lockCalls     int
	pending       int
	published     int
	inserted      []store.BlockRef
	pinsCreated   int
	pinName       string
	incrementedCh []int64
}

func (d *fakeDB) ConfigInt64(context.Context, string) (int64, error) {
	return 0, store.ErrNotFound
}
func (d *fakeDB) ConfigFloat64(context.Context, string) (float64, error) {
	return 0, store.ErrNotFound
}
func (d *fakeDB) ExistingBlocks(context.Context, [][]byte) (map[string]store.BlockRef, error) {
	return map[string]store.BlockRef{}, nil
}
func (d *fakeDB) UpsertBlocks(_ context.Context, blocks []store.BlockRef) error {
	d.inserted = append(d.inserted, blocks...)
	return nil
}
func (d *fakeDB) Car(context.Context, int64) (store.Car, error) {
	return store.Car{}, store.ErrNotFound
}
func (d *fakeDB) DeleteCar(context.Context, int64) error                     { return nil }
func (d *fakeDB) SetCarStatus(context.Context, int64, store.CarStatus) error { return nil }
func (d *fakeDB) CreatePendingCar(context.Context, int64, int64, int) (int64, error) {
	d.pending++
	return int64(d.pending), nil
}
func (d *fakeDB) MarkCarPublished(context.Context, int64, int64) error {
	d.published++
	return nil
}
func (d *fakeDB) UpsertCarFileID(context.Context, int64, int64, string) error { return nil }
func (d *fakeDB) CarFileIDs(context.Context, int64) (map[int64]string, error) {
	return map[int64]string{}, nil
}
func (d *fakeDB) Channels(context.Context) ([]store.Channel, error) { return d.channels, nil }
func (d *fakeDB) ChannelMembers(context.Context, int64) ([]store.BotChannel, error) {
	return d.members, nil
}
func (d *fakeDB) IncrementMessageCount(_ context.Context, id int64) error {
	d.incrementedCh = append(d.incrementedCh, id)
	return nil
}
func (d *fakeDB) Bots(context.Context) ([]store.Bot, error)                 { return d.bots, nil }
func (d *fakeDB) SetBotUnavailable(context.Context, int64, time.Time) error { return nil }
func (d *fakeDB) CreatePin(_ context.Context, _ []byte, name string, _ int64, _ [][]byte) error {
	d.pinsCreated++
	d.pinName = name
	return nil
}
func (d *fakeDB) NotifyNewContent(context.Context, []byte) error { return nil }
func (d *fakeDB) WithSharedPublishLock(ctx context.Context, fn func(ctx context.Context) error) error {
	d.lockCalls++
	return fn(ctx)
}

type fakeTransport struct {
	uploads int
}

func (t *fakeTransport) Upload(_ context.Context, _ string, _ int64, _ string, _ int64, r io.Reader) (telegram.UploadResult, error) {
	t.uploads++
	io.Copy(io.Discard, r)
	return telegram.UploadResult{MessageID: 100, FileID: "fid"}, nil
}
func (t *fakeTransport) CheckMessage(context.Context, string, int64, int64) error { return nil }

// fakePacker writes a single real CAR-ish file and reports one block.
type fakePacker struct {
	dir    string
	blocks []car.PackedBlock
}

func (p *fakePacker) Add(c cid.Cid, data []byte) error {
	p.blocks = append(p.blocks, car.PackedBlock{CID: c, Offset: 0, Length: int32(len(data))})
	return nil
}
func (p *fakePacker) Finish() ([]car.PackedCar, error) {
	f, err := os.CreateTemp(p.dir, "car-*.car")
	if err != nil {
		return nil, err
	}
	f.WriteString("car")
	f.Close()
	return []car.PackedCar{{Path: f.Name(), Size: 3, Blocks: p.blocks}}, nil
}

func TestPublishHappyPath(t *testing.T) {
	root := testCID(t, "root")
	blkCID := testCID(t, "block")

	db := &fakeDB{
		channels: []store.Channel{{ID: 1, TgID: -100, Title: "ch", MessageLimit: 1000, Active: true}},
		members:  []store.BotChannel{{BotID: 1, ChannelID: 1, Member: true, CanPost: true}},
		bots:     []store.Bot{{ID: 1, Active: true, Token: "t", Username: "b"}},
	}
	tr := &fakeTransport{}

	p := New(db, tr, zerolog.New(io.Discard))
	p.newPacker = func(dir string, _ int64, _ cid.Cid) (carPacker, error) {
		return &fakePacker{dir: dir}, nil
	}
	p.loads = selector.NewLoadCounter()

	got, err := p.Publish(context.Background(), root, "myname",
		[]block.Block{{CID: blkCID, Data: []byte("hello")}})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got != root {
		t.Errorf("root = %s, want %s", got, root)
	}
	if db.lockCalls != 1 {
		t.Errorf("shared publish lock taken %d times, want 1", db.lockCalls)
	}
	if tr.uploads != 1 {
		t.Errorf("uploads = %d, want 1", tr.uploads)
	}
	if db.published != 1 {
		t.Errorf("cars published = %d, want 1", db.published)
	}
	if len(db.inserted) != 1 {
		t.Errorf("inserted blocks = %d, want 1", len(db.inserted))
	}
	if db.pinsCreated != 1 || db.pinName != "myname" {
		t.Errorf("pin created = %d name = %q, want 1 / myname", db.pinsCreated, db.pinName)
	}
	if len(db.incrementedCh) != 1 || db.incrementedCh[0] != 1 {
		t.Errorf("incremented channels = %v, want [1]", db.incrementedCh)
	}
}
