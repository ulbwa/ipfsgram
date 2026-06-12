package daemon

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multihash"

	"github.com/ulbwa/ipfsgram/internal/cache"
	"github.com/ulbwa/ipfsgram/internal/model"
	"github.com/ulbwa/ipfsgram/internal/repo"
	"github.com/ulbwa/ipfsgram/internal/tg"
)

// --- fakes ---------------------------------------------------------------

type fakeBlocks struct {
	refs map[string]model.BlockRef
}

func (f *fakeBlocks) Lookup(_ context.Context, c []byte) (model.BlockRef, error) {
	ref, ok := f.refs[string(c)]
	if !ok {
		return model.BlockRef{}, repo.ErrNotFound
	}
	return ref, nil
}

type fakeCars struct {
	mu      sync.Mutex
	car     model.Car
	fileIDs map[int64]string

	deleted        []int64
	statuses       []model.CarStatus
	upserted       map[int64]string // botID -> fileID
	deletedFileIDs []int64          // botIDs
}

func (f *fakeCars) Get(_ context.Context, carID int64) (model.Car, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.car.ID != carID {
		return model.Car{}, repo.ErrNotFound
	}
	return f.car, nil
}

func (f *fakeCars) Delete(_ context.Context, carID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, carID)
	return nil
}

func (f *fakeCars) SetStatus(_ context.Context, _ int64, st model.CarStatus) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statuses = append(f.statuses, st)
	return nil
}

func (f *fakeCars) FileIDs(_ context.Context, _ int64) (map[int64]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[int64]string, len(f.fileIDs))
	for k, v := range f.fileIDs {
		out[k] = v
	}
	return out, nil
}

func (f *fakeCars) UpsertFileID(_ context.Context, _, botID int64, fileID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.upserted == nil {
		f.upserted = make(map[int64]string)
	}
	f.upserted[botID] = fileID
	return nil
}

func (f *fakeCars) DeleteFileID(_ context.Context, _, botID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletedFileIDs = append(f.deletedFileIDs, botID)
	return nil
}

type fakeBots struct {
	mu          sync.Mutex
	bots        []model.Bot
	unavailable map[int64]time.Time
}

func (f *fakeBots) List(_ context.Context) ([]model.Bot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]model.Bot, len(f.bots))
	copy(out, f.bots)
	return out, nil
}

func (f *fakeBots) SetUnavailableUntil(_ context.Context, id int64, until time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unavailable == nil {
		f.unavailable = make(map[int64]time.Time)
	}
	f.unavailable[id] = until
	return nil
}

type fakeMembers struct {
	members []model.BotChannel
}

func (f *fakeMembers) MembersOf(context.Context, int64) ([]model.BotChannel, error) {
	return f.members, nil
}

type fakeChannels struct {
	channel model.Channel
}

func (f *fakeChannels) ChannelByID(context.Context, int64) (model.Channel, error) {
	return f.channel, nil
}

// fakeTransport implements tg.Transport; only Download is exercised.
type fakeTransport struct {
	download func(token string, channelTgID, messageID int64, fileID string) (io.ReadCloser, string, error)
}

func (f *fakeTransport) Download(_ context.Context, token string, channelTgID, messageID int64, fileID string) (io.ReadCloser, string, error) {
	return f.download(token, channelTgID, messageID, fileID)
}

func (f *fakeTransport) ValidateToken(context.Context, string) (int64, string, error) {
	panic("unexpected ValidateToken")
}
func (f *fakeTransport) ProbeChannel(context.Context, string, int64) (tg.ChannelInfo, error) {
	panic("unexpected ProbeChannel")
}
func (f *fakeTransport) Upload(context.Context, string, int64, string, int64, io.Reader) (tg.UploadResult, error) {
	panic("unexpected Upload")
}
func (f *fakeTransport) CheckMessage(context.Context, string, int64, int64) error {
	panic("unexpected CheckMessage")
}
func (f *fakeTransport) DeleteMessage(context.Context, string, int64, int64) error {
	panic("unexpected DeleteMessage")
}

// --- helpers -------------------------------------------------------------

func mustCID(t *testing.T, data []byte) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum(data, multihash.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	return cid.NewCidV1(cid.Raw, mh)
}

// carFixture builds a fake CAR payload with the block embedded at a known
// offset and returns (carBytes, blockData, cid, offset).
func carFixture(t *testing.T) ([]byte, []byte, cid.Cid, int64) {
	t.Helper()
	header := []byte("car-header-padding")
	block := []byte("hello ipfsgram block payload")
	car := append(append([]byte{}, header...), block...)
	return car, block, mustCID(t, block), int64(len(header))
}

func msgID(v int64) *int64 { return &v }

type fixture struct {
	bs       *Blockstore
	cars     *fakeCars
	bots     *fakeBots
	cache    cache.Cache
	cacheDir string
}

func newFixture(t *testing.T, ref model.BlockRef, c cid.Cid, car model.Car, bots []model.Bot, transport tg.Transport) *fixture {
	t.Helper()
	cacheDir := t.TempDir()
	lru, err := cache.NewLRU(cacheDir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lru.Close() })

	members := make([]model.BotChannel, 0, len(bots))
	for _, b := range bots {
		members = append(members, model.BotChannel{
			BotID: b.ID, ChannelID: car.ChannelID, Member: true, CanRead: true,
		})
	}
	cars := &fakeCars{car: car, fileIDs: map[int64]string{}}
	botStore := &fakeBots{bots: bots}
	bs := NewBlockstore(
		&fakeBlocks{refs: map[string]model.BlockRef{string(c.Bytes()): ref}},
		cars,
		botStore,
		&fakeMembers{members: members},
		&fakeChannels{channel: model.Channel{ID: car.ChannelID, TgID: -100123}},
		nil, // keys: not exercised
		lru,
		tmpDirFor(cacheDir),
		transport,
	)
	return &fixture{bs: bs, cars: cars, bots: botStore, cache: lru, cacheDir: cacheDir}
}

// --- tests ---------------------------------------------------------------

func TestGetCacheHit(t *testing.T) {
	carBytes, block, c, off := carFixture(t)
	ref := model.BlockRef{CID: c.Bytes(), CarID: 7, Offset: off, Length: int32(len(block))}
	car := model.Car{ID: 7, ChannelID: 1, MessageID: msgID(42), Status: model.CarPublished}

	transport := &fakeTransport{download: func(string, int64, int64, string) (io.ReadCloser, string, error) {
		t.Fatal("transport must not be hit on cache hit")
		return nil, "", nil
	}}
	f := newFixture(t, ref, c, car, nil, transport)

	src := filepath.Join(t.TempDir(), "seed.car")
	if err := os.WriteFile(src, carBytes, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := f.cache.Put(7, src, int64(len(carBytes))); err != nil {
		t.Fatal(err)
	}

	got, err := f.bs.Get(context.Background(), c)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got.RawData(), block) {
		t.Fatalf("payload mismatch: got %q want %q", got.RawData(), block)
	}
}

func TestGetMessageDeleted(t *testing.T) {
	_, block, c, off := carFixture(t)
	ref := model.BlockRef{CID: c.Bytes(), CarID: 7, Offset: off, Length: int32(len(block))}
	car := model.Car{ID: 7, ChannelID: 1, MessageID: msgID(42), Status: model.CarPublished}
	bots := []model.Bot{{ID: 1, Token: "t1", Active: true}}

	transport := &fakeTransport{download: func(string, int64, int64, string) (io.ReadCloser, string, error) {
		return nil, "", tg.ErrMessageDeleted
	}}
	f := newFixture(t, ref, c, car, bots, transport)

	_, err := f.bs.Get(context.Background(), c)
	if !ipld.IsNotFound(err) {
		t.Fatalf("want ipld.ErrNotFound, got %v", err)
	}
	if len(f.cars.deleted) != 1 || f.cars.deleted[0] != 7 {
		t.Fatalf("want car 7 deleted, got %v", f.cars.deleted)
	}
}

func TestGetNoAccess(t *testing.T) {
	_, block, c, off := carFixture(t)
	ref := model.BlockRef{CID: c.Bytes(), CarID: 7, Offset: off, Length: int32(len(block))}
	car := model.Car{ID: 7, ChannelID: 1, MessageID: msgID(42), Status: model.CarPublished}
	bots := []model.Bot{{ID: 1, Token: "t1", Active: true}}

	transport := &fakeTransport{download: func(string, int64, int64, string) (io.ReadCloser, string, error) {
		return nil, "", tg.ErrNoAccess
	}}
	f := newFixture(t, ref, c, car, bots, transport)

	_, err := f.bs.Get(context.Background(), c)
	if !ipld.IsNotFound(err) {
		t.Fatalf("want ipld.ErrNotFound, got %v", err)
	}
	if len(f.cars.deleted) != 0 {
		t.Fatalf("car must NOT be deleted on no-access, got deletions %v", f.cars.deleted)
	}
	if len(f.cars.statuses) != 1 || f.cars.statuses[0] != model.CarNoBotAccess {
		t.Fatalf("want status no_bot_access, got %v", f.cars.statuses)
	}
}

func TestGetFloodWaitFailsOver(t *testing.T) {
	carBytes, block, c, off := carFixture(t)
	ref := model.BlockRef{CID: c.Bytes(), CarID: 7, Offset: off, Length: int32(len(block))}
	// Status no_bot_access: a successful download must lazily recover it.
	car := model.Car{ID: 7, ChannelID: 1, MessageID: msgID(42), Status: model.CarNoBotAccess}
	bots := []model.Bot{
		{ID: 1, Token: "t1", Active: true},
		{ID: 2, Token: "t2", Active: true},
	}

	var tokens []string
	var mu sync.Mutex
	transport := &fakeTransport{download: func(token string, _, _ int64, _ string) (io.ReadCloser, string, error) {
		mu.Lock()
		tokens = append(tokens, token)
		mu.Unlock()
		if token == "t1" {
			return nil, "", &tg.FloodWaitError{RetryAfter: time.Minute}
		}
		return io.NopCloser(bytes.NewReader(carBytes)), "fresh-file-id", nil
	}}
	f := newFixture(t, ref, c, car, bots, transport)

	got, err := f.bs.Get(context.Background(), c)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got.RawData(), block) {
		t.Fatalf("payload mismatch: got %q want %q", got.RawData(), block)
	}
	if len(tokens) != 2 || tokens[0] != "t1" || tokens[1] != "t2" {
		t.Fatalf("want failover t1->t2, got %v", tokens)
	}
	if _, ok := f.bots.unavailable[1]; !ok {
		t.Fatal("flood-waited bot 1 must be marked unavailable")
	}
	if f.cars.upserted[2] != "fresh-file-id" {
		t.Fatalf("fresh file_id must be recorded for bot 2, got %v", f.cars.upserted)
	}
	if len(f.cars.statuses) != 1 || f.cars.statuses[0] != model.CarPublished {
		t.Fatalf("want lazy recovery to published, got %v", f.cars.statuses)
	}
	// Second Get must be served from cache without touching the transport.
	if _, err := f.bs.Get(context.Background(), c); err != nil {
		t.Fatalf("cached Get: %v", err)
	}
	if len(tokens) != 2 {
		t.Fatalf("transport hit on cached Get: %v", tokens)
	}
}

func TestGetRetriesAfterEviction(t *testing.T) {
	carBytes, block, c, off := carFixture(t)
	ref := model.BlockRef{CID: c.Bytes(), CarID: 7, Offset: off, Length: int32(len(block))}
	car := model.Car{ID: 7, ChannelID: 1, MessageID: msgID(42), Status: model.CarPublished}
	bots := []model.Bot{{ID: 1, Token: "t1", Active: true}}

	var downloads int
	var mu sync.Mutex
	transport := &fakeTransport{download: func(string, int64, int64, string) (io.ReadCloser, string, error) {
		mu.Lock()
		downloads++
		mu.Unlock()
		return io.NopCloser(bytes.NewReader(carBytes)), "", nil
	}}
	f := newFixture(t, ref, c, car, bots, transport)

	// Seed the cache so the first resolution is a hit.
	src := filepath.Join(t.TempDir(), "seed.car")
	if err := os.WriteFile(src, carBytes, 0o644); err != nil {
		t.Fatal(err)
	}
	path, err := f.cache.Put(7, src, int64(len(carBytes)))
	if err != nil {
		t.Fatal(err)
	}

	// Fake an eviction race: the cache still indexes the entry, but the file
	// is gone by the time Get tries to read it.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}

	got, err := f.bs.Get(context.Background(), c)
	if err != nil {
		t.Fatalf("Get after eviction: %v", err)
	}
	if !bytes.Equal(got.RawData(), block) {
		t.Fatalf("payload mismatch: got %q want %q", got.RawData(), block)
	}
	if downloads != 1 {
		t.Fatalf("want exactly one recovery download, got %d", downloads)
	}
}

func TestGetCallerCancelDoesNotAbortDownload(t *testing.T) {
	carBytes, block, c, off := carFixture(t)
	ref := model.BlockRef{CID: c.Bytes(), CarID: 7, Offset: off, Length: int32(len(block))}
	car := model.Car{ID: 7, ChannelID: 1, MessageID: msgID(42), Status: model.CarPublished}
	bots := []model.Bot{{ID: 1, Token: "t1", Active: true}}

	started := make(chan struct{})
	release := make(chan struct{})
	var downloads int
	var mu sync.Mutex
	transport := &fakeTransport{download: func(string, int64, int64, string) (io.ReadCloser, string, error) {
		mu.Lock()
		downloads++
		mu.Unlock()
		close(started)
		<-release
		return io.NopCloser(bytes.NewReader(carBytes)), "", nil
	}}
	f := newFixture(t, ref, c, car, bots, transport)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := f.bs.Get(ctx, c)
		errCh <- err
	}()

	<-started
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled caller: want context.Canceled, got %v", err)
	}

	// The download must keep going despite the cancelled caller; once it
	// completes, the car is cached and a fresh Get is served without a
	// second transport hit.
	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := f.cache.Get(7); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("detached download never populated the cache")
		}
		time.Sleep(10 * time.Millisecond)
	}

	got, err := f.bs.Get(context.Background(), c)
	if err != nil {
		t.Fatalf("Get after detached download: %v", err)
	}
	if !bytes.Equal(got.RawData(), block) {
		t.Fatalf("payload mismatch: got %q want %q", got.RawData(), block)
	}
	mu.Lock()
	defer mu.Unlock()
	if downloads != 1 {
		t.Fatalf("want exactly one download, got %d", downloads)
	}
}

func TestGetUnknownCID(t *testing.T) {
	_, block, c, off := carFixture(t)
	ref := model.BlockRef{CID: c.Bytes(), CarID: 7, Offset: off, Length: int32(len(block))}
	car := model.Car{ID: 7, ChannelID: 1, MessageID: msgID(42), Status: model.CarPublished}
	f := newFixture(t, ref, c, car, nil, &fakeTransport{})

	other := mustCID(t, []byte("missing"))
	_, err := f.bs.Get(context.Background(), other)
	if !ipld.IsNotFound(err) {
		t.Fatalf("want ipld.ErrNotFound, got %v", err)
	}
	if has, err := f.bs.Has(context.Background(), other); err != nil || has {
		t.Fatalf("Has(missing) = %v, %v; want false, nil", has, err)
	}
	if has, err := f.bs.Has(context.Background(), c); err != nil || !has {
		t.Fatalf("Has(known) = %v, %v; want true, nil", has, err)
	}
	if n, err := f.bs.GetSize(context.Background(), c); err != nil || n != len(block) {
		t.Fatalf("GetSize = %d, %v; want %d, nil", n, err, len(block))
	}
}

func TestReadOnly(t *testing.T) {
	_, block, c, off := carFixture(t)
	ref := model.BlockRef{CID: c.Bytes(), CarID: 7, Offset: off, Length: int32(len(block))}
	f := newFixture(t, ref, c, model.Car{ID: 7, ChannelID: 1}, nil, &fakeTransport{})

	if err := f.bs.Put(context.Background(), nil); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Put: want ErrReadOnly, got %v", err)
	}
	if err := f.bs.PutMany(context.Background(), nil); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("PutMany: want ErrReadOnly, got %v", err)
	}
	if err := f.bs.DeleteBlock(context.Background(), c); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("DeleteBlock: want ErrReadOnly, got %v", err)
	}
}
