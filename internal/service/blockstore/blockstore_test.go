package blockstore

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
	mh "github.com/multiformats/go-multihash"
	"github.com/rs/zerolog"

	"github.com/ulbwa/ipfsgram/internal/adapter/carpack"
	"github.com/ulbwa/ipfsgram/internal/adapter/diskcache"
	"github.com/ulbwa/ipfsgram/internal/adapter/selector"
	"github.com/ulbwa/ipfsgram/internal/domain"
	"github.com/ulbwa/ipfsgram/internal/port"
)

// --- fakes ---------------------------------------------------------------

type fakeBlocks struct {
	refs map[string]domain.Block
}

func (f *fakeBlocks) Lookup(_ context.Context, c []byte) (domain.Block, error) {
	b, ok := f.refs[string(c)]
	if !ok {
		return domain.Block{}, domain.ErrNotFound
	}
	return b, nil
}
func (f *fakeBlocks) Existing(context.Context, [][]byte) (map[string]domain.Block, error) {
	panic("unexpected Existing")
}
func (f *fakeBlocks) InsertBatch(context.Context, []domain.Block) error {
	panic("unexpected InsertBatch")
}
func (f *fakeBlocks) Repoint(context.Context, []domain.Block) error { panic("unexpected Repoint") }
func (f *fakeBlocks) CountAll(context.Context) (int64, error)       { panic("unexpected CountAll") }

func (f *fakeBlocks) StreamAllCIDs(ctx context.Context) (<-chan []byte, <-chan error) {
	out := make(chan []byte)
	errCh := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errCh)
		for k := range f.refs {
			select {
			case out <- []byte(k):
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, errCh
}

type fakeCars struct {
	mu      sync.Mutex
	car     domain.Car
	fileIDs map[int64]string

	deleted        []int64
	statuses       []domain.CarStatus
	upserted       map[int64]string // botID -> fileID
	deletedFileIDs []int64          // botIDs
}

func (f *fakeCars) Get(_ context.Context, carID int64) (domain.Car, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.car.ID != carID {
		return domain.Car{}, domain.ErrNotFound
	}
	return f.car, nil
}
func (f *fakeCars) Delete(_ context.Context, carID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, carID)
	return nil
}
func (f *fakeCars) SetStatus(_ context.Context, _ int64, st domain.CarStatus) error {
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

// Unused CarRepository methods.
func (f *fakeCars) CreatePending(context.Context, int64, int64, int) (int64, error) {
	panic("unexpected CreatePending")
}
func (f *fakeCars) MarkPublished(context.Context, int64, int64) error {
	panic("unexpected MarkPublished")
}
func (f *fakeCars) OrphanPending(context.Context, time.Duration) ([]domain.Car, error) {
	panic("unexpected OrphanPending")
}
func (f *fakeCars) UnpinnedCars(context.Context) ([]domain.Car, error) {
	panic("unexpected UnpinnedCars")
}
func (f *fakeCars) ByStatuses(context.Context, ...domain.CarStatus) ([]domain.Car, error) {
	panic("unexpected ByStatuses")
}
func (f *fakeCars) AccessibleOnlyVia(context.Context, int64) ([]domain.Car, error) {
	panic("unexpected AccessibleOnlyVia")
}
func (f *fakeCars) CountByStatus(context.Context) (map[domain.CarStatus]int64, error) {
	panic("unexpected CountByStatus")
}

type fakeBots struct {
	mu          sync.Mutex
	bots        []domain.Bot
	unavailable map[int64]time.Time
}

func (f *fakeBots) List(_ context.Context) ([]domain.Bot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]domain.Bot, len(f.bots))
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

// Unused BotRepository methods.
func (f *fakeBots) Add(context.Context, domain.Bot) (int64, error) { panic("unexpected Add") }
func (f *fakeBots) GetByID(context.Context, int64) (domain.Bot, error) {
	panic("unexpected GetByID")
}
func (f *fakeBots) GetByUsername(context.Context, string) (domain.Bot, error) {
	panic("unexpected GetByUsername")
}
func (f *fakeBots) Remove(context.Context, int64) error          { panic("unexpected Remove") }
func (f *fakeBots) SetActive(context.Context, int64, bool) error { panic("unexpected SetActive") }

type fakeChannels struct {
	channel domain.Channel
	members []domain.BotChannel
}

func (f *fakeChannels) GetByID(context.Context, int64) (domain.Channel, error) {
	return f.channel, nil
}
func (f *fakeChannels) MembersOf(context.Context, int64) ([]domain.BotChannel, error) {
	return f.members, nil
}

// Unused ChannelRepository methods.
func (f *fakeChannels) Add(context.Context, domain.Channel) (int64, error) { panic("unexpected Add") }
func (f *fakeChannels) List(context.Context) ([]domain.Channel, error)     { panic("unexpected List") }
func (f *fakeChannels) GetByTgID(context.Context, int64) (domain.Channel, error) {
	panic("unexpected GetByTgID")
}
func (f *fakeChannels) Remove(context.Context, int64) error { panic("unexpected Remove") }
func (f *fakeChannels) IncrementMessageCount(context.Context, int64) error {
	panic("unexpected IncrementMessageCount")
}
func (f *fakeChannels) UpsertBotChannel(context.Context, domain.BotChannel) error {
	panic("unexpected UpsertBotChannel")
}
func (f *fakeChannels) RemoveStats(context.Context, int64) (int64, int64, error) {
	panic("unexpected RemoveStats")
}
func (f *fakeChannels) DeleteOrphanPins(context.Context) (int64, error) {
	panic("unexpected DeleteOrphanPins")
}

// fakeTransport implements port.Transport; only Download is exercised.
type fakeTransport struct {
	download func(token string, channelTgID, messageID int64, fileID string) (io.ReadCloser, string, error)
}

func (f *fakeTransport) Download(_ context.Context, token string, channelTgID, messageID int64, fileID string) (io.ReadCloser, string, error) {
	return f.download(token, channelTgID, messageID, fileID)
}
func (f *fakeTransport) ValidateToken(context.Context, string) (int64, string, error) {
	panic("unexpected ValidateToken")
}
func (f *fakeTransport) ProbeChannel(context.Context, string, int64) (port.ChannelInfo, error) {
	panic("unexpected ProbeChannel")
}
func (f *fakeTransport) Upload(context.Context, string, int64, string, int64, io.Reader) (port.UploadResult, error) {
	panic("unexpected Upload")
}
func (f *fakeTransport) CheckMessage(context.Context, string, int64, int64) error {
	panic("unexpected CheckMessage")
}
func (f *fakeTransport) DeleteMessage(context.Context, string, int64, int64) error {
	panic("unexpected DeleteMessage")
}

// --- helpers -------------------------------------------------------------

// carFixture builds a real CARv1 file via the carpack adapter and returns the
// CAR bytes, the embedded block payload, its CID, offset and length.
func carFixture(t *testing.T) (carBytes, block []byte, c cid.Cid, off int64, length int32) {
	t.Helper()
	block = []byte("hello ipfsgram block payload")
	h, err := mh.Sum(block, mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	c = cid.NewCidV1(cid.Raw, h)

	dir := t.TempDir()
	p, err := carpack.NewFactory().New(dir, 1<<20, c)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Add(c, block); err != nil {
		t.Fatal(err)
	}
	packed, err := p.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if len(packed) != 1 || len(packed[0].Blocks) != 1 {
		t.Fatalf("unexpected packer output: %+v", packed)
	}
	carBytes, err = os.ReadFile(packed[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	pb := packed[0].Blocks[0]
	return carBytes, block, c, pb.Offset, pb.Length
}

func msgID(v int64) *int64 { return &v }

type fixture struct {
	bs       *Blockstore
	cars     *fakeCars
	bots     *fakeBots
	cache    port.Cache
	cacheDir string
}

func newFixture(t *testing.T, b domain.Block, c cid.Cid, car domain.Car, bots []domain.Bot, transport port.Transport) *fixture {
	t.Helper()
	cacheDir := t.TempDir()
	lru, err := diskcache.NewLRU(cacheDir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lru.Close() })

	members := make([]domain.BotChannel, 0, len(bots))
	for _, bot := range bots {
		members = append(members, domain.BotChannel{
			BotID: bot.ID, ChannelID: car.ChannelID, Member: true, CanRead: true,
		})
	}
	cars := &fakeCars{car: car, fileIDs: map[int64]string{}}
	botStore := &fakeBots{bots: bots}
	bs := New(Deps{
		Blocks:      &fakeBlocks{refs: map[string]domain.Block{string(c.Bytes()): b}},
		Cars:        cars,
		Bots:        botStore,
		Channels:    &fakeChannels{channel: domain.Channel{ID: car.ChannelID, TgID: -100123}, members: members},
		Transport:   transport,
		Cache:       lru,
		Reader:      carpack.NewReader(),
		Selector:    selector.New(),
		Logger:      zerolog.Nop(),
		CacheTmpDir: filepath.Join(cacheDir, "tmp"),
	})
	return &fixture{bs: bs, cars: cars, bots: botStore, cache: lru, cacheDir: cacheDir}
}

// --- tests ---------------------------------------------------------------

func TestGetCacheHit(t *testing.T) {
	carBytes, block, c, off, length := carFixture(t)
	ref := domain.Block{CID: c.Bytes(), CarID: 7, Offset: off, Length: length}
	car := domain.Car{ID: 7, ChannelID: 1, MessageID: msgID(42), Status: domain.CarPublished}

	transport := &fakeTransport{download: func(string, int64, int64, string) (io.ReadCloser, string, error) {
		t.Error("transport must not be hit on cache hit")
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
	_, _, c, off, length := carFixture(t)
	ref := domain.Block{CID: c.Bytes(), CarID: 7, Offset: off, Length: length}
	car := domain.Car{ID: 7, ChannelID: 1, MessageID: msgID(42), Status: domain.CarPublished}
	bots := []domain.Bot{{ID: 1, Token: "t1", Active: true}}

	transport := &fakeTransport{download: func(string, int64, int64, string) (io.ReadCloser, string, error) {
		return nil, "", domain.ErrMessageDeleted
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
	_, _, c, off, length := carFixture(t)
	ref := domain.Block{CID: c.Bytes(), CarID: 7, Offset: off, Length: length}
	car := domain.Car{ID: 7, ChannelID: 1, MessageID: msgID(42), Status: domain.CarPublished}
	bots := []domain.Bot{{ID: 1, Token: "t1", Active: true}}

	transport := &fakeTransport{download: func(string, int64, int64, string) (io.ReadCloser, string, error) {
		return nil, "", domain.ErrNoAccess
	}}
	f := newFixture(t, ref, c, car, bots, transport)

	_, err := f.bs.Get(context.Background(), c)
	if !ipld.IsNotFound(err) {
		t.Fatalf("want ipld.ErrNotFound, got %v", err)
	}
	if len(f.cars.deleted) != 0 {
		t.Fatalf("car must NOT be deleted on no-access, got deletions %v", f.cars.deleted)
	}
	if len(f.cars.statuses) != 1 || f.cars.statuses[0] != domain.CarNoBotAccess {
		t.Fatalf("want status no_bot_access, got %v", f.cars.statuses)
	}
}

func TestGetFloodWaitFailsOver(t *testing.T) {
	carBytes, block, c, off, length := carFixture(t)
	ref := domain.Block{CID: c.Bytes(), CarID: 7, Offset: off, Length: length}
	// Status no_bot_access: a successful download must lazily recover it.
	car := domain.Car{ID: 7, ChannelID: 1, MessageID: msgID(42), Status: domain.CarNoBotAccess}
	bots := []domain.Bot{
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
			return nil, "", &domain.FloodWaitError{RetryAfter: time.Minute}
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
	if len(f.cars.statuses) != 1 || f.cars.statuses[0] != domain.CarPublished {
		t.Fatalf("want lazy recovery to published, got %v", f.cars.statuses)
	}
	// Second Get must be served from cache without touching the transport.
	if _, err := f.bs.Get(context.Background(), c); err != nil {
		t.Fatalf("cached Get: %v", err)
	}
	mu.Lock()
	n := len(tokens)
	mu.Unlock()
	if n != 2 {
		t.Fatalf("transport hit on cached Get: %v", tokens)
	}
}

func TestGetRetriesAfterEviction(t *testing.T) {
	carBytes, block, c, off, length := carFixture(t)
	ref := domain.Block{CID: c.Bytes(), CarID: 7, Offset: off, Length: length}
	car := domain.Car{ID: 7, ChannelID: 1, MessageID: msgID(42), Status: domain.CarPublished}
	bots := []domain.Bot{{ID: 1, Token: "t1", Active: true}}

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

	// Fake an eviction race: the cache still indexes the entry, but the file is
	// gone by the time Get tries to read it.
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
	mu.Lock()
	n := downloads
	mu.Unlock()
	if n != 1 {
		t.Fatalf("want exactly one recovery download, got %d", n)
	}
}

func TestGetCallerCancelDoesNotAbortDownload(t *testing.T) {
	carBytes, block, c, off, length := carFixture(t)
	ref := domain.Block{CID: c.Bytes(), CarID: 7, Offset: off, Length: length}
	car := domain.Car{ID: 7, ChannelID: 1, MessageID: msgID(42), Status: domain.CarPublished}
	bots := []domain.Bot{{ID: 1, Token: "t1", Active: true}}

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
	// completes, the car is cached and a fresh Get is served without a second
	// transport hit.
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
	_, block, c, off, length := carFixture(t)
	ref := domain.Block{CID: c.Bytes(), CarID: 7, Offset: off, Length: length}
	car := domain.Car{ID: 7, ChannelID: 1, MessageID: msgID(42), Status: domain.CarPublished}
	f := newFixture(t, ref, c, car, nil, &fakeTransport{})

	other, err := cid.V1Builder{Codec: cid.Raw, MhType: mh.SHA2_256}.Sum([]byte("missing"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.bs.Get(context.Background(), other); !ipld.IsNotFound(err) {
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
	_, _, c, off, length := carFixture(t)
	ref := domain.Block{CID: c.Bytes(), CarID: 7, Offset: off, Length: length}
	f := newFixture(t, ref, c, domain.Car{ID: 7, ChannelID: 1}, nil, &fakeTransport{})

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

func TestAllKeysChan(t *testing.T) {
	_, _, c, off, length := carFixture(t)
	ref := domain.Block{CID: c.Bytes(), CarID: 7, Offset: off, Length: length}
	f := newFixture(t, ref, c, domain.Car{ID: 7, ChannelID: 1}, nil, &fakeTransport{})

	ch, err := f.bs.AllKeysChan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var got []cid.Cid
	for k := range ch {
		got = append(got, k)
	}
	if len(got) != 1 || !got[0].Equals(c) {
		t.Fatalf("AllKeysChan = %v; want [%v]", got, c)
	}

	// ProvideKeys mirrors AllKeysChan.
	pch, err := f.bs.ProvideKeys(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var pgot []cid.Cid
	for k := range pch {
		pgot = append(pgot, k)
	}
	if len(pgot) != 1 || !pgot[0].Equals(c) {
		t.Fatalf("ProvideKeys = %v; want [%v]", pgot, c)
	}
}
