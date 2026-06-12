package publish

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/rs/zerolog"

	adapterselector "github.com/ulbwa/ipfsgram/internal/adapter/selector"
	"github.com/ulbwa/ipfsgram/internal/domain"
	"github.com/ulbwa/ipfsgram/internal/port"
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

type passThroughLocker struct{ exclusive bool }

func (l *passThroughLocker) WithLock(ctx context.Context, exclusive bool, fn func(ctx context.Context) error) error {
	l.exclusive = exclusive
	return fn(ctx)
}

type fakeConfig struct{ port.ConfigRepository }

func (fakeConfig) GetInt64(context.Context, string) (int64, error) {
	return 0, domain.ErrNotFound
}
func (fakeConfig) GetFloat64(context.Context, string) (float64, error) {
	return 0, domain.ErrNotFound
}

type fakeBlocks struct {
	port.BlockRepository
	inserted []domain.Block
}

func (b *fakeBlocks) Existing(context.Context, [][]byte) (map[string]domain.Block, error) {
	return map[string]domain.Block{}, nil
}
func (b *fakeBlocks) InsertBatch(_ context.Context, blocks []domain.Block) error {
	b.inserted = append(b.inserted, blocks...)
	return nil
}
func (b *fakeBlocks) Repoint(context.Context, []domain.Block) error { return nil }

type fakeCars struct {
	port.CarRepository
	pending   int
	published int
}

func (c *fakeCars) CreatePending(context.Context, int64, int64, int) (int64, error) {
	c.pending++
	return int64(c.pending), nil
}
func (c *fakeCars) MarkPublished(context.Context, int64, int64) error {
	c.published++
	return nil
}
func (c *fakeCars) UpsertFileID(context.Context, int64, int64, string) error { return nil }

type fakeChannels struct {
	port.ChannelRepository
	channels []domain.Channel
	members  []domain.BotChannel
}

func (c *fakeChannels) List(context.Context) ([]domain.Channel, error) { return c.channels, nil }
func (c *fakeChannels) MembersOf(context.Context, int64) ([]domain.BotChannel, error) {
	return c.members, nil
}
func (c *fakeChannels) IncrementMessageCount(context.Context, int64) error { return nil }

type fakeBots struct {
	port.BotRepository
	bots []domain.Bot
}

func (b *fakeBots) List(context.Context) ([]domain.Bot, error) { return b.bots, nil }

type fakePins struct {
	port.PinRepository
	created int
	name    string
}

func (p *fakePins) Create(_ context.Context, _ []byte, name string, _ int64, _ [][]byte) error {
	p.created++
	p.name = name
	return nil
}

type fakeTransport struct {
	port.Transport
	uploads int
}

func (t *fakeTransport) Upload(_ context.Context, _ string, _ int64, _ string, _ int64, r io.Reader) (port.UploadResult, error) {
	t.uploads++
	io.Copy(io.Discard, r)
	return port.UploadResult{MessageID: 100, FileID: "fid"}, nil
}

// realSelector returns the production selector adapter, which is a pure
// in-memory implementation of port.Selector (no I/O), suitable for tests.
func realSelector() port.Selector { return adapterselector.New() }

// fakeBlockSource returns a fixed root and one block.
type fakeBlockSource struct {
	root  cid.Cid
	block domain.RawBlock
}

func (s fakeBlockSource) Load(context.Context) (cid.Cid, []domain.RawBlock, error) {
	return s.root, []domain.RawBlock{s.block}, nil
}

// fakePackerFactory writes a single real CAR-ish file and reports one block.
type fakePackerFactory struct{ dir string }

func (f *fakePackerFactory) New(dir string, _ int64, _ cid.Cid) (port.Packer, error) {
	return &fakePacker{dir: dir}, nil
}

type fakePacker struct {
	dir    string
	blocks []domain.PackedBlock
}

func (p *fakePacker) Add(c cid.Cid, data []byte) error {
	p.blocks = append(p.blocks, domain.PackedBlock{CID: c, Offset: 0, Length: int32(len(data))})
	return nil
}
func (p *fakePacker) Finish() ([]domain.PackedCar, error) {
	f, err := os.CreateTemp(p.dir, "car-*.car")
	if err != nil {
		return nil, err
	}
	f.WriteString("car")
	f.Close()
	return []domain.PackedCar{{Path: f.Name(), Size: 3, Blocks: p.blocks}}, nil
}

func TestPublishHappyPath(t *testing.T) {
	root := testCID(t, "root")
	blkCID := testCID(t, "block")

	blocks := &fakeBlocks{}
	cars := &fakeCars{}
	pins := &fakePins{}
	tr := &fakeTransport{}
	locker := &passThroughLocker{}

	s := &Service{
		Config: fakeConfig{},
		Blocks: blocks,
		Cars:   cars,
		Channels: &fakeChannels{
			channels: []domain.Channel{{ID: 1, TgID: -100, Title: "ch", MessageLimit: 1000, Active: true}},
			members:  []domain.BotChannel{{BotID: 1, ChannelID: 1, Member: true, CanPost: true}},
		},
		Bots:      &fakeBots{bots: []domain.Bot{{ID: 1, Active: true, Token: "t", Username: "b"}}},
		Pins:      pins,
		Transport: tr,
		Selector:  realSelector(),
		Packer:    &fakePackerFactory{},
		Locker:    locker,
		Logger:    zerolog.New(io.Discard),
	}

	got, err := s.Publish(context.Background(), fakeBlockSource{
		root:  root,
		block: domain.RawBlock{CID: blkCID, Data: []byte("hello")},
	}, "myname")
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got != root {
		t.Errorf("root = %s, want %s", got, root)
	}
	if locker.exclusive {
		t.Errorf("Publish must take the lock in shared mode")
	}
	if tr.uploads != 1 {
		t.Errorf("uploads = %d, want 1", tr.uploads)
	}
	if cars.published != 1 {
		t.Errorf("cars published = %d, want 1", cars.published)
	}
	if len(blocks.inserted) != 1 {
		t.Errorf("inserted blocks = %d, want 1", len(blocks.inserted))
	}
	if pins.created != 1 || pins.name != "myname" {
		t.Errorf("pin created = %d name = %q, want 1 / myname", pins.created, pins.name)
	}
}
