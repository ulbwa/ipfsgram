# ipfsgram

An IPFS blockstore on top of Telegram channels.

`ipfsgram` imports content into IPFS blocks, packs them into CAR archives, and
publishes those archives as documents in Telegram channels. A PostgreSQL
database indexes every block (which CAR it lives in, at which offset), so any
block can be fetched back on demand. The included daemon runs a full IPFS node
(libp2p, Bitswap, DHT) that serves your pinned content to the rest of the IPFS
network straight out of Telegram, with a local disk cache in between.

## Requirements

- Go 1.26+
- PostgreSQL

## Quickstart

```sh
go build ./cmd/ipfsgram

export IPFSGRAM_DSN='postgres://user:pass@localhost:5432/ipfsgram?sslmode=disable'

# Apply the embedded schema migrations.
./ipfsgram db migrate

# Register a Telegram bot by its token.
./ipfsgram bot add <token>

# Register a channel by its Telegram ID.
# Add the bot to the channel as an admin (post/delete rights) first.
./ipfsgram channel add <tg_id>

# Publish a file. Prints the root CID on success.
./ipfsgram add ./photo.jpg

# Serve everything as an IPFS node.
./ipfsgram daemon
```

`ipfsgram add` also accepts `--cid <cid>` to fetch a DAG from the public IPFS
network and republish it through Telegram, and `--name` to label the pin.

## Daemon

`ipfsgram daemon` runs the IPFS node. Every setting resolves as
flag → environment variable → default:

| Flag | Environment | Default | Description |
| --- | --- | --- | --- |
| `--data-dir` | `IPFSGRAM_DATA_DIR` | `~/.ipfsgram` | State directory: identity key, sessions, cache |
| `--cache-dir` | `IPFSGRAM_CACHE_DIR` | `<data-dir>/cache` | CAR disk cache directory |
| `--cache-max-bytes` | `IPFSGRAM_CACHE_MAX_BYTES` | `1073741824` (1 GiB) | Cache size limit for the `lru` strategy |
| `--cache-strategy` | `IPFSGRAM_CACHE_STRATEGY` | `lru` | Cache eviction strategy: `lru` or `ttl` |
| `--cache-ttl` | `IPFSGRAM_CACHE_TTL` | `1h` | Entry lifetime for the `ttl` strategy |
| `--listen` | `IPFSGRAM_LISTEN` (comma-separated) | `/ip4/0.0.0.0/tcp/4001`, `/ip4/0.0.0.0/udp/4001/quic-v1` | libp2p listen multiaddr (repeatable) |
| `--dsn` | `IPFSGRAM_DSN` | — | PostgreSQL DSN (shared by all commands) |

## Other commands

- `ipfsgram rm <root-cid>` — unpin content; blocks are freed by the next `gc`.
- `ipfsgram gc` — delete CARs that no pin references (asks for confirmation, `--yes` to skip).
- `ipfsgram status` — system overview: channels, bots, counters.
- `ipfsgram doctor` — diagnostics: orphaned pending CARs, membership revalidation, CAR recovery.
- `ipfsgram config get|set <key> [value]` — read or write global configuration stored in the database.
- `ipfsgram mtproto enable|disable|status` — manage the MTProto transport.
- `ipfsgram bot list|remove`, `ipfsgram channel list|remove` — manage bots and channels.
- `ipfsgram db migrate` — apply the embedded schema migrations.

## Configuration keys

Set with `ipfsgram config set <key> <value>`:

| Key | Default | Description |
| --- | --- | --- |
| `car_max_size` | 15 MiB (`15728640`) | Maximum size of one CAR archive. Must stay within your transport's download limit: 20 MB with the Bot API, ~2 GB with MTProto. |
| `bot_api_url` | official Bot API | Base URL of a self-hosted Bot API server. |
| `channel_warn_threshold` | `0.9` | Channel fill ratio (0..1) at which a near-capacity warning is logged. |

## MTProto

By default, files are downloaded through the Telegram Bot API, which caps
document downloads at 20 MB. Enabling MTProto lifts that cap (documents up to
~2 GB), which in turn allows a larger `car_max_size`. You need an `api_id` and
`api_hash` from [my.telegram.org](https://my.telegram.org):

```sh
ipfsgram mtproto enable
ipfsgram mtproto status
```

## Architecture

ipfsgram follows a hexagonal (ports-and-adapters) layout:

- **domain** — plain models and errors, no I/O.
- **port** — the interfaces the rest of the code depends on.
- **adapter** — concrete implementations of those ports: `gormrepo`
  (PostgreSQL), `telegram` (Bot API + MTProto transport), `diskcache` (CAR
  cache), `carpack` (CARv1 packing/reading), `selector` (bot/channel routing),
  `libp2pnode` (the IPFS node), `blocksource` (file and network DAG sources).
- **service** — use cases: `blockstore` (serve blocks to libp2p), `publish`
  (import and upload), `maintenance` (gc, doctor, unpin).
- **cmd** — the cobra wiring; commands talk to ports only.

Persistence is GORM on top of a schema managed by
[dbmate](https://github.com/amacneil/dbmate). The migrations are embedded in the
binary (`db/migrations`) and applied by `ipfsgram db migrate`. The binary pins
the schema version it was built against and refuses to run against a database at
a different version, so an out-of-date deployment fails fast instead of
corrupting data.

## Data safety

Nothing is deleted automatically. The only data that disappears on its own is
a Telegram message someone physically deleted out from under ipfsgram — in
that case the index records are dropped and the blocks are re-uploaded on the
next `add`. All other deletions happen exclusively through explicit CLI
commands (`rm`, `gc`, `bot remove`, `channel remove`), and destructive ones
ask for confirmation first.
