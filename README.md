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

ipfsgram is organized as a small set of focused packages, each with a single
responsibility. Interfaces are declared by the consumer (small, 1–3 methods);
provider packages export concrete types and sentinel errors.

- **block** — load a DAG into blocks from a local file (UnixFS chunking) or the
  IPFS network (a temporary lite node).
- **car** — CARv1 packing (size-capped, rotating) and `ReadBlockAt` reading.
- **cache** — the daemon's disk cache for downloaded CARs (`lru` and `ttl`
  strategies).
- **store** — the data-access layer: GORM models for the PostgreSQL schema and a
  single `Store` type carrying every query. All database access goes through it.
- **telegram** — the Telegram clients: Bot API (official or self-hosted),
  MTProto (no 20 MB download cap), and the concrete `Client` that routes
  downloads/checks through MTProto when enabled. Owns the classified sentinels.
- **selector** — the bot/channel selection strategies (least-loaded healthy
  bot, fill-first channel) and the in-memory load counter.
- **probe** — check whether a published CAR's Telegram message is still alive,
  with flood-wait bookkeeping and bot failover. Shared by gc and doctor.
- **publish** — the `add` workflow: load a DAG, dedup against stored blocks,
  pack into CARs, upload, record the pin.
- **gc** — garbage-collect unpinned CARs (`gc`): a lock-free candidate preview
  and the deletion pass under the exclusive advisory lock.
- **doctor** — the `doctor` diagnostics: orphaned pending CARs, membership
  revalidation, and recovery of CARs marked unavailable (via probe).
- **remove** — the explicit removal workflows: `rm` (unpin) and the
  plan/execute split behind `bot remove` / `channel remove`.
- **node** — the libp2p stack: host with a persisted identity, Bitswap, DHT and
  the reprovider. No project-internal dependencies.
- **daemon** — the read-only blockstore serving blocks out of Telegram through
  the disk cache, plus the run loop assembling the cache, blockstore and node.
- **db** — connect, migrate and the schema-version check (embedded dbmate
  migrations).
- **cmd/ipfsgram** — the cobra commands; thin wiring over the packages above.

Who imports whom (acyclic; leaves first):

```
block, car, cache, telegram, db, selector     leaves (selector → store)
store        → db
probe        → store, telegram, selector
publish      → store, telegram, selector, car, block
gc           → store, telegram
doctor       → store, telegram, selector, probe
remove       → store
node         → (no internal deps)
daemon       → store, telegram, selector, car, cache, node
cmd/ipfsgram → everything above
```

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
