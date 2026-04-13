# turbolite

turbolite is a SQLite VFS in Rust that serves point lookups and joins directly from S3 with sub-250ms cold latency. 

It also offers page-level compression (zstd) and encryption (AES-256) for efficiency and security at rest, which can be used separately from S3.

> turbolite is **experimental**. It is new and contains bugs. It may corrupt your data. Please be careful.

Object storage is getting fast. [S3 Express One Zone](https://aws.amazon.com/s3/storage-classes/express-one-zone/) delivers single-digit millisecond GETs and [Tigris is also extremely fast](https://www.tigrisdata.com/blog/benchmark-small-objects/). The gap between local disk and cloud storage is shrinking, and turbolite exploits that.

The design and name are inspired by [turbopuffer](https://turbopuffer.com/blog/turbopuffer)'s approach of ruthlessly architecting around cloud storage constraints. The project's initial goal was to beat [Neon's 500ms+ cold starts](https://neon.com/blog/cold-starts-just-got-hot). Goal achieved.

If you have one database per server, use a volume. turbolite explores how to have hundreds or thousands of databases (one per tenant, one per workspace, one per device), don't want a volume for each one, and you're okay with a single write source.

turbolite ships as a Rust library, a [SQLite loadable extension](#loadable-extension) (`.so`/`.dylib`), and language packages for [Python](#python) and [Node.js](#nodejs), plus Github deps for Go. Any S3-compatible storage works (AWS S3, Tigris, R2, MinIO, etc.). It's a standard SQLite VFS operating at the page level, so most SQLite features should work: FTS, R-tree, JSON, WAL mode, etc.

If you want to contribute to turbolite or find bugs, please create a pull request or open an issue.

## Performance

| Query | Type | Cold (S3 Express) | Cold (Tigris) |
|-------|------|-------------------|---------------|
| Post + user | point lookup + join | 77ms | 192ms |
| Profile | multi-table join (5 JOINs) | 190ms | 524ms |
| Who-liked | index search + join | 129ms | 340ms |
| Mutual friends | multi-search join | 82ms | 183ms |
| Indexed filter | covered index scan | 74ms | 173ms |
| Full scan + filter | full table scan | 586ms | 984ms |

1M rows, 1.5GB at with nothing cached, every byte from S3. EC2 c5.2xlarge + S3 Express One Zone (same AZ, ~4ms GET latency). Fly performance-8x + Tigris (~25ms GET latency). Both: 8 dedicated vCPU, 16GB RAM, 8 prefetch threads. See [Benchmarking](#benchmarking) and [Storage backend matters](#storage-backend-matters).

Benchmarks are organized by **cache level** (what's already on local disk when the query runs):

| Cache level | What's cached | What's fetched from S3 | When this happens |
|-------------|--------------|----------------------|-------------------|
| **none** | nothing | everything | Fresh start, empty cache |
| **interior** | interior B-tree pages | index + data pages | First query after connection open |
| **index** | interior + index pages | data pages only | Normal turbolite operation |
| **data** | everything | nothing | Equivalent to local SQLite |

**interior** is the most realistic cold benchmark: interior pages load eagerly on connection open, so by the time you run your first query, they're cached. Index pages aggressively prefetch on first access in the background and may not be ready yet.

### Warm cache (VFS overhead vs plain SQLite)

100K rows, Fly.io performance-2x (dedicated vCPU, NVMe, IAD):

| Operation | SQLite | turbolite | Overhead |
|-----------|--------|-----------|----------|
| Point lookup | 145K/s | 73K/s | 2.0x |
| Range scan | 8.8K/s | 8.3K/s | **parity** |
| Full table scan | 56/s | 60/s | **parity** |
| INSERT | 19K/s | 23K/s | **faster** |
| UPDATE by PK | 40K/s | 27K/s | 1.5x |
| Batch INSERT (in txn) | 685K/s | 740K/s | **faster** |

Point lookups have the highest per-page overhead (~2x). Everything else approaches or beats parity. Lock-free cache architecture means concurrent reads never block writes.

### Checkpoint cost

| After | Local | S3 (same-region RustFS) |
|-------|-------|-------------------------|
| 1K inserts | 19ms | 38ms |
| 10K batch | 17ms | 114ms |
| 1K updates | 9ms | 36ms |

Writes are always local-speed. The S3 cost is at checkpoint only. Numbers with RustFS in same Fly region (~2ms RTT). S3 Express One Zone would be comparable.

## Quick Start

### Python

```bash
pip install turbolite
```

```python
import turbolite

conn = turbolite.connect("my.db", mode="s3",
    bucket="my-bucket",
    endpoint="https://t3.storage.dev")

conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
conn.execute("INSERT INTO users VALUES (1, 'alice', 'alice@example.com')")
conn.commit()

alice = conn.cursor().execute("SELECT * FROM users").fetchone()
print(alice[1])
>>> "alice"
```

See [Installation](#installation) for Node, Go, Rust, local-only mode, and using the `.so` loadable extension directly


## Design

turbolite is designed for S3's constraints over filesystem constraints. Every decision flows from this model:

| S3 Constraint | Implication |
|---------------|-------------|
| **Round trips are slow** | Minimize request count. Batch writes, prefetch reads aggressively. |
| **Bandwidth is a bottleneck** | Maximize bandwidth utilization. |
| **PUTs and GETs charge per-operation** | A 64KB GET costs the same as a 16MB GET. Optimize request count, not byte efficiency. |
| **Objects are immutable** | Never update in place. Write new versions, swap a pointer. No partial-write corruption. |
| **Storage is cheap** | Don't optimize for space. Over-provision, keep old versions, let GC clean up later. |

### Architecture

turbolite adds introspection and indirection layers between SQLite and S3 that efficiently groups, compresses, tracks, and fetches pages. 

SQLite uses a B-tree index and requests for one page at a time. It knows page N is at byte offset `N * page_size`. And those pages are distributed randomly throughout the pagemap for efficient random access. But on S3, fetching one page per request would mean thousands of potentially random GETs per query. 

But pages are not created equally. SQLite has different types of pages. turbolite **separates page groups by type**: interior B-tree, index leaf, and data leaf pages.

Interior pages are touched on every query to route lookups to leaf pages. turbolite detects them, stores them in compressed bundles in S3, and loads them eagerly on VFS open. After that, every B-tree traversal is a cache hit. 

Index leaf pages get the same treatment: separate bundles, lazy background prefetch, pinned against eviction. Cold queries only need to fetch data pages. 

turbolite takes advantage of **B-tree introspection** to understand *which tree (a table or index) a page is part of*, and intelligently stores those pages together in S3 as **page groups**: many pages chunked into a single S3 object. Big enough to saturate bandwidth on prefetch, small enough for point queries. Default: 256 pages per group, ~16MB at 64KB pages.

Storing the same table/index together means that we make the fewest possible GETs for cold queries. 

turbolite **indirects page lookups with a manifest file** that is the source of truth for where every page lives. It replaces SQLite's implicit `offset = page * size` with explicit pointers. Old page group versions are never overwritten; the manifest PUT is the atomic commit point. Old versions become garbage, cleaned up by `gc()`.

SQLite defaults to 4KB pages to match filesystem disk page size. On S3, disk page size is irrelevant. What matters is minimizing request count and maximizing B-tree fan-out. The answer is **large pages**: turbolite defaults to 64KB pages. Fewer pages = fewer S3 round trips to reach a leaf.

To make point queries fast, turbolite uses **seekable compression**: each page group is encoded as multiple **zstd frames** (~4 pages per frame). The manifest stores byte offsets per frame, so a cache miss fetches just the ~256KB sub-chunk with the needed page via S3 range GET, not the entire group.

Prefetching has two layers: **proactive** (query-plan frontrunning) and **reactive** (adaptive miss-based).

**Query-plan frontrunning** runs first. Before a query executes, turbolite intercepts the SQLite query plan via `EXPLAIN QUERY PLAN`, extracts the exact tables and indexes the query will touch, and submits all their page groups to the prefetch pool before the first page is even read. A five-table join that would otherwise trigger five sequential miss-then-fetch cycles instead fires all five fetches in parallel at query start. For `SCAN` queries, this means the entire table is prefetched upfront.

> Caveat: SQLite supports one trace callback per connection. If another extension claims the slot first, frontrunning silently falls back to reactive prefetch.

**Reactive prefetching** handles what frontrunning misses and acts as fallback. On a cache miss, two things happen concurrently:
1. **Inline range GET**: fetch the specific sub-chunk containing the needed page, return to SQLite immediately.
2. **Background prefetch**: submit sibling groups *for that tree* to the prefetch pool according to a schedule.

Miss counters are tracked **per B-tree, not globally**. A profile query that hits `users` (miss 1) then `posts` (miss 1) correctly tracks each tree at 1, not 2. This prevents a multi-table join from accidentally escalating prefetch on every tree just because it touches several.

Each consecutive miss advances through a **prefetch schedule** that controls what fraction of same-tree groups to prefetch. turbolite selects a schedule automatically based on the query plan:
- **Search schedule** `[0.3, 0.3, 0.4]`: for `SEARCH ... USING INDEX` queries that scan unknown portions of indexes. Aggressive from the first miss because we don't know how much of the index will be scanned.
- **Lookup schedule** `[0.0, 0.0, 0.0]`: for point queries and index lookups that hit 1-2 pages per tree. Three free hops before any prefetch. Zero-heavy schedules outperform early-ramp on both S3 Express and Tigris.

You can tune the prefetch schedule for *each query* via `SELECT turbolite_config_set(...)` - you know the query's storage needs, so the VFS doesn't have to guess. See [Runtime tuning](#runtime-tuning).

Both schedules take advantage of B-tree introspection: every prefetched group is guaranteed to contain pages from the right tree. An example: if SQLite requests a page from the `users` table, then requests another from the same table, turbolite assumes a scan is coming and prefetches the rest of the `users` table in the background, and nothing else. Without B-tree introspection, it would accidentally fetch half the users table and half the posts table just because the data lives next to each other on disk.

### Encryption & Compression

#### Compression

All data is zstd-compressed before storage. Page groups use seekable multi-frame encoding that independently compresses each frame (~4 pages, ~256KB), so a point lookup decompresses only the relevant frame rather than the entire page group. Custom zstd dictionaries can improve compression ratios further.

Local (non-S3) mode also compresses at the page level with zstd. See the CLI for dictionary training tools.

#### Encryption

If encryption is enabled, turbolite encrypts everything: S3 objects, local cache, WAL, metadata. S3 data uses AES-256-GCM with random nonces per frame (authenticated, tamper-detecting). Local data uses AES-256-CTR with zero size overhead. Encryption happens after compression: `plaintext → zstd → encrypt → S3`.

**Key rotation:** `rotate_encryption_key(config, new_key)` re-encrypts, adds, or removes encryption on all S3 data without decompressing. `Some` to `Some` rotates keys, `Some` to `None` removes encryption, `None` to `Some` adds it. Crash-safe: old objects are never overwritten, the manifest upload is the atomic commit point, and a verification step confirms new data is readable before committing. Orphans from partial runs are cleaned by `gc()`.

## Strengths and Limitations

### Where turbolite is fast

**Point lookups are the sweet spot.** At cache level `index`, a point lookup fetches 1-2 sub-chunks via S3 range GET (~100KB each). Interior and index pages are already cached. At cache level `none`, add ~120ms for interior re-fetch + first data page. This works on any machine size.

**Scans with enough cores.** The prefetch pool saturates S3 bandwidth with per-tree adaptive scheduling. Search queries ramp prefetch aggressively from the first miss; plan-aware SCAN queries bulk-prefetch the entire table upfront. Sufficient threads can sync multi-GB databases in seconds with 2-3 prefetch batches.

### Where turbolite is slow

**Scans on small machines.** With 1 prefetch thread, a scan over 1.46GB takes seconds, not milliseconds. The bottleneck is S3 round trips: each hop fetches groups serially. If your first query is a full scan on a 1-vCPU machine, expect startup to be painful.

**Bad thread tuning.** Too few prefetch threads and scans stall waiting for S3. Too many and you waste memory on idle threads. The default (`num_cpus + 1`) is reasonable, but scan-heavy workloads on large databases need more CPUs.

**First query penalty.** The first query at cache level `none` pays ~50-200ms for interior page loading plus at least one data fetch. If the query needs an index page before background prefetch finishes, it falls back to an inline range GET.

### Current limitations

- **Single writer only.** Two machines writing to the same prefix will corrupt the manifest.
- **WAL shipping is experimental.** Requires the `wal` feature flag + walrust. See [Durability](#durability).

SQLite features that **do** work: FTS, R-tree, JSON, WAL mode, DELETE journal mode, VACUUM, autovacuum.

## Tuning

### General parameters

| Parameter | What it controls | Default |
|-----------|-----------------|---------|
| `prefetch_threads` | Worker threads for parallel S3 fetches | num_cpus + 1 |
| `pages_per_group` | Pages per S3 object, larger = fewer PUTs, more bytes per fetch | 256 |
| `gc_enabled` | Delete old page group versions after checkpoint | true |
| `sync_mode` | Checkpoint durability: `Durable` (S3 upload in checkpoint) or `LocalThenFlush` (defer upload) | Durable |

### Prefetch schedules

Query-plan frontrunning (see Architecture) is the primary prefetch mechanism. The reactive schedules below act as fallback when frontrunning isn't available or when queries access pages that weren't in the plan.

| Strategy | When | Default schedule | What happens |
|----------|------|-----------------|--------------|
| **SCAN** (frontrun) | EQP says `SCAN table` | All groups upfront | Bulk prefetch of entire table before first read. No hop schedule needed. |
| **SEARCH** (reactive) | EQP says `SEARCH ... USING INDEX` | `[0.3, 0.3, 0.4]` | Aggressive prefetch from first miss; scans unknown index portions. |
| **Lookup** (reactive) | Point queries, no EQP info | `[0.0, 0.0, 0.0]` | Three free hops, zero prefetch. Point queries rarely benefit from prefetch. |

Each element is the fraction of sibling groups to prefetch on the Nth consecutive per-tree cache miss. When misses exceed the array length, fraction=1.0 (all remaining).

**Why two reactive schedules?** SEARCH queries scan unknown portions of indexes/tables and need aggressive warmup. Lookups hit 1-2 pages per tree and barely need prefetch. Per-tree miss counters ensure independent tracking: a profile query hitting users (miss 1) then posts (miss 1) tracks each tree separately.

### Runtime tuning

Schedules can be changed per-connection without reopening via the `turbolite_config_set` SQL function:

```sql
SELECT turbolite_config_set('prefetch_search', '0.4,0.3,0.3');
SELECT turbolite_config_set('prefetch_lookup', '0,0,0.2');
SELECT turbolite_config_set('prefetch', '0.5,0.5');     -- sets both
SELECT turbolite_config_set('prefetch_reset', '');        -- reset to defaults
SELECT turbolite_config_set('plan_aware', 'false');
```

Changes take effect on the next query. Zero overhead when not used.

### Recommended configs

| Workload | Config | Why |
|----------|--------|-----|
| Mixed OLTP | Defaults | Plan-aware handles scans, search schedule warms indexes, lookup schedule stays conservative. |
| Point-heavy (agent DBs) | `prefetch_lookup=0,0,0` | Lookups almost never need prefetch. |
| Scan-heavy analytics | `prefetch_search=0.5,0.5`, `plan_aware=true` | Aggressive search warmup plus plan-aware bulk prefetch. |
| Conservative (bursty serverless) | `prefetch_search=0.1,0.2,0.3`, `prefetch_lookup=0,0,0.1` | Minimal prefetch noise. |

**Note**: prefetch is per-connection. Each new connection starts with cold per-tree miss counters. The cache is shared, so a second connection benefits from pages cached by the first.

### Storage backend matters

Optimal prefetch schedules depend on your S3 backend's latency-bandwidth tradeoff. We tested 10 schedule pairs across 6 queries on both S3 Express (~4ms GET) and Tigris (~25ms GET):

| Backend | GET latency | Best point lookup | Best profile | Tuning gain |
|---------|-------------|-------------------|-------------|-------------|
| **S3 Express** | ~4ms | 74ms (off/off: 96ms) | 188ms (off/off: 212ms) | 5-23% over no prefetch |
| **Tigris** | ~25ms | 192ms (off/off: 231ms) | 524ms (off/off: 616ms) | 8-34% over no prefetch |

On S3 Express, `off/off` (no prefetch at all) is surprisingly competitive for point queries because each sub-chunk range GET is only ~4ms. The gap between "no prefetch" and "optimal prefetch" is small (23% for point lookups) because individual GETs are cheap. On Tigris, the same query benefits much more from prefetch (up to 39% on idx-filter) because each wasted round trip costs 25ms.

The practical effect: on high-latency backends, push search schedules harder and keep lookup schedules with more leading zeros. On S3 Express, the defaults work well and tuning provides smaller gains. Full scan performance is schedule-insensitive on both backends because query-plan frontrunning bulk-prefetches the entire table upfront.

Use `tiered-tune` (see below) to find optimal schedules for your specific backend and queries.

### Tuning tool

`tiered-tune` connects to an existing turbolite database and sweeps prefetch schedules against your actual queries. Instead of guessing schedules, run your real workload and let the tool find the best pair:

```bash
# Connect to existing database, test your queries
cargo run --release --features cloud,zstd --bin tiered-tune -- \
  --prefix "databases/tenant-123" \
  --query "SELECT * FROM users WHERE id = ?1" \
  --query "SELECT p.*, u.name FROM posts p JOIN users u ON p.user_id = u.id WHERE p.id = ?1" \
  --iterations 10

# Custom schedule grid
cargo run --release --features cloud,zstd --bin tiered-tune -- \
  --prefix "databases/tenant-123" \
  --query "SELECT * FROM orders WHERE user_id = ?1 ORDER BY created_at DESC LIMIT 20" \
  --search-schedules "0.3,0.3,0.4;0.5,0.5;1.0" \
  --lookup-schedules "0;0,0,0.1;0,0,0,0.1,0.2" \
  --iterations 10
```

Output is a per-query comparison table (like `tiered-bench --matrix`) showing p50, p90, GET count, and bytes for each schedule pair. The tool recommends a schedule and prints the `turbolite_config_set` SQL to apply it.

## Durability

turbolite is a storage layer, not a replication system. Durability depends on when data reaches S3.

**After checkpoint**: page groups + manifest are in S3. S3 provides 11 nines of durability. This data survives machine loss.

**Between checkpoints**: writes live only in the local WAL on local disk. If the machine dies before the next checkpoint, those writes are gone.

Checkpoint frequency controls the tradeoff: more frequent checkpoints = smaller data-at-risk window but more S3 PUTs. The default is SQLite's auto-checkpoint (every 1000 WAL frames).

### Checkpoint modes

turbolite supports two checkpoint modes via `sync_mode` in `TurboliteConfig`:

**`SyncMode::Durable`** (default). The checkpoint uploads page groups to S3 while holding SQLite's EXCLUSIVE lock. Simple, fully durable on every checkpoint. No writes or reads can proceed until the upload completes. Good for most workloads.

**`SyncMode::LocalThenFlush`**. The checkpoint writes to local disk cache only (~1ms lock hold), then releases the lock. The caller uploads to S3 separately via `flush_to_s3()`, during which reads and writes continue normally. This is useful for write-heavy workloads where blocking readers for the duration of an S3 upload is unacceptable.

Between checkpoint and flush, data exists only in the local disk cache. A process crash is fine (data is on local disk, and staging logs capture the exact page contents for upload). Machine loss before flush means those writes are gone. Cache eviction is safe: turbolite protects pending pages from eviction automatically.

**Crash recovery**: If the process crashes between checkpoint and flush, staging logs survive on disk. On the next `TurboliteVfs::new()`, they are automatically recovered and queued for the next `flush_to_s3()` call. Reads are served immediately from the local cache without waiting for the flush.

### WAL shipping (experimental)

With the `wal` feature flag enabled, turbolite ships WAL frames to S3 via [walrust](https://github.com/russellromney/walrust), closing the durability gap between individual writes and checkpoints.

```toml
# Cargo.toml
turbolite = { version = "0.4", features = ["cloud", "zstd", "wal"] }
```

```rust
let config = TurboliteConfig {
    wal_replication: true,  // enable WAL shipping
    ..Default::default()
};
```

turbolite and walrust stay synchronized via SQLite's file change counter, stored as `manifest.change_counter`. On cold start, turbolite materializes the database from page groups, then walrust replays WAL segments with txid > `change_counter` to recover writes that occurred after the last checkpoint.

**Durability model with WAL shipping**: every committed transaction is shipped to S3 as a WAL segment within the sync interval (default 100ms). If the machine dies, at most one sync interval of writes is lost. After checkpoint, WAL segments with txid <= `change_counter` are garbage collected automatically.

WAL shipping is complementary to SyncMode: SyncMode controls how checkpoints reach S3, WAL shipping makes individual writes durable before checkpoint.

### Consistency model

Single writer, snapshot readers. One process writes; readers see the last committed manifest when they opened. turbolite is not a distributed database and does not coordinate between multiple writers.

## Local Mode (no S3)

turbolite also works as a purely local compressed/encrypted VFS:

Compression: zstd (default), lz4, snappy, gzip. With zstd, you can train and embed custom compression dictionaries and rotate automatically for more efficient compression. Larger page sizes compress better. See CLI for training tools. 

Encryption: AES-256-GCM per page.

Page-level operation means most SQLite features still work: FTS, R-tree, JSON, WAL mode. Most other SQLite compression/encryption extensions operate at the file level or require custom builds.

## Installation

**Python**: `pip install turbolite` — see [packages/python/](packages/python/)

```python
import turbolite

# Local compressed (no S3 needed)
conn = turbolite.connect("my.db")

# S3 cloud
conn = turbolite.connect("my.db", mode="s3", bucket="my-bucket", endpoint="https://t3.storage.dev")

# Manual extension loading for full control
import sqlite3
conn = sqlite3.connect(":memory:")
turbolite.load(conn)
conn.close()
conn = sqlite3.connect("file:my.db?vfs=turbolite", uri=True)       # local
conn = sqlite3.connect("file:my.db?vfs=turbolite-s3", uri=True)    # S3 (needs TURBOLITE_BUCKET)
```

**Node.js**: `npm install turbolite` — see [packages/node/](packages/node/)

**Rust**:
```toml
[dependencies]
turbolite = "0.4"                                              # local VFS
turbolite = { version = "0.4", features = ["cloud"] }          # + S3 storage
turbolite = { version = "0.4", features = ["encryption"] }     # + encryption
```

**Go** (cgo, links the shared library):
```bash
make lib-bundled  # build libturbolite.{so,dylib}
```
```go
// #cgo LDFLAGS: -L/path/to/target/release -lturbolite
// #include <stdlib.h>
// extern int turbolite_register_local(const char* name, const char* cache_dir, int level);
// extern void* turbolite_open(const char* path, const char* vfs_name);
// extern int turbolite_exec(void* db, const char* sql);
// extern char* turbolite_query_json(void* db, const char* sql);
// extern void turbolite_close(void* db);
import "C"
```
See [examples/go/](examples/go/) for a full HTTP server example.

### Loadable extension (any language)

Build the loadable extension for any language with SQLite's `load_extension`:

```bash
# after cloning the turbolite repo
make ext  # produces target/release/turbolite.{so,dylib}
```

```c
sqlite3_enable_load_extension(db, 1);
sqlite3_load_extension(db, "path/to/turbolite", NULL, NULL);
// "turbolite" VFS (local) is always registered
// "turbolite-s3" VFS is also registered if TURBOLITE_BUCKET is set
``` 

### Node.js

```bash
npm install turbolite
```

```js
const { Database } = require("turbolite");

const db = new Database("my.db");
db.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
db.exec("INSERT INTO users VALUES (1, 'alice')");

const rows = db.query("SELECT * FROM users");
// [{ id: 1, name: 'alice' }]
db.close();
```

Note: Node uses a wrapped `Database` class (not `load_extension`) because better-sqlite3 compiles with `SQLITE_USE_URI=0`. See [packages/node/](packages/node/) for full docs.

### Rust (local)

```rust
use turbolite::tiered::{TurboliteVfs, TurboliteConfig, StorageBackend};

let config = TurboliteConfig {
    storage_backend: StorageBackend::Local,
    cache_dir: "/path/to/data".into(),
    ..Default::default()
};
let vfs = TurboliteVfs::new(config)?;
turbolite::tiered::register("turbolite", vfs)?;

let conn = rusqlite::Connection::open_with_flags_and_vfs(
    "mydb.db",
    rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
    "turbolite",
)?;
```

### Rust (S3 cloud)

```rust
use turbolite::tiered::{TurboliteVfs, TurboliteConfig, StorageBackend};

let config = TurboliteConfig {
    storage_backend: StorageBackend::S3 {
        bucket: "my-bucket".into(),
        prefix: "my-database".into(),
        endpoint_url: None,
        region: None,
    },
    cache_dir: "/tmp/cache".into(),
    ..Default::default()
};
let vfs = TurboliteVfs::new(config)?;
turbolite::tiered::register("turbolite", vfs)?;

let conn = rusqlite::Connection::open_with_flags_and_vfs(
    "mydb.db",
    rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
    "turbolite",
)?;
```

## CLI

turbolite ships a CLI for inspecting, managing, and interacting with turbolite databases without writing Rust.

```bash
cargo install turbolite --features cloud,zstd
```

### Commands

```bash
# Inspect a database manifest
turbolite info --db my.db
turbolite info --db my.db --bucket my-bucket --endpoint https://t3.storage.dev

# Interactive SQLite shell (with turbolite VFS)
turbolite shell --db my.db
turbolite shell --db my.db --bucket my-bucket --read-only

# Garbage collection (delete orphaned S3 objects)
turbolite gc --db my.db --bucket my-bucket

# Force checkpoint + S3 upload
turbolite checkpoint --db my.db --bucket my-bucket

# Warm local cache by fetching all page groups
turbolite prefetch --db my.db --bucket my-bucket --threads 8

# Export to plain SQLite (for migration or backup)
turbolite export --db my.db --output plain.db

# Import a plain SQLite file into turbolite S3 format
turbolite import --input plain.db --bucket my-bucket --prefix databases/my-db
```

All S3 commands accept `--bucket`, `--prefix`, `--endpoint`, and `--region` flags, or read from `TURBOLITE_BUCKET`, `TURBOLITE_PREFIX`, `AWS_ENDPOINT_URL`, and `AWS_REGION` environment variables.

## Related Projects and Comparison

There are many projects in the SQLite-over-network space. turbolite borrows ideas from all of them.

### Range requests on raw .db files (read-only)

The most common approach: put an unmodified `.db` file on S3 or a CDN and issue HTTP Range GETs when SQLite reads a page.

- [**sql.js-httpvfs**](https://github.com/phiresky/sql.js-httpvfs): The original. WASM SQLite with HTTP Range requests. Has virtual read heads with exponential prefetch for scans. Pioneered the idea that you don't need to download the whole database to query it.
- [**sqlite_web_vfs**](https://github.com/mlin/sqlite_web_vfs): Native C++ VFS extension with adaptive request consolidation and an optional `.dbi` index file that pre-collects B-tree interior nodes for prefetching - the same idea as turbolite's interior page bundles. Designed to compose with sqlite_zstd_vfs.
- [**sqlite3vfshttp**](https://github.com/psanford/sqlite3vfshttp): Clean, minimal Go VFS. Built for querying SQLite in S3 from Lambda without downloading the file.
- [**sqlite-s3-query**](https://github.com/michalc/sqlite-s3-query): Python library using ctypes to intercept file I/O and translate reads to S3 Range GETs. Requires versioned buckets for consistency during database replacement.
- [**sqlite-wasm-http**](https://github.com/mmomtchev/sqlite-wasm-http): Spiritual successor to sql.js-httpvfs using the official SQLite WASM build. Shared page cache via SharedArrayBuffer. Actively maintained.
- [**s3sqlite**](https://github.com/litements/s3sqlite): Python, uses s3fs (FUSE) + APSW. Lets FUSE handle the range requests.

These are all read-only and fetch uncompressed pages from the raw file. A point lookup transfers a raw 4KB (or 64KB) page per request.

### Replication and backup to S3

These replicate local writes to S3 for backup or restore.

- [**Litestream**](https://github.com/benbjohnson/litestream): Continuously ships WAL frames to S3. The gold standard for SQLite backup. A newer Litestream VFS can serve reads from S3 using Range requests on LTX files with an LRU cache and page index — architecturally the closest thing to turbolite's read path, but read-only and tied to the Litestream replication format.
- [**LiteFS**](https://github.com/superfly/litefs): Fly.io's FUSE-based primary/replica system. Captures page changesets and streams them to replicas. Solves availability, not storage.
- [**Verneuil**](https://github.com/backtrace-labs/verneuil): Splits the database into 64KB chunks with zstd compression and a manifest file, async-replicates to S3. The chunk+manifest model resembles turbolite's page groups + manifest, but Verneuil is a replication tool - you query local disk, not S3.
- [**libSQL/sqld**](https://github.com/tursodatabase/libsql) (by Turso):  Fork of SQLite with a Virtual WAL interface. "Bottomless" mode ships WAL frames to S3. Queries are local; S3 is for restore.

### Custom storage engines

- [**mvsqlite**](https://github.com/losfair/mvsqlite): Pages stored in FoundationDB as content-addressed KV pairs. Full MVCC with time-travel to any snapshot, XOR+zstd delta encoding between page versions. The most sophisticated storage engine in this space, but requires FoundationDB, not S3.
- [**sqlite-s3vfs**](https://github.com/simonw/sqlite-s3vfs): Each SQLite page stored as a separate S3 object. Enables writes but at one PUT per page, costing \$0.02 per 4096 pages vs turbolite's $0.000005 for the same batch (at 64KB page default).

### Compression

- [**sqlite_zstd_vfs**](https://github.com/mlin/sqlite_zstd_vfs): Stores compressed pages as rows in an outer "wrapper" database. zstd with dictionary training. Composes with sqlite_web_vfs for compressed range-request reads over HTTP. The combination of sqlite_web_vfs + sqlite_zstd_vfs is probably the closest existing thing to turbolite's read path, but is read-only and doesn't batch pages into groups.
- [**SQLCipher**](https://github.com/sqlcipher/sqlcipher): Page-level AES-256 encryption for local SQLite. No remote storage.

### Where turbolite differs

| | turbolite | Raw-file range GETs | Litestream VFS | sqlite_web_vfs + zstd_vfs | mvsqlite | sqlite-s3vfs |
|---|---|---|---|---|---|---|
| Reads from S3 | seekable range GETs on compressed page groups | range GETs on raw pages | range GETs on LTX files | range GETs on compressed outer DB | KV lookups on FoundationDB | one GetObject per page |
| Writes to S3 | checkpoint (one PUT per group) | no | no | no | yes (MVCC) | one PUT per page |
| Compression | seekable multi-frame zstd | none | none | zstd (nested DB) | zstd delta encoding | none |
| Encryption | AES-256-GCM per page | none | none | none | none | none |
| Prefetch | look-ahead + hop schedule | none or basic readahead | LRU cache | adaptive consolidation | client buffers | none |
| Interior page optimization | detected, pinned, bundled separately | none | page index from LTX trailers | optional .dbi file | none | none |
| Bytes per point lookup (cache: index) | ~100KB (one compressed frame) | 4-64KB (one raw page) | varies | varies | varies | 4KB (one page) |
| Write cost per 4096 pages | ~$0.000005 (one PUT) | n/a | n/a | n/a | FoundationDB ops | ~$0.02 (4096 PUTs) |

## Benchmarking

All benchmarks live in [`benchmark/`](benchmark/). See [`benchmark/README.md`](benchmark/README.md) for deployment scenarios (local, Fly.io, EC2).

The `tiered-bench` binary generates a social media dataset (users, posts, likes, friendships) and benchmarks queries at each cache level against S3.

```bash
# Basic benchmark: 100K posts, default settings
TIERED_TEST_BUCKET=my-bucket AWS_ENDPOINT_URL=https://t3.storage.dev \
  cargo run --features zstd,cloud --bin tiered-bench --release -- \
    --sizes 100000

# 1M posts, 8 prefetch threads, only interior-level point queries
cargo run --features zstd,cloud --bin tiered-bench --release -- \
    --sizes 1000000 --prefetch-threads 8 --queries post --modes interior

# Quick local VFS comparison (no S3 needed)
cargo run --example quick-bench --features encryption --release
```

Key flags: `--sizes` (row counts), `--ppg` (pages per group), `--prefetch-threads`, `--prefetch-search` (SEARCH schedule), `--prefetch-lookup` (lookup schedule), `--grouping` (positional or btree), `--queries` (post/profile/who-liked/mutual), `--modes` (none/interior/index/data), `--skip-verify` (skip COUNT(*) on small machines), `--iterations`, `--plan-aware` (enable look-ahead prefetch), `--matrix` (sweep schedule pairs). Per-query schedules: `--post-prefetch`/`--post-lookup`, `--profile-prefetch`/`--profile-lookup`, etc. (search and lookup are independent per query).

```bash
# Matrix mode: test 10 schedule pairs x 6 queries at cold level
cargo run --features zstd,cloud --bin tiered-bench --release -- \
    --sizes 1000000 --import auto --plan-aware --matrix --iterations 10

# Tune schedules for your own database and queries
cargo run --features zstd,cloud --bin tiered-tune --release -- \
    --prefix "databases/my-db" \
    --query "SELECT * FROM users WHERE id = ?1" --param 42 \
    --plan-aware --iterations 10
```

## Testing

```bash
cargo test --features zstd                    # local VFS tests
cargo test --features zstd,cloud             # + S3 integration tests
cargo test --features zstd,encryption         # + encryption tests
```

## Experimental: precise prefetch

turbolite includes an experimental "interior page introspection" system (disabled by default) that parses B-tree interior pages to predict exact leaf groups for queries instead of using the hop-schedule heuristic. The idea: if you know the B-tree structure, you can skip guessing and go straight to the right page.

In benchmarks at 1M rows on Tigris, this made things worse. Being precisely wrong turned out to be more expensive than being approximately right. The hop schedule's "guess and overshoot" approach wastes some bandwidth but overlaps S3 I/O effectively. Precise predictions serialized requests, traded speculative parallelism for accuracy, and lost.

The code is there (`TURBOLITE_JENA=true` to enable) for future investigation. It may work better on lower-latency backends (S3 Express, ~4ms GETs) where the cost of an extra GET is cheap and precision matters more. For now, the hop schedule wins.

## Notes

turbolite was previously named `sqlite-compress-encrypt-vfs`, aka `sqlces`.

### Security model details

S3 data uses AES-256-GCM with unique random nonces per frame (authenticated, tamper-detecting). Local files use AES-256-CTR with deterministic nonces (page number / byte offset), providing confidentiality against disk-at-rest attackers. CTR's deterministic nonces mean multi-snapshot attackers could recover XOR of plaintexts at reused offsets, matching SQLite's own SEE extension tradeoff. The local cache is ephemeral and recreatable from S3.

## License

Apache-2.0
