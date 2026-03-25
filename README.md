# turbolite

turbolite is a SQLite VFS in Rust that serves point lookups and joins directly from S3 with sub-250ms cold latency. 

It also offers page-level compression (zstd) and encryption (AES-256) for efficiency and security at rest.

> turbolite is **experimental**. It is new and contains bugs. It may corrupt your data. Please be careful.

Object storage is getting fast. [S3 Express One Zone](https://aws.amazon.com/s3/storage-classes/express-one-zone/) delivers single-digit millisecond GETs and [Tigris is extremely fast](https://www.tigrisdata.com/blog/benchmark-small-objects/). The gap between local disk and cloud storage is shrinking, and turbolite exploits that.

The design and name are inspired by [turbopuffer](https://turbopuffer.com/blog/turbopuffer)'s approach of ruthlessly architecting around cloud storage constraints. The project's initial goal was to beat [Neon's 500ms+ cold starts](https://neon.com/blog/cold-starts-just-got-hot). Goal achieved.

If you have one database per server, use a volume. turbolite explores how to have hundreds or thousands of databases (one per tenant, one per workspace, one per device), don't want a volume for each one, and you're okay with a single write source.

turbolite ships as a Rust library, a [SQLite loadable extension](#loadable-extension) (`.so`/`.dylib`), and language packages for [Python](#python) and [Node.js](#nodejs), plus Github deps for Go. Any S3-compatible storage works (AWS S3, Tigris, R2, MinIO, etc.). It's a standard SQLite VFS operating at the page level, so most SQLite features should work: FTS, R-tree, JSON, WAL mode, etc.

If you want to contribute to turbolite or find bugs, please create a pull request or open an issue.

## Performance

| Query | Cache: none | Cache: interior | Cache: index | Cache: data |
|-------|-------------|-----------------|--------------|-------------|
| Point lookup | 75ms | 11ms | 11ms | 157us |
| Profile (5 joins) | 202ms | 134ms | 113ms | 301us |
| Who-liked | 118ms | 51ms | 5ms | 259us |
| Mutual friends | 82ms | 25ms | 5ms | 116us |
| Indexed filter | 76ms | 12ms | 5ms | 106us |
| Full scan + filter | 691ms | 685ms | 562ms | 280ms |

1M rows (1.46GB uncompressed) on EC2 c5.2xlarge, S3 Express One Zone, same AZ. 8 dedicated vCPU, 16GB RAM, 8 prefetch threads, 64KB pages. See [Benchmarking](#benchmarking). Fly/Tigris or EC2+S3 (not Express) is 20-50% slower. 

Benchmarks are organized by **cache level** (what's already on local disk when the query runs):

| Cache level | What's cached | What's fetched from S3 | When this happens |
|-------------|--------------|----------------------|-------------------|
| **none** | nothing | everything | Fresh start, empty cache |
| **interior** | interior B-tree pages | index + data pages | First query after connection open |
| **index** | interior + index pages | data pages only | Normal turbolite operation |
| **data** | everything | nothing | Equivalent to local SQLite |

**interior** is the most realistic cold benchmark: interior pages load eagerly on connection open, so by the time you run your first query, they're cached. Index pages aggressively prefetch on first access in the background and may not be ready yet.

## Quick Start

### Python

```bash
pip install turbolite
```

```python
import turbolite

# tiered database — serve cold queries from S3-compatible storage (Tigris)
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

turbolite adds introspection and indirection layers between SQLite and S3 that efficiently groups, compresses, and tracks pages. 

SQLite uses a B-tree index and requests for one page at a time. It knows page N is at byte offset `N * page_size`. And those pages are distributed randomly throughout the pagemap for efficient random access. But on S3, fetching one page per request would mean thousands of potentially random GETs per query. 

But pages are not created equally. SQLite has different types of pages. turbolite **separates page groups by type**: interior B-tree, index leaf, and data leaf pages.

Interior pages are touched on every query to route lookups to leaf pages. turbolite detects them, stores them in compressed bundles in S3, and loads them eagerly on VFS open. After that, every B-tree traversal is a cache hit. 

Index leaf pages get the same treatment: separate bundles, lazy background prefetch, pinned against eviction. Cold queries only need to fetch data pages. 

turbolite takes advantage of **B-tree introspection** to understand *which tree (a table or index) a page is part of*, and intelligently stores those pages together in S3 as **page groups**: many pages chunked into a single S3 object. Big enough to saturate bandwidth on prefetch, small enough for point queries. Default: 256 pages per group, ~16MB at 64KB pages.

Storing the same table/index together means that we make the fewest possible GETs for cold queries. 

turbolite **indirects page lookups with a manifest file** that is the source of truth for where every page lives. It replaces SQLite's implicit `offset = page * size` with explicit pointers. Old page group versions are never overwritten; the manifest PUT is the atomic commit point. Old versions become garbage, cleaned up by `gc()`.

SQLite defaults to 4KB pages to match filesystem disk page size. On S3, disk page size is irrelevant. What matters is minimizing request count and maximizing B-tree fan-out. The answer is **large pages**: turbolite defaults to 64KB pages. Fewer pages = fewer S3 round trips to reach a leaf.

To make point queries fast, turbolite uses **seekable compression**: each page group is encoded as multiple **zstd frames** (~4 pages per frame). The manifest stores byte offsets per frame, so a cache miss fetches just the ~256KB sub-chunk with the needed page via S3 range GET, not the entire group.

To make scans not-slow (they'll never be fast), turbolite uses **adaptive prefetching**, inspired by exponential backoff. On a cache miss, two things happen concurrently:
1. **Inline range GET**: fetch the specific page group frame containing the needed page, return to SQLite immediately.
2. **Background prefetch**: submit the full group *for that tree (index or table)* to a thread pool for fetch according to the hop schedule.

Consecutive misses escalate: each miss advances through a **"prefetch aggression" schedule** that controls what fraction of groups to prefetch. 

The default aggression schedule is `[0.0, 0.5, 0.5]` which skips the first miss entirely (so point queries pay zero overhead), then fetches 50% of *same-tree page groups* on the second miss and the remaining 50% on the third.

This schedule takes advantage of B-tree introspection, but it does make a key tradeoff: a slower latency floor (extra GET on cache miss) for extremely efficient scans: every prefetched group in a scan is guaranteed to contain pages from the right tree. In the 1.5GB benchmark, this brings down a full table scan from ~650ms to ~250ms, while adding ~50ms to point fetches. 

An example: if SQLite requests a page from the `users` table, then requests another from the same table, turbolite assumes a scan is coming and immediately fetches the rest of the `users` table in the background - *and nothing else*. Without B-tree introspection, it would accidentally fetch half the users table and half the posts table just because the data lives next to each other on disk.

turbolite also has an experimental **semantic predictive prefetching** feature that tracks **cross-table access patterns** using a lightweight prediction engine held as a [trie](https://ds.cs.rutgers.edu/assignment-trie/) in the manifest. When queries consistently touch the same set of tables together (e.g. `users` then `posts` then `likes`), future queries that touch any subset trigger background prefetch of the rest. See [Predictive Prefetching](#predictive-prefetching-experimental) for details.

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

**Scans with enough cores.** The prefetch pool saturates S3 bandwidth with flexible hop scheduling (default: 33%/33%/remaining of all groups per consecutive miss). Sufficient threads can sync multi-GB databases in seconds with 2-3 prefetch batches.

### Where turbolite is slow

**Scans on small machines.** With 1 prefetch thread, a scan over 1.46GB takes seconds, not milliseconds. The bottleneck is S3 round trips: each hop fetches groups serially. If your first query is a full scan on a 1-vCPU machine, expect startup to be painful.

**Bad thread tuning.** Too few prefetch threads and scans stall waiting for S3. Too many and you waste memory on idle threads. The default (`num_cpus + 1`) is reasonable, but scan-heavy workloads on large databases need more CPUs.

**First query penalty.** The first query at cache level `none` pays ~50-200ms for interior page loading plus at least one data fetch. If the query needs an index page before background prefetch finishes, it falls back to an inline range GET.

### Current limitations

- **Single writer only.** Two machines writing to the same prefix will corrupt the manifest.
- **No WAL shipping.** Writes between checkpoints live only in the local WAL. See [Durability](#durability).

SQLite features that **do** work: FTS, R-tree, JSON, WAL mode, DELETE journal mode, VACUUM, autovacuum.

## Tuning

| Parameter | What it controls | Default |
|-----------|-----------------|---------|
| `prefetch_threads` | Worker threads for parallel S3 fetches | num_cpus + 1 |
| `btree_prefetch_hops` | Sibling prefetch schedule (BTreeAware strategy) | `0.0, 0.5, 0.5` (skip first miss) |
| `pages_per_group` | Pages per S3 object, larger = fewer PUTs, more bytes per fetch | 256 |
| `grouping_strategy` | How pages are packed into groups at import | BTreeAware |
| `gc_enabled` | Delete old page group versions after checkpoint | false |
| `sync_mode` | Checkpoint durability: `Durable` (S3 upload in checkpoint) or `LocalThenFlush` (defer upload) | Durable |

**Recommended configs:**

| Workload | Config | Why |
|----------|--------|-----|
| Point lookups, small machine | `btree_prefetch_hops=0.0,0.0,0.5,0.5` | Zero overhead on first two misses. Only prefetch on sustained access. |
| Mixed OLTP | `btree_prefetch_hops=0.0,0.5,0.5` | Skip point queries, aggressive on scans. Default. |
| Scan-heavy analytics | `prefetch_threads=8+, prefetch_hops=0.5,0.5` | Aggressive hops. Saturate S3 bandwidth. |
| Bursty serverless | `btree_prefetch_hops=0.0,0.0,0.5,0.5` | Conservative. Two misses before any prefetch. |

**Limitation**: prefetch is per-connection. Each new connection starts with a cold hop counter. The cache is shared, so a second connection benefits from pages cached by the first.

## Durability

turbolite is a storage layer, not a replication system. Durability depends on when data reaches S3.

**After checkpoint**: page groups + manifest are in S3. S3 provides 11 nines of durability. This data survives machine loss.

**Between checkpoints**: writes live only in the local WAL on local disk. If the machine dies before the next checkpoint, those writes are gone.

Checkpoint frequency controls the tradeoff: more frequent checkpoints = smaller data-at-risk window but more S3 PUTs. The default is SQLite's auto-checkpoint (every 1000 WAL frames).

### Checkpoint modes

turbolite supports two checkpoint modes via `sync_mode` in `TieredConfig`:

**`SyncMode::Durable`** (default). The checkpoint uploads page groups to S3 while holding SQLite's EXCLUSIVE lock. Simple, fully durable on every checkpoint. No writes or reads can proceed until the upload completes. Good for most workloads.

**`SyncMode::LocalThenFlush`**. The checkpoint writes to local disk cache only (~1ms lock hold), then releases the lock. The caller uploads to S3 separately via `flush_to_s3()`, during which reads and writes continue normally. This is useful for write-heavy workloads where blocking readers for the duration of an S3 upload is unacceptable.

Between checkpoint and flush, data exists only in the local disk cache. A process crash is fine (data is on local disk). Machine loss before flush means those writes are gone. Cache eviction is safe: turbolite protects pending pages from eviction automatically.

### WAL shipping

WAL shipping (not yet implemented): the VFS already intercepts every WAL write, so it could ship WAL frames to S3 in the background, closing the durability gap between writes and checkpoints. This is on the roadmap. WAL shipping is complementary to SyncMode: SyncMode controls how checkpoints reach S3, WAL shipping would make individual writes durable before checkpoint.

### Consistency model

Single writer, snapshot readers. One process writes; readers see the last committed manifest when they opened. turbolite is not a distributed database and does not coordinate between multiple writers.

## Local Mode (no S3)

turbolite also works as a purely local compressed/encrypted VFS:

Compression: zstd (default), lz4, snappy, gzip. With zstd, you can train and embed custom compression dictionaries and rotate automatically for more efficient compression. Larger page sizes compress better. See CLI for training tools. 

Encryption: AES-256-GCM per page.

Page-level operation means most SQLite features still work: FTS, R-tree, JSON, WAL mode. Most other SQLite compression/encryption extensions operate at the file level or require custom builds.

## Predictive Prefetching (Experimental)

turbolite includes an experimental **predictive prefetch engine** that learns cross-table access patterns and prefetches data before SQLite asks for it. This is inspired by [semantic caching for LLMs](https://redis.io/blog/what-is-semantic-caching/). 

This has not been tested very much yet. 

The engine tracks which tables and indexes are accessed together. Repeated co-access builds confidence in a pattern; confidence decays over time so stale patterns (dropped/renamed tables) prune themselves. On a cache miss, if the current session's touched tables match a known pattern, the predicted tables' page groups are prefetched immediately. Patterns that match reality are reinforced; wrong predictions decay faster.

Patterns are keyed by **table/index name**, not page number, so they survive VACUUMk, autovacuum, and local data eviction without re-learning.

**Example.** An e-commerce dashboard query:

```sql
-- "order detail" page: 4 tables, 6 indexes touched
SELECT o.*, c.name, c.email FROM orders o
  JOIN customers c ON c.id = o.customer_id WHERE o.id = ?;
SELECT li.*, p.name, p.sku FROM line_items li
  JOIN products p ON p.id = li.product_id WHERE li.order_id = ?;
```

This touches `{orders, customers, line_items, products, idx_orders_id, idx_customers_id, idx_line_items_order_id, idx_products_id, idx_line_items_product_id, idx_orders_customer_id}`. After a few repetitions, a cache miss on `orders` prefetches all ten trees' page groups in the background. By the time SQLite reaches `line_items`, data is already cached. Without prediction, each table/index hit is a separate cold miss, potentially 10+ sequential S3 round trips.

On connection open, the engine also prompts turbolite to prefetche the **top-N most frequently accessed trees** by historical frequency.Combined with eager interior page loading, the first query on a warm workload often hits zero S3 requests.

**Planned: frame-level correlation.** Currently the ngine prediction prefetches entire tables. Future work will narrow this to specific sub-chunks: "if you read frame 7 of table A, you'll need frame 12 of table B, then frame 17 of table C." This aims to reduce prefetch bandwidth by orders of magnitude for large tables.

## Installation

**Python**: `pip install turbolite` — see [packages/python/](packages/python/)

```python
import turbolite

# Local compressed (no S3 needed)
conn = turbolite.connect("my.db")

# S3 tiered
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
turbolite = "0.2"                                              # local compressed VFS
turbolite = { version = "0.2", features = ["tiered"] }         # + S3 storage
turbolite = { version = "0.2", features = ["encryption"] }     # + encryption
```

**Go** (cgo, links the shared library):
```bash
make lib-bundled  # build libturbolite.{so,dylib}
```
```go
// #cgo LDFLAGS: -L/path/to/target/release -lturbolite
// #include <stdlib.h>
// extern int turbolite_register_compressed(const char* name, const char* base_dir, int level);
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

### Rust

```rust
use turbolite::{register, CompressedVfs};

let vfs = CompressedVfs::new("/path/to/data", 3);  // zstd level 3
register("turbolite", vfs)?;

let conn = rusqlite::Connection::open_with_flags_and_vfs(
    "mydb.db",
    rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
    "turbolite",
)?;
```

### Rust (S3 tiered)

```rust
let config = TieredConfig {
    bucket: "my-bucket".into(),
    prefix: "my-database".into(),
    cache_dir: "/tmp/cache".into(),
    ..Default::default()
};
let vfs = TieredVfs::new(config)?;
turbolite::tiered::register("turbolite", vfs)?;

let conn = rusqlite::Connection::open_with_flags_and_vfs(
    "mydb.db",
    rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
    "turbolite",
)?;
```

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
| Prefetch | fraction-based hop schedule | none or basic readahead | LRU cache | adaptive consolidation | client buffers | none |
| Interior page optimization | detected, pinned, bundled separately | none | page index from LTX trailers | optional .dbi file | none | none |
| Bytes per point lookup (cache: index) | ~100KB (one compressed frame) | 4-64KB (one raw page) | varies | varies | varies | 4KB (one page) |
| Write cost per 4096 pages | ~$0.000005 (one PUT) | n/a | n/a | n/a | FoundationDB ops | ~$0.02 (4096 PUTs) |

## Benchmarking

All benchmarks live in [`benchmark/`](benchmark/). See [`benchmark/README.md`](benchmark/README.md) for deployment scenarios (local, Fly.io, EC2).

The `tiered-bench` binary generates a social media dataset (users, posts, likes, friendships) and benchmarks queries at each cache level against S3.

```bash
# Basic benchmark: 100K posts, default settings
TIERED_TEST_BUCKET=my-bucket AWS_ENDPOINT_URL=https://t3.storage.dev \
  cargo run --features zstd,tiered --bin tiered-bench --release -- \
    --sizes 100000

# 1M posts, 8 prefetch threads, only interior-level point queries
cargo run --features zstd,tiered --bin tiered-bench --release -- \
    --sizes 1000000 --prefetch-threads 8 --queries post --modes interior

# Quick local VFS comparison (no S3 needed)
cargo run --example quick-bench --features encryption --release
```

Key flags: `--sizes` (row counts), `--ppg` (pages per group), `--prefetch-threads`, `--btree-hops` (B-tree prefetch schedule), `--grouping` (positional or btree), `--queries` (post/profile/who-liked/mutual), `--modes` (none/interior/index/data), `--skip-verify` (skip COUNT(*) on small machines), `--iterations`.

## Testing

```bash
cargo test --features zstd                    # local VFS tests
cargo test --features zstd,tiered             # + S3 integration tests
cargo test --features zstd,encryption         # + encryption tests
```

## Notes

turbolite was previously named `sqlite-compress-encrypt-vfs`, aka `sqlces`.

### Security model details

S3 data uses AES-256-GCM with unique random nonces per frame (authenticated, tamper-detecting). Local files use AES-256-CTR with deterministic nonces (page number / byte offset), providing confidentiality against disk-at-rest attackers. CTR's deterministic nonces mean multi-snapshot attackers could recover XOR of plaintexts at reused offsets, matching SQLite's own SEE extension tradeoff. The local cache is ephemeral and recreatable from S3.

## License

Apache-2.0
