# turbolite

turbolite is a SQLite VFS in Rust that serves point lookups and joins directly from S3 with sub-100ms cold latency. 

It also offers page-level compression (zstd) and encryption (AES-256) for efficiency and security at rest.

> turbolite is **experimental**. It is new and may corrupt your data. Please be careful.

Object storage is getting fast. [S3 Express One Zone](https://aws.amazon.com/s3/storage-classes/express-one-zone/) delivers single-digit millisecond GETs and [Tigris is extremely fast](https://www.tigrisdata.com/blog/benchmark-small-objects/). The gap between local disk and cloud storage is shrinking, and turbolite exploits that.

If you have one database per server, use a volume. turbolite is for when you have hundreds or thousands of databases (one per tenant, one per workspace, one per device), don't want a volume for each one, and you're okay with a single write source. 

The design and name are inspired by [turbopuffer](https://turbopuffer.com/blog/turbopuffer)'s approach of ruthlessly architecting around cloud storage constraints. The project's initial goal was to beat [Neon's 500ms+ cold starts](https://neon.com/blog/cold-starts-just-got-hot).

turbolite ships as a Rust library, a [SQLite loadable extension](#loadable-extension) (`.so`/`.dylib`), and language packages for [Python](#python) and [Node.js](#nodejs), plus Github deps for Go. Any S3-compatible storage works (AWS S3, Tigris, R2, MinIO, etc.). It's a standard SQLite VFS operating at the page level, so most SQLite features work transparently: FTS, R-tree, JSON, WAL mode, etc.

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

1M rows (1.46GB uncompressed) on EC2 c5.2xlarge, S3 Express One Zone, same AZ. 8 dedicated vCPU, 16GB RAM, 8 prefetch threads, 64KB pages. See [Benchmarking](#benchmarking).

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

SQLite uses a B-Tree index and requests for one page at a time. It knows page N is at byte offset `N * page_size`. But on S3, fetching one page per request would mean thousands of sequential GETs per query.

turbolite adds an indirection layer between SQLite and S3 that groups, compresses, and tracks pages. 

**Reads** look like: 

```
SQLite: "read page 4,271"
    |
    v
Manifest: "page 4,271 is in group 16, sub-chunk 3, bytes 81,920-106,496"
    |
    v
turbolite: check local storage -> cache miss
    |
    v
S3: GET for ~256KB compressed sub-chunk -> decompress -> return page
```

**Writes** go through SQLite's normal WAL, then flush to S3 at checkpoint:

```
SQLite: "commit + checkpoint"
    |
    v
turbolite: collect dirty pages, group by page group and page type
    |
    v
Encode: zstd compress each frame -> (optional) GCM encrypt
    |
    v
S3: PUT new page group versions -> PUT new manifest (atomic commit)
```

The **manifest** is the source of truth for where every page lives. It replaces SQLite's implicit `offset = page * size` with explicit pointers. Old page group versions are never overwritten; the manifest PUT is the atomic commit point. Old versions become garbage, cleaned up by `gc()`.

SQLite defaults to 4KB pages to match filesystem disk page size. On S3, disk page size is irrelevant. What matters is minimizing request count and maximizing B-tree fan-out. The answer is **large pages**: turbolite defaults to 64KB pages. Fewer pages = fewer S3 round trips to reach a leaf.

**Page groups** batch many pages into a single S3 object. Big enough to saturate bandwidth on prefetch, small enough for point queries. Default: 256 pages per group, ~16MB at 64KB pages.

But pages are not created equally. SQLite has different types of pages. turbolite **separates page groups by type**: interior B-Tree, index leaf, and data leaf pages. 

Interior pages are touched on every query to route lookups to leaf pages. turbolite detects them, stores them in compressed bundles in S3, and loads them eagerly on VFS open. After that, every B-tree traversal is a cache hit. 

Index leaf pages get the same treatment: separate bundles, lazy background prefetch, pinned against eviction. Cold queries only fetch data pages from S3.

To make point queries fast, turbolite uses **seekable compression**: each page group is encoded as multiple **zstd frames** (~4 pages per frame). The manifest stores byte offsets per frame, so a cache miss fetches just the ~256KB sub-chunk with the needed page via S3 range GET, not the entire group.

To make scans not-slow (they'll never be fast), turbolite uses **speculative prefetching**, inspired by exponential backoff. On a cache miss, two things happen concurrently:
1. **Inline range GET**: fetch the sub-chunk containing the needed page, return to SQLite immediately.
2. **Background prefetch**: submit the full group + neighbors to a thread pool.

Consecutive misses escalate aggressively: by default, first miss fetches 33% of all groups, the second miss another 33%, and the third miss the rest. A hit resets the miss counter (so technically you might see more than 3 hops). This is byte-inefficient (it might overfetch data) but request-count-efficient. 

turbolite calls this speculative prefetching (because we're guessing) or adaptive prefetching (because we can adapt aggression level after cache hits)

Prefetch aggression is configurable, .33/.33/.34 is just the default tuned for mixed workloads. 

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
| `prefetch_hops` | Fraction of total groups to fetch per consecutive miss | `0.33, 0.33` (3 hops) |
| `pages_per_group` | Pages per S3 object, larger = fewer PUTs, more bytes per fetch | 256 |
| `gc_enabled` | Delete old page group versions after checkpoint | false |

**Recommended configs:**

| Workload | Config | Why |
|----------|--------|-----|
| Point lookups, small machine | `prefetch_threads=1, prefetch_hops=0.01,0.02,0.97` | Minimal background work. Rely on inline sub-chunk range GET. |
| Mixed OLTP | `prefetch_threads=4, prefetch_hops=0.33,0.33` | Balanced. Good for most workloads. |
| Scan-heavy analytics | `prefetch_threads=8+, prefetch_hops=0.5,0.5` | Aggressive. Saturate S3 bandwidth. |
| Bursty serverless | `prefetch_threads=1-2, prefetch_hops=0.1,0.2,0.7` | Conservative start, ramp on sustained misses. |

**Limitation**: prefetch is per-connection. Each new connection starts with a cold hop counter. The cache is shared, so a second connection benefits from pages cached by the first.

## Durability

turbolite is a storage layer, not a replication system.

**After checkpoint**: page groups + manifest are in S3. S3 provides 11 nines of durability. This data survives machine loss.

**Between checkpoints**: writes live only in the local WAL on local disk. If the machine dies before the next checkpoint, those writes are gone.

Checkpoint frequency controls the tradeoff: more frequent checkpoints = smaller data-at-risk window but more S3 PUTs. The default is SQLite's auto-checkpoint (every 1000 WAL frames).

**WAL shipping** (not yet implemented): the VFS already intercepts every WAL write, so it could ship WAL frames to S3 in the background, closing the durability gap without waiting for checkpoint. This is on the roadmap.

**Consistency model:** single writer, snapshot readers. One process writes; readers see the last committed manifest when they opened. turbolite is not a distributed database and does not coordinate between multiple writers.

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

Key flags: `--sizes` (row counts), `--ppg` (pages per group), `--prefetch-threads`, `--prefetch-hops`, `--queries` (post/profile/who-liked/mutual), `--modes` (none/interior/index/data), `--skip-verify` (skip COUNT(*) on small machines), `--iterations`.

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
