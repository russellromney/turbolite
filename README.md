# turbolite

turbolite is an experimental SQLite VFS designed from first principles to serve cold queries from S3. 

> turbolite is **experimental**. It is new and may corrupt your data. Please be careful. 

turbolite also has page-level compression and encryption for efficiency and encryption at-rest and in-transit.

The design and name are inspired by [turbopuffer](https://turbopuffer.com/blog/turbopuffer)'s approach of designing for cloud storage constraints. S3 has distinct constraints (PUTs are expensive, GETs are cheap, objects are immutable, speed is constrained by ping and bandwidth) and turbolite's architecture is shaped by them rather than traditional filesystem constraints. It was also "inspired" by [Neon's "fast" 500ms+ cold starts](https://neon.com/blog/cold-starts-just-got-hot). 

turbolite ships as a Rust library, a [SQLite loadable extension](#loadable-extension) (`.so`/`.dylib`), and language packages for [Python](#python) and [Node.js](#nodejs), plus Github deps for Go. It's a standard VFS operating at the page level — most SQLite features work transparently: FTS, R-tree, JSON, WAL mode, etc.

If you want to contribute to turbolite or find bugs, please create a pull request or open an issue.

## Performance

| Query | Warm | Cold | Arctic |
|-------|------|------|--------|
| Point lookup | 98us | 23ms | 143ms |
| Profile (5 joins) | 404us | 252ms | 419ms |
| Who-liked | 203us | 11ms | 221ms |
| Mutual friends | 100us | 11ms | 142ms |
| Indexed filter | 65us | 10ms | 155ms |
| Full scan + filter | 287ms | 642ms | 812ms |

Benchmarked on Fly.io with Tigris with 8 vCPU, 16GB RAM, 8 prefetch threads. 1M-row social media dataset (1.46GB uncompressed with 64KB pages). See [Benchmarking](#Benchmarking).

**Warm** = all data in local cache, no connection open, no pages in memory. Normal SQLite case.

**Cold** = data pages evicted from cache, but interior and (optionally) index pages stay cached. Normal turbolite case. 

**Arctic** = everything evicted, including interior and index pages. A new connection is just pointed at an S3 bucket, downloads the manifest and interior pages, then is ready to serve queries. This only happens on a completely fresh start with an empty cache.

## Quick Start

### Python

```bash
pip install turbolite
```

```python
import sqlite3
import turbolite

# convenience wrapper for manually loading the extension
conn = turbolite.connect("my.db")

# execute queries
conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
conn.execute("INSERT INTO users VALUES (1, 'alice', 'alice@example.com')")
conn.commit()

alice = conn.cursor().execute("SELECT * FROM users").fetchone()
print(alice[1])
>>> "alice"
```

You can also manually load the extension:

```python
# Load the extension (registers the "turbolite" VFS process-wide)
conn = sqlite3.connect(":memory:")
turbolite.load(conn)
conn.close()

conn = sqlite3.connect("file:my.db?vfs=turbolite", uri=True)
```

See (installation details below) for Node, Go, Rust, and using the `.so` loadable extension directly


## Design

turbolite is designed for S3's constraints over filesystem constraints. Every decision flows from this model:

| S3 Constraint | Implication |
|---------------|-------------|
| **Round trips are slow** | Minimize request count. Batch writes, prefetch reads aggressively. |
| **Bandwidth is a bottleneck** | Maximize bandwidth utilitization over a time window. |
| **PUTs and GETs charge per-operation** | A 64KB GET costs the same as a 16MB GET. Minimize request count, not byte efficiency. |
| **Objects are immutable** | Never update in place. Write new versions, swap a pointer. No partial-write corruption. |
| **Storage is cheap** | Don't optimize for space. Over-provision, keep old versions, let GC clean up later. |

### Architecture

SQLite uses a B-Tree index and requests for one page at a time. It knows page N is at byte offset `N * page_size`. But on S3, fetching one page per request would mean thousands of GETs per query.

turbolite adds an indirection layer between SQLite and S3 that groups, compresses, and tracks pages:

```
SQLite: "read page 4,271"
    |
    v
Manifest: "page 4,271 is in group 16, sub-chunk 3, bytes 81,920-106,496"
    |
    v
turbolite check local storage: "page not in cache"
    |
    v
S3: GET for ~256KB compressed sub-chunk → decompress → return page
```

The **manifest** is the source of truth for where every page lives. It replaces SQLite's implicit `offset = page * size` with explicit pointers. It's also the durability commit point: at checkpoint, after it uploads page groups, it writes a new manifest. Manifest versions are immutable and always point to a consistent state.

SQLite defaults to 4KB pages to match filesystem disk page size. On S3, disk page size is irrelevant. What matters is minimizing request count and maximizing B-tree fan-out. The answer is **large pages**: turbolite defaults to 64KB pages. Fewer pages = fewer S3 round trips to reach a leaf.

**Page groups** batch sequential pages into a larger S3 object, optimized to saturate bandwidth on prefetching with reasonable latency for point queries. Default is 256 pages per chunk, ~16MB at 64KB pages (uncompressed). On checkpoint, dirty groups are written as new immutable versions.

But pages are not created equally. SQLite has different types of pages. turbolite **separates page groups by type**: interior B-Tree, index leaf, and data leaf pages. 

Interior pages are touched on every query to route lookups to leaf pages. turbolite detects them, stores them in compressed bundles in S3, and loads them eagerly on VFS open. After that, every B-tree traversal is a cache hit. 

Index leaf pages get the same treatment: separate bundles, lazy background prefetch, pinned against eviction. Cold queries only fetch data pages from S3.

To make point queries fast, turbolite uses **seekable compression**: each page group is encoded as multiple zstd frames (~4 pages per frame). The manifest stores byte offsets per frame, so a cache miss fetches just the ~256KB sub-chunk with the needed page via S3 range GET, not the entire group. 

To make scans not-slow (they'll never be fast), turbolite uses adaptive prefetching, inspired by exponential backoff. On a cache miss, two things happen concurrently:
1. **Inline range GET**: fetch the sub-chunk containing the needed page, return to SQLite immediately.
2. **Background prefetch**: submit the full group + neighbors to a thread pool.

Consecutive misses escalate aggressively: by default, first miss fetches 33% of all groups, the second miss another 33%, and the third miss the rest. A hit resets the miss counter (so technically you might see more than 3 hops). This is byte-inefficient (it might overfetch data) but request-count-efficient. 

> Note: prefetch aggression is configurable.

### Encryption & Compression

turbolite encrypts and compresses at the page level.

#### Compression

All data is (optionally) zstd-compressed before storage. Page groups use seekable multi-frame encoding that independently compressed each frame (~4 pages, ~256KB), so a point lookup decompresses only the relevant frame rather than not the entire page group. Custom zstd dictionaries can be trained on your data and embedded in the database for better compression ratios.

#### Encryption

If encryption is enabled, turbolite encrypts everything including S3 objects, local data on disk, WAL/journal files, and manifest files.

```rust
let config = TieredConfig {
    encryption_key: Some(my_32_byte_key),
    ..Default::default()
};
```

**Two encryption modes, chosen automatically by path:**

| Path | Algorithm | Nonce | Overhead | Why |
|------|-----------|-------|----------|-----|
| S3 (page groups, bundles) | AES-256-GCM | `(group_id << 16) \| frame_index` | +16 bytes/frame | Authenticated encryption. Tamper detection. Unique nonce per frame preserves seekable range GETs. |
| Local (cache, WAL, metadata) | AES-256-CTR | page number or byte offset | zero | Same-size ciphertext. Preserves OS page alignment. |

**Encrypt after compress, decrypt before decompress.** Compression operates on plaintext (compressing ciphertext is useless). On the S3 path: `plaintext → zstd compress → GCM encrypt → S3 PUT`. On read: `S3 GET → GCM decrypt → zstd decompress → plaintext`.

**Security model:** S3 data uses authenticated encryption (GCM) with unique nonces per frame - the strongest path for long-lived data. Local files use CTR with deterministic nonces (page number / byte offset), providing confidentiality against single-snapshot disk-at-rest attackers. CTR's deterministic nonces mean that an attacker who can capture multiple snapshots of the same file (e.g., via filesystem snapshots or NVMe wear leveling) could potentially recover XOR of plaintexts at reused offsets. This matches the trade-off made by SQLite's own SEE extension in OFB mode. For most deployments, the local cache is ephemeral and recreatable from S3, so this is acceptable.

## Strengths and Limitations

### When turbolite is fast

**Point lookups are the sweet spot.** A cold point lookup fetches 1-2 sub-chunks via S3 range GET (~100KB each). Interior and index pages are already cached. Arctic adds ~120ms for interior re-fetch + first data page. This works on any machine size.

**Scans with enough cores.** The prefetch pool saturates S3 bandwidth with adaptive hop scheduling (default: 33%/33%/remaining of all groups per consecutive miss). 8 threads cache the entire 1.46GB database in 2-3 hops.

### When turbolite is slow

**Scans on small machines.** With 1 prefetch thread, a cold scan over 1.46GB takes seconds, not milliseconds. The bottleneck is S3 round trips: each hop fetches groups serially. If your first query is a full scan on a 1-vCPU machine, expect cold startup to be painful.

**Bad thread tuning.** Too few prefetch threads and scans stall waiting for S3. Too many and you waste memory on idle threads. The default (`num_cpus + 1`) is reasonable, but scan-heavy workloads on large databases need more CPUs.

**First query penalty.** The first query on an arctic cache pays for interior page loading (~50-200ms depending on database size) plus at least one data fetch. Index pages are aggressively prefetched but slow. If the query needs an index page before the background fetch completes, it falls back to an inline range GET from the data page group.

### Current limitations

- **Single writer only.** Two machines writing to the same prefix will corrupt the manifest. Single-machine multi-process writes use fcntl locks (should work but not battle-tested). Multi-thread writes within one process have a known locking gap (fcntl is per-process, not per-thread).
- **No WAL shipping.** Writes between checkpoints live only in the local WAL. See [Durability](#durability).
- **No mmap.** Cache hits use `pread`, not memory-mapped I/O.
- **WAL shipping tools** (LiteStream, walrust) conflict with turbolite's checkpoint ownership. Built-in WAL shipping is on the roadmap.
- **Shared cache mode** is meaningless across S3.

SQLite features that **do** work transparently: FTS, R-tree, JSON, WAL mode, DELETE journal mode, VACUUM, autovacuum. All go through the page-level VFS interface.

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

turbolite is a storage layer, not a replication system, Some important notes on durability:

**After checkpoint**: page groups + manifest are in S3. S3 provides 11 nines of durability. This data survives machine loss.

**Between checkpoints**: writes live only in the local WAL on local disk. If the machine dies before the next checkpoint, those writes are gone.

Checkpoint frequency controls the tradeoff: more frequent checkpoints = smaller data-at-risk window but more S3 PUTs. The default is SQLite's auto-checkpoint (every 1000 WAL frames).

**WAL shipping** (not yet implemented): the VFS already intercepts every WAL write, so it could ship WAL frames to S3 in the background, closing the durability gap without waiting for checkpoint. This is on the roadmap.

## Local Mode (no S3)

turbolite also works as a purely local compressed/encrypted VFS:

Compression: zstd (default), lz4, snappy, gzip. With zstd, you can train and embed custom compression dictionaries and rotate automatically for more efficient compression. Larger page sizes compress better. See CLI for training tools. 

Encryption: AES-256-GCM per page.

Page-level operation means most SQLite features still work: FTS, R-tree, JSON, WAL mode. Most other SQLite compression/encryption extensions operate at the file level or require custom builds.

## Installation

**Python**: `pip install turbolite` — see [packages/python/](packages/python/)

**Node.js**: `npm install turbolite` — see [packages/node/](packages/node/)

**Rust**:
```toml
[dependencies]
turbolite = "0.1"                                              # local compressed VFS
turbolite = { version = "0.1", features = ["tiered"] }         # + S3 storage
turbolite = { version = "0.1", features = ["encryption"] }     # + encryption
```

### Loadable extension (any language)

Build the loadable extension for any language with SQLite's `load_extension`:

```bash
# after cloning the turbolite repo
make ext  # produces target/release/turbolite.{so,dylib}
```

```c
sqlite3_enable_load_extension(db, 1);
sqlite3_load_extension(db, "path/to/turbolite", NULL, NULL);
// VFS "turbolite" is now registered — open with ?vfs=turbolite
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

There are many projects in the SQLite-over-network space. turbolite builds on ideas from all of them.

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
- [**sqlite-s3vfs**](https://github.com/simonw/sqlite-s3vfs): Each SQLite page stored as a separate S3 object. Enables writes but at one PUT per page - $0.02 per 4096 pages vs turbolite's $0.000005 for the same batch.

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
| Bytes per cold point lookup | ~100KB (one compressed frame) | 4-64KB (one raw page) | varies | varies | varies | 4KB (one page) |
| Write cost per 4096 pages | ~$0.000005 (one PUT) | n/a | n/a | n/a | FoundationDB ops | ~$0.02 (4096 PUTs) |

The projects above each solved pieces of what turbolite implements. Many thanks to all of these projects for the ideas and inspiration.

## Benchmarking

All benchmarks live in [`benchmark/`](benchmark/). See [`benchmark/README.md`](benchmark/README.md) for deployment scenarios (local, Fly.io, EC2).

The `tiered-bench` binary generates a social media dataset (users, posts, likes, friendships) and benchmarks warm/cold/arctic queries against S3.

```bash
# Basic benchmark: 100K posts, default settings
TIERED_TEST_BUCKET=my-bucket AWS_ENDPOINT_URL=https://t3.storage.dev \
  cargo run --features zstd,tiered --bin tiered-bench --release -- \
    --sizes 100000

# 1M posts, 8 prefetch threads, only cold point queries
cargo run --features zstd,tiered --bin tiered-bench --release -- \
    --sizes 1000000 --prefetch-threads 8 --queries post --modes cold

# Quick local VFS comparison (no S3 needed)
cargo run --example quick-bench --features encryption --release
```

Key flags: `--sizes` (row counts), `--ppg` (pages per group), `--prefetch-threads`, `--prefetch-hops`, `--queries` (post/profile/who-liked/mutual), `--modes` (warm/cold/arctic), `--skip-verify` (skip COUNT(*) on small machines), `--iterations`.

## Testing

```bash
cargo test --features zstd                    # local VFS tests
cargo test --features zstd,tiered             # + S3 integration tests (31 tests)
cargo test --features zstd,encryption         # + encryption tests
```

## Notes

turbolite used to be named `sqlite-compress-encrypt-vfs`, aka `sqlces`.

## License

Apache-2.0
