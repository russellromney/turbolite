# turbolite

turbolite is an experimental SQLite VFS designed from first principles to serve cold queries from S3. 

Current performance is sub-50ms cold queries on a multi-GB SQLite database, with sub-500ms filter queries and subsecond full scans (depending on bandwidth and threads). Indexed filter/scan are faster.

turbolite also has page-level compression (speed) and encryption, enabling full-text search with small on-disk size databases and encryption in-transit and at-rest.

Much of the design is inspired by [turbopuffer](https://turbopuffer.com/blog/turbopuffer)'s approach of designing for cloud storage constraints. S3 has distinct constraints (PUTs are expensive, GETs are cheap, objects are immutable, speed is constrained by ping and bandwidth) and turbolite's architecture is shaped by them rather than traditional filesystem constraints. 

turbolite is a Rust library. Loadable extension (`.so`/`.dylib`) is planned.

turbolite is a standard VFS - operating at the page level, so all SQLite features work transparently: FTS, R-tree, JSON, WAL mode, etc.

If you have ideas to improve this, please feel free to contribute a PR directly.

> turbolite is experimental. It is new and it may have bugs that could corrupt your data.. Please don't use this on any data that matters yet. 

> turbolite used to be named "sqlite-compress-encrypt-vfs (sqlces)

## Quick Start

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

## Performance

Benchmarked on Fly.io (iad region) against Tigris S3, 1M-row social media dataset (1.7GB, 104 page groups):

### 8 vCPU, 16GB RAM, 8 prefetch threads

| Query | Warm | Cold | Arctic |
|-------|------|------|--------|
| Point lookup | 37us | 40ms | 165ms |
| Profile (joins) | 131us | 376ms | 487ms |
| Who-liked | 119us | 123ms | 417ms |
| Mutual friends | 55us | 157ms | 170ms |

### shared-cpu-1x, 256MB RAM, 1 prefetch thread

| Query | Cold |
|-------|------|
| Point lookup | 153ms |

**Warm** = all data in local cache, but no connectin is open and there are no pages in memory

**Cold** = data pages evicted from cache, but B-tree interior pages and the root page stay pinned. A point lookup traverses the B-tree using pinned interior pages (cache hits), then fetches only the leaf page from S3. This is the common case: interior pages are loaded once on first query and survive cache evictions.

**Arctic** = everything evicted, including interior pages. The VFS must re-fetch interior page bundles from S3 before any query. This only happens on a completely fresh start with an empty cache.

### When turbolite is fast

**Point lookups are the sweet spot.** A cold point lookup needs 1-2 S3 range GETs (~100KB each) to fetch the leaf page. Interior pages are already pinned. This works on any machine size.

**Scans with enough cores.** A full table scan over 1.7GB completes in ~500ms on 8 threads — the prefetch pool saturates S3 bandwidth and caches everything in 3 hops. But this requires cores to parallelize the S3 fetches.

### When turbolite is slow

**Scans on small machines.** With 1 prefetch thread, a cold scan over 1.7GB takes seconds, not milliseconds. The bottleneck is S3 round trips: each hop fetches groups serially. If your first query is a full scan on a 1-vCPU machine, expect cold startup to be painful, as in many seconds to minutes. 

**Bad thread tuning.** Too few prefetch threads and scans stall waiting for S3. Too many and you waste memory on idle threads. The default (`num_cpus + 1`) is reasonable, but scan-heavy workloads on large databases need a lot of CPUs

**First query penalty.** The first query on a cold cache always pays for interior page loading (~50-200ms depending on database size) plus at least one data fetch. Subsequent queries benefit from pinned interior pages.

## Design

turbolite's architecture ignores filesystem constraints when limited by S3 constraints:

* Round trips are expensive.
* Bandwidth is constrained
* Ping time matters
* Storage size is cheap and should essentially be ignored
* GETs and PUTs charge per-operation not per-byte

The following are core to turbolite's design:

### 1. Immutable Page Groups

Pages are batched into groups (default: 4096 pages = ~16MB uncompressed, ~8MB compressed). Each group is stored as a single S3 object. On checkpoint, dirty groups are written as new versioned objects. The old version is never modified.

This is the key optimization for S3: **one PUT per group instead of one PUT per page.** Writing 4096 pages costs $0.000005 instead of $0.02. And because objects are immutable, there's no partial-write corruption: a group either exists completely or doesn't.

### 2. Manifest

A single JSON manifest tracks which S3 keys hold which page ranges. The manifest is the atomic commit point: upload new page groups first, then swap the manifest. Until the new manifest lands, readers see the old consistent state. If the process crashes mid-checkpoint, the old manifest is still valid.

This gives you write atomicity, crash safety, and point-in-time restore (old page groups still exist in S3 until GC).

### 3. Seekable Compression

This is the killer feature for point queries.

Each page group is encoded as multiple zstd frames (one per ~32 pages). The manifest stores a frame table with byte offsets and sizes. On a cache miss, turbolite issues an S3 byte-range GET for just the frame containing the needed page (~100KB) instead of downloading the entire group (~8MB).

A range GET costs the same as a full GET ($0.40/M requests) as you pay per request, not per byte. So seekable compression gives you the write efficiency of large objects (fewer PUTs) with the read granularity of small ones (fewer bytes per point lookup).

Many other projects have implemented range queries as an S3 backend for SQLite, kudos to them for the idea.

### 4. Aggressive Prefetch with Minimal Hops

On a cache miss, turbolite does two things concurrently:
1. **Inline range GET**: fetch the specific sub-chunk containing the needed page. Returns to SQLite immediately.
2. **Background prefetch**: submit the full group (and neighboring groups) to a thread pool for parallel download.

Consecutive misses escalate prefetch aggressively via a hop schedule. Default: first miss fetches 33% of all groups, second miss another 33%, third miss everything remaining. A cache hit resets the counter.

This means a full table scan over 1.7GB caches the entire database in 3 round trips to S3. Point lookups stay cheap (1 inline range GET + background prefetch of the current group).

### 5. S3 Connection Pool

The prefetch pool is a fixed set of worker threads (default: `num_cpus + 1`), each with its own S3 connection. Groups are dispatched to workers via a channel. The extra thread over CPU count keeps the pipeline full: when one thread blocks on S3 I/O, the extra thread uses that core for decompression and cache writes.

### 6. Interior Page Bundles

B-tree interior pages (type 0x05, 0x02) are the pages SQLite touches on every single query. They're the index nodes that route lookups to leaf pages. Normally, these pages are distributed randomly throughout the pages (and thus the page groups). This means that top-leaf operations would constantly have to fetch random page groups (prefetch is sequential, note). turbolite detects interior pages at read time using the data vs interior page bit and:

- **Pins them** so they survive cache evictions
- **Stores them separately** in compressed bundles in S3 (not mixed into data page groups)
- **Loads them eagerly** on VFS open (one parallel fetch of all interior bundles)

After this initial load, every B-tree traversal is a cache hit. Cold queries only need to fetch leaf data from S3. Interior pages within data groups are just ignored, as they're already cached from the bundles.

### Read Path

```
SQLite read_page(N)
    |
    v
[Page bitmap] --> hit? --> pread from cache file --> done
    |
    miss
    |
    v
[Inline range GET] --> decompress sub-chunk --> write to cache --> return page
    |
    (concurrently)
    v
[Prefetch pool] --> fetch full group + neighbors --> cache all pages
```

### Write Path

```
SQLite writes --> local WAL (source of truth)
    |
    checkpoint
    |
    v
[Group dirty pages] --> [Compress + PUT to S3] --> [Upload manifest] --> done
                         (immutable, versioned)     (atomic commit point)
```

Between checkpoints, local disk is the source of truth. The WAL contains all uncommitted changes. Checkpoint is when data becomes durable in S3.

The gap between "WAL committed locally" and "manifest uploaded to S3" is the data-at-risk window. For zero-loss, layer WAL shipping (walrust, LiteStream) on top.

### Storage Management

Immutable writes mean old page group versions accumulate in S3. This is cheap ($0.02/GB/month) and useful (point-in-time restore), but eventually needs cleanup.

**Garbage collection** (implemented):
- **Post-checkpoint GC** (`gc_enabled=true`): delete replaced versions immediately after checkpoint.
- **Full GC scan** (`TieredVfs::gc()`): list all S3 objects, delete anything not in the current manifest. Catches crash orphans.

**VACUUM and autovacuum** work transparently through the VFS. VACUUM rewrites the database, producing new compact page groups at checkpoint. Autovacuum reclaims freed pages incrementally.

**Planned**: tunable GC retention (keep N old versions for PITR window), hole tracking, CLI.

## Tuning

| Parameter | What it controls | Default |
|-----------|-----------------|---------|
| `prefetch_threads` | Worker threads for parallel S3 fetches | num_cpus + 1 |
| `prefetch_hops` | Fraction of total groups to fetch per consecutive miss | `0.33, 0.33` (3 hops) |
| `pages_per_group` | Pages per S3 object — larger = fewer PUTs, more bytes per fetch | 4096 |
| `gc_enabled` | Delete old page group versions after checkpoint | false |

**Recommended configs:**

| Workload | Config | Why |
|----------|--------|-----|
| Point lookups, small machine | `prefetch_threads=1, prefetch_hops=0.01,0.02,0.97` | Minimal background work. Rely on inline sub-chunk range GET. |
| Mixed OLTP | `prefetch_threads=4, prefetch_hops=0.33,0.33` | Balanced. Good for most workloads. |
| Scan-heavy analytics | `prefetch_threads=8+, prefetch_hops=0.5,0.5` | Aggressive. Saturate S3 bandwidth. |
| Bursty serverless | `prefetch_threads=1-2, prefetch_hops=0.1,0.2,0.7` | Conservative start, ramp on sustained misses. |

**Limitation**: prefetch is per-connection. Each new connection starts with a cold hop counter. The cache is shared, so a second connection benefits from pages cached by the first.

## Local Mode (no S3)

turbolite also works as a purely local compressed/encrypted VFS:

Compression: zstd (default), lz4, snappy, gzip. Encryption: AES-256-GCM per page.

Page-level operation means all SQLite features work — FTS, R-tree, JSON, WAL mode. Most other SQLite compression/encryption extensions operate at the file level or require custom builds.

## Installation

**Rust** (library):
```toml
[dependencies]
turbolite = "0.1"                                              # local compressed VFS
turbolite = { version = "0.1", features = ["tiered"] }         # + S3 storage
turbolite = { version = "0.1", features = ["encryption"] }     # + encryption
```

## Comparison

turbolite aims to be the most efficient S3-native SQLite VFS. 

| | turbolite | sql.js-httpvfs | SQLCipher | sqlite_zstd_vfs | LiteFS |
|---|---|---|---|---|---|
| S3 storage | page-level range GETs | HTTP range on whole file | no | no | FUSE proxy |
| Compression | page-level, seekable | no | no | page-level | no |
| Encryption | page-level AES-256-GCM | no | page-level | no | no |
| Prefetch | adaptive, parallel | fixed readahead | n/a | n/a | n/a |
| Cold point lookup | 40ms | ~200-500ms | n/a | n/a | n/a |
| Writes | checkpoint-to-S3 | read-only | local | local | replicated |
| Language | Rust | JS/WASM | C | C++ | Go |

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

## License

Apache-2.0
