# turbolite

SQLite databases that live in S3. Compressed, encrypted, and fast.

```
Cold point lookup on 812MB database: 40ms (2 S3 requests, 400KB transferred)
Warm queries: 37 microseconds
Idle storage cost: ~$0.02/month per GB
```

## What is this?

turbolite is a SQLite VFS that stores your database in S3 as compressed page groups. Local disk is a cache. Reads are served from cache when warm, or via S3 byte-range GETs when cold. Writes checkpoint to S3 as immutable page group versions with an atomic manifest swap.

It's designed around the economics of object storage, not traditional server constraints — inspired by [turbopuffer](https://turbopuffer.com/blog/turbopuffer)'s approach of building directly on cloud storage primitives.

**Three features that compose transparently:**

| Feature | What it does |
|---------|-------------|
| **S3 storage** | Database pages live in S3/Tigris/R2. Local disk is just a cache. |
| **Page-level compression** | Seekable zstd, lz4, snappy, or gzip. 3-10x smaller on typical data. |
| **Page-level encryption** | AES-256-GCM per page. Composes with compression. |

Because compression and encryption happen at the page level, **all SQLite features work transparently** — FTS, R-tree indexes, JSON functions, WAL mode. Most other SQLite encryption/compression extensions operate at the file level or require custom builds. turbolite is a standard VFS — SQLite doesn't know the difference.

turbolite ships as a loadable SQLite extension (`.so`/`.dylib`/`.dll`) and as a Rust library.

## Quick Start

```python
import sqlite3

conn = sqlite3.connect("file:mydb?vfs=turbolite")
conn.load_extension("turbolite")

# Reads are transparently served from S3 — cached locally after first access
cursor = conn.execute("SELECT name FROM users WHERE id = ?", (42,))
print(cursor.fetchone())
```

```bash
# Import an existing SQLite database to S3
turbolite import local.db --bucket my-bucket --prefix my-database
```

## Performance

Benchmarked on Fly.io (iad region) against Tigris S3, 1M-row social media dataset (812MB, 51 page groups):

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

**Cold** = data evicted, B-tree interior pages + root cached. **Arctic** = everything evicted, interior pages re-fetched from S3.

## Design

turbolite is S3-native. Every design decision flows from the economics of object storage:

| S3 property | Design implication |
|-------------|-------------------|
| **PUTs are expensive** ($5/M) | Batch writes into large page groups. One 16MB PUT costs the same as one 1KB PUT. |
| **GETs are cheap** ($0.40/M) | Be generous with reads. Prefetch aggressively. |
| **Range GETs are free** | Sub-chunk reads cost the same as full object reads. Seekable zstd pays for itself. |
| **Storage is ~free** ($0.02/GB/mo) | Keep immutable page group versions. Old versions enable point-in-time restore. GC is optional. |
| **TTFB is fixed** (~20-50ms) | Latency = round trips, not bandwidth. Fewer larger requests beat many small ones. |
| **Bandwidth is cheap** (free in-region) | Don't optimize for bytes transferred. Optimize for round trips and PUT count. |

This is the same insight behind [turbopuffer](https://turbopuffer.com/blog/turbopuffer): build around cloud storage constraints instead of fighting them. S3 isn't a slow filesystem — it's a different kind of storage with different tradeoffs.

### Page Groups and Seekable Compression

Pages are batched into groups (default: 4096 pages = ~16MB uncompressed). Each group is compressed with multi-frame zstd and stored as a single S3 object. A JSON manifest tracks which S3 keys hold which page ranges.

Each group is encoded with multiple zstd frames (one per sub-chunk of ~32 pages). The manifest stores a frame table with byte offsets. On a cache miss, turbolite issues a byte-range GET for just the frame containing the needed page (~100KB) instead of the full group (~8MB). Since range GETs cost the same as full GETs, this is free granularity — read efficiency of small objects with the write efficiency of large ones.

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
[Seekable range GET] --> decompress sub-chunk --> write to cache --> return page
    |
    (concurrently)
    v
[Prefetch pool] --> fetch full group in background --> cache all pages
```

- **DiskCache**: cache file with per-page bitmap tracking
- **Group states**: None / Fetching / Present — prevents duplicate S3 requests
- **Interior page pinning**: B-tree interior pages (type 0x02, 0x05) are detected at read time and pinned across cache evictions. These pages are hit on every query — pinning them means cold queries only need leaf data from S3.

### Write Path

Writes follow a three-phase checkpoint:

1. **Local WAL**: SQLite writes to the local WAL as normal (local durability boundary)
2. **Page group PUTs**: checkpoint groups dirty pages into immutable S3 objects with versioned keys
3. **Manifest upload**: new manifest references new page group versions (remote durability boundary)

**The manifest upload is the commit point.** Until the new manifest lands, readers see the old consistent state. If the process crashes mid-checkpoint, the old manifest still points to valid groups. Half-written new groups are orphans cleaned up by GC.

This gives you:
- **Write atomicity**: manifest is the single atomic pointer. No partial state visible.
- **Crash safety**: old manifest + old groups always consistent.
- **Point-in-time restore**: old page groups still exist in S3. Restore = point at an old manifest.
- **Zero coordination**: readers and writers don't lock at the S3 level.

The gap between "WAL committed locally" and "manifest uploaded to S3" is the data-at-risk window. For zero-loss, layer WAL shipping (walrust, LiteStream) on top.

### Storage Management

Immutable page group writes mean old versions accumulate. This is cheap ($0.02/GB/month) and useful (point-in-time restore), but eventually needs cleanup.

**Garbage collection** (implemented):
- **Post-checkpoint GC** (`gc_enabled=true`): after each checkpoint, old page group versions replaced by new ones are immediately deleted from S3.
- **Full GC scan** (`TieredVfs::gc()`): lists all S3 objects under the prefix, deletes anything not referenced by the current manifest. Catches crash orphans.

**VACUUM and autovacuum** work transparently through the VFS. SQLite's VACUUM rewrites the database, producing new compact page groups at checkpoint. Old groups become orphans cleaned up by GC. Autovacuum reclaims freed pages incrementally.

**Planned**: tunable GC retention (keep N old versions for PITR), hole tracking (compact groups with >N% dead pages), CLI (`turbolite gc`).

## Tuning

### Prefetch

On a cache miss, turbolite fetches the needed sub-chunk inline (unblocking SQLite immediately) and concurrently submits the full page group to a background prefetch pool. Consecutive misses escalate prefetch aggressively via a configurable hop schedule.

**Three knobs:**

| Parameter | What it controls | Default |
|-----------|-----------------|---------|
| `prefetch_threads` | Worker threads for parallel S3 fetches | num_cpus + 1 |
| `prefetch_hops` | Fraction of total groups to fetch per consecutive miss | `0.33, 0.33` (3 hops) |
| `pages_per_group` | Pages per S3 object — larger = fewer PUTs, more bytes per fetch | 4096 |

**Hop schedule**: on the first miss, fetch 33% of groups. Second consecutive miss, another 33%. Third miss onward, fetch everything remaining. A cache hit resets the counter.

This means:
- **Point lookup**: 1 miss → fetch 1 sub-chunk inline + submit ~17 groups to prefetch. Typically resolves before the second miss.
- **Range scan**: misses escalate → by hop 3, every uncached group is being fetched in parallel.
- **Full table scan**: entire 812MB database cached in 3 hops. Wall-clock ~500ms on 8 threads.

**Recommended configs:**

| Workload | Config |
|----------|--------|
| Point lookups on small machines | `prefetch_threads=1, prefetch_hops=0.01,0.02,0.97` |
| Mixed OLTP | `prefetch_threads=4, prefetch_hops=0.33,0.33` |
| Scan-heavy analytics | `prefetch_threads=8+, prefetch_hops=0.5,0.5` |
| Bursty serverless (Lambda/Fly) | `prefetch_threads=1-2, prefetch_hops=0.1,0.2,0.7` |

**Limitation**: prefetch is per-connection. turbolite doesn't (yet) share prefetch state across connections — each new connection starts cold. The cache is shared, so a second connection benefits from pages fetched by the first, but the prefetch hop counter resets.

## Local Mode (no S3)

turbolite also works as a local compressed/encrypted VFS without S3:

```python
import sqlite3
conn = sqlite3.connect("file:mydb?vfs=turbolite_compressed")
conn.load_extension("turbolite")
```

Compression: zstd (default), lz4, snappy, gzip. Encryption: AES-256-GCM per page.

## Installation

**Python** (loadable extension):
```bash
pip install turbolite
```

**Rust** (library):
```toml
[dependencies]
turbolite = "0.1"                                              # local compressed VFS
turbolite = { version = "0.1", features = ["tiered"] }         # + S3 storage
turbolite = { version = "0.1", features = ["encryption"] }     # + encryption
```

## Comparison

| | turbolite | sql.js-httpvfs | SQLCipher | sqlite_zstd_vfs | LiteFS |
|---|---|---|---|---|---|
| S3 storage | page-level range GETs | HTTP range on whole file | no | no | FUSE proxy |
| Compression | page-level, seekable | no | no | page-level | no |
| Encryption | page-level AES-256-GCM | no | page-level | no | no |
| Prefetch | adaptive, parallel | fixed readahead | n/a | n/a | n/a |
| Cold point lookup | 40ms | ~200-500ms | n/a | n/a | n/a |
| Writes | checkpoint-to-S3 | read-only | local | local | replicated |
| Language | Rust | JS/WASM | C | C++ | Go |

## Testing

```bash
cargo test --features zstd                    # local VFS tests
cargo test --features zstd,tiered             # + S3 integration tests (31 tests)
cargo test --features zstd,encryption         # + encryption tests
```

## License

Apache-2.0
