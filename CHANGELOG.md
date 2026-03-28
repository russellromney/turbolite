# turbolite Changelog

(Formerly `sqlite-compress-encrypt-vfs`, aka `sqlces`)

## Midway: Remove Positional Strategy + turbolite_gc() + Marathon

### Remove Positional strategy
- Removed `PrefetchNeighbors` enum, `prefetch_hops` config field, `grouping_strategy` config field
- `GroupingStrategy` enum kept for serde backward compat but only `BTreeAware` is used
- Removed Positional import path, radial fan-out prefetch, `--grouping`/`--prefetch-hops` CLI flags
- `manifest.prefetch_neighbors()` replaced with `manifest.prefetch_siblings()` (returns `Vec<u64>` directly)
- ~200 lines removed across 10 files

### turbolite_gc() SQL function
- `SELECT turbolite_gc()` runs full orphan scan (list all S3 keys, diff against manifest, delete orphans)
- Returns count of deleted objects. FFI: ext.rs + ext_entry.c, same pattern as turbolite_cache_info

### Marathon: Local Disk Compaction
- Cache file truncation after VACUUM: when checkpoint detects cache file > `page_count * page_size`, truncates to match
- Integration test: insert 1000 rows, VACUUM, verify cache file shrinks

---

## Phase 35: Tiered VFS Test Refactor

Split `tests/tiered_test.rs` (5,399 lines, 49 tests) into domain-focused submodules under `tests/tiered/`.

- Entry point: `tests/tiered.rs` declaring submodules
- `helpers.rs` (179 lines): shared setup, S3 verification, unique VFS names
- `basic.rs` (1,122 lines, 13 tests): core I/O, checkpoint, manifest, cold read, caching
- `data_ops.rs` (1,100 lines, 9 tests): UPDATE/DELETE, VACUUM, rollback, BLOBs, journal modes
- `indexes.rs` (547 lines, 4 tests): index bundles, eager load, OLTP, small-PPG integrity
- `gc.rs` (282 lines, 4 tests): post-checkpoint GC, disabled GC, full scan, no-orphan safety
- `encryption.rs` (1,142 lines, 9 tests): encryption, wrong key, key rotation, add/remove
- `advanced.rs` (954 lines, 10 tests): PPG config, TTL eviction, dictionary, cache management, autovacuum

Tests can now run by domain: `cargo test --test tiered tiered::gc`

---

## Thermopylae: GC + Msgpack Manifest + Autovacuum

### Msgpack manifest
- Manifest stored as msgpack (`manifest.msgpack`) instead of JSON. Smaller, faster serialize/deserialize.
- Automigration: reads msgpack first, falls back to JSON. Always writes msgpack. Old `manifest.json` cleaned up by GC.
- `rmp-serde` dependency added.

### GC improvements
- `gc_enabled` default flipped to `true`. Orphan accumulation was a silent storage leak.
- Post-checkpoint GC is async: spawns delete on tokio runtime, doesn't block checkpoint return.
- `delete_objects_async_owned` on S3Client for fire-and-forget background deletes with error logging.

### Autovacuum verification
- Integration test confirms `PRAGMA auto_vacuum=INCREMENTAL` + `incremental_vacuum(N)` works through the tiered VFS.
- Insert/delete/vacuum/checkpoint/GC cycle stabilizes S3 object count.
- 300 unit + 39 integration tests passing.

---

## Marathon: Local Disk Compaction

Cache file truncation after VACUUM. When checkpoint detects the cache file is larger than `page_count * page_size`, it truncates the file to match. Frees disk space without requiring a fresh reader.

- Truncation runs after bitmap persist, before async GC
- Compares `file.metadata().len()` against `manifest.page_count * page_size`
- Integration test: insert 1000 rows, VACUUM, verify cache file shrinks to match new page_count

---

## Stalingrad: Cache Eviction Policies (complete)

Production cache management: size limits, observability, manual control, and smart eviction.

### a. Size-based eviction
- `max_cache_bytes: Option<u64>` on TieredConfig, `TURBOLITE_CACHE_LIMIT` env var, `turbolite_config_set('cache_limit', '512MB')`
- `current_cache_bytes` on SubChunkTracker (inside mutex, not atomic). Updated on mark_present/remove/clear.
- `evict_to_budget(limit, skip_groups)` on DiskCache: sorts evictable sub-chunks by score, evicts lowest first (O(n log n))
- Between-query trigger in VFS read path (step 3d): builds skip_groups from dirty pages + pending flush + fetching groups
- `parse_byte_size()` supports "512MB", "2GB", raw bytes, fractional ("1.5GB")

### b. Access count tracking + weighted eviction
- `access_counts: HashMap<SubChunkId, u32>` on SubChunkTracker, capped at 64
- Score = tier_bonus + recency_score + frequency_score
- tier_bonus: Data=0, Index=10 (additive: ANY Index beats ANY Data)
- recency_score: 0.0 (1hr+) to 1.0 (just accessed). Window: RECENCY_WINDOW_SECS=3600
- frequency_score: 0.0 (untouched) to 1.0 (64+ accesses)
- Access counts persisted in tracker v2 format (backward compatible with v1)

### c. Cache observability
- Hit/miss/eviction/bytes_evicted counters on DiskCache (AtomicU64)
- Peak cache size tracking (`stat_peak_cache_bytes`) updated on every group present
- Churn detection: WARN log when between-query eviction sheds >50% of cache
- `stat_last_eviction_count` for per-pass eviction tracking
- cache_info JSON includes: size_bytes, peak_bytes, groups, tiers, hits, misses, hit_rate, evictions, bytes_evicted, last_eviction_count, s3_gets_total

### d. Manual eviction controls
- `turbolite_evict_tree(names)`: comma-separated tree names, BTreeAware only
- `turbolite_evict('data'/'index'/'all')`: evict by tier, skips pending flush
- `turbolite_evict_query('SELECT ...')`: runs EQP, extracts tree names, evicts their groups

### e. Checkpoint eviction
- `evict_on_checkpoint: bool`, TURBOLITE_EVICT_ON_CHECKPOINT env, turbolite_config_set runtime toggle
- After S3 upload, evicts Data tier (interior + index remain)

### f. Speculative warm
- `turbolite_warm('SELECT ...')`: runs EQP, submits tree groups to prefetch pool (non-blocking)
- Returns JSON: {"trees_warmed": [...], "groups_submitted": N}

### g. Naming cleanup
- Renamed `TieredBenchHandle` to `TieredSharedState` (used by SQL functions + benchmarks)
- Renamed `bench_handle()` to `shared_state()`

### Infrastructure
- DRY: extracted group_page_nums(), sub_chunk_page_nums(), clear_pages_from_disk() helpers
- 306 unit tests, 3 integration tests

---

## Stalingrad (items 1-3): Cache Eviction Foundations

Between-query eviction boundary, manual tree eviction, and cache observability. The building blocks for production cache management.

### 1. SQLITE_TRACE_PROFILE + end-query signal
- `turbolite_trace_end_query()` FFI entry point, called from C trace profile callback
- AtomicBool end-query signal: set on statement completion, checked+cleared by VFS on next read
- Multiple signals collapse to one check (idempotent). Trace callback handles both SQLITE_TRACE_STMT (plan-aware prefetch) and SQLITE_TRACE_PROFILE (end-query signal)
- Reentrant guard prevents false PROFILE events from inner EQP statements
- Between-query eviction hook wired in VFS read path (step 3d), no-op until size-based eviction (item 4)
- 5 unit tests

### 2. `turbolite_evict_tree(names)` SQL function
- Evicts cached groups for named B-trees via `tree_name_to_groups` manifest lookup
- Accepts comma-separated names: `SELECT turbolite_evict_tree('audit_log, idx_audit_date')`
- Skips groups pending S3 upload (dirty page safety) and interior/pinned groups
- Works with BTreeAware grouping strategy (Positional has no tree-to-group mapping)
- FFI: `turbolite_evict_tree()` in ext.rs, C wrapper in ext_entry.c
- Integration test with BTreeAware import, pending-flush safety test

### 3. `turbolite_cache_info()` SQL function
- Returns JSON: `size_bytes`, `groups_cached`, `groups_total`, tier breakdown (pinned/index/data chunks and bytes), `s3_gets_total`
- Thread-local CString buffer for FFI safety (SQLITE_TRANSIENT copies immediately)
- Integration test: JSON structure validation, size decrease after cache clear

---

## Marne (Memory): Dirty Page Memory Optimization

Replaced `dirty_pages: HashMap<u64, Vec<u8>>` with `dirty_page_nums: HashSet<u64>`. Dirty page data lives only in the disk cache file, not duplicated in memory. Saves 64KB per dirty page (1000 dirty 64KB pages = 64MB saved).

- `write_all_at()` writes to cache file and inserts page number into HashSet (was: clone page data into HashMap + write to cache)
- `read_exact_at()` checks HashSet membership, reads from cache (was: read from HashMap)
- `sync()` snapshots HashSet (was: clone entire HashMap with page data). Interior/index page classification reads from cache at checkpoint time.
- 282 unit tests passing, no regressions

---

## Marne: Query-Plan-Aware Prefetch

The VFS knows which B-trees a query will access BEFORE the first page read. A trace callback runs EXPLAIN QUERY PLAN at the start of sqlite3_step(), parses SCAN/SEARCH + table/index names, and pushes them to a global queue. The VFS drains the queue on first cache miss and submits all planned groups to the prefetch pool in one batch. Falls back to hop schedule when queue is empty.

- `PlannedAccess` struct + global `Mutex<Vec<PlannedAccess>>` queue in `src/tiered/query_plan.rs`
- EQP parser: handles SCAN, SEARCH, COVERING INDEX, rowid lookups, joins, deduplication
- C trace callback in `ext_entry.c` via `sqlite3_trace_v2(SQLITE_TRACE_STMT)`
- VFS `read_exact_at` drains queue, resolves tree names to group IDs, submits to prefetch pool
- Config: `query_plan_prefetch: bool` (default true, no-op without extension)
- 15 unit tests

---

## Verdun (Prediction): Predictive Cross-Tree Prefetch

Learn which B-trees appear together in transactions and prefetch the full set when a partial match is detected. Pattern boundary = lock lifecycle (NONE -> SHARED/EXCLUSIVE -> NONE). K=2 trigger: after 2 distinct trees are touched, predict the rest.

### Lock session tracking (a-f)
- `LockSession` tracks B-tree touches per connection, `PredictionTable` with pair index and confidence math
- Confidence: initial 0.3, reinforce +0.25, time decay *0.95, write decay *0.7, threshold 0.5
- `AccessHistory` tracks B-tree frequency across sessions with exponential decay
- Prediction firing: on 2nd tree touch, predict remaining trees, submit groups to prefetch pool
- Write decay applied per dirty B-tree on lock release; reinforcement on correct prediction
- 25 unit tests

### Name-based prediction keys (i1-i2)
- Refactored from u64 root page IDs to String table/index names (survives VACUUM)
- `page_to_tree_name` and `tree_name_to_groups` reverse indexes in manifest
- All prediction types (LockSession, PredictionTable, AccessHistory) use String keys
- 28 tests including VACUUM resilience, table rename, drop/create same name

---

## Midway: B-Tree-Aware Page Groups

Replaces positional page grouping (pages 0-255 in group 0) with B-tree-aware packing. Pages from the same B-tree go into the same groups. Solves both prefetch efficiency (fetch 2 groups instead of 46) and write amplification (4-5 dirty groups instead of 15-20).

### B-tree map + manifest format
- `walk_all_btrees()` in `src/btree_walker.rs`: BFS from root pages, collects interior + leaf + overflow pages per B-tree
- Manifest: `group_pages: Vec<Vec<u64>>`, `btrees: HashMap<u64, BTreeManifestEntry>`, `page_index` reverse index
- `build_page_index()` builds reverse index from `group_pages`, called after deserialization

### B-tree-aware packing + read/write paths
- Import: large B-trees get own groups, small ones bin-packed
- `DiskCache::write_pages_scattered()` for non-consecutive page writes
- Read path uses `manifest.page_location(page_num).expect()` for gid and index
- Checkpoint groups dirty pages by manifest mapping, carries forward `group_pages` and `btrees`

### Correctness hardening (e2-e5)
- Prefetch worker uses `job.group_page_nums` for scattered writes
- SubChunkTracker positional mismatch fix: `write_pages_scattered` bitmap-only, per-page accurate
- Eliminated all positional mapping from DiskCache + SubChunkTracker (8 fixes, 170 tests updated)
- Restored SubChunkTracker population with manifest-aware `sub_chunk_id_for()` for tiered eviction

### Demand-driven prefetch
- Range GET first (serves page), then background prefetch of full group + siblings
- `btree_groups` reverse index: group -> sibling group_ids from same B-tree
- Fraction-based escalation via `prefetch_hops`, `prefetch_search`, and `prefetch_lookup`
- Range-GET budget per tree: caps inline range GETs, then waits for prefetch
- 236 tests passing (tiered+zstd+encryption)

---

## Tannenberg: File Size Cleanup

Split `tiered.rs` (8,758 lines) into `src/tiered/` with 13 focused modules. Split mod.rs tests (~5,270 lines) into 13 separate `test_*.rs` files using Rust's `#[path]` attribute. The `test_` prefix convention lets LLMs skip test files immediately when reading production code.

### Tiered module split
- mod.rs (208 lines), config.rs, manifest.rs, s3_client.rs, cache_tracking.rs, disk_cache.rs, encoding.rs, prefetch.rs, handle.rs, vfs.rs, import.rs, rotation.rs, bench.rs
- Public API re-exports in mod.rs, crate-internal `pub(crate) use` for submodule access

### Test file extraction
- 13 `test_*.rs` files (6,396 lines total) extracted from inline `#[cfg(test)] mod tests {}`
- Source files end with `#[cfg(test)] #[path = "test_*.rs"] mod tests;`
- 312 tests pass unchanged

---

## Kursk: Write & Checkpoint Benchmarks

Standalone `benchmark/write-bench.rs` with 7 scenarios, all passing against Tigris.

### Bug fixes
- sync page-group skip corruption: sync skipped uploading groups where all dirty pages were interior/index, causing cold readers to get zeros. Fixed by uploading every dirty group regardless of page type.
- S3 PUT counters: added `put_count`/`put_bytes` to S3Client for write benchmarking.

### Scenarios
- sustained, checkpoint-latency, incremental, update, delete, realistic (randomized bursty mix), cold-write
- Default page_size=4096, ppg=8 to force multi-group behavior at modest row counts

---

## Stalingrad: Non-Blocking Checkpoint + SyncMode

Two-phase checkpoint: fast local WAL compaction (~1ms lock) + async S3 upload (no lock). Reads and writes continue during S3 upload. Configurable per-VFS via `SyncMode`.

### Two-phase checkpoint (flush_to_s3)
- Shared `Arc<RwLock<Manifest>>` and `Arc<Mutex<HashSet<u64>>>` between handle and VFS for lock-free flush
- `flush_to_s3()` on both `TieredVfs` and `TieredSharedState`
- Uploads page groups, interior chunks, and index leaf bundles outside any SQLite lock
- `flush_lock` mutex prevents concurrent flush races on version numbers and S3 keys
- Cache eviction protects pending-upload groups from eviction (all `clear_cache*` methods)
- Benchmark: **1,133x lock reduction** (650ms blocking -> 0.6ms local + 601ms flush with no lock)

### SyncMode config
- `SyncMode::Durable` (default): `sync()` uploads to S3 during checkpoint (blocking, full durability)
- `SyncMode::LocalThenFlush`: `sync()` writes local disk cache only; user calls `flush_to_s3()` for S3 durability
- Per-VFS config field on `TieredConfig`, immutable after connection open
- Global `LOCAL_CHECKPOINT_ONLY` flag retained for benchmark use

### Durability model
- Between checkpoint and flush, data exists only in local disk cache
- Process crash: data survives (on local disk)
- Machine loss: data lost (not yet on S3)
- After `flush_to_s3()` completes, data is durable on S3

### Bug fixes
- WAL stub file creation on VFS open (SQLite silently fell back to DELETE journal mode without it)
- Index leaf pages now collected in flush path (scan dirty group pages for type 0x0A)
- Flush uses manifest snapshot consistently (no re-acquiring shared lock mid-flush)

### New files
- `src/tiered/flush.rs`: non-blocking S3 upload logic (~480 lines)
- `benchmark/write-bench.rs`: `two-phase` scenario added

---

## Inchon: Rename to turbolite

Full project rename from `sqlite-compress-encrypt-vfs` / `sqlces` to `turbolite`.

- Cargo.toml package name, all `use` paths, binary names (`sqlces` -> `turbolite`)
- Git remote URL updated to `russellromney/turbolite`
- Makefile, linker flags, .gitignore all updated
- Published to crates.io as `turbolite` 0.2.19

---

## Normandy: SQLite Loadable Extension + Language Packages

Ship turbolite as a SQLite loadable extension and language packages for Python, Node, C, Go, and Rust. Published to PyPI, npm, and crates.io at version 0.2.19.

### Loadable extension
- C shim (`src/ext_entry.c`): `sqlite3_turbolite_init` entry point using `sqlite3ext.h` macros, routes `sqlite3_vfs_register` through the extension API table
- Dual VFS registration: "turbolite" (local compressed, always) and "turbolite-s3" (tiered, when TURBOLITE_BUCKET is set)
- Fail-fast: if TURBOLITE_BUCKET is set but tiered VFS fails to initialize, returns error (no silent fallback)
- `turbolite_version()` SQL function
- Vendored `sqlite3.h`/`sqlite3ext.h` in `vendor/sqlite3/`
- `make ext` builds the .so/.dylib

### Python package (PyPI)
- sqlite-vec pattern: pure Python wrapper bundling platform-specific .so/.dylib
- `turbolite.load(conn)`: finds bundled binary, calls `conn.load_extension()`
- `turbolite.connect(path, mode="local"|"s3", bucket=..., ...)`: explicit mode selection, fail-fast if mode="s3" without bucket
- Platform-specific wheels via `setup.py` PlatformWheel override
- Published to PyPI as `turbolite` 0.2.19

### Node package (npm)
- napi-rs `Database` wrapper (better-sqlite3 compiles with `SQLITE_USE_URI=0`, can't select VFS via URI)
- `exec`, `query`, `prepare` API
- Published to npm as `turbolite` 0.2.19

### Cross-compile CI
- `.github/workflows/release.yml`: builds Python wheels and Node addons for linux-x86_64, linux-aarch64, darwin-x86_64, darwin-aarch64
- PyPI: trusted publisher (OIDC, no token)
- npm: provenance publishing (requires NPM_TOKEN secret)
- `.github/workflows/ci.yml`: cargo test, build ext, build node on push/PR

### Examples
- All 5 languages (Python, Node, Rust, C, Go) with both local and tiered examples
- Python/Node use package APIs; C/Go use FFI; Rust uses native API
- Makefile targets: `make example-<lang>` (local) and `make example-<lang>-tiered` (S3)

### Tests
- Extension loads successfully, VFS registered, idempotent reload
- `turbolite_version()` returns correct version
- Python `sqlite3.load_extension()` full CRUD roundtrip via URI `?vfs=turbolite`
- Data persists across close/reopen, multiple tables, UPDATE, DELETE, rollback
- All column types, index creation, large text/blob, unicode roundtrip
- Compressed magic on disk, uncompressed DB rejected

---

## Ypres: Encryption Key Rotation + Add/Remove Encryption

Rotate, add, or remove encryption on all S3 data without decompressing/recompressing.

### Design
- `rotate_encryption_key(config, new_key: Option<[u8; 32]>)` standalone function
- Three modes: `Some(old), Some(new)` = key rotation; `Some(old), None` = remove encryption; `None, Some(new)` = add encryption
- Seekable page groups: per-frame decrypt/re-encrypt with recalculated frame table offsets
- Non-seekable page groups, interior bundles, index bundles: whole-blob decrypt/re-encrypt
- Manifest upload is the atomic commit point. Old S3 objects GC'd after.
- Local cache cleared (ephemeral, repopulates on next open)

### Safety
- Fail-fast: validates old key by decrypting first page group before any uploads
- Post-upload verification: re-downloads and decompresses one new page group before committing manifest
- Crash-safe: old objects never overwritten, only new versioned objects created
- Atomic: manifest swap is the commit point; partial rotation leaves orphans cleaned by gc()
- Same-key guard: errors if old and new keys are identical

### Tests
- 16 unit tests: seekable/non-seekable/bundle roundtrips for rotation, add encryption, and remove encryption; frame table preservation, nonce uniqueness, wrong key rejection, same-key idempotent, empty DB, large page group (256 pages)
- 6 S3 integration tests: cold read after rotation, old key rejection, GC cleanup, 500-row data integrity, add encryption cold read, remove encryption cold read

---

## Verdun: Tiered Encryption

One key, VFS encrypts everything — S3 objects, local cache, WAL/journal, cache metadata.

### Two-tier encryption model
- **S3 path (GCM)**: AES-256-GCM with random 12-byte nonce per frame, prepended to ciphertext. Authenticated encryption with tamper detection. +28 bytes/frame overhead (negligible on ~256KB frames). Random nonces prevent catastrophic GCM nonce reuse across checkpoint re-encodes.
- **Local path (CTR)**: AES-256-CTR for cache pages, WAL/journal files, and SubChunkTracker metadata. Zero size overhead, preserves 64KB OS page alignment.

### Encryption pipeline
- Encode path: plaintext → zstd compress → GCM encrypt (S3) or CTR encrypt (local)
- Decode path: GCM decrypt → zstd decompress (S3) or CTR decrypt → read (local)
- `encryption_key: Option<[u8; 32]>` in `TieredConfig` threads through all paths

### What's encrypted
- Page groups (seekable per-frame GCM, nonce prepended per frame)
- Interior and index bundles (whole-blob GCM, random nonce)
- Local disk cache pages (CTR, nonce = page_num)
- WAL/journal passthrough files (CTR, nonce = byte offset)
- SubChunkTracker persistence file (CTR, random 16-byte nonce prefix per persist)

### Tests
- 21 encryption unit tests: roundtrip, on-disk not plaintext, wrong key rejection, nonce uniqueness, bulk ops
- 3 S3 integration tests: encrypted write + read at cache `none`, wrong key rejection, full cold start with all page types

---

## Agincourt: Index Bundles + Lazy Prefetch + Page-Size-Aware Chunking

3-7x improvement at cache level `none` through three compounding changes:

### Index bundles
- Manifest field `index_chunk_keys: HashMap<u32, String>` — page-size-aware chunking via `bundle_chunk_range()` (~32MB target per chunk)
- Index leaf pages (0x0A) detected at checkpoint, collected from dirty snapshot + cache's Index-tier sub-chunks
- Reuses `encode_interior_bundle`/`decode_interior_bundle` format
- Skip redundant page group uploads: groups where ALL dirty pages are interior (0x05/0x02) or index leaf (0x0A) are skipped
- GC includes index chunk keys in live key set
- Import path also collects and uploads index leaf bundles

### Lazy background prefetch
- VFS open spawns background thread for index bundle fetch instead of blocking
- First query serves index pages from data groups via inline range GET while background populates full cache
- Eliminates synchronous 107-144MB fetch that dominated latency at low cache levels

### Index page cache survival
- `index_pages: HashSet<u64>` tracks index pages in DiskCache
- Bitmap re-marks index pages in `clear_cache`/`clear_cache_data_only`
- `clear_cache_all` (cache level `none`) properly clears them for full cold testing

### Page-size-aware bundle chunking
- `bundle_chunk_range()` targets ~32MB uncompressed per chunk instead of fixed 32768 page range
- At 64KB pages: 512 page range per chunk (was 32768 — everything in 1 chunk for any DB under 2GB)
- 1M rows now produces 46 index chunks that interleave with data fetches

### Results (1M rows, 1.46GB, Fly iad → Tigris, 8 vCPU, 16GB RAM)
- Cache: none — point lookup: 468ms → 143ms (3.3x)
- Cache: none — profile join: 822ms → 419ms (2.0x)
- Cache: index — point lookup: 54ms → 23ms
- Cache: index — mutual friends: 27ms → 11ms
- Cache: data — point lookup: 98us

---

## Cannae: 64KB Pages + Sub-Chunk Caching Model

Fundamental reframe: optimize for S3 request count, not data size.

### 64KB pages as default
- Default `PRAGMA page_size=65536` for tiered VFS
- 500k-post dataset: 11,569 pages (vs ~104,000 at 4KB) — 9x fewer
- B-tree fan-out increases ~16x → shallower trees → fewer S3 hops
- Default group size: 256 pages per group = 16MB uncompressed
- Default sub-chunk frame: 4 pages per frame = 256KB per range GET

### Sub-chunk caching
- `SubChunkId(group_id, frame_index)` + `SubChunkTier(Pinned/Index/Data)` + `SubChunkTracker` with tiered LRU eviction
- DiskCache uses tracker alongside legacy PageBitmap for backward compat
- Read path detects page types: 0x05/0x02 → Pinned, 0x0A → Index, else Data
- Tracker persists to disk with tier info, reloads on restart
- 25 unit tests for math, tier promotion, eviction order, LRU, persistence

---

## Normandy (early): C FFI + Build Infrastructure

### C FFI layer
- `src/ffi.rs` — `extern "C"` functions for VFS registration + error handling
- `turbolite_register_compressed`, `turbolite_register_passthrough`, `turbolite_register_encrypted`, `turbolite_register_tiered`
- `turbolite_last_error` — thread-local error string
- `turbolite_clear_caches`, `turbolite_invalidate_cache`

### Build infrastructure
- `Cargo.toml`: `crate-type = ["lib", "cdylib"]`, `bundled-sqlite` feature flag
- `cbindgen.toml` + `make header` → generates `turbolite.h`
- `Makefile`: `make lib` (system SQLite), `make lib-bundled` (self-contained), `make install`

---

## Page-Group Model + Seekable Sub-Chunk Range GETs + GC

Major architectural upgrade: page groups with seekable zstd encoding, S3 byte-range GETs for point lookups, concurrent prefetch, and garbage collection.

### Architecture
- Page groups: 4096 pages per S3 object (~16MB uncompressed, ~8MB compressed)
- Seekable zstd: multi-frame encoding with per-frame byte offsets in manifest
- Inline sub-chunk range GETs: on cache miss, fetch only the ~100KB frame containing the needed page
- Concurrent full-group fetch: submit entire group to prefetch pool alongside inline range GET
- Interior page pinning: B-tree interior pages detected at read time, survive cache evictions
- Fraction-based adaptive prefetch: configurable hop schedule (default 33%/33%/remaining)

### Garbage Collection
- Post-checkpoint GC (`gc_enabled`): delete replaced page group versions after manifest upload
- Full-scan GC (`TieredVfs::gc()`): list all S3 objects, delete orphans not in manifest

---

## Tiered v2 Hardening

Bug fixes and correctness improvements. 27 tests pass against Tigris.

### Bug fixes
- Fixed `delete()` calling `destroy_s3()` unconditionally — SQLite calls delete for WAL/journal files during VACUUM and journal mode switch, which destroyed the entire S3 dataset
- Fixed `exists()` returning true for WAL/journal/SHM files — caused SQLite to enter recovery mode in DELETE journal mode
- Fixed chunk_size mismatch → silent corruption — `open()` now uses manifest's chunk_size for existing databases
- Fixed cache serving stale pages after truncation — moved page_count bounds check before cache lookup
- Fixed dirty chunks evicted during read-path — `put_chunk` now passes dirty chunk IDs to eviction
- Fixed `Manifest::empty()` hardcoding chunk_size=128

### Improvements
- Batch S3 deletes in `destroy_s3` — `DeleteObjects` in batches of 1000
- LRU rebuild on restart uses file mtime instead of `Instant::now()`
- DELETE journal mode support

---

## S3-Backed Tiered Storage

Page-level tiered storage where S3 is source of truth and local disk is a cache.

- `TieredVfs`, `TieredHandle`, `S3Client`, `DiskCache` in `src/tiered.rs`
- Extracted compression/encryption to `src/compress.rs` free functions
- `tiered` feature flag + aws-sdk-s3/tokio/crc32fast deps
- zstd dictionary support in `TieredConfig`/`TieredHandle`
- 3-retry with exponential backoff on `put_manifest()`

---

## Client-Controlled Compaction Helpers

- `compact_if_needed(path, threshold_pct)` — compact if dead_space exceeds threshold

---

## Parallel Compression in Compaction

- `rayon` optional dependency with `parallel` feature flag
- `compact_with_recompression()` with `CompactionConfig`

---

## Dictionary Compression Integration

- Pre-compiled `EncoderDictionary`/`DecoderDictionary` in `CompressedHandle`
- VFS constructors: `new_with_dict()`, `compressed_encrypted_with_dict()`
- CLI commands: `embed-dict`, `extract-dict`

---

## Foundation

- Single magic "SQLCEvfS" format with inline page index
- zstd compression (levels 1-22)
- AES-256-GCM encryption
- WAL mode support
- Byte-range locking (SQLite protocol)
- Atomic write_end for concurrent access
