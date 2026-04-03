# turbolite Roadmap

## Unification: TurboliteVfs (merge CompressedVfs + TieredVfs)
> After: Kursk (CHANGELOG) · Before: Borodino

Merge CompressedVfs (local-only) and TieredVfs (S3-backed) into a single TurboliteVfs. Works locally by default. Cloud (S3) is an optional add-on controlled by `cloud` feature flag. No tokio, no AWS deps in local-only mode. On-disk format is manifest + page groups regardless of mode. Existing CompressedVfs databases get a one-time migration tool.

### a. StorageClient abstraction + StorageBackend config

The linchpin. One enum, two variants, abstracts all I/O for page groups and manifests.

```rust
pub enum StorageBackend {
    Local,
    #[cfg(feature = "cloud")]
    S3 { bucket: String, prefix: String, endpoint_url: Option<String>, region: Option<String> },
}
```

- [ ] Add `StorageBackend` enum to config.rs
- [ ] Add `StorageClient` enum: `Local { base_dir: PathBuf }` / `S3(S3Client)`
- [ ] Implement on `StorageClient`: `get_page_group(key)`, `put_page_group(key, data)`, `delete_page_groups(keys)`, `get_manifest()`, `put_manifest(data)`, `exists()`
- [ ] Local variant: page groups stored at `{base_dir}/pg/{key}`, manifest at `{base_dir}/manifest.msgpack`
- [ ] S3 variant: delegates to existing `S3Client` methods
- [ ] Move `bucket`/`prefix` from top-level TieredConfig fields into `StorageBackend::S3`
- [ ] Default: `StorageBackend::Local`
- [ ] Tests: Local StorageClient roundtrips page groups + manifest; file-not-found returns None

### b. Make VFS constructable without S3/tokio

Remove hard dependency on S3Client and tokio runtime from construction path.

- [ ] `s3` field on VFS struct: replace `Arc<S3Client>` with `StorageClient` enum
- [ ] `prefetch_pool`: `Option<Arc<PrefetchPool>>` (None in local mode, no S3 to prefetch from)
- [ ] `runtime_handle`: gate behind `#[cfg(feature = "cloud")]`
- [ ] `TieredVfs::new()`: branch on `StorageBackend`:
  - `Local`: no S3Client, no tokio, load manifest from local `{cache_dir}/manifest.msgpack` only, no PrefetchPool, data served directly from local page groups + cache
  - `S3`: current behavior
- [ ] `load_manifest()` for Local: read local manifest, no S3 fallback
- [ ] `exists()` for Local: check local manifest file
- [ ] Handle missing manifest on first open (new database): create empty manifest locally
- [ ] Tests: construct VFS with StorageBackend::Local, no S3 creds, no tokio. Open database, create table, insert, read back. Checkpoint writes manifest locally.

### c. Local page group storage + local flush

Checkpoint in local mode writes compressed page groups to local disk.

- [ ] `flush_local_groups()` in flush.rs: reads staging logs / dirty pages, encodes page groups (reuse `encode_page_group_seekable`), writes to `{cache_dir}/pg/{gid}_v{version}` via atomic tmp+rename
- [ ] Updates local manifest with new page_group_keys pointing to local paths
- [ ] `sync()` in local mode: always LocalThenFlush path, then immediately flush locally (no deferred S3)
- [ ] OR: keep two-phase (local checkpoint + explicit `flush_local()`) for consistency
- [ ] Local GC: delete old page group files after manifest update
- [ ] `read_exact_at()` for local mode cache miss: read page group from local `pg/` directory, decode, populate cache
- [ ] Tests: write data, checkpoint, verify page group files exist in `pg/` dir. Cold open from local manifest + page groups. Delete cache file, reopen, data recovered from local page groups.

### d. Gate cloud deps behind `#[cfg(feature = "cloud")]`

- [ ] Rename feature flag `tiered` -> `cloud` in Cargo.toml
- [ ] `cloud` feature: aws-sdk-s3, aws-config, aws-smithy-runtime, tokio
- [ ] Gate `S3Client`, `PrefetchPool`, tokio runtime, WAL replication behind `#[cfg(feature = "cloud")]`
- [ ] VFS struct + Handle compiles and works without `cloud` feature
- [ ] Update `wal` and `lambda` features to depend on `cloud` instead of `tiered`
- [ ] CI: test `--features cloud,zstd` AND `--features zstd` (no cloud)
- [ ] Tests: full test suite passes with and without `cloud` feature

### e. Rename TieredVfs -> TurboliteVfs

Mechanical rename across codebase.

- [ ] `TieredVfs` -> `TurboliteVfs`
- [ ] `TieredHandle` -> `TurboliteHandle`
- [ ] `TieredConfig` -> `TurboliteConfig`
- [ ] `TieredSharedState` -> `TurboliteSharedState`
- [ ] Add backward-compat type aliases: `pub type TieredVfs = TurboliteVfs;` etc.
- [ ] `tiered::register()` -> `register()` (keep `tiered::register()` as deprecated alias)
- [ ] Update all doc comments, module-level docs, README
- [ ] `pub use` at crate root: `pub use tiered::TurboliteVfs;`

### f. Update FFI bindings

- [ ] Add `turbolite_register_local(cache_dir, ...)` -- creates TurboliteVfs with Local backend
- [ ] Rename `turbolite_register_tiered()` -> `turbolite_register_cloud()` (keep old name as alias)
- [ ] `turbolite_register_cloud()` creates TurboliteVfs with S3 backend (behind `#[cfg(feature = "cloud")]`)
- [ ] Add `turbolite_register()` unified entry point taking config JSON
- [ ] `turbolite_register_compressed()` delegates to local TurboliteVfs (or stays for CompressedVfs compat)
- [ ] Update ext.rs loadable extension entry point
- [ ] Update cbindgen header generation
- [ ] Tests: FFI roundtrip in local mode

### g. Migration tool for CompressedVfs databases

- [ ] `turbolite migrate <source.db> <dest_dir>` CLI command
- [ ] Read CompressedVfs format (SQLCEvfS header, scan page records)
- [ ] Write as TurboliteVfs local format (manifest + page groups in `pg/`)
- [ ] Handle dictionary embedding (extract from CompressedVfs header, store in config)
- [ ] Handle encryption (re-encrypt from password-derived key to raw key format)
- [ ] `CompressedVfs::migrate_to_turbolite()` programmatic API
- [ ] Tests: migrate a CompressedVfs database, open with TurboliteVfs, verify all data intact

### h. Deprecate and remove CompressedVfs

- [ ] Mark `CompressedVfs`, `CompressedHandle`, old `register()` as `#[deprecated]`
- [ ] Migrate all integration tests from CompressedVfs to TurboliteVfs local mode
- [ ] Remove CompressedVfs code from src/lib.rs (~700 lines)
- [ ] Remove `CompressedHandle` page index, shared write state, append-only format code
- [ ] Keep `compress.rs` and `dict.rs` (shared utilities)

---

## Borodino: Version Counter + Cross-Cutting Correctness
> After: Unification · Before: Stalingrad (remaining)

Blocking bugs and untested interactions discovered during Kursk stress testing. Each subsection is a specific issue with a failing test that must pass before shipping.

### a. Dual counter: manifest.version (S3 keys) + manifest.change_counter (walrust)

**Problem:** In WAL mode, SQLite's file change counter (page 0, offset 24) may not increment on every checkpoint. Using it as the S3 key version causes duplicate keys (`pg/0_v2` written twice), and GC deletes "old" versions that are actually current.

**Root cause:** turbolite and walrust need different things from the version number:
- turbolite needs a unique-per-checkpoint number for S3 key deduplication. `version + 1` is perfect.
- walrust needs to know which transactions are already in the page groups, so it can replay only WAL segments after that point. The file change counter answers this.

These are independent concerns. One number can't serve both.

**Fix: dual counter.**
- `manifest.version`: monotonic `version + 1`. Used for S3 keys (`pg/0_v{version}`). Never reused.
- `manifest.change_counter`: SQLite file change counter from page 0 at checkpoint time. Used by walrust to determine WAL replay window (`replay segments with txid > change_counter`).

**Safety:** WAL replay is always safe to over-replay (idempotent), never under-replay. If `change_counter` is stale (same value for two checkpoints), walrust replays extra segments (wasted work, not data loss). `change_counter` can never jump ahead of what's in the page groups because it's read from the same page 0 in the checkpoint.

**Implementation:**
- [ ] Add `change_counter: u64` to `Manifest` struct (serde, default 0 for backward compat)
- [ ] In `sync()` durable path: `next_version = manifest.version + 1`, read change counter from cache, store both
- [ ] In `flush_to_s3()`: `next_version = manifest_snap.version + 1` (not change counter)
- [ ] In `sync()` LocalThenFlush path: no change (version assigned at flush time)
- [ ] `page_group_key(gid, next_version)` uses `manifest.version` (already correct)
- [ ] Backward compat: old manifests with `change_counter = 0` work fine (walrust replays everything)

**Failing tests (must pass after fix):**
- [ ] `borodino_version_increments_per_checkpoint`: v1 != v2 after two checkpoints
- [ ] `borodino_gc_does_not_delete_current_version`: GC deletes v(N-1) keys, not v(N)
- [ ] `test_manifest_version_increments`: existing test, same fix
- [ ] `test_gc_disabled_preserves_old_versions`: existing test, same fix
- [ ] `test_materialize_after_multiple_checkpoints`: correct S3 key after fix
- [ ] `test_materialize_after_vfs_writes`: same

### b. Walrust uses manifest.change_counter for WAL replay

**Problem:** Somme's `materialize_to_file` and `restore_with_snapshot_source` use `manifest.version` as the snapshot version for WAL replay. After the dual counter fix, walrust must use `manifest.change_counter` instead.

**Cold start flow:**
1. Fetch manifest. `change_counter = N`.
2. `materialize_to_file()` from page groups. DB at state N.
3. walrust `restore_with_snapshot_source()` replays WAL segments with `txid > N`.
4. Checkpoint (turbolite uploads dirty pages, walrust GCs old segments).

**Implementation:**
- [ ] `materialize_to_file()` returns `manifest.change_counter` (not `manifest.version`)
- [ ] WAL recovery in `TieredVfs::new()` uses `manifest.change_counter` for replay cutoff
- [ ] WAL segment GC after checkpoint uses `manifest.change_counter`

**Tests (require `wal` feature):**
- [ ] `change_counter` survives manifest roundtrip (serialize/deserialize)
- [ ] After checkpoint, `manifest.change_counter` matches file change counter from page 0
- [ ] Cold start with WAL segments: replays correct segments based on `change_counter`
- [ ] WAL segment GC deletes segments with txid <= `change_counter`

### c. Encryption + staging log interaction

**Problem:** `write_all_at()` receives plaintext from SQLite, writes encrypted to cache (if encryption enabled), and appends to staging log. If the staging log captures encrypted data, `flush_to_s3()` would double-encrypt during encoding. If it captures plaintext, the staging file on disk is unencrypted (data at rest exposure).

**Tests:**
- [ ] Staging log with encryption enabled: flush produces correct S3 data, cold reader decrypts successfully
- [ ] Staging log bytes are encrypted on disk (not plaintext)
- [ ] Wrong encryption key on recovery VFS: staging log read fails cleanly (not silent corruption)

### d. VACUUM + LocalThenFlush interaction

**Problem:** VACUUM detection + B-tree re-walk runs in the durable sync path. In LocalThenFlush mode, the staging log captures pages during `write_all_at()` (before sync). If VACUUM fires, sync() re-walks B-trees and rebuilds `group_pages`. But the staging log has page data under the old group assignments. `flush_to_s3()` would use the re-walked manifest but the staging pages map to old groups.

**Tests:**
- [ ] LocalThenFlush: INSERT, checkpoint, VACUUM, checkpoint, flush, cold reader sees correct data
- [ ] Staging log pages are assigned to correct groups after VACUUM re-walk
- [ ] VACUUM detection fires in LocalThenFlush mode (not skipped)

### e. Compaction between checkpoint and flush

**Problem:** `turbolite_compact()` repacks page groups with new group IDs. If called between a LocalThenFlush checkpoint and flush, the staging log references group assignments that compaction just invalidated.

**Tests:**
- [ ] LocalThenFlush: checkpoint, compact, flush. Flush uses pre-compaction group assignments (staging is self-contained).
- [ ] LocalThenFlush: checkpoint, compact, checkpoint, flush. Second staging log has post-compaction assignments.
- [ ] Compaction after flush (no pending staging): works as before

### f. Cache eviction under memory pressure with pending staging

**Problem:** Pending pages are protected from eviction, but only tested with unlimited cache. With `max_cache_bytes` set low and heavy read load between checkpoint and flush, does the protection hold?

**Tests:**
- [ ] `max_cache_bytes=1MB`, write 5MB, checkpoint (LocalThenFlush), read different data (triggers eviction), flush. Pending pages survive eviction, flush succeeds.
- [ ] Same but with `clear_cache()` call between checkpoint and flush. Pending pages survive.

### g. Multiple databases on same VFS with LocalThenFlush

**Problem:** Two db files sharing the same VFS + staging_dir. Staging logs use a shared `staging_seq` counter. Do logs stay isolated? Does flush upload the right data for each db?

**Tests:**
- [ ] Two databases on same VFS: each checkpoints with LocalThenFlush, each flushes independently, cold readers see correct data for each db
- [ ] Staging log filenames don't collide (different seq numbers)

### h. Tokio runtime contention under parallel tests

**Problem:** 90 parallel integration tests each create their own tokio runtime + S3 client + prefetch pool. Under heavy parallel load, 3 tests fail intermittently (cache_truncation_after_vacuum, dictionary_roundtrip, custom_pages_per_group).

**Tests/fix:**
- [ ] Investigate: are failures from S3 rate limiting, tokio thread exhaustion, or file descriptor limits?
- [ ] If tokio contention: consider shared runtime across VFS instances in test harness
- [ ] If S3 rate limiting: add retry/backoff to test assertions, or reduce parallelism for S3-heavy tests

---

## Stalingrad (remaining): Query Cost Estimation
> After: Austerlitz (CHANGELOG) · Before: Jena

Diagnostic tools, not blocking production use. Build when needed.

- [ ] `turbolite_query_cost('SELECT ...')` -- EQP + manifest tree sizes -> upper bound cache cost per tree
- [ ] `turbolite_analyze_query('SELECT ...', cache_level)` -- run query at specified cache temp, measure actual vs predicted

---

## Jena: Interior Page Introspection for Precise Prefetch
> After: Stalingrad · Before: Rosetta

The B-tree structure is fully known from interior pages (cached/pinned). By extracting child pointers at checkpoint and persisting them in the manifest, we can predict exact leaf pages for any query without guessing. Replaces the hop schedule heuristic with direct structural knowledge.

**Why it works:** Interior pages are always cached (pinned on open). They contain child pointers to leaf pages. Parsing all interior pages costs ~100us (15 pages, ~4500 cells at 1M rows). Maps live on `TieredHandle` (per-connection), built from local cache (the authority). Manifest persists a snapshot for cold start.

**Freshness:** Maps rebuild when interior pages are written (page splits/merges). `detect_interior_page` already identifies page types 0x02/0x05 on every write. When an interior page write is detected, increment a counter. Every N interior writes (default 1, configurable), rebuild maps from local cache. Data-only writes (the common case) have zero overhead. Checkpoint serializes current maps to manifest for future cold readers.

**What it solves:**
- SEARCH: predict exact leaf group, 1 range GET (18KB) instead of 12 GETs (9.4MB)
- Profile: parse index leaf to find exact data groups (5MB instead of 67MB)
- Joins: pipeline prefetch across tables, overlap S3 I/O
- Replaces hop schedule for BTreeAware (hop schedule becomes Positional-only fallback)

### a. Child pointer maps

Maps live on `TieredHandle`. Built from cached interior pages on connection open, rebuilt on interior page writes, persisted to manifest at checkpoint.

- [ ] `InteriorMap` struct: `child_to_parent: HashMap<u64, u64>`, `interior_children: HashMap<u64, Vec<u64>>`
- [ ] `rebuild_interior_map(cache) -> InteriorMap`: parse each cached interior page's cells (4-byte child pointer per cell + rightmost pointer from page header bytes 8..12)
- [ ] Connection open: call `rebuild_interior_map` from pinned interior pages
- [ ] Interior page write: detect in `write_all_at` via page type check, increment counter, rebuild every N interior writes
- [ ] Checkpoint: serialize current `InteriorMap` to manifest `interior_map` field
- [ ] Cold start: deserialize from manifest (skip rebuild if present)
- [ ] Tests: roundtrip through manifest serde, correct parent/child relationships, rebuild after simulated page split, empty for Positional, survives VACUUM + rebuild

### b. Sibling prefetch ("cheater prefetch")

On leaf miss, look up parent interior page, prefetch sibling leaf groups. Replaces hop schedule fraction math with exact structural knowledge.

- [ ] On cache miss for leaf page P: look up `child_to_parent[P]` to find parent interior page
- [ ] Get `interior_children[parent]` to find all siblings, find P's index
- [ ] For SCAN (from EQP): prefetch ALL sibling groups
- [ ] For SEARCH: prefetch 0-1 siblings (conservative, most of the time the one leaf is enough)
- [ ] For unknown (no EQP info): prefetch next 2 siblings (minimal speculation)
- [ ] Wire into `read_exact_at` replacing `trigger_prefetch` for BTreeAware
- [ ] `trigger_prefetch` becomes Positional-only fallback
- [ ] Remove `consecutive_misses` tracking for BTreeAware (no longer needed)
- [ ] Tests: sibling prediction matches B-tree structure, SCAN prefetches all, SEARCH prefetches 0-1, multi-level B-tree (interior children that are also interior pages), Positional fallback still works
- [ ] Benchmark: compare v6 hop schedule vs sibling prefetch on 1M posts (expect post+user drops from 12 GETs to 1-2)

### c. Exact leaf prediction for SEARCH

Parse key boundaries from interior cells at checkpoint. Given a search key, binary search interior pages to find exact leaf group before SQLite asks.

- [ ] Extend checkpoint extraction: parse key data from interior cells (varint record header + column values), not just child pointers
- [ ] `predict_leaf(interior_map, tree_name, search_key) -> Option<(u64, u32)>` returns (group_id, frame_index)
- [ ] Key comparison: BINARY collation (memcmp), NOCASE (case-fold before compare)
- [ ] Composite key support: multi-column indexes with prefix matching
- [ ] Bench path: pass params through `push_query_plan()`, call `predict_leaf` before first read
- [ ] Extension path: `sqlite3_expanded_sql(stmt)` in trace callback, parse integer/string literals
- [ ] Submit predicted group to prefetch pool (or issue sub-chunk range GET directly)
- [ ] Tests: single-column integer, composite key, NOCASE, key at cell boundary, rightmost child pointer, empty index, single-page index (no interior pages)
- [ ] Benchmark: SEARCH latency with/without prediction on 1M posts (Express + Tigris)

### d. Cross-tree leaf chasing

When a leaf page arrives from S3, parse its cells to extract rowids/foreign keys. Map to groups in the next table in the join plan. Prefetch while SQLite processes current results.

- [ ] SQLite record format parser: varint header length, column type codes, integer/string/blob extraction
- [ ] For table B-tree leaves (0x0D): extract rowid from cell header
- [ ] For index B-tree leaves (0x0A): extract indexed column values from record payload
- [ ] On prefetch completion callback: parse arrived leaf, extract keys, map to target groups via `page_location()`, submit to prefetch pool
- [ ] Join pipeline: EQP gives join order, each leaf arrival triggers prefetch for next table
- [ ] Start with integer rowids only (covers profile query: idx_posts_user leaf -> post rowids -> posts data groups)
- [ ] Tests: parse leaf cells for integer PK, composite index, overflow pages (payload > page), string keys
- [ ] Benchmark: profile query with leaf chasing vs without (expect 53 GETs / 67MB -> ~6 GETs / 5MB)

### e. Overflow chain prefetch

When a leaf page arrives and contains overflow pointers (payload > maxLocal), prefetch the overflow chain proactively instead of blocking on each link. Subframe range GETs already handle small overflow within a group, but multi-MB TEXT/BLOB values with chains spanning multiple page groups cause sequential blocking faults.

Inspired by sqlite-prefetch's overflow cascading (https://github.com/wjordan/sqlite-prefetch).

- [ ] On leaf page (0x0D) fetch completion: parse cells, detect overflow (payload > maxLocal), extract first overflow page number
- [ ] Map overflow page to group via `page_location()`, submit group to prefetch pool
- [ ] On overflow page arrival: read next-page pointer (first 4 bytes), cascade to next group
- [ ] Repeat until next-page pointer is 0 (end of chain)
- [ ] Cap cascade depth (e.g., 64) to bound runaway chains
- [ ] Tests: single overflow page, multi-group chain, chain within same group (no-op), cap enforced, no overflow (common case, zero overhead)
- [ ] Benchmark: table with 1MB+ TEXT values, with/without overflow prefetch

### f. Multi-level interior group lookahead

For very large databases where interior pages span multiple page groups, prefetch the next interior sibling group before SQLite descends into it. Avoids a blocking fault when the current interior group's children are exhausted and SQLite needs the next interior page.

Inspired by sqlite-prefetch's multi-level lookahead (https://github.com/wjordan/sqlite-prefetch).

- [ ] Track remaining sibling groups under current parent interior page during scan
- [ ] When remaining sibling count drops below threshold (e.g., 5 groups), prefetch the next interior sibling's group from the parent level
- [ ] Only relevant when interior pages span multiple groups (very large databases, 10M+ rows)
- [ ] No-op for databases where all interior pages fit in group 0 (the common case today)
- [ ] Tests: synthetic multi-group interior layout, lookahead triggers at threshold, no-op for small databases

---

## Rosetta: Value-Partitioned Index Access
> After: Jena · Before: (future)

Double-store index leaf pages in S3, organized by key value range instead of page number. SEARCH queries skip B-tree traversal entirely: the VFS maps the search key to the right partition and does one range GET. Storage cost is negligible (Tigris $0.02/GB). Normal B-tree groups remain for SCANs and general access.

**How it works:** At import, for each index with enough leaf pages, walk the B-tree in key order, group leaf pages into equal-depth partitions (~256 pages each), store as one seekable S3 object per index. Convert partition boundary keys to a normalized byte format (sort-order-preserving). At query time, the engine passes the normalized search key; the VFS binary-searches boundaries and range-GETs exactly one frame.

**Depends on:** Jena's SQLite record format parser (Jena d) for extracting key values from leaf cells. Jena's interior introspection is complementary, not replaced.

### a. Normalized key bytes

Sort-order-preserving byte encoding so partition boundaries are memcmp-comparable. Handles all SQLite column types.

- [ ] `normalize_key(cell_payload, col_types) -> Vec<u8>`: parse SQLite record format, emit normalized bytes
- [ ] Encoding: NULL `0x00`, INTEGER `0x01` + 8-byte BE with sign bit flipped, REAL `0x02` + 8-byte IEEE 754 with sign manipulation, TEXT `0x03` + raw bytes + `0x00`, BLOB `0x04` + raw bytes
- [ ] Composite indexes: concatenate normalized bytes per column (memcmp on result gives correct multi-column sort)
- [ ] `compare_normalized(a, b) -> Ordering`: simple memcmp wrapper
- [ ] Tests: single-column INTEGER (positive, negative, zero, i64 extremes), TEXT (ASCII, UTF-8, empty), REAL, NULL ordering, composite key (INT, TEXT), round-trip encode/compare matches SQLite's own ordering

### b. Partition builder (import path)

At import time, build value-partitioned copies for qualifying indexes.

- [ ] Threshold: only partition indexes with > 1 page group worth of leaf pages (small indexes don't benefit)
- [ ] Walk each qualifying index B-tree in key order (leaf pages left-to-right via sibling pointers or interior page traversal)
- [ ] Group leaf pages into equal-depth partitions of N pages each (N = pages_per_group)
- [ ] For each partition: extract + normalize the first key from the first leaf page (the min boundary)
- [ ] Encode all partitions as one seekable S3 object per index: `{prefix}/vp/{index_root_page}_v{version}`
- [ ] Seekable frame table: one frame per partition (reuse existing seekable encode infrastructure)
- [ ] Manifest fields: `value_partitions: HashMap<u64, ValuePartition>` where key is index root page
- [ ] `ValuePartition { s3_key: String, boundaries: Vec<Vec<u8>>, frame_table: Vec<FrameEntry>, leaf_page_nums: Vec<Vec<u64>> }`
- [ ] `boundaries[i]` = normalized min key of partition i; binary search finds the right frame
- [ ] Wire into `import_sqlite_file()` after normal page group upload
- [ ] Tests: 1000-page index partitioned into 4 partitions, boundary keys correct, seekable object decodable, small index skipped, composite index boundaries correct

### c. Partition lookup (read path)

VFS uses value partitions for SEARCH queries when available.

- [ ] Extend `PlannedAccess` with optional `search_key: Option<Vec<u8>>` (normalized key bytes from engine)
- [ ] `push_planned_accesses`: if access is SEARCH and value partition exists for the index, binary-search `boundaries` to find target frame index
- [ ] Issue range GET for that single frame of the partition's seekable S3 object
- [ ] Decode frame, write pages to cache (scattered writes, same as normal prefetch)
- [ ] Pages land in cache by their real page numbers; SQLite reads them normally
- [ ] Fallback: if no value partition exists, or search_key is None, use normal B-tree access (Jena or hop schedule)
- [ ] Tests: SEARCH hits correct partition, value at partition boundary, value before first partition, value after last partition, fallback when no partition exists, fallback when key is None

### d. Engine integration

Engine normalizes bound values and passes them to the VFS.

- [ ] Engine extracts bound parameter values from prepared statement (sqlite3_bind_* values)
- [ ] `normalize_query_key(value, col_affinity) -> Vec<u8>`: normalize a Rust value to the same format as partition boundaries
- [ ] For composite indexes: normalize each column, concatenate
- [ ] Pass normalized key via `PlannedAccess.search_key` alongside EQP info
- [ ] Tests: integer lookup, string lookup, composite key lookup, NULL handling, type mismatch (string in integer column)

### e. Staleness and rebuild

Value partitions are read-only, built at import. Handle staleness gracefully.

- [ ] After writes modify an index's pages, mark that index's value partition as stale (don't use it for lookups)
- [ ] Staleness detection: compare manifest version at partition build time vs current manifest version, or track dirty index root pages
- [ ] `rebuild_value_partitions(config) -> io::Result<()>`: CLI/API to rebuild partitions from current S3 data (download page groups, re-sort, re-upload partitioned copies)
- [ ] Wire into compaction (Phase Midway g): when groups are repacked, rebuild value partitions too
- [ ] Tests: stale partition falls back to B-tree, rebuild produces correct partitions, rebuild after INSERT/DELETE

### f. Benchmark

- [ ] Add `--value-partitions` flag to tiered-bench
- [ ] Compare at `interior` and `none` cache levels with/without value partitions
- [ ] Key metrics: S3 GETs per query, bytes fetched, p50/p99 latency
- [ ] Expected wins: Q4 (mutual) and Q5 (idx-filter) see 2-4 fewer GETs; Q6 (scan) unchanged
- [ ] Report partition build time and S3 storage overhead

---

## Future

### mmap cache
- [ ] `mmap` the cache file instead of `pread` for reads
- [ ] Keep `pwrite` for cache population
- [ ] `madvise(MADV_RANDOM)`
- [ ] Handle cache file growth: `mremap` on Linux, re-map on macOS
- [ ] Benchmark: warm lookup latency mmap vs pread (expect ~10-50us to ~1-5us)

### CLI subcommands
- [ ] `turbolite bench` -- move tiered-bench into CLI subcommand
- [ ] `turbolite gc --bucket X --prefix Y` -- one-shot GC
- [ ] `turbolite import --bucket X --prefix Y --db local.db`
- [ ] `turbolite info --bucket X --prefix Y` -- print manifest summary

### Bidirectional prefetch
- Track access direction, prefetch backward for DESC queries

### Application-level fetch API
- `vfs.fetch_all()` -- background hydration
- `vfs.fetch_range(start, end)` -- contiguous range fetch

### Hole tracking
- Manifest tracks freed pages per group
- Groups with >N% dead pages are compaction candidates

### turbolite_recommend()
- [ ] `turbolite_recommend()` SQL function: analyzes connection's access history, returns JSON with:
  - Recommended cache_limit based on peak working set
  - Recommended prefetch schedules based on observed index-lookup vs table-scan ratio
  - Hottest/coldest trees by access frequency
  - Specific suggestions ("evict_tree('audit_log') would free 180MB")
- [ ] Track peak working set, per-tree access counts, scan vs search ratio over connection lifetime

### Multi-writer coordination
- [ ] Distributed locks for concurrent writers (if needed)
