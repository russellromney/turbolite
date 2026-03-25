# turbolite Roadmap

## Kursk: Write & Checkpoint Benchmarks (done)
> After: Ypres · Before: Tannenberg

Standalone `benchmark/write-bench.rs` binary with 7 scenarios, all passing against Tigris.
Default page_size=4096, ppg=8 to force multi-group behavior at modest row counts.

### Bug found and fixed: sync page-group skip corruption
Sync skipped uploading page groups where all dirty pages were interior/index (0x05/0x02/0x0A),
assuming bundles would serve them. But cold readers could read those pages before background
bundle loading completed, getting zeros. Fixed by removing the skip optimization: every dirty
page group is now uploaded regardless of page type. Regression test: `test_small_ppg_index_integrity`.

### Bug found and fixed: S3 PUT counters missing
S3Client only had GET counters (`fetch_count`/`fetch_bytes`). Added `put_count`/`put_bytes`
and `TieredBenchHandle::s3_put_counters()` for write benchmarking.

### Scenarios implemented
- `sustained`: INSERT with auto-checkpoints (low threshold=20 pages), tracks S3 PUTs inline
- `checkpoint-latency`: Seed DB, dirty N pages, checkpoint, measure. Varies 1-500 dirty pages
- `incremental`: Write-checkpoint cycles with cold reader verification each cycle
- `update`: UPDATE 10% of existing rows (scattered), cold verify
- `delete`: DELETE 50% of rows, cold verify
- `realistic`: Randomized bursty mix (30% INSERT, 25% UPDATE, 10% DELETE, 15% UPSERT, 20% SELECT)
- `cold-write`: First write on cold DB (102ms, 2 S3 GETs) vs warm (6ms, 0 GETs)

---

## Tannenberg: File Size Cleanup
> After: Kursk · Before: Marne

`tiered.rs` is 8,758 lines and `lib.rs` is 2,595 lines. Both far exceed the 1,000-line limit. Split by domain into focused modules. Pure refactor, no behavior changes.

### a. Split `tiered.rs` (8,758 lines) into `src/tiered/`
- [ ] `mod.rs` -- module declarations, public re-exports, constants, coordinate math helpers
- [ ] `config.rs` -- `TieredConfig`, `GroupState`, defaults
- [ ] `manifest.rs` -- `Manifest`, `PageLocation`, `BTreeManifestEntry`, `FrameEntry`, index building
- [ ] `s3_client.rs` -- `S3Client`, all AWS SDK integration (GET/PUT/DELETE, manifest ops, key generation)
- [ ] `cache_tracking.rs` -- `PageBitmap`, `SubChunkTracker`, `SubChunkId`, `SubChunkTier`, tiered LRU eviction
- [ ] `disk_cache.rs` -- `DiskCache`, page I/O, group state management, eviction
- [ ] `encoding.rs` -- `encode_page_group()`, `encode_page_group_seekable()`, `decode_*`, `encode_interior_bundle()`, `decode_interior_bundle()`
- [ ] `prefetch.rs` -- `PrefetchPool`, `PrefetchJob`, background worker threads
- [ ] `handle.rs` -- `TieredHandle` struct, initialization, checkpoint, `DatabaseHandle` trait impl
- [ ] `vfs.rs` -- `TieredVfs` struct, `Vfs` trait impl, `register()`
- [ ] `import.rs` -- `import_sqlite_file()` with B-tree walking and page packing
- [ ] `rotation.rs` -- `rotate_encryption_key()` with verification and GC
- [ ] `bench.rs` -- `TieredBenchHandle`

### b. Split `lib.rs` (2,595 lines) into focused modules
- [ ] `lib.rs` -- module declarations, public re-exports only
- [ ] `locks.rs` -- `InProcessLocks`, `SlotState`, `SHARED_FILE_CACHE`, lock/unlock functions, debug tracing
- [ ] `file_format.rs` -- `FileHeader`, `PageIndex`, magic bytes, constants
- [ ] `compressed_handle.rs` -- `CompressedHandle` struct, page ops, `DatabaseHandle` trait impl, `FileWalIndex`
- [ ] `compressed_vfs.rs` -- `CompressedVfs`, `Vfs` trait impl
- [ ] `maintenance.rs` -- `inspect_database()`, `compact()`, `compact_with_recompression()`, `CompactionConfig`

### c. Verify
- [ ] `cargo test` passes with no changes to public API
- [ ] `make ext` builds successfully
- [ ] All `use turbolite::` paths in bin/, tests/, examples/ still compile

---

## Marne: Dirty Page Memory Optimization
> After: Tannenberg · Before: Thermopylae

`write_all_at()` stores a full page copy in `dirty_pages: HashMap<u64, Vec<u8>>` AND writes it to the cache file. The HashMap copy is only needed at checkpoint to know which groups to re-encode — but the data is already in the cache. Holding it twice wastes memory: 1000 dirty 64KB pages = 64MB in the HashMap alone.

### Fix
- [ ] Replace `dirty_pages: HashMap<u64, Vec<u8>>` with `dirty_page_nums: HashSet<u64>` (8 bytes per page instead of 64KB)
- [ ] At checkpoint, read dirty pages back from cache file via `cache.read_page()` (microsecond pread, trivial vs S3 PUT)
- [ ] Remove the `dirty_snapshot.clone()` in `sync()` — just clone the HashSet
- [ ] Update `read_exact_at()` to check cache instead of HashMap for dirty page reads (cache is already up-to-date from `write_all_at`)
- [ ] Test: write 1000 pages, verify memory usage doesn't scale with page count
- [ ] Test: checkpoint after dirty page optimization still produces correct S3 data

---

## Thermopylae: Tunable GC + Autovacuum Integration
> After: Marne · Before: Marathon

### a. Tunable GC policy
- [ ] `gc_keep_versions: u32` — number of old page group versions to retain (default 0 = delete all). Enables point-in-time restore window.
- [ ] `gc_max_age_secs: u64` — delete old versions older than N seconds. Alternative to version count.
- [ ] Combine with existing `gc_enabled` flag: `gc_enabled=true, gc_keep_versions=5` keeps last 5 versions.

### b. Autovacuum-triggered GC
- [ ] Hook GC into SQLite's autovacuum: after incremental autovacuum frees pages and checkpoint flushes to S3, automatically GC orphaned page groups.
- [ ] `PRAGMA auto_vacuum=INCREMENTAL` + periodic `PRAGMA incremental_vacuum(N)` should "just work" through the VFS — verify with integration test.
- [ ] Test: enable autovacuum, insert/delete cycles, verify S3 object count stabilizes.

### c. CLI
- [ ] `turbolite gc --bucket X --prefix Y` — one-shot full GC scan
- [ ] `turbolite gc --dry-run` — list orphans without deleting

### d. Msgpack manifest
- [ ] Replace JSON manifest with msgpack (`rmp-serde`). Smaller, faster serialize/deserialize.
- [ ] S3 key: `manifest.msgpack` (content type `application/msgpack`)
- [ ] Automigrate: `get_manifest` tries msgpack first, falls back to JSON. Next `put_manifest` writes msgpack. Old `manifest.json` cleaned up by GC.
- [ ] `SubChunkTracker` local persistence can stay JSON (local-only, not worth changing)

---

## Marathon: Local Disk Compaction
> After: Thermopylae · Before: Midway (B-Tree-Aware Page Groups)

The local cache file is a sparse file sized to `page_count * page_size`. After VACUUM reduces page_count, a fresh reader creates a smaller cache, but the existing cache doesn't shrink in-place.

- [ ] On checkpoint, if manifest page_count decreased, truncate cache file to match
- [ ] Add `TieredVfs::compact_cache()` — shrink cache file to current manifest size
- [ ] Test: VACUUM → checkpoint → verify cache file size matches new page_count

Note: this is a minor optimization. S3 is the source of truth, local disk is ephemeral cache. A restart naturally right-sizes the cache.

---

## Midway: B-Tree-Aware Page Groups
> After: Marathon · Before: Gallipoli

Replaces the old Midway (two-layer index deltas) and Stalingrad (write amplification) phases. B-tree-aware packing solves both problems as one feature: the manifest becomes a structural map of the database, page groups are packed by B-tree, prefetch becomes demand-driven instead of speculative, and write amplification drops dramatically.

Currently, page groups are assigned by page number order (pages 0-255 in group 0, 256-511 in group 1, etc.). SQLite doesn't allocate pages by B-tree contiguously, so a single index's pages are scattered across dozens of groups. This causes two problems: (1) prefetching an index means fetching pages from many groups, and (2) a dirty index page dirties a group that also contains unrelated pages.

### Design

**B-tree map in manifest.** At import, walk each B-tree from its root page (read `sqlite_master` for root page numbers) to enumerate all pages per B-tree. Store the mapping:

```
btrees: {
  5: { name: "idx_posts_user", type: "index", groups: [12, 13] },
  2: { name: "posts", type: "table", groups: [20, 21, ..., 35] },
}
groups: {
  12: { s3_key: "pg/12_v1", pages: [100, 200, 350, 500, ...] },
}
```

On manifest load, build in-memory reverse index: `page_num -> (group_id, position)`. For 23K pages, ~370KB. Trivial.

**B-tree-aware packing.** Pages from the same B-tree go into the same groups. All of `idx_posts_user` into groups 12-13, all of `posts` data into groups 20-35. Small B-trees (< ~50 pages) can share a group. Leave slack per group (~200 of 256 slots) so writes have room.

**Demand-driven prefetch.** When SQLite reads a root page, look up that B-tree's groups and prefetch them. A point lookup on `posts` fetches 2 groups (12MB) for `idx_posts_user`, not all 3800 index pages (144MB). The relevant index is fully cached after 1-2 S3 GETs (~3-5ms on S3 Express).

**Write amplification solved.** INSERT 100 rows into `posts` dirties pages in `posts` data groups + `idx_posts_user` group + `idx_posts_created` group = 4-5 groups. Currently those pages are scattered across 15-20 groups. ~4x less write amplification, and the improvement scales with index count.

**Checkpoint rebuilds the map.** Re-walk B-trees from `sqlite_master` (microsecond pread of page headers from cache). New pages go into their B-tree's last group (if slack available) or a new group. Freed pages become dead space, cleaned up by compaction.

### Economics (1M rows, 1.46GB, S3 Express)

Current prefetch: 3800 index pages across 46 chunks (144MB). With B-tree packing: 300 pages in 2 groups (12MB) for the relevant index. 12x less bandwidth, prefetch completes in ~5ms instead of hundreds of ms.

Current write amplification: INSERT touching 3 indexes dirties 15-20 groups (240-320MB uploaded). With B-tree packing: 4-5 groups (64-80MB). 4x improvement.

The gap between "interior" and "index" cache levels nearly disappears. With targeted prefetch, index pages for the active query arrive in single-digit ms. Combined with S3 Express, sub-50ms complex joins from cold storage become realistic.

### Implementation

#### a. B-tree map construction
- [x] `src/btree_walker.rs`: `walk_all_btrees()` entry point parses sqlite_master, walks each B-tree via BFS
- [x] Collects interior + leaf + overflow pages per B-tree, follows overflow chains
- [x] Handles page 0 (sqlite_master root with 100-byte header offset), freelist pages go to `unowned_pages`
- [x] Returns `BTreeWalkResult { btrees: HashMap<u64, BTreeEntry>, unowned_pages: Vec<u64> }`
- [x] 5 tests passing (simple DB, multi-level B-tree, overflow pages, varint parsing, serial type sizes)
- Known limitations (non-critical, cause slightly wasteful packing but no corruption):
  - Freelist pages (trunk/leaf) land in `unowned_pages` instead of being tracked separately
  - Lock-byte page (1GB boundary) not excluded from `unowned_pages`
  - Assumes `usable_size == page_size` (no reserved bytes); affects overflow threshold for PRAGMA page_reserved > 0
  - Pointer map pages (auto-vacuum DBs) not detected

#### b. Manifest format change
- [x] `PageLocation { group_id: u64, index: u32 }` for reverse index entries
- [x] `BTreeManifestEntry { name, obj_type, group_ids }` for B-tree metadata
- [x] New Manifest fields: `group_pages: Vec<Vec<u64>>`, `btrees: HashMap<u64, BTreeManifestEntry>`, `page_index: HashMap<u64, PageLocation>` (skip serialize)
- [x] `build_page_index()`: builds reverse index from `group_pages`
- [x] `page_location(page_num)` helper
- [x] `build_page_index()` called after deserialization in `get_manifest_async`
- [x] All Manifest construction sites updated (checkpoint, import, tests)

#### c. B-tree-aware packing (import)
- [x] Import calls `walk_all_btrees()` to discover page ownership
- [x] Large B-trees (>= ppg/4 pages) get their own groups, chunked to ppg
- [x] Small B-trees + unowned pages bin-packed into shared groups
- [x] Builds `group_pages` and `btrees` manifest entries
- [x] Seekable zstd encoding works unchanged (position is per-group, not per-page-number)
- [x] `DiskCache::write_pages_scattered()` added for non-consecutive page writes

#### d. Read path with explicit mapping
- [x] `read_exact_at` uses `manifest.page_location(page_num).expect()` for gid and index
- [x] Seekable path: `group_pages[gid]` for scattered cache writes after sub-chunk decode
- [x] Full group download path: `write_pages_scattered` with `group_pages[gid]`
- [x] All legacy positional fallbacks removed (no backward compat with old manifests)
- [x] `decode_and_cache_group_static` takes `group_page_nums: &[u64]` instead of positional params

#### e. Checkpoint with B-tree awareness
- [x] Dirty pages grouped by `page_location().expect().group_id`
- [x] Group re-encoding uses `group_pages[gid]` for page iteration and S3 merge
- [x] Interior page marking gracefully handles pages not yet in manifest
- [x] Carry forward `group_pages` and `btrees` to new manifest
- [x] New pages assigned to groups via `assign_new_pages_to_groups()` before dirty grouping
- [x] `read_exact_at` checks dirty pages + bounds BEFORE `page_location()` (prevents panic on new pages)
- [x] `detect_interior_page` graceful when page not in manifest
- [x] 6 regression tests for new-page assignment (basic, fill-last-group, overflow, empty, no-dupes, roundtrip)
- [ ] Re-walk B-trees at checkpoint to update mapping for new/moved pages

#### e2. Prefetch worker (Phase Midway)
- [x] Prefetch worker uses `job.group_page_nums` for scattered writes (no legacy path)
- [x] Seekable decode passes `group_page_nums.len()` as group size
- [x] Page type scanning uses explicit page numbers from `group_page_nums`
- [x] `gp_for()` and all submit callers `.expect()` group existence

#### e3. Corruption fix (SubChunkTracker positional mismatch)
- [x] Root cause: `SubChunkTracker::sub_chunk_for_page()` uses positional division, not B-tree mapping
  - Writing page X marks positional sub-chunk as present; unwritten page Y in same sub-chunk returns false cache hit (zeros)
- [x] Fix: `write_pages_scattered` no longer marks SubChunkTracker (bitmap-only, per-page accurate)
- [x] Fix: `write_pages_scattered` only marks bitmap for pages with sufficient data (no early-break desync)
- [x] Fix: `DiskCache::ensure_group_capacity()` called at VFS open when B-tree groups exceed positional formula
- [x] 10 regression tests: tracker pollution, bitmap-only accuracy, partial data, empty data, data integrity, group capacity

#### e4. Eliminate all positional mapping from DiskCache + SubChunkTracker
Phase e3 fixed `write_pages_scattered` but left deeper positional mapping bugs:

- [x] `evict_group(gid)` clears bitmap at `[gid * ppg, gid * ppg + ppg)` (positional), not the actual pages in the group. Fixed: uses `group_pages[gid]` to clear correct pages.
- [x] `clear_cache` family marks group 0 pages as `0..ppg` (positional), not `group_pages[0]`. Fixed: all 3 variants use `group_pages[0]`.
- [x] `DiskCache::is_present` checks SubChunkTracker first (positional `page_num / ppg`), falls back to bitmap. Fixed: bitmap-only.
- [x] `mark_interior_group` / `mark_index_page` use `sub_chunk_for_page(page_num)` (positional). Fixed: accept `(gid, index_in_group)`, use `sub_chunk_id_for()`.
- [x] `detect_interior_page` passes manifest-aware `loc.group_id` and `loc.index` to marking functions.
- [x] `touch_group` uses actual page count from `group_pages` (not positional ppg max).
- [x] Group state initialization in `DiskCache::new()` uses `group_pages[gid]` instead of positional range.
- [x] Added `group_pages: RwLock<Vec<Vec<u64>>>` to DiskCache, updated on sync and open.
- [x] Added `SubChunkTracker::sub_chunk_id_for(gid, index_in_group)` for manifest-aware SubChunkId.
- [x] Updated all callers: read path, inline range GET, prefetch worker, interior chunk loading, sync.
- [x] Updated all 170 tests (3 new B-tree-aware regression tests).

#### e5. Restore SubChunkTracker population for tiered eviction
Phase e3 stopped marking the tracker to prevent positional pollution. With manifest-aware `sub_chunk_id_for()` from e4, the tracker can be correctly populated again, restoring tiered eviction (Pinned > Index > Data).

- [x] `write_pages_scattered` now accepts `gid` and `start_index_in_group`, marks tracker sub-chunks as `Data` tier after writing
- [x] `decode_and_cache_group_static` now accepts `gid`, marks tracker sub-chunks as `Data` tier after writing
- [x] All callers updated: prefetch worker, seekable sub-chunk path, full group download, eager group 0 load
- [x] 2 new regression tests: tracker population across frames, sub-frame offset marking
- [x] Updated existing no_tracker_pollution test to verify tracker IS marked correctly
- [x] 172 lib tests pass

#### f. Demand-driven prefetch
- [x] Read path restructured: range GET first (serves page), then background prefetch
- [x] Legacy inline full-group download path removed (all reads use seekable sub-chunk range GETs)
- [x] `trigger_prefetch`: fraction-based escalation via `prefetch_hops` (33%/33%/all)
- [x] `trigger_prefetch`: claims groups (CAS) before pool submit (deduplication)
- [x] `btree_groups` reverse index in manifest: group -> sibling group_ids from same B-tree
- [x] Built in `build_page_index()`, survives serde roundtrip (skip + rebuild)
- [x] S3 range GET failures return proper errors (no silent zero-fill)
- [x] 236 tests passing (tiered+zstd+encryption)

#### f2. Performance investigation (B-tree groups slower than positional)

B-tree-aware groups are theoretically optimal but benchmarks show 2-6x slower than positional groups at 100K. The gap is real: fair local-only comparison confirms it.

**Fair local comparison** (100K, 64KB pages, ppg=256, macOS to Tigris, same machine):

INDEX level (most revealing: index cached, only data from S3):
| Query      | Main p50 | Main GETs | Main bytes | B-tree p50 | B-tree GETs | B-tree bytes |
|------------|----------|-----------|------------|------------|-------------|--------------|
| post+user  | 138ms    | 2.0       | 36KB       | 382ms      | 7.0         | 11.9MB       |
| profile    | 403ms    | 11.7      | 12.3MB     | 1.0s       | 11.0        | 14.7MB       |
| who-liked  | 66ms     | 1.0       | 18KB       | 430ms      | 5.0         | 13.3MB       |
| mutual     | 171ms    | 1.0       | 18KB       | 103ms      | 2.3         | 2.9MB        |
| idx-filter | 98ms     | 1.0       | 18KB       | 104ms      | 2.0         | 40KB         |
| scan-filt  | 1.5s     | 33.0      | 26.3MB     | 743ms      | 11.3        | 9.7MB        |

**Root cause: over-eager background prefetch.** After the inline range GET serves the needed page, the B-tree branch downloads the full group (~8-16MB) + sibling groups in the background. For a point query needing 1-2 data pages (36-128KB), the branch downloads 11.9MB. This saturates the local-to-Tigris bandwidth (~100Mbps) and slows subsequent range GETs.

**Where B-tree wins**: scan-filter (1.5s vs 743ms at INDEX, 2.2s vs 1.3s at NONE). Full-group + sibling prefetch pays off when SQLite needs many sequential pages from the same B-tree.

**Fix (Phase Midway i):** range-GET budget per group caps inline range GETs, then waits for prefetch. See subphase i below.

#### f3. GroupingStrategy (Positional vs BTreeAware)

Enable both page grouping strategies in the same codebase for A/B benchmarking. The Manifest becomes the strategy: dispatch methods compute positions arithmetically (Positional) or via explicit lookup (BTreeAware).

- [ ] `GroupingStrategy` enum in config.rs: `Positional`, `BTreeAware`
- [ ] `PrefetchNeighbors` enum: `RadialFanout` (positional) vs `BTreeSiblings` (btree)
- [ ] `grouping_strategy` field on `TieredConfig` (default: BTreeAware)
- [ ] Manifest dispatch: `page_location()`, `group_page_nums()`, `prefetch_neighbors()` branch on `strategy`
- [ ] Positional: empty `group_pages` in manifest, computed on the fly
- [ ] handle.rs: replace all `manifest.group_pages.get()` with `manifest.group_page_nums()`
- [ ] handle.rs: `trigger_prefetch()` radial fan-out for Positional, sibling groups for BTreeAware
- [ ] handle.rs: `assign_new_pages_to_groups()` no-op for Positional
- [ ] import.rs: Positional path (sequential chunking, no btree walk)
- [ ] disk_cache.rs: `evict_group()` and `new()` handle empty `group_pages`
- [ ] vfs.rs: `detect_and_normalize_strategy()` on manifest load
- [ ] Benchmark: `--grouping positional` and `--grouping btree` flags in tiered-bench

#### g. Compaction
- [ ] Trigger: B-tree's groups have > 30% dead space, or total waste exceeds threshold
- [ ] Repack: read all pages for B-tree, dense-pack into new groups, upload, update manifest
- [ ] GC old groups after manifest swap
- [ ] VACUUM triggers full repack (all page numbers change)

#### i. Range-GET Budget Per Tree

At 1M posts, scans are 3-4x slower than necessary because every cache miss triggers an individual range GET (~3-20ms each), producing ~390 S3 requests when ~51 full-group prefetch downloads would suffice. The old no-skip schedule `[.33,.33,.34]` proved this: scans dropped from 2.4s to 642ms on S3 Express.

**Mechanism:** per-tree budget (default 2). After N inline range GETs to any groups in the same B-tree, submit ALL of that tree's remaining groups to prefetch and wait. 1 GET per tree = point query (fast). 2 GETs to same tree = scan (switch to bulk prefetch).

- [ ] `group_to_tree_name: HashMap<u64, String>` on Manifest (built on load, not serialized)
- [ ] `max_range_gets_per_tree: u8` field on `TieredConfig` (default 2)
- [ ] `tree_range_get_count: HashMap<String, u8>` on `TieredHandle` (per-connection, per-tree count)
- [ ] In seekable path: after prefetch submission, before range GET, check tree count. If >= max, submit all tree's groups to prefetch pool, `wait_for_group` instead of range GET.
- [ ] `--max-range-gets` CLI flag in tiered-bench
- [ ] Tests: count increment, per-tree independence, max=0 always waits, max=255 unlimited, Positional graceful degradation
- [ ] Benchmark: 1M posts, compare max=2 vs max=255 on scan-filter (expect 3-4x improvement)

#### h. Tests
- [x] Import: manifest mapping verified via IMPORT_VERIFY env (encode/decode roundtrip per group)
- [x] Read: page lookup via explicit mapping matches page content (integrity_check passes on 100k bench)
- [x] Cache: scattered write bitmap/tracker correctness (10 unit tests)
- [ ] Prefetch: root page access triggers only relevant B-tree's group fetches
- [ ] Checkpoint: new pages packed into correct B-tree's groups, only dirty groups re-uploaded
- [ ] Write amplification: INSERT into indexed table dirties fewer groups than positional packing
- [ ] Compaction: dead space reclaimed, B-tree groups repacked optimally
- [ ] VACUUM: full repack produces correct mapping

---

## Gallipoli: Normandy Leftovers
> After: Midway (B-Tree-Aware Page Groups) · Before: Somme

Small items remaining from Phase Normandy (loadable extension + language packages).

- [ ] `SELECT turbolite_register('vfs_name', '/path/to/base', 3)` SQL function for runtime VFS registration
- [ ] Integration test: C program loads extension, registers VFS, roundtrip
- [ ] Test: missing .so in Python package produces clear error message
- [ ] pkg-config `.pc` file for system install discovery

---

## Somme: Built-in WAL Shipping
> After: Gallipoli · Before: Verdun

Close the durability gap between checkpoints. The VFS already intercepts every WAL write via `write_all_at()` and has an S3 client + tokio runtime. Ship WAL frames to S3 in the background so writes are durable before checkpoint.

### a. WAL frame capture + upload
- [ ] Intercept WAL `write_all_at()` — still write to local disk (SQLite needs it), also buffer frame bytes
- [ ] Batch frames into segments (e.g. every 100 frames or N ms) to amortize PUT cost
- [ ] Upload segments to `{prefix}/wal/{sequence_number}` via existing S3 client
- [ ] Monotonic sequence numbers for replay ordering

### b. Recovery
- [ ] On arctic open: after manifest download, list `{prefix}/wal/` objects newer than manifest version
- [ ] Replay WAL frames into local cache before serving queries
- [ ] Test: write data, kill before checkpoint, recover from S3 WAL segments

### c. Cleanup
- [ ] After checkpoint uploads page groups + new manifest, WAL segments older than manifest version are garbage
- [ ] Integrate with existing GC — add WAL segment keys to the "not in manifest = garbage" rule

### d. WAL write callback (future)
- [ ] `TieredConfig::on_wal_write(fn(&[u8]))` — optional hook for external tools (walrust, custom replication)
- [ ] turbolite doesn't care what the callback does — just observes and forwards

---

## Verdun: Predictive Cross-Tree Prefetch + Access History
> After: Somme · Before: Marne

Depends on Phase Midway (B-tree-aware page groups + demand-driven prefetch). Midway solves "fetch one tree fast" (~5ms on Express). But multi-join queries touch 3-4 trees sequentially. Without prediction, each tree is fetched on-demand as SQLite traverses into it: posts index (5ms), then users data (5ms), then likes index (5ms). Serial chain = 15ms of waiting.

Predictive prefetch eliminates the serial chain. When SQLite reads the second B-tree in a lock session and the pair matches a known pattern, all remaining trees prefetch in parallel. 15ms serial collapses to 5ms parallel.

Two complementary features:
1. **Access history** (prefetch on open): track which B-trees are used, eagerly fetch the hot ones when a new connection opens
2. **Predictive cross-tree prefetch** (prefetch within a session): learn which B-trees appear together in transactions, prefetch the full set when a partial match is detected

### Design

**Pattern boundary = lock lifecycle.** The VFS sees `lock()` and `unlock()` calls on `DatabaseHandle`. A lock session is NONE -> SHARED (or EXCLUSIVE) -> NONE. Read queries acquire SHARED for the duration; transactions hold the lock across all queries until commit. The lock lifecycle is the natural repeating unit: a web app hitting the same endpoint runs the same transaction pattern thousands of times.

Each `TieredHandle` is per-connection, so concurrent readers track patterns independently. The prediction table is shared (`Arc<RwLock<PredictionTable>>`) so patterns learned by one connection benefit all others.

**Pattern = unordered set.** `BTreeSet<u64>` of B-tree root pages touched during a lock session. Unordered because `SELECT FROM posts JOIN users` and `SELECT FROM users JOIN posts` touch the same trees (query planner picks traversal order). The prediction value is "these trees appear together," not "in this order."

**B-tree touch detection.** On every `read_exact_at`, translate `page_num` to B-tree root via `manifest.page_index` -> `btree owner`. If this root hasn't been seen in the current lock session, add it to the session's `BTreeSet`. This is an O(1) HashMap lookup + HashSet insert per page read.

**Fingerprint and prediction (K=2).** After 2 distinct B-tree roots are touched in a lock session, hash the pair as a fingerprint and look up the prediction table. If a known pattern contains this pair and confidence > threshold, submit all remaining predicted groups to the prefetch pool in parallel. K=2 is the right width: 1-tree queries never fire predictions (no false positives), and for 3+ tree queries we get 1+ trees of lookahead.

**Confidence scoring.**
- Initial confidence: 0.3 (one observation is not enough to fire; threshold is 0.5)
- Reinforcement: +0.25 per correct prediction (predicted groups ARE subsequently read in the same lock session)
- Time decay: *0.95 per lock session flush (slow fade for patterns that stop appearing)
- Write decay: *0.7 per dirty page in a predicted B-tree (bulk writes fade predictions to zero)
- Threshold to fire: 0.5
- A pattern needs 2 observations to fire (0.3 + 0.25 = 0.55 > 0.5). Three writes to a predicted tree drops confidence below threshold (0.8 * 0.7^3 = 0.27). Patterns that fire correctly every session stabilize around 0.85-0.95.

**Access history (eager prefetch on open).** Track B-tree access frequency across sessions. On checkpoint, merge the current session's touched B-trees into a manifest-persisted frequency map (`HashMap<u64, f32>`). Apply exponential decay (*0.9) per session so B-trees that stop being accessed fade out. On connection open, sort B-trees by frequency and prefetch the top N groups eagerly (parallel, background). A database where 90% of queries hit `users` + `posts` + `idx_posts_user` will have those trees fully cached within milliseconds of opening.

**Persistence.** Both the prediction table and access history are stored as manifest fields. Typical app has <100 unique patterns * avg 4 trees * 8 bytes + stats = ~5KB. Access history is even smaller (~600 bytes for 50 B-trees). Both fit comfortably in the manifest. Rebuilt after VACUUM or schema change (root pages move).

**Graceful degradation.** Wrong or stale predictions fall back to demand-driven prefetch from Midway (still fast, ~5ms per tree). Predictions are purely additive: they submit extra prefetch jobs but never block the read path. The workloads where predictions matter most (repeated transaction patterns, read-heavy CRUD) are the workloads where they're most stable.

### a. Lock session tracking infrastructure ✅

Per-connection state to track B-tree touches within a lock session.

- [x] `prediction.rs`: `LockSession` (btrees HashSet, active, prediction_fired, fired_pattern)
- [x] `prediction.rs`: `PredictionTable` with pair index, confidence math, persist/restore
- [x] `prediction.rs`: `AccessHistory` with frequency tracking, decay, top-N
- [x] `manifest.rs`: `page_to_btree: HashMap<u64, u64>` reverse index (built in `build_page_index()`)
- [x] `manifest.rs`: `btree_access_freq` and `prediction_patterns` fields (serde, manifest-persisted)
- [x] `handle.rs`: `lock_session`, `prediction`, `dirty_btrees` fields on `TieredHandle`
- [x] `handle.rs`: `read_exact_at` hook for B-tree touch tracking via `page_to_btree`
- [x] `handle.rs`: `write_all_at` hook for dirty B-tree tracking
- [x] `handle.rs`: `lock()` hook: NONE->SHARED starts session, *->NONE flushes session
- [x] `handle.rs`: `flush_lock_session()` with write decay + reinforcement via `fired_pattern`
- [x] `config.rs`: `prediction_enabled: bool` (default false)
- [x] `vfs.rs`: `SharedPrediction` on `TieredVfs`, initialized from manifest, passed to handles
- [x] `build_page_index()` auto-detects BTreeAware when group_pages is populated (fixed 13 pre-existing test failures)
- [x] 16 unit tests (LockSession lifecycle, PredictionTable math, AccessHistory decay, pair index, persist roundtrip)

### b. Access history (frequency tracking) ✅

Wire AccessHistory into flush + checkpoint path. Manifest fields already exist from (a).

- [x] `SharedAccessHistory` type alias (`Arc<RwLock<AccessHistory>>`) on `TieredVfs`, initialized from `manifest.btree_access_freq`
- [x] Passed to `TieredHandle` via `new_tiered()` parameter
- [x] In `flush_lock_session`: call `access_history.record(&session.btrees)` for every non-empty session
- [x] In `sync()` (checkpoint): call `access_history.decay_and_prune()`, serialize into manifest `btree_access_freq`
- [x] 4 new unit tests: concurrent shared history, init from manifest freq, decay preserves hot trees, prune on checkpoint

### c. Prediction persistence in checkpoint ✅

Wire prediction table serialization into the checkpoint path.

- [x] `PredictionTable` struct with observe/predict/reinforce/write_decay/prune (done in a)
- [x] `PredictionTable::to_persisted()` / `from_persisted()` (done in a)
- [x] In `sync()` (checkpoint): serialize prediction table to manifest (`prediction_patterns`), call `prune()`

### d. Prediction firing ✅

Trigger parallel prefetch when a partial match is detected during a lock session.

- [x] `prediction_fired` and `fired_pattern` fields on `LockSession` (done in a)
- [x] Pair index on `PredictionTable` for O(1) pattern lookup (done in a)
- [x] In `read_exact_at`, after B-tree touch: if `tree_count() >= 2` and `!prediction_fired`, call `table.predict(&session.btrees)`
- [x] On match: look up predicted roots' group IDs from manifest `btrees`, submit to prefetch pool
- [x] Set `prediction_fired = true`, store matched pattern key in `fired_pattern`
- [x] 4 new unit tests: end-to-end prediction firing, reinforcement on correct fire, no reinforcement on partial touch, concurrent read/write safety

### e. Write decay ✅ (done in a)

- [x] `write_all_at` resolves dirty page to B-tree root via `page_to_btree`
- [x] `dirty_btrees` HashSet tracks per-session (not per-page)
- [x] `flush_lock_session` applies `write_decay()` once per dirty root
- [ ] Test: write-heavy workload fades predictions below threshold; read-only workload keeps them stable (see g2)

### f. Reinforcement ✅ (done in a)

- [x] `flush_lock_session`: if `fired_pattern` is set and all predicted roots were touched, call `reinforce()`
- [x] `reinforce()`: confidence += 0.25, capped at 1.0, increments hit_count
- [ ] Test: reinforcement increases confidence across sessions; no-reinforcement decays (see g2)

### g. Unit tests ✅ (25 tests)

- [x] Unit: lock session tracking accumulates correct B-tree roots
- [x] Unit: prediction table upsert/decay/reinforcement math
- [x] Unit: pair index correctly maps to patterns
- [x] Unit: predict() returns None when all roots already seen
- [x] Unit: fired_pattern cleared on session start
- [x] Unit: page_to_btree reverse index correctness
- [x] Unit: shared access history concurrent record
- [x] Unit: access history from manifest freq
- [x] Unit: prediction table prune on checkpoint
- [x] Unit: access history decay preserves hot B-trees
- [x] Unit: prediction firing end-to-end
- [x] Unit: reinforcement on correct fire
- [x] Unit: no reinforcement on partial touch
- [x] Unit: shared prediction concurrent access
- [x] Unit: 2-root pattern does not decay below threshold (observation boost regression)

### g2. Comprehensive integration + edge case tests

S3 integration tests (require TIERED_TEST_BUCKET):

**Checkpoint roundtrip:**
- [ ] Prediction patterns survive checkpoint -> S3 manifest -> reopen with prediction_enabled=true
- [ ] Access history frequencies survive checkpoint -> reopen, top_roots returns correct order
- [ ] Checkpoint with no patterns produces empty prediction_patterns in manifest (no bloat)
- [ ] Checkpoint with prediction_enabled=false preserves existing patterns/history from manifest (passthrough)

**Real query prediction firing:**
- [ ] 3-table join (CREATE TABLE a,b,c with indexes; JOIN all three): pattern learned after 2 lock sessions, fires on 3rd
- [ ] Prediction submits correct group IDs to prefetch pool (verify via S3 fetch counters)
- [ ] Reinforcement fires when predicted groups are subsequently read in same session

**Decay + write behavior:**
- [ ] Pattern decays below threshold after ~10 sessions without reinforcement (time decay only)
- [ ] Write decay: bulk INSERT into a predicted B-tree drops confidence below threshold within 3 dirty sessions
- [ ] Read-only workload: patterns stabilize around 0.85-0.95 confidence after 10+ sessions
- [ ] Mixed workload: read patterns survive while write-heavy patterns fade

**Strategy edge cases:**
- [ ] Positional strategy: prediction fields exist in manifest but page_to_btree is empty, no panics
- [ ] Positional strategy: prediction_enabled=true is harmless (no B-tree roots detected, no patterns)
- [ ] BTreeAware with single-table DB: no predictions fire (only 1 B-tree root), no overhead

**Negative tests:**
- [ ] Single-tree query never fires predictions (tree_count < 2 guard)
- [ ] Prediction with unknown B-tree root (not in manifest.btrees) is silently skipped
- [ ] max_patterns cap enforced: insert MAX+1 unique patterns, oldest/lowest confidence pruned

**Manifest bloat prevention:**
- [ ] 100 unique patterns: manifest size stays under 10KB for prediction fields
- [ ] 1000 unique patterns: prune() reduces to max_patterns, manifest size bounded
- [ ] Patterns with confidence < PRUNE_THRESHOLD removed on checkpoint

**VACUUM / schema change:**
- [ ] After VACUUM (all root page IDs change): old patterns reference stale IDs, no crash, predictions become no-ops
- [ ] After DROP TABLE + CREATE TABLE: prediction table handles missing B-tree roots gracefully
- [ ] After ADD INDEX: new B-tree root appears, prediction table learns it in subsequent sessions

**Concurrency:**
- [ ] 4 concurrent readers learning independent patterns: no deadlocks, all patterns recorded
- [ ] 2 readers + 1 writer: write decay applies only to writer's patterns, readers unaffected

### h. Prediction benchmark

Measure impact of predictive prefetch on multi-join query latency. Add `--predicted` mode to tiered-bench.

Three-phase benchmark:
1. **Learn**: run full query suite 5-10x with `prediction_enabled=true`. Patterns accumulate. Checkpoint persists patterns + access history to manifest.
2. **Baseline**: `prediction_enabled=false`, clear cache to interior level, run query suite, record per-query latency + S3 fetches.
3. **Predicted**: reopen VFS with `prediction_enabled=true` (loads patterns from manifest). Clear cache to interior level. Run query suite, record latency + S3 fetches. Prediction firing triggers parallel cross-tree prefetch during queries on cache miss.

Compare Baseline vs Predicted.

**Expected impact by query type:**

| Query | Trees | Expected improvement |
|-------|-------|---------------------|
| Point lookup | 1 | None (single tree) |
| Profile (5 joins) | 3-4 | Big (15ms serial -> 5ms parallel) |
| Who-liked | 2 | Moderate (10ms -> 5ms) |
| Mutual friends | 2-3 | Moderate |
| Indexed filter | 1-2 | Small |
| Full scan | all | None (not a join pattern) |

**Metrics to capture:**
- Per-query latency (median, p95) with and without prediction
- S3 fetches per query (count + bytes)
- Prediction stats: patterns learned, fire rate, hit rate
**Isolating the effect:**
- Start with warm interior+index, measure multi-join queries (effect of parallel cross-tree prefetch on cache miss)

Implementation:
- [ ] Add `--predicted` flag to tiered-bench
- [ ] Learning phase: run query suite N times, checkpoint
- [ ] Measurement phase: A/B test with prediction on/off at each cache level
- [ ] Report prediction stats (patterns, fire rate, hit rate) alongside latency
- [ ] Compare "interior" cache level with and without prediction (the production-relevant comparison)
- [ ] Benchmark: measure serial vs parallel tree fetch latency for multi-join queries (standard S3 + Express)

### i. Name-based prediction keys + trie storage

Refactor prediction keys from `u64` root page IDs to `String` table/index names. Switch internal storage from flat `HashMap<BTreeSet<u64>, PredictionEntry>` to a sorted trie. Solves VACUUM resilience (names survive page renumbering) and reduces manifest size (shared prefixes stored once).

#### i1. Name resolution infrastructure ✅

Add name-based lookup paths so the read hot path can translate page -> tree name -> prediction.

- [x] `manifest.rs`: add `page_to_tree_name: HashMap<u64, String>` (skip serde, rebuilt in `build_page_index()`)
  - Built from `btrees` map: for each `(root, BTreeManifestEntry { name, .. })`, iterate `group_pages[gid]` for each gid in entry, map every page_num -> name
  - Replaces `page_to_btree: HashMap<u64, u64>` (root ID) with a name string
- [x] `manifest.rs`: add `tree_name_to_groups: HashMap<String, Vec<u64>>` (skip serde, rebuilt in `build_page_index()`)
  - Built from `btrees` map: for each entry, map name -> group_ids
  - Used at prediction fire time to resolve predicted tree names back to group IDs for prefetch submission
- [x] Update `build_page_index()` to populate both new maps
- [x] Removed `page_to_btree` (replaced by `page_to_tree_name`, no backward compat needed)

#### i2. Prediction table: BTreeSet<String> keys ✅

Replace `u64` root IDs with `String` names throughout the prediction system.

- [x] `prediction.rs`: `LockSession.btrees` changes from `HashSet<u64>` to `HashSet<String>`
- [x] `prediction.rs`: `LockSession.touch()` takes `&str` instead of `u64`
- [x] `prediction.rs`: `LockSession.fired_pattern` changes from `BTreeSet<u64>` to `BTreeSet<String>`
- [x] `prediction.rs`: `PredictionEntry.roots` renamed to `PredictionEntry.names`, type `BTreeSet<String>`
- [x] `prediction.rs`: `PredictionTable.patterns` key type changes to `BTreeSet<String>`
- [x] `prediction.rs`: `PredictionTable.pair_index` key type changes to `(String, String)` (sorted pair)
- [x] `prediction.rs`: `observe()`, `predict()`, `reinforce()`, `write_decay()` all take `&str` / `&HashSet<String>`
- [x] `prediction.rs`: `AccessHistory.freq` changes from `HashMap<u64, f32>` to `HashMap<String, f32>`
- [x] `prediction.rs`: `AccessHistory.record()` takes `&HashSet<String>`
- [x] `prediction.rs`: `AccessHistory.top_roots()` renamed to `top_trees()`, returns `Vec<String>`
- [x] `manifest.rs`: `btree_access_freq` changes from `HashMap<u64, f32>` to `HashMap<String, f32>`
- [x] `manifest.rs`: `prediction_patterns` changes from `Vec<(BTreeSet<u64>, f32)>` to `Vec<(BTreeSet<String>, f32)>`
- [x] `handle.rs`: `read_exact_at` B-tree touch: `page_to_tree_name.get(&page_num)` -> `session.touch(name)`
- [x] `handle.rs`: `write_all_at` dirty tracking: `dirty_btrees` changes to `HashSet<String>`
- [x] `handle.rs`: prediction firing: `predict()` returns `Vec<String>`, resolve via `tree_name_to_groups`
- [x] `handle.rs`: `flush_lock_session()`: write_decay and reinforce use `&str` names
- [x] `handle.rs`: removed eager prefetch on open (prediction only fires on real cache misses)
- [x] `vfs.rs`: initialization from manifest uses new String-based types (no code change needed)
- [x] `import.rs`: manifest construction updated for new field names
- [x] `mod.rs`: test `test_manifest_serde_roundtrip_btree_groups` updated for `page_to_tree_name` and `tree_name_to_groups`
- [x] Updated all 25 existing unit tests to use String names (28 total with 3 new)
- [x] New test: `name_based_pattern_survives_conceptual_vacuum`
- [x] New test: `rename_table_old_patterns_become_dead`
- [x] New test: `drop_create_same_name_carries_over`
- [x] 235 lib tests pass, 28 prediction tests pass

#### i3. Trie storage

Replace `HashMap<BTreeSet<String>, PredictionEntry>` with a sorted trie. Nodes keyed by tree name (alphabetically sorted within each pattern). Each node carries confidence + count. Pair index derived from trie structure.

**Trie structure:**

```rust
struct PredictionTrie {
    root: TrieNode,
}

struct TrieNode {
    children: HashMap<String, TrieNode>,
    /// Non-None if this node represents a complete observed pattern.
    /// e.g. node at path ["idx_posts_user", "posts"] has entry if
    /// {idx_posts_user, posts} was observed as a pattern.
    entry: Option<TrieEntry>,
}

struct TrieEntry {
    confidence: f32,
    hit_count: u32,
    miss_count: u32,
}
```

- [ ] `prediction.rs`: `PredictionTrie` struct with insert, lookup, prune, serialize/deserialize
- [ ] Trie insert: sort pattern names alphabetically, walk/create nodes, set entry at leaf
- [ ] Trie lookup (K=2 trigger): given 2 names (sorted), traverse `root -> name_a -> name_b`. If that node has children with entry.confidence > threshold, collect those children's names as predictions. Also check if the node itself has an entry (2-element pattern).
- [ ] Trie observe: walk to the node for the full pattern, update confidence with time decay + observation boost
- [ ] Trie reinforce: walk to the node for fired_pattern, apply reinforcement boost
- [ ] Trie prune: DFS, remove nodes where entry.confidence < PRUNE_THRESHOLD and no children. Enforce max_patterns by collecting all entries, sorting by confidence, removing lowest.
- [ ] Pair index elimination: the trie IS the index. Given sorted pair (A, B), traverse `root -> A -> B` in O(2) hash lookups. No separate HashMap needed.
- [ ] Serialization: `to_persisted()` returns `Vec<(Vec<String>, f32, u32)>` (path, confidence, hit_count). `from_persisted()` rebuilds trie.
- [ ] Manifest field: `prediction_patterns` changes from `Vec<(BTreeSet<String>, f32)>` to `Vec<(Vec<String>, f32, u32)>`
  - Vec<String> instead of BTreeSet because trie paths are already sorted, no need for BTreeSet overhead in serde
- [ ] Replace `PredictionTable` with `PredictionTrie` throughout handle.rs and vfs.rs
- [ ] Test: trie produces identical predictions as flat HashMap for same input patterns
- [ ] Test: trie with shared prefixes uses less memory than flat HashMap (100 patterns with common 2-element prefixes)
- [ ] Test: prune removes empty branches, max_patterns enforced
- [ ] Test: serialize/deserialize roundtrip preserves all entries and structure
- [ ] Test: K=2 lookup traverses trie in O(2), returns correct predictions

#### i4. Cleanup

- [ ] Remove `page_to_btree: HashMap<u64, u64>` from manifest (replaced by `page_to_tree_name`)
- [ ] Remove `PredictionTable` (replaced by `PredictionTrie`)
- [ ] Remove `pair_index` (trie is the index)
- [ ] Update all doc comments referencing root page IDs to reference tree names
- [ ] Verify all 25+ unit tests pass with trie backend
- [ ] Run S3 integration tests: checkpoint roundtrip with trie-serialized patterns

### j. Frame-level correlation (precision prefetch)

Extends tree-level prediction (subphase i) with frame-level granularity. Instead of "fetch all of tree B", predict "fetch frame 7 of tree B's group 3". Reduces prefetch bandwidth by 10-1000x for large tables.

**Motivation.** With B-tree-aware groups, frames (sub-chunks within a group) cluster pages from the same tree. A point query on a large table needs 1-2 frames, not all 50 groups. If we learn that frame X of index A correlates with frame Y of data table B, we can issue a single S3 range GET for ~256KB instead of downloading all of tree B.

**Key insight.** Index leaf -> data leaf correlations are structurally stable. An index sorted by user_id consistently points to the same data pages for a given user_id range. The correlation only changes on compaction/VACUUM (rare), and time decay handles drift.

#### j1. Frame-level access tracking

Extend lock session to record which frames (not just trees) are accessed.

- [ ] Derive frame index from page position: `frame_idx = index_in_group / sub_pages_per_frame`
- [ ] `LockSession`: add `frame_touches: Vec<(String, u64, u32)>` (tree_name, group_id, frame_idx)
- [ ] `read_exact_at`: after tree name lookup, also record `(name, gid, frame_idx)` in `frame_touches`
- [ ] `frame_touches` is append-only within a session, flushed on lock release
- [ ] Test: frame touches accumulate correct (tree, group, frame) triples

#### j2. Frame correlation table

Learn which frames of tree B are accessed given which frames of tree A were accessed in the same session.

```rust
struct FrameCorrelation {
    /// (source_tree, target_tree) -> frame mapping
    correlations: HashMap<(String, String), FrameMap>,
}

struct FrameMap {
    /// source (gid, frame_idx) -> target (gid, frame_idx) with confidence
    entries: HashMap<(u64, u32), Vec<FrameTarget>>,
    /// Total observation count for decay
    observation_count: u32,
}

struct FrameTarget {
    group_id: u64,
    frame_idx: u32,
    confidence: f32,
}
```

- [ ] On session flush: for each pair of trees (A, B) in the session, cross-correlate their frame touches
  - For each frame of A that was read, record which frames of B were read in the same session
  - Increment confidence for seen correlations, apply time decay to unseen ones
- [ ] Confidence scoring: same as tree-level (initial 0.3, reinforce +0.25, decay *0.95, threshold 0.5)
- [ ] `max_correlations_per_pair: u32` config (default 200): cap frame entries per tree pair, prune lowest confidence
- [ ] Test: point query on indexed table records correct frame correlation (index leaf frame -> data frame)
- [ ] Test: range query records multiple frame correlations (10 index frames -> 10-20 data frames)
- [ ] Test: correlations survive checkpoint roundtrip via manifest

#### j3. Precision prefetch firing

When tree-level prediction fires (from trie), use frame correlations to narrow the prefetch to specific frames.

- [ ] On prediction fire: for each predicted tree C, look up `FrameCorrelation[(current_tree, C)]`
  - Given current frame(s) of current_tree, collect correlated frames of C
  - If correlations exist and confidence > threshold: submit individual frame range GETs (not full groups)
  - If no frame-level data: fall back to full-group prefetch (tree-level, same as before)
- [ ] S3 range GET for a single frame: `bytes={frame.offset}-{frame.offset + frame.len - 1}`
  - Already supported by seekable page groups + frame tables in manifest
  - Decode single frame, write to cache via `write_pages_scattered`
- [ ] Track frame-level prediction accuracy: did we actually read the predicted frames? Reinforce or decay.
- [ ] Test: frame-level prediction fires range GET for 1 frame (~256KB) instead of full group (~8MB)
- [ ] Test: fallback to full-group when no frame correlations exist
- [ ] Test: frame-level reinforcement increases correlation confidence

#### j4. Manifest persistence for frame correlations

- [ ] New manifest field: `frame_correlations: Vec<FrameCorrelationEntry>`
  - `FrameCorrelationEntry { source_tree: String, target_tree: String, source_gid: u64, source_frame: u32, target_gid: u64, target_frame: u32, confidence: f32 }`
  - Flat list, rebuilt into `FrameCorrelation` HashMap on load
- [ ] Serialize in `sync()` alongside prediction patterns and access history
- [ ] Prune on checkpoint: remove entries below threshold, enforce max per pair
- [ ] Size budget: 200 correlations * ~60 bytes = 12KB. Bounded by `max_correlations_per_pair`.
- [ ] Test: manifest roundtrip preserves frame correlations
- [ ] Test: prune reduces correlation count to max on checkpoint

#### j5. Staleness handling

Frame correlations reference (group_id, frame_idx) which can change on compaction or VACUUM.

- [ ] On VACUUM/re-import: frame correlations referencing old group IDs become stale
  - Option A: clear all frame correlations on VACUUM (they'll be relearned quickly)
  - Option B: remap correlations using old_gid -> new_gid from the re-import manifest diff
  - Recommendation: Option A (simpler, correlations relearn within 2-3 sessions)
- [ ] On checkpoint with group repacking: if a group's frame table changes, invalidate correlations for that group
- [ ] Time decay handles gradual drift from INSERT/DELETE reshuffling pages within frames
- [ ] Test: correlations invalidated after VACUUM, relearned within 3 sessions
- [ ] Test: time decay removes stale correlations that no longer fire

#### j6. Benchmark (frame-level)

Measure the bandwidth savings of frame-level vs tree-level prediction.

- [ ] Add `--frame-predict` flag to tiered-bench (requires `--predicted`)
- [ ] Compare for point queries: S3 bytes fetched with tree-level vs frame-level prediction
- [ ] Compare for range queries: verify frame-level doesn't under-fetch (falls back to full group correctly)
- [ ] Expected: point query on 1M-row table with 50 groups: tree-level fetches ~400MB, frame-level fetches ~256KB (1000x reduction)
- [ ] Report: per-query (bytes_tree_level, bytes_frame_level, latency_tree, latency_frame)

---

## Marne: Query-Plan-Aware Prefetch
> After: Verdun · Before: (future)

SQLite extension that intercepts the query plan BEFORE execution and tells the VFS exactly which B-trees will be accessed. Eliminates the hop schedule tradeoff entirely: the VFS knows the access pattern before the first page request.

Verdun learns patterns from past transactions (reactive). Marne knows the plan for the current query (proactive). Together they cover 100% of cases: Marne handles known queries, Verdun handles dynamic/ad-hoc patterns.

### Design

**Query plan interception.** Two options:
1. `sqlite3_set_authorizer` callback: fires for every table/index access in query compilation. Lightweight, synchronous, available before first page read.
2. `EXPLAIN QUERY PLAN` wrapper: run EQP on the SQL string, parse output for SCAN/SEARCH + table/index names, pass to VFS via shared channel.

Option 1 is preferred: zero overhead, no extra query, fires during `sqlite3_prepare_v2`.

**VFS communication channel.** Shared `Arc<Mutex<PendingPlan>>` between the authorizer callback and the VFS handle. Authorizer populates the plan (which tables, which indices, SCAN vs SEARCH). VFS `read_exact_at` checks the plan on first page request.

**Prefetch strategy per access type:**
- SEARCH (index lookup): prefetch index groups only (point query, 1-2 groups)
- SCAN (full table scan): prefetch ALL groups for the table (bulk, aggressive first-miss)
- JOIN: prefetch all participating tables/indices in parallel

**Integration with hop schedule:** When a query plan is available, the hop schedule is bypassed entirely. The VFS issues parallel group fetches for all planned B-trees on first page request. No hops, no budget, no waiting.

### Implementation

- [ ] SQLite loadable extension with `sqlite3_set_authorizer` hook
- [ ] `PendingPlan` struct: `Vec<PlannedAccess>` where `PlannedAccess { btree_name, access_type: Scan|Search }`
- [ ] Shared channel: `Arc<Mutex<Option<PendingPlan>>>` on TieredHandle
- [ ] VFS checks plan on first cache miss: if plan exists, submit all planned groups to prefetch pool
- [ ] Authorizer resets plan on each new `prepare` call
- [ ] Test: SEARCH query prefetches only index groups
- [ ] Test: SCAN query prefetches all table groups
- [ ] Test: JOIN prefetches all participating B-trees in parallel
- [ ] Test: without extension loaded, VFS falls back to hop schedule (backward compatible)
- [ ] Benchmark: compare plan-aware vs hop-schedule on 1M posts, all query types

---

## Future

### mmap cache
The local cache currently uses `pread`/`pwrite`. Memory-mapping the cache file would eliminate syscall overhead for warm reads — the kernel serves pages directly from the page cache.

- [ ] `mmap` the cache file instead of `pread` for reads
- [ ] Keep `pwrite` for cache population (writing pages from S3 fetches)
- [ ] `madvise(MADV_RANDOM)` — access pattern is random, not sequential
- [ ] Handle cache file growth: `mremap` on Linux, re-map on macOS
- [ ] Benchmark: warm lookup latency with mmap vs pread (expect ~10-50us → ~1-5us)

### CLI subcommands
- [ ] `turbolite bench` — move tiered-bench into a CLI subcommand
- [ ] `turbolite gc --bucket X --prefix Y` — one-shot GC (currently in Thermopylae roadmap)
- [ ] `turbolite import --bucket X --prefix Y --db local.db` — import a local SQLite DB to S3
- [ ] `turbolite info --bucket X --prefix Y` — print manifest summary (page count, groups, size)

### Bidirectional prefetch
- Track access direction, prefetch backward for DESC queries

### Application-level fetch API
- `vfs.fetch_all()` — background hydration
- `vfs.fetch_range(start, end)` — contiguous range fetch

### Hole tracking
- Manifest tracks freed pages per group
- Groups with >N% dead pages are compaction candidates
- Re-encode without dead pages, saving one PUT per compacted group

### Multi-writer coordination
- [ ] Distributed locks for concurrent writers (if needed)
