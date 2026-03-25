# turbolite Roadmap

## Tannenberg: File Size Cleanup
> After: Ypres · Before: Marne

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

**Fix direction**: make prefetch conditional on access pattern:
- [ ] **Point queries** (1-2 data page misses): range-GET only, no full-group download, no sibling prefetch
- [ ] **Scan queries** (consecutive misses in same B-tree): escalate to full-group + sibling prefetch
- [ ] Threshold: `consecutive_misses_in_same_btree >= 3` triggers full-group mode
- [ ] Or: cap background download concurrency to 1-2 concurrent groups (prevent bandwidth saturation)
- [ ] Validate on S3 Express (10Gbps) where bandwidth contention may not exist

#### g. Compaction
- [ ] Trigger: B-tree's groups have > 30% dead space, or total waste exceeds threshold
- [ ] Repack: read all pages for B-tree, dense-pack into new groups, upload, update manifest
- [ ] GC old groups after manifest swap
- [ ] VACUUM triggers full repack (all page numbers change)

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
> After: Somme · Before: (future)

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

### a. Lock session tracking infrastructure

Add per-connection state to track B-tree touches within a lock session.

- [ ] Add to `TieredHandle`: `session_btrees: HashSet<u64>` (B-tree roots touched in current lock session)
- [ ] Add to `TieredHandle`: `session_active: bool` (true between lock acquire and release)
- [ ] In `read_exact_at`: after resolving `page_num` to B-tree root via manifest, insert into `session_btrees`
- [ ] Need reverse index: `page_num -> btree_root`. Build from `manifest.btrees` (root -> pages) into `HashMap<u64, u64>` (page -> root). Build alongside `page_index` in `build_page_index()`
- [ ] In `lock()`: when transitioning from NONE to SHARED/RESERVED, set `session_active = true`, clear `session_btrees`
- [ ] In `unlock()` / lock downgrade to NONE: set `session_active = false`, flush session pattern

### b. Access history (eager prefetch on open)

Track B-tree usage frequency across sessions. Prefetch hot trees on connection open.

- [ ] Add to `Manifest`: `btree_access_freq: HashMap<u64, f32>` (root page -> frequency score)
- [ ] On lock session flush: for each root in `session_btrees`, increment `btree_access_freq[root] += 1.0`
- [ ] On checkpoint (`sync()`): apply decay `*= 0.9` to all entries, prune entries below 0.1, serialize into manifest
- [ ] In `new_tiered()`: after manifest download, sort B-trees by frequency, prefetch top N groups eagerly via prefetch pool
- [ ] Config: `eager_btree_count: u32` (default 5) controls how many hot B-trees to prefetch on open
- [ ] Test: open DB, run queries touching 3 B-trees, checkpoint, reopen, verify those 3 B-trees' groups are prefetched before any query
- [ ] Test: B-tree that stops being accessed decays below threshold after ~20 sessions

### c. Prediction table

Learn which B-trees appear together in transactions. Fire parallel prefetch on partial match.

- [ ] `PredictionEntry { predicted_roots: BTreeSet<u64>, confidence: f32, hit_count: u32 }`
- [ ] `PredictionTable { patterns: HashMap<BTreeSet<u64>, PredictionEntry> }` shared via `Arc<RwLock<>>`
- [ ] On lock session flush: if `session_btrees.len() >= 2`, upsert pattern. New pattern gets confidence 0.3. Existing pattern: apply time decay (*0.95) then reinforcement (+0.25 if prediction fired and was correct)
- [ ] Store prediction table in `TieredVfs` (shared across all connections for this VFS instance)
- [ ] Manifest field: `prediction_patterns: Vec<(BTreeSet<u64>, f32)>` for persistence across restarts
- [ ] On checkpoint: serialize prediction table to manifest. Prune patterns below confidence 0.1
- [ ] On VFS open: deserialize prediction table from manifest into shared `Arc<RwLock<>>`

### d. Prediction firing

Trigger parallel prefetch when a partial match is detected during a lock session.

- [ ] Add to `TieredHandle`: `prediction_fired: bool` (prevent re-firing within same session)
- [ ] In `read_exact_at`, after adding a new B-tree root to `session_btrees`: if `session_btrees.len() >= 2` and `!prediction_fired`, scan prediction table for any pattern where `session_btrees` is a subset and confidence >= 0.5
- [ ] On match: submit all groups for predicted roots NOT already in `session_btrees` to prefetch pool
- [ ] Set `prediction_fired = true` for this session
- [ ] Prediction scan optimization: index patterns by pairs. `HashMap<(u64, u64), Vec<usize>>` mapping each pair of roots to pattern indices. On 2nd B-tree touch, look up the pair directly instead of scanning all patterns

### e. Write decay

Dirty pages reduce confidence for predictions involving that B-tree.

- [ ] In `write_all_at`: resolve dirty page to B-tree root (same reverse index as read path)
- [ ] For each prediction containing that root: `confidence *= 0.7`
- [ ] Bulk writes (100 dirty pages in same B-tree) apply decay once per B-tree per lock session, not per page
- [ ] Test: write-heavy workload fades predictions below threshold; read-only workload keeps them stable

### f. Reinforcement

Correct predictions get stronger. Wrong predictions decay naturally.

- [ ] On lock session flush: if `prediction_fired`, check whether predicted roots were actually touched in `session_btrees`
- [ ] If all predicted roots were touched: `confidence += 0.25` (capped at 1.0)
- [ ] If some predicted roots were NOT touched: no reinforcement (time decay handles the fade)
- [ ] Track `hit_count` for observability

### g. Tests

- [ ] Unit: lock session tracking accumulates correct B-tree roots
- [ ] Unit: prediction table upsert/decay/reinforcement math
- [ ] Unit: pair index correctly maps to patterns
- [ ] Integration: 3-table join pattern learned after 2 observations, fires on 3rd
- [ ] Integration: pattern decays below threshold after ~10 sessions without reinforcement
- [ ] Integration: write decay drops prediction after bulk writes
- [ ] Integration: access history prefetches hot B-trees on reopen
- [ ] Integration: concurrent readers learn independent patterns without interference
- [ ] Benchmark: measure serial vs parallel tree fetch latency for multi-join queries (standard S3 + Express)

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
