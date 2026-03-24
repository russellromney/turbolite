# turbolite Roadmap

(Formerly `sqlite-compress-encrypt-vfs`, aka `sqlces`)

## Current Status

Page-group tiered storage with seekable sub-chunk range GETs. S3/Tigris is source of truth, local disk is a page-level LRU cache. Default 64KB pages, 256 pages per group (~16MB uncompressed, ~8MB compressed). Seekable zstd encoding enables byte-range GETs for individual sub-chunks (~256KB) without downloading entire groups.

1M-row social media dataset (1.46GB, 91 page groups, 64KB pages):
- Arctic point lookup: 143ms (true cold start, zero cache)
- Cold point lookup: 23ms (interior + index pages cached)
- Warm point lookup: 98μs

Lazy background index prefetch, page-size-aware bundle chunking (46 index chunks), index page bitmap survival across cache eviction. 34 S3 integration tests + 138 unit tests passing.

---

## Thermopylae: Tunable GC + Autovacuum Integration
> After: (start) · Before: Marathon

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

---

## Marathon: Local Disk Compaction
> After: Thermopylae · Before: Cannae

The local cache file is a sparse file sized to `page_count * page_size`. After VACUUM reduces page_count, a fresh reader creates a smaller cache, but the existing cache doesn't shrink in-place.

- [ ] On checkpoint, if manifest page_count decreased, truncate cache file to match
- [ ] Add `TieredVfs::compact_cache()` — shrink cache file to current manifest size
- [ ] Test: VACUUM → checkpoint → verify cache file size matches new page_count

Note: this is a minor optimization. S3 is the source of truth, local disk is ephemeral cache. A restart naturally right-sizes the cache.

---

## Cannae: 64KB Pages + Sub-Chunk Caching Model (DONE)
> After: Marathon · Before: Agincourt

The fundamental reframe: **optimize for S3 request count, not data size.** A range GET for 64KB costs the same as 4KB — one S3 request. Everything should follow from this.

### a. 64KB pages as default for tiered mode (DONE)
- [x] Default `PRAGMA page_size=65536` for tiered VFS (smaller page sizes remain an option)
- [x] At 64KB pages, 500k-post dataset = 11,569 pages (vs ~104,000 at 4KB) — 9x fewer
- [x] B-tree fan-out increases ~16x → shallower trees → fewer S3 hops per lookup (14 interior pages for 500k vs 617)
- [x] Overflow threshold jumps from ~1KB to ~16KB — most rows fit in a single page
- [x] Default group size: 256 pages per group = 16MB (same physical size as old 4096 * 4KB)
- [x] Default sub-chunk frame: 4 pages per frame = 256KB uncompressed per range GET

### b. Sub-chunk caching (DONE)

Sub-chunk caching model fully implemented:
- `SubChunkId(group_id, frame_index)` + `SubChunkTier(Pinned/Index/Data)` + `SubChunkTracker` with tiered LRU eviction
- DiskCache uses tracker alongside legacy PageBitmap for backward compat
- `write_pages_bulk` (S3 frames) marks sub-chunks; `write_page` (individual) marks bitmap only
- Read path detects page types: 0x05/0x02 → Pinned, 0x0A → Index, else Data
- `clear_cache` removes Data+Index; `clear_cache_data_only` removes Data only; `clear_cache_all` clears everything
- Tracker persists to disk with tier info, reloads on restart
- 25 unit tests covering math, tier promotion, eviction order, LRU, persistence, DiskCache integration

### c. Remaining tests
- [ ] Benchmark: 64KB vs 4KB pages on same dataset — cold point lookup, scan, write amplification
- [ ] Test: evicted sub-chunk → re-fetch from S3 → stale disk bytes correctly overwritten
- [ ] Test: mixed page sizes (verify 4KB still works as non-default option)

---

## Agincourt: Index Bundles (DONE)
> After: Cannae · Before: Midway

Index access patterns are fundamentally different from data access: once you touch an index, you're scanning most or all of it. Data pages are point lookups — fetch exactly what you need. Indexes want minimum time to having the entire index cached.

This is the interior bundle pattern extended to index leaves. Interior bundles already proved the concept: separate S3 storage, eager parallel fetch, permanent pinning.

### Implementation (DONE)

- Manifest field `index_chunk_keys: HashMap<u32, String>` — page-size-aware chunking via `bundle_chunk_range()`
- S3 key format: `{prefix}/ixb/{chunk_id}_v{version}`
- Index leaf pages (0x0A) detected at checkpoint, collected from dirty snapshot + cache's Index-tier sub-chunks
- Same `encode_interior_bundle`/`decode_interior_bundle` format (reused, not duplicated)
- Lazy background prefetch: VFS open spawns background thread, first query serves index pages from data groups via inline range GET
- Index pages survive cache eviction via `index_pages` bitmap re-mark in `clear_cache`/`clear_cache_data_only`
- GC includes index chunk keys in live key set
- Import path also collects and uploads index leaf bundles
- **Skip redundant page group uploads**: groups where ALL dirty pages are interior (0x05/0x02) or index leaf (0x0A) are skipped — those pages are served from bundles

### Remaining tests
- [ ] Benchmark: cold indexed lookup with/without index bundles (expect ~50% latency reduction)
- [ ] Test: REINDEX → checkpoint → verify no data page group uploads (only index bundles)
- [ ] Test: interior page split → checkpoint → verify no data page group re-upload

---

## Midway: Two-Layer Index Storage (Base + Delta)
> After: Agincourt · Before: Stalingrad

Index chunks are currently rewritten in full on every checkpoint that touches an index page. A single dirty index page forces re-upload of the entire ~14MB chunk. This is the write amplification bottleneck for index-heavy workloads (frequent INSERTs into indexed tables).

### Design: base chunks + per-page deltas

Two layers, manifest as the merge point:

- **Base layer**: dense-packed index chunks (~32MB uncompressed, ~8 chunks for 1M rows). Written at import and compaction. Efficient for cold bulk load (one round trip on 8 threads).
- **Delta layer**: individual per-page S3 objects (64KB each). Written at checkpoint for dirty index pages only. Manifest maps `page_num → S3 key` for overrides.
- **Read path**: check `index_page_overrides` in manifest first, fall back to base chunk.
- **Compaction**: when delta count exceeds threshold (e.g., 500 pages or 50% of base size), merge deltas into new base chunks, GC old base + deltas.

### Economics

- Write: 1 dirty index page = 1 PUT of 64KB + manifest update. Was: re-encode + re-upload 14MB chunk.
- Read (cold): fetch 8 base chunks (1 round trip) + N delta pages (1 more round trip if <8 deltas). Negligible overhead.
- Manifest: at 64KB pages, 500 delta entries = ~40KB. Trivially small.
- S3 cost: PUTs are per-operation not per-byte. 50 individual 64KB PUTs costs the same as 50 batched PUTs.

### Implementation

- [ ] Add `index_page_overrides: HashMap<u64, String>` to Manifest
- [ ] S3 key format: `{prefix}/ixp/{page_num}_v{version}` (individual index pages)
- [ ] Checkpoint: dirty index pages uploaded as individual objects, added to `index_page_overrides`
- [ ] Skip chunk rewrite if only delta pages changed in that chunk's range
- [ ] Read path: `index_page_overrides` lookup before base chunk fetch
- [ ] Background prefetch: fetch base chunks in parallel, then fetch any delta pages
- [ ] Compaction trigger: delta count > threshold → merge into new base chunks
- [ ] Compaction: read base chunks + deltas → dense-pack → upload new base chunks → clear deltas → update manifest
- [ ] GC: old base chunks + consumed delta objects added to replaced_keys
- [ ] Tests: checkpoint with index updates → verify only delta objects uploaded, cold read correctness, compaction merges correctly

### Dense packing (deferred)

Current chunking uses page-number-range bucketing which produces uneven chunks. Dense packing (collect all index pages, split every N) would give exactly `ceil(total_index_pages / pages_per_chunk)` chunks regardless of page number distribution. Implement when the range-based approach proves insufficient.

---

## Stalingrad: Write Amplification Optimization
> After: Midway · Before: Normandy

turbolite's unit of write is the page group (16MB at 256 * 64KB pages). A single dirty page forces re-upload of the entire group. Write amplification = dirty groups / dirty pages.

With 64KB pages, group counts are naturally low: 1.7GB = 104 groups, so worst-case is 104 PUTs per checkpoint. This is fine for small databases. For larger databases the math changes: 10GB = 600 groups, 100GB = 6,000 groups — scattered writes at that scale mean thousands of 16MB uploads per checkpoint.

### a. Group size tuning
- [ ] Make `pages_per_group` a meaningful tuning knob with documented tradeoffs
- [ ] Write-heavy workloads: smaller groups (64-128 pages at 64KB = 4-8MB) → less wasted bandwidth per dirty page, more PUTs
- [ ] Scan-heavy workloads: larger groups (512+ pages at 64KB = 32MB+) → fewer S3 objects, amortize request overhead
- [ ] Point lookups: group size mostly irrelevant (seekable sub-chunk range GETs)
- [ ] Benchmark write amplification at different group sizes with INSERT/UPDATE/DELETE workloads

### b. Freelist pre-allocation (S3 over-provisioning)
Low priority for databases under a few GB (group count is naturally small). Matters for 10GB+ write-heavy workloads.

- [ ] After import or VACUUM, extend the database with "write buffer" groups — groups that are entirely freelist pages
- [ ] SQLite's allocator pulls from the freelist for new INSERTs, so writes naturally concentrate in these groups
- [ ] Freelist ordering: rewrite freelist trunk pages at checkpoint so free pages are ordered by group (SQLite exhausts one group before moving to the next)
- [ ] Economics at scale: 100GB database, 100 scattered dirty pages → 100 groups dirty (1.6GB uploaded) vs 2 groups with pre-alloc (32MB uploaded). At small scale (1.7GB, 104 groups max) the ceiling is low enough that this barely matters.

### c. Write amplification metrics
- [ ] Track per-checkpoint: dirty pages, dirty groups, bytes uploaded, write amplification ratio
- [ ] Surface in logs and optionally in manifest metadata
- [ ] Use to validate that group size tuning and freelist pre-allocation are helping

---

## Normandy: Shared Library Distribution (.so / .dylib)
> After: Stalingrad · Before: Inchon

Offer turbolite as a shared library for C FFI consumers (Python ctypes, Go cgo, Node ffi-napi, etc.).

### a. C FFI layer (DONE)
- [x] `src/ffi.rs` — `extern "C"` functions for VFS registration + error handling
- [x] `turbolite_register_compressed`, `turbolite_register_passthrough`, `turbolite_register_encrypted`
- [x] `turbolite_register_tiered` (feature-gated)
- [x] `turbolite_last_error` — thread-local error string
- [x] `turbolite_clear_caches`, `turbolite_invalidate_cache`

### b. Build infrastructure (DONE)
- [x] `Cargo.toml`: `crate-type = ["lib", "cdylib"]`, `bundled-sqlite` feature flag
- [x] `cbindgen.toml` + `make header` → generates `turbolite.h`
- [x] `Makefile`: `make lib` (system SQLite), `make lib-bundled` (self-contained), `make install`

### c. Remaining
- [ ] SQLite loadable extension (`sqlite3_turbolite_init` entry point) — enables `SELECT load_extension('turbolite')`
- [ ] Cross-compile CI: build .so/.dylib for linux-x86_64, linux-aarch64, darwin-x86_64, darwin-aarch64
- [ ] Python wheel wrapping the .so (turbolite-python)
- [ ] pkg-config `.pc` file for system install discovery

---

## Inchon: Rename to turbolite
> After: Normandy · Before: (future)

Rename project from `sqlite-compress-encrypt-vfs` / `sqlces` to `turbolite`.

### Files to update
- Directory rename
- `Cargo.toml`: package name
- All `use sqlite_compress_encrypt_vfs::` → `use turbolite::` in bin/, tests/
- Binary names: `sqlces` → `turbolite`
- Soup project, Fly app name

---

## Future

### Encryption in tiered mode
- [ ] Compose page-level encrypt/decrypt into TieredHandle read/write path

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

### WAL replication
- [ ] Replicate WAL to S3 for zero-durability-gap (layer walrust/LiteStream on top)

### Write buffer groups + hot/cold page separation
If Stalingrad's group size tuning and freelist pre-allocation aren't enough for very large write-heavy databases (100GB+), the next step is page indirection: instead of encoding dirty pages back into their original groups, put all dirty pages into dedicated write buffer groups. Original groups stay clean and don't get re-uploaded. With 64KB pages, the manifest indirection is ~200KB for a 27K-page database — trivially small.

This is essentially a tiny LSM at the page-group level. Write buffers accumulate dirty pages across checkpoints, periodic compaction merges them back into contiguous groups. Could also enable hot/cold separation: track write frequency per page, concentrate hot pages in small frequently-uploaded groups and cold pages in large rarely-touched groups.
