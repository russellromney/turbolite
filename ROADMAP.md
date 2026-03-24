# turbolite Roadmap

(Formerly `sqlite-compress-encrypt-vfs`, aka `sqlces`)

## Current Status

Page-group tiered storage with seekable sub-chunk range GETs. S3/Tigris is source of truth, local disk is a page-level LRU cache. Default 64KB pages, 256 pages per group (~16MB uncompressed, ~8MB compressed). Seekable zstd encoding enables byte-range GETs for individual sub-chunks (~256KB) without downloading entire groups.

Cold point lookup: 36ms (1 S3 request, 18KB at 100k posts). Warm: 63μs. 500k-post dataset (723MB): 14 interior pages, 46 groups.

Post-checkpoint GC (`gc_enabled`) and full-scan GC (`TieredVfs::gc()`) implemented. 31 S3 integration tests + 84 unit tests passing.

---

## Phase 12: Tunable GC + Autovacuum Integration

### Tunable GC policy
- [ ] `gc_keep_versions: u32` — number of old page group versions to retain (default 0 = delete all). Enables point-in-time restore window.
- [ ] `gc_max_age_secs: u64` — delete old versions older than N seconds. Alternative to version count.
- [ ] Combine with existing `gc_enabled` flag: `gc_enabled=true, gc_keep_versions=5` keeps last 5 versions.

### Autovacuum-triggered GC
- [ ] Hook GC into SQLite's autovacuum: after incremental autovacuum frees pages and checkpoint flushes to S3, automatically GC orphaned page groups.
- [ ] `PRAGMA auto_vacuum=INCREMENTAL` + periodic `PRAGMA incremental_vacuum(N)` should "just work" through the VFS — verify with integration test.
- [ ] Test: enable autovacuum, insert/delete cycles, verify S3 object count stabilizes.

### CLI
- [ ] `turbolite gc --bucket X --prefix Y` — one-shot full GC scan
- [ ] `turbolite gc --dry-run` — list orphans without deleting

---

## Phase 13: Local Disk Compaction

The local cache file is a sparse file sized to `page_count * page_size`. After VACUUM reduces page_count, a fresh reader creates a smaller cache, but the existing cache doesn't shrink in-place.

- [ ] On checkpoint, if manifest page_count decreased, truncate cache file to match
- [ ] Add `TieredVfs::compact_cache()` — shrink cache file to current manifest size
- [ ] Test: VACUUM → checkpoint → verify cache file size matches new page_count

Note: this is a minor optimization. S3 is the source of truth, local disk is ephemeral cache. A restart naturally right-sizes the cache.

---

## Phase 14: 64KB Pages + Sub-Chunk Caching Model

The fundamental reframe: **optimize for S3 request count, not data size.** A range GET for 64KB costs the same as 4KB — one S3 request. Everything should follow from this.

### 64KB pages as default for tiered mode (DONE)
- [x] Default `PRAGMA page_size=65536` for tiered VFS (smaller page sizes remain an option)
- [x] At 64KB pages, 500k-post dataset = 11,569 pages (vs ~104,000 at 4KB) — 9x fewer
- [x] B-tree fan-out increases ~16x → shallower trees → fewer S3 hops per lookup (14 interior pages for 500k vs 617)
- [x] Overflow threshold jumps from ~1KB to ~16KB — most rows fit in a single page
- [x] Default group size: 256 pages per group = 16MB (same physical size as old 4096 * 4KB)
- [x] Default sub-chunk frame: 4 pages per frame = 256KB uncompressed per range GET

### Sub-chunk as the caching unit
- [ ] Replace per-page bitmap with per-sub-chunk tracking: `HashSet<(group_id, frame_index)>`
- [ ] 1.7GB at 64KB pages, 4 pages/frame = ~6,700 sub-chunks. Entire tracker fits in a few KB.
- [ ] Cache miss: S3 range GET for one frame → decompress → pwrite all pages to cache file at their page offsets → insert one entry in tracker
- [ ] Cache hit: check sub-chunk tracker → pread from cache file at page offset (O(1), same as today)
- [ ] Disk cache format unchanged — flat file, page N at offset N * page_size. Sub-chunk tracking is purely in-memory.
- [ ] Eviction: drop entire sub-chunks, not individual pages. Don't zero disk — stale bytes are harmless, overwritten on next fetch.

### Tiered sub-chunk eviction
- [ ] Tier 0 (pinned): interior page sub-chunks — never evicted
- [ ] Tier 1 (high priority): index leaf sub-chunks — evicted only when all tier 2 is gone
- [ ] Tier 2 (normal): data sub-chunks — standard LRU
- [ ] Evict from lowest tier first, LRU within each tier

### Why this simplifies everything downstream
- Manifest indirection (Phase 16): 26,700 page entries = ~200KB. Trivially small.
- Hot/cold tracking: sort ~6,700 sub-chunks by temperature. Trivial.
- Write buffer groups: tracking dirty sub-chunks is a tiny bitset.
- The "major architectural change" warnings in Phase 16 largely dissolve at this scale.

### Implementation (DONE)

Sub-chunk caching model fully implemented:
- `SubChunkId(group_id, frame_index)` + `SubChunkTier(Pinned/Index/Data)` + `SubChunkTracker` with tiered LRU eviction
- DiskCache uses tracker alongside legacy PageBitmap for backward compat
- `write_pages_bulk` (S3 frames) marks sub-chunks; `write_page` (individual) marks bitmap only
- Read path detects page types: 0x05/0x02 → Pinned, 0x0A → Index, else Data
- `clear_cache` removes Data+Index; `clear_cache_data_only` removes Data only; `clear_cache_all` clears everything
- Tracker persists to disk with tier info, reloads on restart
- 25 unit tests covering math, tier promotion, eviction order, LRU, persistence, DiskCache integration

### Remaining Tests
- [ ] Benchmark: 64KB vs 4KB pages on same dataset — cold point lookup, scan, write amplification
- [ ] Test: evicted sub-chunk → re-fetch from S3 → stale disk bytes correctly overwritten
- [ ] Test: mixed page sizes (verify 4KB still works as non-default option)

---

## Phase 15: Index Bundles (DONE)

Index access patterns are fundamentally different from data access: once you touch an index, you're scanning most or all of it. Data pages are point lookups — fetch exactly what you need. Indexes want minimum time to having the entire index cached.

This is the interior bundle pattern extended to index leaves. Interior bundles already proved the concept: separate S3 storage, eager parallel fetch, permanent pinning.

### Implementation (DONE)

- Manifest field `index_chunk_keys: HashMap<u32, String>` — same chunking as interior bundles (INTERIOR_CHUNK_RANGE)
- S3 key format: `{prefix}/ixb/{chunk_id}_v{version}`
- Index leaf pages (0x0A) detected at checkpoint, collected from dirty snapshot + cache's Index-tier sub-chunks
- Same `encode_interior_bundle`/`decode_interior_bundle` format (reused, not duplicated)
- `eager_index_load: bool` config (default true) — parallel fetch all index chunks on VFS open
- On cache hit: index pages promoted to Index tier via `mark_index_page()` (from Phase 14 sub-chunk tracker)
- GC includes index chunk keys in live key set
- Import path also collects and uploads index leaf bundles
- **Skip redundant page group uploads**: groups where ALL dirty pages are interior (0x05/0x02) or index leaf (0x0A) are skipped — those pages are served from bundles
- 2 S3 integration tests: checkpoint + cold read with eager loading, and eager_index_load=false fallback

### Remaining Tests
- [ ] Benchmark: cold indexed lookup with/without index bundles (expect ~50% latency reduction)
- [ ] Test: REINDEX → checkpoint → verify no data page group uploads (only index bundles)
- [ ] Test: interior page split → checkpoint → verify no data page group re-upload

---

## Phase 16: Write Amplification Optimization

turbolite's unit of write is the page group (16MB at 256 * 64KB pages). A single dirty page forces re-upload of the entire group. Write amplification = dirty groups / dirty pages.

With 64KB pages (Phase 14), group counts are naturally low: 1.7GB = 104 groups, so worst-case is 104 PUTs per checkpoint. This is fine for small databases. For larger databases the math changes: 10GB = 600 groups, 100GB = 6,000 groups — scattered writes at that scale mean thousands of 16MB uploads per checkpoint.

### Group size tuning
- [ ] Make `pages_per_group` a meaningful tuning knob with documented tradeoffs
- [ ] Write-heavy workloads: smaller groups (64-128 pages at 64KB = 4-8MB) → less wasted bandwidth per dirty page, more PUTs
- [ ] Scan-heavy workloads: larger groups (512+ pages at 64KB = 32MB+) → fewer S3 objects, amortize request overhead
- [ ] Point lookups: group size mostly irrelevant (seekable sub-chunk range GETs)
- [ ] Benchmark write amplification at different group sizes with INSERT/UPDATE/DELETE workloads

### Freelist pre-allocation (S3 over-provisioning)
Low priority for databases under a few GB (group count is naturally small). Matters for 10GB+ write-heavy workloads.

- [ ] After import or VACUUM, extend the database with "write buffer" groups — groups that are entirely freelist pages
- [ ] SQLite's allocator pulls from the freelist for new INSERTs, so writes naturally concentrate in these groups
- [ ] Freelist ordering: rewrite freelist trunk pages at checkpoint so free pages are ordered by group (SQLite exhausts one group before moving to the next)
- [ ] Economics at scale: 100GB database, 100 scattered dirty pages → 100 groups dirty (1.6GB uploaded) vs 2 groups with pre-alloc (32MB uploaded). At small scale (1.7GB, 104 groups max) the ceiling is low enough that this barely matters.

### Write amplification metrics
- [ ] Track per-checkpoint: dirty pages, dirty groups, bytes uploaded, write amplification ratio
- [ ] Surface in logs and optionally in manifest metadata
- [ ] Use to validate that group size tuning and freelist pre-allocation are helping

---

## Phase 17: Shared Library Distribution (.so / .dylib)

Offer turbolite as a shared library for C FFI consumers (Python ctypes, Go cgo, Node ffi-napi, etc.).

### C FFI layer (DONE)
- [x] `src/ffi.rs` — `extern "C"` functions for VFS registration + error handling
- [x] `turbolite_register_compressed`, `turbolite_register_passthrough`, `turbolite_register_encrypted`
- [x] `turbolite_register_tiered` (feature-gated)
- [x] `turbolite_last_error` — thread-local error string
- [x] `turbolite_clear_caches`, `turbolite_invalidate_cache`

### Build infrastructure (DONE)
- [x] `Cargo.toml`: `crate-type = ["lib", "cdylib"]`, `bundled-sqlite` feature flag
- [x] `cbindgen.toml` + `make header` → generates `turbolite.h`
- [x] `Makefile`: `make lib` (system SQLite), `make lib-bundled` (self-contained), `make install`

### Remaining
- [ ] SQLite loadable extension (`sqlite3_turbolite_init` entry point) — enables `SELECT load_extension('turbolite')`
- [ ] Cross-compile CI: build .so/.dylib for linux-x86_64, linux-aarch64, darwin-x86_64, darwin-aarch64
- [ ] Python wheel wrapping the .so (turbolite-python)
- [ ] pkg-config `.pc` file for system install discovery

---

## Phase 18: Rename to turbolite

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
If Phase 16's group size tuning and freelist pre-allocation aren't enough for very large write-heavy databases (100GB+), the next step is page indirection: instead of encoding dirty pages back into their original groups, put all dirty pages into dedicated write buffer groups. Original groups stay clean and don't get re-uploaded. With 64KB pages (Phase 14), the manifest indirection is ~200KB for a 27K-page database — trivially small.

This is essentially a tiny LSM at the page-group level. Write buffers accumulate dirty pages across checkpoints, periodic compaction merges them back into contiguous groups. Could also enable hot/cold separation: track write frequency per page, concentrate hot pages in small frequently-uploaded groups and cold pages in large rarely-touched groups.

The complexity cost is real (compaction logic, prefetch needs to be page-range-aware instead of group-sequential) but manageable at the scale 64KB pages give us. Worth doing if write amplification metrics from Phase 16 show it's needed.
