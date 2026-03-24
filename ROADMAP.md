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
> After: Thermopylae · Before: Midway

The local cache file is a sparse file sized to `page_count * page_size`. After VACUUM reduces page_count, a fresh reader creates a smaller cache, but the existing cache doesn't shrink in-place.

- [ ] On checkpoint, if manifest page_count decreased, truncate cache file to match
- [ ] Add `TieredVfs::compact_cache()` — shrink cache file to current manifest size
- [ ] Test: VACUUM → checkpoint → verify cache file size matches new page_count

Note: this is a minor optimization. S3 is the source of truth, local disk is ephemeral cache. A restart naturally right-sizes the cache.

---

## Midway: Two-Layer Index Storage (Base + Delta)
> After: Marathon · Before: Stalingrad

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

- [ ] SQLite loadable extension (`sqlite3_turbolite_init` entry point) — enables `SELECT load_extension('turbolite')`
- [ ] Cross-compile CI: build .so/.dylib for linux-x86_64, linux-aarch64, darwin-x86_64, darwin-aarch64
- [ ] Python wheel wrapping the .so (turbolite-python)
- [ ] pkg-config `.pc` file for system install discovery

---

## Inchon: Rename to turbolite
> After: Normandy · Before: (future)

Rename project from `sqlite-compress-encrypt-vfs` / `sqlces` to `turbolite`.

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
