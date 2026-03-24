# turbolite Roadmap

(Formerly `sqlite-compress-encrypt-vfs`, aka `sqlces`)

## Current Status

Page-group tiered storage with seekable sub-chunk range GETs. S3/Tigris is source of truth, local disk is a page-level LRU cache. Default 64KB pages, 256 pages per group (~16MB uncompressed, ~8MB compressed). Seekable zstd encoding enables byte-range GETs for individual sub-chunks (~256KB) without downloading entire groups.

Two-tier encryption: AES-256-GCM with random nonces for S3 (authenticated, tamper-detecting), AES-256-CTR for local cache/WAL (zero overhead, OS page alignment). One key encrypts everything.

1M-row social media dataset (1.46GB, 91 page groups, 64KB pages):
- Arctic point lookup: 143ms (true cold start, zero cache)
- Cold point lookup: 23ms (interior + index pages cached)
- Warm point lookup: 98μs

Lazy background index prefetch, page-size-aware bundle chunking (46 index chunks), index page bitmap survival across cache eviction. 37 S3 integration tests + 164 unit tests passing.

---

## Ypres: Encryption Key Rotation
> After: Verdun · Before: Marne

Re-encrypt all S3 data with a new key. The local VFS already has dictionary rotation via `compact_with_recompression()` — this extends the pattern to encryption keys in tiered mode.

### Flow
- [ ] `TieredVfs::rotate_key(old_key, new_key)` — one-shot key rotation
- [ ] Download each page group, decrypt with old key, re-encrypt with new key, upload as new version
- [ ] Same for interior bundles and index bundles
- [ ] Write new manifest (with new version)
- [ ] Re-encrypt local cache file pages in place (read with old CTR nonce, write with new CTR nonce)
- [ ] Re-encrypt SubChunkTracker persistence file
- [ ] Old S3 objects added to `replaced_keys` for GC
- [ ] Test: rotate key, cold read with new key succeeds
- [ ] Test: cold read with old key after rotation fails
- [ ] Test: rotation is atomic — manifest swap is the commit point

### Future
- [ ] `from_password(password, salt)` — Argon2id KDF convenience method. Salt stored in manifest or dedicated S3 object.
- [ ] Local cache nonce hardening: random nonce per page write, stored in a per-page counter file. Eliminates CTR nonce reuse for multi-snapshot attackers.
- [ ] WAL nonce hardening: per-truncation generation counter persisted alongside WAL. Eliminates CTR nonce reuse across WAL checkpoint cycles.

---

## Marne: Dirty Page Memory Optimization
> After: Ypres · Before: Thermopylae

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

## Normandy: SQLite Loadable Extension + Language Packages
> After: Stalingrad · Before: Inchon

Ship turbolite as a SQLite loadable extension so Python/Go/Ruby/etc. users can `load_extension()` into their existing `sqlite3` connections. The extension registers the compressed VFS and exposes SQL helper functions. Language packages bundle the platform-specific binary and provide a `load(conn)` helper (sqlite-vec pattern).

### a. Loadable extension core
- [x] C shim (`src/ext_entry.c`): `sqlite3_turbolite_init` entry point using `sqlite3ext.h` + `SQLITE_EXTENSION_INIT1/2` macros. Receives the extension API table and routes `sqlite3_vfs_register` through it.
- [x] Rust `ext_init()` called from C shim: registers compressed VFS via the extension API's `vfs_register` (not the direct C symbol — loadable extensions must use the API table)
- [x] SQL functions registered by the extension:
  - `turbolite_version()` → returns version string
- [ ] `SELECT turbolite_register('vfs_name', '/path/to/base', 3)` → registers a compressed VFS with given name, base directory, and zstd level
- [x] `Cargo.toml`: `loadable-extension` feature, `cc` build dep, conditional build.rs
- [x] Makefile target: `make ext` builds the loadable extension .so/.dylib
- [x] Vendored `sqlite3.h`/`sqlite3ext.h` in `vendor/sqlite3/` (macOS SDK defines `SQLITE_OMIT_LOAD_EXTENSION`)

### b. Tests — loadable extension
- [x] `sqlite3_turbolite_init` loads successfully, VFS is registered
- [x] `turbolite_version()` SQL function returns correct version
- [x] Integration: Python `sqlite3.load_extension()` → open compressed DB via URI `?vfs=turbolite` → full CRUD roundtrip
- [x] Data persists across close/reopen
- [x] Multiple tables, UPDATE, DELETE, transaction rollback
- [x] All column types (NULL, integer, float, text, blob)
- [x] Index creation and index-based lookups
- [x] Large text/blob values, unicode roundtrip
- [x] Edge: load extension twice → idempotent (no crash)
- [x] Edge: file on disk has compressed magic (SQLCEvfS)
- [x] Edge: uncompressed DB not readable with turbolite VFS
- [ ] Integration: C program loads extension → registers VFS → roundtrip

### c. Python package (sqlite-vec pattern)
- [x] Rewrite `packages/python/` to sqlite-vec pattern: pure Python package that bundles the platform .so/.dylib
- [x] `turbolite/__init__.py`: `load(conn)` helper that finds bundled binary and calls `conn.load_extension(path)`
- [x] `turbolite/__init__.py`: `connect(path, vfs="turbolite")` convenience wrapper
- [x] Platform binary bundled at `turbolite/turbolite.dylib` (or `.so`)
- [x] `pyproject.toml`: setuptools with package-data for .so/.dylib
- [x] Test: `import turbolite; turbolite.load(conn)` → VFS available → compressed CRUD roundtrip
- [x] Test: `turbolite.connect()` convenience → works end to end
- [ ] Test: missing .so → clear error message

### d. Node package (wrapped approach — better-sqlite3 can't select VFS via URI)
- [ ] Keep existing napi-rs `Database` wrapper in `packages/node/`
- [ ] Document why: better-sqlite3 compiles with `SQLITE_USE_URI=0`, no VFS selection via URI
- [ ] If better-sqlite3 adds URI support in the future, switch to loadable extension pattern

### e. Cross-compile CI
- [ ] GitHub Actions workflow: build loadable extension for linux-x86_64, linux-aarch64, darwin-x86_64, darwin-aarch64
- [ ] Publish platform-specific Python wheels to PyPI
- [ ] Publish Node package to npm
- [ ] pkg-config `.pc` file for system install discovery

---

## Inchon: Rename to turbolite
> After: Normandy · Before: Somme

Rename project from `sqlite-compress-encrypt-vfs` / `sqlces` to `turbolite`.

- Directory rename
- `Cargo.toml`: package name
- All `use sqlite_compress_encrypt_vfs::` → `use turbolite::` in bin/, tests/
- Binary names: `sqlces` → `turbolite`
- Soup project, Fly app name

---

## Somme: Built-in WAL Shipping
> After: Inchon · Before: (future)

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

## Future

### mmap cache
The local cache currently uses `pread`/`pwrite`. Memory-mapping the cache file would eliminate syscall overhead for warm reads — the kernel serves pages directly from the page cache.

- [ ] `mmap` the cache file instead of `pread` for reads
- [ ] Keep `pwrite` for cache population (writing pages from S3 fetches)
- [ ] `madvise(MADV_RANDOM)` — access pattern is random, not sequential
- [ ] Handle cache file growth: `mremap` on Linux, re-map on macOS
- [ ] Benchmark: warm lookup latency with mmap vs pread (expect ~10-50us → ~1-5us)

### CLI + rename
The project is still `sqlite-compress-encrypt-vfs` / `sqlces` in code. Ship the rename alongside a usable CLI.

- [ ] Rename: directory, `Cargo.toml` package name, all `use` paths, binary names
- [ ] `turbolite bench` — move tiered-bench into a CLI subcommand
- [ ] `turbolite gc --bucket X --prefix Y` — one-shot GC (currently in Thermopylae roadmap)
- [ ] `turbolite import --bucket X --prefix Y --db local.db` — import a local SQLite DB to S3
- [ ] `turbolite info --bucket X --prefix Y` — print manifest summary (page count, groups, size)
- [ ] Soup project rename: `sqlces` → `turbolite`

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

### Write buffer groups + hot/cold page separation
If Stalingrad's group size tuning and freelist pre-allocation aren't enough for very large write-heavy databases (100GB+), the next step is page indirection: instead of encoding dirty pages back into their original groups, put all dirty pages into dedicated write buffer groups. Original groups stay clean and don't get re-uploaded. With 64KB pages, the manifest indirection is ~200KB for a 27K-page database — trivially small.

This is essentially a tiny LSM at the page-group level. Write buffers accumulate dirty pages across checkpoints, periodic compaction merges them back into contiguous groups. Could also enable hot/cold separation: track write frequency per page, concentrate hot pages in small frequently-uploaded groups and cold pages in large rarely-touched groups.
