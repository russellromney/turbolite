# turbolite Changelog

(Formerly `sqlite-compress-encrypt-vfs`, aka `sqlces`)

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
