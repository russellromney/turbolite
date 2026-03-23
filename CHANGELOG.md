# turbolite Changelog

(Formerly `sqlite-compress-encrypt-vfs`, aka `sqlces`)

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
- `S3Client::delete_objects()` and `list_all_keys()` helpers

### Benchmark CLI
- `--queries` filter (e.g. `--queries post,profile`)
- `--modes` filter (e.g. `--modes cold,arctic`)
- `--skip-verify` to bypass COUNT(*) full scan on small machines

### Tests
- Removed all `#[ignore]` from 27 tiered integration tests (now run directly)
- Fixed `test_page_group_cache_populates` stale path assertion
- Added 4 GC tests: post-checkpoint, disabled, full-scan, no-orphans
- 31 S3 integration tests + 84 unit tests = 115 total, 0 failures

### README
- Rebranded to turbolite
- Consolidated design + architecture into single Design section
- Added tuning section with workload-specific prefetch configs

---

## Phase 6: Tiered v2 Hardening

Bug fixes and correctness improvements found during code review. 27 tests pass against Tigris.

### Bug fixes
- Fixed `delete()` calling `destroy_s3()` unconditionally — SQLite calls delete for WAL/journal files during VACUUM and journal mode switch, which destroyed the entire S3 dataset. Now only deletes local files.
- Fixed `exists()` returning true for WAL/journal/SHM files — it checked S3 manifest for all files, causing SQLite to enter recovery mode in DELETE journal mode and skip writing pages to the main DB.
- Fixed chunk_size mismatch → silent corruption — `open()` now uses manifest's chunk_size for existing databases, not config. Only config chunk_size for new DBs.
- Fixed cache serving stale pages after truncation — moved page_count bounds check before cache lookup in `read_exact_at`.
- Fixed dirty chunks evicted during read-path — `put_chunk` in read path now passes dirty chunk IDs to eviction.
- Fixed `Manifest::empty()` hardcoding chunk_size=128 — now uses 0, allowing custom chunk_size to be recorded on first write.

### Improvements
- Batch S3 deletes in `destroy_s3` — uses `DeleteObjects` in batches of 1000 instead of one-by-one.
- LRU rebuild on restart uses file mtime instead of `Instant::now()` — preserves eviction order across restarts.
- DELETE journal mode support — VFS now works in both WAL and DELETE modes.

### Tests added
- `test_delete_preserves_s3` — connection close doesn't destroy S3 data
- `test_delete_journal_mode` — full write/read/cold-read cycle in DELETE mode
- `test_chunk_size_mismatch_uses_manifest` — manifest chunk_size overrides config
- `test_oltp_with_indexes` — 3 indexes, INSERT/UPDATE/DELETE, cold queries
- `test_update_delete_operations` — UPDATE/DELETE across checkpoints
- `test_multiple_tables` — 4 tables + JOIN queries
- `test_custom_chunk_size` — chunk_size=8, manifest validation
- `test_large_overflow_blobs` — overflow pages with blobs up to 100KB
- `test_vacuum_reorganizes` — VACUUM page count reduction, cold read

---

## Phase 5: S3-Backed Tiered Storage

Page-level tiered storage where S3 is source of truth and local disk is a cache. Inspired by turbopuffer. Designed for append-oriented workloads (time-series, logs, streams).

### Core implementation
- Extracted compression/encryption to `src/compress.rs` free functions
- Added `tiered` feature flag + aws-sdk-s3/tokio/crc32fast deps
- Implemented `TieredVfs`, `TieredHandle`, `S3Client`, `DiskCache` in `src/tiered.rs`
- 10 integration tests (basic, checkpoint, reader, 64kb, cold scan, append, read-only, cache-clear, prefetch, destroy_s3)
- 1 Tigris integration test (real S3, `TIGRIS_TEST=1`)

### Bug fixes
- Fixed `sync()` — manifest version only increments after successful S3 upload (prevents version drift on failure)
- Added 3-retry with exponential backoff to `put_manifest()`
- Fixed `DiskCache::put()` .tmp file leak on rename failure
- Fixed `exists()` to check local cache first, then S3
- Fixed read-only WAL passthrough — always create WAL/journal files locally (prevents `SQLITE_READONLY_CANTINIT`)

### Features
- Added zstd dictionary support to `TieredConfig`/`TieredHandle`
- Support `region: "auto"` for Tigris compatibility

---

## Phase 4: Client-Controlled Compaction Helpers

- `compact_if_needed(path, threshold_pct)` — compact if dead_space exceeds threshold

---

## Phase 2: Parallel Compression in Compaction

- `rayon` optional dependency with `parallel` feature flag
- `compact_with_recompression()` with `CompactionConfig` (compression level, dictionary, parallel toggle)
- Original `compact()` preserved for simple dead-space removal

---

## Phase 1: Dictionary Compression Integration

- Pre-compiled `EncoderDictionary`/`DecoderDictionary` in `CompressedHandle`
- VFS constructors: `new_with_dict()`, `compressed_encrypted_with_dict()`
- CLI commands: `embed-dict`, `extract-dict`

---

## Foundation

- Single magic "SQLCEvfS" format with inline page index
- Header with dict_size and flags fields
- zstd compression (levels 1-22)
- AES-256-GCM encryption
- WAL mode support
- Byte-range locking (SQLite protocol)
- Atomic write_end for concurrent access
- Debug lock tracing (`SQLCES_DEBUG_LOCKS=1`)
