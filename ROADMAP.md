# turbolite Roadmap

(Formerly `sqlite-compress-encrypt-vfs`, aka `sqlces`)

## Current Status

Page-group tiered storage with seekable sub-chunk range GETs. S3/Tigris is source of truth, local disk is a page-level LRU cache. Default 4096 pages per group (~16MB uncompressed, ~8MB compressed). Seekable zstd encoding enables byte-range GETs for individual sub-chunks (~100KB) without downloading entire groups.

Cold point lookup: 40ms (2 S3 requests, 400KB). Warm: 37us. Full table scan (812MB): ~500ms on 8 threads.

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

## Phase 14: Rename to turbolite

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
