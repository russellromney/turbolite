# turbolite Roadmap

(Formerly `sqlite-compress-encrypt-vfs`, aka `sqlces`)

## Current Status

Page-group tiered storage with seekable sub-chunk range GETs. S3/Tigris is source of truth, local disk is a page-level LRU cache. Default 64KB pages, 256 pages per group (~16MB uncompressed, ~8MB compressed). Seekable zstd encoding enables byte-range GETs for individual sub-chunks (~256KB) without downloading entire groups.

Two-tier encryption: AES-256-GCM with random nonces for S3 (authenticated, tamper-detecting), AES-256-CTR for local cache/WAL (zero overhead, OS page alignment). One key encrypts everything. `rotate_encryption_key()` supports key rotation, adding encryption, and removing encryption (decrypt/re-encrypt without decompression, atomic manifest swap, post-upload verification).

1M-row social media dataset (1.46GB, 91 page groups, 64KB pages, EC2 c5.2xlarge, S3 Express One Zone, same AZ):
- Cache: none — point lookup: 75ms, 5-join profile: 202ms
- Cache: interior — point lookup: 11ms, 5-join profile: 134ms
- Cache: index — point lookup: 11ms, 5-join profile: 113ms
- Cache: data — point lookup: 157us, 5-join profile: 301us

Lazy background index prefetch, page-size-aware bundle chunking (46 index chunks), index page bitmap survival across cache eviction. 43 S3 integration tests + 180 unit tests passing.

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
> After: Thermopylae · Before: Midway (B-Tree-Aware Page Groups)

The local cache file is a sparse file sized to `page_count * page_size`. After VACUUM reduces page_count, a fresh reader creates a smaller cache, but the existing cache doesn't shrink in-place.

- [ ] On checkpoint, if manifest page_count decreased, truncate cache file to match
- [ ] Add `TieredVfs::compact_cache()` — shrink cache file to current manifest size
- [ ] Test: VACUUM → checkpoint → verify cache file size matches new page_count

Note: this is a minor optimization. S3 is the source of truth, local disk is ephemeral cache. A restart naturally right-sizes the cache.

---

## Midway: B-Tree-Aware Page Groups
> After: Marathon · Before: Normandy

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
- [ ] Read `sqlite_master` to get root pages, names, types (table vs index)
- [ ] Walk each B-tree from root to enumerate all pages (interior + leaf + overflow)
- [ ] Handle freelist pages (pack separately, or skip upload since they hold no useful data)
- [ ] Handle page 1 (sqlite_master) and other system pages

#### b. Manifest format change
- [ ] Replace positional page-to-group mapping with explicit mapping
- [ ] Add `btrees` section: root_page -> { name, type, groups }
- [ ] Add `pages` list per group (ordered, defines position within group)
- [ ] Build reverse index on manifest load: page_num -> (group_id, position)
- [ ] Automigrate: detect old positional manifest, convert to explicit on first checkpoint

#### c. B-tree-aware packing (import)
- [ ] Pack pages by B-tree into groups during import
- [ ] Large B-trees (> pages_per_group pages) span multiple groups
- [ ] Small B-trees (< ~50 pages) share a group
- [ ] Leave slack per group for future growth (~200 of 256 slots)
- [ ] Existing seekable zstd sub-chunk encoding works unchanged (position is per-group, not per-page-number)

#### d. Demand-driven prefetch
- [ ] On root page read, look up B-tree's groups in manifest
- [ ] Prefetch those groups (whole-group GETs, not scattered sub-chunk range GETs)
- [ ] Remove current "prefetch all index chunks" behavior
- [ ] Optional: access history (track which B-trees were accessed, prefetch those eagerly on next open)

#### e. Checkpoint with B-tree awareness
- [ ] Re-walk B-trees at checkpoint to rebuild page-to-btree map
- [ ] New pages: append to B-tree's last group if slack available, else create new group
- [ ] Freed pages: mark as dead space in group
- [ ] Only re-encode and upload groups containing dirty pages
- [ ] Write amplification metrics: track dirty pages, dirty groups, bytes uploaded per checkpoint

#### f. Compaction
- [ ] Trigger: B-tree's groups have > 30% dead space, or total waste exceeds threshold
- [ ] Repack: read all pages for B-tree, dense-pack into new groups, upload, update manifest
- [ ] GC old groups after manifest swap
- [ ] VACUUM triggers full repack (all page numbers change)

#### g. Tests
- [ ] Import: verify pages packed by B-tree, manifest mapping correct
- [ ] Read: page lookup via explicit mapping matches page content
- [ ] Prefetch: root page access triggers only relevant B-tree's group fetches
- [ ] Checkpoint: new pages packed into correct B-tree's groups, only dirty groups re-uploaded
- [ ] Write amplification: INSERT into indexed table dirties fewer groups than positional packing
- [ ] Compaction: dead space reclaimed, B-tree groups repacked optimally
- [ ] VACUUM: full repack produces correct mapping
- [ ] Automigration: old positional manifest upgraded to explicit on first checkpoint

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
> After: Gallipoli · Before: (future)

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

### Predictive cross-tree prefetch
Depends on Phase Midway (B-tree-aware page groups). B-tree packing solves "fetch one tree fast" (~5ms). But multi-join queries touch 3-4 trees sequentially. Without prediction, each tree is fetched on-demand as SQLite traverses into it: posts index (5ms), then users data (5ms), then likes index (5ms). Serial chain = 15ms of waiting.

Predictive prefetch eliminates the serial chain. When SQLite reads the posts root page, the prediction table fires "this pattern always needs users data + likes index too" and all three trees prefetch in parallel. 15ms serial collapses to 5ms parallel. The difference between "fast" and "feels local."

**Observation layer.** The VFS sees every `read_exact_at` call. Record page access bursts (clustered reads separated by idle gaps) in a ring buffer: `[(page_num, timestamp), ...]`. With the B-tree map from Midway, translate page numbers into B-tree identities (root page numbers). Each burst becomes a B-tree access sequence.

**Prediction table.** Derived from the ring buffer with time-weighted aggregation. Key = first K page reads of a burst (the access fingerprint). Value = full set of B-tree groups needed. When a new burst starts and the first 2-3 page reads match a known fingerprint, speculatively prefetch all predicted groups in parallel.

**Confidence scoring with decay.**
- Time decay: older observations lose weight naturally
- Write decay: when a page in B-tree X is dirtied, multiply X's prediction confidence by a factor (e.g., 0.7). A single delete barely hurts. Bulk deletes fade predictions to zero.
- Reinforcement: when a prediction fires and the pages ARE subsequently requested, bump confidence. Correct predictions get stronger.
- At checkpoint: rebuild the ring buffer baseline. Predictions that survived write decay carry forward; stale ones are dropped.

**Graceful degradation.** If predictions are wrong or stale, the fallback is demand-driven prefetch from Midway (still fast, ~5ms per tree). Predictions are purely additive. The workloads where predictions matter most (repeated query patterns, read-heavy) are the workloads where they're most stable.

- [ ] Ring buffer for page access bursts with timestamps
- [ ] B-tree access sequence derivation from page reads + Midway B-tree map
- [ ] Prediction table: access fingerprint -> predicted B-tree groups
- [ ] Confidence scoring: time decay + write decay + reinforcement
- [ ] Speculative parallel prefetch when fingerprint matches
- [ ] Persistence: store prediction table in manifest sidecar, rebuilt after VACUUM/schema change
- [ ] Benchmark: measure serial vs parallel tree fetch latency for multi-join queries
