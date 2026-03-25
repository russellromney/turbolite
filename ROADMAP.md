# turbolite Roadmap

## Tannenberg: lib.rs Split
> After: Kursk · Before: Marne (Memory)

Split `lib.rs` (2,595 lines) into focused modules. Tiered split is done (see CHANGELOG). lib.rs remains.

- [ ] `lib.rs` -- module declarations, public re-exports only
- [ ] `locks.rs` -- `InProcessLocks`, `SlotState`, `SHARED_FILE_CACHE`, lock/unlock functions, debug tracing
- [ ] `file_format.rs` -- `FileHeader`, `PageIndex`, magic bytes, constants
- [ ] `compressed_handle.rs` -- `CompressedHandle` struct, page ops, `DatabaseHandle` trait impl, `FileWalIndex`
- [ ] `compressed_vfs.rs` -- `CompressedVfs`, `Vfs` trait impl
- [ ] `maintenance.rs` -- `inspect_database()`, `compact()`, `compact_with_recompression()`, `CompactionConfig`
- [ ] `cargo test` passes with no changes to public API
- [ ] `make ext` builds successfully
- [ ] All `use turbolite::` paths in bin/, tests/, examples/ still compile

---

## Marne (Memory): Dirty Page Memory Optimization
> After: Tannenberg · Before: Thermopylae

`write_all_at()` stores a full page copy in `dirty_pages: HashMap<u64, Vec<u8>>` AND writes it to the cache file. The HashMap copy is only needed at checkpoint to know which groups to re-encode, but the data is already in the cache. Holding it twice wastes memory: 1000 dirty 64KB pages = 64MB in the HashMap alone.

- [ ] Replace `dirty_pages: HashMap<u64, Vec<u8>>` with `dirty_page_nums: HashSet<u64>` (8 bytes per page instead of 64KB)
- [ ] At checkpoint, read dirty pages back from cache file via `cache.read_page()` (microsecond pread, trivial vs S3 PUT)
- [ ] Remove the `dirty_snapshot.clone()` in `sync()` -- just clone the HashSet
- [ ] Update `read_exact_at()` to check cache instead of HashMap for dirty page reads (cache is already up-to-date from `write_all_at`)
- [ ] Test: write 1000 pages, verify memory usage doesn't scale with page count
- [ ] Test: checkpoint after dirty page optimization still produces correct S3 data

---

## Thermopylae: Tunable GC + Autovacuum Integration
> After: Marne (Memory) · Before: Marathon

### a. Tunable GC policy
- [ ] `gc_keep_versions: u32` -- number of old page group versions to retain (default 0 = delete all). Enables point-in-time restore window.
- [ ] `gc_max_age_secs: u64` -- delete old versions older than N seconds. Alternative to version count.
- [ ] Combine with existing `gc_enabled` flag: `gc_enabled=true, gc_keep_versions=5` keeps last 5 versions.

### b. Autovacuum-triggered GC
- [ ] Hook GC into SQLite's autovacuum: after incremental autovacuum frees pages and checkpoint flushes to S3, automatically GC orphaned page groups.
- [ ] `PRAGMA auto_vacuum=INCREMENTAL` + periodic `PRAGMA incremental_vacuum(N)` should "just work" through the VFS -- verify with integration test.
- [ ] Test: enable autovacuum, insert/delete cycles, verify S3 object count stabilizes.

### c. CLI
- [ ] `turbolite gc --bucket X --prefix Y` -- one-shot full GC scan
- [ ] `turbolite gc --dry-run` -- list orphans without deleting

### d. Msgpack manifest
- [ ] Replace JSON manifest with msgpack (`rmp-serde`). Smaller, faster serialize/deserialize.
- [ ] S3 key: `manifest.msgpack` (content type `application/msgpack`)
- [ ] Automigrate: `get_manifest` tries msgpack first, falls back to JSON. Next `put_manifest` writes msgpack. Old `manifest.json` cleaned up by GC.
- [ ] `SubChunkTracker` local persistence can stay JSON (local-only, not worth changing)

---

## Marathon: Local Disk Compaction
> After: Thermopylae · Before: Midway (remaining)

The local cache file is a sparse file sized to `page_count * page_size`. After VACUUM reduces page_count, a fresh reader creates a smaller cache, but the existing cache doesn't shrink in-place.

- [ ] On checkpoint, if manifest page_count decreased, truncate cache file to match
- [ ] Add `TieredVfs::compact_cache()` -- shrink cache file to current manifest size
- [ ] Test: VACUUM -> checkpoint -> verify cache file size matches new page_count

Note: minor optimization. S3 is the source of truth, local disk is ephemeral cache.

---

## Midway (remaining): GroupingStrategy + Compaction + Tests
> After: Marathon · Before: Gallipoli

Remaining items from B-Tree-Aware Page Groups (completed work in CHANGELOG).

### GroupingStrategy dispatch (f3)
- [ ] `GroupingStrategy` enum: dispatch methods compute positions arithmetically (Positional) or via explicit lookup (BTreeAware)
- [ ] `PrefetchNeighbors` enum: `RadialFanout` (positional) vs `BTreeSiblings` (btree)
- [ ] Manifest dispatch: `page_location()`, `group_page_nums()`, `prefetch_neighbors()` branch on `strategy`
- [ ] handle.rs: `trigger_prefetch()` radial fan-out for Positional, sibling groups for BTreeAware
- [ ] import.rs: Positional path (sequential chunking, no btree walk)
- [ ] disk_cache.rs + vfs.rs: handle empty `group_pages` for Positional
- [ ] Benchmark: `--grouping positional` and `--grouping btree` flags in tiered-bench

### Compaction (g)
- [ ] Trigger: B-tree's groups have > 30% dead space, or total waste exceeds threshold
- [ ] Repack: read all pages for B-tree, dense-pack into new groups, upload, update manifest
- [ ] GC old groups after manifest swap
- [ ] VACUUM triggers full repack (all page numbers change)

### Remaining tests (h)
- [ ] Re-walk B-trees at checkpoint to update mapping for new/moved pages
- [ ] Prefetch: root page access triggers only relevant B-tree's group fetches
- [ ] Checkpoint: new pages packed into correct B-tree's groups, only dirty groups re-uploaded
- [ ] Write amplification: INSERT into indexed table dirties fewer groups than positional packing
- [ ] Compaction: dead space reclaimed, B-tree groups repacked optimally
- [ ] VACUUM: full repack produces correct mapping

---

## Gallipoli: Normandy Leftovers
> After: Midway (remaining) · Before: Somme

- [ ] `SELECT turbolite_register('vfs_name', '/path/to/base', 3)` SQL function for runtime VFS registration
- [ ] Integration test: C program loads extension, registers VFS, roundtrip
- [ ] Test: missing .so in Python package produces clear error message
- [ ] pkg-config `.pc` file for system install discovery

---

## Somme: Built-in WAL Shipping
> After: Gallipoli · Before: Verdun (remaining)

Close the durability gap between checkpoints. Ship WAL frames to S3 in the background so writes are durable before checkpoint.

### a. WAL frame capture + upload
- [ ] Intercept WAL `write_all_at()` -- buffer frame bytes
- [ ] Batch frames into segments (every 100 frames or N ms) to amortize PUT cost
- [ ] Upload segments to `{prefix}/wal/{sequence_number}` via existing S3 client

### b. Recovery
- [ ] On open: after manifest download, list `{prefix}/wal/` objects newer than manifest version
- [ ] Replay WAL frames into local cache before serving queries
- [ ] Test: write data, kill before checkpoint, recover from S3 WAL segments

### c. Cleanup
- [ ] After checkpoint, WAL segments older than manifest version are garbage
- [ ] Integrate with existing GC

### d. WAL write callback (future)
- [ ] `TieredConfig::on_wal_write(fn(&[u8]))` -- optional hook for external tools

---

## Verdun (remaining): Integration Tests + Trie + Frame Correlation
> After: Somme · Before: Marne (Query Plan remaining)

Remaining items from Predictive Cross-Tree Prefetch (completed work in CHANGELOG).

### Integration tests (g2)

**Checkpoint roundtrip:**
- [ ] Prediction patterns survive checkpoint -> S3 manifest -> reopen
- [ ] Access history frequencies survive checkpoint -> reopen
- [ ] Checkpoint with no patterns produces empty prediction_patterns
- [ ] Checkpoint with prediction_enabled=false preserves existing patterns

**Real query prediction firing:**
- [ ] 3-table join: pattern learned after 2 lock sessions, fires on 3rd
- [ ] Prediction submits correct group IDs (verify via S3 fetch counters)
- [ ] Reinforcement fires when predicted groups are subsequently read

**Decay + write behavior:**
- [ ] Pattern decays below threshold after ~10 sessions without reinforcement
- [ ] Write decay: bulk INSERT drops confidence within 3 dirty sessions
- [ ] Read-only workload: patterns stabilize around 0.85-0.95
- [ ] Mixed workload: read patterns survive, write-heavy patterns fade

**Strategy edge cases:**
- [ ] Positional strategy: prediction fields exist but page_to_btree empty, no panics
- [ ] BTreeAware with single-table DB: no predictions fire, no overhead

**Negative tests:**
- [ ] Single-tree query never fires predictions
- [ ] Prediction with unknown B-tree root silently skipped
- [ ] max_patterns cap enforced

**Manifest bloat:**
- [ ] 100 unique patterns: manifest under 10KB
- [ ] 1000 patterns: prune reduces to max_patterns

**VACUUM / schema change:**
- [ ] After VACUUM: old patterns become no-ops, no crash
- [ ] After DROP TABLE + CREATE TABLE: handles missing roots
- [ ] After ADD INDEX: new B-tree learned in subsequent sessions

**Concurrency:**
- [ ] 4 concurrent readers: no deadlocks, all patterns recorded
- [ ] 2 readers + 1 writer: write decay only for writer's patterns

### Prediction benchmark (h)
- [ ] Add `--predicted` flag to tiered-bench
- [ ] Learning phase: run query suite N times, checkpoint
- [ ] A/B test with prediction on/off at each cache level
- [ ] Report prediction stats (patterns, fire rate, hit rate)
- [ ] Benchmark serial vs parallel tree fetch latency for multi-join queries

### Trie storage (i3)
- [ ] `PredictionTrie` struct: sorted trie keyed by tree name
- [ ] Trie insert, lookup (K=2), observe, reinforce, prune
- [ ] Pair index elimination (trie IS the index)
- [ ] Serialization: `to_persisted()` / `from_persisted()`
- [ ] Replace `PredictionTable` throughout handle.rs and vfs.rs
- [ ] Tests: identical predictions, memory savings, prune, serde roundtrip

### Cleanup (i4)
- [ ] Remove `PredictionTable` (replaced by trie)
- [ ] Remove `pair_index`
- [ ] Update all doc comments to reference tree names
- [ ] Verify all tests pass with trie backend

### Frame-level correlation (j)

Extend tree-level prediction with frame granularity. Instead of "fetch all of tree B", predict "fetch frame 7 of tree B's group 3". Reduces prefetch bandwidth 10-1000x for large tables.

#### j1. Frame-level access tracking
- [ ] Derive frame index: `frame_idx = index_in_group / sub_pages_per_frame`
- [ ] `LockSession`: add `frame_touches: Vec<(String, u64, u32)>`
- [ ] `read_exact_at`: record `(name, gid, frame_idx)` in `frame_touches`

#### j2. Frame correlation table
- [ ] `FrameCorrelation` struct with per-pair frame mappings
- [ ] Cross-correlate frame touches on session flush
- [ ] `max_correlations_per_pair` config (default 200)

#### j3. Precision prefetch firing
- [ ] On prediction fire: use frame correlations to narrow to specific frames
- [ ] S3 range GET for single frame (~256KB) instead of full group (~8MB)
- [ ] Fallback to full-group when no frame correlations exist

#### j4. Manifest persistence
- [ ] `frame_correlations` manifest field, serialize in sync()
- [ ] Prune on checkpoint, enforce max per pair

#### j5. Staleness handling
- [ ] Clear frame correlations on VACUUM (relearned within 2-3 sessions)
- [ ] Time decay handles gradual drift

#### j6. Frame-level benchmark
- [ ] Compare S3 bytes fetched: tree-level vs frame-level prediction
- [ ] Expected: point query 1M rows: ~400MB tree-level vs ~256KB frame-level

---

## Marne (Query Plan remaining): Benchmark
> After: Verdun (remaining) · Before: Jena

- [ ] `--plan-aware` flag in tiered-bench
- [ ] Before each measured query, call `run_eqp_and_parse(db, sql)` + `push_planned_accesses()` to simulate trace callback
- [ ] Compare plan-aware vs hop-schedule on all query types, cache levels none/interior/index

---

## Jena: Interior Page Introspection for Precise Leaf Prefetch
> After: Marne (Query Plan remaining) · Before: (future)

SEARCH queries need 1-3 leaf pages but the hop schedule can't predict which ones. Interior pages are cached (pinned on open). Walk cached interior pages to predict exact leaf group before SQLite asks.

### Design
- On cache miss: fire range GET immediately, in parallel walk cached interior pages to predict next leaf pages
- For joins hitting 3 indexes: 1 first-miss + 2 pre-warmed (1 RTT instead of 3)
- Parameter access: `sqlite3_expanded_sql()` in trace callback for integer/string literals

### Implementation
- [ ] `interior_introspect(cache, manifest, tree_name, search_key) -> Option<u64>`
- [ ] Key parsing from interior cells (varint record header + column values)
- [ ] Key comparison: BINARY (memcmp), NOCASE (case-fold)
- [ ] Composite key support (multi-column indexes with prefix matching)
- [ ] Integration in `read_exact_at`: introspection in parallel with range GET
- [ ] Bench path: pass params through `push_query_plan()`
- [ ] Extension path: `sqlite3_expanded_sql()` parsing in trace callback
- [ ] Tests: single-column integer, multi-column composite, NOCASE, boundary keys, rightmost child, empty/single-page index
- [ ] Benchmark: SEARCH latency with/without introspection on 1M posts (Express + Tigris)

---

## Future

### mmap cache
- [ ] `mmap` the cache file instead of `pread` for reads
- [ ] Keep `pwrite` for cache population
- [ ] `madvise(MADV_RANDOM)`
- [ ] Handle cache file growth: `mremap` on Linux, re-map on macOS
- [ ] Benchmark: warm lookup latency mmap vs pread (expect ~10-50us to ~1-5us)

### CLI subcommands
- [ ] `turbolite bench` -- move tiered-bench into CLI subcommand
- [ ] `turbolite gc --bucket X --prefix Y` -- one-shot GC
- [ ] `turbolite import --bucket X --prefix Y --db local.db`
- [ ] `turbolite info --bucket X --prefix Y` -- print manifest summary

### Bidirectional prefetch
- Track access direction, prefetch backward for DESC queries

### Application-level fetch API
- `vfs.fetch_all()` -- background hydration
- `vfs.fetch_range(start, end)` -- contiguous range fetch

### Hole tracking
- Manifest tracks freed pages per group
- Groups with >N% dead pages are compaction candidates

### Multi-writer coordination
- [ ] Distributed locks for concurrent writers (if needed)
