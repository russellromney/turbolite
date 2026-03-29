# turbolite Roadmap

## Midway (remaining): VACUUM Checkpoint Re-walk + Tests
> After: Thermopylae · Before: Somme

### Bug: VACUUM corrupts B-tree-aware page groups

**Problem:** VACUUM reorganizes all page numbers, but the VFS checkpoint path doesn't re-walk B-trees. The manifest's `group_pages` contains stale page assignments from before VACUUM. When a cold reader fetches groups using the post-VACUUM manifest, the encoded group has fewer pages than `group_pages` claims, producing "short data" warnings and "file is not a database" errors.

**Root cause:** `sync()` in handle.rs assigns new pages to groups using `manifest.page_location()`, which returns stale locations for pages that VACUUM renumbered. The encoded group only contains the pages that were actually dirty (the new page numbers), but `group_pages` still lists the old page numbers.

**Fix: Re-walk B-trees at checkpoint when VACUUM detected.**

VACUUM is detectable: it changes the database cookie (offset 24 in page 0) and typically reduces `page_count`. When detected, re-walk B-trees from page 0 and rebuild `group_pages`, `btrees`, and `page_index` before grouping dirty pages.

### a-d. DONE (see CHANGELOG)

VACUUM detection via schema cookie + dirty ratio. Re-walk B-trees from cache, rebuild group_pages,
upload all repacked groups, GC old keys. Cold reader verified with index scans.

### Remaining tests
- [ ] Prefetch: root page access triggers only relevant B-tree's group fetches
- [ ] VACUUM + additional writes: inserts after VACUUM land in correct groups
- [ ] Schema cookie tracking: re-walk only triggers on VACUUM, not on normal writes

---

## Somme: WAL Durability via walrust Integration
> After: Gallipoli · Before: Verdun (remaining)

Close the durability gap between checkpoints. walrust ships WAL frames to S3; turbolite page groups serve as the snapshot. No WAL parsing in turbolite.

**Architecture:** turbolite and walrust are independent layers that compose:
- **turbolite** owns pages: S3 page groups + local cache. The manifest version = checkpoint marker.
- **walrust** owns WAL: ships WAL frames to S3 as LTX segments. Replays on recovery.
- The page groups ARE the snapshot. walrust doesn't need its own snapshot mechanism.
- WAL segments with txid > manifest version are the ones to replay on cold start.

**Cold start flow:**
1. Fetch manifest from S3 (turbolite, already works)
2. Hydrate full DB from page groups into local file (turbolite, new: `materialize_to_file()`)
3. Download + apply WAL segments since manifest version (walrust `pull_and_apply_incrementals()`)
4. Open DB via turbolite VFS (SQLite sees a clean DB, WAL already applied)
5. Checkpoint (turbolite uploads dirty pages, walrust GCs old segments)

**Key integration: `SnapshotSource` trait.** walrust currently downloads a full LTX snapshot file for restore. With turbolite, it calls a trait method to materialize the base DB from page groups instead. This is the only cross-crate interface needed.

### a. `SnapshotSource` trait (in walrust-core)
- [ ] Define trait:
  ```rust
  #[async_trait]
  pub trait SnapshotSource: Send + Sync {
      /// Materialize the database at the given checkpoint version to a local file.
      /// Returns the checkpoint version that was materialized.
      async fn materialize(&self, output: &Path) -> Result<u64>;
      /// Return the current checkpoint version (manifest version).
      async fn checkpoint_version(&self) -> Result<u64>;
  }
  ```
- [ ] walrust `restore()` accepts optional `SnapshotSource` instead of downloading LTX snapshot
- [ ] If `SnapshotSource` provided: call `materialize()`, then apply incrementals after returned version
- [ ] If no `SnapshotSource`: use existing LTX snapshot download (backward compat)
- [ ] Tests: mock SnapshotSource, verify incrementals applied after materialized version

### b. `materialize_to_file()` in turbolite
- [ ] New function on `TieredSharedState` (or standalone):
  ```rust
  pub fn materialize_to_file(&self, output: &Path) -> io::Result<u64>
  ```
- [ ] Downloads ALL page groups from S3 (parallel, batched)
- [ ] Writes pages to output file in page-number order (standard SQLite file layout)
- [ ] Returns manifest version (= checkpoint version for walrust)
- [ ] Implement `SnapshotSource` for turbolite's shared state
- [ ] Tests: materialize, verify SQLite can open the file, row counts match

### c. walrust integration in turbolite VFS open
- [ ] `TieredConfig::wal_replication: bool` (default false)
- [ ] `TURBOLITE_WAL_REPLICATION=true` env var
- [ ] When enabled on VFS open:
  1. Fetch manifest (existing)
  2. Call `materialize_to_file()` to local temp file
  3. Call walrust `pull_and_apply_incrementals()` with manifest version
  4. Initialize VFS cache from the materialized + WAL-replayed file
  5. Clean up temp file
- [ ] When enabled during runtime:
  - walrust `sync_wal()` runs on a background interval (ships WAL frames to S3)
  - After turbolite checkpoint: signal walrust that manifest version advanced (GC old segments)
- [ ] Tests: write data, kill before checkpoint, cold start recovers WAL-shipped data

### d. WAL segment GC coordination
- [ ] After checkpoint uploads new manifest version N, WAL segments with txid <= N are garbage
- [ ] turbolite signals walrust (or walrust polls manifest version)
- [ ] walrust deletes obsolete LTX segments from S3
- [ ] Tests: segments accumulate between checkpoints, cleaned after checkpoint

### e. Shared S3 credentials
- [ ] walrust uses same bucket + endpoint as turbolite
- [ ] WAL segments stored under `{turbolite_prefix}/wal/` (same prefix, `/wal/` subdirectory)
- [ ] Or separate walrust prefix configurable via `TURBOLITE_WAL_PREFIX`
- [ ] `ObjectStore` adapter: implement walrust's `ObjectStore` trait using turbolite's `S3Client` (or use `S3Backend::from_env()` with same env vars)

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

## Stalingrad (remaining): Query Cost Estimation
> After: Marne · Before: Austerlitz

Diagnostic tools, not blocking production use. Build when needed.

- [ ] `turbolite_query_cost('SELECT ...')` -- EQP + manifest tree sizes -> upper bound cache cost per tree
- [ ] `turbolite_analyze_query('SELECT ...', cache_level)` -- run query at specified cache temp, measure actual vs predicted

---

## Austerlitz: Per-Query Adaptive Prefetch Schedules
> After: Stalingrad · Before: Jena

The VFS already selects search vs lookup schedule based on EQP output. Extend this to automatically tune schedules per query pattern over time, based on observed access patterns.

### a. Range-GET budget per tree
- [ ] `max_range_gets_per_tree: u8` config field (default 2)
- [ ] `tree_range_get_count: HashMap<String, u8>` on TieredHandle
- [ ] When count >= max for a tree: skip inline range GET, submit ALL of that tree's groups to prefetch pool, wait
- [ ] Point queries (1 GET per tree in a join) stay fast; scans (2+ GETs to same tree) switch to bulk prefetch after 2 range GETs
- [ ] `group_to_tree_name: HashMap<u64, String>` on Manifest (built on load from btrees)
- [ ] Graceful degradation for Positional strategy (no tree info = budget never triggers)
- [ ] `--max-range-gets` CLI flag in tiered-bench
- [ ] Tests: budget increment, independent per-tree counts, exhaustion triggers wait, max=0 always waits, max=255 unlimited, all tree groups submitted on exhaustion

### b. Extended-zero lookup schedules
- [ ] Test lookup schedules with 3-4 leading zeros: `[0,0,0,0.1,0.2]`, `[0,0,0,0,0.2]`
- [ ] Hypothesis: more leading zeros improve point queries further (sub-70ms on S3 Express)
- [ ] Matrix benchmark with extended-zero grid
- [ ] Update defaults if results justify it

### c. Per-query schedule learning
- [ ] Track (query_hash, tree_name, miss_count) over time
- [ ] After N executions of the same query pattern, adjust schedule based on observed miss distribution
- [ ] Queries that consistently miss 1-2 times get conservative schedule; queries with 10+ misses get aggressive
- [ ] Persist learned schedules in manifest (per query hash)
- [ ] `turbolite_config_set('prefetch_auto', 'true')` to enable

### d. Backend-adaptive defaults
- [ ] Measure GET latency on first S3 request (or during interior page load)
- [ ] If latency > 15ms (standard S3/Tigris): shift search schedule more aggressive, keep lookup conservative
- [ ] If latency < 8ms (S3 Express): use current defaults
- [ ] Log detected backend class on connection open

---

## Jena: Interior Page Introspection for Precise Prefetch
> After: Austerlitz · Before: Rosetta

The B-tree structure is fully known from interior pages (cached/pinned). By extracting child pointers at checkpoint and persisting them in the manifest, we can predict exact leaf pages for any query without guessing. Replaces the hop schedule heuristic with direct structural knowledge.

**Why it works:** Interior pages are always cached (pinned on open). They contain child pointers to leaf pages. Parsing all interior pages costs ~100us (15 pages, ~4500 cells at 1M rows). Maps live on `TieredHandle` (per-connection), built from local cache (the authority). Manifest persists a snapshot for cold start.

**Freshness:** Maps rebuild when interior pages are written (page splits/merges). `detect_interior_page` already identifies page types 0x02/0x05 on every write. When an interior page write is detected, increment a counter. Every N interior writes (default 1, configurable), rebuild maps from local cache. Data-only writes (the common case) have zero overhead. Checkpoint serializes current maps to manifest for future cold readers.

**What it solves:**
- SEARCH: predict exact leaf group, 1 range GET (18KB) instead of 12 GETs (9.4MB)
- Profile: parse index leaf to find exact data groups (5MB instead of 67MB)
- Joins: pipeline prefetch across tables, overlap S3 I/O
- Replaces hop schedule for BTreeAware (hop schedule becomes Positional-only fallback)

### a. Child pointer maps

Maps live on `TieredHandle`. Built from cached interior pages on connection open, rebuilt on interior page writes, persisted to manifest at checkpoint.

- [ ] `InteriorMap` struct: `child_to_parent: HashMap<u64, u64>`, `interior_children: HashMap<u64, Vec<u64>>`
- [ ] `rebuild_interior_map(cache) -> InteriorMap`: parse each cached interior page's cells (4-byte child pointer per cell + rightmost pointer from page header bytes 8..12)
- [ ] Connection open: call `rebuild_interior_map` from pinned interior pages
- [ ] Interior page write: detect in `write_all_at` via page type check, increment counter, rebuild every N interior writes
- [ ] Checkpoint: serialize current `InteriorMap` to manifest `interior_map` field
- [ ] Cold start: deserialize from manifest (skip rebuild if present)
- [ ] Tests: roundtrip through manifest serde, correct parent/child relationships, rebuild after simulated page split, empty for Positional, survives VACUUM + rebuild

### b. Sibling prefetch ("cheater prefetch")

On leaf miss, look up parent interior page, prefetch sibling leaf groups. Replaces hop schedule fraction math with exact structural knowledge.

- [ ] On cache miss for leaf page P: look up `child_to_parent[P]` to find parent interior page
- [ ] Get `interior_children[parent]` to find all siblings, find P's index
- [ ] For SCAN (from EQP): prefetch ALL sibling groups
- [ ] For SEARCH: prefetch 0-1 siblings (conservative, most of the time the one leaf is enough)
- [ ] For unknown (no EQP info): prefetch next 2 siblings (minimal speculation)
- [ ] Wire into `read_exact_at` replacing `trigger_prefetch` for BTreeAware
- [ ] `trigger_prefetch` becomes Positional-only fallback
- [ ] Remove `consecutive_misses` tracking for BTreeAware (no longer needed)
- [ ] Tests: sibling prediction matches B-tree structure, SCAN prefetches all, SEARCH prefetches 0-1, multi-level B-tree (interior children that are also interior pages), Positional fallback still works
- [ ] Benchmark: compare v6 hop schedule vs sibling prefetch on 1M posts (expect post+user drops from 12 GETs to 1-2)

### c. Exact leaf prediction for SEARCH

Parse key boundaries from interior cells at checkpoint. Given a search key, binary search interior pages to find exact leaf group before SQLite asks.

- [ ] Extend checkpoint extraction: parse key data from interior cells (varint record header + column values), not just child pointers
- [ ] `predict_leaf(interior_map, tree_name, search_key) -> Option<(u64, u32)>` returns (group_id, frame_index)
- [ ] Key comparison: BINARY collation (memcmp), NOCASE (case-fold before compare)
- [ ] Composite key support: multi-column indexes with prefix matching
- [ ] Bench path: pass params through `push_query_plan()`, call `predict_leaf` before first read
- [ ] Extension path: `sqlite3_expanded_sql(stmt)` in trace callback, parse integer/string literals
- [ ] Submit predicted group to prefetch pool (or issue sub-chunk range GET directly)
- [ ] Tests: single-column integer, composite key, NOCASE, key at cell boundary, rightmost child pointer, empty index, single-page index (no interior pages)
- [ ] Benchmark: SEARCH latency with/without prediction on 1M posts (Express + Tigris)

### d. Cross-tree leaf chasing

When a leaf page arrives from S3, parse its cells to extract rowids/foreign keys. Map to groups in the next table in the join plan. Prefetch while SQLite processes current results.

- [ ] SQLite record format parser: varint header length, column type codes, integer/string/blob extraction
- [ ] For table B-tree leaves (0x0D): extract rowid from cell header
- [ ] For index B-tree leaves (0x0A): extract indexed column values from record payload
- [ ] On prefetch completion callback: parse arrived leaf, extract keys, map to target groups via `page_location()`, submit to prefetch pool
- [ ] Join pipeline: EQP gives join order, each leaf arrival triggers prefetch for next table
- [ ] Start with integer rowids only (covers profile query: idx_posts_user leaf -> post rowids -> posts data groups)
- [ ] Tests: parse leaf cells for integer PK, composite index, overflow pages (payload > page), string keys
- [ ] Benchmark: profile query with leaf chasing vs without (expect 53 GETs / 67MB -> ~6 GETs / 5MB)

### e. Overflow chain prefetch

When a leaf page arrives and contains overflow pointers (payload > maxLocal), prefetch the overflow chain proactively instead of blocking on each link. Subframe range GETs already handle small overflow within a group, but multi-MB TEXT/BLOB values with chains spanning multiple page groups cause sequential blocking faults.

Inspired by sqlite-prefetch's overflow cascading (https://github.com/wjordan/sqlite-prefetch).

- [ ] On leaf page (0x0D) fetch completion: parse cells, detect overflow (payload > maxLocal), extract first overflow page number
- [ ] Map overflow page to group via `page_location()`, submit group to prefetch pool
- [ ] On overflow page arrival: read next-page pointer (first 4 bytes), cascade to next group
- [ ] Repeat until next-page pointer is 0 (end of chain)
- [ ] Cap cascade depth (e.g., 64) to bound runaway chains
- [ ] Tests: single overflow page, multi-group chain, chain within same group (no-op), cap enforced, no overflow (common case, zero overhead)
- [ ] Benchmark: table with 1MB+ TEXT values, with/without overflow prefetch

### f. Multi-level interior group lookahead

For very large databases where interior pages span multiple page groups, prefetch the next interior sibling group before SQLite descends into it. Avoids a blocking fault when the current interior group's children are exhausted and SQLite needs the next interior page.

Inspired by sqlite-prefetch's multi-level lookahead (https://github.com/wjordan/sqlite-prefetch).

- [ ] Track remaining sibling groups under current parent interior page during scan
- [ ] When remaining sibling count drops below threshold (e.g., 5 groups), prefetch the next interior sibling's group from the parent level
- [ ] Only relevant when interior pages span multiple groups (very large databases, 10M+ rows)
- [ ] No-op for databases where all interior pages fit in group 0 (the common case today)
- [ ] Tests: synthetic multi-group interior layout, lookahead triggers at threshold, no-op for small databases

---

## Rosetta: Value-Partitioned Index Access
> After: Jena · Before: (future)

Double-store index leaf pages in S3, organized by key value range instead of page number. SEARCH queries skip B-tree traversal entirely: the VFS maps the search key to the right partition and does one range GET. Storage cost is negligible (Tigris $0.02/GB). Normal B-tree groups remain for SCANs and general access.

**How it works:** At import, for each index with enough leaf pages, walk the B-tree in key order, group leaf pages into equal-depth partitions (~256 pages each), store as one seekable S3 object per index. Convert partition boundary keys to a normalized byte format (sort-order-preserving). At query time, the engine passes the normalized search key; the VFS binary-searches boundaries and range-GETs exactly one frame.

**Depends on:** Jena's SQLite record format parser (Jena d) for extracting key values from leaf cells. Jena's interior introspection is complementary, not replaced.

### a. Normalized key bytes

Sort-order-preserving byte encoding so partition boundaries are memcmp-comparable. Handles all SQLite column types.

- [ ] `normalize_key(cell_payload, col_types) -> Vec<u8>`: parse SQLite record format, emit normalized bytes
- [ ] Encoding: NULL `0x00`, INTEGER `0x01` + 8-byte BE with sign bit flipped, REAL `0x02` + 8-byte IEEE 754 with sign manipulation, TEXT `0x03` + raw bytes + `0x00`, BLOB `0x04` + raw bytes
- [ ] Composite indexes: concatenate normalized bytes per column (memcmp on result gives correct multi-column sort)
- [ ] `compare_normalized(a, b) -> Ordering`: simple memcmp wrapper
- [ ] Tests: single-column INTEGER (positive, negative, zero, i64 extremes), TEXT (ASCII, UTF-8, empty), REAL, NULL ordering, composite key (INT, TEXT), round-trip encode/compare matches SQLite's own ordering

### b. Partition builder (import path)

At import time, build value-partitioned copies for qualifying indexes.

- [ ] Threshold: only partition indexes with > 1 page group worth of leaf pages (small indexes don't benefit)
- [ ] Walk each qualifying index B-tree in key order (leaf pages left-to-right via sibling pointers or interior page traversal)
- [ ] Group leaf pages into equal-depth partitions of N pages each (N = pages_per_group)
- [ ] For each partition: extract + normalize the first key from the first leaf page (the min boundary)
- [ ] Encode all partitions as one seekable S3 object per index: `{prefix}/vp/{index_root_page}_v{version}`
- [ ] Seekable frame table: one frame per partition (reuse existing seekable encode infrastructure)
- [ ] Manifest fields: `value_partitions: HashMap<u64, ValuePartition>` where key is index root page
- [ ] `ValuePartition { s3_key: String, boundaries: Vec<Vec<u8>>, frame_table: Vec<FrameEntry>, leaf_page_nums: Vec<Vec<u64>> }`
- [ ] `boundaries[i]` = normalized min key of partition i; binary search finds the right frame
- [ ] Wire into `import_sqlite_file()` after normal page group upload
- [ ] Tests: 1000-page index partitioned into 4 partitions, boundary keys correct, seekable object decodable, small index skipped, composite index boundaries correct

### c. Partition lookup (read path)

VFS uses value partitions for SEARCH queries when available.

- [ ] Extend `PlannedAccess` with optional `search_key: Option<Vec<u8>>` (normalized key bytes from engine)
- [ ] `push_planned_accesses`: if access is SEARCH and value partition exists for the index, binary-search `boundaries` to find target frame index
- [ ] Issue range GET for that single frame of the partition's seekable S3 object
- [ ] Decode frame, write pages to cache (scattered writes, same as normal prefetch)
- [ ] Pages land in cache by their real page numbers; SQLite reads them normally
- [ ] Fallback: if no value partition exists, or search_key is None, use normal B-tree access (Jena or hop schedule)
- [ ] Tests: SEARCH hits correct partition, value at partition boundary, value before first partition, value after last partition, fallback when no partition exists, fallback when key is None

### d. Engine integration

Engine normalizes bound values and passes them to the VFS.

- [ ] Engine extracts bound parameter values from prepared statement (sqlite3_bind_* values)
- [ ] `normalize_query_key(value, col_affinity) -> Vec<u8>`: normalize a Rust value to the same format as partition boundaries
- [ ] For composite indexes: normalize each column, concatenate
- [ ] Pass normalized key via `PlannedAccess.search_key` alongside EQP info
- [ ] Tests: integer lookup, string lookup, composite key lookup, NULL handling, type mismatch (string in integer column)

### e. Staleness and rebuild

Value partitions are read-only, built at import. Handle staleness gracefully.

- [ ] After writes modify an index's pages, mark that index's value partition as stale (don't use it for lookups)
- [ ] Staleness detection: compare manifest version at partition build time vs current manifest version, or track dirty index root pages
- [ ] `rebuild_value_partitions(config) -> io::Result<()>`: CLI/API to rebuild partitions from current S3 data (download page groups, re-sort, re-upload partitioned copies)
- [ ] Wire into compaction (Phase Midway g): when groups are repacked, rebuild value partitions too
- [ ] Tests: stale partition falls back to B-tree, rebuild produces correct partitions, rebuild after INSERT/DELETE

### f. Benchmark

- [ ] Add `--value-partitions` flag to tiered-bench
- [ ] Compare at `interior` and `none` cache levels with/without value partitions
- [ ] Key metrics: S3 GETs per query, bytes fetched, p50/p99 latency
- [ ] Expected wins: Q4 (mutual) and Q5 (idx-filter) see 2-4 fewer GETs; Q6 (scan) unchanged
- [ ] Report partition build time and S3 storage overhead

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

### turbolite_recommend()
- [ ] `turbolite_recommend()` SQL function: analyzes connection's access history, returns JSON with:
  - Recommended cache_limit based on peak working set
  - Recommended prefetch schedules based on observed index-lookup vs table-scan ratio
  - Hottest/coldest trees by access frequency
  - Specific suggestions ("evict_tree('audit_log') would free 180MB")
- [ ] Track peak working set, per-tree access counts, scan vs search ratio over connection lifetime

### Multi-writer coordination
- [ ] Distributed locks for concurrent writers (if needed)
