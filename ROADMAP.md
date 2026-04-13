# turbolite Roadmap

## Soyuz: Migrate from sqlite-vfs to sqlite-plugin
> After: Mercury (CHANGELOG) · Before: Valkyrie

Replace `sqlite-vfs` (rkusa, unmaintained since 2022, self-described prototype) with
`sqlite-plugin` (orbitinghail/Carl Sverre, active, used by Graft).

**Why:** sqlite-vfs has a copy-based WAL-index model where each connection gets a local
32KB buffer synced via push/pull. This caused a 40% concurrent corruption rate (Phase
Pelican stress test) because `shm_barrier` failed to push when the writer held an
exclusive WAL-index lock. We patched it (russellromney/sqlite-vfs#fix/shm-barrier-wal-push)
but the crate has deeper limitations: no mmap page reads (xFetch/xUnfetch), no extension
loading (xDl*), no custom device characteristics, hardcoded 1024 sector size.

sqlite-plugin returns `NonNull<u8>` from `shm_map` (direct pointer, no copy layer).
SQLite reads/writes the mmap directly. No push/pull, no barrier bugs by design. Also
supports xFetch/xUnfetch for memory-mapped page reads.

### a. Trait migration
- [ ] Replace `sqlite_vfs::Vfs` with `sqlite_plugin::Vfs` (6 methods on TurboliteVfs, 8 on SharedTurboliteVfs)
- [ ] Replace `sqlite_vfs::DatabaseHandle` with sqlite-plugin equivalent (11 methods on TurboliteHandle)
- [ ] Move FileWalIndex (mmap + in-process locks) into shm_map/shm_lock/shm_barrier/shm_unmap on Vfs
- [ ] Map type differences: OpenKind, OpenOptions, LockKind, WalIndexLock
- [ ] Update registration calls (2 sites in mod.rs)

### b. Direct pointer WAL-index
- [ ] shm_map returns NonNull<u8> pointing to mmap'd region (delete copy layer)
- [ ] shm_barrier becomes a memory fence (no push/pull)
- [ ] Delete FileWalIndex struct, push/pull/map methods (~260 lines)
- [ ] Preserve in-process lock coordination (lib.rs:55-141, required for thread-level mutual exclusion)

### c. Memory-mapped page reads (xFetch/xUnfetch)
- [ ] Implement xFetch: return mmap pointer to cache file page (zero-copy read)
- [ ] Implement xUnfetch: release mmap reference
- [ ] Expected: warm point-lookup latency from ~5us (pread) to <1us (pointer dereference)

### d. Tests
- [ ] Port 25 WAL-index/lock tests to new API
- [ ] Concurrent stress test: 1 writer + 4 readers, 30x runs, 0 failures
- [ ] Benchmark: compare pread vs mmap page reads on warm cache

---

## Valkyrie: io_uring VFS (Linux, faster than SQLite)
> After: Soyuz · Before: Stalingrad

Replace syscall-per-page I/O with batched io_uring submissions. On Linux, this makes turbolite's VFS **faster than SQLite's default VFS** because:


1. SQLite's default VFS does one `pread` syscall per page read (even with mmap WAL-index, page I/O is syscall-based)
2. With io_uring, turbolite can batch multiple page reads into a single submission queue drain, amortizing the kernel transition cost across pages
3. SQLite reads 1 page at a time; turbolite knows (from the manifest + query plan) which pages are coming next, so it can submit multiple reads before SQLite asks for them

**Why this is possible now:** Phase Pelican's mmap WAL-index + lock-free cache file removed all per-page locks. The remaining overhead is pure syscall cost. io_uring eliminates that.

### a. io_uring cache file reads
- Replace `pread` in `DiskCache::read_page()` with io_uring `Read` op
- Batch: when prefetch submits N page reads, collect them into a single SQE batch
- `read_exact_at` fast path: check if page is already completed in io_uring CQE ring, return immediately if so
- Fallback: if io_uring unavailable (old kernel, macOS), use current pread path
- Expected: point lookup from 5us to <1us (no kernel transition for cached pages)

### b. io_uring prefetch pipeline
- Prefetch pool submits page reads to io_uring instead of spawning threads
- io_uring handles the kernel-level parallelism (no thread pool needed for I/O)
- SQE chain: read page -> write to cache file -> mark bitmap (all in kernel)
- Expected: cold query latency reduction from prefetch overlap

### c. io_uring checkpoint
- Batch cache file reads during page group encoding (currently one pread per page per group)
- Async fsync for staging log (currently blocks ~2ms on macOS, ~0.5ms on Linux)
- Parallel pg/ directory writes for background flush
- Expected: local checkpoint from 8ms to <2ms

### d. io_uring S3 upload pipeline
- Encode group -> submit to io_uring for local write -> simultaneously start S3 PUT
- io_uring handles the file I/O (cache reads for encoding) while tokio handles S3 network
- True CPU/IO/network overlap without thread pool overhead

**Crate:** `io-uring` (1.x) for raw ring access, or `tokio-uring` for async integration.
**Gating:** Linux-only, kernel 5.6+. Feature flag `io-uring`. macOS/other: current pread path.
**Risk:** io_uring API complexity, fixed-buffer registration lifetime management.

---

## Stalingrad (remaining): Query Cost Estimation
> After: Austerlitz (CHANGELOG) · Before: Jena

Diagnostic tools, not blocking production use. Build when needed.

- [ ] `turbolite_query_cost('SELECT ...')` -- EQP + manifest tree sizes -> upper bound cache cost per tree
- [ ] `turbolite_analyze_query('SELECT ...', cache_level)` -- run query at specified cache temp, measure actual vs predicted

---

## Jena: Interior Page Introspection for Precise Prefetch
> After: Stalingrad · Before: Rosetta

The B-tree structure is fully known from interior pages (cached/pinned). By extracting child pointers at checkpoint and persisting them in the manifest, we can predict exact leaf pages for any query without guessing. Replaces the hop schedule heuristic with direct structural knowledge.

**Why it works:** Interior pages are always cached (pinned on open). They contain child pointers to leaf pages. Parsing all interior pages costs ~100us (15 pages, ~4500 cells at 1M rows). Maps live on `TurboliteHandle` (per-connection), built from local cache (the authority). Manifest persists a snapshot for cold start.

**Freshness:** Maps rebuild when interior pages are written (page splits/merges). `detect_interior_page` already identifies page types 0x02/0x05 on every write. When an interior page write is detected, increment a counter. Every N interior writes (default 1, configurable), rebuild maps from local cache. Data-only writes (the common case) have zero overhead. Checkpoint serializes current maps to manifest for future cold readers.

**What it solves:**
- SEARCH: predict exact leaf group, 1 range GET (18KB) instead of 12 GETs (9.4MB)
- Profile: parse index leaf to find exact data groups (5MB instead of 67MB)
- Joins: pipeline prefetch across tables, overlap S3 I/O
- Replaces hop schedule for BTreeAware (hop schedule becomes Positional-only fallback)

### a. Child pointer maps

Maps live on `TurboliteHandle`. Built from cached interior pages on connection open, rebuilt on interior page writes, persisted to manifest at checkpoint.

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
