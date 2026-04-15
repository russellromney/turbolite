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

## Phase Apollo: Standardized Language SDKs

> After: Flume (page cache) . Before: Valkyrie

**Status:** Not started

**Goal:** Every language SDK returns the language's standard SQLite connection. turbolite is a storage engine, not a query API.

**Current:** Python and Rust return standard connections. Node and Go return crippled custom wrappers (no prepared statements, no param binding, no transactions).

**Target:** `turbolite.connect()` in every language returns the standard SQLite connection for that language, opened through turbolite's VFS.

### Node (better-sqlite3)
1. Build turbolite loadable extension against better-sqlite3's bundled SQLite headers
2. `turbolite.connect(path, opts)` loads extension, opens via VFS URI, returns `better-sqlite3.Database`
3. Ships platform-specific `.dylib/.so/.dll` alongside JS wrapper

### Go (mattn/go-sqlite3)
1. Build turbolite loadable extension against go-sqlite3's bundled SQLite headers
2. `turbolite.Open(path, opts)` returns `*sql.DB` (full `database/sql` interface)
3. Custom driver loads turbolite extension on init

### Shared concerns
- Loadable extension must NOT bundle its own SQLite (links against host's)
- CI matrix: macOS arm64, macOS x86_64, Linux x86_64, Linux arm64
- Tests: prepared statements, param binding, transactions, concurrent reads

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
> After: Austerlitz (CHANGELOG) · Before: Rosetta

Diagnostic tools, not blocking production use. Build when needed.

- [ ] `turbolite_query_cost('SELECT ...')` -- EQP + manifest tree sizes -> upper bound cache cost per tree
- [ ] `turbolite_analyze_query('SELECT ...', cache_level)` -- run query at specified cache temp, measure actual vs predicted

---

## Rosetta: Value-Partitioned Index Access
> After: Stalingrad · Before: (future)

Double-store index leaf pages in S3, organized by key value range instead of page number. SEARCH queries skip B-tree traversal entirely: the VFS maps the search key to the right partition and does one range GET. Storage cost is negligible (Tigris $0.02/GB). Normal B-tree groups remain for SCANs and general access.

**How it works:** At import, for each index with enough leaf pages, walk the B-tree in key order, group leaf pages into equal-depth partitions (~256 pages each), store as one seekable S3 object per index. Convert partition boundary keys to a normalized byte format (sort-order-preserving). At query time, the engine passes the normalized search key; the VFS binary-searches boundaries and range-GETs exactly one frame.

**Depends on:** SQLite record format parser for extracting key values from leaf cells.

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
- [ ] Fallback: if no value partition exists, or search_key is None, use normal B-tree access (hop schedule)
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

### `pip install turbolite` ships the CLI (maturin)
Replace setuptools with maturin as the Python build backend. `pip install turbolite` should install both the Python API (loadable extension) and the `turbolite` CLI binary.

- [ ] Switch `packages/python/pyproject.toml` from setuptools to maturin
- [ ] Configure maturin to build the `turbolite` binary (bin target) alongside the loadable extension (cdylib)
- [ ] Declare `turbolite` as a console script entry point in pyproject.toml
- [ ] Verify wheel contains both the `.so`/`.dylib` loadable extension and the CLI binary
- [ ] Update release CI: replace setuptools wheel build with `maturin build`
- [ ] Test: `pip install turbolite && turbolite --version && python -c "import turbolite"`
- [ ] Update README install instructions

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
