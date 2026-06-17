use super::*;
use crate::tiered::btree_peek::parse_index_leaf_rowids;
use crate::DatabaseHandle;
use anyhow::Result;
use arc_swap::ArcSwap;
use async_trait::async_trait;
use hadb_storage::StorageBackend;
use hadb_storage_mem::MemStorage;
use std::collections::{HashMap, HashSet};
#[cfg(feature = "encryption")]
use std::fs::OpenOptions as FsOpenOptions;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;

#[cfg(feature = "encryption")]
fn test_key() -> [u8; 32] {
    let mut key = [0u8; 32];
    for (i, b) in key.iter_mut().enumerate() {
        *b = i as u8;
    }
    key
}

#[cfg(feature = "encryption")]
fn wrong_key() -> [u8; 32] {
    [0xFFu8; 32]
}

#[test]
fn lookahead_anchor_window_aims_contiguous_run_around_served_leaf() {
    assert_eq!(anchored_lookahead_window(100, &[40], 32), Some(25..57));
    assert_eq!(anchored_lookahead_window(100, &[2], 32), Some(0..32));
    assert_eq!(anchored_lookahead_window(100, &[96], 32), Some(68..100));
    assert_eq!(
        anchored_lookahead_window(100, &[40, 41, 42], 32),
        Some(26..58)
    );
    assert_eq!(
        anchored_lookahead_window(100, &[10, 60], 32),
        Some(0..32),
        "an anchor span wider than the cap must aim from the first anchored row"
    );
    assert_eq!(anchored_lookahead_window(10, &[4], 32), Some(0..10));
    assert_eq!(anchored_lookahead_window(10, &[], 32), None);
}

#[test]
fn lookahead_key_hint_narrows_to_contiguous_key_run() {
    let mut keys = vec![Some(1); 20];
    keys.extend(vec![Some(7); 12]);
    keys.extend(vec![Some(9); 20]);

    assert_eq!(
        key_or_anchor_lookahead_window(keys.len(), &keys, Some(7), &[2, 3, 4, 21, 22], 32),
        Some(20..32),
        "exact key hints must beat noisy table-leaf anchor matches from unrelated keys"
    );
    assert_eq!(
        key_or_anchor_lookahead_window(keys.len(), &keys, None, &[21], 32),
        anchored_lookahead_window(keys.len(), &[21], 32)
    );
}

fn handle_with_manifest(
    dir: &TempDir,
    page_group_keys: Vec<String>,
) -> (TurboliteHandle, Arc<DiskCache>) {
    let page_size = 64;
    let pages_per_group = 4;
    let page_count = (page_group_keys.len().max(1) as u64) * pages_per_group as u64;
    let cache = Arc::new(
        DiskCache::new(
            dir.path(),
            3600,
            pages_per_group,
            2,
            page_size,
            page_count,
            None,
            Vec::new(),
        )
        .unwrap(),
    );
    let manifest = Manifest {
        page_count,
        page_size,
        pages_per_group,
        page_group_keys,
        ..Manifest::empty()
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    let handle = TurboliteHandle::new_tiered(
        Some(Arc::new(MemStorage::new())),
        Some(rt.handle().clone()),
        Arc::clone(&cache),
        Arc::new(ArcSwap::from_pointee(manifest)),
        Arc::new(Mutex::new(HashSet::new())),
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicU64::new(0)),
        dir.path().join("db.lock"),
        pages_per_group,
        0,
        true,
        vec![0.3, 0.3, 0.4],
        vec![0.0, 0.0, 0.0],
        None,
        false,
        #[cfg(feature = "zstd")]
        None,
        None,
        false,
        false,
        4,
        32 * 1024 * 1024,
        None,
        false,
        0,
        0,
        false,
        false,
        Arc::new(parking_lot::RwLock::new(())),
        Arc::new(AtomicU64::new(0)),
    )
    .unwrap();
    (handle, cache)
}

struct RangeIgnoringReadBackend {
    blob: Vec<u8>,
    full_gets: AtomicU64,
}

#[async_trait]
impl StorageBackend for RangeIgnoringReadBackend {
    async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>> {
        self.full_gets.fetch_add(1, Ordering::Relaxed);
        Ok(Some(self.blob.clone()))
    }

    async fn put(&self, _key: &str, _data: &[u8]) -> Result<()> {
        Ok(())
    }

    async fn delete(&self, _key: &str) -> Result<()> {
        Ok(())
    }

    async fn list(&self, _prefix: &str, _after: Option<&str>) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    async fn put_if_absent(&self, _key: &str, _data: &[u8]) -> Result<hadb_storage::CasResult> {
        Ok(hadb_storage::CasResult {
            success: true,
            etag: None,
        })
    }

    async fn put_if_match(
        &self,
        _key: &str,
        _data: &[u8],
        _etag: &str,
    ) -> Result<hadb_storage::CasResult> {
        Ok(hadb_storage::CasResult {
            success: true,
            etag: None,
        })
    }

    async fn range_get(&self, _key: &str, _start: u64, _len: u32) -> Result<Option<Vec<u8>>> {
        Ok(Some(self.blob.clone()))
    }
}

struct BlockingSeekableBackend {
    blob: Vec<u8>,
    release: AtomicBool,
    full_gets: AtomicU64,
    range_gets: AtomicU64,
}

#[async_trait]
impl StorageBackend for BlockingSeekableBackend {
    async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>> {
        self.full_gets.fetch_add(1, Ordering::AcqRel);
        while !self.release.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(Some(self.blob.clone()))
    }

    async fn put(&self, _key: &str, _data: &[u8]) -> Result<()> {
        Ok(())
    }

    async fn delete(&self, _key: &str) -> Result<()> {
        Ok(())
    }

    async fn list(&self, _prefix: &str, _after: Option<&str>) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    async fn put_if_absent(&self, _key: &str, _data: &[u8]) -> Result<hadb_storage::CasResult> {
        Ok(hadb_storage::CasResult {
            success: true,
            etag: None,
        })
    }

    async fn put_if_match(
        &self,
        _key: &str,
        _data: &[u8],
        _etag: &str,
    ) -> Result<hadb_storage::CasResult> {
        Ok(hadb_storage::CasResult {
            success: true,
            etag: None,
        })
    }

    async fn range_get(&self, _key: &str, _start: u64, _len: u32) -> Result<Option<Vec<u8>>> {
        self.range_gets.fetch_add(1, Ordering::AcqRel);
        Ok(None)
    }
}

struct BlockingRangeSeekableBackend {
    blob: Vec<u8>,
    release: AtomicBool,
    range_gets: AtomicU64,
}

#[async_trait]
impl StorageBackend for BlockingRangeSeekableBackend {
    async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    async fn put(&self, _key: &str, _data: &[u8]) -> Result<()> {
        Ok(())
    }

    async fn delete(&self, _key: &str) -> Result<()> {
        Ok(())
    }

    async fn list(&self, _prefix: &str, _after: Option<&str>) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    async fn put_if_absent(&self, _key: &str, _data: &[u8]) -> Result<hadb_storage::CasResult> {
        Ok(hadb_storage::CasResult {
            success: true,
            etag: None,
        })
    }

    async fn put_if_match(
        &self,
        _key: &str,
        _data: &[u8],
        _etag: &str,
    ) -> Result<hadb_storage::CasResult> {
        Ok(hadb_storage::CasResult {
            success: true,
            etag: None,
        })
    }

    async fn range_get(&self, _key: &str, start: u64, len: u32) -> Result<Option<Vec<u8>>> {
        self.range_gets.fetch_add(1, Ordering::AcqRel);
        while !self.release.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(1));
        }
        let start = start as usize;
        let end = start + len as usize;
        Ok(Some(self.blob[start..end].to_vec()))
    }
}

fn sqlite_page(db: &[u8], page_size: usize, page_num: u64) -> &[u8] {
    let start = page_num as usize * page_size;
    &db[start..start + page_size]
}

fn sqlite_page_type(page: &[u8], page_num: u64) -> Option<u8> {
    let hdr_off = if page_num == 0 { 100 } else { 0 };
    page.get(hdr_off).copied()
}

fn read_sqlite_varint(buf: &[u8], off: usize) -> Option<(u64, usize)> {
    let tail = buf.get(off..)?;
    let mut val = 0u64;
    for i in 0..8 {
        let b = *tail.get(i)?;
        val = (val << 7) | u64::from(b & 0x7f);
        if b & 0x80 == 0 {
            return Some((val, i + 1));
        }
    }
    let b = *tail.get(8)?;
    val = (val << 8) | u64::from(b);
    Some((val, 9))
}

fn table_leaf_rowids_for_test(page: &[u8], page_num: u64) -> Vec<i64> {
    let hdr = if page_num == 0 { 100 } else { 0 };
    if page.get(hdr).copied() != Some(0x0d) || page.len() < hdr + 8 {
        return Vec::new();
    }
    let cell_count = u16::from_be_bytes([page[hdr + 3], page[hdr + 4]]) as usize;
    let mut rowids = Vec::new();
    for i in 0..cell_count {
        let ptr = hdr + 8 + i * 2;
        if ptr + 2 > page.len() {
            break;
        }
        let cell = u16::from_be_bytes([page[ptr], page[ptr + 1]]) as usize;
        let Some((_, payload_varint_len)) = read_sqlite_varint(page, cell) else {
            continue;
        };
        let Some((rowid, _)) = read_sqlite_varint(page, cell + payload_varint_len) else {
            continue;
        };
        if let Ok(rowid) = i64::try_from(rowid) {
            rowids.push(rowid);
        }
    }
    rowids
}

fn page_size_from_sqlite_header(db: &[u8]) -> usize {
    let raw = u16::from_be_bytes([db[16], db[17]]);
    if raw == 1 {
        65_536
    } else {
        raw as usize
    }
}

#[test]
fn scan_prefetch_refill_admits_only_fixed_window() {
    let dir = TempDir::new().unwrap();
    let keys: Vec<String> = (0..6).map(|gid| format!("g{gid}")).collect();
    let (mut handle, cache) = handle_with_manifest(&dir, keys.clone());
    let storage: Arc<dyn StorageBackend> = Arc::new(MemStorage::new());
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pool = Arc::new(PrefetchPool::new(
        1,
        storage,
        rt.handle().clone(),
        Arc::clone(&cache),
        4,
        Arc::new(AtomicU64::new(24)),
        #[cfg(feature = "zstd")]
        None,
        None,
        Arc::clone(&handle.manifest),
        Arc::new(parking_lot::RwLock::new(())),
        Arc::new(AtomicU64::new(0)),
        8,
        RemoteIoBudget::new(1, 0),
    ));
    handle.prefetch_pool = Some(Arc::clone(&pool));
    handle.scan_window_groups = 2;
    handle.scan_window_bytes = 1024 * 1024;
    handle.active_scan_prefetch.insert(
        "users".to_string(),
        ActiveScanPrefetch {
            group_ids: (0..6).collect(),
            cursor: 0,
            submitted: HashSet::new(),
        },
    );

    let mut manifest = (**handle.manifest.load()).clone();
    manifest.tree_name_to_groups = HashMap::from([("users".to_string(), (0..6).collect())]);
    handle.refill_active_scan_prefetch(&manifest, cache.as_ref(), None);

    let stats = pool.stats_json();
    assert_eq!(stats["accepted"], 2);
    assert!(
        stats["accepted"].as_u64().unwrap() < 6,
        "SCAN prefetch must not drain all planned groups at once"
    );
    pool.wait_idle();
}

#[test]
fn planned_scan_read_path_does_not_submit_current_group_to_optional_window() {
    let _plan_guard = query_plan::plan_queue_test_guard();
    let dir = TempDir::new().unwrap();
    let keys: Vec<String> = (0..5).map(|gid| format!("g{gid}")).collect();
    let (mut handle, cache) = handle_with_manifest(&dir, keys);
    let storage: Arc<dyn StorageBackend> = Arc::new(MemStorage::new());
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pool = Arc::new(PrefetchPool::new(
        1,
        storage,
        rt.handle().clone(),
        Arc::clone(&cache),
        4,
        Arc::new(AtomicU64::new(20)),
        #[cfg(feature = "zstd")]
        None,
        None,
        Arc::clone(&handle.manifest),
        Arc::new(parking_lot::RwLock::new(())),
        Arc::new(AtomicU64::new(0)),
        8,
        RemoteIoBudget::new(1, 1),
    ));
    handle.prefetch_pool = Some(Arc::clone(&pool));
    handle.query_plan_prefetch = true;
    handle.scan_window_groups = 2;

    let mut manifest = (**handle.manifest.load()).clone();
    manifest.tree_name_to_groups = HashMap::from([("users".to_string(), (0..5).collect())]);
    handle.manifest.store(Arc::new(manifest));
    query_plan::push_planned_accesses(vec![query_plan::PlannedAccess {
        tree_name: "users".to_string(),
        access_type: query_plan::AccessType::Scan,
        table_name: Some("users".to_string()),
        constraint_columns: Vec::new(),
    }]);

    // Pre-seed group 0 in the local cache so the read path can exercise the
    // SCAN prefetch logic without relying on fabricated zeros for missing
    // remote objects (A2).
    let page_size = 64u32;
    let pages_per_group = 4u32;
    let page_nums: Vec<u64> = (0..pages_per_group as u64).collect();
    let data: Vec<u8> = (0..pages_per_group)
        .flat_map(|i| vec![i as u8; page_size as usize])
        .collect();
    cache.write_pages_scattered(&page_nums, &data, 0, 0).unwrap();
    cache.mark_group_present(0);
    // Force the slow path so the plan queue is drained and SCAN prefetch
    // state is populated; otherwise the fast path returns before touching it.
    cache.bump_generation();

    let mut buf = vec![0u8; 64];
    handle.read_exact_at(&mut buf, 0).unwrap();

    let stats = pool.stats_json();
    assert!(stats["max_queued"].as_u64().unwrap() <= 2);
    // A pure SCAN plan leaves planned_lookahead_searches empty. The read-path
    // fast path is gated on `lookahead_enabled && !planned_lookahead_searches
    // .is_empty()`, so an empty map here is exactly what keeps scans on the
    // fast path instead of paying the lookahead slow path.
    assert!(handle.planned_lookahead_searches.is_empty());
    assert_eq!(stats["lookahead"]["fired"], 0);
    assert!(handle.active_scan_prefetch["users"].submitted.len() <= 2);
}

#[test]
fn retained_lookahead_missing_table_root_bails_without_firing() {
    let dir = TempDir::new().unwrap();
    let (mut handle, cache) = handle_with_manifest(&dir, vec!["g0".to_string()]);
    let storage: Arc<dyn StorageBackend> = Arc::new(MemStorage::new());
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pool = Arc::new(PrefetchPool::new(
        0,
        storage,
        rt.handle().clone(),
        Arc::clone(&cache),
        4,
        Arc::new(AtomicU64::new(4)),
        #[cfg(feature = "zstd")]
        None,
        None,
        Arc::clone(&handle.manifest),
        Arc::new(parking_lot::RwLock::new(())),
        Arc::new(AtomicU64::new(0)),
        8,
        RemoteIoBudget::new(1, 0),
    ));
    handle.prefetch_pool = Some(Arc::clone(&pool));
    handle.retained_lookahead.insert(
        "posts".to_string(),
        RetainedLookahead {
            index_tree_name: "idx_posts_user_created".to_string(),
            table_name: "posts".to_string(),
            rowids: vec![1, 2, 3],
            first_int_keys: vec![Some(7), Some(7), Some(7)],
            key_hint: Some(7),
        },
    );

    let mut manifest = (**handle.manifest.load()).clone();
    manifest.sub_pages_per_frame = 2;
    manifest.frame_tables = vec![vec![FrameEntry { offset: 0, len: 1 }]];
    handle.manifest.store(Arc::new(manifest.clone()));
    assert!(!manifest.tree_name_to_root_page.contains_key("posts"));
    handle.fire_retained_lookahead("posts", 0, None, cache.as_ref(), &manifest);

    let stats = pool.stats_json();
    assert_eq!(stats["lookahead"]["fired"], 0);
    assert_eq!(stats["lookahead"]["frames_submitted"], 0);
    assert_eq!(stats["lookahead"]["bailouts"]["no_anchor"], 1);
    assert!(!handle.retained_lookahead.contains_key("posts"));
}

#[test]
fn retained_lookahead_from_real_multi_key_index_submits_anchored_contiguous_frames() {
    let _plan_guard = query_plan::plan_queue_test_guard();
    let sqlite_dir = TempDir::new().unwrap();
    let db_path = sqlite_dir.path().join("real_index.db");
    let writer = rusqlite::Connection::open(&db_path).expect("open sqlite fixture");
    writer
        .execute_batch(
            "PRAGMA page_size=4096;
             CREATE TABLE items (
               id INTEGER PRIMARY KEY,
               category TEXT NOT NULL,
               bucket INTEGER NOT NULL,
               payload TEXT NOT NULL
             );
             CREATE INDEX idx_items_category_bucket
               ON items(category, bucket);",
        )
        .expect("schema");
    {
        let tx = writer.unchecked_transaction().expect("fixture tx");
        let payload = "x".repeat(180);
        for n in 0..420i64 {
            let id = 10_000 + n;
            tx.execute(
                "INSERT INTO items (id, category, bucket, payload)
                 VALUES (?1, 'target', ?2, ?3)",
                rusqlite::params![id, n, payload],
            )
            .expect("insert fixture row");
        }
        tx.commit().expect("commit fixture");
    }
    let table_root_page_1based: u64 = writer
        .query_row(
            "SELECT rootpage FROM sqlite_master WHERE name = 'items'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .expect("items rootpage") as u64;
    drop(writer);

    let db = std::fs::read(&db_path).expect("read sqlite fixture");
    let page_size = page_size_from_sqlite_header(&db);
    assert_eq!(page_size, 4096);
    let page_count = db.len() / page_size;
    let mut rowid_to_leaf = HashMap::new();
    let mut index_leaf_candidates = Vec::new();
    let mut interior_pages = Vec::new();
    for page_num in 0..page_count as u64 {
        let page = sqlite_page(&db, page_size, page_num);
        match sqlite_page_type(page, page_num) {
            Some(0x05) => interior_pages.push(page_num),
            Some(0x0d) => {
                for rowid in table_leaf_rowids_for_test(page, page_num) {
                    if rowid >= 10_000 {
                        rowid_to_leaf.insert(rowid, page_num);
                    }
                }
            }
            Some(0x0a) => {
                let parsed = parse_index_leaf_rowids(page, page_num == 0, 256, Some(page_size));
                if parsed.rowids.len() > 32 {
                    index_leaf_candidates.push((page_num, parsed.rowids));
                }
            }
            _ => {}
        }
    }
    let (index_leaf_page_num, index_rowids) = index_leaf_candidates
        .into_iter()
        .max_by_key(|(_, rowids)| rowids.len())
        .expect("fixture should have a populated index leaf");
    assert!(
        index_rowids.len() > 32,
        "fixture index leaf must be larger than the lookahead cap"
    );
    let anchor_rowid = index_rowids[index_rowids.len() / 2];
    let anchor_page_num = *rowid_to_leaf
        .get(&anchor_rowid)
        .expect("anchor rowid should resolve to a table leaf");
    let anchor_matches: Vec<usize> = index_rowids
        .iter()
        .enumerate()
        .filter_map(|(idx, rowid)| {
            (rowid_to_leaf.get(rowid) == Some(&anchor_page_num)).then_some(idx)
        })
        .collect();
    let window = anchored_lookahead_window(index_rowids.len(), &anchor_matches, 32)
        .expect("real anchor should produce a lookahead window");
    let mut all_leaf_frames: Vec<usize> = index_rowids
        .iter()
        .filter_map(|rowid| rowid_to_leaf.get(rowid).map(|page| *page as usize))
        .collect();
    all_leaf_frames.sort_unstable();
    all_leaf_frames.dedup();
    let mut expected_frames: Vec<usize> = index_rowids[window]
        .iter()
        .filter_map(|rowid| rowid_to_leaf.get(rowid).map(|page| *page as usize))
        .collect();
    expected_frames.sort_unstable();
    expected_frames.dedup();
    assert!(
        expected_frames.len() < all_leaf_frames.len(),
        "anchored run should be smaller than the whole parsed leaf; expected={expected_frames:?}, all={all_leaf_frames:?}"
    );

    let support_leaf = rowid_to_leaf
        .values()
        .copied()
        .find(|page| !expected_frames.contains(&(*page as usize)))
        .expect("fixture should have a table leaf outside the anchored run");
    let pages: Vec<Option<Vec<u8>>> = (0..page_count as u64)
        .map(|page_num| Some(sqlite_page(&db, page_size, page_num).to_vec()))
        .collect();
    let (blob, frame_table) = encode_page_group_seekable(
        &pages,
        page_size as u32,
        1,
        3,
        #[cfg(feature = "zstd")]
        None,
        &[],
        None,
    )
    .expect("encode seekable fixture");

    let cache_dir = TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new(
            cache_dir.path(),
            3600,
            page_count as u32,
            1,
            page_size as u32,
            page_count as u64,
            None,
            Vec::new(),
        )
        .unwrap(),
    );
    let mut manifest = Manifest {
        page_count: page_count as u64,
        page_size: page_size as u32,
        pages_per_group: page_count as u32,
        sub_pages_per_frame: 1,
        page_group_keys: vec!["g0".to_string()],
        group_pages: vec![(0..page_count as u64).collect()],
        frame_tables: vec![frame_table.clone()],
        tree_name_to_root_page: HashMap::from([("items".to_string(), table_root_page_1based - 1)]),
        page_to_tree_name: HashMap::from([
            (index_leaf_page_num, "idx_items_category_bucket".to_string()),
            (anchor_page_num, "items".to_string()),
        ]),
        ..Manifest::empty()
    };
    manifest.tree_name_to_groups = HashMap::from([
        ("items".to_string(), vec![0]),
        ("idx_items_category_bucket".to_string(), vec![0]),
    ]);
    for page_num in interior_pages
        .into_iter()
        .chain(std::iter::once(support_leaf))
    {
        cache
            .write_pages_scattered(
                &[page_num],
                sqlite_page(&db, page_size, page_num),
                0,
                page_num as u32,
            )
            .unwrap();
    }

    let backend = Arc::new(BlockingRangeSeekableBackend {
        blob,
        release: AtomicBool::new(false),
        range_gets: AtomicU64::new(0),
    });
    let rt = tokio::runtime::Runtime::new().unwrap();
    let shared_manifest = Arc::new(ArcSwap::from_pointee(manifest.clone()));
    let storage: Arc<dyn StorageBackend> = backend.clone();
    let mut handle = TurboliteHandle::new_tiered(
        Some(storage.clone()),
        Some(rt.handle().clone()),
        Arc::clone(&cache),
        Arc::clone(&shared_manifest),
        Arc::new(Mutex::new(HashSet::new())),
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicU64::new(0)),
        cache_dir.path().join("real-index.lock"),
        page_count as u32,
        0,
        true,
        vec![0.3, 0.3, 0.4],
        vec![0.0, 0.0, 0.0],
        None,
        false,
        #[cfg(feature = "zstd")]
        None,
        None,
        false,
        false,
        4,
        32 * 1024 * 1024,
        None,
        false,
        0,
        0,
        false,
        true,
        Arc::new(parking_lot::RwLock::new(())),
        Arc::new(AtomicU64::new(0)),
    )
    .unwrap();
    let pool = Arc::new(PrefetchPool::new(
        1,
        storage,
        rt.handle().clone(),
        Arc::clone(&cache),
        page_count as u32,
        Arc::new(AtomicU64::new(page_count as u64)),
        #[cfg(feature = "zstd")]
        None,
        None,
        Arc::clone(&shared_manifest),
        Arc::new(parking_lot::RwLock::new(())),
        Arc::new(AtomicU64::new(0)),
        64,
        RemoteIoBudget::new(1, 0),
    ));
    handle.prefetch_pool = Some(Arc::clone(&pool));
    handle.query_plan_prefetch = true;
    handle.lookahead_enabled = true;
    handle.planned_lookahead_searches.insert(
        "idx_items_category_bucket".to_string(),
        PlannedLookaheadSearch {
            table_name: "items".to_string(),
            key_hint: None,
        },
    );

    let manifest_arc = shared_manifest.load_full();
    let index_page = sqlite_page(&db, page_size, index_leaf_page_num);
    handle.note_served_page_for_lookahead(
        index_leaf_page_num,
        Some(&"idx_items_category_bucket".to_string()),
        index_page,
        cache.as_ref(),
        &manifest_arc,
    );
    assert_eq!(
        handle
            .retained_lookahead
            .get("items")
            .map(|retained| retained.rowids.len()),
        Some(index_rowids.len())
    );
    handle.fire_retained_lookahead(
        "items",
        anchor_page_num,
        Some(sqlite_page(&db, page_size, anchor_page_num)),
        cache.as_ref(),
        &manifest_arc,
    );

    let mut submitted_frames = Vec::new();
    for _ in 0..1000 {
        submitted_frames = pool.pending_frame_indices_for_gid(0);
        if !submitted_frames.is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        submitted_frames, expected_frames,
        "lookahead must submit the anchored contiguous frame run, not every parsed index leaf frame"
    );
    assert_eq!(
        pool.stats_json()["lookahead"]["frames_submitted"],
        expected_frames.len() as u64
    );

    backend.release.store(true, Ordering::Release);
    pool.wait_idle();
}

#[test]
fn retained_lookahead_falls_back_to_served_leaf_anchor_when_root_uncached() {
    let _plan_guard = query_plan::plan_queue_test_guard();
    let sqlite_dir = TempDir::new().unwrap();
    let db_path = sqlite_dir.path().join("real_index_uncached_root.db");
    let writer = rusqlite::Connection::open(&db_path).expect("open sqlite fixture");
    writer
        .execute_batch(
            "PRAGMA page_size=4096;
             CREATE TABLE items (
               id INTEGER PRIMARY KEY,
               category TEXT NOT NULL,
               bucket INTEGER NOT NULL,
               payload TEXT NOT NULL
             );
             CREATE INDEX idx_items_category_bucket
               ON items(category, bucket);",
        )
        .expect("schema");
    {
        let tx = writer.unchecked_transaction().expect("fixture tx");
        let payload = "y".repeat(180);
        for n in 0..420i64 {
            let id = 20_000 + n;
            tx.execute(
                "INSERT INTO items (id, category, bucket, payload)
                 VALUES (?1, 'target', ?2, ?3)",
                rusqlite::params![id, n, payload],
            )
            .expect("insert fixture row");
        }
        tx.commit().expect("commit fixture");
    }
    let table_root_page_1based: u64 = writer
        .query_row(
            "SELECT rootpage FROM sqlite_master WHERE name = 'items'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .expect("items rootpage") as u64;
    drop(writer);

    let db = std::fs::read(&db_path).expect("read sqlite fixture");
    let page_size = page_size_from_sqlite_header(&db);
    let page_count = db.len() / page_size;
    let mut rowid_to_leaf = HashMap::new();
    let mut index_leaf_candidates = Vec::new();
    for page_num in 0..page_count as u64 {
        let page = sqlite_page(&db, page_size, page_num);
        match sqlite_page_type(page, page_num) {
            Some(0x0d) => {
                for rowid in table_leaf_rowids_for_test(page, page_num) {
                    if rowid >= 20_000 {
                        rowid_to_leaf.insert(rowid, page_num);
                    }
                }
            }
            Some(0x0a) => {
                let parsed = parse_index_leaf_rowids(page, page_num == 0, 256, Some(page_size));
                if parsed.rowids.len() > 32 {
                    index_leaf_candidates.push((page_num, parsed.rowids));
                }
            }
            _ => {}
        }
    }
    let (index_leaf_page_num, index_rowids) = index_leaf_candidates
        .into_iter()
        .max_by_key(|(_, rowids)| rowids.len())
        .expect("fixture should have a populated index leaf");
    let anchor_rowid = index_rowids[index_rowids.len() / 2];
    let anchor_page_num = *rowid_to_leaf
        .get(&anchor_rowid)
        .expect("anchor rowid should resolve to a table leaf");

    let pages: Vec<Option<Vec<u8>>> = (0..page_count as u64)
        .map(|page_num| Some(sqlite_page(&db, page_size, page_num).to_vec()))
        .collect();
    let (blob, frame_table) = encode_page_group_seekable(
        &pages,
        page_size as u32,
        1,
        3,
        #[cfg(feature = "zstd")]
        None,
        &[],
        None,
    )
    .expect("encode seekable fixture");

    let cache_dir = TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new(
            cache_dir.path(),
            3600,
            page_count as u32,
            1,
            page_size as u32,
            page_count as u64,
            None,
            Vec::new(),
        )
        .unwrap(),
    );
    let mut manifest = Manifest {
        page_count: page_count as u64,
        page_size: page_size as u32,
        pages_per_group: page_count as u32,
        sub_pages_per_frame: 1,
        page_group_keys: vec!["g0".to_string()],
        group_pages: vec![(0..page_count as u64).collect()],
        frame_tables: vec![frame_table.clone()],
        tree_name_to_root_page: HashMap::from([("items".to_string(), table_root_page_1based - 1)]),
        page_to_tree_name: HashMap::from([
            (index_leaf_page_num, "idx_items_category_bucket".to_string()),
            (anchor_page_num, "items".to_string()),
        ]),
        ..Manifest::empty()
    };
    manifest.tree_name_to_groups = HashMap::from([
        ("items".to_string(), vec![0]),
        ("idx_items_category_bucket".to_string(), vec![0]),
    ]);

    cache
        .write_pages_scattered(
            &[anchor_page_num],
            sqlite_page(&db, page_size, anchor_page_num),
            0,
            anchor_page_num as u32,
        )
        .unwrap();

    let backend = Arc::new(BlockingRangeSeekableBackend {
        blob,
        release: AtomicBool::new(false),
        range_gets: AtomicU64::new(0),
    });
    let rt = tokio::runtime::Runtime::new().unwrap();
    let shared_manifest = Arc::new(ArcSwap::from_pointee(manifest.clone()));
    let storage: Arc<dyn StorageBackend> = backend.clone();
    let mut handle = TurboliteHandle::new_tiered(
        Some(storage.clone()),
        Some(rt.handle().clone()),
        Arc::clone(&cache),
        Arc::clone(&shared_manifest),
        Arc::new(Mutex::new(HashSet::new())),
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicU64::new(0)),
        cache_dir.path().join("uncached-root.lock"),
        page_count as u32,
        0,
        true,
        vec![0.3, 0.3, 0.4],
        vec![0.0, 0.0, 0.0],
        None,
        false,
        #[cfg(feature = "zstd")]
        None,
        None,
        false,
        false,
        4,
        32 * 1024 * 1024,
        None,
        false,
        0,
        0,
        false,
        true,
        Arc::new(parking_lot::RwLock::new(())),
        Arc::new(AtomicU64::new(0)),
    )
    .unwrap();
    let pool = Arc::new(PrefetchPool::new(
        1,
        storage,
        rt.handle().clone(),
        Arc::clone(&cache),
        page_count as u32,
        Arc::new(AtomicU64::new(page_count as u64)),
        #[cfg(feature = "zstd")]
        None,
        None,
        Arc::clone(&shared_manifest),
        Arc::new(parking_lot::RwLock::new(())),
        Arc::new(AtomicU64::new(0)),
        64,
        RemoteIoBudget::new(1, 0),
    ));
    handle.prefetch_pool = Some(Arc::clone(&pool));
    handle.query_plan_prefetch = true;
    handle.lookahead_enabled = true;
    handle.planned_lookahead_searches.insert(
        "idx_items_category_bucket".to_string(),
        PlannedLookaheadSearch {
            table_name: "items".to_string(),
            key_hint: None,
        },
    );

    let manifest_arc = shared_manifest.load_full();
    handle.note_served_page_for_lookahead(
        index_leaf_page_num,
        Some(&"idx_items_category_bucket".to_string()),
        sqlite_page(&db, page_size, index_leaf_page_num),
        cache.as_ref(),
        &manifest_arc,
    );
    handle.fire_retained_lookahead(
        "items",
        anchor_page_num,
        Some(sqlite_page(&db, page_size, anchor_page_num)),
        cache.as_ref(),
        &manifest_arc,
    );

    let anchor_frame = anchor_page_num as usize;
    let mut expected_frames: Vec<usize> =
        anchored_frame_window(frame_table.len(), anchor_frame, 32).collect();
    expected_frames.retain(|frame| *frame != anchor_frame);
    let mut submitted_frames = Vec::new();
    for _ in 0..1000 {
        submitted_frames = pool.pending_frame_indices_for_gid(0);
        if !submitted_frames.is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        submitted_frames, expected_frames,
        "served-leaf fallback should submit the bounded contiguous frame run around the anchor"
    );
    assert_eq!(pool.stats_json()["lookahead"]["fired"], 1);
    assert_eq!(
        pool.stats_json()["lookahead"]["bailouts"]["uncached"],
        0,
        "served leaf should provide the anchor even when the table root is not cached"
    );

    backend.release.store(true, Ordering::Release);
    pool.wait_idle();
}

#[test]
fn lookahead_frame_respects_dirty_page_written_through_handle() {
    let dir = TempDir::new().unwrap();
    let (mut handle, cache) = handle_with_manifest(&dir, vec!["g0".to_string()]);
    handle.read_only = false;
    let page_size = 64u32;
    let pages: Vec<Option<Vec<u8>>> = (0..4)
        .map(|idx| Some(vec![idx as u8 + 1; page_size as usize]))
        .collect();
    let (blob, frame_table) = encode_page_group_seekable(
        &pages,
        page_size,
        2,
        3,
        #[cfg(feature = "zstd")]
        None,
        &[],
        None,
    )
    .unwrap();
    let backend = Arc::new(CountingSeekableBackend {
        blob,
        full_gets: AtomicU64::new(0),
        range_gets: AtomicU64::new(0),
    });
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pool = Arc::new(PrefetchPool::new(
        1,
        backend,
        rt.handle().clone(),
        Arc::clone(&cache),
        4,
        Arc::new(AtomicU64::new(4)),
        #[cfg(feature = "zstd")]
        None,
        None,
        Arc::clone(&handle.manifest),
        Arc::new(parking_lot::RwLock::new(())),
        Arc::new(AtomicU64::new(0)),
        8,
        RemoteIoBudget::new(1, 0),
    ));
    handle.prefetch_pool = Some(Arc::clone(&pool));

    handle
        .write_all_at(&vec![77u8; page_size as usize], 0)
        .unwrap();
    assert!(handle.dirty_page_nums.read().contains(&0));
    let manifest = handle.manifest.load();
    let outcome = pool.submit_frame_batch(
        Some("lookahead".to_string()),
        0,
        "g0".to_string(),
        frame_table,
        page_size,
        2,
        manifest.group_page_nums(0).into_owned(),
        vec![0],
        HashMap::new(),
        manifest.version,
        cache.as_ref(),
    );
    assert_eq!(outcome.outcome, PrefetchSubmitOutcome::Accepted);
    drop(manifest);
    pool.wait_idle();

    let mut page0 = vec![0u8; page_size as usize];
    cache.read_page(0, &mut page0).unwrap();
    assert_eq!(page0, vec![77u8; page_size as usize]);
    let mut page1 = vec![0u8; page_size as usize];
    cache.read_page(1, &mut page1).unwrap();
    assert_eq!(page1, vec![2u8; page_size as usize]);
}

#[test]
fn clear_timed_out_fetching_does_not_cancel_in_flight_frame_job() {
    let dir = TempDir::new().unwrap();
    let (handle, cache) = handle_with_manifest(&dir, vec!["g0".to_string()]);
    let page_size = 64u32;
    let pages: Vec<Option<Vec<u8>>> = (0..4)
        .map(|idx| Some(vec![idx as u8 + 11; page_size as usize]))
        .collect();
    let (blob, frame_table) = encode_page_group_seekable(
        &pages,
        page_size,
        2,
        3,
        #[cfg(feature = "zstd")]
        None,
        &[],
        None,
    )
    .unwrap();
    let backend = Arc::new(BlockingRangeSeekableBackend {
        blob,
        release: AtomicBool::new(false),
        range_gets: AtomicU64::new(0),
    });
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pool = Arc::new(PrefetchPool::new(
        1,
        backend.clone(),
        rt.handle().clone(),
        Arc::clone(&cache),
        4,
        Arc::new(AtomicU64::new(4)),
        #[cfg(feature = "zstd")]
        None,
        None,
        Arc::clone(&handle.manifest),
        Arc::new(parking_lot::RwLock::new(())),
        Arc::new(AtomicU64::new(0)),
        8,
        RemoteIoBudget::new(1, 0),
    ));
    let manifest = handle.manifest.load();
    assert_eq!(
        pool.submit_frame_batch(
            Some("lookahead".to_string()),
            0,
            "g0".to_string(),
            frame_table,
            page_size,
            2,
            manifest.group_page_nums(0).into_owned(),
            vec![0],
            HashMap::new(),
            manifest.version,
            cache.as_ref(),
        )
        .outcome,
        PrefetchSubmitOutcome::Accepted
    );
    drop(manifest);
    while backend.range_gets.load(Ordering::Acquire) == 0 {
        std::thread::sleep(Duration::from_millis(1));
    }

    assert!(cache.try_claim_group(0));
    assert!(handle.clear_timed_out_fetching(cache.as_ref(), 0));
    backend.release.store(true, Ordering::Release);
    pool.wait_idle();

    let mut page1 = vec![0u8; page_size as usize];
    cache.read_page(1, &mut page1).unwrap();
    assert_eq!(page1, vec![12u8; page_size as usize]);
    assert_eq!(cache.group_state(0), GroupState::None);
}

#[test]
fn demand_cache_hit_after_lookahead_frame_install_counts_hit() {
    let dir = TempDir::new().unwrap();
    let (mut handle, cache) = handle_with_manifest(&dir, vec!["g0".to_string()]);
    let page_size = 64u32;
    let pages: Vec<Option<Vec<u8>>> = (0..4)
        .map(|idx| Some(vec![idx as u8 + 21; page_size as usize]))
        .collect();
    let (blob, frame_table) = encode_page_group_seekable(
        &pages,
        page_size,
        2,
        3,
        #[cfg(feature = "zstd")]
        None,
        &[],
        None,
    )
    .unwrap();
    let backend = Arc::new(CountingSeekableBackend {
        blob,
        full_gets: AtomicU64::new(0),
        range_gets: AtomicU64::new(0),
    });
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pool = Arc::new(PrefetchPool::new(
        1,
        backend,
        rt.handle().clone(),
        Arc::clone(&cache),
        4,
        Arc::new(AtomicU64::new(4)),
        #[cfg(feature = "zstd")]
        None,
        None,
        Arc::clone(&handle.manifest),
        Arc::new(parking_lot::RwLock::new(())),
        Arc::new(AtomicU64::new(0)),
        8,
        RemoteIoBudget::new(1, 0),
    ));
    handle.prefetch_pool = Some(Arc::clone(&pool));
    handle.query_plan_prefetch = true;
    handle.lookahead_enabled = true;
    // Hits accrue while a lookahead-armed SEARCH query is live: the reads that
    // consume installed frames happen in the same query that populated
    // planned_lookahead_searches. The read-path fast path is gated on that map
    // being non-empty, so reproduce the arming context here.
    handle.planned_lookahead_searches.insert(
        "idx_data".to_string(),
        PlannedLookaheadSearch {
            table_name: "data".to_string(),
            key_hint: None,
        },
    );
    let mut manifest_for_hit = (**handle.manifest.load()).clone();
    manifest_for_hit.sub_pages_per_frame = 2;
    manifest_for_hit.frame_tables = vec![frame_table.clone()];
    handle.manifest.store(Arc::new(manifest_for_hit));

    let manifest = handle.manifest.load();
    assert_eq!(
        pool.submit_frame_batch(
            Some("lookahead".to_string()),
            0,
            "g0".to_string(),
            frame_table,
            page_size,
            2,
            manifest.group_page_nums(0).into_owned(),
            vec![0],
            HashMap::new(),
            manifest.version,
            cache.as_ref(),
        )
        .outcome,
        PrefetchSubmitOutcome::Accepted
    );
    drop(manifest);
    pool.wait_idle();

    query_plan::check_and_clear_end_query();
    let mut buf = vec![0u8; page_size as usize];
    handle.read_exact_at(&mut buf, page_size as u64).unwrap();
    assert_eq!(buf, vec![22u8; page_size as usize]);
    assert_eq!(pool.stats_json()["lookahead"]["hits"], 1);
}

#[test]
fn frame_job_counts_dup_bytes_when_demand_installs_frame_first() {
    let dir = TempDir::new().unwrap();
    let (handle, cache) = handle_with_manifest(&dir, vec!["g0".to_string()]);
    let page_size = 64u32;
    let pages: Vec<Option<Vec<u8>>> = (0..4)
        .map(|idx| Some(vec![idx as u8 + 31; page_size as usize]))
        .collect();
    let (blob, frame_table) = encode_page_group_seekable(
        &pages,
        page_size,
        1,
        3,
        #[cfg(feature = "zstd")]
        None,
        &[],
        None,
    )
    .unwrap();
    let backend = Arc::new(BlockingRangeSeekableBackend {
        blob,
        release: AtomicBool::new(false),
        range_gets: AtomicU64::new(0),
    });
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pool = Arc::new(PrefetchPool::new(
        1,
        backend.clone(),
        rt.handle().clone(),
        Arc::clone(&cache),
        4,
        Arc::new(AtomicU64::new(4)),
        #[cfg(feature = "zstd")]
        None,
        None,
        Arc::clone(&handle.manifest),
        Arc::new(parking_lot::RwLock::new(())),
        Arc::new(AtomicU64::new(0)),
        8,
        RemoteIoBudget::new(1, 0),
    ));
    let manifest = handle.manifest.load();
    assert_eq!(
        pool.submit_frame_batch(
            Some("lookahead".to_string()),
            0,
            "g0".to_string(),
            frame_table,
            page_size,
            1,
            manifest.group_page_nums(0).into_owned(),
            vec![1],
            HashMap::new(),
            manifest.version,
            cache.as_ref(),
        )
        .outcome,
        PrefetchSubmitOutcome::Accepted
    );
    drop(manifest);
    while backend.range_gets.load(Ordering::Acquire) == 0 {
        std::thread::sleep(Duration::from_millis(1));
    }
    cache
        .write_pages_scattered(&[1], &vec![99u8; page_size as usize], 0, 1)
        .unwrap();

    backend.release.store(true, Ordering::Release);
    pool.wait_idle();
    assert!(
        pool.stats_json()["lookahead"]["dup_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    let mut page1 = vec![0u8; page_size as usize];
    cache.read_page(1, &mut page1).unwrap();
    assert_eq!(page1, vec![99u8; page_size as usize]);
}

#[test]
fn seekable_read_waits_for_active_optional_full_prefetch_without_range_duplicate() {
    let dir = TempDir::new().unwrap();
    let page_size = 64u32;
    let pages_per_group = 4u32;
    let pages: Vec<Option<Vec<u8>>> = (0..pages_per_group)
        .map(|i| Some(vec![i as u8 + 11; page_size as usize]))
        .collect();
    let (blob, frame_table) = encode_page_group_seekable(
        &pages,
        page_size,
        2,
        7,
        #[cfg(feature = "zstd")]
        None,
        &[],
        None,
    )
    .unwrap();
    let backend = Arc::new(BlockingSeekableBackend {
        blob,
        release: AtomicBool::new(false),
        full_gets: AtomicU64::new(0),
        range_gets: AtomicU64::new(0),
    });
    let cache = Arc::new(
        DiskCache::new(
            dir.path(),
            3600,
            pages_per_group,
            2,
            page_size,
            8,
            None,
            Vec::new(),
        )
        .unwrap(),
    );
    let manifest = Manifest {
        page_count: 8,
        page_size,
        pages_per_group,
        sub_pages_per_frame: 2,
        page_group_keys: vec![String::new(), "g1".to_string()],
        group_pages: vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7]],
        frame_tables: vec![Vec::new(), frame_table.clone()],
        ..Manifest::empty()
    };
    let manifest_ref = Arc::new(ArcSwap::from_pointee(manifest));
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pool = Arc::new(PrefetchPool::new(
        1,
        backend.clone(),
        rt.handle().clone(),
        Arc::clone(&cache),
        pages_per_group,
        Arc::new(AtomicU64::new(8)),
        #[cfg(feature = "zstd")]
        None,
        None,
        Arc::clone(&manifest_ref),
        Arc::new(parking_lot::RwLock::new(())),
        Arc::new(AtomicU64::new(0)),
        8,
        RemoteIoBudget::new(2, 0),
    ));
    let mut handle = TurboliteHandle::new_tiered(
        Some(backend.clone()),
        Some(rt.handle().clone()),
        Arc::clone(&cache),
        manifest_ref,
        Arc::new(Mutex::new(HashSet::new())),
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicU64::new(0)),
        dir.path().join("db.lock"),
        pages_per_group,
        0,
        true,
        vec![0.3, 0.3, 0.4],
        vec![0.0, 0.0, 0.0],
        Some(pool.clone()),
        false,
        #[cfg(feature = "zstd")]
        None,
        None,
        false,
        false,
        4,
        32 * 1024 * 1024,
        None,
        false,
        0,
        0,
        false,
        false,
        Arc::new(parking_lot::RwLock::new(())),
        Arc::new(AtomicU64::new(0)),
    )
    .unwrap();

    assert_eq!(
        pool.submit_optional(
            Some("posts".to_string()),
            1,
            "g1".to_string(),
            frame_table,
            page_size,
            2,
            vec![4, 5, 6, 7],
            HashMap::new(),
            0,
            cache.as_ref(),
        ),
        PrefetchSubmitOutcome::Accepted
    );
    while backend.full_gets.load(Ordering::Acquire) == 0 || !pool.is_optional_fetching(1) {
        std::thread::sleep(Duration::from_millis(1));
    }

    let release = backend.clone();
    let releaser = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(25));
        release.release.store(true, Ordering::Release);
    });

    let mut buf = vec![0u8; page_size as usize];
    handle
        .read_exact_at(&mut buf, pages_per_group as u64 * page_size as u64)
        .unwrap();
    releaser.join().unwrap();
    pool.wait_idle();

    assert_eq!(buf, vec![11u8; page_size as usize]);
    assert_eq!(backend.full_gets.load(Ordering::Relaxed), 1);
    assert_eq!(backend.range_gets.load(Ordering::Relaxed), 0);
}

#[test]
fn planned_scan_seekable_current_group_uses_foreground_full_get_not_ranges() {
    let dir = TempDir::new().unwrap();
    let page_size = 64u32;
    let pages_per_group = 4u32;
    let pages: Vec<Option<Vec<u8>>> = (0..pages_per_group)
        .map(|i| Some(vec![i as u8 + 21; page_size as usize]))
        .collect();
    let (blob, frame_table) = encode_page_group_seekable(
        &pages,
        page_size,
        2,
        11,
        #[cfg(feature = "zstd")]
        None,
        &[],
        None,
    )
    .unwrap();
    let backend = Arc::new(RangeIgnoringReadBackend {
        blob,
        full_gets: AtomicU64::new(0),
    });
    let cache = Arc::new(
        DiskCache::new(
            dir.path(),
            3600,
            pages_per_group,
            2,
            page_size,
            8,
            None,
            Vec::new(),
        )
        .unwrap(),
    );
    let manifest = Manifest {
        page_count: 8,
        page_size,
        pages_per_group,
        sub_pages_per_frame: 2,
        page_group_keys: vec![String::new(), "g1".to_string()],
        group_pages: vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7]],
        frame_tables: vec![Vec::new(), frame_table],
        group_to_tree_name: HashMap::from([(1, "posts".to_string())]),
        ..Manifest::empty()
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut handle = TurboliteHandle::new_tiered(
        Some(backend.clone()),
        Some(rt.handle().clone()),
        Arc::clone(&cache),
        Arc::new(ArcSwap::from_pointee(manifest)),
        Arc::new(Mutex::new(HashSet::new())),
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicU64::new(0)),
        dir.path().join("db.lock"),
        pages_per_group,
        0,
        true,
        vec![0.3, 0.3, 0.4],
        vec![0.0, 0.0, 0.0],
        None,
        false,
        #[cfg(feature = "zstd")]
        None,
        None,
        false,
        false,
        4,
        32 * 1024 * 1024,
        None,
        false,
        0,
        0,
        false,
        false,
        Arc::new(parking_lot::RwLock::new(())),
        Arc::new(AtomicU64::new(0)),
    )
    .unwrap();
    handle.active_scan_prefetch.insert(
        "posts".to_string(),
        ActiveScanPrefetch {
            group_ids: vec![1],
            cursor: 0,
            submitted: HashSet::new(),
        },
    );

    let mut buf = vec![0u8; page_size as usize];
    handle
        .read_exact_at(&mut buf, pages_per_group as u64 * page_size as u64)
        .unwrap();

    assert_eq!(buf, vec![21u8; page_size as usize]);
    assert_eq!(backend.full_gets.load(Ordering::Relaxed), 1);
}

#[test]
fn planned_scan_seekable_group_uses_full_get_even_without_current_tree_name() {
    let dir = TempDir::new().unwrap();
    let page_size = 64u32;
    let pages_per_group = 4u32;
    let pages: Vec<Option<Vec<u8>>> = (0..pages_per_group)
        .map(|i| Some(vec![i as u8 + 31; page_size as usize]))
        .collect();
    let (blob, frame_table) = encode_page_group_seekable(
        &pages,
        page_size,
        2,
        13,
        #[cfg(feature = "zstd")]
        None,
        &[],
        None,
    )
    .unwrap();
    let backend = Arc::new(BlockingSeekableBackend {
        blob,
        release: AtomicBool::new(true),
        full_gets: AtomicU64::new(0),
        range_gets: AtomicU64::new(0),
    });
    let cache = Arc::new(
        DiskCache::new(
            dir.path(),
            3600,
            pages_per_group,
            2,
            page_size,
            8,
            None,
            Vec::new(),
        )
        .unwrap(),
    );
    let manifest = Manifest {
        page_count: 8,
        page_size,
        pages_per_group,
        sub_pages_per_frame: 2,
        page_group_keys: vec![String::new(), "g1".to_string()],
        group_pages: vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7]],
        frame_tables: vec![Vec::new(), frame_table],
        ..Manifest::empty()
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut handle = TurboliteHandle::new_tiered(
        Some(backend.clone()),
        Some(rt.handle().clone()),
        Arc::clone(&cache),
        Arc::new(ArcSwap::from_pointee(manifest)),
        Arc::new(Mutex::new(HashSet::new())),
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicU64::new(0)),
        dir.path().join("db.lock"),
        pages_per_group,
        0,
        true,
        vec![0.3, 0.3, 0.4],
        vec![0.0, 0.0, 0.0],
        None,
        false,
        #[cfg(feature = "zstd")]
        None,
        None,
        false,
        false,
        4,
        32 * 1024 * 1024,
        None,
        false,
        0,
        0,
        false,
        false,
        Arc::new(parking_lot::RwLock::new(())),
        Arc::new(AtomicU64::new(0)),
    )
    .unwrap();
    handle.active_scan_prefetch.insert(
        "planned-scan".to_string(),
        ActiveScanPrefetch {
            group_ids: vec![1],
            cursor: 0,
            submitted: HashSet::new(),
        },
    );

    let mut buf = vec![0u8; page_size as usize];
    handle
        .read_exact_at(&mut buf, pages_per_group as u64 * page_size as u64)
        .unwrap();

    assert_eq!(buf, vec![31u8; page_size as usize]);
    assert_eq!(backend.full_gets.load(Ordering::Relaxed), 1);
    assert_eq!(backend.range_gets.load(Ordering::Relaxed), 0);
}

#[test]
fn seekable_read_rejects_range_ignoring_backend_without_full_get_fallback() {
    let dir = TempDir::new().unwrap();
    let page_size = 64u32;
    let pages_per_group = 4u32;
    let pages: Vec<Option<Vec<u8>>> = (0..pages_per_group)
        .map(|i| Some(vec![i as u8 + 1; page_size as usize]))
        .collect();
    let (blob, frame_table) = encode_page_group_seekable(
        &pages,
        page_size,
        2,
        3,
        #[cfg(feature = "zstd")]
        None,
        &[],
        None,
    )
    .unwrap();
    let backend = Arc::new(RangeIgnoringReadBackend {
        blob,
        full_gets: AtomicU64::new(0),
    });
    let cache = Arc::new(
        DiskCache::new(
            dir.path(),
            3600,
            pages_per_group,
            2,
            page_size,
            8,
            None,
            Vec::new(),
        )
        .unwrap(),
    );
    let manifest = Manifest {
        page_count: 8,
        page_size,
        pages_per_group,
        sub_pages_per_frame: 2,
        page_group_keys: vec![String::new(), "g1".to_string()],
        group_pages: vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7]],
        frame_tables: vec![Vec::new(), frame_table],
        ..Manifest::empty()
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut handle = TurboliteHandle::new_tiered(
        Some(backend.clone()),
        Some(rt.handle().clone()),
        Arc::clone(&cache),
        Arc::new(ArcSwap::from_pointee(manifest)),
        Arc::new(Mutex::new(HashSet::new())),
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicU64::new(0)),
        dir.path().join("db.lock"),
        pages_per_group,
        0,
        true,
        vec![0.3, 0.3, 0.4],
        vec![0.0, 0.0, 0.0],
        None,
        false,
        #[cfg(feature = "zstd")]
        None,
        None,
        false,
        false,
        4,
        32 * 1024 * 1024,
        None,
        false,
        0,
        0,
        false,
        false,
        Arc::new(parking_lot::RwLock::new(())),
        Arc::new(AtomicU64::new(0)),
    )
    .unwrap();

    let mut buf = vec![0u8; page_size as usize];
    let err = handle
        .read_exact_at(&mut buf, pages_per_group as u64 * page_size as u64)
        .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(backend.full_gets.load(Ordering::Relaxed), 0);
}

#[test]
fn tiered_read_releases_claim_when_page_group_key_missing() {
    let dir = TempDir::new().unwrap();
    let (mut handle, cache) = handle_with_manifest(&dir, Vec::new());

    let mut buf = vec![0u8; 64];
    let err = handle.read_exact_at(&mut buf, 0).unwrap_err();

    // Remote reads for in-bounds pages with no backend object must error (A2).
    assert_eq!(cache.group_state(0), GroupState::None);
    assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
}

#[test]
fn tiered_read_releases_claim_when_page_group_key_empty() {
    let dir = TempDir::new().unwrap();
    let (mut handle, cache) = handle_with_manifest(&dir, vec![String::new()]);

    let mut buf = vec![0u8; 64];
    let err = handle.read_exact_at(&mut buf, 0).unwrap_err();

    assert_eq!(cache.group_state(0), GroupState::None);
    assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
}

#[test]
#[cfg(feature = "encryption")]
fn test_passthrough_write_read_roundtrip_encrypted() {
    // Passthrough files (WAL/journal) should CTR-encrypt on write and decrypt on read
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.wal");
    let file = FsOpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .unwrap();
    let key = test_key();
    let mut handle = TurboliteHandle::new_passthrough(file, path.clone(), Some(key));

    // Write data at various offsets
    let data1 = vec![0xAAu8; 128];
    let data2 = vec![0xBBu8; 256];
    handle.write_all_at(&data1, 0).unwrap();
    handle.write_all_at(&data2, 128).unwrap();

    // Read back — should get plaintext back through CTR decrypt
    let mut buf1 = vec![0u8; 128];
    let mut buf2 = vec![0u8; 256];
    handle.read_exact_at(&mut buf1, 0).unwrap();
    handle.read_exact_at(&mut buf2, 128).unwrap();

    assert_eq!(buf1, data1, "read at offset 0 must match written data");
    assert_eq!(buf2, data2, "read at offset 128 must match written data");
}

#[test]
#[cfg(feature = "encryption")]
fn test_passthrough_on_disk_not_plaintext() {
    // WAL file bytes on disk must NOT be plaintext when encryption enabled
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.wal");
    let file = FsOpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .unwrap();
    let key = test_key();
    let mut handle = TurboliteHandle::new_passthrough(file, path.clone(), Some(key));

    let plaintext = vec![0xCCu8; 512];
    handle.write_all_at(&plaintext, 0).unwrap();

    // Read raw bytes from disk (bypassing VFS)
    let raw = std::fs::read(&path).unwrap();
    assert_ne!(
        &raw[..512],
        &plaintext[..],
        "on-disk bytes must NOT be plaintext"
    );
}

#[test]
#[cfg(feature = "encryption")]
fn test_passthrough_wrong_key_returns_garbage() {
    // Reading with a different key must return garbage, not the original data
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.wal");

    // Write with correct key
    {
        let file = FsOpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        let key = test_key();
        let mut handle = TurboliteHandle::new_passthrough(file, path.clone(), Some(key));
        handle.write_all_at(&vec![0xDDu8; 256], 0).unwrap();
    }

    // Read with wrong key
    let file = FsOpenOptions::new().read(true).open(&path).unwrap();
    let mut handle = TurboliteHandle::new_passthrough(file, path.clone(), Some(wrong_key()));
    let mut buf = vec![0u8; 256];
    handle.read_exact_at(&mut buf, 0).unwrap(); // CTR doesn't fail, just produces wrong data
    assert_ne!(
        buf,
        vec![0xDDu8; 256],
        "wrong key must not produce original plaintext"
    );
}

#[test]
#[cfg(feature = "encryption")]
fn test_passthrough_no_encryption_is_plaintext() {
    // Without encryption key, passthrough should be plain read/write
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.wal");
    let file = FsOpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .unwrap();
    let mut handle = TurboliteHandle::new_passthrough(file, path.clone(), None);

    let plaintext = vec![0xEEu8; 128];
    handle.write_all_at(&plaintext, 0).unwrap();

    // On-disk should be plaintext
    let raw = std::fs::read(&path).unwrap();
    assert_eq!(
        &raw[..128],
        &plaintext[..],
        "without encryption, on-disk must be plaintext"
    );
}

#[test]
#[cfg(feature = "encryption")]
fn test_passthrough_different_offsets_different_ciphertext() {
    // Same data written at different offsets must produce different ciphertext
    // (each write picks a fresh random nonce stored in the sidecar).
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.wal");
    let file = FsOpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .unwrap();
    let key = test_key();
    let mut handle = TurboliteHandle::new_passthrough(file, path.clone(), Some(key));

    let data = vec![0xFFu8; 64];
    handle.write_all_at(&data, 0).unwrap();
    handle.write_all_at(&data, 64).unwrap();

    let raw = std::fs::read(&path).unwrap();
    assert_ne!(
        &raw[..64],
        &raw[64..128],
        "same data at different offsets must produce different ciphertext"
    );
}

#[test]
#[cfg(feature = "encryption")]
fn test_passthrough_same_offset_rewrite_decrypts_and_no_keystream_reuse() {
    // Rewriting the same WAL slot must (a) decrypt to the newest plaintext and
    // (b) use a fresh nonce so the two ciphertexts differ — the property the
    // append-only nonce sidecar exists to guarantee.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.wal");
    let open = || {
        FsOpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap()
    };
    let key = test_key();
    let mut handle = TurboliteHandle::new_passthrough(open(), path.clone(), Some(key));

    let v1 = vec![0x11u8; 128];
    handle.write_all_at(&v1, 0).unwrap();
    let raw1 = std::fs::read(&path).unwrap()[..128].to_vec();

    let v2 = vec![0x22u8; 128];
    handle.write_all_at(&v2, 0).unwrap();
    let raw2 = std::fs::read(&path).unwrap()[..128].to_vec();

    // Same offset, but XOR of the two ciphertexts must not equal XOR of the
    // two plaintexts (that equality is the two-time-pad signature).
    let ct_xor: Vec<u8> = raw1.iter().zip(&raw2).map(|(a, b)| a ^ b).collect();
    let pt_xor: Vec<u8> = v1.iter().zip(&v2).map(|(a, b)| a ^ b).collect();
    assert_ne!(ct_xor, pt_xor, "rewrite must not reuse the keystream");

    // Newest write decrypts.
    let mut buf = vec![0u8; 128];
    handle.read_exact_at(&mut buf, 0).unwrap();
    assert_eq!(buf, v2, "read after rewrite must return the newest value");
}

#[test]
#[cfg(feature = "encryption")]
fn test_passthrough_sidecar_reloads_and_compacts_across_restart() {
    // The append-only sidecar must survive a reopen (the load path replays the
    // log) and compact down to one record per live extent rather than growing
    // unbounded with rewrites.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.wal");
    let sidecar = {
        let mut s = path.clone().into_os_string();
        s.push(".tlnonce");
        std::path::PathBuf::from(s)
    };
    let open = || {
        FsOpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap()
    };
    let key = test_key();

    // Many rewrites of the same single extent: the append log grows by one
    // record each, but the live extent count stays at 1.
    {
        let mut handle = TurboliteHandle::new_passthrough(open(), path.clone(), Some(key));
        for i in 0..20u8 {
            handle.write_all_at(&vec![i; 128], 0).unwrap();
        }
        // Drop handle (simulates shutdown).
    }
    let grown = std::fs::metadata(&sidecar).unwrap().len();
    assert!(grown >= 24, "sidecar should hold at least one record");

    // Reopen: load() replays the log and compacts it to the single live extent
    // (24 bytes), and the newest plaintext still decrypts.
    let mut handle = TurboliteHandle::new_passthrough(open(), path.clone(), Some(key));
    let compacted = std::fs::metadata(&sidecar).unwrap().len();
    assert_eq!(
        compacted, 24,
        "load must compact the log to one record per live extent"
    );

    let mut buf = vec![0u8; 128];
    handle.read_exact_at(&mut buf, 0).unwrap();
    assert_eq!(
        buf,
        vec![19u8; 128],
        "newest value must survive the reload + compaction"
    );

    // A further write after reload still appends durably and reads back.
    handle.write_all_at(&vec![0x55u8; 128], 0).unwrap();
    let mut buf2 = vec![0u8; 128];
    handle.read_exact_at(&mut buf2, 0).unwrap();
    assert_eq!(buf2, vec![0x55u8; 128]);
}

struct CountingSeekableBackend {
    blob: Vec<u8>,
    full_gets: AtomicU64,
    range_gets: AtomicU64,
}

#[async_trait]
impl StorageBackend for CountingSeekableBackend {
    async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>> {
        self.full_gets.fetch_add(1, Ordering::AcqRel);
        Ok(Some(self.blob.clone()))
    }

    async fn put(&self, _key: &str, _data: &[u8]) -> Result<()> {
        Ok(())
    }

    async fn delete(&self, _key: &str) -> Result<()> {
        Ok(())
    }

    async fn list(&self, _prefix: &str, _after: Option<&str>) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    async fn put_if_absent(&self, _key: &str, _data: &[u8]) -> Result<hadb_storage::CasResult> {
        Ok(hadb_storage::CasResult {
            success: true,
            etag: None,
        })
    }

    async fn put_if_match(
        &self,
        _key: &str,
        _data: &[u8],
        _etag: &str,
    ) -> Result<hadb_storage::CasResult> {
        Ok(hadb_storage::CasResult {
            success: true,
            etag: None,
        })
    }

    async fn range_get(&self, _key: &str, start: u64, len: u32) -> Result<Option<Vec<u8>>> {
        self.range_gets.fetch_add(1, Ordering::AcqRel);
        let start = start as usize;
        let end = start + len as usize;
        Ok(Some(self.blob[start..end].to_vec()))
    }
}

/// A seekable demand miss outside any planned scan is range-first, and it
/// also warms the rest of the current group with one optional full-group
/// prefetch so later pages from the same group become cache hits instead of
/// more serial range round trips.
#[test]
fn seekable_miss_warms_current_group_via_optional_prefetch() {
    let dir = TempDir::new().unwrap();
    let page_size = 64u32;
    let pages_per_group = 4u32;
    let pages: Vec<Option<Vec<u8>>> = (0..pages_per_group)
        .map(|i| Some(vec![i as u8 + 41; page_size as usize]))
        .collect();
    let (blob, frame_table) = encode_page_group_seekable(
        &pages,
        page_size,
        2,
        13,
        #[cfg(feature = "zstd")]
        None,
        &[],
        None,
    )
    .unwrap();
    let backend = Arc::new(CountingSeekableBackend {
        blob,
        full_gets: AtomicU64::new(0),
        range_gets: AtomicU64::new(0),
    });
    let cache = Arc::new(
        DiskCache::new(
            dir.path(),
            3600,
            pages_per_group,
            2,
            page_size,
            8,
            None,
            Vec::new(),
        )
        .unwrap(),
    );
    let manifest = Manifest {
        page_count: 8,
        page_size,
        pages_per_group,
        sub_pages_per_frame: 2,
        page_group_keys: vec![String::new(), "g1".to_string()],
        group_pages: vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7]],
        frame_tables: vec![Vec::new(), frame_table],
        group_to_tree_name: HashMap::from([(1, "posts".to_string())]),
        ..Manifest::empty()
    };
    let manifest_ref = Arc::new(ArcSwap::from_pointee(manifest));
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pool = Arc::new(PrefetchPool::new(
        1,
        backend.clone(),
        rt.handle().clone(),
        Arc::clone(&cache),
        pages_per_group,
        Arc::new(AtomicU64::new(8)),
        #[cfg(feature = "zstd")]
        None,
        None,
        Arc::clone(&manifest_ref),
        Arc::new(parking_lot::RwLock::new(())),
        Arc::new(AtomicU64::new(0)),
        8,
        RemoteIoBudget::new(4, 1),
    ));
    let mut handle = TurboliteHandle::new_tiered(
        Some(backend.clone()),
        Some(rt.handle().clone()),
        Arc::clone(&cache),
        manifest_ref,
        Arc::new(Mutex::new(HashSet::new())),
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicU64::new(0)),
        dir.path().join("db.lock"),
        pages_per_group,
        0,
        true,
        vec![0.3, 0.3, 0.4],
        vec![0.0, 0.0, 0.0],
        Some(pool.clone()),
        false,
        #[cfg(feature = "zstd")]
        None,
        None,
        false,
        false,
        4,
        32 * 1024 * 1024,
        None,
        false,
        0,
        0,
        false,
        false,
        Arc::new(parking_lot::RwLock::new(())),
        Arc::new(AtomicU64::new(0)),
    )
    .unwrap();

    // First read of group 1 misses: foreground serves it with one range GET
    // and submits one optional full-group warm prefetch.
    let mut buf = vec![0u8; page_size as usize];
    handle
        .read_exact_at(&mut buf, pages_per_group as u64 * page_size as u64)
        .unwrap();
    assert_eq!(buf, vec![41u8; page_size as usize]);
    assert_eq!(backend.range_gets.load(Ordering::Relaxed), 1);

    pool.wait_idle();
    assert_eq!(backend.full_gets.load(Ordering::Relaxed), 1);
    assert_eq!(cache.group_state(1), GroupState::Present);

    // A page from the other frame of the same group is now a cache hit:
    // no additional range or full GETs.
    let mut buf2 = vec![0u8; page_size as usize];
    handle
        .read_exact_at(&mut buf2, (pages_per_group as u64 + 2) * page_size as u64)
        .unwrap();
    assert_eq!(buf2, vec![43u8; page_size as usize]);
    assert_eq!(backend.range_gets.load(Ordering::Relaxed), 1);
    assert_eq!(backend.full_gets.load(Ordering::Relaxed), 1);
}

struct TimedOutTakeoverBackend {
    stale_full_blob: Vec<u8>,
    fresh_range_blob: Vec<u8>,
    release_full_get: AtomicBool,
    full_gets: AtomicU64,
    range_gets: AtomicU64,
}

#[async_trait]
impl StorageBackend for TimedOutTakeoverBackend {
    async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>> {
        self.full_gets.fetch_add(1, Ordering::AcqRel);
        while !self.release_full_get.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(Some(self.stale_full_blob.clone()))
    }

    async fn put(&self, _key: &str, _data: &[u8]) -> Result<()> {
        Ok(())
    }

    async fn delete(&self, _key: &str) -> Result<()> {
        Ok(())
    }

    async fn list(&self, _prefix: &str, _after: Option<&str>) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    async fn put_if_absent(&self, _key: &str, _data: &[u8]) -> Result<hadb_storage::CasResult> {
        Ok(hadb_storage::CasResult {
            success: true,
            etag: None,
        })
    }

    async fn put_if_match(
        &self,
        _key: &str,
        _data: &[u8],
        _etag: &str,
    ) -> Result<hadb_storage::CasResult> {
        Ok(hadb_storage::CasResult {
            success: true,
            etag: None,
        })
    }

    async fn range_get(&self, _key: &str, start: u64, len: u32) -> Result<Option<Vec<u8>>> {
        self.range_gets.fetch_add(1, Ordering::AcqRel);
        let start = start as usize;
        let end = start + len as usize;
        Ok(Some(self.fresh_range_blob[start..end].to_vec()))
    }
}

#[test]
fn seekable_timeout_takeover_keeps_foreground_frame_when_stale_prefetch_finishes() {
    let dir = TempDir::new().unwrap();
    let page_size = 64u32;
    let pages_per_group = 4u32;
    let fresh_pages: Vec<Option<Vec<u8>>> = (0..pages_per_group)
        .map(|i| Some(vec![i as u8 + 51; page_size as usize]))
        .collect();
    let stale_pages: Vec<Option<Vec<u8>>> = (0..pages_per_group)
        .map(|i| Some(vec![i as u8 + 101; page_size as usize]))
        .collect();
    let (fresh_blob, frame_table) = encode_page_group_seekable(
        &fresh_pages,
        page_size,
        2,
        17,
        #[cfg(feature = "zstd")]
        None,
        &[],
        None,
    )
    .unwrap();
    let (stale_blob, stale_frame_table) = encode_page_group_seekable(
        &stale_pages,
        page_size,
        2,
        17,
        #[cfg(feature = "zstd")]
        None,
        &[],
        None,
    )
    .unwrap();
    assert_eq!(
        frame_table
            .iter()
            .map(|entry| (entry.offset, entry.len))
            .collect::<Vec<_>>(),
        stale_frame_table
            .iter()
            .map(|entry| (entry.offset, entry.len))
            .collect::<Vec<_>>(),
        "fresh and stale fixtures must share frame ranges for this race test"
    );

    let backend = Arc::new(TimedOutTakeoverBackend {
        stale_full_blob: stale_blob,
        fresh_range_blob: fresh_blob,
        release_full_get: AtomicBool::new(false),
        full_gets: AtomicU64::new(0),
        range_gets: AtomicU64::new(0),
    });
    let cache = Arc::new(
        DiskCache::new(
            dir.path(),
            3600,
            pages_per_group,
            2,
            page_size,
            8,
            None,
            Vec::new(),
        )
        .unwrap(),
    );
    let manifest = Manifest {
        page_count: 8,
        page_size,
        pages_per_group,
        sub_pages_per_frame: 2,
        page_group_keys: vec![String::new(), "g1".to_string()],
        group_pages: vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7]],
        frame_tables: vec![Vec::new(), frame_table.clone()],
        ..Manifest::empty()
    };
    let manifest_ref = Arc::new(ArcSwap::from_pointee(manifest));
    let replay_gate = Arc::new(parking_lot::RwLock::new(()));
    let replay_epoch = Arc::new(AtomicU64::new(0));
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pool = Arc::new(PrefetchPool::new(
        1,
        backend.clone(),
        rt.handle().clone(),
        Arc::clone(&cache),
        pages_per_group,
        Arc::new(AtomicU64::new(8)),
        #[cfg(feature = "zstd")]
        None,
        None,
        Arc::clone(&manifest_ref),
        Arc::clone(&replay_gate),
        Arc::clone(&replay_epoch),
        8,
        RemoteIoBudget::new(2, 0),
    ));
    let mut handle = TurboliteHandle::new_tiered(
        Some(backend.clone()),
        Some(rt.handle().clone()),
        Arc::clone(&cache),
        manifest_ref,
        Arc::new(Mutex::new(HashSet::new())),
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicU64::new(0)),
        dir.path().join("db.lock"),
        pages_per_group,
        0,
        true,
        vec![0.3, 0.3, 0.4],
        vec![0.0, 0.0, 0.0],
        Some(pool.clone()),
        false,
        #[cfg(feature = "zstd")]
        None,
        None,
        false,
        false,
        4,
        32 * 1024 * 1024,
        None,
        false,
        0,
        0,
        false,
        false,
        Arc::clone(&replay_gate),
        Arc::clone(&replay_epoch),
    )
    .unwrap();

    assert_eq!(
        pool.submit_optional(
            Some("posts".to_string()),
            1,
            "g1".to_string(),
            frame_table,
            page_size,
            2,
            vec![4, 5, 6, 7],
            HashMap::new(),
            0,
            cache.as_ref(),
        ),
        PrefetchSubmitOutcome::Accepted
    );
    while backend.full_gets.load(Ordering::Acquire) == 0 || !pool.is_optional_fetching(1) {
        std::thread::sleep(Duration::from_millis(1));
    }

    replay_epoch.fetch_add(1, Ordering::AcqRel);

    let mut buf = vec![0u8; page_size as usize];
    handle
        .read_exact_at(&mut buf, pages_per_group as u64 * page_size as u64)
        .unwrap();
    assert_eq!(buf, vec![51u8; page_size as usize]);
    assert_eq!(backend.range_gets.load(Ordering::Relaxed), 1);

    backend.release_full_get.store(true, Ordering::Release);
    pool.wait_idle();

    let mut cached = vec![0u8; page_size as usize];
    cache
        .read_page(pages_per_group as u64, &mut cached)
        .unwrap();
    assert_eq!(
        cached,
        vec![51u8; page_size as usize],
        "late optional full-group prefetch must not overwrite the foreground frame"
    );
    assert_eq!(cache.group_state(1), GroupState::None);
    assert_eq!(pool.stats_json()["cancelled"], 1);
}

struct FailingPutBackend;

#[async_trait]
impl StorageBackend for FailingPutBackend {
    async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
    async fn put(&self, _key: &str, _data: &[u8]) -> Result<()> {
        Err(anyhow::anyhow!("simulated put failure"))
    }
    async fn delete(&self, _key: &str) -> Result<()> {
        Ok(())
    }
    async fn list(&self, _prefix: &str, _after: Option<&str>) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
    async fn put_if_absent(&self, _key: &str, _data: &[u8]) -> Result<hadb_storage::CasResult> {
        Ok(hadb_storage::CasResult {
            success: true,
            etag: None,
        })
    }
    async fn put_if_match(
        &self,
        _key: &str,
        _data: &[u8],
        _etag: &str,
    ) -> Result<hadb_storage::CasResult> {
        Ok(hadb_storage::CasResult {
            success: true,
            etag: None,
        })
    }
    async fn range_get(&self, _key: &str, _start: u64, _len: u32) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
}

/// A flush failure during lock downgrade must be propagated to the caller,
/// not swallowed with an eprintln (A5).
#[test]
fn test_lock_downgrade_flush_error_propagated() {
    let dir = TempDir::new().unwrap();
    let page_size = 64u32;
    let pages_per_group = 4u32;
    let cache = Arc::new(
        DiskCache::new(
            dir.path(),
            3600,
            pages_per_group,
            2,
            page_size,
            pages_per_group as u64,
            None,
            Vec::new(),
        )
        .unwrap(),
    );
    let manifest = Manifest {
        page_count: pages_per_group as u64,
        page_size,
        pages_per_group,
        page_group_keys: vec!["".to_string(); 1],
        ..Manifest::empty()
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut handle = TurboliteHandle::new_tiered(
        Some(Arc::new(FailingPutBackend)),
        Some(rt.handle().clone()),
        Arc::clone(&cache),
        Arc::new(ArcSwap::from_pointee(manifest)),
        Arc::new(Mutex::new(HashSet::new())),
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicU64::new(0)),
        dir.path().join("db.lock"),
        pages_per_group,
        0,
        false, // read/write
        vec![0.3, 0.3, 0.4],
        vec![0.0, 0.0, 0.0],
        None,
        false,
        #[cfg(feature = "zstd")]
        None,
        None,
        false,
        false,
        4,
        32 * 1024 * 1024,
        None,
        false,
        0,
        0,
        false,
        false,
        Arc::new(parking_lot::RwLock::new(())),
        Arc::new(AtomicU64::new(0)),
    )
    .unwrap();

    // Acquire exclusive lock via the normal SQLite escalation path.
    assert!(handle.lock(LockKind::Shared).unwrap());
    assert!(handle.lock(LockKind::Exclusive).unwrap());

    // Write every page in group 0 so flush does not need to fetch missing
    // pages from the backend.
    for i in 0..pages_per_group as u64 {
        handle
            .write_all_at(&vec![0xABu8; page_size as usize], i * page_size as u64)
            .unwrap();
    }

    // Pretend xSync fired, so a subsequent downgrade would try to flush.
    handle.synced_since_write = true;

    // Downgrading to shared must fail because the backend put fails, and the
    // error must be returned (not swallowed).
    let err = handle.lock(LockKind::Shared).unwrap_err();
    assert!(
        err.to_string().contains("simulated put failure"),
        "expected flush error to propagate, got: {}",
        err
    );
}

/// Remote reads for in-bounds pages whose backend object is missing must
/// return a hard I/O error instead of silently zero-filling (A2).
#[test]
fn test_remote_read_missing_group_errors() {
    let dir = TempDir::new().unwrap();
    let page_size = 64u32;
    let pages_per_group = 4u32;
    let cache = Arc::new(
        DiskCache::new(
            dir.path(),
            3600,
            pages_per_group,
            2,
            page_size,
            pages_per_group as u64,
            None,
            Vec::new(),
        )
        .unwrap(),
    );
    let manifest = Manifest {
        page_count: pages_per_group as u64,
        page_size,
        pages_per_group,
        // Point to a key that the backend does not contain.
        page_group_keys: vec!["nonexistent-group-key".to_string()],
        ..Manifest::empty()
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut handle = TurboliteHandle::new_tiered(
        Some(Arc::new(MemStorage::new())),
        Some(rt.handle().clone()),
        Arc::clone(&cache),
        Arc::new(ArcSwap::from_pointee(manifest)),
        Arc::new(Mutex::new(HashSet::new())),
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicU64::new(0)),
        dir.path().join("db.lock"),
        pages_per_group,
        0,
        false,
        vec![0.3, 0.3, 0.4],
        vec![0.0, 0.0, 0.0],
        None,
        false,
        #[cfg(feature = "zstd")]
        None,
        None,
        false,
        false,
        4,
        32 * 1024 * 1024,
        None,
        false,
        0,
        0,
        false,
        false,
        Arc::new(parking_lot::RwLock::new(())),
        Arc::new(AtomicU64::new(0)),
    )
    .unwrap();

    let mut buf = vec![0u8; page_size as usize];
    let err = handle.read_exact_at(&mut buf, 0).unwrap_err();
    assert_eq!(
        err.kind(),
        io::ErrorKind::NotFound,
        "missing remote group should produce NotFound, got: {}",
        err
    );
}

/// Speculative file growth from xWrite must not be visible to other handles
/// through the shared manifest until sync() commits it (A6).
#[test]
fn test_speculative_page_count_not_shared() {
    let dir = TempDir::new().unwrap();
    let page_size = 64u32;
    let pages_per_group = 4u32;
    let shared_manifest = Arc::new(ArcSwap::from_pointee(Manifest {
        page_count: 1,
        page_size,
        pages_per_group,
        ..Manifest::empty()
    }));
    let shared_dirty = Arc::new(Mutex::new(HashSet::new()));
    let shared_pending = Arc::new(Mutex::new(Vec::new()));
    let shared_seq = Arc::new(AtomicU64::new(0));

    let make_handle = || {
        let cache = Arc::new(
            DiskCache::new(
                dir.path(),
                3600,
                pages_per_group,
                2,
                page_size,
                16,
                None,
                Vec::new(),
            )
            .unwrap(),
        );
        let rt = tokio::runtime::Runtime::new().unwrap();
        TurboliteHandle::new_tiered(
            Some(Arc::new(MemStorage::new())),
            Some(rt.handle().clone()),
            cache,
            Arc::clone(&shared_manifest),
            Arc::clone(&shared_dirty),
            Arc::clone(&shared_pending),
            Arc::clone(&shared_seq),
            dir.path().join("db.lock"),
            pages_per_group,
            0,
            false,
            vec![0.3, 0.3, 0.4],
            vec![0.0, 0.0, 0.0],
            None,
            false,
            #[cfg(feature = "zstd")]
            None,
            None,
            false,
            false,
            4,
            32 * 1024 * 1024,
            None,
            false,
            0,
            0,
            false,
            false,
            Arc::new(parking_lot::RwLock::new(())),
            Arc::new(AtomicU64::new(0)),
        )
        .unwrap()
    };

    let mut writer = make_handle();
    let reader = make_handle();

    assert_eq!(reader.size().unwrap(), page_size as u64);
    assert_eq!(shared_manifest.load().page_count, 1);

    // Write a page beyond the current file size.
    writer
        .write_all_at(&vec![0xCDu8; page_size as usize], 3 * page_size as u64)
        .unwrap();

    // The writing handle sees the speculative size; the shared manifest and
    // the other handle do not.
    assert_eq!(writer.size().unwrap(), 4 * page_size as u64);
    assert_eq!(shared_manifest.load().page_count, 1);
    assert_eq!(reader.size().unwrap(), page_size as u64);
}

// --- D4/D5 regression test ---

#[test]
fn end_of_query_clears_prefetch_state() {
    let dir = TempDir::new().unwrap();
    let (mut handle, cache) = handle_with_manifest(&dir, vec!["g0".to_string()]);

    // Seed page 0 so the slow-path read can succeed without remote I/O.
    let page = vec![1u8; 64];
    cache
        .write_pages_scattered(&[0], &page, 0, 0)
        .expect("seed page");

    // Populate per-query prefetch state as if a query just ran.
    handle.search_trees.insert("users".to_string());
    handle.active_scan_prefetch.insert(
        "posts".to_string(),
        ActiveScanPrefetch {
            group_ids: vec![0],
            cursor: 0,
            submitted: HashSet::new(),
        },
    );
    handle.last_scan_refill_gid = Some(0);

    // Signal end-of-query and force the slow path so the cleanup block runs.
    query_plan::signal_end_query();
    handle.dirty_since_sync = true;
    let mut buf = vec![0u8; 64];
    handle.read_exact_at(&mut buf, 0).expect("read seeded page");

    assert!(
        handle.search_trees.is_empty(),
        "search_trees must be cleared at end of query"
    );
    assert!(
        handle.active_scan_prefetch.is_empty(),
        "active_scan_prefetch must be cleared at end of query"
    );
    assert_eq!(
        handle.last_scan_refill_gid, None,
        "last_scan_refill_gid must be cleared at end of query"
    );
}

