use super::*;
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
    query_plan::drain_planned_accesses();
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

    let mut buf = vec![0u8; 64];
    handle.read_exact_at(&mut buf, 0).unwrap();

    let stats = pool.stats_json();
    assert!(stats["max_queued"].as_u64().unwrap() <= 2);
    assert!(handle.active_scan_prefetch["users"].submitted.len() <= 2);
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
    handle.read_exact_at(&mut buf, 0).unwrap();

    assert_eq!(cache.group_state(0), GroupState::None);
    assert_eq!(buf, vec![0u8; 64]);
}

#[test]
fn tiered_read_releases_claim_when_page_group_key_empty() {
    let dir = TempDir::new().unwrap();
    let (mut handle, cache) = handle_with_manifest(&dir, vec![String::new()]);

    let mut buf = vec![0u8; 64];
    handle.read_exact_at(&mut buf, 0).unwrap();

    assert_eq!(cache.group_state(0), GroupState::None);
    assert_eq!(buf, vec![0u8; 64]);
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
