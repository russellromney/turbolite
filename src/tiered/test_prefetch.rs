use super::*;
use crate::tiered::*;
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[derive(Default)]
struct DeterministicPressureBackend {
    base_sleep_ms: u64,
    started: AtomicU64,
    active: AtomicU64,
    max_active: AtomicU64,
}

struct BlockingPayloadBackend {
    release: AtomicBool,
    started: AtomicU64,
    payload: Vec<u8>,
}

#[async_trait]
impl StorageBackend for DeterministicPressureBackend {
    async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>> {
        self.started.fetch_add(1, Ordering::Release);
        let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
        self.max_active.fetch_max(active, Ordering::Relaxed);
        if self.base_sleep_ms > 0 {
            tokio::time::sleep(Duration::from_millis(self.base_sleep_ms * active)).await;
        }
        self.active.fetch_sub(1, Ordering::AcqRel);
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
}

#[async_trait]
impl StorageBackend for BlockingPayloadBackend {
    async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>> {
        self.started.fetch_add(1, Ordering::Release);
        while !self.release.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Ok(Some(self.payload.clone()))
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
}

fn test_pool(
    cache: Arc<DiskCache>,
    backend: Arc<dyn StorageBackend>,
    queue_capacity: u32,
    io_permits: u32,
    foreground_reserved: u32,
) -> PrefetchPool {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let handle = rt.handle().clone();
    std::mem::forget(rt);
    PrefetchPool::new(
        1,
        backend,
        handle,
        cache,
        4,
        Arc::new(AtomicU64::new(0)),
        #[cfg(feature = "zstd")]
        None,
        None,
        Arc::new(ArcSwap::from_pointee(Manifest::empty())),
        Arc::new(parking_lot::RwLock::new(())),
        Arc::new(AtomicU64::new(0)),
        queue_capacity,
        RemoteIoBudget::new(io_permits, foreground_reserved),
    )
}

fn submit_test_job(pool: &PrefetchPool, cache: &DiskCache, gid: u64) -> PrefetchSubmitOutcome {
    pool.submit_optional(
        Some("scan".to_string()),
        gid,
        format!("g{gid}"),
        Vec::new(),
        64,
        0,
        Vec::new(),
        HashMap::new(),
        0,
        cache,
    )
}

fn test_encode_group(page_size: u32, pages_per_group: usize, fill: u8) -> Vec<u8> {
    let pages: Vec<Option<Vec<u8>>> = (0..pages_per_group)
        .map(|_| Some(vec![fill; page_size as usize]))
        .collect();
    encode_page_group(
        &pages,
        page_size,
        3,
        #[cfg(feature = "zstd")]
        None,
        &[],
        None,
    )
    .unwrap()
}

// =========================================================================
// Demand-Driven Prefetch: btree_groups data structure tests
// =========================================================================

#[test]
fn test_btree_groups_single_btree_multiple_groups() {
    // A B-tree spanning 3 groups: each group should map to all siblings
    let mut m = Manifest {
        page_count: 12,
        page_size: 4096,
        pages_per_group: 4,
        page_group_keys: vec!["a".into(), "b".into(), "c".into()],
        group_pages: vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7], vec![8, 9, 10, 11]],
        btrees: {
            let mut h = HashMap::new();
            h.insert(
                0,
                BTreeManifestEntry {
                    name: "users".into(),
                    obj_type: "table".into(),
                    group_ids: vec![0, 1, 2],
                },
            );
            h
        },
        ..Manifest::empty()
    };
    m.build_page_index();

    // All three groups should map to the same sibling list [0, 1, 2]
    assert_eq!(m.btree_groups.get(&0).unwrap(), &vec![0u64, 1, 2]);
    assert_eq!(m.btree_groups.get(&1).unwrap(), &vec![0u64, 1, 2]);
    assert_eq!(m.btree_groups.get(&2).unwrap(), &vec![0u64, 1, 2]);
}

#[test]
fn test_btree_groups_multiple_btrees_disjoint() {
    // Two B-trees, each with their own groups, no overlap
    let mut m = Manifest {
        page_count: 20,
        page_size: 4096,
        pages_per_group: 4,
        page_group_keys: vec!["a".into(), "b".into(), "c".into(), "d".into()],
        group_pages: vec![
            vec![0, 1, 2, 3],     // B-tree A
            vec![4, 5, 6, 7],     // B-tree A
            vec![8, 9, 10, 11],   // B-tree B
            vec![12, 13, 14, 15], // B-tree B
        ],
        btrees: {
            let mut h = HashMap::new();
            h.insert(
                0,
                BTreeManifestEntry {
                    name: "table_a".into(),
                    obj_type: "table".into(),
                    group_ids: vec![0, 1],
                },
            );
            h.insert(
                8,
                BTreeManifestEntry {
                    name: "table_b".into(),
                    obj_type: "table".into(),
                    group_ids: vec![2, 3],
                },
            );
            h
        },
        ..Manifest::empty()
    };
    m.build_page_index();

    // B-tree A groups map to [0, 1]
    assert_eq!(m.btree_groups.get(&0).unwrap(), &vec![0u64, 1]);
    assert_eq!(m.btree_groups.get(&1).unwrap(), &vec![0u64, 1]);
    // B-tree B groups map to [2, 3]
    assert_eq!(m.btree_groups.get(&2).unwrap(), &vec![2u64, 3]);
    assert_eq!(m.btree_groups.get(&3).unwrap(), &vec![2u64, 3]);
    // No cross-contamination
    assert!(!m.btree_groups[&0].contains(&2));
    assert!(!m.btree_groups[&2].contains(&0));
}

#[test]
fn test_btree_groups_single_group_btree_has_self_only() {
    // A B-tree that fits in a single group: btree_groups maps it to [self]
    let mut m = Manifest {
        page_count: 4,
        page_size: 4096,
        pages_per_group: 4,
        page_group_keys: vec!["a".into()],
        group_pages: vec![vec![0, 1, 2, 3]],
        btrees: {
            let mut h = HashMap::new();
            h.insert(
                0,
                BTreeManifestEntry {
                    name: "small_table".into(),
                    obj_type: "table".into(),
                    group_ids: vec![0],
                },
            );
            h
        },
        ..Manifest::empty()
    };
    m.build_page_index();

    // Single group: sibling list is [0] (only self)
    assert_eq!(m.btree_groups.get(&0).unwrap(), &vec![0u64]);
    // trigger_prefetch skips self, so no siblings to prefetch (correct)
}

#[test]
fn test_btree_groups_empty_when_no_btrees() {
    // Manifest with group_pages but no btrees: btree_groups is empty
    let mut m = Manifest {
        page_count: 8,
        page_size: 4096,
        pages_per_group: 4,
        page_group_keys: vec!["a".into(), "b".into()],
        group_pages: vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7]],
        btrees: HashMap::new(),
        ..Manifest::empty()
    };
    m.build_page_index();

    assert!(m.btree_groups.is_empty());
}

#[test]
fn test_btree_groups_group_not_in_any_btree() {
    // Group 2 exists in group_pages but isn't claimed by any btree
    let mut m = Manifest {
        page_count: 12,
        page_size: 4096,
        pages_per_group: 4,
        page_group_keys: vec!["a".into(), "b".into(), "c".into()],
        group_pages: vec![
            vec![0, 1, 2, 3],
            vec![4, 5, 6, 7],
            vec![8, 9, 10, 11], // orphan group
        ],
        btrees: {
            let mut h = HashMap::new();
            h.insert(
                0,
                BTreeManifestEntry {
                    name: "users".into(),
                    obj_type: "table".into(),
                    group_ids: vec![0, 1], // only groups 0 and 1
                },
            );
            h
        },
        ..Manifest::empty()
    };
    m.build_page_index();

    assert!(m.btree_groups.get(&0).is_some());
    assert!(m.btree_groups.get(&1).is_some());
    assert!(
        m.btree_groups.get(&2).is_none(),
        "orphan group should not be in btree_groups"
    );
}

#[test]
fn test_btree_groups_rebuild_clears_stale() {
    // Calling build_page_index twice with different btrees replaces stale mappings
    let mut m = Manifest {
        page_count: 8,
        page_size: 4096,
        pages_per_group: 4,
        page_group_keys: vec!["a".into(), "b".into()],
        group_pages: vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7]],
        btrees: {
            let mut h = HashMap::new();
            h.insert(
                0,
                BTreeManifestEntry {
                    name: "v1".into(),
                    obj_type: "table".into(),
                    group_ids: vec![0, 1],
                },
            );
            h
        },
        ..Manifest::empty()
    };
    m.build_page_index();
    assert_eq!(m.btree_groups.len(), 2);
    assert_eq!(m.btree_groups[&0], vec![0u64, 1]);

    // Change btrees: group 1 is now a separate B-tree
    m.btrees.clear();
    m.btrees.insert(
        0,
        BTreeManifestEntry {
            name: "v2_a".into(),
            obj_type: "table".into(),
            group_ids: vec![0],
        },
    );
    m.btrees.insert(
        4,
        BTreeManifestEntry {
            name: "v2_b".into(),
            obj_type: "table".into(),
            group_ids: vec![1],
        },
    );
    m.build_page_index();

    // Now group 0 and group 1 are in separate B-trees
    assert_eq!(m.btree_groups[&0], vec![0u64]);
    assert_eq!(m.btree_groups[&1], vec![1u64]);
}

#[test]
fn test_btree_groups_overwrite_when_group_in_multiple_btrees() {
    // Edge case: a group appears in multiple B-tree entries.
    // Last-writer-wins due to HashMap iteration order.
    // This shouldn't happen in practice (B-tree-aware grouping assigns
    // each group to exactly one B-tree), but verify no panic.
    let mut m = Manifest {
        page_count: 8,
        page_size: 4096,
        pages_per_group: 4,
        page_group_keys: vec!["a".into(), "b".into()],
        group_pages: vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7]],
        btrees: {
            let mut h = HashMap::new();
            // Both B-trees claim group 0
            h.insert(
                0,
                BTreeManifestEntry {
                    name: "tree_a".into(),
                    obj_type: "table".into(),
                    group_ids: vec![0],
                },
            );
            h.insert(
                4,
                BTreeManifestEntry {
                    name: "tree_b".into(),
                    obj_type: "index".into(),
                    group_ids: vec![0, 1], // group 0 also here
                },
            );
            h
        },
        ..Manifest::empty()
    };
    m.build_page_index();

    // Group 0 ends up in btree_groups (no panic, last write wins)
    assert!(m.btree_groups.contains_key(&0));
    // Group 1 should be present too
    assert!(m.btree_groups.contains_key(&1));
}

// =========================================================================
// Fraction-based prefetch escalation
// =========================================================================

#[test]
fn test_fraction_prefetch_first_miss_33_percent() {
    // With hops=[0.33, 0.33] and consecutive_misses=1, prefetch 33% of siblings
    let hops = [0.33f32, 0.33];
    let hop_idx = 0usize; // consecutive_misses=1, so hop_idx=0
    let fraction = hops[hop_idx];
    // 10 eligible siblings -> ceil(10 * 0.33) = 4
    let eligible = 10;
    let max_submit = ((eligible as f32) * fraction).ceil() as usize;
    assert_eq!(max_submit, 4);
}

#[test]
fn test_fraction_prefetch_second_miss_66_percent() {
    let hops = [0.33f32, 0.33];
    let hop_idx = 1usize; // consecutive_misses=2
    let fraction = hops[hop_idx];
    let eligible = 10;
    let max_submit = ((eligible as f32) * fraction).ceil() as usize;
    assert_eq!(max_submit, 4);
    // Total after 2 misses: 4 + 4 = 8 out of 10
}

#[test]
fn test_fraction_prefetch_third_miss_all() {
    let hops = [0.33f32, 0.33];
    let hop_idx = 2usize; // consecutive_misses=3, beyond hops.len()
    let fraction = hops.get(hop_idx).copied().unwrap_or(1.0);
    assert_eq!(fraction, 1.0);
    let eligible = 10;
    let max_submit = ((eligible as f32) * fraction).ceil() as usize;
    assert_eq!(max_submit, 10); // all remaining
}

#[test]
fn test_fraction_prefetch_single_sibling() {
    // With only 1 eligible sibling, even 33% rounds up to 1
    let hops = [0.33f32, 0.33];
    let fraction = hops[0];
    let eligible = 1;
    let max_submit = ((eligible as f32) * fraction).ceil() as usize;
    assert_eq!(max_submit, 1);
}

#[test]
fn test_fraction_prefetch_zero_eligible() {
    // All siblings already fetching/present -> 0 eligible -> 0 to submit
    let hops = [0.33f32, 0.33];
    let fraction = hops[0];
    let eligible = 0;
    let max_submit = ((eligible as f32) * fraction).ceil() as usize;
    assert_eq!(max_submit, 0);
}

#[test]
fn test_fraction_prefetch_empty_hops_fetches_all() {
    // Empty hops vec -> fraction = 1.0 -> fetch all on first miss
    let hops: Vec<f32> = vec![];
    let hop_idx = 0usize;
    let fraction = if hop_idx < hops.len() {
        hops[hop_idx]
    } else {
        1.0
    };
    assert_eq!(fraction, 1.0);
    let eligible = 10;
    let max_submit = ((eligible as f32) * fraction).ceil() as usize;
    assert_eq!(max_submit, 10);
}

#[test]
fn test_prefetch_dedup_claim_prevents_double_download() {
    // Simulates trigger_prefetch deduplication: claiming before submitting
    // ensures at most one download per group
    let dir = TempDir::new().unwrap();
    let cache = Arc::new(DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap());

    // First call claims group 1
    assert!(cache.try_claim_group(1));
    assert_eq!(cache.group_state(1), GroupState::Fetching);

    // Second call (from another trigger_prefetch) fails to claim
    assert!(!cache.try_claim_group(1));

    // After first finishes, marks Present
    cache.mark_group_present(1);

    // Third call (from yet another trigger_prefetch) also can't claim
    assert!(!cache.try_claim_group(1));
}

#[test]
fn test_prefetch_claim_reset_on_failure() {
    // If pool.submit fails, state must be reset to None so the group
    // can be retried by another miss
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap();

    assert!(cache.try_claim_group(1));
    assert_eq!(cache.group_state(1), GroupState::Fetching);

    // Simulate submit failure: reset to None
    {
        let states = cache.group_states.lock();
        if let Some(s) = states.get(1) {
            s.store(GroupState::None as u8, Ordering::Release);
        }
    }
    cache.group_condvar.notify_all();

    // Now another path can claim it
    assert!(cache.try_claim_group(1));
    assert_eq!(cache.group_state(1), GroupState::Fetching);
}

#[test]
fn test_read_path_range_get_before_prefetch() {
    // Verify the logical ordering: range GET should complete before
    // background prefetch is submitted. We test this by checking that
    // after a cache miss, the page is served immediately while the
    // group state transitions happen after.
    //
    // This is a structural test of the invariant, not an integration test.
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap();

    // Before range GET: group state should be None
    assert_eq!(cache.group_state(0), GroupState::None);

    // After range GET writes sub-chunk to cache, page is present
    // but group state is still None (prefetch not yet submitted)
    let page_data = vec![42u8; 64];
    cache.write_pages_scattered(&[0], &page_data, 0, 0).unwrap();
    assert!(cache.is_present(0));
    assert_eq!(
        cache.group_state(0),
        GroupState::None,
        "group state should remain None until prefetch is submitted"
    );

    assert_eq!(
        cache.group_state(0),
        GroupState::None,
        "seekable range demand must not also claim current group for full prefetch"
    );
}

#[test]
fn optional_prefetch_queue_full_is_nonblocking_and_does_not_claim_rejected_group() {
    let dir = TempDir::new().unwrap();
    let cache = Arc::new(DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap());
    let backend = Arc::new(DeterministicPressureBackend {
        base_sleep_ms: 200,
        ..Default::default()
    });
    let pool = test_pool(Arc::clone(&cache), backend.clone(), 1, 2, 1);

    assert_eq!(
        submit_test_job(&pool, cache.as_ref(), 0),
        PrefetchSubmitOutcome::Accepted
    );
    while backend.started.load(Ordering::Acquire) == 0 {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        submit_test_job(&pool, cache.as_ref(), 1),
        PrefetchSubmitOutcome::Accepted
    );

    let start = Instant::now();
    let outcome = submit_test_job(&pool, cache.as_ref(), 2);
    assert_eq!(outcome, PrefetchSubmitOutcome::Full);
    assert!(
        start.elapsed() < Duration::from_millis(50),
        "queue pressure must be reported without blocking foreground callers"
    );
    assert_eq!(cache.group_state(2), GroupState::None);
    assert_eq!(
        cache.group_state(1),
        GroupState::None,
        "queued optional work is not Fetching"
    );

    pool.wait_idle();
    let stats = pool.stats_json();
    assert_eq!(stats["full"], 1);
    assert_eq!(stats["accepted"], 2);
}

#[test]
fn optional_prefetch_permit_unavailable_never_sets_fetching() {
    let dir = TempDir::new().unwrap();
    let cache = Arc::new(DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap());
    let backend = Arc::new(DeterministicPressureBackend::default());
    let pool = test_pool(Arc::clone(&cache), backend, 1, 1, 1);

    assert_eq!(
        submit_test_job(&pool, cache.as_ref(), 0),
        PrefetchSubmitOutcome::Accepted
    );
    pool.wait_idle();

    assert_eq!(cache.group_state(0), GroupState::None);
    assert_eq!(pool.stats_json()["permit_unavailable"], 1);
}

#[test]
fn optional_prefetch_workers_drop_busy_permit_without_claiming_or_hanging() {
    let dir = TempDir::new().unwrap();
    let cache = Arc::new(DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap());
    let backend = Arc::new(BlockingPayloadBackend {
        release: AtomicBool::new(false),
        started: AtomicU64::new(0),
        payload: test_encode_group(64, 4, 9),
    });
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pool = PrefetchPool::new(
        2,
        backend.clone(),
        rt.handle().clone(),
        Arc::clone(&cache),
        4,
        Arc::new(AtomicU64::new(8)),
        #[cfg(feature = "zstd")]
        None,
        None,
        Arc::new(ArcSwap::from_pointee(Manifest::empty())),
        Arc::new(parking_lot::RwLock::new(())),
        Arc::new(AtomicU64::new(0)),
        4,
        RemoteIoBudget::new(2, 1),
    );

    for gid in 0..2 {
        assert_eq!(
            pool.submit_optional(
                Some("scan".to_string()),
                gid,
                format!("g{gid}"),
                Vec::new(),
                64,
                0,
                vec![gid * 4, gid * 4 + 1, gid * 4 + 2, gid * 4 + 3],
                HashMap::new(),
                0,
                cache.as_ref(),
            ),
            PrefetchSubmitOutcome::Accepted
        );
    }

    while backend.started.load(Ordering::Acquire) == 0 {
        std::thread::sleep(Duration::from_millis(1));
    }
    std::thread::sleep(Duration::from_millis(25));

    assert_eq!(backend.started.load(Ordering::Acquire), 1);
    assert_eq!(cache.group_state(1), GroupState::None);
    assert_eq!(pool.stats_json()["permit_unavailable"], 1);

    backend.release.store(true, Ordering::Release);
    pool.wait_idle();

    assert_eq!(backend.started.load(Ordering::Acquire), 1);
    assert_eq!(pool.stats_json()["completed"], 1);
    assert_eq!(pool.stats_json()["permit_unavailable"], 1);
    assert_eq!(cache.group_state(0), GroupState::Present);
    assert_eq!(cache.group_state(1), GroupState::None);
    assert_eq!(pool.stats_json()["in_flight"], 0);
}

#[test]
fn zero_worker_prefetch_pool_rejects_without_hanging() {
    let dir = TempDir::new().unwrap();
    let cache = Arc::new(DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap());
    let backend = Arc::new(DeterministicPressureBackend::default());
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pool = PrefetchPool::new(
        0,
        backend,
        rt.handle().clone(),
        Arc::clone(&cache),
        4,
        Arc::new(AtomicU64::new(0)),
        #[cfg(feature = "zstd")]
        None,
        None,
        Arc::new(ArcSwap::from_pointee(Manifest::empty())),
        Arc::new(parking_lot::RwLock::new(())),
        Arc::new(AtomicU64::new(0)),
        1,
        RemoteIoBudget::new(1, 0),
    );

    assert_eq!(
        submit_test_job(&pool, cache.as_ref(), 0),
        PrefetchSubmitOutcome::Closed
    );
    pool.wait_idle();
    assert_eq!(pool.stats_json()["in_flight"], 0);
}

#[test]
fn cancelled_active_prefetch_does_not_install_late_bytes() {
    let dir = TempDir::new().unwrap();
    let cache = Arc::new(DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap());
    let payload = test_encode_group(64, 4, 7);
    let backend = Arc::new(BlockingPayloadBackend {
        release: AtomicBool::new(false),
        started: AtomicU64::new(0),
        payload,
    });
    let pool = test_pool(Arc::clone(&cache), backend.clone(), 2, 1, 0);

    let outcome = pool.submit_optional(
        Some("scan".to_string()),
        0,
        "g0".to_string(),
        Vec::new(),
        64,
        0,
        vec![0, 1, 2, 3],
        HashMap::new(),
        0,
        cache.as_ref(),
    );
    assert_eq!(outcome, PrefetchSubmitOutcome::Accepted);
    while backend.started.load(Ordering::Acquire) == 0 || !pool.is_optional_fetching(0) {
        std::thread::sleep(Duration::from_millis(1));
    }

    assert!(pool.cancel_optional_fetching(0));
    assert!(cache.unclaim_if_fetching(0));
    backend.release.store(true, Ordering::Release);
    pool.wait_idle();

    assert!(!cache.is_present(0));
    assert_eq!(cache.group_state(0), GroupState::None);
    assert_eq!(pool.stats_json()["cancelled"], 1);
}

#[test]
fn foreground_permit_survives_exhausted_prefetch_capacity() {
    let budget = RemoteIoBudget::new(2, 1);
    let _prefetch = budget
        .try_acquire_prefetch()
        .expect("one spare prefetch permit");
    let start = Instant::now();
    let _foreground = budget.acquire_foreground();
    assert!(
        start.elapsed() < Duration::from_millis(50),
        "reserved foreground permit must remain available"
    );
}

#[test]
fn legacy_claim_before_queue_rejection_would_make_foreground_wait() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap();
    let (tx, _rx) = flume::bounded::<u64>(0);

    assert!(cache.try_claim_group(0));
    assert!(
        tx.try_send(0).is_err(),
        "legacy full queue rejects after claim"
    );

    let start = Instant::now();
    let observed = cache.wait_for_group(0);
    assert_eq!(observed, GroupState::Fetching);
    assert!(
        start.elapsed() >= Duration::from_millis(900),
        "legacy claim-before-admission makes foreground wait for the lost Fetching claim"
    );
    cache.unclaim_if_fetching(0);
}

#[test]
fn legacy_blocking_submit_would_park_foreground_when_queue_full() {
    let (tx, _rx) = flume::bounded::<u64>(1);
    tx.send(0).unwrap();

    let start = Instant::now();
    let err = tx
        .send_timeout(1, Duration::from_millis(150))
        .expect_err("legacy blocking send would wait on a full optional queue");
    assert!(matches!(err, flume::SendTimeoutError::Timeout(_)));
    assert!(
        start.elapsed() >= Duration::from_millis(125),
        "old blocking optional admission parks the caller under queue pressure"
    );
}
