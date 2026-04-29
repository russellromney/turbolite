use super::*;
use crate::tiered::*;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tempfile::TempDir;

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
    let hops = vec![0.33f32, 0.33];
    let hop_idx = 0usize; // consecutive_misses=1, so hop_idx=0
    let fraction = hops[hop_idx];
    // 10 eligible siblings -> ceil(10 * 0.33) = 4
    let eligible = 10;
    let max_submit = ((eligible as f32) * fraction).ceil() as usize;
    assert_eq!(max_submit, 4);
}

#[test]
fn test_fraction_prefetch_second_miss_66_percent() {
    let hops = vec![0.33f32, 0.33];
    let hop_idx = 1usize; // consecutive_misses=2
    let fraction = hops[hop_idx];
    let eligible = 10;
    let max_submit = ((eligible as f32) * fraction).ceil() as usize;
    assert_eq!(max_submit, 4);
    // Total after 2 misses: 4 + 4 = 8 out of 10
}

#[test]
fn test_fraction_prefetch_third_miss_all() {
    let hops = vec![0.33f32, 0.33];
    let hop_idx = 2usize; // consecutive_misses=3, beyond hops.len()
    let fraction = if hop_idx < hops.len() {
        hops[hop_idx]
    } else {
        1.0
    };
    assert_eq!(fraction, 1.0);
    let eligible = 10;
    let max_submit = ((eligible as f32) * fraction).ceil() as usize;
    assert_eq!(max_submit, 10); // all remaining
}

#[test]
fn test_fraction_prefetch_single_sibling() {
    // With only 1 eligible sibling, even 33% rounds up to 1
    let hops = vec![0.33f32, 0.33];
    let fraction = hops[0];
    let eligible = 1;
    let max_submit = ((eligible as f32) * fraction).ceil() as usize;
    assert_eq!(max_submit, 1);
}

#[test]
fn test_fraction_prefetch_zero_eligible() {
    // All siblings already fetching/present -> 0 eligible -> 0 to submit
    let hops = vec![0.33f32, 0.33];
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

    // Now the read path would claim and submit to pool
    assert!(cache.try_claim_group(0));
    assert_eq!(cache.group_state(0), GroupState::Fetching);
}
