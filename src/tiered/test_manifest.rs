use super::*;
use crate::tiered::*;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

// =========================================================================
// Manifest
// =========================================================================

#[test]
fn test_manifest_total_groups() {
    let m = Manifest {
        version: 1,
        page_count: 10000,
        page_size: 4096,
        pages_per_group: 2048,
        ..Manifest::empty()
    };
    assert_eq!(m.total_groups(), 5); // ceil(10000/2048)

    let m2 = Manifest {
        page_count: 2048,
        pages_per_group: 2048,
        ..Manifest::empty()
    };
    assert_eq!(m2.total_groups(), 1); // exact fit

    let m3 = Manifest {
        page_count: 2049,
        pages_per_group: 2048,
        ..Manifest::empty()
    };
    assert_eq!(m3.total_groups(), 2); // one extra page → 2 groups
}

#[test]
fn test_manifest_total_groups_edge_cases() {
    assert_eq!(Manifest::empty().total_groups(), 0);
    let m = Manifest {
        page_count: 1,
        pages_per_group: 2048,
        ..Manifest::empty()
    };
    assert_eq!(m.total_groups(), 1); // 1 page → 1 group

    let m2 = Manifest {
        page_count: 0,
        pages_per_group: 2048,
        ..Manifest::empty()
    };
    assert_eq!(m2.total_groups(), 0);
}

#[test]
fn test_manifest_empty() {
    let m = Manifest::empty();
    assert_eq!(m.version, 0);
    assert_eq!(m.page_count, 0);
    assert_eq!(m.page_size, 0);
    assert_eq!(m.pages_per_group, 0);
    assert!(m.page_group_keys.is_empty());
}

#[test]
fn test_manifest_serde_roundtrip() {
    let m = Manifest {
        version: 42,
        page_count: 10000,
        page_size: 4096,
        pages_per_group: 2048,
        page_group_keys: vec![
            "p/d/0_v1".to_string(),
            "p/d/1_v1".to_string(),
            "p/d/2_v1".to_string(),
        ],
        ..Manifest::empty()
    };
    let json = serde_json::to_string(&m).unwrap();
    let m2: Manifest = serde_json::from_str(&json).unwrap();
    assert_eq!(m.version, m2.version);
    assert_eq!(m.page_count, m2.page_count);
    assert_eq!(m.page_size, m2.page_size);
    assert_eq!(m.pages_per_group, m2.pages_per_group);
    assert_eq!(m.page_group_keys, m2.page_group_keys);
    assert_eq!(m.interior_chunk_keys, m2.interior_chunk_keys);
}

#[test]
fn test_manifest_deserialize_missing_pages_per_group() {
    // Simulate old manifest without pages_per_group
    let json = r#"{"version":1,"page_count":100,"page_size":4096,"page_group_keys":[]}"#;
    let m: Manifest = serde_json::from_str(json).unwrap();
    assert_eq!(m.pages_per_group, DEFAULT_PAGES_PER_GROUP); // default
}

#[test]
fn test_manifest_deserialize_missing_page_group_keys() {
    let json = r#"{"version":1,"page_count":100,"page_size":4096,"pages_per_group":512}"#;
    let m: Manifest = serde_json::from_str(json).unwrap();
    assert!(m.page_group_keys.is_empty()); // default
    assert_eq!(m.pages_per_group, 512);
}

#[test]
fn test_manifest_deserialize_invalid_json() {
    let result: Result<Manifest, _> = serde_json::from_str("not valid json");
    assert!(result.is_err());
}

#[test]
fn test_manifest_serialize_empty() {
    let m = Manifest::empty();
    let json = serde_json::to_string(&m).unwrap();
    assert!(json.contains("\"version\":0"));
    assert!(json.contains("\"page_count\":0"));
}

// =========================================================================
// Seekable manifest serde
// =========================================================================

#[test]
fn test_seekable_manifest_serde_with_frames() {
    let m = Manifest {
        version: 1,
        page_count: 128,
        page_size: 4096,
        pages_per_group: 64,
        page_group_keys: vec!["p/d/0_v1".to_string(), "p/d/1_v1".to_string()],
        frame_tables: vec![
            vec![
                FrameEntry {
                    offset: 0,
                    len: 500,
                },
                FrameEntry {
                    offset: 500,
                    len: 600,
                },
            ],
            vec![
                FrameEntry {
                    offset: 0,
                    len: 450,
                },
                FrameEntry {
                    offset: 450,
                    len: 550,
                },
            ],
        ],
        sub_pages_per_frame: 32,
        ..Manifest::empty()
    };
    let json = serde_json::to_string(&m).unwrap();
    let m2: Manifest = serde_json::from_str(&json).unwrap();
    assert_eq!(m2.sub_pages_per_frame, 32);
    assert_eq!(m2.frame_tables.len(), 2);
    assert_eq!(m2.frame_tables[0].len(), 2);
    assert_eq!(m2.frame_tables[0][0].offset, 0);
    assert_eq!(m2.frame_tables[0][0].len, 500);
}

#[test]
fn test_seekable_manifest_backward_compat() {
    // Old manifest without frame_tables/sub_pages_per_frame
    let json = r#"{"version":1,"page_count":100,"page_size":4096,"pages_per_group":512,"page_group_keys":[]}"#;
    let m: Manifest = serde_json::from_str(json).unwrap();
    assert!(m.frame_tables.is_empty());
    assert_eq!(m.sub_pages_per_frame, 0);
}

// Regression tests

#[test]
fn test_assign_new_pages_to_groups_basic() {
    let mut manifest = Manifest {
        page_count: 10,
        page_size: 4096,
        pages_per_group: 4,
        group_pages: vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7]],
        ..Manifest::empty()
    };
    manifest.build_page_index();

    // Pages 8 and 9 are new (not in page_index)
    let unassigned = vec![8, 9];
    TurboliteHandle::assign_new_pages_to_groups(&mut manifest, &unassigned, 4);

    // Should be added to a group and in page_index
    assert!(manifest.page_location(8).is_some());
    assert!(manifest.page_location(9).is_some());
    // All original pages still mapped correctly
    for p in 0..8 {
        assert!(manifest.page_location(p).is_some());
    }
}

#[test]
fn test_assign_new_pages_fills_last_group_first() {
    let mut manifest = Manifest {
        page_count: 5,
        page_size: 4096,
        pages_per_group: 4,
        group_pages: vec![
            vec![0, 1, 2, 3], // full
            vec![4],          // room for 3 more
        ],
        ..Manifest::empty()
    };
    manifest.build_page_index();

    let unassigned = vec![5, 6];
    TurboliteHandle::assign_new_pages_to_groups(&mut manifest, &unassigned, 4);

    // Pages 5,6 should fill group 1 (had room)
    let loc5 = manifest.page_location(5).unwrap();
    let loc6 = manifest.page_location(6).unwrap();
    assert_eq!(loc5.group_id, 1);
    assert_eq!(loc6.group_id, 1);
    assert_eq!(manifest.group_pages.len(), 2); // no new groups needed
}

#[test]
fn test_assign_new_pages_overflow_to_new_group() {
    let mut manifest = Manifest {
        page_count: 8,
        page_size: 4096,
        pages_per_group: 4,
        group_pages: vec![
            vec![0, 1, 2, 3], // full
            vec![4, 5, 6, 7], // full
        ],
        ..Manifest::empty()
    };
    manifest.build_page_index();

    let unassigned = vec![8, 9, 10, 11, 12];
    TurboliteHandle::assign_new_pages_to_groups(&mut manifest, &unassigned, 4);

    // 5 new pages, both groups full -> need new group(s)
    assert_eq!(manifest.group_pages.len(), 4); // 2 original + 2 new (4 + 1)
    for p in 8..=12 {
        assert!(manifest.page_location(p).is_some());
    }
    // First 4 go to group 2, last 1 to group 3
    assert_eq!(manifest.page_location(8).unwrap().group_id, 2);
    assert_eq!(manifest.page_location(12).unwrap().group_id, 3);
}

#[test]
fn test_assign_new_pages_empty_input() {
    let mut manifest = Manifest {
        page_count: 4,
        page_size: 4096,
        pages_per_group: 4,
        group_pages: vec![vec![0, 1, 2, 3]],
        ..Manifest::empty()
    };
    manifest.build_page_index();
    let original_groups = manifest.group_pages.len();

    TurboliteHandle::assign_new_pages_to_groups(&mut manifest, &[], 4);

    assert_eq!(manifest.group_pages.len(), original_groups);
}

#[test]
fn test_assign_new_pages_no_duplicate_assignments() {
    let mut manifest = Manifest {
        page_count: 4,
        page_size: 4096,
        pages_per_group: 4,
        group_pages: vec![vec![0, 1, 2, 3]],
        ..Manifest::empty()
    };
    manifest.build_page_index();

    let unassigned = vec![4, 5, 6, 7, 8, 9];
    TurboliteHandle::assign_new_pages_to_groups(&mut manifest, &unassigned, 4);

    // Each page should appear exactly once across all groups
    let mut all_pages: Vec<u64> = manifest.group_pages.iter().flatten().copied().collect();
    all_pages.sort_unstable();
    all_pages.dedup();
    let total: usize = manifest.group_pages.iter().map(|g| g.len()).sum();
    assert_eq!(all_pages.len(), total, "duplicate page in group_pages");
}

#[test]
fn test_build_page_index_roundtrip() {
    let mut manifest = Manifest {
        page_count: 10,
        page_size: 4096,
        pages_per_group: 4,
        group_pages: vec![
            vec![5, 0, 3, 8], // non-sequential
            vec![1, 7, 2],
            vec![9, 4, 6],
        ],
        ..Manifest::empty()
    };
    manifest.build_page_index();

    // Every page 0-9 should have a location
    for p in 0..10 {
        let loc = manifest
            .page_location(p)
            .unwrap_or_else(|| panic!("missing page {}", p));
        // Verify the reverse: group_pages[gid][index] == p
        assert_eq!(
            manifest.group_pages[loc.group_id as usize][loc.index as usize],
            p
        );
    }
}

// B-tree-aware page groups tests

#[test]
fn test_total_groups_btree_vs_positional() {
    // Positional: ceil(100/32) = 4
    let m = Manifest {
        page_count: 100,
        pages_per_group: 32,
        ..Manifest::empty()
    };
    assert_eq!(m.total_groups(), 4);

    // B-tree groups: explicit mapping with more groups than positional formula
    let m2 = Manifest {
        page_count: 100,
        pages_per_group: 32,
        group_pages: vec![
            vec![0, 1, 2],
            vec![3, 4, 5],
            vec![6, 7, 8],
            vec![9, 10, 11],
            vec![12, 13, 14, 15, 16, 17, 18, 19], // extra group from B-tree packing
        ],
        ..Manifest::empty()
    };
    // B-tree mapping takes priority: 5 groups, not positional 4
    assert_eq!(m2.total_groups(), 5);
}

// =========================================================================
// More B-tree manifest tests
// =========================================================================

#[test]
fn test_manifest_btree_page_location_non_sequential() {
    // B-tree groups have non-sequential page numbers
    let mut manifest = Manifest {
        page_count: 20,
        page_size: 4096,
        pages_per_group: 8,
        group_pages: vec![
            vec![0, 5, 10, 15],                   // gid=0: scattered pages from B-tree A
            vec![1, 2, 3, 4],                     // gid=1: sequential pages from B-tree B
            vec![6, 7, 8, 9],                     // gid=2: sequential from B-tree C
            vec![11, 12, 13, 14, 16, 17, 18, 19], // gid=3: remaining pages
        ],
        ..Manifest::empty()
    };
    manifest.build_page_index();

    // Verify page 10 is in gid=0 at index=2
    let loc = manifest.page_location(10).unwrap();
    assert_eq!(loc.group_id, 0);
    assert_eq!(loc.index, 2);

    // Page 3 is in gid=1 at index=2
    let loc = manifest.page_location(3).unwrap();
    assert_eq!(loc.group_id, 1);
    assert_eq!(loc.index, 2);

    // Page 18 is in gid=3 at index=6
    let loc = manifest.page_location(18).unwrap();
    assert_eq!(loc.group_id, 3);
    assert_eq!(loc.index, 6);
}

#[test]
fn test_manifest_serde_roundtrip_btree_groups() {
    // group_pages and btrees must survive JSON serialization.
    // page_index is #[serde(skip)] and rebuilt from group_pages.
    let mut m = Manifest {
        version: 5,
        page_count: 20,
        page_size: 4096,
        pages_per_group: 8,
        page_group_keys: vec!["p/d/0_v5".into(), "p/d/1_v5".into(), "p/d/2_v5".into()],
        group_pages: vec![
            vec![0, 5, 10, 15], // B-tree A (scattered)
            vec![1, 2, 3, 4],   // B-tree B (sequential)
            vec![6, 7, 8, 9, 11, 12, 13, 14, 16, 17, 18, 19],
        ],
        btrees: {
            let mut h = HashMap::new();
            h.insert(
                0,
                BTreeManifestEntry {
                    name: "users".into(),
                    obj_type: "table".into(),
                    group_ids: vec![0],
                },
            );
            h.insert(
                5,
                BTreeManifestEntry {
                    name: "idx_users_name".into(),
                    obj_type: "index".into(),
                    group_ids: vec![1],
                },
            );
            h
        },
        ..Manifest::empty()
    };
    m.build_page_index();

    let json = serde_json::to_string(&m).unwrap();
    let mut m2: Manifest = serde_json::from_str(&json).unwrap();

    // group_pages survives
    assert_eq!(m.group_pages, m2.group_pages);
    // btrees survives
    assert_eq!(m.btrees.len(), m2.btrees.len());
    assert_eq!(m.btrees[&0].name, m2.btrees[&0].name);
    assert_eq!(m.btrees[&5].group_ids, m2.btrees[&5].group_ids);
    // page_index is NOT serialized (skip)
    assert!(
        m2.page_index.is_empty(),
        "page_index should be empty after deserialize"
    );
    // Rebuild and verify
    m2.build_page_index();
    assert_eq!(m2.page_location(10).unwrap().group_id, 0);
    assert_eq!(m2.page_location(10).unwrap().index, 2);
    assert_eq!(m2.page_location(3).unwrap().group_id, 1);
    assert_eq!(m2.page_location(3).unwrap().index, 2);
    // total_groups uses group_pages, not positional
    assert_eq!(m2.total_groups(), 3);
    // btree_groups rebuilt correctly (group -> sibling group_ids)
    assert_eq!(m2.btree_groups.get(&0).unwrap(), &vec![0u64]);
    assert_eq!(m2.btree_groups.get(&1).unwrap(), &vec![1u64]);
    // group 2 has no btree entry, so no btree_groups mapping
    assert!(m2.btree_groups.get(&2).is_none());
    // Page_to_tree_name reverse index rebuilt from btrees
    // B-tree root=0 ("users") owns group 0 with pages [0, 5, 10, 15]
    assert_eq!(
        m2.page_to_tree_name.get(&0).map(|s| s.as_str()),
        Some("users")
    );
    assert_eq!(
        m2.page_to_tree_name.get(&5).map(|s| s.as_str()),
        Some("users")
    );
    assert_eq!(
        m2.page_to_tree_name.get(&10).map(|s| s.as_str()),
        Some("users")
    );
    assert_eq!(
        m2.page_to_tree_name.get(&15).map(|s| s.as_str()),
        Some("users")
    );
    // B-tree root=5 ("idx_users_name") owns group 1 with pages [1, 2, 3, 4]
    assert_eq!(
        m2.page_to_tree_name.get(&1).map(|s| s.as_str()),
        Some("idx_users_name")
    );
    assert_eq!(
        m2.page_to_tree_name.get(&4).map(|s| s.as_str()),
        Some("idx_users_name")
    );
    // Pages in group 2 (no btree entry) should NOT be in page_to_tree_name
    assert!(m2.page_to_tree_name.get(&6).is_none());
    assert!(m2.page_to_tree_name.get(&19).is_none());
    // page_to_tree_name is skip-serialized (rebuilt, not persisted)
    assert!(!m2.page_to_tree_name.is_empty()); // rebuilt by build_page_index
                                               // tree_name_to_groups also rebuilt
    assert_eq!(m2.tree_name_to_groups.get("users").unwrap(), &vec![0u64]);
    assert_eq!(
        m2.tree_name_to_groups.get("idx_users_name").unwrap(),
        &vec![1u64]
    );
}

// =========================================================================
// B-tree manifest tests continued
// =========================================================================

#[test]
fn test_manifest_btree_group_partial_last_group() {
    // Last B-tree group may have fewer pages than ppg.
    // total_groups, page_location, frame calculations all must respect actual size.
    let mut manifest = Manifest {
        page_count: 10,
        page_size: 4096,
        pages_per_group: 4,
        group_pages: vec![
            vec![0, 1, 2, 3], // full group
            vec![4, 5, 6, 7], // full group
            vec![8, 9],       // partial group (2 of 4)
        ],
        ..Manifest::empty()
    };
    manifest.build_page_index();

    assert_eq!(manifest.total_groups(), 3);

    // Page 9 is in the partial group at index 1
    let loc = manifest.page_location(9).unwrap();
    assert_eq!(loc.group_id, 2);
    assert_eq!(loc.index, 1);

    // Group 2 has 2 pages, not 4
    assert_eq!(manifest.group_pages[2].len(), 2);
}

#[test]
fn test_manifest_deserialize_btree_fields_default_when_missing() {
    // Old manifests without group_pages/btrees should deserialize cleanly
    let json = r#"{"version":1,"page_count":100,"page_size":4096,"pages_per_group":32,"page_group_keys":["p/d/0_v1","p/d/1_v1","p/d/2_v1","p/d/3_v1"]}"#;
    let m: Manifest = serde_json::from_str(json).unwrap();

    // B-tree fields default to empty
    assert!(m.group_pages.is_empty());
    assert!(m.btrees.is_empty());
    assert!(m.page_index.is_empty());

    // Falls back to positional total_groups
    assert_eq!(m.total_groups(), 4); // ceil(100/32)
}

// =========================================================================
// GroupingStrategy dispatch: page_location, group_page_nums, group_size,
//                           prefetch_neighbors
// =========================================================================

#[test]
fn test_positional_page_location() {
    let m = Manifest {
        page_count: 100,
        pages_per_group: 32,
        strategy: GroupingStrategy::Positional,
        ..Manifest::empty()
    };
    // page 0 -> gid=0, idx=0
    let loc = m.page_location(0).unwrap();
    assert_eq!(loc.group_id, 0);
    assert_eq!(loc.index, 0);
    // page 31 -> gid=0, idx=31
    let loc = m.page_location(31).unwrap();
    assert_eq!(loc.group_id, 0);
    assert_eq!(loc.index, 31);
    // page 32 -> gid=1, idx=0
    let loc = m.page_location(32).unwrap();
    assert_eq!(loc.group_id, 1);
    assert_eq!(loc.index, 0);
    // page 99 -> gid=3, idx=3
    let loc = m.page_location(99).unwrap();
    assert_eq!(loc.group_id, 3);
    assert_eq!(loc.index, 3);
    // page 100 -> None (out of bounds)
    assert!(m.page_location(100).is_none());
}

#[test]
fn test_btreeaware_page_location() {
    let mut m = Manifest {
        page_count: 10,
        pages_per_group: 4,
        group_pages: vec![
            vec![0, 5, 10],   // gid=0: scattered
            vec![1, 2, 3, 4], // gid=1: sequential
        ],
        ..Manifest::empty()
    };
    m.build_page_index();
    assert_eq!(m.strategy, GroupingStrategy::BTreeAware);

    let loc = m.page_location(5).unwrap();
    assert_eq!(loc.group_id, 0);
    assert_eq!(loc.index, 1);

    let loc = m.page_location(3).unwrap();
    assert_eq!(loc.group_id, 1);
    assert_eq!(loc.index, 2);

    // Page 6 not assigned
    assert!(m.page_location(6).is_none());
}

#[test]
fn test_positional_group_page_nums() {
    let m = Manifest {
        page_count: 10,
        pages_per_group: 4,
        strategy: GroupingStrategy::Positional,
        ..Manifest::empty()
    };
    // gid 0: [0,1,2,3]
    assert_eq!(m.group_page_nums(0).as_ref(), &[0, 1, 2, 3]);
    // gid 1: [4,5,6,7]
    assert_eq!(m.group_page_nums(1).as_ref(), &[4, 5, 6, 7]);
    // gid 2: [8,9] (partial last group)
    assert_eq!(m.group_page_nums(2).as_ref(), &[8, 9]);
}

#[test]
fn test_btreeaware_group_page_nums_borrows() {
    let mut m = Manifest {
        page_count: 10,
        pages_per_group: 4,
        group_pages: vec![vec![0, 5, 10], vec![1, 2, 3, 4]],
        ..Manifest::empty()
    };
    m.build_page_index();

    let gp = m.group_page_nums(0);
    // BTreeAware should return Cow::Borrowed (zero-allocation)
    assert!(
        matches!(gp, std::borrow::Cow::Borrowed(_)),
        "BTreeAware should borrow, not allocate"
    );
    assert_eq!(gp.as_ref(), &[0, 5, 10]);
}

#[test]
fn test_positional_group_page_nums_owned() {
    let m = Manifest {
        page_count: 8,
        pages_per_group: 4,
        strategy: GroupingStrategy::Positional,
        ..Manifest::empty()
    };

    let gp = m.group_page_nums(0);
    // Positional must allocate (Cow::Owned)
    assert!(
        matches!(gp, std::borrow::Cow::Owned(_)),
        "Positional should allocate Cow::Owned"
    );
    assert_eq!(gp.as_ref(), &[0, 1, 2, 3]);
}

#[test]
fn test_positional_group_size() {
    let m = Manifest {
        page_count: 10,
        pages_per_group: 4,
        strategy: GroupingStrategy::Positional,
        ..Manifest::empty()
    };
    assert_eq!(m.group_size(0), 4);
    assert_eq!(m.group_size(1), 4);
    assert_eq!(m.group_size(2), 2); // partial
}

#[test]
fn test_btreeaware_group_size() {
    let mut m = Manifest {
        page_count: 10,
        pages_per_group: 4,
        group_pages: vec![vec![0, 5, 10], vec![1, 2, 3, 4]],
        ..Manifest::empty()
    };
    m.build_page_index();
    assert_eq!(m.group_size(0), 3);
    assert_eq!(m.group_size(1), 4);
}

#[test]
fn test_prefetch_siblings() {
    let mut m = Manifest {
        page_count: 20,
        pages_per_group: 8,
        group_pages: vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7], vec![8, 9, 10, 11]],
        btrees: HashMap::from([(
            0,
            BTreeManifestEntry {
                name: "test_table".into(),
                obj_type: "table".into(),
                group_ids: vec![0, 1],
            },
        )]),
        ..Manifest::empty()
    };
    m.build_page_index();

    // gid=0 is in a btree with siblings [0, 1]
    assert_eq!(m.prefetch_siblings(0), vec![0, 1]);

    // gid=2 is NOT in any btree
    assert!(
        m.prefetch_siblings(2).is_empty(),
        "non-btree group should have empty siblings"
    );
}

#[test]
fn test_detect_and_normalize_strategy() {
    // Manifest with group_pages but strategy=Positional (simulates old manifest)
    let mut m = Manifest {
        page_count: 10,
        pages_per_group: 4,
        strategy: GroupingStrategy::Positional,
        group_pages: vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7]],
        ..Manifest::empty()
    };
    m.detect_and_normalize_strategy();
    assert_eq!(m.strategy, GroupingStrategy::BTreeAware);
    // page_index should be built
    assert!(m.page_location(3).is_some());
}

#[test]
fn test_build_page_index_auto_detects_btreeaware() {
    // build_page_index should also auto-detect BTreeAware from group_pages
    let mut m = Manifest {
        page_count: 10,
        pages_per_group: 4,
        group_pages: vec![vec![0, 1], vec![2, 3]],
        ..Manifest::empty() // strategy defaults to Positional
    };
    assert_eq!(m.strategy, GroupingStrategy::Positional);
    m.build_page_index();
    assert_eq!(
        m.strategy,
        GroupingStrategy::BTreeAware,
        "build_page_index should auto-detect BTreeAware from non-empty group_pages"
    );
    assert!(m.page_location(0).is_some());
}

#[test]
fn test_positional_empty_manifest() {
    // Edge case: empty Positional manifest
    let m = Manifest::empty();
    assert_eq!(m.strategy, GroupingStrategy::Positional);
    assert_eq!(m.total_groups(), 0);
    assert!(m.page_location(0).is_none());
}

#[test]
fn test_positional_page_location_ppg_zero() {
    // Edge case: ppg=0 should not panic
    let m = Manifest {
        page_count: 10,
        pages_per_group: 0,
        strategy: GroupingStrategy::Positional,
        ..Manifest::empty()
    };
    assert!(m.page_location(0).is_none());
}

#[test]
fn test_shared_dirty_groups_arc_semantics() {
    // Verify that Arc<Mutex<HashSet<u64>>> drains correctly (simulates
    // handle writing + flush_to_storage draining the same shared state).
    let shared = Arc::new(Mutex::new(HashSet::new()));

    // Simulate handle writing during local checkpoint
    {
        let mut pending = shared.lock().unwrap();
        pending.insert(0);
        pending.insert(3);
        pending.insert(7);
    }

    // Simulate flush_to_storage draining
    let drained: HashSet<u64> = {
        let mut pending = shared.lock().unwrap();
        std::mem::take(&mut *pending)
    };

    assert_eq!(drained.len(), 3);
    assert!(drained.contains(&0));
    assert!(drained.contains(&3));
    assert!(drained.contains(&7));
    assert!(shared.lock().unwrap().is_empty());
}

#[test]
fn test_shared_manifest_arc_write_visible_to_readers() {
    // Verify that manifest updates from the handle are visible to flush_to_storage
    let shared = Arc::new(RwLock::new(Manifest::empty()));
    let reader = Arc::clone(&shared);

    // Simulate handle updating manifest during write
    {
        let mut m = shared.write();
        m.page_count = 42;
        m.page_size = 65536;
        m.version = 5;
    }

    // Simulate flush reading the updated manifest
    {
        let m = reader.read();
        assert_eq!(m.page_count, 42);
        assert_eq!(m.page_size, 65536);
        assert_eq!(m.version, 5);
    }
}

// =========================================================================
// Dirty_frames_for_group tests
// =========================================================================

use crate::tiered::manifest::{dirty_frames_for_group, SubframeOverride};

#[test]
fn test_dirty_frames_single_page_correct_frame() {
    let group_pages = vec![10, 11, 12, 13, 14, 15, 16, 17];
    let ft = vec![
        FrameEntry {
            offset: 0,
            len: 100,
        },
        FrameEntry {
            offset: 100,
            len: 100,
        },
    ];
    // Page 10 is at position 0, frame 0 (sub_ppf=4, 0/4=0)
    let result = dirty_frames_for_group(&[10], &group_pages, &ft, 4);
    assert_eq!(result, vec![0]);
}

#[test]
fn test_dirty_frames_multiple_in_same_frame() {
    let group_pages = vec![10, 11, 12, 13, 14, 15, 16, 17];
    let ft = vec![
        FrameEntry {
            offset: 0,
            len: 100,
        },
        FrameEntry {
            offset: 100,
            len: 100,
        },
    ];
    // Pages 10, 11, 12 all in frame 0
    let result = dirty_frames_for_group(&[10, 11, 12], &group_pages, &ft, 4);
    assert_eq!(result, vec![0]);
}

#[test]
fn test_dirty_frames_across_frames() {
    let group_pages = vec![10, 11, 12, 13, 14, 15, 16, 17];
    let ft = vec![
        FrameEntry {
            offset: 0,
            len: 100,
        },
        FrameEntry {
            offset: 100,
            len: 100,
        },
    ];
    // Page 10 in frame 0, page 14 in frame 1
    let result = dirty_frames_for_group(&[10, 14], &group_pages, &ft, 4);
    assert_eq!(result, vec![0, 1]);
}

#[test]
fn test_dirty_frames_no_dirty_pages() {
    let group_pages = vec![10, 11, 12, 13];
    let ft = vec![FrameEntry {
        offset: 0,
        len: 100,
    }];
    let result = dirty_frames_for_group(&[], &group_pages, &ft, 4);
    assert!(result.is_empty());
}

#[test]
fn test_dirty_frames_page_not_in_group() {
    let group_pages = vec![10, 11, 12, 13];
    let ft = vec![FrameEntry {
        offset: 0,
        len: 100,
    }];
    // Page 99 is not in the group
    let result = dirty_frames_for_group(&[99], &group_pages, &ft, 4);
    assert!(result.is_empty());
}

#[test]
fn test_dirty_frames_zero_sub_ppf() {
    let group_pages = vec![10, 11, 12, 13];
    let ft = vec![FrameEntry {
        offset: 0,
        len: 100,
    }];
    let result = dirty_frames_for_group(&[10], &group_pages, &ft, 0);
    assert!(result.is_empty());
}

#[test]
fn test_dirty_frames_empty_frame_table() {
    let group_pages = vec![10, 11, 12, 13];
    let ft: Vec<FrameEntry> = Vec::new();
    let result = dirty_frames_for_group(&[10], &group_pages, &ft, 4);
    assert!(result.is_empty());
}

#[test]
fn test_dirty_frames_beyond_frame_table() {
    // Regression: pages assigned to a group beyond the base frame table bounds
    // were silently dropped, causing data loss on S3 restore.
    // Base was created with 3 pages (1 frame), then 4 more pages added.
    let group_pages = vec![0, 1, 2, 3, 4, 5, 6];
    let ft = vec![
        // Only 1 frame entry from the original base (covers pages 0-4)
        FrameEntry {
            offset: 0,
            len: 100,
        },
    ];
    // Pages 5 and 6 are at positions 5-6, frame_idx = 5/5 = 1 (beyond ft.len())
    // Page 3 is at position 3, frame_idx = 3/5 = 0 (within ft.len())
    let result = dirty_frames_for_group(&[3, 5, 6], &group_pages, &ft, 5);
    // Must include frame 1 even though it's beyond the base frame table
    assert_eq!(result, vec![0, 1]);
}

#[test]
fn test_dirty_frames_all_beyond_frame_table() {
    // All dirty pages are in frames beyond the frame table
    let group_pages = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    let ft = vec![FrameEntry {
        offset: 0,
        len: 100,
    }];
    // Pages 5-9 at positions 5-9, frame_idx = 1 and 2 (both beyond ft)
    let result = dirty_frames_for_group(&[5, 6, 7, 8, 9], &group_pages, &ft, 5);
    assert_eq!(result, vec![1]);
}

// =========================================================================
// SubframeOverride serde tests
// =========================================================================

#[test]
fn test_subframe_override_json_roundtrip() {
    let ovr = SubframeOverride {
        key: "p/d/0_f1_v5".to_string(),
        entry: FrameEntry {
            offset: 0,
            len: 1234,
        },
    };
    let json = serde_json::to_string(&ovr).expect("json serialize");
    let ovr2: SubframeOverride = serde_json::from_str(&json).expect("json deserialize");
    assert_eq!(ovr, ovr2);
}

#[test]
fn test_subframe_override_msgpack_roundtrip() {
    let ovr = SubframeOverride {
        key: "p/d/3_f2_v10".to_string(),
        entry: FrameEntry {
            offset: 0,
            len: 5678,
        },
    };
    let bytes = rmp_serde::to_vec(&ovr).expect("msgpack serialize");
    let ovr2: SubframeOverride = rmp_serde::from_slice(&bytes).expect("msgpack deserialize");
    assert_eq!(ovr, ovr2);
}

#[test]
fn test_manifest_backward_compat_no_overrides() {
    // Old manifest JSON without subframe_overrides should deserialize to empty vec
    let json = r#"{"version":1,"page_count":100,"page_size":4096,"pages_per_group":32,"page_group_keys":["p/d/0_v1"]}"#;
    let m: Manifest = serde_json::from_str(json).expect("deserialize");
    assert!(m.subframe_overrides.is_empty());
}

#[test]
fn test_manifest_with_overrides_serde_roundtrip() {
    let mut overrides = HashMap::new();
    overrides.insert(
        1,
        SubframeOverride {
            key: "p/d/0_f1_v5".to_string(),
            entry: FrameEntry {
                offset: 0,
                len: 1000,
            },
        },
    );
    let m = Manifest {
        version: 5,
        page_count: 100,
        page_size: 4096,
        pages_per_group: 32,
        page_group_keys: vec!["p/d/0_v4".to_string()],
        subframe_overrides: vec![overrides],
        ..Manifest::empty()
    };
    let json = serde_json::to_string(&m).expect("serialize");
    let m2: Manifest = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(m2.subframe_overrides.len(), 1);
    assert_eq!(m2.subframe_overrides[0].len(), 1);
    let ovr = &m2.subframe_overrides[0][&1];
    assert_eq!(ovr.key, "p/d/0_f1_v5");
    assert_eq!(ovr.entry.len, 1000);
}

#[test]
fn test_vacuum_clears_overrides() {
    let mut overrides = HashMap::new();
    overrides.insert(
        0,
        SubframeOverride {
            key: "p/d/0_f0_v3".to_string(),
            entry: FrameEntry {
                offset: 0,
                len: 500,
            },
        },
    );
    let mut m = Manifest {
        version: 3,
        page_count: 100,
        page_size: 4096,
        pages_per_group: 32,
        subframe_overrides: vec![overrides],
        ..Manifest::empty()
    };
    // Simulate VACUUM clearing overrides (collecting keys for GC)
    let mut gc_keys = Vec::new();
    for overrides in &m.subframe_overrides {
        for ovr in overrides.values() {
            gc_keys.push(ovr.key.clone());
        }
    }
    m.subframe_overrides.clear();
    assert_eq!(gc_keys, vec!["p/d/0_f0_v3"]);
    assert!(m.subframe_overrides.is_empty());
}

#[test]
fn test_normalize_overrides_extends_to_match_groups() {
    let mut m = Manifest {
        version: 1,
        page_count: 100,
        page_size: 4096,
        pages_per_group: 32,
        page_group_keys: vec!["p/d/0_v1".into(), "p/d/1_v1".into(), "p/d/2_v1".into()],
        subframe_overrides: Vec::new(), // empty
        ..Manifest::empty()
    };
    m.normalize_overrides();
    assert_eq!(m.subframe_overrides.len(), 3);
    assert!(m.subframe_overrides[0].is_empty());
    assert!(m.subframe_overrides[1].is_empty());
    assert!(m.subframe_overrides[2].is_empty());
}

#[test]
fn test_full_manifest_with_overrides_msgpack_roundtrip() {
    // Test that a full Manifest with subframe_overrides roundtrips through msgpack.
    // This catches issues with HashMap<usize, SubframeOverride> serialization.
    let mut overrides_g0 = HashMap::new();
    overrides_g0.insert(
        2usize,
        manifest::SubframeOverride {
            key: "p/d/0_f2_v3".to_string(),
            entry: FrameEntry {
                offset: 0,
                len: 500,
            },
        },
    );
    overrides_g0.insert(
        5usize,
        manifest::SubframeOverride {
            key: "p/d/0_f5_v3".to_string(),
            entry: FrameEntry {
                offset: 0,
                len: 800,
            },
        },
    );

    let m = Manifest {
        version: 3,
        page_count: 256,
        page_size: 4096,
        pages_per_group: 128,
        sub_pages_per_frame: 8,
        page_group_keys: vec!["p/d/0_v2".to_string(), "p/d/1_v2".to_string()],
        frame_tables: vec![
            vec![
                FrameEntry {
                    offset: 0,
                    len: 100,
                },
                FrameEntry {
                    offset: 100,
                    len: 200,
                },
            ],
            vec![FrameEntry {
                offset: 0,
                len: 150,
            }],
        ],
        subframe_overrides: vec![overrides_g0, HashMap::new()],
        ..Manifest::empty()
    };

    let bytes = rmp_serde::to_vec(&m).expect("msgpack serialize full manifest with overrides");
    let m2: Manifest =
        rmp_serde::from_slice(&bytes).expect("msgpack deserialize full manifest with overrides");

    assert_eq!(m2.version, 3);
    assert_eq!(m2.subframe_overrides.len(), 2);
    assert_eq!(
        m2.subframe_overrides[0].len(),
        2,
        "group 0 should have 2 overrides"
    );
    assert!(
        m2.subframe_overrides[0].contains_key(&2),
        "should have override for frame 2"
    );
    assert!(
        m2.subframe_overrides[0].contains_key(&5),
        "should have override for frame 5"
    );
    assert_eq!(m2.subframe_overrides[0][&2].key, "p/d/0_f2_v3");
    assert_eq!(m2.subframe_overrides[0][&5].entry.len, 800);
    assert!(
        m2.subframe_overrides[1].is_empty(),
        "group 1 should have no overrides"
    );
}
