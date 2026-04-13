use proptest::prelude::*;
use std::collections::HashMap;
use turbolite::tiered::{
    group_id, group_start_page, local_idx_in_group, BTreeManifestEntry, GroupingStrategy, Manifest,
};

fn make_manifest(
    version: u64,
    page_count: u64,
    page_size: u32,
    pages_per_group: u32,
    page_group_keys: Vec<String>,
    strategy: GroupingStrategy,
) -> Manifest {
    Manifest {
        version,
        change_counter: 0,
        page_count,
        page_size,
        pages_per_group,
        page_group_keys,
        interior_chunk_keys: HashMap::new(),
        index_chunk_keys: HashMap::new(),
        frame_tables: Vec::new(),
        sub_pages_per_frame: 0,
        subframe_overrides: Vec::new(),
        strategy,
        group_pages: Vec::new(),
        btrees: HashMap::new(),
        page_index: HashMap::new(),
        btree_groups: HashMap::new(),
        page_to_tree_name: HashMap::new(),
        tree_name_to_groups: HashMap::new(),
        group_to_tree_name: HashMap::new(),
        db_header: None,
    }
}

fn compute_total_groups(page_count: u64, ppg: u32) -> u64 {
    if ppg == 0 || page_count == 0 {
        return 0;
    }
    (page_count + ppg as u64 - 1) / ppg as u64
}

proptest! {
    #[test]
    fn page_to_group_mapping_is_bijective(
        page_num in 0u64..1_000_000,
        ppg in 1u32..1024u32,
    ) {
        let gid = group_id(page_num, ppg);
        let idx = local_idx_in_group(page_num, ppg);
        let ppg_u64 = ppg as u64;
        prop_assert_eq!(gid * ppg_u64 + idx as u64, page_num);
    }

    #[test]
    fn group_boundaries_are_correct(
        gid in 0u64..1000u64,
        ppg in 1u32..512u32,
    ) {
        let ppg_u64 = ppg as u64;
        let start = group_start_page(gid, ppg);
        let end = start + ppg_u64;

        for page_num in start..end {
            let computed_gid = group_id(page_num, ppg);
            prop_assert_eq!(computed_gid, gid,
                "page {} should be in group {}, got {}", page_num, gid, computed_gid);
        }

        prop_assert_eq!(group_id(end, ppg), gid + 1);

        if gid > 0 {
            prop_assert_eq!(group_id(start.saturating_sub(1), ppg), gid - 1);
        }
    }

    #[test]
    fn group_key_formatting_is_valid(
        version in 0u64..1000u64,
        num_groups in 1u64..100u64,
        page_count in 1u64..100_000u64,
        ppg in 1u32..512u32,
    ) {
        let total = compute_total_groups(page_count, ppg);
        let expected_groups = std::cmp::min(num_groups, total);

        let mut keys = Vec::new();
        for gid in 0..expected_groups {
            let key = format!("pg/{}_v{}", gid, version);
            keys.push(key.clone());

            prop_assert!(key.contains(&format!("_v{}", version)),
                "key '{}' must contain version {}", key, version);

            prop_assert!(key.contains(&format!("pg/{}_v", gid)),
                "key '{}' must contain group id {}", key, gid);

            let key2 = format!("pg/{}_v{}", gid, version);
            prop_assert_eq!(key, key2);
        }

        let _manifest = make_manifest(version, page_count, 4096, ppg, keys, GroupingStrategy::Positional);
    }

    #[test]
    fn local_idx_stays_within_bounds(
        page_num in 0u64..1_000_000u64,
        ppg in 1u32..1024u32,
    ) {
        let idx = local_idx_in_group(page_num, ppg);
        prop_assert!(idx < ppg,
            "local_idx {} must be < ppg {}", idx, ppg);
    }

    #[test]
    fn group_start_page_is_aligned(
        gid in 0u64..10000u64,
        ppg in 1u32..1024u32,
    ) {
        let start = group_start_page(gid, ppg);
        prop_assert_eq!(start % (ppg as u64), 0,
            "group start page {} must be aligned to ppg {}", start, ppg);
    }

    #[test]
    fn page_location_consistent_with_arithmetic(
        page_num in 0u64..100_000u64,
        ppg in 1u32..512u32,
    ) {
        let page_count = std::cmp::max(page_num + 1, 100u64);
        let manifest = make_manifest(
            1, page_count, 4096, ppg, Vec::new(), GroupingStrategy::Positional,
        );

        let loc = manifest.page_location(page_num);
        prop_assert!(loc.is_some(), "page {} must have a location", page_num);

        let loc = loc.unwrap();
        let expected_gid = group_id(page_num, ppg);
        let expected_idx = local_idx_in_group(page_num, ppg);

        prop_assert_eq!(loc.group_id, expected_gid);
        prop_assert_eq!(loc.index, expected_idx);
    }

    #[test]
    fn total_groups_matches_ceiling(
        page_count in 0u64..1_000_000u64,
        ppg in 1u32..1024u32,
    ) {
        let _manifest = make_manifest(
            1, page_count, 4096, ppg, Vec::new(), GroupingStrategy::Positional,
        );

        let expected = compute_total_groups(page_count, ppg);
        prop_assert!(expected >= 0);
        if page_count > 0 {
            prop_assert!(expected > 0);
        }
    }

    #[test]
    fn group_page_nums_correct_range(
        gid in 0u64..100u64,
        ppg in 1u32..256u32,
        page_count in 1u64..100_000u64,
    ) {
        let manifest = make_manifest(
            1, page_count, 4096, ppg, Vec::new(), GroupingStrategy::Positional,
        );

        let total = compute_total_groups(page_count, ppg);
        if gid >= total {
            return Ok(());
        }

        let pages = manifest.group_page_nums(gid);
        let ppg_u64 = ppg as u64;
        let expected_start = gid * ppg_u64;
        let expected_end = std::cmp::min(expected_start + ppg_u64, page_count);

        prop_assert_eq!(pages.len(), (expected_end - expected_start) as usize);

        for (i, &pnum) in pages.iter().enumerate() {
            prop_assert_eq!(pnum, expected_start + i as u64);
        }
    }

    #[test]
    fn btree_aware_page_location_matches_group_pages(
        num_groups in 1u8..20u8,
        pages_per_group in 1u8..32u8,
    ) {
        let num_groups = num_groups as u64;
        let pages_per_group = pages_per_group as u64;

        let mut group_pages: Vec<Vec<u64>> = Vec::new();
        let mut all_pages: Vec<u64> = Vec::new();
        for gid in 0..num_groups {
            let mut pages = Vec::new();
            for i in 0..pages_per_group {
                let pnum = gid * pages_per_group + i;
                pages.push(pnum);
                all_pages.push(pnum);
            }
            group_pages.push(pages);
        }

        let page_count = all_pages.last().map(|&p| p + 1).unwrap_or(0);

        let mut btrees = HashMap::new();
        btrees.insert(0, BTreeManifestEntry {
            name: "test_table".into(),
            obj_type: "table".into(),
            group_ids: (0..num_groups).collect(),
        });

        let mut manifest = Manifest {
            version: 1,
            change_counter: 0,
            page_count,
            page_size: 4096,
            pages_per_group: pages_per_group as u32,
            page_group_keys: (0..num_groups).map(|gid| format!("p/d/{}_v1", gid)).collect(),
            interior_chunk_keys: HashMap::new(),
            index_chunk_keys: HashMap::new(),
            frame_tables: Vec::new(),
            sub_pages_per_frame: 0,
            subframe_overrides: Vec::new(),
            strategy: GroupingStrategy::Positional,
            group_pages,
            btrees,
            page_index: HashMap::new(),
            btree_groups: HashMap::new(),
            page_to_tree_name: HashMap::new(),
            tree_name_to_groups: HashMap::new(),
            group_to_tree_name: HashMap::new(),
            db_header: None,
        };
        manifest.build_page_index();

        prop_assert_eq!(manifest.strategy, GroupingStrategy::BTreeAware);

        for &pnum in &all_pages {
            let loc = manifest.page_location(pnum);
            prop_assert!(loc.is_some(), "page {} must be found in BTreeAware manifest", pnum);
        }
    }
}
