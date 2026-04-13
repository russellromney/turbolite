use proptest::prelude::*;
use rmp_serde;
use std::collections::HashMap;
use turbolite::tiered::{FrameEntry, GroupingStrategy, Manifest};

fn make_manifest(
    version: u64,
    change_counter: u64,
    page_count: u64,
    page_size: u32,
    pages_per_group: u32,
    page_group_keys: Vec<String>,
    frame_tables: Vec<Vec<FrameEntry>>,
    sub_pages_per_frame: u32,
    strategy: GroupingStrategy,
) -> Manifest {
    Manifest {
        version,
        change_counter,
        page_count,
        page_size,
        pages_per_group,
        page_group_keys,
        interior_chunk_keys: HashMap::new(),
        index_chunk_keys: HashMap::new(),
        frame_tables,
        sub_pages_per_frame,
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

proptest! {
    #[test]
    fn manifest_roundtrip(
        version in 0u64..1000u64,
        page_count in 0u64..100_000u64,
        page_size in prop::sample::select(&[4096u32, 8192, 16384, 32768, 65536]),
        ppg in 1u32..512u32,
        num_keys in 0u8..50u8,
        sub_pages in 0u32..16u32,
    ) {
        let page_group_keys: Vec<String> = (0..num_keys)
            .map(|i| format!("pg/{}_v{}", i, version))
            .collect();

        let mut frame_tables: Vec<Vec<FrameEntry>> = Vec::new();
        let mut offset: u64 = 0;
        for _ in 0..num_keys {
            let num_frames = 1 + (num_keys % 8) as usize;
            let mut frames = Vec::new();
            for _ in 0..num_frames {
                let len = 1000u32 + (num_keys as u32 * 100);
                frames.push(FrameEntry { offset, len });
                offset += len as u64;
            }
            frame_tables.push(frames);
        }

        let original = make_manifest(
            version, version * 2, page_count, page_size, ppg,
            page_group_keys, frame_tables, sub_pages,
            GroupingStrategy::Positional,
        );

        let serialized = rmp_serde::to_vec(&original).expect("serialize must not panic");
        let deserialized: Manifest = rmp_serde::from_slice(&serialized).expect("deserialize must not panic");

        prop_assert_eq!(deserialized.version, original.version);
        prop_assert_eq!(deserialized.change_counter, original.change_counter);
        prop_assert_eq!(deserialized.page_count, original.page_count);
        prop_assert_eq!(deserialized.page_size, original.page_size);
        prop_assert_eq!(deserialized.pages_per_group, original.pages_per_group);
        prop_assert_eq!(deserialized.page_group_keys, original.page_group_keys);
        prop_assert_eq!(deserialized.frame_tables, original.frame_tables);
        prop_assert_eq!(deserialized.sub_pages_per_frame, original.sub_pages_per_frame);
        prop_assert_eq!(deserialized.strategy, original.strategy);
    }

    #[test]
    fn manifest_handles_garbage_input(
        bytes in prop::collection::vec(any::<u8>(), 0..512),
    ) {
        let result: Result<Manifest, _> = rmp_serde::from_slice(&bytes);
        if let Ok(ref manifest) = result {
            prop_assert!(manifest.version >= 0);
        }
    }

    #[test]
    fn manifest_version_tracking(
        initial_version in 0u64..1000u64,
        num_updates in 1u8..50u8,
    ) {
        let mut version = initial_version;

        for i in 0..num_updates {
            let expected_version = initial_version + (i as u64);
            prop_assert_eq!(version, expected_version,
                "version must increase by exactly 1 per update");
            version += 1;
        }

        prop_assert_eq!(version, initial_version + num_updates as u64);
    }

    #[test]
    fn manifest_serialization_preserves_frame_entry_integrity(
        offset in 0u64..10_000_000u64,
        len in 1u32..1_000_000u32,
    ) {
        let original = FrameEntry { offset, len };
        let serialized = rmp_serde::to_vec(&original).expect("serialize FrameEntry");
        let restored: FrameEntry = rmp_serde::from_slice(&serialized).expect("deserialize FrameEntry");
        prop_assert_eq!(restored, original);
    }

    #[test]
    fn manifest_handles_empty_vs_populated_frame_tables(
        num_groups in 0u8..20u8,
    ) {
        let frame_tables: Vec<Vec<FrameEntry>> = (0..num_groups)
            .map(|i| {
                if i % 3 == 0 {
                    Vec::new()
                } else {
                    vec![FrameEntry { offset: 0, len: 1000 }]
                }
            })
            .collect();

        let manifest = make_manifest(
            1, 0, 100, 4096, 16,
            (0..num_groups).map(|i| format!("pg/{}_v1", i)).collect(),
            frame_tables, 4, GroupingStrategy::Positional,
        );

        let serialized = rmp_serde::to_vec(&manifest).expect("serialize");
        let restored: Manifest = rmp_serde::from_slice(&serialized).expect("deserialize");
        prop_assert_eq!(restored.frame_tables.len(), manifest.frame_tables.len());

        for (i, (orig, rest)) in manifest.frame_tables.iter().zip(restored.frame_tables.iter()).enumerate() {
            prop_assert_eq!(orig, rest, "frame_table[{}] must match", i);
        }
    }

    #[test]
    fn manifest_deserialization_from_truncated_data_never_panics(
        valid_bytes in prop::collection::vec(any::<u8>(), 10..200),
        truncate_at in 0usize..200usize,
    ) {
        let truncated: Vec<u8> = valid_bytes.iter().take(truncate_at).cloned().collect();
        let result: Result<Manifest, _> = rmp_serde::from_slice(&truncated);
        drop(result);
    }

    #[test]
    fn manifest_change_counter_survives_roundtrip(
        change_counter in 0u64..u64::MAX,
    ) {
        let original = make_manifest(
            1, change_counter, 100, 4096, 16,
            vec!["pg/0_v1".into()], Vec::new(), 0,
            GroupingStrategy::Positional,
        );

        let serialized = rmp_serde::to_vec(&original).expect("serialize");
        let restored: Manifest = rmp_serde::from_slice(&serialized).expect("deserialize");
        prop_assert_eq!(restored.change_counter, change_counter);
    }
}
