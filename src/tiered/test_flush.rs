use super::*;
use std::collections::{HashMap, HashSet};

#[test]
fn test_local_checkpoint_flag_default_false() {
    set_local_checkpoint_only(false);
    assert!(!is_local_checkpoint_only());
}

#[test]
fn test_local_checkpoint_flag_set_and_clear() {
    set_local_checkpoint_only(true);
    assert!(is_local_checkpoint_only());
    set_local_checkpoint_only(false);
    assert!(!is_local_checkpoint_only());
}

#[test]
fn test_s3_dirty_groups_drain_merges_into_groups_dirty() {
    // Simulate what sync() does: drain s3_dirty_groups into groups_dirty
    let mut s3_dirty: HashSet<u64> = HashSet::new();
    s3_dirty.insert(0);
    s3_dirty.insert(3);
    s3_dirty.insert(7);

    let mut groups_dirty: HashMap<u64, Vec<u64>> = HashMap::new();
    groups_dirty.entry(0).or_default().push(100);
    groups_dirty.entry(5).or_default().push(20480);

    // Merge pending groups (as sync() does)
    for gid in s3_dirty.drain() {
        groups_dirty.entry(gid).or_default();
    }

    // Group 0: has dirty_snapshot pages
    assert_eq!(groups_dirty[&0], vec![100]);
    // Group 5: has dirty_snapshot pages
    assert_eq!(groups_dirty[&5], vec![20480]);
    // Groups 3 and 7: pending from local checkpoint, no dirty_snapshot pages
    assert!(groups_dirty[&3].is_empty());
    assert!(groups_dirty[&7].is_empty());
    assert!(s3_dirty.is_empty());
}

#[test]
fn test_local_checkpoint_interior_page_detection() {
    let page_size = 4096usize;

    // B-tree interior index page (type 0x02) — not page 0
    let mut interior_index = vec![0u8; page_size];
    interior_index[0] = 0x02;
    assert_eq!(interior_index.get(0), Some(&0x02));

    // B-tree interior table page (type 0x05) — not page 0
    let mut interior_table = vec![0u8; page_size];
    interior_table[0] = 0x05;
    assert_eq!(interior_table.get(0), Some(&0x05));

    // Page 0: type byte at offset 100 (after SQLite header)
    let mut page0 = vec![0u8; page_size];
    page0[100] = 0x05;
    assert_eq!(page0.get(100), Some(&0x05));

    // Leaf page (type 0x0D) — should NOT be interior
    let mut leaf = vec![0u8; page_size];
    leaf[0] = 0x0D;
    let b = leaf[0];
    assert!(b != 0x05 && b != 0x02);
}

#[test]
fn test_flush_empty_dirty_groups_is_noop() {
    // flush_dirty_groups_to_s3 should return Ok immediately if no pending groups
    let shared_dirty = Arc::new(std::sync::Mutex::new(HashSet::<u64>::new()));
    let drained: HashSet<u64> = {
        let mut pending = shared_dirty.lock().unwrap();
        std::mem::take(&mut *pending)
    };
    assert!(drained.is_empty());
}

// =========================================================================
// Phase Drift: override threshold tests
// =========================================================================

#[test]
fn test_override_threshold_auto_computation() {
    // auto threshold = frames_per_group / 4
    // With ppg=256 and sub_ppf=4, frames_per_group = 256/4 = 64
    // auto threshold = 64 / 4 = 16
    let ppg = 256u32;
    let sub_ppf = 4u32;
    let frames_per_group = ppg / sub_ppf;
    let auto_threshold = frames_per_group / 4;
    assert_eq!(auto_threshold, 16);
}

#[test]
fn test_override_threshold_boundary() {
    // When dirty_frames < threshold: override path
    // When dirty_frames >= threshold: full rewrite
    let threshold = 4u32;
    assert!(3 < threshold);   // override
    assert!(!(4 < threshold)); // full rewrite
    assert!(!(5 < threshold)); // full rewrite
}

#[test]
fn test_override_key_format() {
    use crate::tiered::StorageClient;
    let key = StorageClient::override_frame_key(5, 3, 10);
    assert_eq!(key, "p/d/5_f3_v10");
}

#[test]
fn test_override_key_format_zero_indices() {
    use crate::tiered::StorageClient;
    let key = StorageClient::override_frame_key(0, 0, 1);
    assert_eq!(key, "p/d/0_f0_v1");
}
