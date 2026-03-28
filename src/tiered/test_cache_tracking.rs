use super::*;
use crate::tiered::*;
use tempfile::TempDir;
use std::time::Duration;

#[cfg(feature = "encryption")]
fn test_key() -> [u8; 32] {
    let mut key = [0u8; 32];
    for (i, b) in key.iter_mut().enumerate() { *b = i as u8; }
    key
}

#[cfg(feature = "encryption")]
fn wrong_key() -> [u8; 32] {
    [0xFFu8; 32]
}

// =========================================================================
// SubChunkTracker — Comprehensive Tests
// =========================================================================

#[test]
fn test_sub_chunk_for_page_basic() {
    let dir = TempDir::new().unwrap();
    let t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
    // ppg=8, spf=2: group has 4 frames (0..3), each covering 2 pages
    // Page 0,1 → group 0, frame 0
    assert_eq!(t.sub_chunk_for_page(0), SubChunkId { group_id: 0, frame_index: 0 });
    assert_eq!(t.sub_chunk_for_page(1), SubChunkId { group_id: 0, frame_index: 0 });
    // Page 2,3 → group 0, frame 1
    assert_eq!(t.sub_chunk_for_page(2), SubChunkId { group_id: 0, frame_index: 1 });
    assert_eq!(t.sub_chunk_for_page(3), SubChunkId { group_id: 0, frame_index: 1 });
    // Page 6,7 → group 0, frame 3
    assert_eq!(t.sub_chunk_for_page(6), SubChunkId { group_id: 0, frame_index: 3 });
    assert_eq!(t.sub_chunk_for_page(7), SubChunkId { group_id: 0, frame_index: 3 });
    // Page 8 → group 1, frame 0
    assert_eq!(t.sub_chunk_for_page(8), SubChunkId { group_id: 1, frame_index: 0 });
    // Page 15 → group 1, frame 3
    assert_eq!(t.sub_chunk_for_page(15), SubChunkId { group_id: 1, frame_index: 3 });
}

#[test]
fn test_sub_chunk_for_page_large_config() {
    let dir = TempDir::new().unwrap();
    // 64KB pages: ppg=256, spf=4 (production defaults)
    let t = SubChunkTracker::new(dir.path().join("t"), 256, 4);
    // Page 0 → group 0, frame 0
    assert_eq!(t.sub_chunk_for_page(0), SubChunkId { group_id: 0, frame_index: 0 });
    // Page 3 → group 0, frame 0 (still within first 4 pages)
    assert_eq!(t.sub_chunk_for_page(3), SubChunkId { group_id: 0, frame_index: 0 });
    // Page 4 → group 0, frame 1
    assert_eq!(t.sub_chunk_for_page(4), SubChunkId { group_id: 0, frame_index: 1 });
    // Page 255 → group 0, frame 63
    assert_eq!(t.sub_chunk_for_page(255), SubChunkId { group_id: 0, frame_index: 63 });
    // Page 256 → group 1, frame 0
    assert_eq!(t.sub_chunk_for_page(256), SubChunkId { group_id: 1, frame_index: 0 });
}

#[test]
fn test_sub_chunk_for_page_zero_config() {
    let dir = TempDir::new().unwrap();
    let t = SubChunkTracker::new(dir.path().join("t"), 0, 0);
    // Edge: zero config should not panic, returns (0, 0)
    assert_eq!(t.sub_chunk_for_page(100), SubChunkId { group_id: 0, frame_index: 0 });
}

#[test]
fn test_sub_chunk_tracker_mark_and_present() {
    let dir = TempDir::new().unwrap();
    let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
    let id = SubChunkId { group_id: 0, frame_index: 1 };
    assert!(!t.is_sub_chunk_present(&id));
    assert!(!t.is_present(2)); // page 2 maps to this sub-chunk

    t.mark_present(id, SubChunkTier::Data);
    assert!(t.is_sub_chunk_present(&id));
    assert!(t.is_present(2));
    assert!(t.is_present(3)); // same sub-chunk
    assert!(!t.is_present(0)); // different sub-chunk (frame 0)
    assert!(!t.is_present(4)); // different sub-chunk (frame 2)
}

#[test]
fn test_sub_chunk_tracker_tier_promotion_respects_priority() {
    let dir = TempDir::new().unwrap();
    let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
    let id = SubChunkId { group_id: 0, frame_index: 0 };

    // Start as Data
    t.mark_present(id, SubChunkTier::Data);
    assert_eq!(t.tiers.get(&id), Some(&SubChunkTier::Data));

    // Promote to Index
    t.mark_index(id);
    assert_eq!(t.tiers.get(&id), Some(&SubChunkTier::Index));

    // Promote to Pinned
    t.mark_pinned(id);
    assert_eq!(t.tiers.get(&id), Some(&SubChunkTier::Pinned));

    // Cannot demote from Pinned via mark_present
    t.mark_present(id, SubChunkTier::Data);
    assert_eq!(t.tiers.get(&id), Some(&SubChunkTier::Pinned));

    // Cannot demote from Pinned via mark_index
    t.mark_index(id);
    assert_eq!(t.tiers.get(&id), Some(&SubChunkTier::Pinned));
}

#[test]
fn test_sub_chunk_tracker_mark_present_does_not_demote() {
    let dir = TempDir::new().unwrap();
    let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
    let id = SubChunkId { group_id: 0, frame_index: 0 };

    // Set as Index
    t.mark_present(id, SubChunkTier::Index);
    assert_eq!(t.tiers.get(&id), Some(&SubChunkTier::Index));

    // mark_present with Data should NOT demote to Data
    t.mark_present(id, SubChunkTier::Data);
    assert_eq!(t.tiers.get(&id), Some(&SubChunkTier::Index));

    // mark_present with Index (same level) should keep Index
    t.mark_present(id, SubChunkTier::Index);
    assert_eq!(t.tiers.get(&id), Some(&SubChunkTier::Index));
}

#[test]
fn test_sub_chunk_tracker_evict_one_prefers_data_over_index() {
    let dir = TempDir::new().unwrap();
    let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
    let data_id = SubChunkId { group_id: 0, frame_index: 0 };
    let index_id = SubChunkId { group_id: 0, frame_index: 1 };

    t.mark_present(index_id, SubChunkTier::Index);
    t.mark_present(data_id, SubChunkTier::Data);

    // Should evict Data first (higher tier number)
    let evicted = t.evict_one().unwrap();
    assert_eq!(evicted, data_id);
    assert!(t.is_sub_chunk_present(&index_id));
    assert!(!t.is_sub_chunk_present(&data_id));
}

#[test]
fn test_sub_chunk_tracker_evict_one_never_evicts_pinned() {
    let dir = TempDir::new().unwrap();
    let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
    let pinned = SubChunkId { group_id: 0, frame_index: 0 };
    let data = SubChunkId { group_id: 0, frame_index: 1 };

    t.mark_present(pinned, SubChunkTier::Data);
    t.mark_pinned(pinned);
    t.mark_present(data, SubChunkTier::Data);

    // Should evict data, not pinned
    let evicted = t.evict_one().unwrap();
    assert_eq!(evicted, data);
    assert!(t.is_sub_chunk_present(&pinned));

    // With only pinned left, evict_one returns None
    assert!(t.evict_one().is_none());
}

#[test]
fn test_sub_chunk_tracker_evict_one_lru_within_tier() {
    let dir = TempDir::new().unwrap();
    let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
    let old = SubChunkId { group_id: 0, frame_index: 0 };
    let new = SubChunkId { group_id: 0, frame_index: 1 };

    t.mark_present(old, SubChunkTier::Data);
    std::thread::sleep(Duration::from_millis(10));
    t.mark_present(new, SubChunkTier::Data);

    // Should evict the older one
    let evicted = t.evict_one().unwrap();
    assert_eq!(evicted, old);
}

#[test]
fn test_sub_chunk_tracker_evict_cascade_data_then_index() {
    let dir = TempDir::new().unwrap();
    let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
    let pinned = SubChunkId { group_id: 0, frame_index: 0 };
    let index = SubChunkId { group_id: 0, frame_index: 1 };
    let data1 = SubChunkId { group_id: 0, frame_index: 2 };
    let data2 = SubChunkId { group_id: 0, frame_index: 3 };

    t.mark_present(pinned, SubChunkTier::Data);
    t.mark_pinned(pinned);
    t.mark_present(index, SubChunkTier::Index);
    std::thread::sleep(Duration::from_millis(5));
    t.mark_present(data1, SubChunkTier::Data);
    std::thread::sleep(Duration::from_millis(5));
    t.mark_present(data2, SubChunkTier::Data);

    // First two evictions should be Data tier (oldest first)
    assert_eq!(t.evict_one().unwrap(), data1);
    assert_eq!(t.evict_one().unwrap(), data2);
    // Then Index tier
    assert_eq!(t.evict_one().unwrap(), index);
    // Pinned never evicted
    assert!(t.evict_one().is_none());
    assert!(t.is_sub_chunk_present(&pinned));
}

#[test]
fn test_sub_chunk_tracker_evict_empty() {
    let dir = TempDir::new().unwrap();
    let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
    assert!(t.evict_one().is_none());
}

#[test]
fn test_sub_chunk_tracker_remove_group() {
    let dir = TempDir::new().unwrap();
    let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
    // Add sub-chunks across two groups
    t.mark_present(SubChunkId { group_id: 0, frame_index: 0 }, SubChunkTier::Data);
    t.mark_present(SubChunkId { group_id: 0, frame_index: 1 }, SubChunkTier::Pinned);
    t.mark_present(SubChunkId { group_id: 1, frame_index: 0 }, SubChunkTier::Data);

    t.remove_group(0);
    // Group 0 sub-chunks gone (even pinned)
    assert!(!t.is_sub_chunk_present(&SubChunkId { group_id: 0, frame_index: 0 }));
    assert!(!t.is_sub_chunk_present(&SubChunkId { group_id: 0, frame_index: 1 }));
    // Group 1 unaffected
    assert!(t.is_sub_chunk_present(&SubChunkId { group_id: 1, frame_index: 0 }));
}

#[test]
fn test_sub_chunk_tracker_clear_data_keeps_pinned() {
    let dir = TempDir::new().unwrap();
    let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
    let pinned = SubChunkId { group_id: 0, frame_index: 0 };
    let index = SubChunkId { group_id: 0, frame_index: 1 };
    let data = SubChunkId { group_id: 0, frame_index: 2 };

    t.mark_present(pinned, SubChunkTier::Data);
    t.mark_pinned(pinned);
    t.mark_present(index, SubChunkTier::Index);
    t.mark_present(data, SubChunkTier::Data);

    t.clear_data();
    // Pinned survives
    assert!(t.is_sub_chunk_present(&pinned));
    // Index and Data are gone
    assert!(!t.is_sub_chunk_present(&index));
    assert!(!t.is_sub_chunk_present(&data));
}

#[test]
fn test_sub_chunk_tracker_clear_data_only_keeps_index_and_pinned() {
    let dir = TempDir::new().unwrap();
    let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
    let pinned = SubChunkId { group_id: 0, frame_index: 0 };
    let index = SubChunkId { group_id: 0, frame_index: 1 };
    let data = SubChunkId { group_id: 0, frame_index: 2 };

    t.mark_present(pinned, SubChunkTier::Data);
    t.mark_pinned(pinned);
    t.mark_present(index, SubChunkTier::Index);
    t.mark_present(data, SubChunkTier::Data);

    t.clear_data_only();
    // Pinned and Index survive
    assert!(t.is_sub_chunk_present(&pinned));
    assert!(t.is_sub_chunk_present(&index));
    // Data is gone
    assert!(!t.is_sub_chunk_present(&data));
}

#[test]
fn test_sub_chunk_tracker_clear_all() {
    let dir = TempDir::new().unwrap();
    let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
    t.mark_present(SubChunkId { group_id: 0, frame_index: 0 }, SubChunkTier::Data);
    t.mark_pinned(SubChunkId { group_id: 0, frame_index: 0 });
    t.mark_present(SubChunkId { group_id: 0, frame_index: 1 }, SubChunkTier::Index);
    t.mark_present(SubChunkId { group_id: 0, frame_index: 2 }, SubChunkTier::Data);

    t.clear_all();
    assert_eq!(t.present.len(), 0);
    assert_eq!(t.tiers.len(), 0);
    assert_eq!(t.access_times.len(), 0);
}

#[test]
fn test_sub_chunk_tracker_clear_index_and_data_keeps_pinned() {
    let dir = TempDir::new().unwrap();
    let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
    let pinned = SubChunkId { group_id: 0, frame_index: 0 };
    let index = SubChunkId { group_id: 0, frame_index: 1 };
    let data = SubChunkId { group_id: 0, frame_index: 2 };

    t.mark_present(pinned, SubChunkTier::Data);
    t.mark_pinned(pinned);
    t.mark_present(index, SubChunkTier::Index);
    t.mark_present(data, SubChunkTier::Data);

    t.clear_index_and_data();
    // Pinned survives
    assert!(t.is_sub_chunk_present(&pinned));
    // Index and Data are gone
    assert!(!t.is_sub_chunk_present(&index));
    assert!(!t.is_sub_chunk_present(&data));
}

#[test]
fn test_sub_chunk_tracker_persist_and_reload() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("tracker_persist");
    {
        let mut t = SubChunkTracker::new(path.clone(), 8, 2);
        t.mark_present(SubChunkId { group_id: 0, frame_index: 0 }, SubChunkTier::Pinned);
        t.mark_present(SubChunkId { group_id: 0, frame_index: 1 }, SubChunkTier::Index);
        t.mark_present(SubChunkId { group_id: 1, frame_index: 2 }, SubChunkTier::Data);
        t.persist().unwrap();
    }
    // Reload
    let t2 = SubChunkTracker::new(path, 8, 2);
    assert!(t2.is_sub_chunk_present(&SubChunkId { group_id: 0, frame_index: 0 }));
    assert!(t2.is_sub_chunk_present(&SubChunkId { group_id: 0, frame_index: 1 }));
    assert!(t2.is_sub_chunk_present(&SubChunkId { group_id: 1, frame_index: 2 }));
    // Tiers survive persistence
    assert_eq!(t2.tiers.get(&SubChunkId { group_id: 0, frame_index: 0 }), Some(&SubChunkTier::Pinned));
    assert_eq!(t2.tiers.get(&SubChunkId { group_id: 0, frame_index: 1 }), Some(&SubChunkTier::Index));
    assert_eq!(t2.tiers.get(&SubChunkId { group_id: 1, frame_index: 2 }), Some(&SubChunkTier::Data));
}

#[test]
fn test_sub_chunk_tracker_len_and_count_tier() {
    let dir = TempDir::new().unwrap();
    let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
    assert_eq!(t.len(), 0);

    t.mark_present(SubChunkId { group_id: 0, frame_index: 0 }, SubChunkTier::Pinned);
    t.mark_present(SubChunkId { group_id: 0, frame_index: 1 }, SubChunkTier::Index);
    t.mark_present(SubChunkId { group_id: 0, frame_index: 2 }, SubChunkTier::Data);
    t.mark_present(SubChunkId { group_id: 0, frame_index: 3 }, SubChunkTier::Data);

    assert_eq!(t.len(), 4);
    assert_eq!(t.count_tier(SubChunkTier::Pinned), 1);
    assert_eq!(t.count_tier(SubChunkTier::Index), 1);
    assert_eq!(t.count_tier(SubChunkTier::Data), 2);
}

#[test]
fn test_sub_chunk_tracker_touch_updates_lru_order() {
    let dir = TempDir::new().unwrap();
    let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
    let old = SubChunkId { group_id: 0, frame_index: 0 };
    let new = SubChunkId { group_id: 0, frame_index: 1 };

    t.mark_present(old, SubChunkTier::Data);
    std::thread::sleep(Duration::from_millis(10));
    t.mark_present(new, SubChunkTier::Data);

    // Touch the old one to make it "new"
    std::thread::sleep(Duration::from_millis(10));
    t.touch(old);

    // Now `new` is the least recently used
    let evicted = t.evict_one().unwrap();
    assert_eq!(evicted, new);
}

#[test]
fn test_sub_chunk_tracker_pages_for_sub_chunk() {
    let dir = TempDir::new().unwrap();
    let t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
    let page_count = 16u64;
    // Frame 0 of group 0: pages 0..2
    let pages = t.pages_for_sub_chunk(SubChunkId { group_id: 0, frame_index: 0 }, page_count);
    assert_eq!(pages, 0..2);
    // Frame 2 of group 0: pages 4..6
    let pages = t.pages_for_sub_chunk(SubChunkId { group_id: 0, frame_index: 2 }, page_count);
    assert_eq!(pages, 4..6);
    // Frame 0 of group 1: pages 8..10
    let pages = t.pages_for_sub_chunk(SubChunkId { group_id: 1, frame_index: 0 }, page_count);
    assert_eq!(pages, 8..10);
    // Boundary: last frame clamped to page_count
    let pages = t.pages_for_sub_chunk(SubChunkId { group_id: 1, frame_index: 3 }, page_count);
    assert_eq!(pages, 14..16);
}

// ===== Regression tests for review bugs =====

#[test]
fn test_evict_one_no_access_time_treated_as_oldest() {
    // Regression: evict_one() used Instant::now() per-iteration for missing access times,
    // making those entries effectively unevictable. Now uses a fixed epoch (1hr ago).
    let dir = TempDir::new().unwrap();
    let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);

    // Insert a sub-chunk via present set and tier, but no access time
    let orphan = SubChunkId { group_id: 0, frame_index: 0 };
    t.present.insert(orphan);
    t.tiers.insert(orphan, SubChunkTier::Data);
    // Intentionally no access_times entry

    // Insert a normal sub-chunk with access time
    let normal = SubChunkId { group_id: 0, frame_index: 1 };
    t.mark_present(normal, SubChunkTier::Data);

    // The orphan (no access time) should be evicted first — it gets epoch (oldest)
    let evicted = t.evict_one().unwrap();
    assert_eq!(evicted, orphan, "sub-chunk without access time should be evicted first");
    // Normal one should still be present
    assert!(t.is_sub_chunk_present(&normal));
}

#[test]
fn test_evict_one_multiple_missing_access_times_all_evictable() {
    // Regression: with per-iteration Instant::now(), multiple missing-access-time entries
    // would compete unfairly. With fixed epoch, they're all equally old.
    let dir = TempDir::new().unwrap();
    let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);

    // Insert 3 sub-chunks with no access times
    for i in 0..3u16 {
        let id = SubChunkId { group_id: 0, frame_index: i };
        t.present.insert(id);
        t.tiers.insert(id, SubChunkTier::Data);
    }

    // All 3 should be evictable
    assert!(t.evict_one().is_some());
    assert!(t.evict_one().is_some());
    assert!(t.evict_one().is_some());
    assert!(t.evict_one().is_none());
}

#[test]
fn test_mark_pinned_adds_to_present_set() {
    // Regression: mark_pinned() only set tier without adding to present set.
    // This meant pinned sub-chunks from write_page path weren't tracked.
    let dir = TempDir::new().unwrap();
    let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);

    let id = SubChunkId { group_id: 0, frame_index: 0 };

    // Before fix: mark_pinned without prior mark_present left sub-chunk not in present
    t.mark_pinned(id);

    assert!(t.is_sub_chunk_present(&id), "mark_pinned must add to present set");
    assert_eq!(t.tiers[&id], SubChunkTier::Pinned);
    assert!(t.access_times.contains_key(&id), "mark_pinned must set access time");
    assert_eq!(t.len(), 1);
    assert_eq!(t.count_tier(SubChunkTier::Pinned), 1);
}

#[test]
fn test_mark_index_adds_to_present_set() {
    // Regression: mark_index() only set tier without adding to present set.
    let dir = TempDir::new().unwrap();
    let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);

    let id = SubChunkId { group_id: 0, frame_index: 0 };

    t.mark_index(id);

    assert!(t.is_sub_chunk_present(&id), "mark_index must add to present set");
    assert_eq!(t.tiers[&id], SubChunkTier::Index);
    assert!(t.access_times.contains_key(&id), "mark_index must set access time");
    assert_eq!(t.len(), 1);
    assert_eq!(t.count_tier(SubChunkTier::Index), 1);
}

#[test]
fn test_mark_pinned_does_not_duplicate_when_already_present() {
    // mark_pinned on an already-present sub-chunk should promote tier, not add duplicate.
    let dir = TempDir::new().unwrap();
    let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);

    let id = SubChunkId { group_id: 0, frame_index: 0 };
    t.mark_present(id, SubChunkTier::Data);
    assert_eq!(t.len(), 1);

    t.mark_pinned(id);
    assert_eq!(t.len(), 1); // still just 1 entry
    assert_eq!(t.tiers[&id], SubChunkTier::Pinned);
}

#[test]
fn test_mark_index_respects_pinned_priority() {
    // mark_index on a Pinned sub-chunk should NOT demote it.
    let dir = TempDir::new().unwrap();
    let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);

    let id = SubChunkId { group_id: 0, frame_index: 0 };
    t.mark_pinned(id);
    t.mark_index(id);

    assert_eq!(t.tiers[&id], SubChunkTier::Pinned, "mark_index must not demote from Pinned");
}

#[test]
fn test_clear_data_removes_index_and_data() {
    // Verify clear_data() removes both Data and Index, keeps only Pinned.
    // (Used by clear_cache_all-like scenarios, not by clear_cache.)
    let dir = TempDir::new().unwrap();
    let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);

    let pinned = SubChunkId { group_id: 0, frame_index: 0 };
    let index = SubChunkId { group_id: 0, frame_index: 1 };
    let data = SubChunkId { group_id: 0, frame_index: 2 };

    t.mark_present(pinned, SubChunkTier::Pinned);
    t.mark_present(index, SubChunkTier::Index);
    t.mark_present(data, SubChunkTier::Data);

    t.clear_data();

    assert!(t.is_sub_chunk_present(&pinned), "Pinned must survive clear_data");
    assert!(!t.is_sub_chunk_present(&index), "Index must be removed by clear_data");
    assert!(!t.is_sub_chunk_present(&data), "Data must be removed by clear_data");
}

// ===== SubChunkTracker persistence encryption tests =====

#[test]
#[cfg(feature = "encryption")]
fn test_sub_chunk_tracker_encrypted_persist_roundtrip() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("tracker_enc");
    let key = test_key();

    // Create tracker with encryption, add some sub-chunks
    {
        let mut t = SubChunkTracker::new_encrypted(path.clone(), 8, 2, Some(key));
        let id1 = SubChunkId { group_id: 0, frame_index: 0 };
        let id2 = SubChunkId { group_id: 1, frame_index: 2 };
        t.mark_present(id1, SubChunkTier::Pinned);
        t.mark_present(id2, SubChunkTier::Data);
        t.persist().unwrap();
    }

    // Reload with same key — data must survive
    let t2 = SubChunkTracker::new_encrypted(path, 8, 2, Some(key));
    let id1 = SubChunkId { group_id: 0, frame_index: 0 };
    let id2 = SubChunkId { group_id: 1, frame_index: 2 };
    assert!(t2.is_sub_chunk_present(&id1), "sub-chunk 0:0 must survive persist+reload");
    assert!(t2.is_sub_chunk_present(&id2), "sub-chunk 1:2 must survive persist+reload");
    assert_eq!(t2.tiers.get(&id1).copied(), Some(SubChunkTier::Pinned), "tier must survive");
    assert_eq!(t2.tiers.get(&id2).copied(), Some(SubChunkTier::Data), "tier must survive");
}

#[test]
#[cfg(feature = "encryption")]
fn test_sub_chunk_tracker_encrypted_on_disk_not_plaintext() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("tracker_enc");
    let key = test_key();

    let mut t = SubChunkTracker::new_encrypted(path.clone(), 8, 2, Some(key));
    let id = SubChunkId { group_id: 5, frame_index: 3 };
    t.mark_present(id, SubChunkTier::Index);
    t.persist().unwrap();

    // Raw file should not be valid JSON (it's encrypted)
    let raw = std::fs::read(&path).unwrap();
    let parse_result: Result<Vec<(SubChunkId, u8)>, _> = serde_json::from_slice(&raw);
    assert!(parse_result.is_err(), "encrypted tracker file must not be valid JSON");
}

#[test]
#[cfg(feature = "encryption")]
fn test_sub_chunk_tracker_wrong_key_returns_empty() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("tracker_enc");
    let key = test_key();

    // Write with correct key
    {
        let mut t = SubChunkTracker::new_encrypted(path.clone(), 8, 2, Some(key));
        let id = SubChunkId { group_id: 0, frame_index: 0 };
        t.mark_present(id, SubChunkTier::Pinned);
        t.persist().unwrap();
    }

    // Load with wrong key — should fail gracefully (empty tracker, not crash)
    let t2 = SubChunkTracker::new_encrypted(path, 8, 2, Some(wrong_key()));
    let id = SubChunkId { group_id: 0, frame_index: 0 };
    assert!(!t2.is_sub_chunk_present(&id), "wrong key must not load data");
}

#[test]
#[cfg(feature = "encryption")]
fn test_sub_chunk_tracker_no_encryption_stays_plaintext() {
    // Without encryption, tracker persist is still plain JSON
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("tracker_plain");

    let mut t = SubChunkTracker::new(path.clone(), 8, 2);
    let id = SubChunkId { group_id: 2, frame_index: 1 };
    t.mark_present(id, SubChunkTier::Data);
    t.persist().unwrap();

    // Raw file should be valid JSON
    let raw = std::fs::read(&path).unwrap();
    let parse_result: Result<Vec<(SubChunkId, u8)>, _> = serde_json::from_slice(&raw);
    assert!(parse_result.is_ok(), "unencrypted tracker file must be valid JSON");
}

// ── Phase Stalingrad: cache byte tracking tests ──

#[test]
fn test_current_cache_bytes_tracks_mark_present() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("tracker.json");
    let mut t = SubChunkTracker::new(path, 8, 4);
    t.set_sub_chunk_byte_size(256 * 1024); // 256KB per sub-chunk

    assert_eq!(t.current_cache_bytes, 0);

    let id1 = SubChunkId { group_id: 0, frame_index: 0 };
    let id2 = SubChunkId { group_id: 0, frame_index: 1 };
    t.mark_present(id1, SubChunkTier::Data);
    assert_eq!(t.current_cache_bytes, 256 * 1024);

    t.mark_present(id2, SubChunkTier::Index);
    assert_eq!(t.current_cache_bytes, 512 * 1024);

    // Marking same sub-chunk again doesn't double-count
    t.mark_present(id1, SubChunkTier::Data);
    assert_eq!(t.current_cache_bytes, 512 * 1024);
}

#[test]
fn test_current_cache_bytes_tracks_remove() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("tracker.json");
    let mut t = SubChunkTracker::new(path, 8, 4);
    t.set_sub_chunk_byte_size(100);

    let id1 = SubChunkId { group_id: 0, frame_index: 0 };
    let id2 = SubChunkId { group_id: 1, frame_index: 0 };
    t.mark_present(id1, SubChunkTier::Data);
    t.mark_present(id2, SubChunkTier::Data);
    assert_eq!(t.current_cache_bytes, 200);

    t.remove(id1);
    assert_eq!(t.current_cache_bytes, 100);

    // Removing non-existent doesn't go negative
    t.remove(id1);
    assert_eq!(t.current_cache_bytes, 100);
}

#[test]
fn test_current_cache_bytes_clear_all_resets() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("tracker.json");
    let mut t = SubChunkTracker::new(path, 8, 4);
    t.set_sub_chunk_byte_size(100);

    for i in 0..10 {
        t.mark_present(SubChunkId { group_id: i, frame_index: 0 }, SubChunkTier::Data);
    }
    assert_eq!(t.current_cache_bytes, 1000);

    t.clear_all();
    assert_eq!(t.current_cache_bytes, 0);
}

#[test]
fn test_mark_pinned_and_index_track_bytes() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("tracker.json");
    let mut t = SubChunkTracker::new(path, 8, 4);
    t.set_sub_chunk_byte_size(100);

    let id1 = SubChunkId { group_id: 0, frame_index: 0 };
    let id2 = SubChunkId { group_id: 1, frame_index: 0 };

    // mark_pinned on new sub-chunk increments bytes
    t.mark_pinned(id1);
    assert_eq!(t.current_cache_bytes, 100);

    // mark_index on new sub-chunk increments bytes
    t.mark_index(id2);
    assert_eq!(t.current_cache_bytes, 200);

    // Promoting existing to pinned doesn't double-count
    t.mark_pinned(id2);
    assert_eq!(t.current_cache_bytes, 200);
}

#[test]
fn test_pinned_bytes() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("tracker.json");
    let mut t = SubChunkTracker::new(path, 8, 4);
    t.set_sub_chunk_byte_size(100);

    t.mark_present(SubChunkId { group_id: 0, frame_index: 0 }, SubChunkTier::Pinned);
    t.mark_present(SubChunkId { group_id: 1, frame_index: 0 }, SubChunkTier::Index);
    t.mark_present(SubChunkId { group_id: 2, frame_index: 0 }, SubChunkTier::Data);

    assert_eq!(t.pinned_bytes(), 100);
    assert_eq!(t.current_cache_bytes, 300);
}

#[test]
fn test_evict_one_skipping() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("tracker.json");
    let mut t = SubChunkTracker::new(path, 8, 4);
    t.set_sub_chunk_byte_size(100);

    let id_skip = SubChunkId { group_id: 0, frame_index: 0 };
    let id_keep = SubChunkId { group_id: 1, frame_index: 0 };
    let id_evict = SubChunkId { group_id: 2, frame_index: 0 };

    // All Data tier, stagger access times
    t.mark_present(id_skip, SubChunkTier::Data);
    std::thread::sleep(std::time::Duration::from_millis(2));
    t.mark_present(id_keep, SubChunkTier::Data);
    std::thread::sleep(std::time::Duration::from_millis(2));
    t.mark_present(id_evict, SubChunkTier::Data);

    // Group 0 is in skip set, so id_skip won't be evicted
    // Oldest non-skipped is id_keep (group 1)
    let mut skip = HashSet::new();
    skip.insert(0u64);

    let evicted = t.evict_one_skipping(&skip);
    assert_eq!(evicted, Some(id_keep));
    assert_eq!(t.current_cache_bytes, 200);

    // id_skip and id_evict remain
    assert!(t.present.contains(&id_skip));
    assert!(t.present.contains(&id_evict));
}

#[test]
fn test_evict_one_skipping_all_skipped_returns_none() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("tracker.json");
    let mut t = SubChunkTracker::new(path, 8, 4);
    t.set_sub_chunk_byte_size(100);

    t.mark_present(SubChunkId { group_id: 0, frame_index: 0 }, SubChunkTier::Data);
    t.mark_present(SubChunkId { group_id: 1, frame_index: 0 }, SubChunkTier::Pinned);

    let mut skip = HashSet::new();
    skip.insert(0u64); // skip the only Data sub-chunk

    // Only Pinned and skipped remain, nothing evictable
    let evicted = t.evict_one_skipping(&skip);
    assert!(evicted.is_none());
    assert_eq!(t.current_cache_bytes, 200);
}

#[test]
fn test_set_sub_chunk_byte_size_recomputes() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("tracker.json");
    let mut t = SubChunkTracker::new(path, 8, 4);

    // Add sub-chunks before byte size is known
    t.mark_present(SubChunkId { group_id: 0, frame_index: 0 }, SubChunkTier::Data);
    t.mark_present(SubChunkId { group_id: 1, frame_index: 0 }, SubChunkTier::Data);
    assert_eq!(t.current_cache_bytes, 0); // byte size unknown

    // Now set byte size: recomputes from present.len()
    t.set_sub_chunk_byte_size(1024);
    assert_eq!(t.current_cache_bytes, 2048);
}
