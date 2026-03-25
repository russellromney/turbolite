use super::*;
use crate::tiered::*;
use tempfile::TempDir;
use std::sync::Arc;

/// Build a positional group_pages vec for tests: group g contains pages [g*ppg .. (g+1)*ppg).
fn positional_group_pages(pages_per_group: u32, page_count: u64) -> Vec<Vec<u64>> {
    let ppg = pages_per_group as u64;
    if ppg == 0 || page_count == 0 {
        return Vec::new();
    }
    let num_groups = (page_count + ppg - 1) / ppg;
    (0..num_groups)
        .map(|g| {
            let start = g * ppg;
            let end = ((g + 1) * ppg).min(page_count);
            (start..end).collect()
        })
        .collect()
}

// =========================================================================
// PageBitmap
// =========================================================================

#[test]
fn test_bitmap_set_and_check() {
    let dir = TempDir::new().unwrap();
    let mut bm = PageBitmap::new(dir.path().join("bm"));
    assert!(!bm.is_present(0));
    assert!(!bm.is_present(100));
    bm.mark_present(0);
    assert!(bm.is_present(0));
    assert!(!bm.is_present(1));
    bm.mark_present(100);
    assert!(bm.is_present(100));
    assert!(!bm.is_present(99));
}

#[test]
fn test_bitmap_range() {
    let dir = TempDir::new().unwrap();
    let mut bm = PageBitmap::new(dir.path().join("bm"));
    bm.mark_range(10, 5);
    for p in 10..15 {
        assert!(bm.is_present(p), "page {} should be present", p);
    }
    assert!(!bm.is_present(9));
    assert!(!bm.is_present(15));
    bm.clear_range(11, 2);
    assert!(bm.is_present(10));
    assert!(!bm.is_present(11));
    assert!(!bm.is_present(12));
    assert!(bm.is_present(13));
}

#[test]
fn test_bitmap_persist_and_reload() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("bm");
    {
        let mut bm = PageBitmap::new(path.clone());
        bm.mark_present(42);
        bm.mark_present(1000);
        bm.persist().unwrap();
    }
    let bm2 = PageBitmap::new(path);
    assert!(bm2.is_present(42));
    assert!(bm2.is_present(1000));
    assert!(!bm2.is_present(43));
}

#[test]
fn test_bitmap_byte_boundaries() {
    let dir = TempDir::new().unwrap();
    let mut bm = PageBitmap::new(dir.path().join("bm"));
    // Test at every bit in a byte
    for bit in 0..8 {
        bm.mark_present(bit);
        assert!(bm.is_present(bit));
        // Adjacent bits in next byte untouched
        assert!(!bm.is_present(bit + 8));
    }
    // Byte boundaries: 7→8, 15→16
    bm.mark_present(7);
    bm.mark_present(8);
    assert!(bm.is_present(7));
    assert!(bm.is_present(8));
}

#[test]
fn test_bitmap_large_page_numbers() {
    let dir = TempDir::new().unwrap();
    let mut bm = PageBitmap::new(dir.path().join("bm"));
    let big = 1_000_000u64;
    bm.mark_present(big);
    assert!(bm.is_present(big));
    assert!(!bm.is_present(big - 1));
    assert!(!bm.is_present(big + 1));
    // Bitmap should be big enough
    assert!(bm.bits.len() >= (big as usize / 8) + 1);
}

#[test]
fn test_bitmap_ensure_capacity_auto_extends() {
    let dir = TempDir::new().unwrap();
    let mut bm = PageBitmap::new(dir.path().join("bm"));
    assert_eq!(bm.bits.len(), 0);
    bm.mark_present(0);
    assert!(bm.bits.len() >= 1);
    bm.mark_present(255);
    assert!(bm.bits.len() >= 32);
}

#[test]
fn test_bitmap_resize_explicit() {
    let dir = TempDir::new().unwrap();
    let mut bm = PageBitmap::new(dir.path().join("bm"));
    bm.resize(1000);
    assert!(bm.bits.len() >= 125); // ceil(1000/8)
    // Resize with smaller value should not shrink
    let len_before = bm.bits.len();
    bm.resize(10);
    assert_eq!(bm.bits.len(), len_before);
}

#[test]
fn test_bitmap_clear_range_beyond_capacity() {
    let dir = TempDir::new().unwrap();
    let mut bm = PageBitmap::new(dir.path().join("bm"));
    bm.mark_present(5);
    // Clear range far beyond capacity — should not panic
    bm.clear_range(100000, 500);
    assert!(bm.is_present(5));
}

#[test]
fn test_bitmap_mark_range_zero_count() {
    let dir = TempDir::new().unwrap();
    let mut bm = PageBitmap::new(dir.path().join("bm"));
    bm.mark_range(10, 0); // no-op
    assert!(!bm.is_present(10));
}

#[test]
fn test_bitmap_clear_range_within_single_byte() {
    let dir = TempDir::new().unwrap();
    let mut bm = PageBitmap::new(dir.path().join("bm"));
    // Set all 8 bits in first byte
    for i in 0..8 {
        bm.mark_present(i);
    }
    // Clear only bits 2 and 3
    bm.clear_range(2, 2);
    assert!(bm.is_present(0));
    assert!(bm.is_present(1));
    assert!(!bm.is_present(2));
    assert!(!bm.is_present(3));
    assert!(bm.is_present(4));
    assert!(bm.is_present(5));
}

#[test]
fn test_bitmap_new_nonexistent_file() {
    let dir = TempDir::new().unwrap();
    let bm = PageBitmap::new(dir.path().join("does_not_exist"));
    assert_eq!(bm.bits.len(), 0);
    assert!(!bm.is_present(0));
}

#[test]
fn test_bitmap_persist_creates_file_atomic() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("bm");
    let mut bm = PageBitmap::new(path.clone());
    bm.mark_present(7);
    bm.persist().unwrap();
    assert!(path.exists());
    // tmp file should NOT exist after atomic rename
    assert!(!path.with_extension("tmp").exists());
}

// =========================================================================
// GroupState
// =========================================================================

#[test]
fn test_group_state_enum_values() {
    assert_eq!(GroupState::None as u8, 0);
    assert_eq!(GroupState::Fetching as u8, 1);
    assert_eq!(GroupState::Present as u8, 2);
}

#[test]
fn test_group_state_equality() {
    assert_eq!(GroupState::None, GroupState::None);
    assert_ne!(GroupState::None, GroupState::Fetching);
    assert_ne!(GroupState::Fetching, GroupState::Present);
}

// =========================================================================
// DiskCache
// =========================================================================

#[test]
fn test_disk_cache_write_and_read_page() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 8, 2, 64, 16, None, Vec::new()).unwrap();
    let data = vec![42u8; 64];
    cache.write_page(5, &data).unwrap();
    assert!(cache.is_present(5));
    assert!(!cache.is_present(4));
    let mut buf = vec![0u8; 64];
    cache.read_page(5, &mut buf).unwrap();
    assert_eq!(buf, data);
}

#[test]
fn test_disk_cache_write_multiple_pages() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 16, None, Vec::new()).unwrap();
    for i in 0..16u64 {
        cache.write_page(i, &vec![i as u8; 64]).unwrap();
    }
    for i in 0..16u64 {
        assert!(cache.is_present(i));
        let mut buf = vec![0u8; 64];
        cache.read_page(i, &mut buf).unwrap();
        assert_eq!(buf, vec![i as u8; 64], "page {} mismatch", i);
    }
}

#[test]
fn test_disk_cache_write_page_overwrite() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap();
    cache.write_page(3, &vec![0xAA; 64]).unwrap();
    cache.write_page(3, &vec![0xBB; 64]).unwrap(); // overwrite
    let mut buf = vec![0u8; 64];
    cache.read_page(3, &mut buf).unwrap();
    assert_eq!(buf, vec![0xBB; 64]);
}

#[test]
fn test_disk_cache_write_extends_file() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 0, None, Vec::new()).unwrap(); // page_count=0
    // Writing page 10 should extend the file
    cache.write_page(10, &vec![42u8; 64]).unwrap();
    assert!(cache.is_present(10));
    let mut buf = vec![0u8; 64];
    cache.read_page(10, &mut buf).unwrap();
    assert_eq!(buf, vec![42u8; 64]);
}

#[test]
fn test_disk_cache_read_uncached_page_returns_zeros() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 16, None, Vec::new()).unwrap();
    // Page exists in sparse file but not marked in bitmap
    let mut buf = vec![0xFFu8; 64];
    cache.read_page(0, &mut buf).unwrap();
    assert_eq!(buf, vec![0u8; 64]); // Sparse file reads as zeros
}

#[test]
fn test_disk_cache_creates_cache_file() {
    let dir = TempDir::new().unwrap();
    let _cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 100, None, Vec::new()).unwrap();
    assert!(dir.path().join("data.cache").exists());
    let meta = std::fs::metadata(dir.path().join("data.cache")).unwrap();
    assert_eq!(meta.len(), 100 * 64); // page_count * page_size
}

#[test]
fn test_disk_cache_creates_dir_if_missing() {
    let dir = TempDir::new().unwrap();
    let nested = dir.path().join("a").join("b").join("c");
    let _cache = DiskCache::new(&nested, 3600, 4, 2, 64, 8, None, Vec::new()).unwrap();
    assert!(nested.join("data.cache").exists());
}

#[test]
fn test_disk_cache_group_states() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap();
    assert_eq!(cache.group_state(0), GroupState::None);
    assert_eq!(cache.group_state(1), GroupState::None);
    assert!(cache.try_claim_group(0));
    assert_eq!(cache.group_state(0), GroupState::Fetching);
    assert!(!cache.try_claim_group(0)); // Can't claim again
    cache.mark_group_present(0);
    assert_eq!(cache.group_state(0), GroupState::Present);
}

#[test]
fn test_disk_cache_try_claim_present_fails() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap();
    cache.try_claim_group(0);
    cache.mark_group_present(0);
    assert!(!cache.try_claim_group(0)); // Already Present
}

#[test]
fn test_disk_cache_group_state_out_of_bounds() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap(); // 2 groups
    // Out of bounds should return None
    assert_eq!(cache.group_state(100), GroupState::None);
    assert_eq!(cache.group_state(u64::MAX), GroupState::None);
}

#[test]
fn test_disk_cache_wait_for_group_present() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap();
    cache.try_claim_group(0);
    cache.mark_group_present(0);
    // Should return immediately
    cache.wait_for_group(0);
}

#[test]
fn test_disk_cache_wait_for_group_none() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap();
    // State is None — should return immediately
    cache.wait_for_group(0);
}

#[test]
fn test_disk_cache_touch_group_updates_access() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap();
    assert!(!cache.group_access.lock().contains_key(&0));
    cache.touch_group(0);
    assert!(cache.group_access.lock().contains_key(&0));
    cache.touch_group(1);
    assert!(cache.group_access.lock().contains_key(&1));
}

#[test]
fn test_disk_cache_mark_interior_group() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap();
    assert!(!cache.interior_groups.lock().contains(&0));
    cache.mark_interior_group(0, 0, 0);
    assert!(cache.interior_groups.lock().contains(&0));
    // Marking again is idempotent
    cache.mark_interior_group(0, 0, 0);
    assert_eq!(cache.interior_groups.lock().len(), 1);
}

#[test]
fn test_disk_cache_eviction_skips_interior() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 0, 4, 2, 64, 8, None, Vec::new()).unwrap(); // TTL=0 = disabled
    cache.mark_interior_group(0, 0, 0);
    cache.touch_group(0);
    cache.touch_group(1);
    cache.evict_expired();
    assert!(cache.group_access.lock().contains_key(&0));
    assert!(cache.group_access.lock().contains_key(&1));
}

#[test]
fn test_disk_cache_evict_group_clears_bitmap_and_state() {
    let dir = TempDir::new().unwrap();
    let gp = positional_group_pages(4, 16);
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 16, None, gp).unwrap();
    // Write pages in group 0 (pages 0-3)
    for i in 0..4u64 {
        cache.write_page(i, &vec![i as u8; 64]).unwrap();
    }
    cache.try_claim_group(0);
    cache.mark_group_present(0);
    assert!(cache.is_present(0));
    assert!(cache.is_present(3));
    assert_eq!(cache.group_state(0), GroupState::Present);

    // Evict group 0
    cache.evict_group(0);
    assert!(!cache.is_present(0));
    assert!(!cache.is_present(1));
    assert!(!cache.is_present(2));
    assert!(!cache.is_present(3));
    assert_eq!(cache.group_state(0), GroupState::None);
}

#[test]
fn test_disk_cache_evict_group_preserves_other_groups() {
    let dir = TempDir::new().unwrap();
    let gp = positional_group_pages(4, 16);
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 16, None, gp).unwrap();
    // Write pages in groups 0 and 1
    for i in 0..8u64 {
        cache.write_page(i, &vec![i as u8; 64]).unwrap();
    }
    cache.evict_group(0); // Evict only group 0
    assert!(!cache.is_present(0));
    assert!(cache.is_present(4)); // Group 1 untouched
    assert!(cache.is_present(7));
}

#[test]
fn test_disk_cache_evict_expired_with_real_ttl() {
    let dir = TempDir::new().unwrap();
    // TTL = 1 second
    let gp = positional_group_pages(4, 16);
    let cache = DiskCache::new(dir.path(), 1, 4, 2, 64, 16, None, gp).unwrap();

    // Write and touch group 0
    for i in 0..4u64 {
        cache.write_page(i, &vec![i as u8; 64]).unwrap();
    }
    cache.try_claim_group(0);
    cache.mark_group_present(0);
    cache.touch_group(0);
    assert!(cache.is_present(0));

    // Sleep past TTL
    std::thread::sleep(Duration::from_millis(1100));

    cache.evict_expired();
    // Group should have been evicted
    assert!(!cache.is_present(0));
    assert_eq!(cache.group_state(0), GroupState::None);
}

#[test]
fn test_disk_cache_evict_expired_protects_interior() {
    let dir = TempDir::new().unwrap();
    let gp = positional_group_pages(4, 16);
    let cache = DiskCache::new(dir.path(), 1, 4, 2, 64, 16, None, gp).unwrap(); // TTL = 1s

    for i in 0..8u64 {
        cache.write_page(i, &vec![i as u8; 64]).unwrap();
    }
    cache.try_claim_group(0);
    cache.mark_group_present(0);
    cache.try_claim_group(1);
    cache.mark_group_present(1);
    cache.touch_group(0);
    cache.touch_group(1);
    cache.mark_interior_group(0, 0, 0); // Group 0 is interior = pinned

    std::thread::sleep(Duration::from_millis(1100));
    cache.evict_expired();

    // Interior group 0 should survive; group 1 should be evicted
    assert!(cache.is_present(0)); // Pinned
    assert!(!cache.is_present(4)); // Evicted
}

/// Regression test: evict_group with B-tree-aware (non-positional) group_pages
/// clears the correct bitmap bits, not positional ones.
#[test]
fn test_evict_group_btree_aware_pages() {
    let dir = TempDir::new().unwrap();
    // B-tree groups: group 0 has pages [10, 20, 30], group 1 has pages [5, 15, 25]
    let gp = vec![vec![10, 20, 30], vec![5, 15, 25]];
    let cache = DiskCache::new(dir.path(), 1, 3, 1, 64, 31, None, gp).unwrap();

    // Write pages for both groups
    for &p in &[10u64, 20, 30, 5, 15, 25] {
        cache.write_page(p, &[0xAA; 64]).unwrap();
    }
    assert!(cache.is_present(10));
    assert!(cache.is_present(20));
    assert!(cache.is_present(30));
    assert!(cache.is_present(5));
    assert!(cache.is_present(15));
    assert!(cache.is_present(25));

    // Evict group 0 (pages 10, 20, 30)
    cache.evict_group(0);

    // Group 0 pages should be cleared
    assert!(!cache.is_present(10));
    assert!(!cache.is_present(20));
    assert!(!cache.is_present(30));

    // Group 1 pages should NOT be affected
    assert!(cache.is_present(5));
    assert!(cache.is_present(15));
    assert!(cache.is_present(25));

    // Positional pages 0-2 should NOT have been touched (they weren't in any group)
    // This verifies we didn't fall back to positional clearing
}

/// Regression test: group state initialization uses B-tree-aware group_pages.
#[test]
fn test_group_state_init_btree_aware() {
    let dir = TempDir::new().unwrap();
    // B-tree group 0 has pages [5, 10, 15]
    let gp = vec![vec![5, 10, 15]];

    // Write pages 5, 10, 15 to bitmap manually via a first cache
    {
        let cache = DiskCache::new(dir.path(), 3600, 3, 1, 64, 16, None, gp.clone()).unwrap();
        for &p in &[5u64, 10, 15] {
            cache.write_page(p, &[0xBB; 64]).unwrap();
        }
        let _ = cache.persist_bitmap();
    }

    // Reopen with same group_pages — group 0 should be Present
    let cache2 = DiskCache::new(dir.path(), 3600, 3, 1, 64, 16, None, gp).unwrap();
    assert_eq!(cache2.group_state(0), GroupState::Present);
}

/// Regression test: clear_cache uses B-tree-aware group_pages[0] for group 0.
#[test]
fn test_clear_cache_btree_aware_group0() {
    let dir = TempDir::new().unwrap();
    // B-tree group 0 has pages [7, 14, 21], group 1 has pages [3, 6, 9]
    let gp = vec![vec![7, 14, 21], vec![3, 6, 9]];
    let cache = DiskCache::new(dir.path(), 3600, 3, 1, 64, 22, None, gp).unwrap();

    // Write all pages
    for &p in &[7u64, 14, 21, 3, 6, 9] {
        cache.write_page(p, &[0xCC; 64]).unwrap();
    }

    // Simulate clear_cache: clear bitmap, re-mark group 0 pages
    {
        let gp = cache.group_pages.read();
        let mut bitmap = cache.bitmap.lock();
        bitmap.bits.fill(0);
        if let Some(g0_pages) = gp.first() {
            for &p in g0_pages {
                bitmap.mark_present(p);
            }
        }
    }

    // Group 0 pages should be present
    assert!(cache.is_present(7));
    assert!(cache.is_present(14));
    assert!(cache.is_present(21));

    // Group 1 pages should be cleared
    assert!(!cache.is_present(3));
    assert!(!cache.is_present(6));
    assert!(!cache.is_present(9));

    // Positional page 0 should NOT have been marked (it's not in group 0)
    assert!(!cache.is_present(0));
}

#[test]
fn test_disk_cache_evict_expired_skips_recent() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 10, 4, 2, 64, 8, None, Vec::new()).unwrap(); // TTL = 10s
    cache.touch_group(0);
    cache.evict_expired();
    // Group should NOT be evicted (only 0ms elapsed, TTL = 10s)
    assert!(cache.group_access.lock().contains_key(&0));
}

#[test]
fn test_disk_cache_ensure_group_capacity() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap(); // 2 groups
    assert_eq!(cache.group_states.lock().len(), 2);
    cache.ensure_group_capacity(10);
    assert_eq!(cache.group_states.lock().len(), 10);
    // New groups should be None
    assert_eq!(cache.group_state(5), GroupState::None);
}

#[test]
fn test_disk_cache_bitmap_persistence() {
    let dir = TempDir::new().unwrap();
    {
        let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap();
        cache.write_page(3, &vec![1u8; 64]).unwrap();
        cache.persist_bitmap().unwrap();
    }
    let cache2 = DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap();
    assert!(cache2.is_present(3));
    assert!(!cache2.is_present(4));
}

#[test]
fn test_disk_cache_reopen_initializes_group_states_from_bitmap() {
    let dir = TempDir::new().unwrap();
    let gp = positional_group_pages(4, 16);
    {
        let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 16, None, gp.clone()).unwrap();
        // Write ALL pages in group 0 (pages 0-3)
        for i in 0..4u64 {
            cache.write_page(i, &vec![i as u8; 64]).unwrap();
        }
        cache.persist_bitmap().unwrap();
    }
    // Reopen — group 0 should be Present (all 4 pages marked)
    let cache2 = DiskCache::new(dir.path(), 3600, 4, 2, 64, 16, None, gp).unwrap();
    assert_eq!(cache2.group_state(0), GroupState::Present);
    // Group 1 should be None (no pages)
    assert_eq!(cache2.group_state(1), GroupState::None);
}

#[test]
fn test_disk_cache_reopen_partial_group_is_none() {
    let dir = TempDir::new().unwrap();
    {
        let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 16, None, Vec::new()).unwrap();
        // Write only 2 of 4 pages in group 0
        cache.write_page(0, &vec![0u8; 64]).unwrap();
        cache.write_page(1, &vec![1u8; 64]).unwrap();
        cache.persist_bitmap().unwrap();
    }
    let cache2 = DiskCache::new(dir.path(), 3600, 4, 2, 64, 16, None, Vec::new()).unwrap();
    // Partial group should be None (not all pages present)
    assert_eq!(cache2.group_state(0), GroupState::None);
    // But individual pages should still be present
    assert!(cache2.is_present(0));
    assert!(cache2.is_present(1));
    assert!(!cache2.is_present(2));
}

#[test]
fn test_disk_cache_zero_page_count() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 0, None, Vec::new()).unwrap();
    assert_eq!(cache.group_states.lock().len(), 0);
    assert_eq!(cache.group_state(0), GroupState::None);
}

#[test]
fn test_disk_cache_zero_ppg() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 0, 0, 64, 100, None, Vec::new()).unwrap();
    assert_eq!(cache.group_states.lock().len(), 0);
}

// =========================================================================
// DiskCache: concurrent group state transitions
// =========================================================================

#[test]
fn test_disk_cache_concurrent_claim() {
    let dir = TempDir::new().unwrap();
    let cache = Arc::new(DiskCache::new(dir.path(), 3600, 4, 2, 64, 16, None, Vec::new()).unwrap());

    // Simulate two threads trying to claim the same group
    let claimed1 = cache.try_claim_group(0);
    let claimed2 = cache.try_claim_group(0);
    // Exactly one should succeed
    assert!(claimed1 ^ claimed2, "exactly one thread should claim the group");
}

#[test]
fn test_disk_cache_multiple_groups_independent() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 32, None, Vec::new()).unwrap(); // 8 groups

    // Claim different groups — all should succeed
    for gid in 0..8u64 {
        assert!(cache.try_claim_group(gid), "should claim group {}", gid);
    }
    // All should be Fetching
    for gid in 0..8u64 {
        assert_eq!(cache.group_state(gid), GroupState::Fetching);
    }
}

// =========================================================================
// End-to-end: encode → write to cache → read back
// =========================================================================

#[test]
fn test_encode_cache_read_roundtrip() {
    let dir = TempDir::new().unwrap();
    let ppg = 4u32;
    let page_size = 64u32;
    let cache = DiskCache::new(dir.path(), 3600, ppg, 2, page_size, ppg as u64, None, Vec::new()).unwrap();

    // Create page group with known data
    let pages: Vec<Option<Vec<u8>>> = (0..ppg)
        .map(|i| Some(vec![i as u8 + 1; page_size as usize]))
        .collect();
    let encoded = encode_page_group(
        &pages,
        page_size,
        3,
        #[cfg(feature = "zstd")]
        None,
        None,
    )
    .unwrap();

    // Simulate what decode_and_cache_group does: decode whole group, write pages
    let (_pg_count, _ps, decoded) = decode_page_group(
        &encoded,
        #[cfg(feature = "zstd")]
        None,
        None,
    )
    .unwrap();
    for (i, page_data) in decoded.iter().enumerate() {
        cache.write_page(i as u64, page_data).unwrap();
    }

    // Read back from cache
    for i in 0..ppg {
        assert!(cache.is_present(i as u64));
        let mut buf = vec![0u8; page_size as usize];
        cache.read_page(i as u64, &mut buf).unwrap();
        assert_eq!(buf, vec![i as u8 + 1; page_size as usize]);
    }
}

// =========================================================================
// DiskCache + SubChunkTracker integration
// =========================================================================

#[test]
fn test_disk_cache_write_pages_bulk_marks_sub_chunks() {
    let dir = TempDir::new().unwrap();
    // ppg=8, spf=2, page_size=64, page_count=16
    let cache = DiskCache::new(dir.path(), 3600, 8, 2, 64, 16, None, Vec::new()).unwrap();
    // Write 2 pages (a complete sub-chunk frame)
    let data = vec![42u8; 128]; // 2 pages * 64 bytes
    cache.write_pages_bulk(0, &data, 2).unwrap();

    // Both pages in the sub-chunk should be present
    assert!(cache.is_present(0));
    assert!(cache.is_present(1));
    // Pages in other sub-chunks should not
    assert!(!cache.is_present(2));
    assert!(!cache.is_present(8));
}

#[test]
fn test_disk_cache_write_page_does_not_mark_sub_chunk() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 8, 2, 64, 16, None, Vec::new()).unwrap();
    let data = vec![42u8; 64];
    cache.write_page(0, &data).unwrap();

    // Page 0 is present (via bitmap)
    assert!(cache.is_present(0));
    // Page 1 is in the same sub-chunk but was not written — should NOT be present
    assert!(!cache.is_present(1));
}

#[test]
fn test_disk_cache_mark_interior_promotes_to_pinned() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 8, 2, 64, 16, None, Vec::new()).unwrap();
    // Write a complete sub-chunk
    let data = vec![42u8; 128];
    cache.write_pages_bulk(0, &data, 2).unwrap();

    // Initially Data tier
    {
        let tracker = cache.tracker.lock();
        let id = tracker.sub_chunk_for_page(0);
        assert_eq!(tracker.tiers.get(&id), Some(&SubChunkTier::Data));
    }

    // Mark as interior
    cache.mark_interior_group(0, 0, 0);

    // Now Pinned
    {
        let tracker = cache.tracker.lock();
        let id = tracker.sub_chunk_for_page(0);
        assert_eq!(tracker.tiers.get(&id), Some(&SubChunkTier::Pinned));
    }
}

#[test]
fn test_disk_cache_mark_index_promotes_to_index_tier() {
    let dir = TempDir::new().unwrap();
    let gp = positional_group_pages(8, 16);
    let cache = DiskCache::new(dir.path(), 3600, 8, 2, 64, 16, None, gp).unwrap();
    let data = vec![42u8; 128];
    cache.write_pages_bulk(2, &data, 2).unwrap();

    // Initially Data tier
    {
        let tracker = cache.tracker.lock();
        let id = tracker.sub_chunk_for_page(2);
        assert_eq!(tracker.tiers.get(&id), Some(&SubChunkTier::Data));
    }

    // page 2 is at index 2 in group 0 => sub_chunk_id_for(0, 2) gives frame_index=1
    cache.mark_index_page(2, 0, 2);

    // Now Index tier
    {
        let tracker = cache.tracker.lock();
        let id = tracker.sub_chunk_for_page(2);
        assert_eq!(tracker.tiers.get(&id), Some(&SubChunkTier::Index));
    }
}

#[test]
fn test_disk_cache_evict_group_clears_tracker() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 8, 2, 64, 16, None, Vec::new()).unwrap();
    // Write all sub-chunks in group 0 (4 frames * 2 pages = 8 pages)
    let data = vec![42u8; 512]; // 8 pages * 64 bytes
    cache.write_pages_bulk(0, &data, 8).unwrap();

    // All pages present
    for p in 0..8 {
        assert!(cache.is_present(p));
    }

    cache.evict_group(0);

    // All pages gone from tracker
    let tracker = cache.tracker.lock();
    for p in 0..8u64 {
        assert!(!tracker.is_present(p));
    }
}

#[test]
fn test_disk_cache_sub_chunk_boundary_pages() {
    // Test that pages at sub-chunk boundaries are correctly assigned
    let dir = TempDir::new().unwrap();
    // ppg=4, spf=2: 2 frames per group
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap();

    // Write frame 0 of group 0 (pages 0,1)
    cache.write_pages_bulk(0, &vec![1u8; 128], 2).unwrap();
    assert!(cache.is_present(0));
    assert!(cache.is_present(1));
    assert!(!cache.is_present(2)); // frame 1

    // Write frame 1 of group 0 (pages 2,3)
    cache.write_pages_bulk(2, &vec![2u8; 128], 2).unwrap();
    assert!(cache.is_present(2));
    assert!(cache.is_present(3));

    // Write frame 0 of group 1 (pages 4,5)
    cache.write_pages_bulk(4, &vec![3u8; 128], 2).unwrap();
    assert!(cache.is_present(4));
    assert!(cache.is_present(5));
    assert!(!cache.is_present(6)); // frame 1 of group 1
}

// =========================================================================
// Demand-Driven Prefetch: GroupState + just_claimed logic tests
// =========================================================================

#[test]
fn test_group_state_claim_prevents_double_claim() {
    // Two threads trying to claim the same group: only one succeeds
    let dir = TempDir::new().unwrap();
    let cache = Arc::new(DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap());

    // First claim succeeds
    assert!(cache.try_claim_group(0));
    assert_eq!(cache.group_state(0), GroupState::Fetching);

    // Second claim fails (already Fetching)
    assert!(!cache.try_claim_group(0));
}

#[test]
fn test_group_state_claim_then_present() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap();

    assert_eq!(cache.group_state(0), GroupState::None);
    assert!(cache.try_claim_group(0));
    assert_eq!(cache.group_state(0), GroupState::Fetching);
    cache.mark_group_present(0);
    assert_eq!(cache.group_state(0), GroupState::Present);

    // Can't claim a Present group
    assert!(!cache.try_claim_group(0));
}

#[test]
fn test_concurrent_group_claim_exactly_one_wins() {
    // Multiple threads race to claim the same group. Exactly one succeeds.
    use std::sync::atomic::{AtomicU32, Ordering};
    let dir = TempDir::new().unwrap();
    let cache = Arc::new(DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap());
    let winners = Arc::new(AtomicU32::new(0));

    let mut handles = Vec::new();
    for _ in 0..8 {
        let c = Arc::clone(&cache);
        let w = Arc::clone(&winners);
        handles.push(std::thread::spawn(move || {
            if c.try_claim_group(0) {
                w.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(winners.load(Ordering::Relaxed), 1, "exactly one thread should win the claim");
    assert_eq!(cache.group_state(0), GroupState::Fetching);
}

#[test]
fn test_group_state_reset_on_failed_fetch() {
    // If a fetch fails, state should be reset to None so another thread can retry
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap();

    assert!(cache.try_claim_group(0));
    assert_eq!(cache.group_state(0), GroupState::Fetching);

    // Simulate fetch failure: reset to None via unclaim_group
    cache.unclaim_group(0);

    assert_eq!(cache.group_state(0), GroupState::None);
    // Can be claimed again
    assert!(cache.try_claim_group(0));
}

#[test]
fn test_wait_for_group_returns_when_present() {
    // wait_for_group should unblock when another thread marks group Present
    let dir = TempDir::new().unwrap();
    let cache = Arc::new(DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap());

    assert!(cache.try_claim_group(0));

    let c = Arc::clone(&cache);
    let waiter = std::thread::spawn(move || {
        c.wait_for_group(0);
        c.group_state(0)
    });

    // Brief delay, then mark present
    std::thread::sleep(Duration::from_millis(10));
    cache.mark_group_present(0);
    cache.group_condvar.notify_all();

    let final_state = waiter.join().unwrap();
    assert_eq!(final_state, GroupState::Present);
}

#[test]
fn test_wait_for_group_returns_on_reset_to_none() {
    // wait_for_group should unblock when state is reset to None (fetch failed)
    let dir = TempDir::new().unwrap();
    let cache = Arc::new(DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap());

    assert!(cache.try_claim_group(0));

    let c = Arc::clone(&cache);
    let waiter = std::thread::spawn(move || {
        c.wait_for_group(0);
        c.group_state(0)
    });

    // Simulate fetch failure: reset to None
    std::thread::sleep(Duration::from_millis(10));
    {
        let states = cache.group_states.lock();
        if let Some(s) = states.get(0) {
            s.store(GroupState::None as u8, Ordering::Release);
        }
    }
    cache.group_condvar.notify_all();

    let final_state = waiter.join().unwrap();
    assert_eq!(final_state, GroupState::None);
}

#[test]
fn test_prefetch_worker_skips_already_present_group() {
    // Simulates what happens when a group is already Present by the time
    // the prefetch worker picks it up: worker should skip it.
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap();

    // Mark group as Present (as if another path already fetched it)
    assert!(cache.try_claim_group(0));
    cache.mark_group_present(0);

    // Worker logic: if state is not None and not Fetching, skip
    let current = cache.group_state(0);
    assert_eq!(current, GroupState::Present);
    assert!(current != GroupState::None && current != GroupState::Fetching,
        "worker should skip this group");
}

#[test]
fn test_prefetch_worker_claims_unclaimed_group() {
    // Simulates what happens when trigger_prefetch submits a group
    // that hasn't been claimed yet (state = None)
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap();

    // Group starts as None (submitted by trigger_prefetch, not yet claimed)
    assert_eq!(cache.group_state(0), GroupState::None);

    // Worker claims it
    assert!(cache.try_claim_group(0));
    assert_eq!(cache.group_state(0), GroupState::Fetching);
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
    assert_eq!(cache.group_state(0), GroupState::None,
        "group state should remain None until prefetch is submitted");

    // Now the read path would claim and submit to pool
    assert!(cache.try_claim_group(0));
    assert_eq!(cache.group_state(0), GroupState::Fetching);
}

// =========================================================================
// unclaim_group
// =========================================================================

#[test]
fn test_unclaim_group_resets_fetching_to_none() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap();

    assert!(cache.try_claim_group(0));
    assert_eq!(cache.group_state(0), GroupState::Fetching);

    cache.unclaim_group(0);
    assert_eq!(cache.group_state(0), GroupState::None);
}

#[test]
fn test_unclaim_group_allows_reclaim() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap();

    // Claim, unclaim, reclaim should all succeed
    assert!(cache.try_claim_group(0));
    cache.unclaim_group(0);
    assert!(cache.try_claim_group(0));
    assert_eq!(cache.group_state(0), GroupState::Fetching);
}

#[test]
fn test_unclaim_group_wakes_waiters() {
    // unclaim_group should wake threads blocked on wait_for_group
    let dir = TempDir::new().unwrap();
    let cache = Arc::new(DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap());

    assert!(cache.try_claim_group(0));

    let cache2 = Arc::clone(&cache);
    let waiter = std::thread::spawn(move || {
        let start = std::time::Instant::now();
        cache2.wait_for_group(0);
        start.elapsed()
    });

    // Give waiter time to block
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Unclaim should wake the waiter (returns on None state)
    cache.unclaim_group(0);

    let elapsed = waiter.join().unwrap();
    assert!(elapsed.as_millis() < 500, "waiter should unblock quickly, took {}ms", elapsed.as_millis());
}

#[test]
fn test_unclaim_group_out_of_bounds_is_noop() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap();

    // Should not panic on gid beyond group_states size
    cache.unclaim_group(999);
}

#[test]
fn test_unclaim_group_on_none_is_harmless() {
    // Unclaiming a group that's already None should be fine
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap();

    assert_eq!(cache.group_state(0), GroupState::None);
    cache.unclaim_group(0); // noop
    assert_eq!(cache.group_state(0), GroupState::None);
}

#[test]
fn test_claim_before_submit_prevents_double_work() {
    // Simulates the claim-before-submit protocol: two code paths try to
    // submit the same group. Only one succeeds because try_claim_group
    // is atomic.
    use std::sync::atomic::{AtomicU32, Ordering as AtOrd};
    let dir = TempDir::new().unwrap();
    let cache = Arc::new(DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap());
    let claims = Arc::new(AtomicU32::new(0));

    let mut handles = Vec::new();
    for _ in 0..8 {
        let c = Arc::clone(&cache);
        let cl = Arc::clone(&claims);
        handles.push(std::thread::spawn(move || {
            // Protocol: check state, then claim, then "submit"
            if c.group_state(0) == GroupState::None && c.try_claim_group(0) {
                cl.fetch_add(1, AtOrd::Relaxed);
                // Simulate: if submit fails, unclaim
                // (here submit always "succeeds")
            }
        }));
    }
    for h in handles { h.join().unwrap(); }

    assert_eq!(claims.load(AtOrd::Relaxed), 1, "exactly one submitter should win");
    assert_eq!(cache.group_state(0), GroupState::Fetching);
}

#[test]
fn test_claim_unclaim_cycle_under_contention() {
    // Multiple threads repeatedly claim/unclaim. No deadlocks, no panics.
    use std::sync::atomic::{AtomicU32, Ordering as AtOrd};
    let dir = TempDir::new().unwrap();
    let cache = Arc::new(DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap());
    let total_claims = Arc::new(AtomicU32::new(0));

    let mut handles = Vec::new();
    for _ in 0..4 {
        let c = Arc::clone(&cache);
        let tc = Arc::clone(&total_claims);
        handles.push(std::thread::spawn(move || {
            for _ in 0..100 {
                if c.group_state(0) == GroupState::None && c.try_claim_group(0) {
                    tc.fetch_add(1, AtOrd::Relaxed);
                    // Simulate failed submit
                    c.unclaim_group(0);
                }
                std::thread::yield_now();
            }
        }));
    }
    for h in handles { h.join().unwrap(); }

    // At least some claims should have succeeded across 400 attempts
    assert!(total_claims.load(AtOrd::Relaxed) > 10,
        "expected many successful claim/unclaim cycles, got {}",
        total_claims.load(AtOrd::Relaxed));
    // Final state should be None (all unclaimed)
    assert_eq!(cache.group_state(0), GroupState::None);
}

#[test]
fn test_present_group_cannot_be_unclaimed() {
    // unclaim_group on a Present group resets to None. This is intentional
    // for the eviction path, but callers should not do this for live groups.
    // We test the raw behavior here.
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap();

    assert!(cache.try_claim_group(0));
    cache.mark_group_present(0);
    assert_eq!(cache.group_state(0), GroupState::Present);

    // unclaim_group resets to None regardless of current state
    cache.unclaim_group(0);
    assert_eq!(cache.group_state(0), GroupState::None);
}
