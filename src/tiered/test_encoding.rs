use super::*;
use crate::tiered::*;
use tempfile::TempDir;

// =========================================================================
// Page Group Encoding
// =========================================================================

/// Helper: encode a page group with default compression settings (level 3, no dict).
fn test_encode(pages: &[Option<Vec<u8>>], page_size: u32) -> Vec<u8> {
    encode_page_group(
        pages,
        page_size,
        3,
        #[cfg(feature = "zstd")]
        None,
        None,
    )
    .unwrap()
}

/// Helper: decode a page group with no dict.
fn test_decode(compressed: &[u8]) -> (u32, u32, Vec<Vec<u8>>) {
    decode_page_group(
        compressed,
        #[cfg(feature = "zstd")]
        None,
        None,
    )
    .unwrap()
}

#[test]
fn test_encode_decode_page_group_roundtrip() {
    let page_size = 64u32;
    let mut pages: Vec<Option<Vec<u8>>> = vec![None; 8];
    pages[0] = Some(vec![1; page_size as usize]);
    pages[3] = Some(vec![4; page_size as usize]);
    pages[7] = Some(vec![8; page_size as usize]);

    let encoded = test_encode(&pages, page_size);
    let (pg_count, ps, decoded) = test_decode(&encoded);

    assert_eq!(ps, page_size);
    assert_eq!(pg_count, 8); // last non-None is index 7 → 8 pages
    assert_eq!(decoded[0], vec![1u8; page_size as usize]);
    assert_eq!(decoded[3], vec![4u8; page_size as usize]);
    assert_eq!(decoded[7], vec![8u8; page_size as usize]);
    // Empty slots are zero-filled
    assert_eq!(decoded[1], vec![0u8; page_size as usize]);
}

#[test]
fn test_encode_empty_page_group() {
    let page_size = 64u32;
    let pages: Vec<Option<Vec<u8>>> = vec![None; 4];
    let encoded = test_encode(&pages, page_size);
    let (pg_count, ps, decoded) = test_decode(&encoded);

    assert_eq!(pg_count, 0);
    assert_eq!(ps, page_size);
    assert!(decoded.is_empty());
}

#[test]
fn test_encode_all_pages_filled() {
    let page_size = 100u32;
    let pages: Vec<Option<Vec<u8>>> = (0..4u32)
        .map(|i| Some(vec![i as u8; page_size as usize]))
        .collect();
    let encoded = test_encode(&pages, page_size);
    let (pg_count, _ps, decoded) = test_decode(&encoded);

    assert_eq!(pg_count, 4);
    for i in 0..4 {
        assert_eq!(decoded[i], vec![i as u8; page_size as usize]);
    }
}

#[test]
fn test_encode_single_page_filled() {
    let page_size = 50u32;
    let mut pages: Vec<Option<Vec<u8>>> = vec![None; 4];
    pages[2] = Some(vec![0xAB; page_size as usize]);
    let encoded = test_encode(&pages, page_size);
    let (pg_count, _ps, decoded) = test_decode(&encoded);

    // page_count = 3 (last non-None is index 2)
    assert_eq!(pg_count, 3);
    assert_eq!(decoded[0], vec![0u8; page_size as usize]); // zero-filled
    assert_eq!(decoded[1], vec![0u8; page_size as usize]); // zero-filled
    assert_eq!(decoded[2], vec![0xABu8; page_size as usize]);
}

#[test]
fn test_encode_page_group_large_data() {
    let page_size = 1_000_000u32; // 1MB page
    let big_page = vec![0xFFu8; page_size as usize];
    let pages = vec![Some(big_page.clone()), None];
    let encoded = test_encode(&pages, page_size);
    let (pg_count, _ps, decoded) = test_decode(&encoded);

    assert_eq!(pg_count, 1);
    assert_eq!(decoded[0].len(), page_size as usize);
    assert_eq!(decoded[0], big_page);
}

#[test]
fn test_decode_truncated_data() {
    // Too short to even have the header
    let short = vec![0u8; 3];
    let result = decode_page_group(
        &short,
        #[cfg(feature = "zstd")]
        None,
        None,
    );
    assert!(result.is_err());
}

#[test]
fn test_decode_empty_data() {
    let result = decode_page_group(
        &[],
        #[cfg(feature = "zstd")]
        None,
        None,
    );
    assert!(result.is_err());
}

#[test]
fn test_encode_decode_roundtrip_various_page_sizes() {
    for page_size in [64u32, 256, 1024, 4096] {
        let pages: Vec<Option<Vec<u8>>> = (0..4u32)
            .map(|i| {
                if i % 2 == 0 {
                    Some(vec![i as u8; page_size as usize])
                } else {
                    None
                }
            })
            .collect();
        let encoded = test_encode(&pages, page_size);
        let (pg_count, ps, decoded) = test_decode(&encoded);

        assert_eq!(ps, page_size, "page_size mismatch for {}", page_size);
        // Last non-None is index 2, so pg_count = 3
        assert_eq!(pg_count, 3, "pg_count mismatch for {}", page_size);
        assert_eq!(decoded[0], vec![0u8; page_size as usize]);
        assert_eq!(decoded[1], vec![0u8; page_size as usize]); // None → zero
        assert_eq!(decoded[2], vec![2u8; page_size as usize]);
    }
}

// =========================================================================
// Page Type Detection
// =========================================================================

#[test]
fn test_page_type_detection() {
    let check = |buf: &[u8], page_num: u64| -> bool {
        let type_byte = if page_num == 0 { buf.get(100) } else { buf.get(0) };
        matches!(type_byte, Some(&0x05) | Some(&0x02))
    };
    // Table interior (0x05)
    assert!(check(&[0x05u8; 4096], 1));
    // Index interior (0x02)
    assert!(check(&[0x02u8; 4096], 1));
    // Table leaf (0x0d)
    assert!(!check(&[0x0du8; 4096], 1));
    // Index leaf (0x0a)
    assert!(!check(&[0x0au8; 4096], 1));
    // Zero page
    assert!(!check(&[0x00u8; 4096], 1));
}

#[test]
fn test_page_type_page_zero_offset_100() {
    let check = |buf: &[u8], page_num: u64| -> bool {
        let type_byte = if page_num == 0 { buf.get(100) } else { buf.get(0) };
        matches!(type_byte, Some(&0x05) | Some(&0x02))
    };
    let mut page0 = vec![0u8; 4096];
    page0[100] = 0x05;
    assert!(check(&page0, 0));
    page0[100] = 0x02;
    assert!(check(&page0, 0));
    page0[100] = 0x0d;
    assert!(!check(&page0, 0));
    // Byte at offset 0 should NOT be checked for page 0
    page0[0] = 0x05;
    page0[100] = 0x0d;
    assert!(!check(&page0, 0));
}

// =========================================================================
// Seekable Encode/Decode
// =========================================================================

#[test]
fn test_seekable_encode_decode_roundtrip() {
    let page_size = 4096u32;
    let sub_ppg = 4u32; // 4 pages per sub-chunk
    let total_pages = 10;

    // Create test pages with recognizable content
    let mut pages: Vec<Option<Vec<u8>>> = Vec::new();
    for i in 0..total_pages {
        let mut page = vec![0u8; page_size as usize];
        page[0] = (i + 1) as u8; // marker byte
        page[page_size as usize - 1] = (i + 100) as u8; // tail marker
        pages.push(Some(page));
    }

    // Encode as seekable
    let (blob, frame_table) = encode_page_group_seekable(
        &pages,
        page_size,
        sub_ppg,
        3, // compression level
        #[cfg(feature = "zstd")]
        None,
        None,
    )
    .expect("encode seekable");

    // Should have ceil(10/4) = 3 frames
    assert_eq!(frame_table.len(), 3);

    // Full decode should produce identical pages
    let (pg_count, pg_size, full_data) = decode_page_group_seekable_full(
        &blob,
        &frame_table,
        page_size,
        total_pages as u32, // ppg = total_pages for this test
        total_pages as u64,
        0,
        #[cfg(feature = "zstd")]
        None,
        None,
    )
    .expect("full decode");
    assert_eq!(pg_count, total_pages as u32);
    assert_eq!(pg_size, page_size);

    for i in 0..total_pages {
        let start = i * page_size as usize;
        assert_eq!(full_data[start], (i + 1) as u8, "page {} marker", i);
        assert_eq!(
            full_data[start + page_size as usize - 1],
            (i + 100) as u8,
            "page {} tail",
            i,
        );
    }

    // Sub-chunk decode: fetch frame 1 (pages 4-7)
    let entry = &frame_table[1];
    let frame_bytes = &blob[entry.offset as usize..(entry.offset as usize + entry.len as usize)];
    let subchunk = decode_seekable_subchunk(
        frame_bytes,
        #[cfg(feature = "zstd")]
        None,
        None,
    )
    .expect("subchunk decode");

    // Should contain 4 pages (pages 4,5,6,7)
    assert_eq!(subchunk.len(), 4 * page_size as usize);
    for j in 0..4 {
        let i = 4 + j; // global page index
        let start = j * page_size as usize;
        assert_eq!(subchunk[start], (i + 1) as u8, "subchunk page {} marker", j);
    }

    // Sub-chunk decode: fetch frame 2 (pages 8-9, partial)
    let entry2 = &frame_table[2];
    let frame_bytes2 = &blob[entry2.offset as usize..(entry2.offset as usize + entry2.len as usize)];
    let subchunk2 = decode_seekable_subchunk(
        frame_bytes2,
        #[cfg(feature = "zstd")]
        None,
        None,
    )
    .expect("subchunk2 decode");
    // Last frame has 2 pages
    assert_eq!(subchunk2.len(), 2 * page_size as usize);
    assert_eq!(subchunk2[0], 9u8); // page 8 marker (8+1=9)
    assert_eq!(subchunk2[page_size as usize], 10u8); // page 9 marker (9+1=10)
}

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
                FrameEntry { offset: 0, len: 500 },
                FrameEntry { offset: 500, len: 600 },
            ],
            vec![
                FrameEntry { offset: 0, len: 450 },
                FrameEntry { offset: 450, len: 550 },
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

// ===== is_valid_btree_page tests =====

#[test]
fn test_valid_btree_page_real_index_leaf() {
    // Simulate a real 0x0A (leaf index) page: type=0x0A, cell_count=5, content_area=3950
    let mut page = vec![0u8; 4096];
    page[0] = 0x0A; // type
    page[1] = 0; page[2] = 0; // freeblock = 0
    page[3] = 0; page[4] = 5; // cell_count = 5
    page[5] = 0x0F; page[6] = 0x6E; // content_area = 3950
    page[7] = 0; // frag = 0
    assert!(is_valid_btree_page(&page, 0));
}

#[test]
fn test_valid_btree_page_overflow_false_positive() {
    // Overflow page: first 4 bytes are next-page pointer, rest is payload.
    // If the pointer happens to start with 0x0A, the type byte check passes
    // but the "header" fields are garbage.
    let mut page = vec![0u8; 4096];
    page[0] = 0x0A; // coincidental type match
    // bytes 1-2: "freeblock" = arbitrary
    page[1] = 0x12; page[2] = 0x34;
    // bytes 3-4: "cell_count" = 0 (overflow has no cells)
    page[3] = 0; page[4] = 0;
    // Invalid: cell_count == 0
    assert!(!is_valid_btree_page(&page, 0), "overflow page with 0 cells must fail");
}

#[test]
fn test_valid_btree_page_overflow_nonzero_cells() {
    // Overflow page where next-page pointer bytes 3-4 happen to be nonzero
    let mut page = vec![0u8; 4096];
    page[0] = 0x0A;
    page[3] = 0xFF; page[4] = 0xFF; // "cell_count" = 65535
    // Invalid: cell_count > 10000
    assert!(!is_valid_btree_page(&page, 0), "overflow with 65535 cells must fail");
}

#[test]
fn test_valid_btree_page_content_area_too_small() {
    // Page with valid-looking cell count but content_area in the header
    let mut page = vec![0u8; 4096];
    page[0] = 0x0A;
    page[3] = 0; page[4] = 10; // cell_count = 10
    page[5] = 0; page[6] = 5; // content_area = 5 (way too small, header alone is 28 bytes)
    assert!(!is_valid_btree_page(&page, 0));
}

#[test]
fn test_valid_btree_page_page_zero_offset() {
    // Page 0 has 100-byte SQLite header, so B-tree header starts at offset 100
    let mut page = vec![0u8; 4096];
    page[100] = 0x0A;
    page[103] = 0; page[104] = 3; // cell_count = 3
    // min content = 100 + 8 + 2*3 = 114
    page[105] = 0; page[106] = 120; // content_area = 120 (valid, > 114)
    page[107] = 0;
    assert!(is_valid_btree_page(&page, 100));
}

#[test]
fn test_valid_btree_page_buffer_too_short() {
    let page = vec![0x0A, 0, 0, 0, 5]; // only 5 bytes, need at least 8
    assert!(!is_valid_btree_page(&page, 0));
}

#[test]
fn test_valid_btree_page_high_frag_bytes() {
    let mut page = vec![0u8; 4096];
    page[0] = 0x0A;
    page[3] = 0; page[4] = 1;
    page[5] = 0x0F; page[6] = 0xF0;
    page[7] = 200; // frag > 128
    assert!(!is_valid_btree_page(&page, 0));
}

// ── Eager load tracker poisoning regression tests ──
// Root cause: write_page() + mark_index_page()/mark_interior_group() would
// mark the entire sub-chunk as present in the tracker, but only 1 of 4 pages
// in the sub-chunk was actually written. Adjacent pages read as zeros →
// silent corruption ("database disk image is malformed").
// Fix: mark_index_page and mark_interior_group only upgrade the tracker
// tier if the sub-chunk is already present (i.e., loaded via write_pages_bulk).

#[test]
fn test_write_page_does_not_mark_sub_chunk_tracker() {
    // write_page should only update the bitmap, not the sub-chunk tracker.
    // This is critical for eager index loading where individual pages are
    // written without their sub-chunk neighbors.
    let dir = tempfile::tempdir().unwrap();
    // ppg=4, sub_ppf=2, page_size=64, page_count=8 → 2 groups, 2 sub-chunks/group
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap();
    let data = vec![0xAA; 64];

    // Write page 0 individually (simulates eager index load)
    cache.write_page(0, &data).unwrap();

    // Bitmap should mark page 0 present
    assert!(cache.bitmap.read().is_present(0));

    // Sub-chunk tracker should NOT mark sub-chunk as present
    let tracker = cache.tracker.lock();
    let sc = tracker.sub_chunk_for_page(0);
    assert!(!tracker.is_sub_chunk_present(&sc),
        "write_page must not mark sub-chunk as present in tracker");
    drop(tracker);

    // is_present should still find page 0 via bitmap fallback
    assert!(cache.is_present(0));

    // Page 1 (same sub-chunk) should NOT be present
    assert!(!cache.is_present(1),
        "adjacent page in same sub-chunk must not be reported as cached");
}

#[test]
fn test_eager_index_load_no_false_cache_hits() {
    // Simulates the eager index load scenario: scattered index pages are
    // written individually. Adjacent non-index pages in the same sub-chunk
    // must NOT be reported as cached.
    let dir = tempfile::tempdir().unwrap();
    // ppg=8, sub_ppf=4, page_size=64, page_count=16
    // Sub-chunk 0 = pages 0-3, Sub-chunk 1 = pages 4-7
    let cache = DiskCache::new(dir.path(), 3600, 8, 4, 64, 16, None, Vec::new()).unwrap();

    // Simulate eager load: write pages 0, 5 (scattered across sub-chunks)
    let index_data = vec![0xBB; 64];
    cache.write_page(0, &index_data).unwrap();
    cache.write_page(5, &index_data).unwrap();
    // NOTE: we intentionally do NOT call mark_index_page() — that's the fix

    // Pages 0 and 5 should be found (via bitmap)
    assert!(cache.is_present(0));
    assert!(cache.is_present(5));

    // Pages 1,2,3 (same sub-chunk as 0) must NOT be present
    assert!(!cache.is_present(1));
    assert!(!cache.is_present(2));
    assert!(!cache.is_present(3));

    // Pages 4,6,7 (same sub-chunk as 5) must NOT be present
    assert!(!cache.is_present(4));
    assert!(!cache.is_present(6));
    assert!(!cache.is_present(7));
}

#[test]
fn test_mark_index_page_safe_when_sub_chunk_not_in_tracker() {
    // After fix: mark_index_page only upgrades tier if sub-chunk is already
    // present in the tracker. If called after write_page (bitmap only),
    // it does NOT poison the tracker.
    let dir = tempfile::tempdir().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 8, 4, 64, 16, None, Vec::new()).unwrap();

    // Write only page 0 (simulates eager index load)
    let data = vec![0xCC; 64];
    cache.write_page(0, &data).unwrap();

    // Sub-chunk is NOT in tracker (write_page doesn't mark tracker)
    assert!(!cache.is_present(1));

    // mark_index_page should be a no-op (sub-chunk not in tracker)
    cache.mark_index_page(0, 0, 0);

    // Page 1 must still NOT be present (no poisoning)
    assert!(!cache.is_present(1),
        "mark_index_page must not poison sub-chunk when not in tracker");

    // Page 0 still findable via bitmap
    assert!(cache.is_present(0));
}

#[test]
fn test_mark_interior_group_safe_when_sub_chunk_not_in_tracker() {
    // Same fix as mark_index_page: mark_interior_group only calls
    // mark_pinned if the sub-chunk is already in the tracker.
    let dir = tempfile::tempdir().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 8, 4, 64, 16, None, Vec::new()).unwrap();

    // Write only page 0 (simulates eager interior load)
    let data = vec![0xDD; 64];
    cache.write_page(0, &data).unwrap();

    // Sub-chunk NOT in tracker
    assert!(!cache.is_present(1));

    // mark_interior_group should add to interior_pages/groups
    // but NOT poison the tracker
    cache.mark_interior_group(0, 0, 0);

    // interior_pages set updated
    assert!(cache.interior_pages.lock().contains(&0));
    assert!(cache.interior_groups.lock().contains(&0));

    // Page 1 must still NOT be present (no poisoning)
    assert!(!cache.is_present(1),
        "mark_interior_group must not poison sub-chunk when not in tracker");

    // Page 0 still findable via bitmap
    assert!(cache.is_present(0));

    // Tracker should NOT have the sub-chunk
    let tracker = cache.tracker.lock();
    let id = tracker.sub_chunk_for_page(0);
    assert!(!tracker.is_sub_chunk_present(&id),
        "sub-chunk must not be added to tracker by mark_interior_group");
}

#[test]
fn test_write_pages_bulk_correctly_marks_tracker() {
    // write_pages_bulk writes complete sub-chunks and should mark the tracker.
    // This is the correct path used by on-demand S3 fetch.
    let dir = tempfile::tempdir().unwrap();
    // ppg=8, sub_ppf=4, page_size=64, page_count=16
    let cache = DiskCache::new(dir.path(), 3600, 8, 4, 64, 16, None, Vec::new()).unwrap();

    // Write a complete sub-chunk (pages 0-3) via bulk
    let bulk_data = vec![0xDD; 64 * 4];
    cache.write_pages_bulk(0, &bulk_data, 4).unwrap();

    // All 4 pages in the sub-chunk should be present
    assert!(cache.is_present(0));
    assert!(cache.is_present(1));
    assert!(cache.is_present(2));
    assert!(cache.is_present(3));

    // Pages in next sub-chunk should not be present
    assert!(!cache.is_present(4));

    // Verify actual data was written
    let mut buf = vec![0u8; 64];
    cache.read_page(2, &mut buf).unwrap();
    assert!(buf.iter().all(|&b| b == 0xDD));
}

#[test]
fn test_bitmap_fallback_after_clear_cache_index() {
    // Raw bitmap clear (without index_pages re-marking) loses index pages.
    // This tests the low-level bitmap behavior.
    let dir = tempfile::tempdir().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 8, 4, 64, 16, None, Vec::new()).unwrap();

    // Simulate eager load: write individual page via write_page (bitmap only)
    let data = vec![0xEE; 64];
    cache.write_page(5, &data).unwrap();
    assert!(cache.is_present(5));

    // Raw bitmap clear (no index_pages re-mark) — page disappears
    for b in &cache.bitmap.read().bits {
        b.store(0, std::sync::atomic::Ordering::Relaxed);
    }
    assert!(!cache.is_present(5),
        "after raw bitmap clear, eagerly loaded page should not be present");
}

#[test]
fn test_index_pages_survive_clear_cache() {
    // Index pages tracked in index_pages set must survive clear_cache.
    // clear_cache re-marks them in the bitmap after clearing.
    let dir = tempfile::tempdir().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 8, 4, 64, 16, None, Vec::new()).unwrap();

    // Simulate eager index load: write pages and register in index_pages
    let data = vec![0xEE; 64];
    cache.write_page(5, &data).unwrap();
    cache.write_page(9, &data).unwrap();
    cache.index_pages.lock().insert(5);
    cache.index_pages.lock().insert(9);
    assert!(cache.is_present(5));
    assert!(cache.is_present(9));

    // Simulate clear_cache: clear bitmap, then re-mark interior + index + group 0
    let pinned = cache.interior_pages.lock().clone();
    let idx = cache.index_pages.lock().clone();
    let ppg = cache.pages_per_group as u64;
    {
        let bitmap = cache.bitmap.read();
        let total_pages = bitmap.bits.len() as u64 * 8;
        for b in &bitmap.bits {
            b.store(0, std::sync::atomic::Ordering::Relaxed);
        }
        for &p in &pinned { bitmap.mark_present(p); }
        for &p in &idx { bitmap.mark_present(p); }
        let g0_end = ppg.min(total_pages);
        for p in 0..g0_end { bitmap.mark_present(p); }
    }

    // Index pages must still be present
    assert!(cache.is_present(5), "index page 5 must survive clear_cache");
    assert!(cache.is_present(9), "index page 9 must survive clear_cache");

    // Non-index page outside group 0 must NOT be present
    assert!(!cache.is_present(10), "non-index page must not survive clear_cache");

    // Verify data is readable and correct
    let mut buf = vec![0u8; 64];
    cache.read_page(5, &mut buf).unwrap();
    assert!(buf.iter().all(|&b| b == 0xEE), "index page data must be intact");
}

// ---- Encryption tests ----

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
// Phase Drift: encode_override_frame tests
// =========================================================================

#[test]
fn test_encode_override_frame_single_page_roundtrip() {
    let page_size = 64u32;
    let page_data = vec![(42u64, vec![0xAB; page_size as usize])];
    let encoded = encode_override_frame(
        &page_data, page_size, 3,
        #[cfg(feature = "zstd")] None,
        None,
    ).expect("encode");
    // Decode with decode_seekable_subchunk (same format)
    let decoded = decode_seekable_subchunk(
        &encoded,
        #[cfg(feature = "zstd")] None,
        None,
    ).expect("decode");
    assert_eq!(decoded.len(), page_size as usize);
    assert_eq!(&decoded[..], &vec![0xAB; page_size as usize][..]);
}

#[test]
fn test_encode_override_frame_multiple_pages() {
    let page_size = 64u32;
    let pages = vec![
        (10u64, vec![0x11; page_size as usize]),
        (11u64, vec![0x22; page_size as usize]),
        (12u64, vec![0x33; page_size as usize]),
    ];
    let encoded = encode_override_frame(
        &pages, page_size, 3,
        #[cfg(feature = "zstd")] None,
        None,
    ).expect("encode");
    let decoded = decode_seekable_subchunk(
        &encoded,
        #[cfg(feature = "zstd")] None,
        None,
    ).expect("decode");
    assert_eq!(decoded.len(), 3 * page_size as usize);
    assert_eq!(&decoded[0..64], &vec![0x11u8; 64][..]);
    assert_eq!(&decoded[64..128], &vec![0x22u8; 64][..]);
    assert_eq!(&decoded[128..192], &vec![0x33u8; 64][..]);
}

#[test]
fn test_encode_override_frame_short_page_padding() {
    let page_size = 64u32;
    let short_data = vec![0xCC; 32]; // only 32 bytes, should pad to 64
    let pages = vec![(0u64, short_data)];
    let encoded = encode_override_frame(
        &pages, page_size, 3,
        #[cfg(feature = "zstd")] None,
        None,
    ).expect("encode");
    let decoded = decode_seekable_subchunk(
        &encoded,
        #[cfg(feature = "zstd")] None,
        None,
    ).expect("decode");
    assert_eq!(decoded.len(), page_size as usize);
    assert_eq!(&decoded[..32], &vec![0xCC; 32][..]);
    assert_eq!(&decoded[32..64], &vec![0u8; 32][..]); // zero-padded
}

#[test]
fn test_encode_override_frame_empty() {
    let page_size = 64u32;
    let pages: Vec<(u64, Vec<u8>)> = Vec::new();
    let encoded = encode_override_frame(
        &pages, page_size, 3,
        #[cfg(feature = "zstd")] None,
        None,
    ).expect("encode");
    let decoded = decode_seekable_subchunk(
        &encoded,
        #[cfg(feature = "zstd")] None,
        None,
    ).expect("decode");
    assert!(decoded.is_empty());
}

#[test]
fn test_encode_override_frame_compression_reduces_size() {
    let page_size = 4096u32;
    // Highly compressible data (all same byte)
    let pages = vec![
        (0u64, vec![0x00; page_size as usize]),
        (1u64, vec![0x00; page_size as usize]),
    ];
    let encoded = encode_override_frame(
        &pages, page_size, 3,
        #[cfg(feature = "zstd")] None,
        None,
    ).expect("encode");
    let raw_size = 2 * page_size as usize;
    assert!(encoded.len() < raw_size, "compressed {} should be < raw {}", encoded.len(), raw_size);
}

#[test]
#[cfg(feature = "encryption")]
fn test_encrypted_cache_write_read_roundtrip() {
    let dir = TempDir::new().unwrap();
    let key = test_key();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 16, Some(key), Vec::new()).unwrap();

    let page_data = vec![0xABu8; 64];
    cache.write_page(3, &page_data).unwrap();

    let mut buf = vec![0u8; 64];
    cache.read_page(3, &mut buf).unwrap();
    assert_eq!(buf, page_data, "decrypted data must match original");
}

#[test]
#[cfg(feature = "encryption")]
fn test_encrypted_cache_data_on_disk_is_not_plaintext() {
    use std::os::unix::fs::FileExt;
    let dir = TempDir::new().unwrap();
    let key = test_key();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 16, Some(key), Vec::new()).unwrap();

    let page_data = vec![0xABu8; 64];
    cache.write_page(3, &page_data).unwrap();

    // Read raw bytes from cache file — should NOT be plaintext
    let file = std::fs::File::open(dir.path().join("data.cache")).unwrap();
    let mut raw = vec![0u8; 64];
    file.read_exact_at(&mut raw, 3 * 64).unwrap();
    assert_ne!(&raw[..], &page_data[..], "on-disk data must be encrypted, not plaintext");
}

#[test]
#[cfg(feature = "encryption")]
fn test_encrypted_cache_wrong_key_returns_garbage() {
    let dir = TempDir::new().unwrap();
    let key = test_key();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 16, Some(key), Vec::new()).unwrap();

    let page_data = vec![0xCDu8; 64];
    cache.write_page(5, &page_data).unwrap();
    drop(cache);

    // Reopen with wrong key — CTR decrypts to garbage (no auth tag to reject)
    let bad_cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 16, Some(wrong_key()), Vec::new()).unwrap();
    let mut buf = vec![0u8; 64];
    bad_cache.read_page(5, &mut buf).unwrap();
    assert_ne!(buf, page_data, "wrong key must not produce correct plaintext");
}

#[test]
#[cfg(feature = "encryption")]
fn test_encrypted_cache_bulk_write_read_roundtrip() {
    let dir = TempDir::new().unwrap();
    let key = test_key();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 16, Some(key), Vec::new()).unwrap();

    // Write 4 pages at once (one full group)
    let mut bulk_data = Vec::with_capacity(4 * 64);
    for i in 0u8..4 {
        bulk_data.extend_from_slice(&[i + 1; 64]);
    }
    cache.write_pages_bulk(0, &bulk_data, 4).unwrap();

    // Read each page back individually
    for i in 0u8..4 {
        let mut buf = vec![0u8; 64];
        cache.read_page(i as u64, &mut buf).unwrap();
        assert_eq!(buf, vec![i + 1; 64], "page {} data mismatch", i);
    }
}

#[test]
#[cfg(feature = "encryption")]
fn test_encrypted_cache_different_pages_different_ciphertext() {
    use std::os::unix::fs::FileExt;
    let dir = TempDir::new().unwrap();
    let key = test_key();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 16, Some(key), Vec::new()).unwrap();

    // Write identical data to two different pages — ciphertext must differ (different nonces)
    let page_data = vec![0xFFu8; 64];
    cache.write_page(0, &page_data).unwrap();
    cache.write_page(1, &page_data).unwrap();

    let file = std::fs::File::open(dir.path().join("data.cache")).unwrap();
    let page_sz = 64usize;
    let mut raw0 = vec![0u8; page_sz];
    let mut raw1 = vec![0u8; page_sz];
    file.read_exact_at(&mut raw0, 0).unwrap();
    file.read_exact_at(&mut raw1, page_sz as u64).unwrap();
    assert_ne!(raw0, raw1, "same plaintext at different pages must produce different ciphertext");
}

#[test]
#[cfg(feature = "encryption")]
fn test_encrypted_cache_file_size_matches_page_size() {
    // Cache file size = page_count * page_size (no overhead from encryption)
    let dir = TempDir::new().unwrap();
    let key = test_key();
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, Some(key), Vec::new()).unwrap();
    let _ = cache;
    let meta = std::fs::metadata(dir.path().join("data.cache")).unwrap();
    assert_eq!(meta.len(), 8 * 64); // page_count * page_size, no overhead
}

#[test]
#[cfg(feature = "encryption")]
fn test_encode_decode_seekable_encrypted_roundtrip() {
    let key = test_key();
    let page_size = 64u32;
    let spf = 2u32;

    // Build 4 pages as Option<Vec<u8>>
    let pages: Vec<Option<Vec<u8>>> = (0u8..4)
        .map(|i| Some(vec![i + 1; 64]))
        .collect();

    let (blob, frames) = encode_page_group_seekable(
        &pages, page_size, spf, 3,
        #[cfg(feature = "zstd")] None,
        Some(&key),
    ).unwrap();

    // Decode each frame
    for (frame_idx, fe) in frames.iter().enumerate() {
        let frame_data = &blob[fe.offset as usize..(fe.offset + fe.len as u64) as usize];
        let decoded = decode_seekable_subchunk(
            frame_data,
            #[cfg(feature = "zstd")] None,
            Some(&key),
        ).unwrap();

        // decoded is a flat vec of bytes: spf pages * page_size
        let start_page = frame_idx * spf as usize;
        for j in 0..spf as usize {
            if start_page + j >= 4 { break; }
            let page_start = j * page_size as usize;
            let page_end = page_start + page_size as usize;
            if page_end <= decoded.len() {
                let expected = vec![(start_page + j + 1) as u8; 64];
                assert_eq!(&decoded[page_start..page_end], &expected[..], "frame {} page {} mismatch", frame_idx, j);
            }
        }
    }
}

#[test]
#[cfg(feature = "encryption")]
fn test_encode_decode_seekable_wrong_key_fails() {
    let key = test_key();
    let page_size = 64u32;
    let spf = 2u32;

    let pages: Vec<Option<Vec<u8>>> = (0..4).map(|_| Some(vec![0xABu8; 64])).collect();

    let (blob, frames) = encode_page_group_seekable(
        &pages, page_size, spf, 3,
        #[cfg(feature = "zstd")] None,
        Some(&key),
    ).unwrap();

    // Try decoding first frame with wrong key — GCM must reject
    let fe = &frames[0];
    let frame_data = &blob[fe.offset as usize..(fe.offset + fe.len as u64) as usize];
    let result = decode_seekable_subchunk(
        frame_data,
        #[cfg(feature = "zstd")] None,
        Some(&wrong_key()),
    );
    assert!(result.is_err(), "wrong key must produce GCM auth error");
}

#[test]
#[cfg(feature = "encryption")]
fn test_encode_decode_interior_bundle_encrypted_roundtrip() {
    let key = test_key();
    let page_size = 64u32;

    // Interior bundle input: Vec<(page_num, page_data)>
    let page_data_0 = vec![0xDDu8; 64];
    let page_data_1 = vec![0xEEu8; 64];
    let pages: Vec<(u64, &[u8])> = vec![
        (10, &page_data_0),
        (20, &page_data_1),
    ];

    let encoded = encode_interior_bundle(
        &pages, page_size, 3,
        #[cfg(feature = "zstd")] None,
        Some(&key),
    ).unwrap();

    // Decode
    let decoded = decode_interior_bundle(
        &encoded,
        #[cfg(feature = "zstd")] None,
        Some(&key),
    ).unwrap();
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0].0, 10);
    assert_eq!(decoded[0].1, page_data_0);
    assert_eq!(decoded[1].0, 20);
    assert_eq!(decoded[1].1, page_data_1);

    // Wrong key must fail
    let result = decode_interior_bundle(
        &encoded,
        #[cfg(feature = "zstd")] None,
        Some(&wrong_key()),
    );
    assert!(result.is_err(), "wrong key must produce GCM auth error");
}

#[test]
#[cfg(feature = "encryption")]
fn test_gcm_random_nonce_produces_unique_ciphertext() {
    // Same data encrypted twice must produce different ciphertext (random nonce)
    let key = test_key();
    let data = vec![0xAAu8; 256];
    let enc1 = compress::encrypt_gcm_random_nonce(&data, &key).unwrap();
    let enc2 = compress::encrypt_gcm_random_nonce(&data, &key).unwrap();

    // Nonce is first 12 bytes — must differ
    assert_ne!(&enc1[..12], &enc2[..12], "random nonces must differ between encryptions");
    // Full ciphertext must differ
    assert_ne!(enc1, enc2, "same plaintext must produce different ciphertext");

    // Both must decrypt to the same original data
    let dec1 = compress::decrypt_gcm_random_nonce(&enc1, &key).unwrap();
    let dec2 = compress::decrypt_gcm_random_nonce(&enc2, &key).unwrap();
    assert_eq!(dec1, data);
    assert_eq!(dec2, data);
}

#[test]
#[cfg(feature = "encryption")]
fn test_gcm_random_nonce_wrong_key_fails() {
    let key = test_key();
    let data = vec![0xBBu8; 128];
    let encrypted = compress::encrypt_gcm_random_nonce(&data, &key).unwrap();

    // Wrong key must produce GCM auth tag failure
    let result = compress::decrypt_gcm_random_nonce(&encrypted, &wrong_key());
    assert!(result.is_err(), "wrong key must fail GCM authentication");
}

#[test]
#[cfg(feature = "encryption")]
fn test_gcm_random_nonce_tamper_detection() {
    // Flipping a bit in the ciphertext must be caught by GCM
    let key = test_key();
    let data = vec![0xCCu8; 64];
    let mut encrypted = compress::encrypt_gcm_random_nonce(&data, &key).unwrap();

    // Flip a bit in the ciphertext body (after the 12-byte nonce prefix)
    encrypted[20] ^= 0x01;

    let result = compress::decrypt_gcm_random_nonce(&encrypted, &key);
    assert!(result.is_err(), "tampered ciphertext must fail GCM authentication");
}

#[test]
#[cfg(feature = "encryption")]
fn test_seekable_encode_twice_produces_different_ciphertext() {
    // Encoding the same page group twice must produce different blobs (random nonces per frame)
    let key = test_key();
    let pages: Vec<Option<Vec<u8>>> = (0u8..4).map(|i| Some(vec![i + 1; 64])).collect();

    let (blob1, _) = encode_page_group_seekable(
        &pages, 64, 2, 3,
        #[cfg(feature = "zstd")] None,
        Some(&key),
    ).unwrap();
    let (blob2, _) = encode_page_group_seekable(
        &pages, 64, 2, 3,
        #[cfg(feature = "zstd")] None,
        Some(&key),
    ).unwrap();

    assert_ne!(blob1, blob2, "same data encoded twice must produce different S3 blobs");
}

#[test]
#[cfg(feature = "encryption")]
fn test_sub_chunk_tracker_persist_twice_different_ciphertext() {
    // Same tracker state persisted twice must produce different on-disk bytes
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("tracker_nonce");
    let key = test_key();

    let mut t = SubChunkTracker::new_encrypted(path.clone(), 8, 2, Some(key));
    let id = SubChunkId { group_id: 0, frame_index: 0 };
    t.mark_present(id, SubChunkTier::Pinned);

    t.persist().unwrap();
    let bytes1 = std::fs::read(&path).unwrap();

    t.persist().unwrap();
    let bytes2 = std::fs::read(&path).unwrap();

    assert_ne!(bytes1, bytes2, "same tracker data persisted twice must produce different ciphertext");
}
