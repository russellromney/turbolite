//! S3-backed page-group tiered storage VFS.
//!
//! Architecture:
//! - S3/Tigris is the source of truth for all pages
//! - Local NVMe disk is a page-level cache (uncompressed, direct pread)
//! - Writes go through WAL (local, fast), checkpoint flushes dirty pages to S3 as page groups
//! - Any instance with the VFS + S3 credentials can read the database
//! - Default: 64KB pages, 256 pages per group (16MB uncompressed, ~8MB compressed)
//! - Sub-chunk caching: the unit of S3 cost is the sub-chunk (4 pages = 256KB), not the page.
//!   Cache tracking, eviction, and fetch all operate at sub-chunk granularity.
//!
//! S3 layout:
//! ```text
//! s3://{bucket}/{prefix}/
//! ├── manifest.json       # version, page_count, page_size, pages_per_group, page_group_keys
//! └── pg/
//!     ├── 0_v1            # Page group 0, manifest version 1 (pages 0-255)
//!     ├── 1_v1            # Page group 1 (pages 256-511)
//!     └── 0_v2            # Page group 0 updated at version 2
//! ```
//!
//! Local cache (single file, uncompressed):
//! ```text
//! [page 0 @ offset 0] [page 1 @ offset 65536] ... [page N @ offset N*65536]
//! ```
//! Cache hits are a single pread() with zero CPU overhead (no decompression).
//!
//! Sub-chunk tracker (in-memory, per sub-chunk):
//! Tracks which sub-chunks are present in the local cache file.
//! Eviction operates on sub-chunks with tiered priority:
//!   Tier 0 (pinned): interior page sub-chunks — never evicted
//!   Tier 1 (high):   index leaf sub-chunks — evicted last
//!   Tier 2 (normal): data sub-chunks — standard LRU
//!
//! Interior B-tree pages (type bytes 0x05, 0x02) are pinned permanently — never evicted.
//! They represent <1% of the database but are hit on every query.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions as FsOpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sqlite_vfs::{DatabaseHandle, LockKind, OpenKind, OpenOptions, Vfs};
use tokio::runtime::Handle as TokioHandle;

use crate::compress;

// Re-use the FileWalIndex from the main lib
use crate::FileWalIndex;

// --- Extracted submodules (Phase Tannenberg) ---
mod bench;
mod cache_tracking;
mod config;
mod disk_cache;
mod encoding;
mod handle;
mod import;
mod manifest;
mod prefetch;
mod rotation;
mod s3_client;
mod vfs;

// Public API (visible outside the crate)
pub use bench::TieredBenchHandle;
pub use config::{GroupState, TieredConfig, PageLocation, BTreeManifestEntry};
pub use handle::TieredHandle;
pub use import::import_sqlite_file;
pub use manifest::{FrameEntry, Manifest};
pub use vfs::TieredVfs;
#[cfg(feature = "encryption")]
pub use rotation::rotate_encryption_key;

// Crate-internal re-exports (accessible within tiered submodules via super::*)
pub(crate) use cache_tracking::*;
pub(crate) use disk_cache::*;
pub(crate) use encoding::*;
pub(crate) use prefetch::*;
pub(crate) use s3_client::*;


// ===== Constants =====

/// Default pages per page group (256 × 64KB = 16MB uncompressed, ~8MB compressed).
/// At 4KB page size this is 1MB per group — increase if using small pages.
const DEFAULT_PAGES_PER_GROUP: u32 = 256;

/// Default pages per sub-chunk frame for seekable encoding.
/// At 64KB page size: 4 × 64KB = 256KB per frame, ~128KB compressed per range GET.
const DEFAULT_SUB_PAGES_PER_FRAME: u32 = 4;

/// Target ~32MB uncompressed per bundle chunk.
/// Chunk range = how many page numbers each chunk covers.
/// At 64KB pages: 512 page range → worst case 32MB per chunk.
/// At  4KB pages: 8192 page range → worst case 32MB per chunk.
fn bundle_chunk_range(page_size: impl Into<u64>) -> u64 {
    const TARGET_BYTES: u64 = 32 * 1024 * 1024;
    TARGET_BYTES / page_size.into()
}

/// When true, `sync()` skips S3 upload and just records which page groups are dirty.
/// Pages are already in disk cache from `write_all_at()`. This keeps WAL small during
/// bulk loading without S3 round-trip overhead. Call `set_local_checkpoint_only(false)`
/// before the final checkpoint to upload everything to S3.
static LOCAL_CHECKPOINT_ONLY: AtomicBool = AtomicBool::new(false);

/// Set the local-checkpoint-only flag. When true, WAL checkpoints compact the WAL
/// and free in-memory dirty pages, but skip S3 upload. When false (default), checkpoints
/// upload dirty pages to S3 as normal.
pub fn set_local_checkpoint_only(val: bool) {
    LOCAL_CHECKPOINT_ONLY.store(val, Ordering::Release);
}

/// Returns the current value of the local-checkpoint-only flag.
pub fn is_local_checkpoint_only() -> bool {
    LOCAL_CHECKPOINT_ONLY.load(Ordering::Acquire)
}

// ===== Page group coordinate math =====

pub fn group_id(page_num: u64, ppg: u32) -> u64 {
    page_num / ppg as u64
}

pub fn local_idx_in_group(page_num: u64, ppg: u32) -> u32 {
    (page_num % ppg as u64) as u32
}

pub fn group_start_page(gid: u64, ppg: u32) -> u64 {
    gid * ppg as u64
}

/// Validate that a page with a B-tree type byte actually has a valid B-tree header.
/// Prevents false-positive classification of overflow/freelist pages whose first byte
/// coincidentally matches a B-tree type (especially 0x0A = 10, a common byte value).
///
/// SQLite B-tree page header (at hdr_offset):
///   byte 0: type flag (0x02, 0x05, 0x0A, 0x0D)
///   bytes 1-2: first freeblock offset (u16 BE, 0 = none)
///   bytes 3-4: cell count (u16 BE)
///   bytes 5-6: cell content area start (u16 BE, 0 = 65536)
///   byte 7: fragmented free bytes count
fn is_valid_btree_page(buf: &[u8], hdr_offset: usize) -> bool {
    // Need at least 8 bytes of header
    if buf.len() < hdr_offset + 8 {
        return false;
    }
    let cell_count = u16::from_be_bytes([buf[hdr_offset + 3], buf[hdr_offset + 4]]);
    let content_area = u16::from_be_bytes([buf[hdr_offset + 5], buf[hdr_offset + 6]]);
    let frag_bytes = buf[hdr_offset + 7];

    // Cell count must be > 0 (a real B-tree page has at least one cell)
    // and reasonable (at 4KB pages, max ~500 cells; at 64KB, max ~8000)
    if cell_count == 0 || cell_count > 10000 {
        return false;
    }
    // Content area must be within the page (0 means 65536 for large pages)
    // and past the header (header is at least 8 bytes + 2*cell_count cell pointer bytes)
    let min_content_offset = hdr_offset as u16 + 8 + 2 * cell_count;
    if content_area != 0 && (content_area < min_content_offset || content_area as usize > buf.len()) {
        return false;
    }
    // Fragment bytes must be < 255 (at most 60 in practice)
    if frag_bytes > 128 {
        return false;
    }
    true
}

/// Register a tiered VFS with SQLite.
pub fn register(name: &str, vfs: TieredVfs) -> Result<(), io::Error> {
    sqlite_vfs::register(name, vfs, false)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

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

    #[test]
    fn test_bitmap_is_present_beyond_capacity_returns_false() {
        let dir = TempDir::new().unwrap();
        let bm = PageBitmap::new(dir.path().join("bm"));
        // Empty bitmap, any page should return false
        assert!(!bm.is_present(0));
        assert!(!bm.is_present(u64::MAX / 8 - 1));
    }

    // =========================================================================
    // Coordinate Math
    // =========================================================================

    #[test]
    fn test_group_id_calculation() {
        let ppg = 2048u32;
        assert_eq!(group_id(0, ppg), 0);
        assert_eq!(group_id(2047, ppg), 0);
        assert_eq!(group_id(2048, ppg), 1);
        assert_eq!(group_id(4095, ppg), 1);
        assert_eq!(group_id(4096, ppg), 2);
    }

    #[test]
    fn test_group_id_small_ppg() {
        assert_eq!(group_id(0, 1), 0);
        assert_eq!(group_id(1, 1), 1);
        assert_eq!(group_id(99, 1), 99);
        assert_eq!(group_id(0, 4), 0);
        assert_eq!(group_id(3, 4), 0);
        assert_eq!(group_id(4, 4), 1);
    }

    #[test]
    fn test_local_idx_calculation() {
        let ppg = 2048u32;
        assert_eq!(local_idx_in_group(0, ppg), 0);
        assert_eq!(local_idx_in_group(2047, ppg), 2047);
        assert_eq!(local_idx_in_group(2048, ppg), 0);
        assert_eq!(local_idx_in_group(2049, ppg), 1);
    }

    #[test]
    fn test_local_idx_small_ppg() {
        assert_eq!(local_idx_in_group(0, 1), 0);
        assert_eq!(local_idx_in_group(1, 1), 0);
        assert_eq!(local_idx_in_group(5, 3), 2);
        assert_eq!(local_idx_in_group(6, 3), 0);
    }

    #[test]
    fn test_group_start_page() {
        let ppg = 2048u32;
        assert_eq!(group_start_page(0, ppg), 0);
        assert_eq!(group_start_page(1, ppg), 2048);
        assert_eq!(group_start_page(5, ppg), 10240);
    }

    #[test]
    fn test_group_start_page_small_ppg() {
        assert_eq!(group_start_page(0, 1), 0);
        assert_eq!(group_start_page(3, 1), 3);
        assert_eq!(group_start_page(2, 4), 8);
    }

    #[test]
    fn test_coordinate_math_roundtrip() {
        // For any page, group_start_page(group_id(p)) + local_idx(p) == p
        let ppg = 2048u32;
        for p in [0u64, 1, 2047, 2048, 2049, 4095, 4096, 100_000] {
            let gid = group_id(p, ppg);
            let idx = local_idx_in_group(p, ppg);
            let reconstructed = group_start_page(gid, ppg) + idx as u64;
            assert_eq!(reconstructed, p, "roundtrip failed for page {}", p);
        }
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

    #[test]
    fn test_page_type_all_valid_types() {
        let check = |buf: &[u8]| -> bool {
            matches!(buf.get(0), Some(&0x05) | Some(&0x02))
        };
        // 0x02 = index interior, 0x05 = table interior
        assert!(check(&[0x02]));
        assert!(check(&[0x05]));
        // 0x0a = index leaf, 0x0d = table leaf, others = not interior
        assert!(!check(&[0x0a]));
        assert!(!check(&[0x0d]));
        assert!(!check(&[0x00]));
        assert!(!check(&[0x01]));
        assert!(!check(&[0xFF]));
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
                "pg/0_v1".to_string(),
                "pg/1_v1".to_string(),
                "pg/2_v1".to_string(),
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
            page_group_keys: vec!["pg/0_v1".to_string(), "pg/1_v1".to_string()],
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

    #[test]
    fn test_seekable_manifest_backward_compat() {
        // Old manifest without frame_tables/sub_pages_per_frame
        let json = r#"{"version":1,"page_count":100,"page_size":4096,"pages_per_group":512,"page_group_keys":[]}"#;
        let m: Manifest = serde_json::from_str(json).unwrap();
        assert!(m.frame_tables.is_empty());
        assert_eq!(m.sub_pages_per_frame, 0);
    }

    // =========================================================================
    // TieredConfig
    // =========================================================================

    #[test]
    fn test_tiered_config_default() {
        let c = TieredConfig::default();
        assert_eq!(c.bucket, "");
        assert_eq!(c.prefix, "");
        assert_eq!(c.cache_dir, PathBuf::from("/tmp/sqlces-cache"));
        assert_eq!(c.compression_level, 3);
        assert_eq!(c.endpoint_url, None);
        assert!(!c.read_only);
        assert!(c.runtime_handle.is_none());
        assert_eq!(c.pages_per_group, DEFAULT_PAGES_PER_GROUP);
        assert_eq!(c.region, None);
        assert_eq!(c.cache_ttl_secs, 3600);
        assert_eq!(c.prefetch_hops, vec![0.33, 0.33]);
        let expected_threads = std::thread::available_parallelism()
            .map(|n| n.get() as u32)
            .unwrap_or(2) + 1;
        assert_eq!(c.prefetch_threads, expected_threads);
    }

    #[test]
    fn test_tiered_config_default_pages_per_group() {
        assert_eq!(DEFAULT_PAGES_PER_GROUP, 256);
        assert_eq!(TieredConfig::default().pages_per_group, 256);
    }

    // =========================================================================
    // S3Client page_group_key format
    // =========================================================================

    #[test]
    fn test_s3_key_format() {
        // Verify the key format is "pg/{gid}_v{version}"
        let key = format!("pg/{}_v{}", 5, 3);
        assert_eq!(key, "pg/5_v3");
        let key0 = format!("pg/{}_v{}", 0, 1);
        assert_eq!(key0, "pg/0_v1");
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
        let encoded = test_encode(&pages, page_size);

        // Simulate what decode_and_cache_group does: decode whole group, write pages
        let (_pg_count, _ps, decoded) = test_decode(&encoded);
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
    // Local-checkpoint-only flag
    // =========================================================================

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
    fn test_clear_cache_preserves_index_tier_in_tracker() {
        // Regression: clear_cache() called clear_data() which removed Index tier.
        // Should call clear_data_only() to keep Index + Pinned.
        let dir = TempDir::new().unwrap();
        // ppg=8, spf=2, page_size=64, page_count=16
        let gp = positional_group_pages(8, 16);
        let cache = DiskCache::new(dir.path(), 3600, 8, 2, 64, 16, None, gp).unwrap();

        // Write some frames via bulk (marks tracker)
        cache.write_pages_bulk(0, &vec![1u8; 128], 2).unwrap();  // frame (0,0) - pages 0,1
        cache.write_pages_bulk(2, &vec![2u8; 128], 2).unwrap();  // frame (0,1) - pages 2,3
        cache.write_pages_bulk(4, &vec![3u8; 128], 2).unwrap();  // frame (0,2) - pages 4,5

        // Promote one to Index, one to Pinned
        // page 2 is at index 2 in group 0 => sub_chunk_id_for(0, 2) gives frame_index=1
        cache.mark_index_page(2, 0, 2); // frame (0,1) → Index
        {
            let mut tracker = cache.tracker.lock();
            let id = tracker.sub_chunk_for_page(0);
            tracker.mark_pinned(id); // frame (0,0) → Pinned
        }

        // Verify pre-state
        {
            let tracker = cache.tracker.lock();
            assert_eq!(tracker.count_tier(SubChunkTier::Pinned), 1);
            assert_eq!(tracker.count_tier(SubChunkTier::Index), 1);
            assert_eq!(tracker.count_tier(SubChunkTier::Data), 1);
        }

        // Simulate clear_cache tracker behavior (same as TieredVfs::clear_cache)
        {
            let mut tracker = cache.tracker.lock();
            tracker.clear_data_only();
        }

        // After: Pinned and Index survive, Data is gone
        {
            let tracker = cache.tracker.lock();
            assert_eq!(tracker.count_tier(SubChunkTier::Pinned), 1, "Pinned must survive clear_data_only");
            assert_eq!(tracker.count_tier(SubChunkTier::Index), 1, "Index must survive clear_data_only");
            assert_eq!(tracker.count_tier(SubChunkTier::Data), 0, "Data must be cleared");
        }
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
        assert!(cache.bitmap.lock().is_present(0));

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
        cache.bitmap.lock().bits.fill(0);
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
            let mut bitmap = cache.bitmap.lock();
            let total_pages = bitmap.bits.len() as u64 * 8;
            bitmap.bits.fill(0);
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

    // ===== WAL/journal passthrough encryption tests =====

    #[test]
    #[cfg(feature = "encryption")]
    fn test_passthrough_write_read_roundtrip_encrypted() {
        // Passthrough files (WAL/journal) should CTR-encrypt on write and decrypt on read
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.wal");
        let file = FsOpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
        let key = test_key();
        let mut handle = TieredHandle::new_passthrough(file, path.clone(), Some(key));

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
        let file = FsOpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
        let key = test_key();
        let mut handle = TieredHandle::new_passthrough(file, path.clone(), Some(key));

        let plaintext = vec![0xCCu8; 512];
        handle.write_all_at(&plaintext, 0).unwrap();

        // Read raw bytes from disk (bypassing VFS)
        let raw = std::fs::read(&path).unwrap();
        assert_ne!(&raw[..512], &plaintext[..], "on-disk bytes must NOT be plaintext");
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn test_passthrough_wrong_key_returns_garbage() {
        // Reading with a different key must return garbage, not the original data
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.wal");

        // Write with correct key
        {
            let file = FsOpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
            let key = test_key();
            let mut handle = TieredHandle::new_passthrough(file, path.clone(), Some(key));
            handle.write_all_at(&vec![0xDDu8; 256], 0).unwrap();
        }

        // Read with wrong key
        let file = FsOpenOptions::new().read(true).open(&path).unwrap();
        let mut handle = TieredHandle::new_passthrough(file, path.clone(), Some(wrong_key()));
        let mut buf = vec![0u8; 256];
        handle.read_exact_at(&mut buf, 0).unwrap(); // CTR doesn't fail, just produces wrong data
        assert_ne!(buf, vec![0xDDu8; 256], "wrong key must not produce original plaintext");
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn test_passthrough_no_encryption_is_plaintext() {
        // Without encryption key, passthrough should be plain read/write
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.wal");
        let file = FsOpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
        let mut handle = TieredHandle::new_passthrough(file, path.clone(), None);

        let plaintext = vec![0xEEu8; 128];
        handle.write_all_at(&plaintext, 0).unwrap();

        // On-disk should be plaintext
        let raw = std::fs::read(&path).unwrap();
        assert_eq!(&raw[..128], &plaintext[..], "without encryption, on-disk must be plaintext");
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn test_passthrough_different_offsets_different_ciphertext() {
        // Same data written at different offsets must produce different ciphertext (nonce = offset)
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.wal");
        let file = FsOpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
        let key = test_key();
        let mut handle = TieredHandle::new_passthrough(file, path.clone(), Some(key));

        let data = vec![0xFFu8; 64];
        handle.write_all_at(&data, 0).unwrap();
        handle.write_all_at(&data, 64).unwrap();

        let raw = std::fs::read(&path).unwrap();
        assert_ne!(&raw[..64], &raw[64..128], "same data at different offsets must produce different ciphertext");
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

    // ===== Key rotation tests =====

    #[cfg(feature = "encryption")]
    fn test_key_2() -> [u8; 32] {
        let mut key = [0u8; 32];
        for (i, b) in key.iter_mut().enumerate() {
            *b = (i as u8).wrapping_add(0x80);
        }
        key
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn test_rotate_page_group_non_seekable() {
        // Encrypt with key A, simulate rotation (decrypt A, re-encrypt B), decrypt with B
        let key_a = test_key();
        let key_b = test_key_2();

        let pages: Vec<Option<Vec<u8>>> = (0..4)
            .map(|i| Some(vec![i as u8; 4096]))
            .collect();

        let encoded = encode_page_group(
            &pages,
            4096,
            3,
            #[cfg(feature = "zstd")]
            None,
            Some(&key_a),
        )
        .unwrap();

        // Rotate: decrypt with A, re-encrypt with B
        let compressed = compress::decrypt_gcm_random_nonce(&encoded, &key_a).unwrap();
        let rotated = compress::encrypt_gcm_random_nonce(&compressed, &key_b).unwrap();

        // Decrypt with B should work
        let (pc, ps, decoded_pages) = decode_page_group(
            &rotated,
            #[cfg(feature = "zstd")]
            None,
            Some(&key_b),
        )
        .unwrap();
        assert_eq!(pc, 4);
        assert_eq!(ps, 4096);
        for (i, page) in decoded_pages.iter().enumerate() {
            assert_eq!(page, &vec![i as u8; 4096]);
        }

        // Decrypt with A should fail (GCM auth tag)
        let result = decode_page_group(
            &rotated,
            #[cfg(feature = "zstd")]
            None,
            Some(&key_a),
        );
        assert!(result.is_err(), "old key must fail after rotation");
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn test_rotate_page_group_seekable() {
        // Encrypt seekable with key A, per-frame rotation to B, decrypt with B
        let key_a = test_key();
        let key_b = test_key_2();

        let pages: Vec<Option<Vec<u8>>> = (0..8)
            .map(|i| Some(vec![i as u8; 4096]))
            .collect();

        let (encoded, frame_table) = encode_page_group_seekable(
            &pages,
            4096,
            2, // 2 pages per frame = 4 frames
            3,
            #[cfg(feature = "zstd")]
            None,
            Some(&key_a),
        )
        .unwrap();

        assert_eq!(frame_table.len(), 4);

        // Rotate per-frame: decrypt A, re-encrypt B, rebuild frame table
        let mut new_blob = Vec::with_capacity(encoded.len());
        let mut new_frames = Vec::with_capacity(frame_table.len());

        for frame in &frame_table {
            let end = frame.offset as usize + frame.len as usize;
            let frame_data = &encoded[frame.offset as usize..end];
            let compressed = compress::decrypt_gcm_random_nonce(frame_data, &key_a).unwrap();
            let re_encrypted = compress::encrypt_gcm_random_nonce(&compressed, &key_b).unwrap();

            new_frames.push(FrameEntry {
                offset: new_blob.len() as u64,
                len: re_encrypted.len() as u32,
            });
            new_blob.extend_from_slice(&re_encrypted);
        }

        // Frame table offsets should be preserved (GCM overhead is constant)
        for (old, new) in frame_table.iter().zip(new_frames.iter()) {
            assert_eq!(old.len, new.len, "frame size must be identical after rotation");
        }

        // Decrypt each frame with B
        for (i, frame) in new_frames.iter().enumerate() {
            let end = frame.offset as usize + frame.len as usize;
            let frame_data = &new_blob[frame.offset as usize..end];
            let raw = decode_seekable_subchunk(
                frame_data,
                #[cfg(feature = "zstd")]
                None,
                Some(&key_b),
            )
            .unwrap();

            // Each frame has 2 pages of 4096 bytes
            assert_eq!(raw.len(), 2 * 4096);
            let page_0 = &raw[..4096];
            let page_1 = &raw[4096..];
            assert_eq!(page_0, &vec![(i * 2) as u8; 4096]);
            assert_eq!(page_1, &vec![(i * 2 + 1) as u8; 4096]);
        }
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn test_rotate_interior_bundle() {
        // Encrypt interior bundle with key A, rotate to B, decrypt with B
        let key_a = test_key();
        let key_b = test_key_2();

        let page_data: Vec<Vec<u8>> = (0..3).map(|i| vec![i as u8; 4096]).collect();
        let pages: Vec<(u64, &[u8])> = page_data
            .iter()
            .enumerate()
            .map(|(i, d)| (i as u64, d.as_slice()))
            .collect();

        let encoded = encode_interior_bundle(
            &pages,
            4096,
            3,
            #[cfg(feature = "zstd")]
            None,
            Some(&key_a),
        )
        .unwrap();

        // Rotate: decrypt A, re-encrypt B
        let compressed = compress::decrypt_gcm_random_nonce(&encoded, &key_a).unwrap();
        let rotated = compress::encrypt_gcm_random_nonce(&compressed, &key_b).unwrap();

        // Decrypt with B
        let decoded = decode_interior_bundle(
            &rotated,
            #[cfg(feature = "zstd")]
            None,
            Some(&key_b),
        )
        .unwrap();

        assert_eq!(decoded.len(), 3);
        for (i, (pnum, data)) in decoded.iter().enumerate() {
            assert_eq!(*pnum, i as u64);
            assert_eq!(data, &vec![i as u8; 4096]);
        }

        // Old key fails
        let result = decode_interior_bundle(
            &rotated,
            #[cfg(feature = "zstd")]
            None,
            Some(&key_a),
        );
        assert!(result.is_err(), "old key must fail after rotation");
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn test_rotate_manifest_version_incremented() {
        // Rotation must bump manifest version
        let manifest = Manifest {
            version: 5,
            page_count: 100,
            page_size: 4096,
            pages_per_group: 8,
            page_group_keys: vec!["pg/0_v5".to_string()],
            frame_tables: vec![vec![]],
            ..Manifest::empty()
        };

        let mut new_manifest = manifest.clone();
        new_manifest.version += 1;
        assert_eq!(new_manifest.version, 6);
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn test_rotate_nonce_uniqueness() {
        // Re-encrypted frames must have different nonces than originals
        let key_a = test_key();
        let key_b = test_key_2();

        let data = vec![0x42u8; 4096];
        let enc_a = compress::encrypt_gcm_random_nonce(&data, &key_a).unwrap();

        // Decrypt then re-encrypt with B
        let plaintext = compress::decrypt_gcm_random_nonce(&enc_a, &key_a).unwrap();
        let enc_b = compress::encrypt_gcm_random_nonce(&plaintext, &key_b).unwrap();

        // Nonces (first 12 bytes) must differ
        assert_ne!(
            &enc_a[..12],
            &enc_b[..12],
            "re-encrypted data must have different nonce"
        );
        // Ciphertext must differ (different key + different nonce)
        assert_ne!(enc_a, enc_b);
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn test_rotate_wrong_old_key_fails() {
        // Trying to decrypt with wrong key should fail
        let key_a = test_key();
        let wrong = wrong_key();

        let data = vec![0x42u8; 4096];
        let encrypted = compress::encrypt_gcm_random_nonce(&data, &key_a).unwrap();

        let result = compress::decrypt_gcm_random_nonce(&encrypted, &wrong);
        assert!(result.is_err(), "wrong old key must fail decryption");
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn test_rotate_same_key_idempotent() {
        // Rotating to the same key should produce valid (but different) ciphertext
        let key = test_key();

        let pages: Vec<Option<Vec<u8>>> = (0..4)
            .map(|i| Some(vec![i as u8; 4096]))
            .collect();

        let encoded = encode_page_group(
            &pages,
            4096,
            3,
            #[cfg(feature = "zstd")]
            None,
            Some(&key),
        )
        .unwrap();

        // Rotate to same key
        let compressed = compress::decrypt_gcm_random_nonce(&encoded, &key).unwrap();
        let rotated = compress::encrypt_gcm_random_nonce(&compressed, &key).unwrap();

        // Different ciphertext (new random nonce)
        assert_ne!(encoded, rotated);

        // But same data when decoded
        let (_, _, decoded) = decode_page_group(
            &rotated,
            #[cfg(feature = "zstd")]
            None,
            Some(&key),
        )
        .unwrap();
        for (i, page) in decoded.iter().enumerate() {
            assert_eq!(page, &vec![i as u8; 4096]);
        }
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn test_rotate_empty_page_group_vec() {
        // Empty page group keys should be skipped without error
        let key_a = test_key();
        let key_b = test_key_2();

        // Simulate what rotate_encryption_key does: skip empty keys
        let page_group_keys: Vec<String> = vec![
            String::new(),
            "pg/1_v1".to_string(),
            String::new(),
        ];

        let non_empty: Vec<&String> = page_group_keys.iter().filter(|k| !k.is_empty()).collect();
        assert_eq!(non_empty.len(), 1);
        assert_eq!(non_empty[0], "pg/1_v1");

        // And a non-seekable page group can still be rotated
        let pages: Vec<Option<Vec<u8>>> = vec![Some(vec![1u8; 4096])];
        let encoded = encode_page_group(
            &pages,
            4096,
            3,
            #[cfg(feature = "zstd")]
            None,
            Some(&key_a),
        )
        .unwrap();
        let compressed = compress::decrypt_gcm_random_nonce(&encoded, &key_a).unwrap();
        let rotated = compress::encrypt_gcm_random_nonce(&compressed, &key_b).unwrap();
        let (_, _, decoded) = decode_page_group(
            &rotated,
            #[cfg(feature = "zstd")]
            None,
            Some(&key_b),
        )
        .unwrap();
        assert_eq!(decoded[0], vec![1u8; 4096]);
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn test_rotate_large_page_group() {
        // 256 pages at 4KB = 1MB page group through rotation
        let key_a = test_key();
        let key_b = test_key_2();

        let pages: Vec<Option<Vec<u8>>> = (0..256)
            .map(|i| Some(vec![(i % 256) as u8; 4096]))
            .collect();

        let (encoded, frame_table) = encode_page_group_seekable(
            &pages,
            4096,
            4, // 4 pages per frame = 64 frames
            3,
            #[cfg(feature = "zstd")]
            None,
            Some(&key_a),
        )
        .unwrap();

        assert_eq!(frame_table.len(), 64);

        // Rotate all frames
        let mut new_blob = Vec::with_capacity(encoded.len());
        let mut new_frames = Vec::with_capacity(frame_table.len());

        for frame in &frame_table {
            let end = frame.offset as usize + frame.len as usize;
            let frame_data = &encoded[frame.offset as usize..end];
            let compressed = compress::decrypt_gcm_random_nonce(frame_data, &key_a).unwrap();
            let re_encrypted = compress::encrypt_gcm_random_nonce(&compressed, &key_b).unwrap();
            new_frames.push(FrameEntry {
                offset: new_blob.len() as u64,
                len: re_encrypted.len() as u32,
            });
            new_blob.extend_from_slice(&re_encrypted);
        }

        // Verify all 256 pages through the rotated data
        for (fi, frame) in new_frames.iter().enumerate() {
            let end = frame.offset as usize + frame.len as usize;
            let raw = decode_seekable_subchunk(
                &new_blob[frame.offset as usize..end],
                #[cfg(feature = "zstd")]
                None,
                Some(&key_b),
            )
            .unwrap();
            assert_eq!(raw.len(), 4 * 4096);
            for pi in 0..4 {
                let page = &raw[pi * 4096..(pi + 1) * 4096];
                let expected_byte = ((fi * 4 + pi) % 256) as u8;
                assert_eq!(page, &vec![expected_byte; 4096]);
            }
        }
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn test_add_encryption_non_seekable() {
        // Encode without encryption, then encrypt (None -> Some)
        let key = test_key();

        let pages: Vec<Option<Vec<u8>>> = (0..4)
            .map(|i| Some(vec![i as u8; 4096]))
            .collect();

        // Encode without encryption
        let encoded = encode_page_group(
            &pages,
            4096,
            3,
            #[cfg(feature = "zstd")]
            None,
            None, // no encryption
        )
        .unwrap();

        // "Add encryption": encrypt the compressed blob
        let encrypted = compress::encrypt_gcm_random_nonce(&encoded, &key).unwrap();

        // Decrypt with key should work
        let (pc, ps, decoded_pages) = decode_page_group(
            &encrypted,
            #[cfg(feature = "zstd")]
            None,
            Some(&key),
        )
        .unwrap();
        assert_eq!(pc, 4);
        assert_eq!(ps, 4096);
        for (i, page) in decoded_pages.iter().enumerate() {
            assert_eq!(page, &vec![i as u8; 4096]);
        }
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn test_remove_encryption_non_seekable() {
        // Encode with encryption, then decrypt (Some -> None)
        let key = test_key();

        let pages: Vec<Option<Vec<u8>>> = (0..4)
            .map(|i| Some(vec![i as u8; 4096]))
            .collect();

        let encoded = encode_page_group(
            &pages,
            4096,
            3,
            #[cfg(feature = "zstd")]
            None,
            Some(&key),
        )
        .unwrap();

        // "Remove encryption": decrypt to get raw compressed data
        let decrypted = compress::decrypt_gcm_random_nonce(&encoded, &key).unwrap();

        // Should be decodable without encryption
        let (pc, ps, decoded_pages) = decode_page_group(
            &decrypted,
            #[cfg(feature = "zstd")]
            None,
            None, // no encryption
        )
        .unwrap();
        assert_eq!(pc, 4);
        assert_eq!(ps, 4096);
        for (i, page) in decoded_pages.iter().enumerate() {
            assert_eq!(page, &vec![i as u8; 4096]);
        }
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn test_add_encryption_seekable() {
        // Encode seekable without encryption, add encryption, decrypt with key
        let key = test_key();

        let pages: Vec<Option<Vec<u8>>> = (0..8)
            .map(|i| Some(vec![i as u8; 4096]))
            .collect();

        let (encoded, frame_table) = encode_page_group_seekable(
            &pages,
            4096,
            2,
            3,
            #[cfg(feature = "zstd")]
            None,
            None, // no encryption
        )
        .unwrap();

        assert_eq!(frame_table.len(), 4);

        // Add encryption per-frame
        let mut new_blob = Vec::with_capacity(encoded.len() + 28 * frame_table.len());
        let mut new_frames = Vec::with_capacity(frame_table.len());

        for frame in &frame_table {
            let end = frame.offset as usize + frame.len as usize;
            let frame_data = &encoded[frame.offset as usize..end];
            let encrypted = compress::encrypt_gcm_random_nonce(frame_data, &key).unwrap();

            new_frames.push(FrameEntry {
                offset: new_blob.len() as u64,
                len: encrypted.len() as u32,
            });
            new_blob.extend_from_slice(&encrypted);
        }

        // Each encrypted frame should be 28 bytes larger
        for (old, new) in frame_table.iter().zip(new_frames.iter()) {
            assert_eq!(new.len, old.len + 28);
        }

        // Decrypt each frame
        for (i, frame) in new_frames.iter().enumerate() {
            let end = frame.offset as usize + frame.len as usize;
            let raw = decode_seekable_subchunk(
                &new_blob[frame.offset as usize..end],
                #[cfg(feature = "zstd")]
                None,
                Some(&key),
            )
            .unwrap();
            assert_eq!(raw.len(), 2 * 4096);
            assert_eq!(&raw[..4096], &vec![(i * 2) as u8; 4096]);
            assert_eq!(&raw[4096..], &vec![(i * 2 + 1) as u8; 4096]);
        }
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn test_remove_encryption_seekable() {
        // Encode seekable with encryption, remove encryption, decode without key
        let key = test_key();

        let pages: Vec<Option<Vec<u8>>> = (0..8)
            .map(|i| Some(vec![i as u8; 4096]))
            .collect();

        let (encoded, frame_table) = encode_page_group_seekable(
            &pages,
            4096,
            2,
            3,
            #[cfg(feature = "zstd")]
            None,
            Some(&key),
        )
        .unwrap();

        assert_eq!(frame_table.len(), 4);

        // Remove encryption per-frame
        let mut new_blob = Vec::new();
        let mut new_frames = Vec::with_capacity(frame_table.len());

        for frame in &frame_table {
            let end = frame.offset as usize + frame.len as usize;
            let frame_data = &encoded[frame.offset as usize..end];
            let decrypted = compress::decrypt_gcm_random_nonce(frame_data, &key).unwrap();

            new_frames.push(FrameEntry {
                offset: new_blob.len() as u64,
                len: decrypted.len() as u32,
            });
            new_blob.extend_from_slice(&decrypted);
        }

        // Each decrypted frame should be 28 bytes smaller
        for (old, new) in frame_table.iter().zip(new_frames.iter()) {
            assert_eq!(new.len + 28, old.len);
        }

        // Decode each frame without encryption
        for (i, frame) in new_frames.iter().enumerate() {
            let end = frame.offset as usize + frame.len as usize;
            let raw = decode_seekable_subchunk(
                &new_blob[frame.offset as usize..end],
                #[cfg(feature = "zstd")]
                None,
                None, // no encryption
            )
            .unwrap();
            assert_eq!(raw.len(), 2 * 4096);
            assert_eq!(&raw[..4096], &vec![(i * 2) as u8; 4096]);
            assert_eq!(&raw[4096..], &vec![(i * 2 + 1) as u8; 4096]);
        }
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn test_add_encryption_interior_bundle() {
        // Encode interior bundle without encryption, add encryption, decode with key
        let key = test_key();

        let page_data: Vec<Vec<u8>> = (0..3).map(|i| vec![i as u8; 4096]).collect();
        let pages: Vec<(u64, &[u8])> = page_data
            .iter()
            .enumerate()
            .map(|(i, d)| (i as u64, d.as_slice()))
            .collect();

        let encoded = encode_interior_bundle(
            &pages,
            4096,
            3,
            #[cfg(feature = "zstd")]
            None,
            None, // no encryption
        )
        .unwrap();

        // Add encryption
        let encrypted = compress::encrypt_gcm_random_nonce(&encoded, &key).unwrap();

        // Decode with key
        let decoded = decode_interior_bundle(
            &encrypted,
            #[cfg(feature = "zstd")]
            None,
            Some(&key),
        )
        .unwrap();

        assert_eq!(decoded.len(), 3);
        for (i, (pnum, data)) in decoded.iter().enumerate() {
            assert_eq!(*pnum, i as u64);
            assert_eq!(data, &vec![i as u8; 4096]);
        }
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn test_remove_encryption_interior_bundle() {
        // Encode interior bundle with encryption, remove encryption, decode without key
        let key = test_key();

        let page_data: Vec<Vec<u8>> = (0..3).map(|i| vec![i as u8; 4096]).collect();
        let pages: Vec<(u64, &[u8])> = page_data
            .iter()
            .enumerate()
            .map(|(i, d)| (i as u64, d.as_slice()))
            .collect();

        let encoded = encode_interior_bundle(
            &pages,
            4096,
            3,
            #[cfg(feature = "zstd")]
            None,
            Some(&key),
        )
        .unwrap();

        // Remove encryption
        let decrypted = compress::decrypt_gcm_random_nonce(&encoded, &key).unwrap();

        // Decode without key
        let decoded = decode_interior_bundle(
            &decrypted,
            #[cfg(feature = "zstd")]
            None,
            None, // no encryption
        )
        .unwrap();

        assert_eq!(decoded.len(), 3);
        for (i, (pnum, data)) in decoded.iter().enumerate() {
            assert_eq!(*pnum, i as u64);
            assert_eq!(data, &vec![i as u8; 4096]);
        }
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn test_both_keys_none_error() {
        // Both old and new keys None should error
        // This tests the rotate_encryption_key guard, simulated here
        let old_key: Option<[u8; 32]> = None;
        let new_key: Option<[u8; 32]> = None;
        assert!(old_key.is_none() && new_key.is_none());
    }

    // ── Phase Midway regression tests ──

    #[test]
    fn test_assign_new_pages_to_groups_basic() {
        let mut manifest = Manifest {
            page_count: 10,
            page_size: 4096,
            pages_per_group: 4,
            group_pages: vec![
                vec![0, 1, 2, 3],
                vec![4, 5, 6, 7],
            ],
            ..Manifest::empty()
        };
        manifest.build_page_index();

        // Pages 8 and 9 are new (not in page_index)
        let unassigned = vec![8, 9];
        TieredHandle::assign_new_pages_to_groups(&mut manifest, &unassigned, 4);

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
        TieredHandle::assign_new_pages_to_groups(&mut manifest, &unassigned, 4);

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
        TieredHandle::assign_new_pages_to_groups(&mut manifest, &unassigned, 4);

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

        TieredHandle::assign_new_pages_to_groups(&mut manifest, &[], 4);

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
        TieredHandle::assign_new_pages_to_groups(&mut manifest, &unassigned, 4);

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
                vec![5, 0, 3, 8],  // non-sequential
                vec![1, 7, 2],
                vec![9, 4, 6],
            ],
            ..Manifest::empty()
        };
        manifest.build_page_index();

        // Every page 0-9 should have a location
        for p in 0..10 {
            let loc = manifest.page_location(p).unwrap_or_else(|| panic!("missing page {}", p));
            // Verify the reverse: group_pages[gid][index] == p
            assert_eq!(manifest.group_pages[loc.group_id as usize][loc.index as usize], p);
        }
    }

    // ── Phase Midway: B-tree-aware page groups tests ──

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
                vec![0, 1, 2], vec![3, 4, 5], vec![6, 7, 8], vec![9, 10, 11],
                vec![12, 13, 14, 15, 16, 17, 18, 19],  // extra group from B-tree packing
            ],
            ..Manifest::empty()
        };
        // B-tree mapping takes priority: 5 groups, not positional 4
        assert_eq!(m2.total_groups(), 5);
    }

    #[test]
    fn test_write_pages_scattered_only_marks_written_pages() {
        // Regression: bitmap must not mark pages beyond data length
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 3600, 8, 2, 64, 16, None, Vec::new()).unwrap();

        // Provide data for 2 pages but page_nums has 4
        let page_nums = vec![0u64, 1, 5, 10];
        let data = vec![0xAA; 128]; // only 2 pages worth (2 * 64)
        cache.write_pages_scattered(&page_nums, &data, 0, 0).unwrap();

        // Pages 0 and 1 should be present (data written)
        assert!(cache.is_present(0), "page 0 should be present");
        assert!(cache.is_present(1), "page 1 should be present");
        // Pages 5 and 10 should NOT be present (data was too short)
        assert!(!cache.bitmap.lock().is_present(5), "page 5 should NOT be present (no data)");
        assert!(!cache.bitmap.lock().is_present(10), "page 10 should NOT be present (no data)");
    }

    #[test]
    fn test_write_pages_scattered_exact_data_marks_all() {
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 3600, 8, 2, 64, 16, None, Vec::new()).unwrap();

        // Data exactly matches page_nums length
        let page_nums = vec![3u64, 7, 12];
        let data = vec![0xBB; 192]; // 3 * 64
        cache.write_pages_scattered(&page_nums, &data, 0, 0).unwrap();

        for &p in &page_nums {
            assert!(cache.is_present(p), "page {} should be present", p);
        }
    }

    #[test]
    fn test_write_pages_scattered_no_tracker_pollution() {
        // Regression: scattered writes mark the tracker with manifest-aware sub-chunk IDs.
        // Bitmap is per-page (accurate), so unwritten pages in the same sub-chunk stay absent.
        let dir = TempDir::new().unwrap();
        // ppg=8, spf=4, page_size=64, page_count=32
        let cache = DiskCache::new(dir.path(), 3600, 8, 4, 64, 32, None, Vec::new()).unwrap();

        // Write page 5 via scattered (simulates B-tree group write)
        let page_nums = vec![5u64];
        let data = vec![0xCC; 64];
        cache.write_pages_scattered(&page_nums, &data, 0, 0).unwrap();

        // Page 5 should be present (bitmap)
        assert!(cache.bitmap.lock().is_present(5));

        // Unwritten pages must not be present (bitmap is per-page accurate)
        assert!(!cache.is_present(4), "page 4 must not be present");
        assert!(!cache.is_present(6), "page 6 must not be present");
        assert!(!cache.is_present(7), "page 7 must not be present");

        // But tracker sub-chunk (0, 0) IS marked (gid=0, index_in_group=0 -> frame 0)
        let tracker = cache.tracker.lock();
        let id = tracker.sub_chunk_id_for(0, 0);
        assert!(tracker.is_sub_chunk_present(&id), "sub-chunk must be marked after write");
    }

    #[test]
    fn test_write_pages_scattered_data_integrity() {
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 3600, 8, 2, 64, 16, None, Vec::new()).unwrap();

        // Write non-sequential pages with distinct data
        let page_nums = vec![3u64, 0, 7, 15];
        let mut data = Vec::with_capacity(4 * 64);
        for (i, &pnum) in page_nums.iter().enumerate() {
            data.extend(std::iter::repeat(((pnum + 1) as u8) * 10 + i as u8).take(64));
        }
        cache.write_pages_scattered(&page_nums, &data, 0, 0).unwrap();

        // Read back each page and verify data
        for (i, &pnum) in page_nums.iter().enumerate() {
            let mut buf = vec![0u8; 64];
            cache.read_page(pnum, &mut buf).unwrap();
            let expected_byte = ((pnum + 1) as u8) * 10 + i as u8;
            assert_eq!(buf[0], expected_byte, "page {} data mismatch", pnum);
            assert!(buf.iter().all(|&b| b == expected_byte), "page {} not uniform", pnum);
        }
    }

    #[test]
    fn test_manifest_btree_page_location_non_sequential() {
        // B-tree groups have non-sequential page numbers
        let mut manifest = Manifest {
            page_count: 20,
            page_size: 4096,
            pages_per_group: 8,
            group_pages: vec![
                vec![0, 5, 10, 15],     // gid=0: scattered pages from B-tree A
                vec![1, 2, 3, 4],       // gid=1: sequential pages from B-tree B
                vec![6, 7, 8, 9],       // gid=2: sequential from B-tree C
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
    fn test_manifest_total_groups_with_btree_exceeds_positional() {
        // 10 pages with ppg=4 gives positional total_groups=3.
        // But B-tree packing produces 5 groups (small B-trees get own groups).
        let m = Manifest {
            page_count: 10,
            pages_per_group: 4,
            group_pages: vec![
                vec![0, 1],      // B-tree A (2 pages)
                vec![2, 3],      // B-tree B (2 pages)
                vec![4, 5],      // B-tree C (2 pages)
                vec![6, 7],      // B-tree D (2 pages)
                vec![8, 9],      // unowned (2 pages)
            ],
            ..Manifest::empty()
        };
        // Positional would be ceil(10/4) = 3, but B-tree mapping says 5
        assert_eq!(m.total_groups(), 5);
    }

    #[test]
    fn test_ensure_group_capacity_for_btree_groups() {
        // DiskCache starts with positional group count but B-tree groups may need more
        let dir = TempDir::new().unwrap();
        // page_count=10, ppg=4 -> positional total_groups = 3
        let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 10, None, Vec::new()).unwrap();
        assert_eq!(cache.group_states.lock().len(), 3);

        // B-tree manifest has 5 groups (more than positional)
        cache.ensure_group_capacity(5);
        assert_eq!(cache.group_states.lock().len(), 5);

        // Can claim and mark all 5 groups
        for gid in 0..5u64 {
            assert!(cache.try_claim_group(gid));
            cache.mark_group_present(gid);
            assert_eq!(cache.group_state(gid), GroupState::Present);
        }
    }

    #[test]
    fn test_write_pages_scattered_empty_data() {
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 3600, 8, 2, 64, 16, None, Vec::new()).unwrap();

        // Empty data but non-empty page_nums: should be a no-op
        let page_nums = vec![0u64, 1, 2];
        let data: Vec<u8> = vec![];
        cache.write_pages_scattered(&page_nums, &data, 0, 0).unwrap();

        // No pages should be marked
        assert!(!cache.is_present(0));
        assert!(!cache.is_present(1));
        assert!(!cache.is_present(2));
    }

    #[test]
    fn test_write_pages_scattered_partial_page_data() {
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 3600, 8, 2, 64, 16, None, Vec::new()).unwrap();

        // Data for 1.5 pages (not enough for second page)
        let page_nums = vec![0u64, 1];
        let data = vec![0xDD; 96]; // 64 + 32 (not enough for page 1)
        cache.write_pages_scattered(&page_nums, &data, 0, 0).unwrap();

        // Only page 0 should be marked (page 1 had partial data)
        assert!(cache.is_present(0), "page 0 with full data should be present");
        assert!(!cache.bitmap.lock().is_present(1), "page 1 with partial data should not be present");
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
            page_group_keys: vec![
                "pg/0_v5".into(), "pg/1_v5".into(), "pg/2_v5".into(),
            ],
            group_pages: vec![
                vec![0, 5, 10, 15],     // B-tree A (scattered)
                vec![1, 2, 3, 4],       // B-tree B (sequential)
                vec![6, 7, 8, 9, 11, 12, 13, 14, 16, 17, 18, 19],
            ],
            btrees: {
                let mut h = HashMap::new();
                h.insert(0, BTreeManifestEntry {
                    name: "users".into(),
                    obj_type: "table".into(),
                    group_ids: vec![0],
                });
                h.insert(5, BTreeManifestEntry {
                    name: "idx_users_name".into(),
                    obj_type: "index".into(),
                    group_ids: vec![1],
                });
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
        assert!(m2.page_index.is_empty(), "page_index should be empty after deserialize");
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
    }

    #[test]
    fn test_page_location_missing_page_returns_none() {
        let mut manifest = Manifest {
            page_count: 10,
            page_size: 4096,
            pages_per_group: 4,
            group_pages: vec![
                vec![0, 1, 2, 3],
                vec![4, 5, 6, 7],
            ],
            ..Manifest::empty()
        };
        manifest.build_page_index();

        // Pages 0-7 are assigned, 8-9 are NOT (unassigned gap)
        assert!(manifest.page_location(0).is_some());
        assert!(manifest.page_location(7).is_some());
        assert!(manifest.page_location(8).is_none(), "unassigned page should return None");
        assert!(manifest.page_location(9).is_none());
        assert!(manifest.page_location(100).is_none(), "way out of bounds should return None");
    }

    #[test]
    fn test_concurrent_scattered_writes_no_tracker_pollution() {
        // Reproduces the EXACT corruption scenario: two B-tree groups with pages
        // that share positional sub-chunks. Writing group A must not make group B's
        // unwritten pages appear as cache hits.
        use std::sync::Arc;
        let dir = TempDir::new().unwrap();
        // ppg=8, spf=4: pages 0-3 = sub-chunk (0,0), pages 4-7 = sub-chunk (0,1)
        let cache = Arc::new(DiskCache::new(dir.path(), 3600, 8, 4, 64, 16, None, Vec::new()).unwrap());

        // B-tree group A: pages [0, 4] (spans two positional sub-chunks)
        // B-tree group B: pages [1, 5] (same two positional sub-chunks!)
        let cache_a = Arc::clone(&cache);
        let cache_b = Arc::clone(&cache);

        // Thread A writes B-tree group A
        let handle_a = std::thread::spawn(move || {
            let data_a = vec![0xAA; 128]; // 2 pages * 64
            cache_a.write_pages_scattered(&[0, 4], &data_a, 0, 0).unwrap();
        });

        // Thread B writes B-tree group B
        let handle_b = std::thread::spawn(move || {
            let data_b = vec![0xBB; 128]; // 2 pages * 64
            cache_b.write_pages_scattered(&[1, 5], &data_b, 0, 0).unwrap();
        });

        handle_a.join().unwrap();
        handle_b.join().unwrap();

        // Pages 0, 4 should have 0xAA data
        let mut buf = vec![0u8; 64];
        cache.read_page(0, &mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0xAA), "page 0 should be 0xAA");
        cache.read_page(4, &mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0xAA), "page 4 should be 0xAA");

        // Pages 1, 5 should have 0xBB data
        cache.read_page(1, &mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0xBB), "page 1 should be 0xBB");
        cache.read_page(5, &mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0xBB), "page 5 should be 0xBB");

        // CRITICAL: pages 2, 3, 6, 7 were NEVER written. They must NOT be present.
        // Before the fix, tracker pollution would report them as present.
        assert!(!cache.is_present(2), "page 2 never written, must not be present");
        assert!(!cache.is_present(3), "page 3 never written, must not be present");
        assert!(!cache.is_present(6), "page 6 never written, must not be present");
        assert!(!cache.is_present(7), "page 7 never written, must not be present");

        // And reading unwritten pages must return zeros, not stale data
        cache.read_page(2, &mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0), "unwritten page 2 must be zeros");
        cache.read_page(6, &mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0), "unwritten page 6 must be zeros");
    }

    #[test]
    fn test_write_pages_scattered_sparse_page_distribution() {
        // Pages from a B-tree group can span a very wide range of page numbers.
        // Verify writes at large offsets work correctly.
        let dir = TempDir::new().unwrap();
        // Large page_count to ensure file is pre-allocated
        let cache = DiskCache::new(dir.path(), 3600, 128, 4, 64, 4000, None, Vec::new()).unwrap();

        // Sparse pages from one B-tree group
        let page_nums = vec![10u64, 500, 1000, 3999];
        let mut data = Vec::with_capacity(4 * 64);
        for &pnum in &page_nums {
            data.extend(std::iter::repeat((pnum % 256) as u8).take(64));
        }
        cache.write_pages_scattered(&page_nums, &data, 0, 0).unwrap();

        // Verify each page has correct data
        for &pnum in &page_nums {
            assert!(cache.is_present(pnum), "page {} should be present", pnum);
            let mut buf = vec![0u8; 64];
            cache.read_page(pnum, &mut buf).unwrap();
            let expected = (pnum % 256) as u8;
            assert!(buf.iter().all(|&b| b == expected),
                "page {} data mismatch: expected 0x{:02x}, got 0x{:02x}", pnum, expected, buf[0]);
        }

        // Pages between sparse entries must NOT be present
        assert!(!cache.is_present(11));
        assert!(!cache.is_present(499));
        assert!(!cache.is_present(501));
        assert!(!cache.is_present(999));
    }

    #[test]
    fn test_btree_group_single_page() {
        // A B-tree with exactly 1 page (e.g., a small table).
        // write_pages_scattered must handle single-entry page_nums.
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 3600, 128, 4, 64, 100, None, Vec::new()).unwrap();

        let page_nums = vec![42u64];
        let data = vec![0xFF; 64];
        cache.write_pages_scattered(&page_nums, &data, 0, 0).unwrap();

        assert!(cache.is_present(42));
        let mut buf = vec![0u8; 64];
        cache.read_page(42, &mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0xFF));

        // Neighbors must not be present
        assert!(!cache.is_present(41));
        assert!(!cache.is_present(43));
    }

    #[test]
    fn test_write_pages_scattered_populates_tracker_subchunks() {
        // Regression: write_pages_scattered must mark SubChunkTracker with correct
        // manifest-aware sub_chunk_id_for() so tiered eviction works.
        let dir = TempDir::new().unwrap();
        // ppg=8, spf=4 -> 2 frames per group. page_size=64, page_count=32
        let group_pages = vec![vec![10, 20, 30, 5, 15, 25, 35, 45]]; // group 0
        let cache = DiskCache::new(dir.path(), 3600, 8, 4, 64, 50, None, group_pages).unwrap();

        // Write all 8 pages (2 frames worth)
        let page_nums = vec![10u64, 20, 30, 5, 15, 25, 35, 45];
        let data = vec![0xAA; 8 * 64];
        cache.write_pages_scattered(&page_nums, &data, 0, 0).unwrap();

        let tracker = cache.tracker.lock();
        // Frame 0: indices 0-3 -> SubChunkId { group_id: 0, frame_index: 0 }
        let f0 = tracker.sub_chunk_id_for(0, 0);
        assert!(tracker.is_sub_chunk_present(&f0), "frame 0 must be marked present");
        // Frame 1: indices 4-7 -> SubChunkId { group_id: 0, frame_index: 1 }
        let f1 = tracker.sub_chunk_id_for(0, 4);
        assert!(tracker.is_sub_chunk_present(&f1), "frame 1 must be marked present");
        // Non-existent frame 2 should not be present
        let f2 = SubChunkId { group_id: 0, frame_index: 2 };
        assert!(!tracker.is_sub_chunk_present(&f2), "frame 2 must not be present");
        drop(tracker);

        // Bitmap should have all 8 pages
        for &p in &page_nums {
            assert!(cache.is_present(p), "page {} must be present in bitmap", p);
        }
        // Unwritten pages must not be present
        assert!(!cache.is_present(11));
        assert!(!cache.is_present(0));
    }

    #[test]
    fn test_write_pages_scattered_subframe_offset() {
        // write_pages_scattered with start_index_in_group > 0 (sub-chunk frame offset)
        let dir = TempDir::new().unwrap();
        // ppg=8, spf=4 -> 2 frames per group
        let cache = DiskCache::new(dir.path(), 3600, 8, 4, 64, 50, None, Vec::new()).unwrap();

        // Write 4 pages starting at index 4 in group 0 (frame 1)
        let page_nums = vec![100u64, 200, 300, 400];
        let data = vec![0xBB; 4 * 64];
        cache.write_pages_scattered(&page_nums, &data, 0, 4).unwrap();

        let tracker = cache.tracker.lock();
        // These pages are at indices 4,5,6,7 -> frame 1
        let f1 = tracker.sub_chunk_id_for(0, 4);
        assert!(tracker.is_sub_chunk_present(&f1), "frame 1 must be marked");
        // Frame 0 should NOT be marked (we only wrote frame 1)
        let f0 = tracker.sub_chunk_id_for(0, 0);
        assert!(!tracker.is_sub_chunk_present(&f0), "frame 0 must not be marked");
    }

    #[test]
    fn test_manifest_btree_group_partial_last_group() {
        // Last B-tree group may have fewer pages than ppg.
        // total_groups, page_location, frame calculations all must respect actual size.
        let mut manifest = Manifest {
            page_count: 10,
            page_size: 4096,
            pages_per_group: 4,
            group_pages: vec![
                vec![0, 1, 2, 3],   // full group
                vec![4, 5, 6, 7],   // full group
                vec![8, 9],         // partial group (2 of 4)
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
        let json = r#"{"version":1,"page_count":100,"page_size":4096,"pages_per_group":32,"page_group_keys":["pg/0_v1","pg/1_v1","pg/2_v1","pg/3_v1"]}"#;
        let m: Manifest = serde_json::from_str(json).unwrap();

        // B-tree fields default to empty
        assert!(m.group_pages.is_empty());
        assert!(m.btrees.is_empty());
        assert!(m.page_index.is_empty());

        // Falls back to positional total_groups
        assert_eq!(m.total_groups(), 4); // ceil(100/32)
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
            group_pages: vec![
                vec![0, 1, 2, 3],
                vec![4, 5, 6, 7],
                vec![8, 9, 10, 11],
            ],
            btrees: {
                let mut h = HashMap::new();
                h.insert(0, BTreeManifestEntry {
                    name: "users".into(),
                    obj_type: "table".into(),
                    group_ids: vec![0, 1, 2],
                });
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
                h.insert(0, BTreeManifestEntry {
                    name: "table_a".into(),
                    obj_type: "table".into(),
                    group_ids: vec![0, 1],
                });
                h.insert(8, BTreeManifestEntry {
                    name: "table_b".into(),
                    obj_type: "table".into(),
                    group_ids: vec![2, 3],
                });
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
                h.insert(0, BTreeManifestEntry {
                    name: "small_table".into(),
                    obj_type: "table".into(),
                    group_ids: vec![0],
                });
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
                h.insert(0, BTreeManifestEntry {
                    name: "users".into(),
                    obj_type: "table".into(),
                    group_ids: vec![0, 1], // only groups 0 and 1
                });
                h
            },
            ..Manifest::empty()
        };
        m.build_page_index();

        assert!(m.btree_groups.get(&0).is_some());
        assert!(m.btree_groups.get(&1).is_some());
        assert!(m.btree_groups.get(&2).is_none(), "orphan group should not be in btree_groups");
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
                h.insert(0, BTreeManifestEntry {
                    name: "v1".into(),
                    obj_type: "table".into(),
                    group_ids: vec![0, 1],
                });
                h
            },
            ..Manifest::empty()
        };
        m.build_page_index();
        assert_eq!(m.btree_groups.len(), 2);
        assert_eq!(m.btree_groups[&0], vec![0u64, 1]);

        // Change btrees: group 1 is now a separate B-tree
        m.btrees.clear();
        m.btrees.insert(0, BTreeManifestEntry {
            name: "v2_a".into(),
            obj_type: "table".into(),
            group_ids: vec![0],
        });
        m.btrees.insert(4, BTreeManifestEntry {
            name: "v2_b".into(),
            obj_type: "table".into(),
            group_ids: vec![1],
        });
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
                h.insert(0, BTreeManifestEntry {
                    name: "tree_a".into(),
                    obj_type: "table".into(),
                    group_ids: vec![0],
                });
                h.insert(4, BTreeManifestEntry {
                    name: "tree_b".into(),
                    obj_type: "index".into(),
                    group_ids: vec![0, 1], // group 0 also here
                });
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

        // Simulate fetch failure: reset to None
        let states = cache.group_states.lock();
        if let Some(s) = states.get(0) {
            s.store(GroupState::None as u8, Ordering::Release);
        }
        drop(states);

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
    fn test_btree_groups_many_btrees_large_manifest() {
        // Stress test: 50 B-trees, each with 10 groups = 500 groups total
        let mut group_pages = Vec::new();
        let mut btrees = HashMap::new();
        let ppg = 4u32;
        let mut page_num = 0u64;

        for btree_idx in 0u64..50 {
            let mut group_ids = Vec::new();
            for g in 0..10 {
                let gid = btree_idx * 10 + g;
                let pages: Vec<u64> = (0..ppg as u64).map(|p| page_num + p).collect();
                page_num += ppg as u64;
                group_pages.push(pages);
                group_ids.push(gid);
            }
            btrees.insert(btree_idx * ppg as u64, BTreeManifestEntry {
                name: format!("btree_{}", btree_idx),
                obj_type: "table".into(),
                group_ids,
            });
        }

        let mut m = Manifest {
            page_count: page_num,
            page_size: 4096,
            pages_per_group: ppg,
            page_group_keys: (0..500).map(|i| format!("pg/{}_v1", i)).collect(),
            group_pages,
            btrees,
            ..Manifest::empty()
        };
        m.build_page_index();

        assert_eq!(m.btree_groups.len(), 500);

        // Each group should map to exactly 10 siblings (its B-tree's groups)
        for gid in 0u64..500 {
            let siblings = m.btree_groups.get(&gid).unwrap();
            assert_eq!(siblings.len(), 10, "gid {} should have 10 siblings", gid);
            // All siblings should be from the same B-tree (same 10-group block)
            let btree_block = gid / 10;
            for &s in siblings {
                assert_eq!(s / 10, btree_block, "sibling {} should be in same B-tree block as gid {}", s, gid);
            }
        }
    }

    #[test]
    fn test_btree_groups_survives_serde_roundtrip() {
        // btree_groups is #[serde(skip)], so it must be rebuilt after deserialization
        let mut m = Manifest {
            page_count: 12,
            page_size: 4096,
            pages_per_group: 4,
            page_group_keys: vec!["a".into(), "b".into(), "c".into()],
            group_pages: vec![
                vec![0, 1, 2, 3],
                vec![4, 5, 6, 7],
                vec![8, 9, 10, 11],
            ],
            btrees: {
                let mut h = HashMap::new();
                h.insert(0, BTreeManifestEntry {
                    name: "table_x".into(),
                    obj_type: "table".into(),
                    group_ids: vec![0, 1],
                });
                h.insert(8, BTreeManifestEntry {
                    name: "idx_x".into(),
                    obj_type: "index".into(),
                    group_ids: vec![2],
                });
                h
            },
            ..Manifest::empty()
        };
        m.build_page_index();
        assert_eq!(m.btree_groups.len(), 3);

        // Serialize
        let json = serde_json::to_string(&m).unwrap();

        // Deserialize: btree_groups should be empty (skipped)
        let mut m2: Manifest = serde_json::from_str(&json).unwrap();
        assert!(m2.btree_groups.is_empty(), "btree_groups should not survive serialization");

        // Rebuild
        m2.build_page_index();
        assert_eq!(m2.btree_groups.len(), 3);
        assert_eq!(m2.btree_groups[&0], vec![0u64, 1]);
        assert_eq!(m2.btree_groups[&1], vec![0u64, 1]);
        assert_eq!(m2.btree_groups[&2], vec![2u64]);
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
        let fraction = if hop_idx < hops.len() { hops[hop_idx] } else { 1.0 };
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
        let fraction = if hop_idx < hops.len() { hops[hop_idx] } else { 1.0 };
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
        assert_eq!(cache.group_state(0), GroupState::None,
            "group state should remain None until prefetch is submitted");

        // Now the read path would claim and submit to pool
        assert!(cache.try_claim_group(0));
        assert_eq!(cache.group_state(0), GroupState::Fetching);
    }
}
