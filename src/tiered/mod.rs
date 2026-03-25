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
mod flush;
mod handle;
mod import;
mod manifest;
mod prediction;
mod prefetch;
mod query_plan;
mod rotation;
mod s3_client;
mod vfs;

// Public API (visible outside the crate)
pub use bench::TieredBenchHandle;
pub use config::{GroupState, GroupingStrategy, PrefetchNeighbors, SyncMode, TieredConfig, PageLocation, BTreeManifestEntry};
pub use handle::TieredHandle;
pub use import::import_sqlite_file;
pub use manifest::{FrameEntry, Manifest};
pub use vfs::TieredVfs;
pub use query_plan::{AccessType, PlannedAccess, parse_eqp_output, push_planned_accesses};
#[cfg(feature = "encryption")]
pub use rotation::rotate_encryption_key;

// Crate-internal re-exports (accessible within tiered submodules via super::*)
pub(crate) use cache_tracking::*;
pub(crate) use disk_cache::*;
pub(crate) use encoding::*;
pub(crate) use prefetch::*;
pub(crate) use query_plan::*;
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

/// Check if a manifest exists at the given S3 prefix. Returns the manifest if found.
/// Useful for checking whether data has already been imported before re-importing.
pub fn get_manifest(config: &TieredConfig) -> std::io::Result<Option<Manifest>> {
    let runtime = tokio::runtime::Runtime::new()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let handle = runtime.handle().clone();
    let s3_cfg = TieredConfig {
        bucket: config.bucket.clone(),
        prefix: config.prefix.clone(),
        endpoint_url: config.endpoint_url.clone(),
        region: config.region.clone(),
        runtime_handle: Some(handle.clone()),
        ..Default::default()
    };
    let s3 = S3Client::block_on(&handle, S3Client::new_async(&s3_cfg))?;
    s3.get_manifest()
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
#[path = "test_mod.rs"]
mod tests;
