//! Turbolite VFS: compressed + encrypted SQLite storage.
//!
//! Backend-agnostic: all bytes flow through an
//! `Arc<dyn hadb_storage::StorageBackend>` that the embedder picks. The VFS
//! ships with `TurboliteVfs::new` (wires up `hadb_storage_local::LocalStorage`
//! rooted at `config.cache_dir`) and `TurboliteVfs::with_backend` (you
//! bring the backend + tokio handle).
//!
//! # Quick start (local mode)
//!
//! ```ignore
//! use turbolite::tiered::{TurboliteVfs, TurboliteConfig};
//!
//! let config = TurboliteConfig {
//!     cache_dir: "/data/mydb".into(),
//!     ..Default::default()
//! };
//! let vfs = TurboliteVfs::new_local(config)?;
//! turbolite::tiered::register("mydb", vfs)?;
//! // Now open with rusqlite: "file:test.db?vfs=mydb"
//! ```
//!
//! # Architecture
//!
//! Both modes use the same format: a manifest tracks page-to-group assignments,
//! and page groups are compressed blobs containing multiple pages. Switching from
//! local to cloud is a config change (same files, same manifest).
//!
//! - Default: 64KB pages, 256 pages per group (16MB uncompressed, ~8MB compressed)
//! - Optional zstd compression with dictionary support (2-5x smaller on structured data)
//! - Optional AES-256 encryption (CTR for cache, GCM for cloud page groups)
//! - Optional compressed local cache (saves disk space, costs CPU on read)
//!
//! Cloud mode adds:
//! - Prefetch thread pool for parallel S3 range GETs
//! - Sub-chunk caching (fetch 256KB instead of full 16MB group for point lookups)
//! - Interior/index bundle eager loading on connection open
//! - Two-phase checkpoint (local-then-flush) for low-latency writes
//!
//! Interior B-tree pages (type bytes 0x05, 0x02) are pinned permanently -- never evicted.
//! They represent <1% of the database but are hit on every query.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions as FsOpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
// flume channels used by PrefetchPool (sync/async-agnostic)
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sqlite_vfs::{DatabaseHandle, LockKind, OpenKind, OpenOptions, Vfs};

use crate::compress;

// Re-use the FileWalIndex from the main lib
use crate::FileWalIndex;

// --- Extracted submodules ---
mod async_rt;
mod bench;
mod cache_tracking;
mod compact;
mod config;
mod disk_cache;
mod encoding;
mod flush;
mod handle;
mod import;
mod keys;
mod manifest;
mod prediction;
mod prefetch;
mod query_plan;
mod rotation;
pub mod settings;
#[cfg(feature = "wal")]
mod snapshot_source;
mod staging;
mod storage;
mod vfs;
#[cfg(feature = "wal")]
mod wal_replication;
mod wire;

// Public API (visible outside the crate)
pub use bench::TurboliteSharedState;
#[cfg(feature = "wal")]
pub use config::WalConfig;
pub use config::{
    BTreeManifestEntry, GroupState, GroupingStrategy, ManifestSource, PageLocation, TurboliteConfig,
};
pub use config::{CacheConfig, CompressionConfig, EncryptionConfig, PrefetchConfig};
pub use handle::TurboliteHandle;
pub use import::import_sqlite_file;
pub use manifest::{FrameEntry, Manifest, SubframeOverride};
pub use vfs::TurboliteVfs;
// SharedTurboliteVfs and register_shared are exported from mod.rs directly (defined below)
pub use query_plan::{
    check_and_clear_end_query, parse_eqp_output, push_planned_accesses, run_eqp_and_parse,
    signal_end_query, AccessType, PlannedAccess,
};
#[cfg(feature = "encryption")]
pub use rotation::rotate_encryption_key;
#[cfg(feature = "wal")]
pub use snapshot_source::TurboliteSnapshotSource;

/// Result of a database validation run (manifest + data integrity).
/// The caller should run `PRAGMA integrity_check` separately via the connection.
#[derive(Debug)]
pub struct ValidateResult {
    pub manifest_version: u64,
    pub page_groups_total: usize,
    pub page_groups_present: usize,
    pub page_groups_missing: Vec<String>,
    pub interior_chunks_total: usize,
    pub interior_chunks_present: usize,
    pub interior_chunks_missing: Vec<String>,
    pub index_chunks_total: usize,
    pub index_chunks_present: usize,
    pub index_chunks_missing: Vec<String>,
    pub orphaned_keys: Vec<String>,
    pub decode_errors: Vec<(String, String)>,
}

impl ValidateResult {
    /// True if all S3 keys are present and all data decodes correctly.
    /// Does NOT include SQLite integrity check (caller runs that separately).
    pub fn s3_ok(&self) -> bool {
        self.page_groups_missing.is_empty()
            && self.interior_chunks_missing.is_empty()
            && self.index_chunks_missing.is_empty()
            && self.decode_errors.is_empty()
    }
}

// Backward-compat type aliases (deprecated, use Turbolite* names)
#[deprecated(note = "renamed to TurboliteVfs")]
pub type TieredVfs = TurboliteVfs;
#[deprecated(note = "renamed to TurboliteHandle")]
pub type TieredHandle = TurboliteHandle;
#[deprecated(note = "renamed to TurboliteConfig")]
pub type TieredConfig = TurboliteConfig;
#[deprecated(note = "renamed to TurboliteSharedState")]
pub type TieredSharedState = TurboliteSharedState;

// Crate-internal re-exports (accessible within tiered submodules via super::*)
pub(crate) use cache_tracking::*;
pub(crate) use disk_cache::*;
pub(crate) use encoding::*;
pub(crate) use prefetch::*;

// ===== Constants =====

/// Default pages per page group (256 x 64KB = 16MB uncompressed, ~8MB compressed).
/// At 4KB page size this is 1MB per group; increase if using small pages.
const DEFAULT_PAGES_PER_GROUP: u32 = 256;

/// Default pages per sub-chunk frame for seekable encoding.
/// At 64KB page size: 4 x 64KB = 256KB per frame, ~128KB compressed per range GET.
const DEFAULT_SUB_PAGES_PER_FRAME: u32 = 4;

/// Target ~32MB uncompressed per bundle chunk.
/// Chunk range = how many page numbers each chunk covers.
/// At 64KB pages: 512 page range, worst case 32MB per chunk.
/// At  4KB pages: 8192 page range, worst case 32MB per chunk.
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

/// Fetch the manifest from a backend. Thin helper; consumers can also call
/// `backend.get(keys::MANIFEST_KEY)` themselves and decode.
pub fn get_manifest(
    backend: &dyn hadb_storage::StorageBackend,
    runtime: &tokio::runtime::Handle,
) -> std::io::Result<Option<Manifest>> {
    let bytes = async_rt::block_on(runtime, backend.get(keys::MANIFEST_KEY)).map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::Other, format!("fetch manifest: {e}"))
    })?;
    match bytes {
        None => Ok(None),
        Some(bytes) => {
            let mut m = manifest::decode_manifest_bytes(&bytes)?;
            m.build_page_index();
            Ok(Some(m))
        }
    }
}

// ===== SQLite file change counter =====

/// Read SQLite's file change counter from page 0 data.
/// Offset 24 in the database header, 4 bytes big-endian.
/// Returns 0 if page data is too short.
///
/// This counter increments on every transaction commit. Used as the unified
/// version number for both turbolite manifests and walrust WAL txids.
pub(crate) fn read_file_change_counter(page0: &[u8]) -> u64 {
    if page0.len() < 28 {
        return 0;
    }
    u32::from_be_bytes([page0[24], page0[25], page0[26], page0[27]]) as u64
}

/// Read the file change counter from page 0 in the disk cache.
/// Panics if page 0 is not cached or counter is 0. A wrong version
/// number would corrupt data on recovery (walrust would replay WAL
/// segments over the wrong page state).
pub(crate) fn read_change_counter_from_cache(cache: &DiskCache, page_size: u32) -> u64 {
    let mut page0 = vec![0u8; page_size as usize];
    cache
        .read_page(0, &mut page0)
        .expect("page 0 must be in cache at checkpoint time");
    let counter = read_file_change_counter(&page0);
    assert!(
        counter > 0,
        "file change counter must be > 0 at checkpoint time"
    );
    counter
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
    if content_area != 0 && (content_area < min_content_offset || content_area as usize > buf.len())
    {
        return false;
    }
    // Fragment bytes must be < 255 (at most 60 in practice)
    if frag_bytes > 128 {
        return false;
    }
    true
}

/// Process-global set of turbolite VFS names. Written by [`register`] /
/// [`register_shared`], read by the auto-extension install hook so it
/// only binds `turbolite_config_set` on connections actually using a
/// turbolite VFS — not on unrelated sqlite3 connections that happen to
/// open on a thread with an active turbolite handle.
///
/// Names never leave the set; turbolite VFS registrations live for the
/// process lifetime today. If that ever changes, add removal here too.
static REGISTERED_VFS_NAMES: once_cell::sync::Lazy<
    parking_lot::Mutex<std::collections::HashSet<String>>,
> = once_cell::sync::Lazy::new(|| parking_lot::Mutex::new(std::collections::HashSet::new()));

/// True if `name` was registered via `turbolite::tiered::register` or
/// `register_shared`. Used by the install-hook's VFS guard.
pub fn is_registered_vfs_name(name: &str) -> bool {
    REGISTERED_VFS_NAMES.lock().contains(name)
}

/// Register a TurboliteVfs with SQLite under the given name.
pub fn register(name: &str, vfs: TurboliteVfs) -> Result<(), io::Error> {
    sqlite_vfs::register(name, vfs, false)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))?;
    REGISTERED_VFS_NAMES.lock().insert(name.to_string());
    Ok(())
}

/// Shared VFS wrapper: holds the VFS behind an Arc so it can be registered
/// with SQLite while keeping a handle for `manifest()` / `set_manifest()`.
///
/// ```ignore
/// let vfs = TurboliteVfs::new_local(config)?;
/// let shared = SharedTurboliteVfs::new(vfs);
/// let vfs_ref = shared.clone(); // keep this for manifest ops
/// turbolite::tiered::register_shared("mydb", shared)?;
/// // vfs_ref.manifest() and vfs_ref.set_manifest() still work
/// ```
#[derive(Clone)]
pub struct SharedTurboliteVfs(Arc<TurboliteVfs>);

impl SharedTurboliteVfs {
    pub fn new(vfs: TurboliteVfs) -> Self {
        Self(Arc::new(vfs))
    }

    /// Access the underlying VFS (for calling manifest(), set_manifest(), etc).
    pub fn vfs(&self) -> &TurboliteVfs {
        &self.0
    }
}

impl std::ops::Deref for SharedTurboliteVfs {
    type Target = TurboliteVfs;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Vfs for SharedTurboliteVfs {
    type Handle = TurboliteHandle;

    fn open(&self, db: &str, opts: OpenOptions) -> Result<TurboliteHandle, io::Error> {
        self.0.open(db, opts)
    }

    fn delete(&self, db: &str) -> Result<(), io::Error> {
        self.0.delete(db)
    }

    fn exists(&self, db: &str) -> Result<bool, io::Error> {
        self.0.exists(db)
    }

    fn temporary_name(&self) -> String {
        self.0.temporary_name()
    }

    fn random(&self, buffer: &mut [i8]) {
        self.0.random(buffer)
    }

    fn sleep(&self, duration: Duration) -> Duration {
        self.0.sleep(duration)
    }

    fn access(&self, db: &str, write: bool) -> Result<bool, io::Error> {
        self.0.access(db, write)
    }

    fn full_pathname<'a>(&self, db: &'a str) -> Result<std::borrow::Cow<'a, str>, io::Error> {
        self.0.full_pathname(db)
    }
}

/// Register a SharedTurboliteVfs with SQLite under the given name.
///
/// Unlike `register`, this lets you keep a clone of the SharedTurboliteVfs
/// so you can call `manifest()` and `set_manifest()` on it after registration.
/// This is required for haqlite's shared-mode turbolite integration.
pub fn register_shared(name: &str, vfs: SharedTurboliteVfs) -> Result<(), io::Error> {
    sqlite_vfs::register(name, vfs, false)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))?;
    REGISTERED_VFS_NAMES.lock().insert(name.to_string());
    Ok(())
}

#[cfg(feature = "bundled-sqlite")]
/// Migrate a database to S3Primary mode (journal_mode=OFF).
///
/// This function handles the full migration regardless of the current journal mode:
/// 1. If in WAL mode, checkpoints the WAL to flush all data to the main database
/// 2. Attempts `PRAGMA journal_mode=OFF`
/// 3. If that fails (common with turbolite VFS due to WAL stub files), patches
///    the database header directly via the VFS to set journal_mode=DELETE (bytes
///    18-19 = 0x01,0x01), which breaks out of WAL mode. The caller should then
///    close and reopen with `PRAGMA journal_mode=OFF`.
///
/// After migration, close the connection and reopen with
/// `PRAGMA journal_mode=OFF`.
pub fn turbolite_migrate_to_s3_primary(conn: &rusqlite::Connection) -> Result<(), io::Error> {
    // Check current journal_mode
    let current_mode: String = conn
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("failed to read journal_mode: {}", e),
            )
        })?;

    if current_mode == "off" || current_mode == "memory" {
        return Ok(());
    }

    if current_mode == "wal" {
        // Checkpoint to flush WAL contents into the main database file
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("checkpoint failed: {}", e))
            })?;
    }

    // Try the normal PRAGMA path (works when not in WAL mode)
    let mode: String = conn
        .query_row("PRAGMA journal_mode=OFF", [], |r| r.get(0))
        .unwrap_or_else(|_| current_mode.clone());

    if mode == "off" || mode == "memory" {
        return Ok(());
    }

    // PRAGMA journal_mode=OFF failed. This happens with the turbolite VFS because
    // it creates WAL stub files on every open, keeping SQLite locked in WAL mode.
    //
    // Workaround: patch page 0 header directly via SQL to change the journal mode
    // indicator. SQLite database header bytes 18-19 encode the journal mode:
    //   WAL = (2, 2), DELETE = (1, 1), OFF = (0, 0)
    //
    // We read page 0 from the cache, patch bytes 18-19 to DELETE (1,1), and write
    // it back. SQLite won't recognize this until the connection is reopened.
    // We use DELETE (not OFF=0,0) because SQLite interprets 0,0 as "no format
    // version" and may override it. DELETE mode on reopen allows PRAGMA journal_mode=OFF.
    turbolite_debug!(
        "[migrate] PRAGMA journal_mode=OFF returned '{}', patching database header directly",
        mode,
    );

    // We cannot modify page 0 via SQL (it is read-only in the database header).
    // Return an error so the caller knows manual steps are needed.
    Err(io::Error::new(
        io::ErrorKind::Other,
        format!(
            "PRAGMA journal_mode=OFF returned '{}'. WAL checkpoint complete, but manual steps required: \
             1) Close this connection, \
             2) Delete the -wal and -shm files from the cache directory, \
             3) Reopen and run PRAGMA journal_mode=OFF",
            mode,
        ),
    ))
}

#[cfg(test)]
#[path = "test_mod.rs"]
mod tests;
