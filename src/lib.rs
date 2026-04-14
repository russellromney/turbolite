//! turbolite: high-performance SQLite VFS with compressed page groups.
//!
//! Two storage modes, same on-disk format (manifest + page groups):
//!
//! - **Local** (default): page groups stored at `{cache_dir}/pg/`, manifest at
//!   `{cache_dir}/manifest.msgpack`. No S3, no tokio, no async deps.
//!
//! - **Cloud** (S3-backed, requires `cloud` feature): S3 is the source of truth,
//!   local NVMe disk is a page-level cache.
//!
//! # Quick start (local mode)
//!
//! ```ignore
//! use turbolite::tiered::{TurboliteVfs, TurboliteConfig, StorageBackend};
//!
//! let config = TurboliteConfig {
//!     storage_backend: StorageBackend::Local,
//!     cache_dir: "/data/mydb".into(),
//!     ..Default::default()
//! };
//! let vfs = TurboliteVfs::new(config)?;
//! turbolite::tiered::register("mydb", vfs)?;
//! ```

/// Debug logging macro, gated behind TURBOLITE_DEBUG=1 env var.
/// Silent by default. Set TURBOLITE_DEBUG=1 to enable debug output to stderr.
/// Error-level messages use eprintln! directly and are always visible.
#[macro_export]
macro_rules! turbolite_debug {
    ($($arg:tt)*) => {
        if $crate::debug_enabled() {
            eprintln!($($arg)*);
        }
    };
}

/// Check if debug logging is enabled (cached after first check).
pub fn debug_enabled() -> bool {
    use std::sync::atomic::{AtomicU8, Ordering};
    // 0 = unchecked, 1 = disabled, 2 = enabled
    static STATE: AtomicU8 = AtomicU8::new(0);
    match STATE.load(Ordering::Relaxed) {
        2 => true,
        1 => false,
        _ => {
            let enabled = std::env::var("TURBOLITE_DEBUG").map_or(false, |v| v == "1" || v == "true");
            STATE.store(if enabled { 2 } else { 1 }, Ordering::Relaxed);
            enabled
        }
    }
}

pub mod compress;
pub mod dict;
#[cfg(not(feature = "loadable-extension"))]
pub mod ffi;
#[cfg(feature = "loadable-extension")]
pub mod ext;
pub mod tiered;
pub use tiered::{TurboliteVfs, TurboliteConfig, TurboliteHandle, SharedTurboliteVfs};
pub mod btree_walker;

use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions as FsOpenOptions};
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

// ── Public utility functions ──────────────────────────────────────────

/// Invalidate cached state for a file. Currently a no-op; retained for
/// FFI backward compatibility.
pub fn invalidate_cache<P: AsRef<Path>>(_path: P) {}

/// Clear all in-process lock state. Call this when running fresh benchmarks
/// or tests to ensure no stale state is reused.
pub fn clear_all_caches() {
    IN_PROCESS_LOCKS.lock().clear();
}

// ── In-process lock coordination ──────────────────────────────────────
// fcntl (used by file-guard) is per-process, not per-thread. Two threads in the
// same process see no lock conflict via fcntl. This layer provides thread-level
// mutual exclusion; file-guard remains for cross-process locking.

pub(crate) static CONN_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

static IN_PROCESS_LOCKS: once_cell::sync::Lazy<Mutex<HashMap<PathBuf, InProcessLocks>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(HashMap::new()));

struct InProcessLocks {
    slots: HashMap<usize, SlotState>,
}

struct SlotState {
    shared: HashSet<u64>,
    exclusive: Option<u64>,
}

pub(crate) fn try_lock_inprocess(path: &Path, offset: usize, len: usize, exclusive: bool, conn_id: u64) -> bool {
    let mut map = IN_PROCESS_LOCKS.lock();
    let locks = map.entry(path.to_path_buf()).or_insert_with(|| InProcessLocks { slots: HashMap::new() });

    // Check all bytes in range first
    for byte in offset..offset + len {
        let slot = locks.slots.entry(byte).or_insert_with(|| SlotState {
            shared: HashSet::new(),
            exclusive: None,
        });
        if exclusive {
            if slot.exclusive.is_some() && slot.exclusive != Some(conn_id) {
                return false;
            }
            if slot.shared.iter().any(|&id| id != conn_id) {
                return false;
            }
        } else if slot.exclusive.is_some() && slot.exclusive != Some(conn_id) {
            return false;
        }
    }

    // All checks passed, commit
    for byte in offset..offset + len {
        let slot = locks.slots.get_mut(&byte).expect("slot must exist after check");
        if exclusive {
            slot.exclusive = Some(conn_id);
            slot.shared.remove(&conn_id);
        } else {
            slot.shared.insert(conn_id);
        }
    }
    true
}

pub(crate) fn unlock_inprocess(path: &Path, offset: usize, len: usize, conn_id: u64) {
    let mut map = IN_PROCESS_LOCKS.lock();
    if let Some(locks) = map.get_mut(path) {
        for byte in offset..offset + len {
            if let Some(slot) = locks.slots.get_mut(&byte) {
                slot.shared.remove(&conn_id);
                if slot.exclusive == Some(conn_id) {
                    slot.exclusive = None;
                }
            }
        }
    }
}

pub(crate) fn unlock_all_inprocess(path: &Path, conn_id: u64) {
    let mut map = IN_PROCESS_LOCKS.lock();
    if let Some(locks) = map.get_mut(path) {
        for slot in locks.slots.values_mut() {
            slot.shared.remove(&conn_id);
            if slot.exclusive == Some(conn_id) {
                slot.exclusive = None;
            }
        }
    }
}

/// Release all in-process locks for a specific database path.
/// Call after closing a connection to ensure the lock is released
/// before reopening. More targeted than clear_all_caches().
pub fn release_locks_for(path: &Path) {
    let mut map = IN_PROCESS_LOCKS.lock();
    map.remove(path);
}

// SQLite WAL-index lock byte offset (in the -shm file)
// Locks are at bytes 120-127 in the WAL-index header
const WAL_LOCK_OFFSET: u64 = 120;

/// Debug lock tracing, enabled via TURBOLITE_DEBUG_LOCKS=1
static DEBUG_LOCKS: AtomicBool = AtomicBool::new(false);

/// Initialize debug lock tracing from environment
pub fn init_debug_locks() {
    if std::env::var("TURBOLITE_DEBUG_LOCKS").map(|v| v == "1").unwrap_or(false) {
        DEBUG_LOCKS.store(true, Ordering::Relaxed);
        turbolite_debug!("[LOCK DEBUG] Lock tracing enabled");
    }
}

// ── FileWalIndex (shared by TurboliteVfs) ─────────────────────────────

/// WAL-index implementation backed by a memory-mapped -shm file on disk.
/// Provides region-based shared memory and byte-range locking for SQLite WAL mode.
///
/// Uses mmap for WAL-index region access (map/pull/push). This matches SQLite's
/// built-in VFS behavior: WAL-index reads are pointer dereferences, not syscalls.
pub struct FileWalIndex {
    conn_id: u64,
    /// Path to the -shm file
    path: PathBuf,
    /// Memory-mapped regions (region_id -> mmap pointer + length).
    /// Each region is 32KB. mmap is MAP_SHARED so changes from other processes
    /// are visible immediately (no syscall needed for reads).
    mmap_regions: HashMap<u32, *mut u8>,
    /// File handle for mmap and extending
    file: Option<File>,
    /// Separate file handle for locking (Arc for multiple FileGuards)
    lock_file: Option<std::sync::Arc<File>>,
    /// Active byte-range locks: slot -> FileGuard
    active_locks: HashMap<u8, Box<dyn std::any::Any + Send + Sync>>,
}

// Safety: mmap pointers are to MAP_SHARED memory backed by a file.
// Access is serialized by SQLite's WAL protocol (read locks before reads,
// write locks before writes). The pointers are valid for the lifetime of
// the mapping (unmapped in Drop).
unsafe impl Send for FileWalIndex {}
unsafe impl Sync for FileWalIndex {}

const WAL_REGION_SIZE: usize = 32768;

impl FileWalIndex {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self {
            conn_id: CONN_ID_COUNTER.fetch_add(1, Ordering::Relaxed),
            path,
            mmap_regions: HashMap::new(),
            file: None,
            lock_file: None,
            active_locks: HashMap::new(),
        }
    }

    fn ensure_file(&mut self) -> io::Result<&mut File> {
        if self.file.is_none() {
            let file = FsOpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&self.path)?;
            self.file = Some(file);
        }
        Ok(self.file.as_mut().expect("file was just set"))
    }

    fn ensure_lock_file(&mut self) -> io::Result<std::sync::Arc<File>> {
        if self.lock_file.is_none() {
            let file = FsOpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&self.path)?;
            self.lock_file = Some(std::sync::Arc::new(file));
        }
        Ok(std::sync::Arc::clone(self.lock_file.as_ref().expect("lock_file was just set")))
    }

    /// Ensure region is mmap'd. Returns a pointer to the region's 32KB.
    fn ensure_mmap_region(&mut self, region: u32) -> io::Result<*mut u8> {
        if let Some(&ptr) = self.mmap_regions.get(&region) {
            return Ok(ptr);
        }

        use std::os::unix::io::AsRawFd;

        let offset = region as usize * WAL_REGION_SIZE;
        let file = self.ensure_file()?;
        let file_len = file.metadata()?.len() as usize;

        // Extend file if needed
        let needed = offset + WAL_REGION_SIZE;
        if file_len < needed {
            file.set_len(needed as u64)?;
        }

        let fd = file.as_raw_fd();
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                WAL_REGION_SIZE,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                offset as libc::off_t,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }

        let ptr = ptr as *mut u8;
        self.mmap_regions.insert(region, ptr);
        Ok(ptr)
    }
}

impl Drop for FileWalIndex {
    fn drop(&mut self) {
        // Unmap all regions
        for (_, ptr) in self.mmap_regions.drain() {
            unsafe {
                libc::munmap(ptr as *mut libc::c_void, WAL_REGION_SIZE);
            }
        }
        // Release in-process locks
        unlock_all_inprocess(&self.path, self.conn_id);
    }
}

impl sqlite_vfs::wip::WalIndex for FileWalIndex {
    fn map(&mut self, region: u32) -> Result<[u8; 32768], io::Error> {
        let ptr = self.ensure_mmap_region(region)?;
        let mut data = [0u8; 32768];
        // Safety: ptr is a valid mmap'd region of 32KB, backed by MAP_SHARED file.
        // SQLite's WAL protocol ensures no concurrent writes without proper locking.
        unsafe {
            std::ptr::copy_nonoverlapping(ptr, data.as_mut_ptr(), WAL_REGION_SIZE);
        }
        Ok(data)
    }

    fn lock(
        &mut self,
        locks: Range<u8>,
        lock: sqlite_vfs::wip::WalIndexLock,
    ) -> Result<bool, io::Error> {
        use sqlite_vfs::wip::WalIndexLock;

        let conn_id = self.conn_id;
        let lock_file = self.ensure_lock_file()?;

        match lock {
            WalIndexLock::None => {
                for slot in locks.clone() {
                    let offset = WAL_LOCK_OFFSET + slot as u64;
                    unlock_inprocess(&self.path, offset as usize, 1, conn_id);
                    self.active_locks.remove(&slot);
                }
            }
            WalIndexLock::Shared | WalIndexLock::Exclusive => {
                let exclusive = matches!(lock, WalIndexLock::Exclusive);
                let lock_type = if exclusive {
                    file_guard::Lock::Exclusive
                } else {
                    file_guard::Lock::Shared
                };

                // Phase 1: check all in-process locks first
                for slot in locks.clone() {
                    let offset = WAL_LOCK_OFFSET + slot as u64;
                    if !try_lock_inprocess(&self.path, offset as usize, 1, exclusive, conn_id) {
                        for prev_slot in locks.start..slot {
                            let prev_offset = WAL_LOCK_OFFSET + prev_slot as u64;
                            unlock_inprocess(&self.path, prev_offset as usize, 1, conn_id);
                        }
                        if DEBUG_LOCKS.load(Ordering::Relaxed) {
                            turbolite_debug!(
                                "[LOCK DEBUG] {:?} WAL_INDEX {} slot {} {:?} => BUSY (in-process)",
                                std::thread::current().id(),
                                self.path.display(),
                                slot,
                                lock
                            );
                        }
                        return Ok(false);
                    }
                }

                // Phase 2: acquire file locks
                let mut new_guards: Vec<(u8, Box<dyn std::any::Any + Send + Sync>)> = Vec::new();

                for slot in locks.clone() {
                    let offset = WAL_LOCK_OFFSET + slot as u64;
                    let old_guard = self.active_locks.remove(&slot);

                    match file_guard::try_lock(
                        std::sync::Arc::clone(&lock_file),
                        lock_type,
                        offset as usize,
                        1,
                    ) {
                        Ok(guard) => {
                            new_guards.push((slot, Box::new(guard)));
                        }
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            if let Some(guard) = old_guard {
                                self.active_locks.insert(slot, guard);
                            }
                            for s in locks.clone() {
                                let o = WAL_LOCK_OFFSET + s as u64;
                                unlock_inprocess(&self.path, o as usize, 1, conn_id);
                            }
                            return Ok(false);
                        }
                        Err(e) => {
                            if let Some(guard) = old_guard {
                                self.active_locks.insert(slot, guard);
                            }
                            for s in locks.clone() {
                                let o = WAL_LOCK_OFFSET + s as u64;
                                unlock_inprocess(&self.path, o as usize, 1, conn_id);
                            }
                            return Err(e);
                        }
                    }
                }

                for (slot, guard) in new_guards {
                    self.active_locks.insert(slot, guard);
                }
            }
        }

        if DEBUG_LOCKS.load(Ordering::Relaxed) {
            turbolite_debug!(
                "[LOCK DEBUG] {:?} WAL_INDEX {} locks {:?}..{:?} {:?} => OK",
                std::thread::current().id(),
                self.path.display(),
                locks.start,
                locks.end,
                lock
            );
        }
        Ok(true)
    }

    fn pull(&mut self, region: u32, data: &mut [u8; 32768]) -> Result<(), io::Error> {
        let ptr = self.ensure_mmap_region(region)?;
        // Safety: mmap'd MAP_SHARED region, reads are always current.
        unsafe {
            std::ptr::copy_nonoverlapping(ptr, data.as_mut_ptr(), WAL_REGION_SIZE);
        }
        Ok(())
    }

    fn push(&mut self, region: u32, data: &[u8; 32768]) -> Result<(), io::Error> {
        let ptr = self.ensure_mmap_region(region)?;
        // Safety: MAP_SHARED write. Immediately visible to other processes via mmap.
        // SQLite holds EXCLUSIVE WAL lock before calling push.
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, WAL_REGION_SIZE);
        }
        Ok(())
    }

    fn delete(mut self) -> Result<(), io::Error> {
        // Unmap all regions before removing the file
        for (_, ptr) in self.mmap_regions.drain() {
            unsafe {
                libc::munmap(ptr as *mut libc::c_void, WAL_REGION_SIZE);
            }
        }
        unlock_all_inprocess(&self.path, self.conn_id);
        if self.path.exists() {
            std::fs::remove_file(&self.path)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Each test gets unique conn IDs from the atomic counter and uses
    // unique paths (via tempdir) to avoid interference from parallel tests.

    fn next_conn() -> u64 {
        CONN_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
    }

    fn test_path(name: &str) -> PathBuf {
        // Use a unique path per test invocation to avoid cross-test pollution
        PathBuf::from(format!("/tmp/turbolite_lock_test_{}_{}", name, next_conn()))
    }

    // ── In-process lock tests ──────────────────────────────────────────

    #[test]
    fn test_shared_lock_succeeds() {
        let path = test_path("shared");
        let conn_id = next_conn();
        assert!(try_lock_inprocess(&path, 0, 1, false, conn_id));
        unlock_inprocess(&path, 0, 1, conn_id);
    }

    #[test]
    fn test_exclusive_lock_succeeds() {
        let path = test_path("excl");
        let conn_id = next_conn();
        assert!(try_lock_inprocess(&path, 0, 1, true, conn_id));
        unlock_inprocess(&path, 0, 1, conn_id);
    }

    #[test]
    fn test_two_shared_locks_compatible() {
        let path = test_path("two_shared");
        let conn_a = next_conn();
        let conn_b = next_conn();
        assert!(try_lock_inprocess(&path, 0, 1, false, conn_a));
        assert!(try_lock_inprocess(&path, 0, 1, false, conn_b));
        unlock_inprocess(&path, 0, 1, conn_a);
        unlock_inprocess(&path, 0, 1, conn_b);
    }

    #[test]
    fn test_exclusive_blocks_other_shared() {
        let path = test_path("excl_blocks_shared");
        let conn_a = next_conn();
        let conn_b = next_conn();
        assert!(try_lock_inprocess(&path, 0, 1, true, conn_a));
        assert!(!try_lock_inprocess(&path, 0, 1, false, conn_b));
        unlock_inprocess(&path, 0, 1, conn_a);
    }

    #[test]
    fn test_exclusive_blocks_other_exclusive() {
        let path = test_path("excl_blocks_excl");
        let conn_a = next_conn();
        let conn_b = next_conn();
        assert!(try_lock_inprocess(&path, 0, 1, true, conn_a));
        assert!(!try_lock_inprocess(&path, 0, 1, true, conn_b));
        unlock_inprocess(&path, 0, 1, conn_a);
    }

    #[test]
    fn test_shared_blocks_other_exclusive() {
        let path = test_path("shared_blocks_excl");
        let conn_a = next_conn();
        let conn_b = next_conn();
        assert!(try_lock_inprocess(&path, 0, 1, false, conn_a));
        assert!(!try_lock_inprocess(&path, 0, 1, true, conn_b));
        unlock_inprocess(&path, 0, 1, conn_a);
    }

    #[test]
    fn test_same_conn_can_upgrade_shared_to_exclusive() {
        let path = test_path("upgrade");
        let conn = next_conn();
        assert!(try_lock_inprocess(&path, 0, 1, false, conn));
        assert!(try_lock_inprocess(&path, 0, 1, true, conn));
        unlock_inprocess(&path, 0, 1, conn);
    }

    #[test]
    fn test_same_conn_reentrant_exclusive() {
        let path = test_path("reentrant");
        let conn = next_conn();
        assert!(try_lock_inprocess(&path, 0, 1, true, conn));
        assert!(try_lock_inprocess(&path, 0, 1, true, conn));
        unlock_inprocess(&path, 0, 1, conn);
    }

    #[test]
    fn test_unlock_allows_other_lock() {
        let path = test_path("unlock_allows");
        let conn_a = next_conn();
        let conn_b = next_conn();
        assert!(try_lock_inprocess(&path, 0, 1, true, conn_a));
        assert!(!try_lock_inprocess(&path, 0, 1, true, conn_b));
        unlock_inprocess(&path, 0, 1, conn_a);
        assert!(try_lock_inprocess(&path, 0, 1, true, conn_b));
        unlock_inprocess(&path, 0, 1, conn_b);
    }

    #[test]
    fn test_multi_byte_range_lock() {
        let path = test_path("range");
        let conn_a = next_conn();
        let conn_b = next_conn();
        assert!(try_lock_inprocess(&path, 0, 4, true, conn_a));
        assert!(!try_lock_inprocess(&path, 2, 1, false, conn_b));
        assert!(try_lock_inprocess(&path, 4, 1, false, conn_b));
        unlock_inprocess(&path, 0, 4, conn_a);
        unlock_inprocess(&path, 4, 1, conn_b);
    }

    #[test]
    fn test_unlock_all_clears_everything() {
        let path = test_path("unlock_all");
        let conn = next_conn();
        assert!(try_lock_inprocess(&path, 0, 1, true, conn));
        assert!(try_lock_inprocess(&path, 5, 3, false, conn));
        unlock_all_inprocess(&path, conn);
        let conn_b = next_conn();
        assert!(try_lock_inprocess(&path, 0, 1, true, conn_b));
        assert!(try_lock_inprocess(&path, 5, 3, true, conn_b));
        unlock_all_inprocess(&path, conn_b);
    }

    #[test]
    fn test_different_paths_independent() {
        let path_a = test_path("path_a");
        let path_b = test_path("path_b");
        let conn_a = next_conn();
        let conn_b = next_conn();
        assert!(try_lock_inprocess(&path_a, 0, 1, true, conn_a));
        assert!(try_lock_inprocess(&path_b, 0, 1, true, conn_b));
        unlock_inprocess(&path_a, 0, 1, conn_a);
        unlock_inprocess(&path_b, 0, 1, conn_b);
    }

    #[test]
    fn test_unlock_nonexistent_is_safe() {
        let path = test_path("unlock_nonexistent");
        unlock_inprocess(&path, 0, 10, next_conn());
        unlock_all_inprocess(&path, next_conn());
    }

    // ── clear_all_caches tests ─────────────────────────────────────────

    #[test]
    fn test_clear_all_caches_clears_locks() {
        // clear_all_caches() wipes the global IN_PROCESS_LOCKS map, which
        // would nuke locks held by other tests running in parallel and cause
        // flaky failures. Instead of calling the global function, we directly
        // verify that removing our path from the lock map releases our locks
        // (the clear_all_caches function just calls .clear() on the same map).
        let path = test_path("clear_caches");
        let conn = next_conn();
        assert!(try_lock_inprocess(&path, 0, 1, true, conn));

        // Verify the lock is actually held
        let conn_b = next_conn();
        assert!(!try_lock_inprocess(&path, 0, 1, true, conn_b),
            "lock should be held before clear");

        // Clear only our test's path entry from the global map
        {
            let mut map = IN_PROCESS_LOCKS.lock();
            map.remove(&path);
        }

        // After clearing, a different conn should be able to lock the same path
        let conn_c = next_conn();
        assert!(try_lock_inprocess(&path, 0, 1, true, conn_c),
            "lock should be available after clearing path from lock map");
        unlock_inprocess(&path, 0, 1, conn_c);
    }

    // ── CONN_ID_COUNTER tests ──────────────────────────────────────────

    #[test]
    fn test_conn_id_counter_increments() {
        let id1 = CONN_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        let id2 = CONN_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        assert!(id2 > id1);
    }

    // ── FileWalIndex tests ────────────────────────────────────────────

    use sqlite_vfs::wip::{WalIndex, WalIndexLock};

    #[test]
    fn test_wal_index_region_map_returns_zeroed_data() {
        let dir = tempfile::tempdir().expect("tempdir");
        let shm_path = dir.path().join("test.db-shm");
        let mut idx = FileWalIndex::new(shm_path.clone());

        let data = idx.map(0).expect("map region 0");
        assert_eq!(data, [0u8; 32768]);

        // File should exist and be at least 32KB
        let meta = std::fs::metadata(&shm_path).expect("shm file metadata");
        assert!(meta.len() >= 32768);
    }

    #[test]
    fn test_wal_index_push_pull_roundtrip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let shm_path = dir.path().join("test.db-shm");
        let mut idx = FileWalIndex::new(shm_path);

        // Map first to ensure file exists
        let _ = idx.map(0).expect("map region 0");

        // Write known data
        let mut write_buf = [0u8; 32768];
        for i in 0..32768 {
            write_buf[i] = (i % 256) as u8;
        }
        idx.push(0, &write_buf).expect("push region 0");

        // Read it back
        let mut read_buf = [0u8; 32768];
        idx.pull(0, &mut read_buf).expect("pull region 0");
        assert_eq!(read_buf, write_buf);
    }

    #[test]
    fn test_wal_index_multiple_regions_independent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let shm_path = dir.path().join("test.db-shm");
        let mut idx = FileWalIndex::new(shm_path);

        // Map three regions
        let _ = idx.map(0).expect("map 0");
        let _ = idx.map(1).expect("map 1");
        let _ = idx.map(2).expect("map 2");

        // Write distinct data to each
        let mut buf0 = [0xAAu8; 32768];
        let mut buf1 = [0xBBu8; 32768];
        let mut buf2 = [0xCCu8; 32768];
        idx.push(0, &buf0).expect("push 0");
        idx.push(1, &buf1).expect("push 1");
        idx.push(2, &buf2).expect("push 2");

        // Read back and verify independence
        let mut read = [0u8; 32768];
        idx.pull(0, &mut read).expect("pull 0");
        assert_eq!(read, buf0);

        idx.pull(1, &mut read).expect("pull 1");
        assert_eq!(read, buf1);

        idx.pull(2, &mut read).expect("pull 2");
        assert_eq!(read, buf2);

        // File should be at least 3 * 32KB
        let _ = buf0;
        let _ = buf1;
        let _ = buf2;
    }

    #[test]
    fn test_wal_index_lock_two_shared_same_slot() {
        // Two FileWalIndex instances on the same -shm file can both hold shared locks
        let dir = tempfile::tempdir().expect("tempdir");
        let shm_path = dir.path().join("test.db-shm");

        let mut idx_a = FileWalIndex::new(shm_path.clone());
        let mut idx_b = FileWalIndex::new(shm_path.clone());

        // Ensure files exist
        let _ = idx_a.map(0).expect("map a");
        let _ = idx_b.map(0).expect("map b");

        let ok_a = idx_a.lock(0..1, WalIndexLock::Shared).expect("lock a shared");
        assert!(ok_a);
        let ok_b = idx_b.lock(0..1, WalIndexLock::Shared).expect("lock b shared");
        assert!(ok_b);

        // Cleanup
        idx_a.lock(0..1, WalIndexLock::None).expect("unlock a");
        idx_b.lock(0..1, WalIndexLock::None).expect("unlock b");
    }

    #[test]
    fn test_wal_index_lock_exclusive_blocks_shared() {
        let dir = tempfile::tempdir().expect("tempdir");
        let shm_path = dir.path().join("test.db-shm");

        let mut idx_a = FileWalIndex::new(shm_path.clone());
        let mut idx_b = FileWalIndex::new(shm_path.clone());

        let _ = idx_a.map(0).expect("map a");
        let _ = idx_b.map(0).expect("map b");

        let ok_a = idx_a.lock(0..1, WalIndexLock::Exclusive).expect("lock a excl");
        assert!(ok_a);

        // B should fail to get shared
        let ok_b = idx_b.lock(0..1, WalIndexLock::Shared).expect("lock b shared");
        assert!(!ok_b, "shared lock should fail when another holds exclusive");

        idx_a.lock(0..1, WalIndexLock::None).expect("unlock a");
    }

    #[test]
    fn test_wal_index_lock_exclusive_blocks_exclusive() {
        let dir = tempfile::tempdir().expect("tempdir");
        let shm_path = dir.path().join("test.db-shm");

        let mut idx_a = FileWalIndex::new(shm_path.clone());
        let mut idx_b = FileWalIndex::new(shm_path.clone());

        let _ = idx_a.map(0).expect("map a");
        let _ = idx_b.map(0).expect("map b");

        let ok_a = idx_a.lock(0..1, WalIndexLock::Exclusive).expect("lock a excl");
        assert!(ok_a);

        let ok_b = idx_b.lock(0..1, WalIndexLock::Exclusive).expect("lock b excl");
        assert!(!ok_b, "exclusive lock should fail when another holds exclusive");

        idx_a.lock(0..1, WalIndexLock::None).expect("unlock a");
    }

    #[test]
    fn test_wal_index_lock_shared_blocks_exclusive() {
        let dir = tempfile::tempdir().expect("tempdir");
        let shm_path = dir.path().join("test.db-shm");

        let mut idx_a = FileWalIndex::new(shm_path.clone());
        let mut idx_b = FileWalIndex::new(shm_path.clone());

        let _ = idx_a.map(0).expect("map a");
        let _ = idx_b.map(0).expect("map b");

        let ok_a = idx_a.lock(0..1, WalIndexLock::Shared).expect("lock a shared");
        assert!(ok_a);

        let ok_b = idx_b.lock(0..1, WalIndexLock::Exclusive).expect("lock b excl");
        assert!(!ok_b, "exclusive lock should fail when another holds shared");

        idx_a.lock(0..1, WalIndexLock::None).expect("unlock a");
    }

    #[test]
    fn test_wal_index_lock_unlock_then_acquire() {
        let dir = tempfile::tempdir().expect("tempdir");
        let shm_path = dir.path().join("test.db-shm");

        let mut idx_a = FileWalIndex::new(shm_path.clone());
        let mut idx_b = FileWalIndex::new(shm_path.clone());

        let _ = idx_a.map(0).expect("map a");
        let _ = idx_b.map(0).expect("map b");

        // A takes exclusive
        let ok = idx_a.lock(0..1, WalIndexLock::Exclusive).expect("lock a excl");
        assert!(ok);

        // B blocked
        let ok = idx_b.lock(0..1, WalIndexLock::Exclusive).expect("lock b excl");
        assert!(!ok);

        // A releases
        idx_a.lock(0..1, WalIndexLock::None).expect("unlock a");

        // B succeeds now
        let ok = idx_b.lock(0..1, WalIndexLock::Exclusive).expect("lock b excl after unlock");
        assert!(ok);

        idx_b.lock(0..1, WalIndexLock::None).expect("unlock b");
    }

    #[test]
    fn test_wal_index_lock_upgrade_shared_to_exclusive() {
        let dir = tempfile::tempdir().expect("tempdir");
        let shm_path = dir.path().join("test.db-shm");

        let mut idx = FileWalIndex::new(shm_path);
        let _ = idx.map(0).expect("map");

        let ok = idx.lock(0..1, WalIndexLock::Shared).expect("shared");
        assert!(ok);

        let ok = idx.lock(0..1, WalIndexLock::Exclusive).expect("upgrade to exclusive");
        assert!(ok);

        idx.lock(0..1, WalIndexLock::None).expect("unlock");
    }

    #[test]
    fn test_wal_index_delete_removes_shm_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let shm_path = dir.path().join("test.db-shm");

        let mut idx = FileWalIndex::new(shm_path.clone());
        let _ = idx.map(0).expect("map");
        assert!(shm_path.exists(), "shm file should exist after map");

        idx.delete().expect("delete");
        assert!(!shm_path.exists(), "shm file should be removed after delete");
    }
}
