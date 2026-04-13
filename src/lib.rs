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
        eprintln!("[LOCK DEBUG] Lock tracing enabled");
    }
}

// ── FileWalIndex (SHM state for WAL-index) ──────────────────────────

/// WAL-index state backed by a memory-mapped -shm file on disk.
/// Provides region-based shared memory and byte-range locking for SQLite WAL mode.
///
/// With sqlite-plugin, shm_map returns a direct mmap pointer to SQLite.
/// No copy layer, no push/pull. Memory barriers are just atomic fences.
pub struct FileWalIndex {
    pub(crate) conn_id: u64,
    /// Path to the -shm file
    pub(crate) path: PathBuf,
    /// Memory-mapped regions (region_id -> mmap pointer).
    /// Each region is 32KB. mmap is MAP_SHARED so changes from other processes
    /// are visible immediately (pointer dereference, no syscall).
    pub(crate) mmap_regions: HashMap<u32, *mut u8>,
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

pub(crate) const WAL_REGION_SIZE: usize = 32768;

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

    /// Ensure region is mmap'd. Returns a direct pointer to the region's memory.
    /// sqlite-plugin returns this pointer to SQLite via shm_map (no copy).
    pub(crate) fn ensure_mmap_region(&mut self, region: u32) -> io::Result<*mut u8> {
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

    /// Handle SHM lock operations using sqlite-plugin's ShmLockMode.
    /// Two-phase locking: in-process coordination + fcntl file locks.
    pub(crate) fn shm_lock(
        &mut self,
        offset: u32,
        count: u32,
        mode: sqlite_plugin::flags::ShmLockMode,
    ) -> Result<(), io::Error> {
        use sqlite_plugin::flags::ShmLockMode;

        let conn_id = self.conn_id;

        match mode {
            ShmLockMode::UnlockShared | ShmLockMode::UnlockExclusive => {
                for slot in offset..offset + count {
                    let byte = WAL_LOCK_OFFSET + slot as u64;
                    unlock_inprocess(&self.path, byte as usize, 1, conn_id);
                    self.active_locks.remove(&(slot as u8));
                }
                Ok(())
            }
            ShmLockMode::LockShared | ShmLockMode::LockExclusive => {
                let exclusive = matches!(mode, ShmLockMode::LockExclusive);
                let lock_type = if exclusive {
                    file_guard::Lock::Exclusive
                } else {
                    file_guard::Lock::Shared
                };
                let lock_file = self.ensure_lock_file()?;

                // Phase 1: check all in-process locks first
                for slot in offset..offset + count {
                    let byte = WAL_LOCK_OFFSET + slot as u64;
                    if !try_lock_inprocess(&self.path, byte as usize, 1, exclusive, conn_id) {
                        // Roll back any in-process locks we just acquired
                        for prev in offset..slot {
                            let prev_byte = WAL_LOCK_OFFSET + prev as u64;
                            unlock_inprocess(&self.path, prev_byte as usize, 1, conn_id);
                        }
                        if DEBUG_LOCKS.load(Ordering::Relaxed) {
                            eprintln!(
                                "[LOCK DEBUG] {:?} WAL_INDEX {} slot {} {:?} => BUSY (in-process)",
                                std::thread::current().id(),
                                self.path.display(),
                                slot,
                                mode
                            );
                        }
                        return Err(io::Error::new(io::ErrorKind::WouldBlock, "shm lock busy (in-process)"));
                    }
                }

                // Phase 2: acquire file locks
                let mut new_guards: Vec<(u8, Box<dyn std::any::Any + Send + Sync>)> = Vec::new();

                for slot in offset..offset + count {
                    let byte = WAL_LOCK_OFFSET + slot as u64;
                    let old_guard = self.active_locks.remove(&(slot as u8));

                    match file_guard::try_lock(
                        std::sync::Arc::clone(&lock_file),
                        lock_type,
                        byte as usize,
                        1,
                    ) {
                        Ok(guard) => {
                            new_guards.push((slot as u8, Box::new(guard)));
                        }
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            if let Some(guard) = old_guard {
                                self.active_locks.insert(slot as u8, guard);
                            }
                            // Roll back all in-process locks
                            for s in offset..offset + count {
                                let o = WAL_LOCK_OFFSET + s as u64;
                                unlock_inprocess(&self.path, o as usize, 1, conn_id);
                            }
                            return Err(e);
                        }
                        Err(e) => {
                            if let Some(guard) = old_guard {
                                self.active_locks.insert(slot as u8, guard);
                            }
                            for s in offset..offset + count {
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

                if DEBUG_LOCKS.load(Ordering::Relaxed) {
                    eprintln!(
                        "[LOCK DEBUG] {:?} WAL_INDEX {} shm_lock offset={} count={} {:?} => OK",
                        std::thread::current().id(),
                        self.path.display(),
                        offset,
                        count,
                        mode
                    );
                }
                Ok(())
            }
        }
    }

    /// Unmap all regions and release all in-process locks.
    pub(crate) fn cleanup(&mut self) {
        for (_, ptr) in self.mmap_regions.drain() {
            unsafe {
                libc::munmap(ptr as *mut libc::c_void, WAL_REGION_SIZE);
            }
        }
        unlock_all_inprocess(&self.path, self.conn_id);
        self.active_locks.clear();
    }

    /// Unmap SHM regions and optionally delete the -shm file.
    /// When delete=false, only release locks but keep mmap regions alive
    /// (other connections may still be using their pointers).
    /// When delete=true, full cleanup + file removal.
    pub(crate) fn unmap(&mut self, delete: bool) -> io::Result<()> {
        if delete {
            self.cleanup();
            if self.path.exists() {
                std::fs::remove_file(&self.path)?;
            }
        } else {
            // Release in-process locks but keep mmap regions alive.
            // SQLite may still reference the pointers between shm_unmap and close.
            unlock_all_inprocess(&self.path, self.conn_id);
            self.active_locks.clear();
        }
        Ok(())
    }
}

impl Drop for FileWalIndex {
    fn drop(&mut self) {
        self.cleanup();
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
        let path = test_path("clear_caches");
        let conn = next_conn();
        assert!(try_lock_inprocess(&path, 0, 1, true, conn));
        clear_all_caches();
        let conn_b = next_conn();
        assert!(try_lock_inprocess(&path, 0, 1, true, conn_b));
        unlock_inprocess(&path, 0, 1, conn_b);
    }

    // ── CONN_ID_COUNTER tests ──────────────────────────────────────────

    #[test]
    fn test_conn_id_counter_increments() {
        let id1 = CONN_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        let id2 = CONN_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        assert!(id2 > id1);
    }

    // ── FileWalIndex mmap tests ────────────────────────────────────────

    #[test]
    fn test_wal_index_region_returns_valid_pointer() {
        let dir = tempfile::tempdir().expect("tempdir");
        let shm_path = dir.path().join("test.db-shm");
        let mut idx = FileWalIndex::new(shm_path.clone());

        let ptr = idx.ensure_mmap_region(0).expect("map region 0");
        assert!(!ptr.is_null());

        // File should exist and be at least 32KB
        let meta = std::fs::metadata(&shm_path).expect("shm file metadata");
        assert!(meta.len() >= 32768);
    }

    #[test]
    fn test_wal_index_mmap_shared_visibility() {
        // Two FileWalIndex instances on the same file see each other's writes
        let dir = tempfile::tempdir().expect("tempdir");
        let shm_path = dir.path().join("test.db-shm");

        let mut idx_a = FileWalIndex::new(shm_path.clone());
        let mut idx_b = FileWalIndex::new(shm_path);

        let ptr_a = idx_a.ensure_mmap_region(0).expect("map a");
        let ptr_b = idx_b.ensure_mmap_region(0).expect("map b");

        // Write via A, read via B
        unsafe { *ptr_a = 0x42; }
        let val = unsafe { *ptr_b };
        assert_eq!(val, 0x42, "MAP_SHARED should make writes visible across mappings");
    }

    #[test]
    fn test_wal_index_multiple_regions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let shm_path = dir.path().join("test.db-shm");
        let mut idx = FileWalIndex::new(shm_path);

        let p0 = idx.ensure_mmap_region(0).expect("region 0");
        let p1 = idx.ensure_mmap_region(1).expect("region 1");
        let p2 = idx.ensure_mmap_region(2).expect("region 2");

        // All should be valid, non-null, distinct pointers
        assert!(!p0.is_null());
        assert!(!p1.is_null());
        assert!(!p2.is_null());
        assert_ne!(p0, p1);
        assert_ne!(p1, p2);
    }

    // ── SHM lock tests (using sqlite-plugin ShmLockMode) ──────────────

    use sqlite_plugin::flags::ShmLockMode;

    #[test]
    fn test_shm_lock_two_shared_same_slot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let shm_path = dir.path().join("test.db-shm");

        let mut idx_a = FileWalIndex::new(shm_path.clone());
        let mut idx_b = FileWalIndex::new(shm_path);

        let _ = idx_a.ensure_mmap_region(0).expect("map a");
        let _ = idx_b.ensure_mmap_region(0).expect("map b");

        idx_a.shm_lock(0, 1, ShmLockMode::LockShared).expect("lock a shared");
        idx_b.shm_lock(0, 1, ShmLockMode::LockShared).expect("lock b shared");

        idx_a.shm_lock(0, 1, ShmLockMode::UnlockShared).expect("unlock a");
        idx_b.shm_lock(0, 1, ShmLockMode::UnlockShared).expect("unlock b");
    }

    #[test]
    fn test_shm_lock_exclusive_blocks_shared() {
        let dir = tempfile::tempdir().expect("tempdir");
        let shm_path = dir.path().join("test.db-shm");

        let mut idx_a = FileWalIndex::new(shm_path.clone());
        let mut idx_b = FileWalIndex::new(shm_path);

        let _ = idx_a.ensure_mmap_region(0).expect("map a");
        let _ = idx_b.ensure_mmap_region(0).expect("map b");

        idx_a.shm_lock(0, 1, ShmLockMode::LockExclusive).expect("lock a excl");
        let result = idx_b.shm_lock(0, 1, ShmLockMode::LockShared);
        assert!(result.is_err(), "shared lock should fail when another holds exclusive");

        idx_a.shm_lock(0, 1, ShmLockMode::UnlockExclusive).expect("unlock a");
    }

    #[test]
    fn test_shm_lock_exclusive_blocks_exclusive() {
        let dir = tempfile::tempdir().expect("tempdir");
        let shm_path = dir.path().join("test.db-shm");

        let mut idx_a = FileWalIndex::new(shm_path.clone());
        let mut idx_b = FileWalIndex::new(shm_path);

        let _ = idx_a.ensure_mmap_region(0).expect("map a");
        let _ = idx_b.ensure_mmap_region(0).expect("map b");

        idx_a.shm_lock(0, 1, ShmLockMode::LockExclusive).expect("lock a excl");
        let result = idx_b.shm_lock(0, 1, ShmLockMode::LockExclusive);
        assert!(result.is_err(), "exclusive lock should fail when another holds exclusive");

        idx_a.shm_lock(0, 1, ShmLockMode::UnlockExclusive).expect("unlock a");
    }

    #[test]
    fn test_shm_lock_unlock_then_acquire() {
        let dir = tempfile::tempdir().expect("tempdir");
        let shm_path = dir.path().join("test.db-shm");

        let mut idx_a = FileWalIndex::new(shm_path.clone());
        let mut idx_b = FileWalIndex::new(shm_path);

        let _ = idx_a.ensure_mmap_region(0).expect("map a");
        let _ = idx_b.ensure_mmap_region(0).expect("map b");

        idx_a.shm_lock(0, 1, ShmLockMode::LockExclusive).expect("lock a excl");
        assert!(idx_b.shm_lock(0, 1, ShmLockMode::LockExclusive).is_err());

        idx_a.shm_lock(0, 1, ShmLockMode::UnlockExclusive).expect("unlock a");
        idx_b.shm_lock(0, 1, ShmLockMode::LockExclusive).expect("lock b excl after unlock");

        idx_b.shm_lock(0, 1, ShmLockMode::UnlockExclusive).expect("unlock b");
    }

    #[test]
    fn test_shm_unmap_removes_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let shm_path = dir.path().join("test.db-shm");

        let mut idx = FileWalIndex::new(shm_path.clone());
        let _ = idx.ensure_mmap_region(0).expect("map");
        assert!(shm_path.exists(), "shm file should exist after map");

        idx.unmap(true).expect("unmap with delete");
        assert!(!shm_path.exists(), "shm file should be removed after unmap(true)");
    }

    #[test]
    fn test_shm_unmap_preserves_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let shm_path = dir.path().join("test.db-shm");

        let mut idx = FileWalIndex::new(shm_path.clone());
        let _ = idx.ensure_mmap_region(0).expect("map");
        assert!(shm_path.exists());

        idx.unmap(false).expect("unmap without delete");
        assert!(shm_path.exists(), "shm file should be preserved after unmap(false)");
    }
}
