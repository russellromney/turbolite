//! Lightweight benchmark handle for cache-level testing.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::*;

/// Lightweight handle for benchmarking -- shares the same S3 client, cache,
/// manifest, and dirty groups as the VFS.
/// Obtained via [`TieredVfs::bench_handle`] before registering the VFS.
pub struct TieredBenchHandle {
    pub(super) s3: Arc<S3Client>,
    pub(super) cache: Arc<DiskCache>,
    pub(super) prefetch_pool: Arc<PrefetchPool>,
    // Shared state for flush_to_s3()
    pub(super) shared_manifest: Arc<RwLock<Manifest>>,
    pub(super) shared_dirty_groups: Arc<Mutex<HashSet<u64>>>,
    pub(super) flush_lock: Arc<Mutex<()>>,
    pub(super) compression_level: i32,
    #[cfg(feature = "zstd")]
    pub(super) dictionary: Option<Vec<u8>>,
    pub(super) encryption_key: Option<[u8; 32]>,
    pub(super) gc_enabled: bool,
    pub(super) prediction: Option<prediction::SharedPrediction>,
    pub(super) access_history: Option<prediction::SharedAccessHistory>,
}

impl TieredBenchHandle {
    /// Evict data pages only -- interior B-tree pages and group 0 stay warm.
    /// Simulates production where structural pages are always hot.
    ///
    /// Safe to call with pending flush: groups awaiting S3 upload are protected
    /// (their pages remain in the disk cache bitmap).
    pub fn clear_cache_data_only(&self) {
        self.prefetch_pool.wait_idle();

        let pinned_pages = self.cache.interior_pages.lock().clone();
        let index_pages = self.cache.index_pages.lock().clone();
        let pending_groups = self.pending_group_pages();

        // Reset group states to None, except group 0 and pending groups stay Present
        let states = self.cache.group_states.lock();
        for (i, s) in states.iter().enumerate() {
            if i == 0 || pending_groups.contains_key(&(i as u64)) {
                s.store(GroupState::Present as u8, Ordering::Release);
            } else {
                s.store(GroupState::None as u8, Ordering::Release);
            }
        }
        drop(states);

        self.cache.group_access.lock().clear();

        // Clear bitmap except for interior pages, index pages, group 0, and pending groups
        {
            let gp = self.cache.group_pages.read();
            let mut bitmap = self.cache.bitmap.lock();
            bitmap.bits.fill(0);
            for &page in &pinned_pages {
                bitmap.mark_present(page);
            }
            for &page in &index_pages {
                bitmap.mark_present(page);
            }
            // Protect pending flush pages
            for pages in pending_groups.values() {
                for &p in pages {
                    bitmap.mark_present(p);
                }
            }
            if let Some(g0_pages) = gp.first() {
                for &p in g0_pages {
                    bitmap.mark_present(p);
                }
            } else {
                let ppg = self.cache.pages_per_group as u64;
                for p in 0..ppg {
                    bitmap.mark_present(p);
                }
            }
            let _ = bitmap.persist();
        }

        // Clear sub-chunk tracker: evict Data tier only, keep Pinned + Index
        {
            let mut tracker = self.cache.tracker.lock();
            tracker.clear_data_only();
        }
    }

    /// Evict index + data pages -- interior B-tree pages and group 0 stay warm.
    /// Simulates first query after connection open (interior loaded eagerly,
    /// index prefetch hasn't finished yet).
    ///
    /// Safe to call with pending flush: groups awaiting S3 upload are protected.
    pub fn clear_cache_interior_only(&self) {
        self.prefetch_pool.wait_idle();

        let pinned_pages = self.cache.interior_pages.lock().clone();
        let pending_groups = self.pending_group_pages();

        // Reset group states to None, except group 0 and pending groups stay Present
        let states = self.cache.group_states.lock();
        for (i, s) in states.iter().enumerate() {
            if i == 0 || pending_groups.contains_key(&(i as u64)) {
                s.store(GroupState::Present as u8, Ordering::Release);
            } else {
                s.store(GroupState::None as u8, Ordering::Release);
            }
        }
        drop(states);

        self.cache.group_access.lock().clear();
        self.cache.index_pages.lock().clear();

        // Clear bitmap except for interior pages, group 0, and pending groups
        {
            let gp = self.cache.group_pages.read();
            let mut bitmap = self.cache.bitmap.lock();
            bitmap.bits.fill(0);
            for &page in &pinned_pages {
                bitmap.mark_present(page);
            }
            // Protect pending flush pages
            for pages in pending_groups.values() {
                for &p in pages {
                    bitmap.mark_present(p);
                }
            }
            if let Some(g0_pages) = gp.first() {
                for &p in g0_pages {
                    bitmap.mark_present(p);
                }
            } else {
                let ppg = self.cache.pages_per_group as u64;
                for p in 0..ppg {
                    bitmap.mark_present(p);
                }
            }
            let _ = bitmap.persist();
        }

        // Clear sub-chunk tracker: evict Index + Data tiers, keep Pinned only
        {
            let mut tracker = self.cache.tracker.lock();
            tracker.clear_index_and_data();
        }
    }

    /// Evict everything -- interior pages, group 0, all data. Nothing cached.
    /// Next connection open must re-fetch interior chunks from S3.
    ///
    /// DANGER: If there are groups pending S3 upload (from local-only checkpoint),
    /// this will lose unflushed data. Call flush_to_s3() first if durability matters.
    pub fn clear_cache_all(&self) {
        self.prefetch_pool.wait_idle();

        // Warn if there are pending groups
        let pending_count = self.shared_dirty_groups.lock().unwrap().len();
        if pending_count > 0 {
            eprintln!(
                "[bench] WARNING: clear_cache_all with {} groups pending S3 upload! Data may be lost.",
                pending_count,
            );
        }

        let states = self.cache.group_states.lock();
        for s in states.iter() {
            s.store(GroupState::None as u8, Ordering::Release);
        }
        drop(states);

        self.cache.group_access.lock().clear();
        self.cache.interior_pages.lock().clear();
        self.cache.interior_groups.lock().clear();
        self.cache.index_pages.lock().clear();

        {
            let mut bitmap = self.cache.bitmap.lock();
            bitmap.bits.fill(0);
            let _ = bitmap.persist();
        }

        // Clear ALL sub-chunk tracker entries including Pinned and Index
        {
            let mut tracker = self.cache.tracker.lock();
            tracker.clear_all();
        }
    }

    /// Upload locally-checkpointed dirty pages to S3 without holding any SQLite lock.
    /// See [`TieredVfs::flush_to_s3`] for full documentation.
    pub fn flush_to_s3(&self) -> io::Result<()> {
        let _guard = self.flush_lock.lock().unwrap();
        flush::flush_dirty_groups_to_s3(
            &self.s3,
            &self.cache,
            &self.shared_manifest,
            &self.shared_dirty_groups,
            self.compression_level,
            #[cfg(feature = "zstd")]
            self.dictionary.as_deref(),
            self.encryption_key,
            self.gc_enabled,
            self.access_history.as_ref(),
            self.prediction.as_ref(),
        )
    }

    /// Returns true if there are dirty groups pending S3 upload.
    pub fn has_pending_flush(&self) -> bool {
        !self.shared_dirty_groups.lock().unwrap().is_empty()
    }

    /// Reset S3 I/O counters. Returns (fetch_count, fetch_bytes) before reset.
    pub fn reset_s3_counters(&self) -> (u64, u64) {
        let count = self.s3.fetch_count.swap(0, Ordering::Relaxed);
        let bytes = self.s3.fetch_bytes.swap(0, Ordering::Relaxed);
        self.s3.put_count.swap(0, Ordering::Relaxed);
        self.s3.put_bytes.swap(0, Ordering::Relaxed);
        (count, bytes)
    }

    /// Read current S3 GET counters without resetting.
    pub fn s3_counters(&self) -> (u64, u64) {
        (
            self.s3.fetch_count.load(Ordering::Relaxed),
            self.s3.fetch_bytes.load(Ordering::Relaxed),
        )
    }

    /// Read current S3 PUT counters without resetting.
    pub fn s3_put_counters(&self) -> (u64, u64) {
        (
            self.s3.put_count.load(Ordering::Relaxed),
            self.s3.put_bytes.load(Ordering::Relaxed),
        )
    }

    /// Get page numbers for all groups pending S3 upload.
    /// Used by clear_cache methods to protect unflushed pages from eviction.
    fn pending_group_pages(&self) -> HashMap<u64, Vec<u64>> {
        let pending = self.shared_dirty_groups.lock().unwrap();
        if pending.is_empty() {
            return HashMap::new();
        }
        let manifest = self.shared_manifest.read();
        let mut result = HashMap::new();
        for &gid in pending.iter() {
            let pages = manifest.group_page_nums(gid).into_owned();
            result.insert(gid, pages);
        }
        result
    }
}
