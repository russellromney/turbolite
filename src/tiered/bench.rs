//! Lightweight benchmark handle for cache-level testing.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::*;

/// Lightweight handle for benchmarking -- shares the same S3 client and cache.
/// Obtained via [`TieredVfs::bench_handle`] before registering the VFS.
pub struct TieredBenchHandle {
    pub(super) s3: Arc<S3Client>,
    pub(super) cache: Arc<DiskCache>,
    pub(super) prefetch_pool: Arc<PrefetchPool>,
}

impl TieredBenchHandle {
    /// Evict data pages only -- interior B-tree pages and group 0 stay warm.
    /// Simulates production where structural pages are always hot.
    pub fn clear_cache_data_only(&self) {
        self.prefetch_pool.wait_idle();

        let pinned_pages = self.cache.interior_pages.lock().clone();
        let index_pages = self.cache.index_pages.lock().clone();

        // Reset group states to None, except group 0 stays Present
        let states = self.cache.group_states.lock();
        for (i, s) in states.iter().enumerate() {
            if i == 0 {
                s.store(GroupState::Present as u8, Ordering::Release);
            } else {
                s.store(GroupState::None as u8, Ordering::Release);
            }
        }
        drop(states);

        self.cache.group_access.lock().clear();

        // Clear bitmap except for interior pages, index pages, and group 0
        // Uses B-tree-aware group_pages[0] (not positional 0..ppg)
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
            if let Some(g0_pages) = gp.first() {
                for &p in g0_pages {
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
    pub fn clear_cache_interior_only(&self) {
        self.prefetch_pool.wait_idle();

        let pinned_pages = self.cache.interior_pages.lock().clone();

        // Reset group states to None, except group 0 stays Present
        let states = self.cache.group_states.lock();
        for (i, s) in states.iter().enumerate() {
            if i == 0 {
                s.store(GroupState::Present as u8, Ordering::Release);
            } else {
                s.store(GroupState::None as u8, Ordering::Release);
            }
        }
        drop(states);

        self.cache.group_access.lock().clear();
        self.cache.index_pages.lock().clear();

        // Clear bitmap except for interior pages and group 0
        // Uses B-tree-aware group_pages[0] (not positional 0..ppg)
        {
            let gp = self.cache.group_pages.read();
            let mut bitmap = self.cache.bitmap.lock();
            bitmap.bits.fill(0);
            for &page in &pinned_pages {
                bitmap.mark_present(page);
            }
            if let Some(g0_pages) = gp.first() {
                for &p in g0_pages {
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
    pub fn clear_cache_all(&self) {
        self.prefetch_pool.wait_idle();

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

    /// Reset S3 I/O counters. Returns (fetch_count, fetch_bytes) before reset.
    pub fn reset_s3_counters(&self) -> (u64, u64) {
        let count = self.s3.fetch_count.swap(0, Ordering::Relaxed);
        let bytes = self.s3.fetch_bytes.swap(0, Ordering::Relaxed);
        (count, bytes)
    }

    /// Read current S3 I/O counters without resetting.
    pub fn s3_counters(&self) -> (u64, u64) {
        (
            self.s3.fetch_count.load(Ordering::Relaxed),
            self.s3.fetch_bytes.load(Ordering::Relaxed),
        )
    }
}
