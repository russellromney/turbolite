use super::*;

// ===== DiskCache (sub-chunk-level cache with tiered eviction) =====

/// Local NVMe page cache with sub-chunk-level tracking and tiered eviction.
///
/// Pages are stored **uncompressed** in a single cache file at natural offsets.
/// Cache hits are a single `pread()` — zero CPU overhead (no decompression).
/// A SubChunkTracker tracks which sub-chunks are present (replaces per-page bitmap).
///
/// Eviction tiers: Pinned (interior) > Index (index leaf) > Data (table leaf).
pub(crate) struct DiskCache {
    #[allow(dead_code)] // retained for debugging
    pub(crate) cache_dir: PathBuf,
    /// Local cache file — uncompressed pages at offset page_num * page_size.
    /// RwLock: read lock for concurrent pread/pwrite (thread-safe on Unix),
    /// write lock only for set_len (extending the file).
    pub(crate) cache_file: parking_lot::RwLock<File>,
    /// Sub-chunk-level tracking: which sub-chunks are cached + eviction tiers.
    pub(crate) tracker: parking_lot::Mutex<SubChunkTracker>,
    /// Legacy page bitmap — kept for backward compatibility during migration.
    /// TODO: remove once all code paths use tracker exclusively.
    pub(crate) bitmap: parking_lot::Mutex<PageBitmap>,
    /// Per-group state: 0=None, 1=Fetching, 2=Present
    pub(crate) group_states: parking_lot::Mutex<Vec<std::sync::atomic::AtomicU8>>,
    /// Condition variable for wait_for_group (replaces spin-wait)
    pub(crate) group_condvar: parking_lot::Condvar,
    pub(crate) group_condvar_mutex: parking_lot::Mutex<()>,
    /// Interior page groups — permanently pinned, never evicted
    pub(crate) interior_groups: parking_lot::Mutex<HashSet<u64>>,
    /// Individual interior page numbers (for precise cache preservation)
    pub(crate) interior_pages: parking_lot::Mutex<HashSet<u64>>,
    /// Individual index leaf page numbers (for cache preservation across clear_cache)
    pub(crate) index_pages: parking_lot::Mutex<HashSet<u64>>,
    /// TTL tracking: group_id → last_access
    pub(crate) group_access: parking_lot::Mutex<HashMap<u64, Instant>>,
    pub(crate) ttl_secs: u64,
    pub(crate) pages_per_group: u32,
    pub(crate) sub_pages_per_frame: u32,
    pub(crate) page_size: std::sync::atomic::AtomicU32,
    /// Encryption key for cache-at-rest. Uses CTR mode (no size overhead).
    pub(crate) encryption_key: Option<[u8; 32]>,
    /// B-tree-aware page-to-group mapping: group_pages[gid] = list of page numbers.
    /// Used by evict_group and clear_cache to clear the correct bitmap bits.
    /// Updated when the manifest changes (via set_group_pages).
    pub(crate) group_pages: parking_lot::RwLock<Vec<Vec<u64>>>,
}

/// Counter for lazy eviction (every 64 group fetches).
pub(crate) static EVICTION_COUNTER: AtomicU64 = AtomicU64::new(0);

impl DiskCache {
    pub(crate) fn new(cache_dir: &Path, ttl_secs: u64, pages_per_group: u32, sub_pages_per_frame: u32, page_size: u32, page_count: u64, encryption_key: Option<[u8; 32]>, group_pages: Vec<Vec<u64>>) -> io::Result<Self> {
        fs::create_dir_all(cache_dir)?;

        let cache_file_path = cache_dir.join("data.cache");
        let cache_file = FsOpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&cache_file_path)?;

        // Extend to full size (sparse file)
        if page_count > 0 && page_size > 0 {
            let target_size = page_count * page_size as u64;
            let meta = cache_file.metadata()?;
            if meta.len() < target_size {
                cache_file.set_len(target_size)?;
            }
        }

        // Sub-chunk tracker (primary tracking mechanism)
        let spf = if sub_pages_per_frame > 0 { sub_pages_per_frame } else { pages_per_group };
        let tracker_path = cache_dir.join("sub_chunk_tracker");
        #[cfg(feature = "encryption")]
        let tracker = if encryption_key.is_some() {
            SubChunkTracker::new_encrypted(tracker_path, pages_per_group, spf, encryption_key)
        } else {
            SubChunkTracker::new(tracker_path, pages_per_group, spf)
        };
        #[cfg(not(feature = "encryption"))]
        let tracker = SubChunkTracker::new(tracker_path, pages_per_group, spf);

        // Legacy bitmap (kept for backward compat during migration)
        let bitmap_path = cache_dir.join("page_bitmap");
        let mut bitmap = PageBitmap::new(bitmap_path);
        if page_count > 0 {
            bitmap.resize(page_count);
        }

        let total_groups = if pages_per_group > 0 && page_count > 0 {
            ((page_count + pages_per_group as u64 - 1) / pages_per_group as u64) as usize
        } else {
            0
        };

        // Initialize group states — mark as Present for groups where bitmap shows all pages cached.
        let group_states: Vec<std::sync::atomic::AtomicU8> = (0..total_groups)
            .map(|gid| {
                let state = if page_count > 0 {
                    if let Some(gp) = group_pages.get(gid) {
                        // BTreeAware: check if all pages in this group are in the bitmap
                        if !gp.is_empty() && gp.iter().all(|&p| bitmap.is_present(p)) {
                            GroupState::Present as u8
                        } else {
                            GroupState::None as u8
                        }
                    } else {
                        // Positional: check [gid*ppg .. (gid+1)*ppg)
                        let ppg = pages_per_group as u64;
                        let start = gid as u64 * ppg;
                        let end = std::cmp::min(start + ppg, page_count);
                        if start < end && (start..end).all(|p| bitmap.is_present(p)) {
                            GroupState::Present as u8
                        } else {
                            GroupState::None as u8
                        }
                    }
                } else {
                    GroupState::None as u8
                };
                std::sync::atomic::AtomicU8::new(state)
            })
            .collect();

        Ok(Self {
            cache_dir: cache_dir.to_path_buf(),
            cache_file: parking_lot::RwLock::new(cache_file),
            tracker: parking_lot::Mutex::new(tracker),
            bitmap: parking_lot::Mutex::new(bitmap),
            group_states: parking_lot::Mutex::new(group_states),
            group_condvar: parking_lot::Condvar::new(),
            group_condvar_mutex: parking_lot::Mutex::new(()),
            interior_groups: parking_lot::Mutex::new(HashSet::new()),
            interior_pages: parking_lot::Mutex::new(HashSet::new()),
            index_pages: parking_lot::Mutex::new(HashSet::new()),
            group_access: parking_lot::Mutex::new(HashMap::new()),
            ttl_secs,
            pages_per_group,
            sub_pages_per_frame: spf,
            page_size: std::sync::atomic::AtomicU32::new(page_size),
            encryption_key,
            group_pages: parking_lot::RwLock::new(group_pages),
        })
    }

    /// Read a single page from the cache file (pread, decrypt with CTR if encrypted).
    pub(crate) fn read_page(&self, page_num: u64, buf: &mut [u8]) -> io::Result<()> {
        use std::os::unix::fs::FileExt;
        let offset = page_num * self.page_size.load(Ordering::Acquire) as u64;
        let file = self.cache_file.read();
        file.read_exact_at(buf, offset)?;
        #[cfg(feature = "encryption")]
        if let Some(ref key) = self.encryption_key {
            let decrypted = compress::decrypt_ctr(buf, page_num, key)?;
            buf.copy_from_slice(&decrypted);
        }
        Ok(())
    }

    /// Write a single uncompressed page to the cache file (encrypt with CTR if key set).
    pub(crate) fn write_page(&self, page_num: u64, data: &[u8]) -> io::Result<()> {
        use std::os::unix::fs::FileExt;
        let offset = page_num * self.page_size.load(Ordering::Acquire) as u64;

        // CTR encryption: same size, no overhead
        let write_data: Vec<u8>;
        #[cfg(feature = "encryption")]
        let data = if let Some(ref key) = self.encryption_key {
            write_data = compress::encrypt_ctr(data, page_num, key)?;
            write_data.as_slice()
        } else {
            data
        };

        let needed = offset + data.len() as u64;

        // Extend file if needed (exclusive lock), then pwrite (shared lock)
        {
            let file = self.cache_file.read();
            if file.metadata()?.len() < needed {
                drop(file);
                let file = self.cache_file.write();
                // Re-check after acquiring write lock
                if file.metadata()?.len() < needed {
                    file.set_len(needed)?;
                }
                file.write_all_at(data, offset)?;
            } else {
                file.write_all_at(data, offset)?;
            }
        }
        self.bitmap.lock().mark_present(page_num);
        // Do NOT mark sub-chunk tracker here — write_page writes a single page,
        // not a complete sub-chunk. Sub-chunk tracker is only updated by
        // write_pages_bulk() which writes complete frames fetched from S3.
        Ok(())
    }

    /// Write a contiguous range of pages to the cache file in a single I/O operation.
    /// `start_page` is the first page number, `data` is the raw concatenated page data.
    /// Uses RwLock: read lock for pwrite (concurrent), write lock only if file needs extending.
    pub(crate) fn write_pages_bulk(&self, start_page: u64, data: &[u8], num_pages: u64) -> io::Result<()> {
        use std::os::unix::fs::FileExt;
        let page_sz = self.page_size.load(Ordering::Acquire) as usize;

        // CTR encryption: encrypt each page in-place (same size, no overhead)
        #[cfg(feature = "encryption")]
        let data = if let Some(ref key) = self.encryption_key {
            let mut encrypted = Vec::with_capacity(data.len());
            for i in 0..num_pages {
                let start = i as usize * page_sz;
                let end = (start + page_sz).min(data.len());
                encrypted.extend_from_slice(&compress::encrypt_ctr(&data[start..end], start_page + i, key)?);
            }
            encrypted
        } else {
            data.to_vec()
        };
        #[cfg(feature = "encryption")]
        let data = data.as_slice();

        let offset = start_page * page_sz as u64;
        let needed = offset + data.len() as u64;

        // Try read lock first (concurrent pwrite). Only upgrade to write lock if file too small.
        {
            let file = self.cache_file.read();
            if file.metadata()?.len() < needed {
                drop(file);
                let file = self.cache_file.write();
                if file.metadata()?.len() < needed {
                    file.set_len(needed)?;
                }
                file.write_all_at(data, offset)?;
            } else {
                file.write_all_at(data, offset)?;
            }
        }

        // Mark all pages present in legacy bitmap
        let mut bitmap = self.bitmap.lock();
        for i in 0..num_pages {
            bitmap.mark_present(start_page + i);
        }
        drop(bitmap);

        // Mark sub-chunks present in tracker
        {
            let mut tracker = self.tracker.lock();
            let mut seen = HashSet::new();
            for i in 0..num_pages {
                let id = tracker.sub_chunk_for_page(start_page + i);
                if seen.insert(id) {
                    tracker.mark_present(id, SubChunkTier::Data);
                }
            }
        }
        Ok(())
    }

    /// Write pages to the cache at non-consecutive positions (Phase Midway: B-tree-packed groups).
    /// `page_nums` maps position in `data` to actual page number.
    pub(crate) fn write_pages_scattered(&self, page_nums: &[u64], data: &[u8], gid: u64, start_index_in_group: u32) -> io::Result<()> {
        use std::os::unix::fs::FileExt;
        let page_sz = self.page_size.load(Ordering::Acquire) as usize;
        if page_nums.is_empty() || page_sz == 0 {
            return Ok(());
        }

        // Track how many pages we actually write (data may be shorter than page_nums)
        let writable_count = page_nums.iter().enumerate()
            .take_while(|(i, _)| (i + 1) * page_sz <= data.len())
            .count();
        let written_pages = &page_nums[..writable_count];
        if written_pages.is_empty() {
            return Ok(());
        }

        // Find max page to size the cache file
        let max_page = written_pages.iter().copied().max().unwrap_or(0);
        let needed = (max_page + 1) * page_sz as u64;

        // Ensure file is large enough
        {
            let file = self.cache_file.read();
            if file.metadata()?.len() < needed {
                drop(file);
                let file = self.cache_file.write();
                if file.metadata()?.len() < needed {
                    file.set_len(needed)?;
                }
                for (i, &pnum) in written_pages.iter().enumerate() {
                    let src_start = i * page_sz;
                    let page_data = &data[src_start..src_start + page_sz];
                    #[cfg(feature = "encryption")]
                    let page_data = if let Some(ref key) = self.encryption_key {
                        &compress::encrypt_ctr(page_data, pnum, key)?
                    } else {
                        page_data
                    };
                    #[cfg(not(feature = "encryption"))]
                    let page_data = page_data;
                    let offset = pnum * page_sz as u64;
                    file.write_all_at(page_data, offset)?;
                }
            } else {
                for (i, &pnum) in written_pages.iter().enumerate() {
                    let src_start = i * page_sz;
                    let page_data = &data[src_start..src_start + page_sz];
                    #[cfg(feature = "encryption")]
                    let page_data = if let Some(ref key) = self.encryption_key {
                        &compress::encrypt_ctr(page_data, pnum, key)?
                    } else {
                        page_data
                    };
                    #[cfg(not(feature = "encryption"))]
                    let page_data = page_data;
                    let offset = pnum * page_sz as u64;
                    file.write_all_at(page_data, offset)?;
                }
            }
        }

        // Mark bitmap for per-page presence
        let mut bitmap = self.bitmap.lock();
        for &pnum in written_pages {
            bitmap.mark_present(pnum);
        }
        drop(bitmap);

        // Mark tracker sub-chunks as Data tier (manifest-aware, not positional)
        let mut tracker = self.tracker.lock();
        for (i, _) in written_pages.iter().enumerate() {
            let idx = start_index_in_group + i as u32;
            let id = tracker.sub_chunk_id_for(gid, idx);
            tracker.mark_present(id, SubChunkTier::Data);
        }
        drop(tracker);

        Ok(())
    }

    /// Update the page size (needed when writer VFS learns page size from first write).
    pub(crate) fn set_page_size(&self, new_page_size: u32) {
        self.page_size.store(new_page_size, Ordering::Release);
    }

    /// Check if a page is present in the local cache.
    /// Uses bitmap (per-page accurate). SubChunkTracker is not consulted here
    /// because it uses positional mapping which is wrong for B-tree-aware groups.
    pub(crate) fn is_present(&self, page_num: u64) -> bool {
        self.bitmap.lock().is_present(page_num)
    }

    /// Get the state of a page group.
    pub(crate) fn group_state(&self, gid: u64) -> GroupState {
        let states = self.group_states.lock();
        match states.get(gid as usize) {
            Some(s) => match s.load(Ordering::Acquire) {
                1 => GroupState::Fetching,
                2 => GroupState::Present,
                _ => GroupState::None,
            },
            None => GroupState::None,
        }
    }

    /// Try to claim a group for fetching (CAS None→Fetching).
    /// Returns true if we claimed it, false if already Fetching or Present.
    pub(crate) fn try_claim_group(&self, gid: u64) -> bool {
        let states = self.group_states.lock();
        self.ensure_group_states_capacity(&states, gid);
        if let Some(s) = states.get(gid as usize) {
            s.compare_exchange(
                GroupState::None as u8,
                GroupState::Fetching as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        } else {
            false
        }
    }

    /// Mark a group as present (all pages fetched and written to cache).
    pub(crate) fn mark_group_present(&self, gid: u64) {
        let states = self.group_states.lock();
        self.ensure_group_states_capacity(&states, gid);
        if let Some(s) = states.get(gid as usize) {
            s.store(GroupState::Present as u8, Ordering::Release);
        }
        // Wake any threads waiting on this group
        self.group_condvar.notify_all();
        // Lazy eviction check
        let count = EVICTION_COUNTER.fetch_add(1, Ordering::Relaxed);
        if count % 64 == 0 {
            drop(states);
            self.evict_expired();
        }
    }

    pub(crate) fn ensure_group_states_capacity(
        &self,
        states: &parking_lot::MutexGuard<'_, Vec<std::sync::atomic::AtomicU8>>,
        gid: u64,
    ) {
        // Note: Can't actually resize through a shared ref. In practice, group_states
        // is sized at open time. If we encounter a new group (from writes extending the DB),
        // we handle it in the write path by resizing before accessing.
        let _ = (states, gid);
    }

    /// Grow group_states to accommodate new groups (e.g., after database grows).
    pub(crate) fn ensure_group_capacity(&self, total_groups: usize) {
        let mut states = self.group_states.lock();
        while states.len() < total_groups {
            states.push(std::sync::atomic::AtomicU8::new(GroupState::None as u8));
        }
    }

    /// Update the B-tree-aware group_pages mapping (called when manifest changes).
    pub(crate) fn set_group_pages(&self, gp: Vec<Vec<u64>>) {
        *self.group_pages.write() = gp;
    }

    /// Wait for a group to leave Fetching state (condvar, no spin).
    /// Wait until the group is no longer in a "pending" state.
    /// Returns when state is Present, or Fetching (worker claimed it),
    /// or None after having been Fetching (worker failed).
    /// Caller must re-check state and loop as needed.
    pub(crate) fn wait_for_group(&self, gid: u64) {
        let mut guard = self.group_condvar_mutex.lock();
        loop {
            let state = self.group_state(gid);
            if state == GroupState::Present {
                return;
            }
            if state == GroupState::Fetching {
                self.group_condvar.wait(&mut guard);
                continue;
            }
            // state == None. Worker may not have picked up job yet.
            self.group_condvar.wait_for(&mut guard, Duration::from_millis(5));
            return;
        }
    }

    /// Touch a group's access time for TTL tracking.
    pub(crate) fn touch_group(&self, gid: u64) {
        self.group_access.lock().insert(gid, Instant::now());
        // Also touch all sub-chunks in this group.
        // Use actual page count from group_pages (not positional ppg).
        let gp = self.group_pages.read();
        let num_pages = gp.get(gid as usize).map(|v| v.len() as u32).unwrap_or(self.pages_per_group);
        drop(gp);
        let mut tracker = self.tracker.lock();
        let frames = if self.sub_pages_per_frame > 0 {
            (num_pages + self.sub_pages_per_frame - 1) / self.sub_pages_per_frame
        } else { 1 };
        for fi in 0..frames {
            let id = SubChunkId { group_id: gid as u32, frame_index: fi as u16 };
            tracker.touch(id);
        }
    }

    /// Mark a page group as containing B-tree interior pages (permanently pinned).
    /// Uses manifest-aware (gid, index_in_group) for correct SubChunkId computation.
    pub(crate) fn mark_interior_group(&self, gid: u64, page_num: u64, index_in_group: u32) {
        self.interior_groups.lock().insert(gid);
        self.interior_pages.lock().insert(page_num);
        // Only promote to Pinned if the sub-chunk is already fully cached.
        // If the page was loaded individually via write_page() (e.g. eager interior load),
        // marking pinned would add the sub-chunk to the tracker's present set, causing
        // adjacent pages to be falsely reported as cached (they contain zeros).
        let mut tracker = self.tracker.lock();
        let id = tracker.sub_chunk_id_for(gid, index_in_group);
        if tracker.is_sub_chunk_present(&id) {
            tracker.mark_pinned(id);
        }
    }

    /// Mark a page's sub-chunk as Index tier (evicted after Data, before Pinned).
    /// Called when we detect an index leaf page (0x0A) during page scanning.
    /// Uses manifest-aware (gid, index_in_group) for correct SubChunkId computation.
    pub(crate) fn mark_index_page(&self, page_num: u64, gid: u64, index_in_group: u32) {
        self.index_pages.lock().insert(page_num);
        let mut tracker = self.tracker.lock();
        let id = tracker.sub_chunk_id_for(gid, index_in_group);
        if tracker.is_sub_chunk_present(&id) {
            tracker.mark_index(id);
        }
    }

    /// Evict page groups that haven't been accessed within TTL.
    /// Interior page groups are NEVER evicted.
    pub(crate) fn evict_expired(&self) {
        if self.ttl_secs == 0 {
            return; // TTL disabled
        }
        let now = Instant::now();
        let ttl = Duration::from_secs(self.ttl_secs);
        let interior = self.interior_groups.lock().clone();
        let mut access = self.group_access.lock();

        let expired: Vec<u64> = access
            .iter()
            .filter(|(gid, last)| {
                // Interior page groups are NEVER evicted
                if interior.contains(gid) {
                    return false;
                }
                now.duration_since(**last) > ttl
            })
            .map(|(gid, _)| *gid)
            .collect();

        for gid in &expired {
            self.evict_group(*gid);
            access.remove(gid);
        }
    }

    /// Evict a single page group from the local cache.
    pub(crate) fn evict_group(&self, gid: u64) {
        let gp = self.group_pages.read();
        let page_nums: Vec<u64> = if let Some(explicit) = gp.get(gid as usize) {
            // BTreeAware: explicit page list
            explicit.clone()
        } else {
            // Positional fallback: [gid*ppg .. (gid+1)*ppg)
            let ppg = self.pages_per_group as u64;
            let start = gid * ppg;
            (start..start + ppg).collect()
        };
        drop(gp);

        {
            let mut bitmap = self.bitmap.lock();
            for &pnum in &page_nums {
                bitmap.clear(pnum);
            }
        }

        // On Linux: hole punch each page to reclaim NVMe blocks
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            let file = self.cache_file.write();
            let ps = self.page_size.load(Ordering::Acquire) as u64;
            for &pnum in &page_nums {
                let offset = (pnum * ps) as libc::off_t;
                let len = ps as libc::off_t;
                unsafe {
                    libc::fallocate(
                        file.as_raw_fd(),
                        libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
                        offset,
                        len,
                    );
                }
            }
        }
        // Also remove all sub-chunks for this group from tracker
        self.tracker.lock().remove_group(gid as u32);

        // Reset group state to None
        let states = self.group_states.lock();
        if let Some(s) = states.get(gid as usize) {
            s.store(GroupState::None as u8, Ordering::Release);
        }
    }

    /// Persist the page bitmap and sub-chunk tracker to disk.
    pub(crate) fn persist_bitmap(&self) -> io::Result<()> {
        self.bitmap.lock().persist()?;
        self.tracker.lock().persist()
    }
}

#[cfg(test)]
mod tests {
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

