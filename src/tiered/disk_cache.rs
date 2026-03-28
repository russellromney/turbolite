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

    // ── Phase Stalingrad-c: cache stats counters ──
    /// Cache hits (page was in bitmap/cache, served from local disk).
    pub(crate) stat_hits: AtomicU64,
    /// Cache misses (page not cached, triggered S3 fetch).
    pub(crate) stat_misses: AtomicU64,
    /// Sub-chunks evicted (by budget enforcement, TTL, or manual eviction).
    pub(crate) stat_evictions: AtomicU64,
    /// Bytes evicted from cache.
    pub(crate) stat_bytes_evicted: AtomicU64,
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
        let mut tracker = if encryption_key.is_some() {
            SubChunkTracker::new_encrypted(tracker_path, pages_per_group, spf, encryption_key)
        } else {
            SubChunkTracker::new(tracker_path, pages_per_group, spf)
        };
        #[cfg(not(feature = "encryption"))]
        let mut tracker = SubChunkTracker::new(tracker_path, pages_per_group, spf);

        // Set sub-chunk byte size if page_size is known at construction
        if page_size > 0 && spf > 0 {
            tracker.set_sub_chunk_byte_size(spf as u64 * page_size as u64);
        }

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
            stat_hits: AtomicU64::new(0),
            stat_misses: AtomicU64::new(0),
            stat_evictions: AtomicU64::new(0),
            stat_bytes_evicted: AtomicU64::new(0),
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
        // Update tracker's sub_chunk_byte_size so cache byte accounting is correct
        let scbs = self.sub_pages_per_frame as u64 * new_page_size as u64;
        self.tracker.lock().set_sub_chunk_byte_size(scbs);
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

    /// Reset a group from Fetching back to None (e.g., submit failed or claim no longer needed).
    /// Wakes any threads waiting on this group so they can fall through.
    pub(crate) fn unclaim_group(&self, gid: u64) {
        let states = self.group_states.lock();
        if let Some(s) = states.get(gid as usize) {
            s.store(GroupState::None as u8, Ordering::Release);
        }
        self.group_condvar.notify_all();
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

    /// Get page numbers for a group (BTreeAware lookup or Positional fallback).
    fn group_page_nums(&self, gid: u64) -> Vec<u64> {
        let gp = self.group_pages.read();
        if let Some(explicit) = gp.get(gid as usize) {
            explicit.clone()
        } else {
            let ppg = self.pages_per_group as u64;
            let start = gid * ppg;
            (start..start + ppg).collect()
        }
    }

    /// Get page numbers for a sub-chunk within a group.
    pub(crate) fn sub_chunk_page_nums(&self, id: SubChunkId) -> Vec<u64> {
        let gp = self.group_pages.read();
        if let Some(explicit) = gp.get(id.group_id as usize) {
            let spf = self.sub_pages_per_frame as usize;
            let start = id.frame_index as usize * spf;
            let end = std::cmp::min(start + spf, explicit.len());
            if start < explicit.len() {
                explicit[start..end].to_vec()
            } else {
                Vec::new()
            }
        } else {
            let ppg = self.pages_per_group as u64;
            let spf = self.sub_pages_per_frame as u64;
            let start = id.group_id as u64 * ppg + id.frame_index as u64 * spf;
            (start..start + spf).collect()
        }
    }

    /// Clear bitmap bits and hole-punch pages on Linux.
    pub(crate) fn clear_pages_from_disk(&self, page_nums: &[u64]) {
        {
            let mut bitmap = self.bitmap.lock();
            for &pnum in page_nums {
                bitmap.clear(pnum);
            }
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            let file = self.cache_file.write();
            let ps = self.page_size.load(Ordering::Acquire) as u64;
            for &pnum in page_nums {
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
    }

    /// Evict a single page group from the local cache.
    pub(crate) fn evict_group(&self, gid: u64) {
        let page_nums = self.group_page_nums(gid);
        self.clear_pages_from_disk(&page_nums);
        self.tracker.lock().remove_group(gid as u32);

        let states = self.group_states.lock();
        if let Some(s) = states.get(gid as usize) {
            s.store(GroupState::None as u8, Ordering::Release);
        }
    }

    /// Current cache size in bytes (sub-chunk granularity).
    pub(crate) fn cache_bytes(&self) -> u64 {
        self.tracker.lock().current_cache_bytes
    }

    /// Evict a single sub-chunk from the cache. Clears bitmap, hole-punches on Linux,
    /// removes from tracker. Does NOT reset group state (other sub-chunks may remain).
    pub(crate) fn evict_sub_chunk(&self, id: SubChunkId) {
        let page_nums = self.sub_chunk_page_nums(id);
        self.clear_pages_from_disk(&page_nums);
        let scbs = self.tracker.lock().sub_chunk_byte_size;
        self.tracker.lock().remove(id);
        self.stat_evictions.fetch_add(1, Ordering::Relaxed);
        self.stat_bytes_evicted.fetch_add(scbs, Ordering::Relaxed);
    }

    /// Evict sub-chunks until cache is within budget. Skips groups in skip_groups
    /// (dirty, pending flush, or currently being fetched). Returns number evicted.
    ///
    /// Collects all evictable sub-chunks, sorts by score (ascending = most evictable
    /// first), then evicts in order. O(n log n) total instead of O(n^2) per-iteration scan.
    pub(crate) fn evict_to_budget(&self, budget_bytes: u64, skip_groups: &HashSet<u64>) -> u32 {
        // Collect and sort victims in one tracker lock
        let victims: Vec<SubChunkId> = {
            let mut tracker = self.tracker.lock();
            if tracker.current_cache_bytes <= budget_bytes {
                return 0;
            }
            if tracker.current_cache_bytes <= tracker.pinned_bytes() {
                return 0;
            }
            let mut scored = tracker.score_evictable(skip_groups);
            scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
            scored.into_iter().map(|(id, _)| id).collect()
        };

        let mut evicted = 0u32;
        for id in victims {
            // Check if we're under budget (re-lock tracker briefly)
            {
                let tracker = self.tracker.lock();
                if tracker.current_cache_bytes <= budget_bytes {
                    break;
                }
            }
            // Remove from tracker, then clean disk
            let scbs = {
                let mut tracker = self.tracker.lock();
                let scbs = tracker.sub_chunk_byte_size;
                tracker.remove(id);
                scbs
            };
            let page_nums = self.sub_chunk_page_nums(id);
            self.clear_pages_from_disk(&page_nums);
            self.stat_evictions.fetch_add(1, Ordering::Relaxed);
            self.stat_bytes_evicted.fetch_add(scbs, Ordering::Relaxed);
            evicted += 1;
        }
        evicted
    }

    /// Persist the page bitmap and sub-chunk tracker to disk.
    pub(crate) fn persist_bitmap(&self) -> io::Result<()> {
        self.bitmap.lock().persist()?;
        self.tracker.lock().persist()
    }
}

#[cfg(test)]
#[path = "test_disk_cache.rs"]
mod tests;

