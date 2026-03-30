//! Lightweight benchmark handle for cache-level testing.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::*;

/// Lightweight handle for benchmarking -- shares the same S3 client, cache,
/// manifest, and dirty groups as the VFS.
/// Obtained via [`TieredVfs::shared_state`] before registering the VFS.
pub struct TieredSharedState {
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

impl TieredSharedState {
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

    /// Evict all cached data for the named trees. Accepts comma-separated tree
    /// names (e.g., "audit_log, idx_audit_date"). Looks up each name in the
    /// manifest's tree_name_to_groups, evicts those groups from DiskCache.
    /// Skips groups that are pending S3 upload (dirty page safety) or pinned.
    /// Returns the number of groups evicted.
    pub fn evict_tree(&self, tree_names_csv: &str) -> u32 {
        let pending = self.shared_dirty_groups.lock().unwrap().clone();
        let manifest = self.shared_manifest.read();
        let interior_groups = self.cache.interior_groups.lock().clone();

        let mut evicted = 0u32;
        for name in tree_names_csv.split(',') {
            let name = name.trim();
            if name.is_empty() {
                continue;
            }
            if let Some(group_ids) = manifest.tree_name_to_groups.get(name) {
                for &gid in group_ids {
                    if pending.contains(&gid) {
                        continue;
                    }
                    if interior_groups.contains(&gid) {
                        continue;
                    }
                    self.cache.evict_group(gid);
                    evicted += 1;
                }
            }
        }
        evicted
    }

    /// Evict cached sub-chunks by tier. Accepts "data", "index", or "all".
    /// Returns number of sub-chunks evicted. Skips pending flush groups.
    /// "data" = evict Data tier only. "index" = evict Index + Data. "all" = everything except Pinned.
    pub fn evict_tier(&self, tier: &str) -> u32 {
        let pending = self.shared_dirty_groups.lock().unwrap().clone();
        let mut evicted = 0u32;
        let mut tracker = self.cache.tracker.lock();
        let target_tiers: Vec<cache_tracking::SubChunkTier> = match tier {
            "data" => vec![cache_tracking::SubChunkTier::Data],
            "index" => vec![cache_tracking::SubChunkTier::Index, cache_tracking::SubChunkTier::Data],
            "all" => vec![cache_tracking::SubChunkTier::Index, cache_tracking::SubChunkTier::Data],
            _ => return 0,
        };
        let to_evict: Vec<SubChunkId> = tracker.present.iter()
            .filter(|id| {
                let t = tracker.tiers.get(id).copied().unwrap_or(cache_tracking::SubChunkTier::Data);
                target_tiers.contains(&t) && !pending.contains(&(id.group_id as u64))
            })
            .copied()
            .collect();
        let scbs = tracker.sub_chunk_byte_size;
        for id in &to_evict {
            tracker.remove(*id);
        }
        drop(tracker);
        for id in &to_evict {
            let page_nums = self.cache.sub_chunk_page_nums(*id);
            self.cache.clear_pages_from_disk(&page_nums);
            evicted += 1;
        }
        self.cache.stat_evictions.fetch_add(evicted as u64, Ordering::Relaxed);
        self.cache.stat_bytes_evicted.fetch_add(evicted as u64 * scbs, Ordering::Relaxed);
        evicted
    }

    /// Evict cached data for trees referenced by a query. Runs EQP to extract
    /// tree names, then evicts those trees' groups. Returns number of groups evicted.
    pub fn evict_query(&self, accesses: &[query_plan::PlannedAccess]) -> u32 {
        let tree_names: Vec<&str> = accesses.iter().map(|a| a.tree_name.as_str()).collect();
        let csv = tree_names.join(",");
        self.evict_tree(&csv)
    }

    /// Return cache info as a JSON string. Includes size, tier breakdown,
    /// cached/total group counts, and S3 stats.
    pub fn cache_info(&self) -> String {
        let manifest = self.shared_manifest.read();
        let page_size = manifest.page_size as u64;
        let sub_pages = self.cache.sub_pages_per_frame as u64;
        let sub_chunk_bytes = sub_pages * page_size;
        let total_groups = manifest.page_group_keys.len() as u64;

        let tracker = self.cache.tracker.lock();
        let mut pinned_chunks = 0u64;
        let mut index_chunks = 0u64;
        let mut data_chunks = 0u64;

        let mut groups_with_data: HashSet<u32> = HashSet::new();

        for id in &tracker.present {
            groups_with_data.insert(id.group_id);
            match tracker.tiers.get(id) {
                Some(&cache_tracking::SubChunkTier::Pinned) => pinned_chunks += 1,
                Some(&cache_tracking::SubChunkTier::Index) => index_chunks += 1,
                Some(&cache_tracking::SubChunkTier::Data) | None => data_chunks += 1,
            }
        }
        drop(tracker);

        let pinned_bytes = pinned_chunks * sub_chunk_bytes;
        let index_bytes = index_chunks * sub_chunk_bytes;
        let data_bytes = data_chunks * sub_chunk_bytes;
        let total_bytes = pinned_bytes + index_bytes + data_bytes;

        let s3_gets = self.s3.fetch_count.load(Ordering::Relaxed);
        let hits = self.cache.stat_hits.load(Ordering::Relaxed);
        let misses = self.cache.stat_misses.load(Ordering::Relaxed);
        let evictions = self.cache.stat_evictions.load(Ordering::Relaxed);
        let bytes_evicted = self.cache.stat_bytes_evicted.load(Ordering::Relaxed);
        let peak_bytes = self.cache.stat_peak_cache_bytes.load(Ordering::Relaxed);
        let last_eviction = self.cache.stat_last_eviction_count.load(Ordering::Relaxed);
        let hit_rate = if hits + misses > 0 {
            hits as f64 / (hits + misses) as f64
        } else {
            0.0
        };

        format!(
            "{{\"size_bytes\":{},\"peak_bytes\":{},\"groups_cached\":{},\"groups_total\":{},\
            \"tiers\":{{\"pinned\":{{\"chunks\":{},\"bytes\":{}}},\
            \"index\":{{\"chunks\":{},\"bytes\":{}}},\
            \"data\":{{\"chunks\":{},\"bytes\":{}}}}},\
            \"hits\":{},\"misses\":{},\"hit_rate\":{:.4},\
            \"evictions\":{},\"bytes_evicted\":{},\"last_eviction_count\":{},\
            \"s3_gets_total\":{}}}",
            total_bytes, peak_bytes,
            groups_with_data.len(),
            total_groups,
            pinned_chunks, pinned_bytes,
            index_chunks, index_bytes,
            data_chunks, data_bytes,
            hits, misses, hit_rate,
            evictions, bytes_evicted, last_eviction,
            s3_gets,
        )
    }

    /// Warm cache for a planned query. Parses EQP output to extract trees,
    /// submits their groups to the prefetch pool. Non-blocking: returns immediately
    /// after submitting. Returns JSON with trees warmed and groups submitted.
    ///
    /// Note: requires a valid sqlite3 db handle to run EQP. The FFI entry point
    /// in ext_entry.c passes the db handle from the SQL function context.
    pub fn warm_from_plan(&self, accesses: &[query_plan::PlannedAccess]) -> String {
        let manifest = self.shared_manifest.read();
        let mut trees_warmed: Vec<String> = Vec::new();
        let mut groups_submitted = 0u32;

        for access in accesses {
            if let Some(group_ids) = manifest.tree_name_to_groups.get(&access.tree_name) {
                trees_warmed.push(access.tree_name.clone());
                for &gid in group_ids {
                    if let Some(key) = manifest.page_group_keys.get(gid as usize) {
                        if key.is_empty() {
                            continue;
                        }
                        let ft = manifest.frame_tables.get(gid as usize)
                            .cloned().unwrap_or_default();
                        let gp = manifest.group_pages.get(gid as usize)
                            .cloned().unwrap_or_default();
                        self.prefetch_pool.submit(
                            gid, key.clone(), ft,
                            manifest.page_size, manifest.sub_pages_per_frame,
                            gp,
                        );
                        groups_submitted += 1;
                    }
                }
            }
        }

        format!(
            "{{\"trees_warmed\":[{}],\"groups_submitted\":{}}}",
            trees_warmed.iter().map(|n| format!("\"{}\"", n)).collect::<Vec<_>>().join(","),
            groups_submitted,
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

    /// Full GC: list all S3 objects, diff against manifest, delete orphans.
    /// Returns count of objects deleted.
    pub fn gc(&self) -> io::Result<usize> {
        let manifest = self.s3.get_manifest()?.unwrap_or_else(Manifest::empty);
        let all_keys = self.s3.list_all_keys()?;

        let mut live_keys: HashSet<String> = HashSet::new();
        live_keys.insert(self.s3.manifest_key_msgpack());
        for key in &manifest.page_group_keys {
            if !key.is_empty() {
                live_keys.insert(key.clone());
            }
        }
        for key in manifest.interior_chunk_keys.values() {
            live_keys.insert(key.clone());
        }
        for key in manifest.index_chunk_keys.values() {
            live_keys.insert(key.clone());
        }

        let orphans: Vec<String> = all_keys
            .into_iter()
            .filter(|k| !live_keys.contains(k))
            .collect();

        let count = orphans.len();
        if count > 0 {
            eprintln!("[gc] deleting {} orphaned S3 objects...", count);
            self.s3.delete_objects(&orphans)?;
            eprintln!("[gc] deleted {} orphaned objects", count);
        }
        Ok(count)
    }
}
