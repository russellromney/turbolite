//! Lightweight benchmark handle for cache-level testing.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::*;

/// Lightweight handle for benchmarking -- shares the same S3 client, cache,
/// manifest, and dirty groups as the VFS.
/// Obtained via [`TurboliteVfs::shared_state`] before registering the VFS.
pub struct TurboliteSharedState {
    pub(super) s3: Arc<S3Client>,
    pub(super) cache: Arc<DiskCache>,
    pub(super) prefetch_pool: Arc<PrefetchPool>,
    // Shared state for flush_to_s3()
    pub(super) shared_manifest: Arc<ArcSwap<Manifest>>,
    pub(super) shared_dirty_groups: Arc<Mutex<HashSet<u64>>>,
    pub(super) pending_flushes: Arc<Mutex<Vec<staging::PendingFlush>>>,
    pub(super) flush_lock: Arc<Mutex<()>>,
    pub(super) compression_level: i32,
    #[cfg(feature = "zstd")]
    pub(super) dictionary: Option<Vec<u8>>,
    pub(super) encryption_key: Option<[u8; 32]>,
    pub(super) gc_enabled: bool,
    pub(super) override_threshold: u32,
    pub(super) compaction_threshold: u32,
}

impl TurboliteSharedState {
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
            let bitmap = self.cache.bitmap.read();
            for b in &bitmap.bits {
                b.store(0, std::sync::atomic::Ordering::Relaxed);
            }
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

        // Prune compressed cache index
        {
            let mut keep = pinned_pages.clone();
            keep.extend(&index_pages);
            for pages in pending_groups.values() {
                keep.extend(pages);
            }
            let gp = self.cache.group_pages.read();
            if let Some(g0_pages) = gp.first() {
                keep.extend(g0_pages);
            } else {
                let ppg = self.cache.pages_per_group as u64;
                for p in 0..ppg { keep.insert(p); }
            }
            self.cache.prune_cache_index(&keep);
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
            let bitmap = self.cache.bitmap.read();
            for b in &bitmap.bits {
                b.store(0, std::sync::atomic::Ordering::Relaxed);
            }
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

        // Prune compressed cache index
        {
            let mut keep = pinned_pages.clone();
            for pages in pending_groups.values() {
                keep.extend(pages);
            }
            let gp = self.cache.group_pages.read();
            if let Some(g0_pages) = gp.first() {
                keep.extend(g0_pages);
            } else {
                let ppg = self.cache.pages_per_group as u64;
                for p in 0..ppg { keep.insert(p); }
            }
            self.cache.prune_cache_index(&keep);
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
            let bitmap = self.cache.bitmap.read();
            for b in &bitmap.bits {
                b.store(0, std::sync::atomic::Ordering::Relaxed);
            }
            let _ = bitmap.persist();
        }

        // Clear compressed cache index entirely
        self.cache.clear_cache_index();

        // Clear ALL sub-chunk tracker entries including Pinned and Index
        {
            let mut tracker = self.cache.tracker.lock();
            tracker.clear_all();
        }
    }

    /// Upload locally-checkpointed dirty pages to S3 without holding any SQLite lock.
    /// See [`TurboliteVfs::flush_to_s3`] for full documentation.
    pub fn flush_to_s3(&self) -> io::Result<()> {
        let _guard = self.flush_lock.lock().unwrap();
        flush::flush_dirty_groups_to_s3(
            &self.s3,
            &self.cache,
            &self.shared_manifest,
            &self.shared_dirty_groups,
            &self.pending_flushes,
            self.compression_level,
            #[cfg(feature = "zstd")]
            self.dictionary.as_deref(),
            self.encryption_key,
            self.gc_enabled,
            self.override_threshold,
            self.compaction_threshold,
        )
    }

    /// Returns true if there are dirty groups or staging logs pending S3 upload.
    pub fn has_pending_flush(&self) -> bool {
        !self.shared_dirty_groups.lock().unwrap().is_empty()
            || !self.pending_flushes.lock().unwrap().is_empty()
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
        let manifest = self.shared_manifest.load();
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
        let manifest = self.shared_manifest.load();
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
        let manifest = self.shared_manifest.load();
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
                        let ovrs = manifest.subframe_overrides.get(gid as usize).cloned().unwrap_or_default();
                        self.prefetch_pool.submit(
                            gid, key.clone(), ft,
                            manifest.page_size, manifest.sub_pages_per_frame,
                            gp, ovrs, manifest.version,
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
        let manifest = self.shared_manifest.load();
        let mut result = HashMap::new();
        for &gid in pending.iter() {
            let pages = manifest.group_page_nums(gid).into_owned();
            result.insert(gid, pages);
        }
        result
    }

    /// Compact B-tree groups: re-walk B-trees, identify dead pages, repack.
    /// `threshold` is the dead-page ratio (0.0-1.0) above which a B-tree is repacked.
    /// All pages must be in the local cache before calling (run a query or warm first).
    /// Returns JSON report with compaction results.
    pub fn compact(&self, threshold: f64) -> io::Result<String> {
        let manifest = (**self.shared_manifest.load()).clone();
        let page_size = manifest.page_size;
        let ppg = manifest.pages_per_group;

        // Analyze dead space (reads pages from local cache only)
        let report = compact::analyze_dead_space(
            &manifest,
            page_size,
            &|pnum| {
                let mut buf = vec![0u8; page_size as usize];
                self.cache.read_page(pnum, &mut buf).ok()?;
                Some(buf)
            },
            threshold,
        );

        if report.candidates.is_empty() {
            return Ok(format!(
                "{{\"compacted\":0,\"total_dead\":{},\"total_live\":{},\"message\":\"no B-trees exceed {:.0}% dead space threshold\"}}",
                report.total_dead, report.total_live, threshold * 100.0,
            ));
        }

        // Compact each candidate B-tree
        let mut compacted = 0u32;
        let mut total_freed = 0usize;
        let mut replaced_keys: Vec<String> = Vec::new();
        let mut manifest = (**self.shared_manifest.load()).clone();
        // Phase Somme: use file change counter from cache for version
        let next_version = read_change_counter_from_cache(&self.cache, page_size);

        for btree_info in &report.btrees {
            if !report.candidates.contains(&btree_info.name) {
                continue;
            }

            let root = btree_info.root_page;
            let compact_result = match compact::compact_btree(
                &manifest,
                root,
                ppg,
                page_size,
                &|pnum| {
                    let mut buf = vec![0u8; page_size as usize];
                    if self.cache.read_page(pnum, &mut buf).is_ok() {
                        Some(buf)
                    } else {
                        None
                    }
                },
            ) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("[compact] ERROR compacting B-tree {}: {}", btree_info.name, e);
                    continue;
                }
            };

            eprintln!(
                "[compact] {} : {} pages -> {} pages ({} freed), {} groups -> {} groups",
                compact_result.btree_name,
                compact_result.pages_before,
                compact_result.pages_after,
                compact_result.pages_freed(),
                compact_result.old_group_ids.len(),
                compact_result.new_groups.len(),
            );

            // Allocate new group IDs, encode and upload
            let mut new_group_ids: Vec<u64> = Vec::new();
            let mut uploads: Vec<(String, Vec<u8>)> = Vec::new();
            let mut new_frame_tables_patch: Vec<(usize, Vec<FrameEntry>)> = Vec::new();

            for new_pages in &compact_result.new_groups {
                // Reuse an old group ID if available, otherwise allocate new
                let gid = if new_group_ids.len() < compact_result.old_group_ids.len() {
                    compact_result.old_group_ids[new_group_ids.len()]
                } else {
                    manifest.group_pages.len() as u64 + new_group_ids.len() as u64
                        - compact_result.old_group_ids.len() as u64
                };
                new_group_ids.push(gid);

                // Read pages from cache
                let mut pages: Vec<Option<Vec<u8>>> = Vec::with_capacity(new_pages.len());
                for &pnum in new_pages {
                    let mut buf = vec![0u8; page_size as usize];
                    if self.cache.read_page(pnum, &mut buf).is_ok() {
                        pages.push(Some(buf));
                    } else {
                        pages.push(None);
                    }
                }

                let key = self.s3.page_group_key(gid, next_version);
                let use_seekable = manifest.sub_pages_per_frame > 0;

                if use_seekable {
                    let (encoded, ft) = encode_page_group_seekable(
                        &pages,
                        page_size,
                        manifest.sub_pages_per_frame,
                        self.compression_level,
                        #[cfg(feature = "zstd")]
                        None, // dictionaries not used in compaction (same as import)
                        self.encryption_key.as_ref(),
                    )?;
                    uploads.push((key.clone(), encoded));
                    new_frame_tables_patch.push((gid as usize, ft));
                } else {
                    let encoded = encode_page_group(
                        &pages,
                        page_size,
                        self.compression_level,
                        #[cfg(feature = "zstd")]
                        None,
                        self.encryption_key.as_ref(),
                    )?;
                    uploads.push((key.clone(), encoded));
                }

                // Update manifest
                while manifest.page_group_keys.len() <= gid as usize {
                    manifest.page_group_keys.push(String::new());
                }
                while manifest.group_pages.len() <= gid as usize {
                    manifest.group_pages.push(Vec::new());
                }
                // Track old key for GC
                let old_key = manifest.page_group_keys.get(gid as usize).cloned().unwrap_or_default();
                if !old_key.is_empty() {
                    replaced_keys.push(old_key);
                }
                manifest.page_group_keys[gid as usize] = key;
                manifest.group_pages[gid as usize] = new_pages.clone();
            }

            // Clear old group IDs that are no longer used
            for &old_gid in &compact_result.old_group_ids {
                if !new_group_ids.contains(&old_gid) {
                    if let Some(old_key) = manifest.page_group_keys.get(old_gid as usize) {
                        if !old_key.is_empty() {
                            replaced_keys.push(old_key.clone());
                        }
                    }
                    if let Some(k) = manifest.page_group_keys.get_mut(old_gid as usize) {
                        *k = String::new();
                    }
                    if let Some(p) = manifest.group_pages.get_mut(old_gid as usize) {
                        p.clear();
                    }
                }
            }

            // Update B-tree manifest entry with new group IDs
            if let Some(entry) = manifest.btrees.get_mut(&root) {
                entry.group_ids = new_group_ids;
            }

            // Upload new groups
            if !uploads.is_empty() {
                self.s3.put_page_groups(&uploads)?;
            }

            // Patch frame tables
            for (gid_idx, ft) in new_frame_tables_patch {
                while manifest.frame_tables.len() <= gid_idx {
                    manifest.frame_tables.push(Vec::new());
                }
                manifest.frame_tables[gid_idx] = ft;
            }

            total_freed += compact_result.pages_freed();
            compacted += 1;
        }

        // Update manifest version and rebuild indexes
        manifest.version = next_version;
        manifest.build_page_index();

        // Upload new manifest
        self.s3.put_manifest(&manifest)?;
        self.shared_manifest.store(Arc::new(manifest));

        // GC old groups
        if !replaced_keys.is_empty() && self.gc_enabled {
            let keys = replaced_keys.clone();
            let s3 = self.s3.clone();
            eprintln!("[compact] GC: deleting {} replaced S3 objects", keys.len());
            std::thread::spawn(move || {
                if let Err(e) = s3.delete_objects(&keys) {
                    eprintln!("[compact] GC ERROR: {}", e);
                }
            });
        }

        Ok(format!(
            "{{\"compacted\":{},\"pages_freed\":{},\"replaced_keys\":{},\"total_dead\":{},\"total_live\":{}}}",
            compacted, total_freed, replaced_keys.len(), report.total_dead, report.total_live,
        ))
    }

    /// Materialize the full database from S3 page groups into a local SQLite file.
    ///
    /// Downloads ALL page groups, decodes them, and writes pages in order to produce
    /// a standard SQLite database file. Returns the manifest version (checkpoint version).
    ///
    /// This is the "snapshot source" for walrust integration: turbolite page groups
    /// serve as the snapshot, walrust applies WAL increments on top.
    pub fn materialize_to_file(&self, output: &std::path::Path) -> io::Result<u64> {
        use std::os::unix::fs::FileExt;

        let manifest = (**self.shared_manifest.load()).clone();
        let page_size = manifest.page_size;
        let page_count = manifest.page_count;

        if page_count == 0 || page_size == 0 {
            return Err(io::Error::new(io::ErrorKind::NotFound, "empty manifest, no data to materialize"));
        }

        eprintln!(
            "[materialize] page_count={}, page_size={}, groups={}, version={}",
            page_count, page_size, manifest.page_group_keys.len(), manifest.version,
        );

        // Create output file
        let file = std::fs::File::create(output)?;
        // Pre-allocate the file to the correct size
        let total_size = page_count * page_size as u64;
        file.set_len(total_size)?;

        // Collect non-empty page group keys
        let keys_with_gids: Vec<(u64, String)> = manifest.page_group_keys.iter()
            .enumerate()
            .filter(|(_, k)| !k.is_empty())
            .map(|(gid, k)| (gid as u64, k.clone()))
            .collect();

        // Download in batches of 50 (parallel within each batch)
        let batch_size = 50;
        let total = keys_with_gids.len();
        let use_seekable = manifest.sub_pages_per_frame > 0;

        for (batch_idx, batch) in keys_with_gids.chunks(batch_size).enumerate() {
            let keys: Vec<String> = batch.iter().map(|(_, k)| k.clone()).collect();
            let data_map = self.s3.get_page_groups_by_key(&keys)?;

            for &(gid, ref key) in batch {
                let pg_data = match data_map.get(key) {
                    Some(d) => d,
                    None => {
                        eprintln!("[materialize] WARNING: missing S3 object for group {} key {}", gid, key);
                        continue;
                    }
                };

                let page_nums = manifest.group_page_nums(gid);

                // Decode the page group
                let ft = manifest.frame_tables.get(gid as usize);
                let has_ft = use_seekable && ft.map(|f| !f.is_empty()).unwrap_or(false);

                let decoded_pages: Vec<u8> = if has_ft {
                    let ft = ft.unwrap();
                    let (_pc, _ps, bulk) = decode_page_group_seekable_full(
                        pg_data,
                        ft,
                        page_size,
                        page_nums.len() as u32,
                        page_count,
                        0,
                        #[cfg(feature = "zstd")]
                        None,
                        self.encryption_key.as_ref(),
                    )?;
                    bulk
                } else {
                    let (_pc, _ps, pages) = decode_page_group(
                        pg_data,
                        #[cfg(feature = "zstd")]
                        None,
                        self.encryption_key.as_ref(),
                    )?;
                    pages.into_iter().flatten().collect()
                };

                // Write each page to the correct offset in the output file
                let ps = page_size as usize;
                for (i, &pnum) in page_nums.iter().enumerate() {
                    if pnum >= page_count { break; }
                    let start = i * ps;
                    let end = start + ps;
                    if end <= decoded_pages.len() {
                        file.write_all_at(&decoded_pages[start..end], pnum * page_size as u64)?;
                    }
                }
            }

            let done = std::cmp::min((batch_idx + 1) * batch_size, total);
            eprintln!("[materialize] downloaded {}/{} page groups", done, total);
        }

        eprintln!(
            "[materialize] wrote {:.1}MB to {} (version {}, change_counter {})",
            total_size as f64 / (1024.0 * 1024.0),
            output.display(),
            manifest.version,
            manifest.change_counter,
        );

        // Return change_counter for walrust WAL replay (txid > change_counter)
        Ok(manifest.change_counter)
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
