use super::*;

// ===== TieredHandle =====

/// Database handle for tiered S3-backed storage.
///
/// MainDb files are backed by S3 with a local page-level cache.
/// WAL/journal files are passthrough to local disk.
pub struct TieredHandle {
    // --- Tiered mode (MainDb) ---
    s3: Option<Arc<S3Client>>,
    cache: Option<Arc<DiskCache>>,
    /// Shared manifest (Arc'd so flush_to_s3 can read/update outside SQLite lock).
    manifest: Arc<RwLock<Manifest>>,
    /// Dirty page numbers (data lives in disk cache, not in memory).
    /// Phase Marne: replaced HashMap<u64, Vec<u8>> with HashSet<u64> to avoid
    /// holding a second copy of every dirty page in memory.
    dirty_page_nums: RwLock<HashSet<u64>>,
    /// Page group IDs that were locally checkpointed but not yet synced to S3.
    /// Populated during local-checkpoint-only mode; drained by flush_to_s3().
    /// Arc'd so flush_to_s3 can drain from outside SQLite lock.
    s3_dirty_groups: Arc<Mutex<HashSet<u64>>>,
    page_size: RwLock<u32>,
    pages_per_group: u32,
    compression_level: i32,
    read_only: bool,
    /// Per-VFS sync mode (Durable = S3 upload in sync, LocalThenFlush = defer to flush_to_s3)
    sync_mode: SyncMode,
    /// Per-tree consecutive cache miss counters (for fraction-based prefetch).
    /// Keyed by B-tree name. Unknown trees use `default_miss_count`.
    tree_miss_counts: HashMap<String, u8>,
    /// Miss counter for pages not belonging to any known tree (Positional strategy).
    default_miss_count: u8,
    /// Radial prefetch schedule (Positional strategy).
    prefetch_hops: Vec<f32>,
    /// Prefetch schedule for SEARCH queries (aggressive warmup).
    prefetch_search: Vec<f32>,
    /// Prefetch schedule for index lookups / point queries (conservative).
    prefetch_lookup: Vec<f32>,
    /// Tree names from current query plan that are SEARCH (not SCAN).
    search_trees: HashSet<String>,
    /// Fixed thread pool for background prefetch.
    prefetch_pool: Option<Arc<PrefetchPool>>,

    // --- Compression dictionary ---
    #[cfg(feature = "zstd")]
    encoder_dict: Option<zstd::dict::EncoderDictionary<'static>>,
    #[cfg(feature = "zstd")]
    decoder_dict: Option<zstd::dict::DecoderDictionary<'static>>,

    /// Auto-GC: delete old page group versions after checkpoint.
    gc_enabled: bool,

    /// AES-256-GCM encryption key for S3 data and local cache.
    encryption_key: Option<[u8; 32]>,

    // --- Phase Marne: query-plan-aware prefetch ---
    /// When true, the VFS drains the global plan queue on first read after step().
    query_plan_prefetch: bool,

    // --- Phase Verdun: predictive prefetch ---
    /// Per-connection lock session tracker (B-tree touches within a lock lifecycle).
    lock_session: prediction::LockSession,
    /// Shared prediction table (lives on TieredVfs, shared across connections).
    prediction: Option<prediction::SharedPrediction>,
    /// Shared access history (lives on TieredVfs, shared across connections).
    access_history: Option<prediction::SharedAccessHistory>,
    /// B-tree names that had dirty pages this session (for write decay, applied once per flush).
    dirty_btrees: HashSet<String>,

    // --- Phase Stalingrad: cache eviction ---
    /// Maximum cache size in bytes. None = unlimited.
    cache_limit: Option<u64>,

    // --- Passthrough mode (WAL/journal) ---
    passthrough_file: Option<RwLock<File>>,

    // --- Shared ---
    lock: RwLock<LockKind>,
    db_path: PathBuf,
    /// Separate file handle for byte-range locking
    lock_file: Option<std::sync::Arc<File>>,
    /// Active byte-range locks
    active_db_locks: HashMap<String, Box<dyn std::any::Any + Send + Sync>>,
}

// SQLite main database lock byte offsets (same as lib.rs)
const PENDING_BYTE: u64 = 0x40000000;
const RESERVED_BYTE: u64 = PENDING_BYTE + 1;
const SHARED_FIRST: u64 = PENDING_BYTE + 2;
const SHARED_SIZE: u64 = 510;

impl TieredHandle {
    /// Create a tiered handle backed by S3 + local page cache.
    pub(crate) fn new_tiered(
        s3: Arc<S3Client>,
        cache: Arc<DiskCache>,
        shared_manifest: Arc<RwLock<Manifest>>,
        shared_dirty_groups: Arc<Mutex<HashSet<u64>>>,
        db_path: PathBuf,
        pages_per_group: u32,
        compression_level: i32,
        read_only: bool,
        sync_mode: SyncMode,
        prefetch_hops: Vec<f32>,
        prefetch_search: Vec<f32>,
        prefetch_lookup: Vec<f32>,
        prefetch_pool: Option<Arc<PrefetchPool>>,
        gc_enabled: bool,
        eager_index_load: bool,
        #[cfg(feature = "zstd")] dictionary: Option<&[u8]>,
        encryption_key: Option<[u8; 32]>,
        prediction: Option<prediction::SharedPrediction>,
        access_history: Option<prediction::SharedAccessHistory>,
        query_plan_prefetch: bool,
        max_cache_bytes: Option<u64>,
    ) -> Self {
        // Snapshot the shared manifest for initialization (avoid holding lock during S3 I/O)
        let manifest = shared_manifest.read().clone();
        let page_size = manifest.page_size;

        // Eagerly fetch page group 0 (contains schema + root page, hit on every query)
        if manifest.page_count > 0 && cache.group_state(0) != GroupState::Present {
            if let Some(key) = manifest.page_group_keys.first() {
                if !key.is_empty() {
                    if cache.try_claim_group(0) {
                        if let Ok(Some(pg_data)) = s3.get_page_group(key) {
                            let ft = manifest.frame_tables.first().map(|v| v.as_slice());
                            let gp0 = manifest.group_page_nums(0);
                            let _ = Self::decode_and_cache_group_static(
                                &cache,
                                &pg_data,
                                &gp0,
                                0, // gid
                                manifest.page_size,
                                manifest.page_count,
                                ft,
                                #[cfg(feature = "zstd")]
                                dictionary,
                                encryption_key.as_ref(),
                            );
                            cache.mark_group_present(0);
                            cache.touch_group(0);
                        }
                    }
                }
            }
        }

        // Eagerly fetch interior chunks (B-tree interior pages split across S3 objects).
        // Fetched in parallel — after this, every B-tree traversal is a cache hit.
        // Skip if interior pages are already cached (survived clear_cache pinning).
        let interior_already_cached = !cache.interior_pages.lock().is_empty();
        if interior_already_cached {
            eprintln!(
                "[tiered] interior pages already cached ({} pages), skipping chunk fetch",
                cache.interior_pages.lock().len(),
            );
        } else if !manifest.interior_chunk_keys.is_empty() {
            // Parallel fetch all interior chunks
            let chunk_keys: Vec<String> = manifest.interior_chunk_keys.values().cloned().collect();
            eprintln!("[tiered] fetching {} interior chunks in parallel...", chunk_keys.len());
            match s3.get_page_groups_by_key(&chunk_keys) {
                Ok(results) => {
                    #[cfg(feature = "zstd")]
                    let ib_decoder = dictionary.map(zstd::dict::DecoderDictionary::copy);
                    let mut total_pages = 0usize;
                    let mut total_bytes = 0usize;
                    for (key, data) in &results {
                        total_bytes += data.len();
                        match decode_interior_bundle(
                            data,
                            #[cfg(feature = "zstd")]
                            ib_decoder.as_ref(),
                            encryption_key.as_ref(),
                        ) {
                            Ok(pages) => {
                                total_pages += pages.len();
                                for (pnum, pdata) in &pages {
                                    let _ = cache.write_page(*pnum, pdata);
                                    let loc = manifest.page_location(*pnum)
                                        .expect("interior page must have group assignment");
                                    cache.mark_interior_group(loc.group_id, *pnum, loc.index);
                                }
                            }
                            Err(e) => eprintln!("[tiered] interior chunk {} decode failed: {}", key, e),
                        }
                    }
                    eprintln!(
                        "[tiered] interior chunks loaded: {} pages from {} chunks ({:.1}KB total)",
                        total_pages, results.len(), total_bytes as f64 / 1024.0,
                    );
                }
                Err(e) => eprintln!("[tiered] interior chunk fetch failed: {}", e),
            }
        }

        // Index leaf bundles: lazy-aggressive prefetch.
        // Instead of blocking connection open on potentially large index bundles
        // (107MB at 750k rows), spawn a background thread. The first query serves
        // index pages from data page groups via inline range GET (~100KB), while
        // the background thread populates the full index cache.
        let index_already_cached = !cache.index_pages.lock().is_empty();
        if eager_index_load && !manifest.index_chunk_keys.is_empty() && !index_already_cached {
            let cache_bg = Arc::clone(&cache);
            let s3_bg = Arc::clone(&s3);
            let chunk_keys: Vec<String> = manifest.index_chunk_keys.values().cloned().collect();
            #[cfg(feature = "zstd")]
            let dict_bg = dictionary.map(|d| d.to_vec());
            let n_chunks = chunk_keys.len();
            let encryption_key_bg = encryption_key;
            eprintln!("[tiered] scheduling {} index leaf chunks for background fetch...", n_chunks);
            std::thread::spawn(move || {
                match s3_bg.get_page_groups_by_key(&chunk_keys) {
                    Ok(results) => {
                        #[cfg(feature = "zstd")]
                        let ix_decoder = dict_bg.as_deref().map(zstd::dict::DecoderDictionary::copy);
                        let mut total_pages = 0usize;
                        let mut total_bytes = 0usize;
                        for (key, data) in &results {
                            total_bytes += data.len();
                            match decode_interior_bundle(
                                data,
                                #[cfg(feature = "zstd")]
                                ix_decoder.as_ref(),
                                encryption_key_bg.as_ref(),
                            ) {
                                Ok(pages) => {
                                    total_pages += pages.len();
                                    for (pnum, pdata) in &pages {
                                        let _ = cache_bg.write_page(*pnum, pdata);
                                        cache_bg.index_pages.lock().insert(*pnum);
                                    }
                                }
                                Err(e) => eprintln!("[tiered] index chunk {} decode failed: {}", key, e),
                            }
                        }
                        eprintln!(
                            "[tiered] index leaf chunks loaded (background): {} pages from {} chunks ({:.1}KB total)",
                            total_pages, results.len(), total_bytes as f64 / 1024.0,
                        );
                    }
                    Err(e) => eprintln!("[tiered] index chunk fetch failed: {}", e),
                }
            });
        } else if index_already_cached {
            eprintln!(
                "[tiered] index pages already cached ({} pages), skipping chunk fetch",
                cache.index_pages.lock().len(),
            );
        }

        #[cfg(feature = "zstd")]
        let (encoder_dict, decoder_dict) = match dictionary {
            Some(dict_bytes) => (
                Some(zstd::dict::EncoderDictionary::copy(dict_bytes, compression_level)),
                Some(zstd::dict::DecoderDictionary::copy(dict_bytes)),
            ),
            None => (None, None),
        };

        // Drop the snapshot; handle uses the shared Arc from now on
        drop(manifest);

        Self {
            s3: Some(s3),
            cache: Some(cache),
            manifest: shared_manifest,
            dirty_page_nums: RwLock::new(HashSet::new()),
            s3_dirty_groups: shared_dirty_groups,
            page_size: RwLock::new(page_size),
            pages_per_group,
            compression_level,
            read_only,
            sync_mode,
            tree_miss_counts: HashMap::new(),
            default_miss_count: 0,
            prefetch_hops,
            prefetch_search,
            prefetch_lookup,
            search_trees: HashSet::new(),
            prefetch_pool,
            gc_enabled,
            encryption_key,
            #[cfg(feature = "zstd")]
            encoder_dict,
            #[cfg(feature = "zstd")]
            decoder_dict,
            query_plan_prefetch,
            lock_session: prediction::LockSession::new(),
            prediction,
            access_history,
            dirty_btrees: HashSet::new(),
            cache_limit: max_cache_bytes.and_then(|n| if n == 0 { None } else { Some(n) }),
            passthrough_file: None,
            lock: RwLock::new(LockKind::None),
            db_path,
            lock_file: None,
            active_db_locks: HashMap::new(),
        }
    }

    /// Create a passthrough handle for WAL/journal files (local file I/O).
    pub(crate) fn new_passthrough(file: File, db_path: PathBuf, encryption_key: Option<[u8; 32]>) -> Self {
        Self {
            s3: None,
            cache: None,
            manifest: Arc::new(RwLock::new(Manifest::empty())),
            dirty_page_nums: RwLock::new(HashSet::new()),
            s3_dirty_groups: Arc::new(Mutex::new(HashSet::new())),
            page_size: RwLock::new(0),
            pages_per_group: DEFAULT_PAGES_PER_GROUP,
            compression_level: 0,
            read_only: false,
            sync_mode: SyncMode::Durable,
            tree_miss_counts: HashMap::new(),
            default_miss_count: 0,
            prefetch_hops: vec![0.33, 0.33],
            prefetch_search: vec![0.3, 0.3, 0.4],
            prefetch_lookup: vec![0.0, 0.0, 0.0],
            search_trees: HashSet::new(),
            prefetch_pool: None,
            gc_enabled: false,
            encryption_key,
            #[cfg(feature = "zstd")]
            encoder_dict: None,
            #[cfg(feature = "zstd")]
            decoder_dict: None,
            query_plan_prefetch: false,
            lock_session: prediction::LockSession::new(),
            prediction: None,
            access_history: None,
            dirty_btrees: HashSet::new(),
            cache_limit: None,
            passthrough_file: Some(RwLock::new(file)),
            lock: RwLock::new(LockKind::None),
            db_path,
            lock_file: None,
            active_db_locks: HashMap::new(),
        }
    }

    pub(crate) fn is_passthrough(&self) -> bool {
        self.passthrough_file.is_some()
    }

    /// Get miss count for a tree (or default if no tree name).
    fn miss_count(&self, tree_name: Option<&String>) -> u8 {
        match tree_name {
            Some(name) => *self.tree_miss_counts.get(name).unwrap_or(&0),
            None => self.default_miss_count,
        }
    }

    /// Increment miss count for a tree (or default).
    fn increment_misses(&mut self, tree_name: Option<&String>) {
        match tree_name {
            Some(name) => {
                let count = self.tree_miss_counts.entry(name.clone()).or_insert(0);
                *count = count.saturating_add(1);
            }
            None => self.default_miss_count = self.default_miss_count.saturating_add(1),
        }
    }

    /// Reset miss count for a tree (or default) on cache hit.
    fn reset_misses(&mut self, tree_name: Option<&String>) {
        match tree_name {
            Some(name) => { self.tree_miss_counts.remove(name); }
            None => self.default_miss_count = 0,
        }
    }

    pub(crate) fn s3(&self) -> &S3Client {
        self.s3.as_ref().expect("s3 client required for tiered mode")
    }

    pub(crate) fn disk_cache(&self) -> &DiskCache {
        self.cache
            .as_ref()
            .expect("disk cache required for tiered mode")
    }

    /// Copy raw page data into the output buffer, handling sub-page offsets.
    pub(crate) fn copy_raw_into_buf(
        raw: &[u8],
        buf: &mut [u8],
        offset: u64,
        page_size: u64,
    ) {
        let page_offset = (offset % page_size) as usize;
        let copy_len = buf.len().min(raw.len().saturating_sub(page_offset));
        if copy_len > 0 {
            buf[..copy_len].copy_from_slice(&raw[page_offset..page_offset + copy_len]);
        }
        if copy_len < buf.len() {
            buf[copy_len..].fill(0);
        }
    }

    /// Decode a page group and write all pages to the cache file.
    /// Static version for use in constructors and prefetch workers.
    /// Handles both seekable (multi-frame) and legacy (single-frame with 8-byte header) formats.
    pub(crate) fn decode_and_cache_group_static(
        cache: &DiskCache,
        pg_data: &[u8],
        group_page_nums: &[u64],
        gid: u64,
        page_size: u32,
        page_count: u64,
        frame_table: Option<&[FrameEntry]>,
        #[cfg(feature = "zstd")] dictionary: Option<&[u8]>,
        encryption_key: Option<&[u8; 32]>,
    ) -> io::Result<()> {
        #[cfg(feature = "zstd")]
        let decoder_dict = dictionary.map(zstd::dict::DecoderDictionary::copy);

        // Dispatch: seekable (frame_table present and non-empty) vs legacy bulk
        let (_pg_count, page_data) = match frame_table.filter(|ft| !ft.is_empty()) {
            Some(ft) => {
                let (c, _s, d) = decode_page_group_seekable_full(
                    pg_data,
                    ft,
                    page_size,
                    group_page_nums.len() as u32,
                    page_count,
                    0, // B-tree groups: size from group_page_nums
                    #[cfg(feature = "zstd")]
                    decoder_dict.as_ref(),
                    encryption_key,
                )?;
                (c, d)
            }
            None => {
                let (c, _s, d) = decode_page_group_bulk(
                    pg_data,
                    #[cfg(feature = "zstd")]
                    decoder_dict.as_ref(),
                    encryption_key,
                )?;
                (c, d)
            }
        };

        let ps = page_size as usize;
        let mut written = 0usize;
        for (i, &pnum) in group_page_nums.iter().enumerate() {
            if pnum >= page_count {
                continue;
            }
            let offset = i * ps;
            let end = offset + ps;
            if end > page_data.len() {
                eprintln!(
                    "[decode_and_cache_group_static] short data: group has {} page_nums, decoded {} bytes ({} pages), breaking at i={}",
                    group_page_nums.len(), page_data.len(), page_data.len() / ps, i,
                );
                break;
            }
            cache.write_page(pnum, &page_data[offset..end])?;
            written += 1;
        }
        if written < group_page_nums.len() {
            eprintln!(
                "[decode_and_cache_group_static] WARNING: wrote {}/{} pages (data has {} bytes for {} byte pages)",
                written, group_page_nums.len(), page_data.len(), ps,
            );
        }

        // Mark tracker sub-chunks as Data tier (manifest-aware)
        let mut tracker = cache.tracker.lock();
        for i in 0..written {
            let id = tracker.sub_chunk_id_for(gid, i as u32);
            tracker.mark_present(id, SubChunkTier::Data);
        }
        drop(tracker);

        Ok(())
    }

    /// Decode a page group and write all pages to the cache file.
    #[allow(dead_code)]
    pub(crate) fn decode_and_cache_group(
        &self,
        cache: &DiskCache,
        pg_data: &[u8],
        gid: u64,
    ) -> io::Result<()> {
        let manifest = self.manifest.read();
        let page_count = manifest.page_count;
        let group_page_nums = manifest.group_page_nums(gid).into_owned();
        drop(manifest);

        let (_pg_count, _pg_size, pages) = decode_page_group(
            pg_data,
            #[cfg(feature = "zstd")]
            self.decoder_dict.as_ref(),
            self.encryption_key.as_ref(),
        )?;

        for (i, page_data) in pages.iter().enumerate() {
            if i >= group_page_nums.len() { break; }
            let pnum = group_page_nums[i];
            if pnum >= page_count {
                continue;
            }
            cache.write_page(pnum, page_data)?;
        }
        Ok(())
    }

    /// Check if a page is a B-tree interior page and mark its group.
    /// Detect page type and set sub-chunk tier accordingly:
    /// - 0x05 (table interior) / 0x02 (index interior) → Pinned (never evicted)
    /// - 0x0A (index leaf) → Index tier (evicted after data, before pinned)
    pub(crate) fn detect_interior_page(&self, buf: &[u8], page_num: u64, cache: &DiskCache) {
        let hdr_offset = if page_num == 0 { 100 } else { 0 };
        let type_byte = buf.get(hdr_offset).copied();
        if let Some(b) = type_byte {
            if b == 0x05 || b == 0x02 {
                // New pages (not yet synced) may not have a group assignment yet
                if let Some(loc) = self.manifest.read().page_location(page_num) {
                    cache.mark_interior_group(loc.group_id, page_num, loc.index);
                }
            } else if b == 0x0A && is_valid_btree_page(buf, hdr_offset) {
                if let Some(loc) = self.manifest.read().page_location(page_num) {
                    cache.mark_index_page(page_num, loc.group_id, loc.index);
                }
            }
        }
    }

    /// Assign new pages (not in page_index) to new groups during sync.
    /// Bin-packs unassigned pages into groups of ppg, extends group_pages,
    /// and updates page_index. Called before dirty page grouping in sync().
    pub(crate) fn assign_new_pages_to_groups(manifest: &mut Manifest, unassigned: &[u64], ppg: u32) {
        if unassigned.is_empty() {
            return;
        }
        // Positional strategy: no explicit assignment needed, arithmetic handles it.
        // Just ensure page_count covers the new pages.
        if manifest.strategy == GroupingStrategy::Positional {
            if let Some(&max_page) = unassigned.iter().max() {
                if max_page >= manifest.page_count {
                    manifest.page_count = max_page + 1;
                }
            }
            return;
        }
        let mut sorted = unassigned.to_vec();
        sorted.sort_unstable();

        // Check if last existing group has room for some pages
        let last_gid = manifest.group_pages.len().saturating_sub(1);
        let last_group_room = if !manifest.group_pages.is_empty() {
            ppg as usize - manifest.group_pages[last_gid].len()
        } else {
            0
        };

        let mut idx = 0;
        // Fill remaining space in last group
        if last_group_room > 0 && !manifest.group_pages.is_empty() {
            let fill = std::cmp::min(last_group_room, sorted.len());
            for &pnum in &sorted[..fill] {
                manifest.group_pages[last_gid].push(pnum);
                manifest.page_index.insert(pnum, PageLocation {
                    group_id: last_gid as u64,
                    index: (manifest.group_pages[last_gid].len() - 1) as u32,
                });
            }
            idx = fill;
        }

        // Create new groups for remaining pages
        for chunk in sorted[idx..].chunks(ppg as usize) {
            let new_gid = manifest.group_pages.len() as u64;
            for (i, &pnum) in chunk.iter().enumerate() {
                manifest.page_index.insert(pnum, PageLocation {
                    group_id: new_gid,
                    index: i as u32,
                });
            }
            manifest.group_pages.push(chunk.to_vec());
        }

        eprintln!(
            "[sync] assigned {} new pages to groups (total groups: {})",
            unassigned.len(),
            manifest.group_pages.len(),
        );
    }

    /// Fraction-based prefetch. Strategy dispatches neighbor selection:
    /// BTreeAware: sibling groups from the same B-tree.
    /// Positional: radial fan-out (gid+1, gid-1, gid+2, gid-2, ...).
    pub(crate) fn trigger_prefetch(
        &self,
        current_gid: u64,
        manifest: &Manifest,
        cache: &Arc<DiskCache>,
        _s3: &Arc<S3Client>,
    ) {
        let pool = match &self.prefetch_pool {
            Some(pool) => pool,
            None => return,
        };

        let ps = manifest.page_size;
        let sub_ppf = manifest.sub_pages_per_frame;

        // Helper: try to claim and submit a group for prefetch.
        // Returns true if submitted, false otherwise.
        let try_submit = |gid: u64, submitted: &mut usize| -> bool {
            if cache.group_state(gid) != GroupState::None {
                return false;
            }
            if !cache.try_claim_group(gid) {
                return false;
            }
            if let Some(key) = manifest.page_group_keys.get(gid as usize) {
                if !key.is_empty() {
                    let ft = manifest.frame_tables.get(gid as usize).cloned().unwrap_or_default();
                    let gp = manifest.group_page_nums(gid).into_owned();
                    if pool.submit(gid, key.clone(), ft, ps, sub_ppf, gp) {
                        *submitted += 1;
                        return true;
                    }
                }
            }
            // Failed to submit: reset state
            cache.unclaim_group(gid);
            false
        };

        match manifest.prefetch_neighbors(current_gid) {
            PrefetchNeighbors::BTreeSiblings(siblings) => {
                let eligible: Vec<u64> = siblings.iter()
                    .copied()
                    .filter(|&gid| gid != current_gid && cache.group_state(gid) == GroupState::None)
                    .collect();
                if eligible.is_empty() {
                    return;
                }

                // Pick schedule: SEARCH trees get aggressive warmup, lookups are conservative.
                // Per-tree miss counts ensure independent tracking across trees in a query.
                let tree_name = manifest.group_to_tree_name.get(&current_gid);
                let is_search = tree_name.map(|n| self.search_trees.contains(n)).unwrap_or(false);
                let hops = if is_search { &self.prefetch_search } else { &self.prefetch_lookup };

                let miss_count = self.miss_count(tree_name);
                let hop_idx = miss_count.saturating_sub(1) as usize;
                let fraction = if hop_idx < hops.len() {
                    hops[hop_idx]
                } else {
                    1.0
                };
                if fraction <= 0.0 {
                    if std::env::var("BENCH_VERBOSE").is_ok() {
                        eprintln!(
                            "  [prefetch] gid={} misses={} SKIP ({}hop fraction=0)",
                            current_gid, miss_count,
                            if is_search { "search " } else { "" },
                        );
                    }
                    return;
                }
                let max_submit = ((eligible.len() as f32) * fraction).ceil() as usize;
                let mut submitted = 0usize;

                for &gid in &eligible {
                    if submitted >= max_submit { break; }
                    try_submit(gid, &mut submitted);
                }

                if std::env::var("BENCH_VERBOSE").is_ok() {
                    eprintln!(
                        "  [prefetch] gid={} misses={} fraction={:.0}% eligible={} submitted={} schedule={}",
                        current_gid, miss_count, fraction * 100.0,
                        eligible.len(), submitted,
                        if is_search { "search" } else { "default" },
                    );
                }
            }

            PrefetchNeighbors::RadialFanout { total_groups } => {
                if total_groups <= 1 {
                    return;
                }

                let hop_idx = (self.default_miss_count as usize).saturating_sub(1);
                let fraction = if hop_idx < self.prefetch_hops.len() {
                    self.prefetch_hops[hop_idx]
                } else {
                    1.0
                };
                let prefetch_count = ((total_groups as f32) * fraction).ceil() as u64;
                if prefetch_count == 0 {
                    return;
                }

                let mut submitted = 0usize;
                for delta in 1..=total_groups {
                    if submitted as u64 >= prefetch_count { break; }
                    // Forward
                    let fwd = current_gid + delta;
                    if fwd < total_groups {
                        try_submit(fwd, &mut submitted);
                    }
                    // Backward
                    if delta <= current_gid && (submitted as u64) < prefetch_count {
                        let bwd = current_gid - delta;
                        try_submit(bwd, &mut submitted);
                    }
                }

                if std::env::var("BENCH_VERBOSE").is_ok() {
                    eprintln!(
                        "  [prefetch] from gid={} miss={} hop_frac={:.2} submitted={}/{}",
                        current_gid, self.default_miss_count, fraction, submitted, prefetch_count,
                    );
                }
            }
        }
    }

    /// Ensure a lock file exists for byte-range locking.
    pub(crate) fn ensure_lock_file(&mut self) -> io::Result<std::sync::Arc<File>> {
        if self.lock_file.is_none() {
            let lock_path = self.db_path.with_extension("db-lock");
            let file = FsOpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&lock_path)?;
            self.lock_file = Some(std::sync::Arc::new(file));
        }
        Ok(std::sync::Arc::clone(
            self.lock_file.as_ref().unwrap(),
        ))
    }

    /// Phase Verdun: flush the current lock session.
    /// Records the B-tree pattern in the prediction table and applies write decay.
    fn flush_lock_session(&mut self) {
        let prediction = match &self.prediction {
            Some(p) => Arc::clone(p),
            None => {
                self.lock_session.flush();
                self.dirty_btrees.clear();
                return;
            }
        };

        if !self.lock_session.btrees.is_empty() {
            // Phase Verdun (b): record B-tree access frequency
            if let Some(ref ah) = self.access_history {
                ah.write().record(&self.lock_session.btrees);
            }

            let mut table = prediction.write();

            // Apply write decay for dirty B-trees (once per tree name, not per page)
            for name in &self.dirty_btrees {
                table.write_decay(name);
            }

            // Reinforcement: if a prediction fired, check if all predicted names
            // were actually touched. Uses fired_pattern (set by subphase d).
            if let Some(ref fired) = self.lock_session.fired_pattern {
                if fired.iter().all(|r| self.lock_session.btrees.contains(r.as_str())) {
                    table.reinforce(fired);
                }
            }

            // Observe the pattern from this session (needs >= 2 trees)
            if let Some(pattern) = self.lock_session.flush() {
                table.observe(&pattern);
            }
        } else {
            self.lock_session.flush();
        }

        self.dirty_btrees.clear();
    }
}

impl DatabaseHandle for TieredHandle {
    type WalIndex = FileWalIndex;

    fn size(&self) -> Result<u64, io::Error> {
        if self.is_passthrough() {
            let file = self.passthrough_file.as_ref().unwrap().read();
            return file.metadata().map(|m| m.len());
        }

        let manifest = self.manifest.read();
        if manifest.page_size > 0 && manifest.page_count > 0 {
            Ok(manifest.page_count * manifest.page_size as u64)
        } else {
            Ok(0)
        }
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> Result<(), io::Error> {
        if self.is_passthrough() {
            use std::os::unix::fs::FileExt;
            let file = self.passthrough_file.as_ref().unwrap().read();
            file.read_exact_at(buf, offset)?;
            #[cfg(feature = "encryption")]
            if let Some(ref key) = self.encryption_key {
                let decrypted = compress::decrypt_ctr(buf, offset, key)?;
                buf[..decrypted.len()].copy_from_slice(&decrypted);
            }
            return Ok(());
        }

        // Determine page number
        let page_size = {
            let ps = *self.page_size.read();
            if ps > 0 { ps as u64 } else { buf.len() as u64 }
        };
        let page_num = offset / page_size;

        // 1. Check dirty pages first (new pages may not be in manifest yet).
        // Phase Marne: data lives in disk cache, not in memory.
        if self.dirty_page_nums.read().contains(&page_num) {
            if let Some(cache) = &self.cache {
                cache.read_page(page_num, buf)?;
                return Ok(());
            }
        }

        // 2. Bounds check - page beyond manifest is zero-filled
        let manifest_page_count = self.manifest.read().page_count;
        if page_num >= manifest_page_count {
            buf.fill(0);
            return Ok(());
        }

        // 3. Look up page location (Phase Midway). Safe to .expect() here because:
        //    - New pages (not in manifest) are always in dirty_page_nums (returned above)
        //    - Pages beyond page_count are zero-filled (returned above)
        //    - sync() assigns new pages to groups before clearing dirty_page_nums
        let manifest_ref = self.manifest.read();
        let loc = manifest_ref.page_location(page_num)
            .expect("page within manifest bounds must have group assignment");

        // 3b. Phase Verdun-i: track B-tree name touch for predictive prefetch
        let mut predicted_groups: Vec<u64> = Vec::new();
        if self.prediction.is_some() && self.lock_session.active {
            if let Some(tree_name) = manifest_ref.page_to_tree_name.get(&page_num) {
                let is_new = self.lock_session.touch(tree_name);
                // Fire prediction on 2nd+ B-tree touch (new name, not re-touch)
                if is_new && !self.lock_session.prediction_fired && self.lock_session.tree_count() >= 2 {
                    if let Some(ref pred) = self.prediction {
                        let table = pred.read();
                        if let Some(extras) = table.predict(&self.lock_session.btrees) {
                            // Collect group IDs for predicted B-tree names
                            for predicted_name in &extras {
                                if let Some(group_ids) = manifest_ref.tree_name_to_groups.get(predicted_name) {
                                    predicted_groups.extend_from_slice(group_ids);
                                }
                            }
                            // Record the full predicted pattern for reinforcement on flush
                            let mut full_pattern: std::collections::BTreeSet<String> =
                                self.lock_session.btrees.iter().cloned().collect();
                            for r in &extras {
                                full_pattern.insert(r.clone());
                            }
                            self.lock_session.fired_pattern = Some(full_pattern);
                            self.lock_session.prediction_fired = true;
                        }
                    }
                }
            }
        }

        let gid = loc.group_id;
        let current_tree_name = manifest_ref.group_to_tree_name.get(&gid).cloned();
        drop(manifest_ref);
        let page_in_group_idx = loc.index as usize;

        // 3c. Phase Verdun (d): submit predicted groups to prefetch pool
        if !predicted_groups.is_empty() {
            if let Some(pool) = &self.prefetch_pool {
                let manifest_snap = self.manifest.read();
                let sub_ppf = manifest_snap.sub_pages_per_frame;
                for &pred_gid in &predicted_groups {
                    let cache_ref = self.cache.as_ref().expect("disk cache required");
                    if cache_ref.group_state(pred_gid) == GroupState::None
                        && cache_ref.try_claim_group(pred_gid)
                    {
                        if let Some(key) = manifest_snap.page_group_keys.get(pred_gid as usize) {
                            if !key.is_empty() {
                                let ft = manifest_snap.frame_tables
                                    .get(pred_gid as usize)
                                    .cloned()
                                    .unwrap_or_default();
                                let gp = manifest_snap.group_page_nums(pred_gid).into_owned();
                                if !pool.submit(pred_gid, key.clone(), ft, manifest_snap.page_size, sub_ppf, gp) {
                                    cache_ref.unclaim_group(pred_gid);
                                }
                            } else {
                                cache_ref.unclaim_group(pred_gid);
                            }
                        } else {
                            cache_ref.unclaim_group(pred_gid);
                        }
                    }
                }
            }
        }

        // 3d. Phase Stalingrad: between-query eviction trigger.
        //
        // Check if a query completed since our last read (SQLITE_TRACE_PROFILE
        // fires on statement completion and sets the end-query signal). If so,
        // this is the boundary between two queries: the right time to evict
        // cached data down to the budget.
        //
        // Order matters: evict BEFORE draining settings and plan queue, so the
        // new query starts with a trimmed cache.
        if query_plan::check_and_clear_end_query() {
            if let Some(limit) = self.cache_limit {
                if let Some(cache) = &self.cache {
                    // Build skip set: dirty pages + pending flush + fetching groups
                    let mut skip_groups: HashSet<u64> = HashSet::new();
                    // Dirty page groups
                    {
                        let dirty = self.dirty_page_nums.read();
                        let manifest = self.manifest.read();
                        for &pn in dirty.iter() {
                            if let Some(loc) = manifest.page_location(pn) {
                                skip_groups.insert(loc.group_id);
                            }
                        }
                    }
                    // Pending S3 flush groups
                    if let Ok(pending) = self.s3_dirty_groups.lock() {
                        skip_groups.extend(pending.iter());
                    }
                    // Groups currently being fetched by prefetch workers
                    {
                        let states = cache.group_states.lock();
                        for (i, s) in states.iter().enumerate() {
                            if s.load(Ordering::Acquire) == GroupState::Fetching as u8 {
                                skip_groups.insert(i as u64);
                            }
                        }
                    }
                    cache.evict_to_budget(limit, &skip_groups);
                }
            }
        }

        // 3e. Drain per-connection settings (turbolite_config_set SQL function).
        // Same global-queue pattern as plan drain. Users can tune prefetch schedules
        // before each query without reopening the connection.
        //
        // Keys:
        //   prefetch        - convenience, sets both search and lookup
        //   prefetch_search - SEARCH queries (aggressive warmup)
        //   prefetch_lookup - index lookups / point queries (conservative)
        //   prefetch_reset  - reset both to defaults
        //   plan_aware      - enable/disable plan-aware prefetch
        {
            let updates = settings::drain_settings();
            for update in updates {
                match update.key.as_str() {
                    "prefetch" => {
                        if let Some(hops) = settings::parse_hops(&update.value) {
                            self.prefetch_search = hops.clone();
                            self.prefetch_lookup = hops;
                        }
                    }
                    "prefetch_search" => {
                        if let Some(hops) = settings::parse_hops(&update.value) {
                            self.prefetch_search = hops;
                        }
                    }
                    "prefetch_lookup" => {
                        if let Some(hops) = settings::parse_hops(&update.value) {
                            self.prefetch_lookup = hops;
                        }
                    }
                    "prefetch_reset" => {
                        self.prefetch_search = vec![0.3, 0.3, 0.4];
                        self.prefetch_lookup = vec![0.0, 0.0, 0.0];
                    }
                    "plan_aware" => {
                        self.query_plan_prefetch = matches!(update.value.as_str(), "true" | "1");
                    }
                    "cache_limit" => {
                        if let Some(bytes) = settings::parse_byte_size(&update.value) {
                            self.cache_limit = if bytes == 0 { None } else { Some(bytes) };
                        }
                    }
                    _ => {}
                }
            }
        }

        // 3f. Phase Marne: query-plan-aware prefetch.
        //
        // Drain the global plan queue and submit ALL planned groups to the
        // prefetch pool in EQP order: all groups for tree 1, then all groups
        // for tree 2, etc. The pool's FIFO channel preserves this order, so
        // worker threads fetch what SQLite needs first.
        //
        // Runs on every read (before cache hit check) so the first read after
        // step() triggers submission. First reads are always interior page
        // hits (pinned, ~microseconds), giving the pool a head start before
        // SQLite needs leaf pages.
        //
        // The drain is a no-op (empty Vec, one mutex check) when no plan is
        // queued, which is the common case for cached reads within a query.
        if self.query_plan_prefetch {
            let planned = query_plan::drain_planned_accesses();
            if !planned.is_empty() {
                // Collect SEARCH tree names for per-query hop schedule selection.
                // Clear previous query's search trees and populate from new plan.
                self.search_trees.clear();
                for access in &planned {
                    if access.access_type == query_plan::AccessType::Search {
                        self.search_trees.insert(access.tree_name.clone());
                    }
                }
                if std::env::var("BENCH_VERBOSE").is_ok() {
                    let manifest_snap = self.manifest.read();
                    let tree_names: Vec<&String> = manifest_snap.tree_name_to_groups.keys().collect();
                    let planned_names: Vec<&str> = planned.iter().map(|a| a.tree_name.as_str()).collect();
                    let search_names: Vec<&String> = self.search_trees.iter().collect();
                    eprintln!(
                        "  [plan-drain] planned={:?} search_trees={:?} manifest_trees={:?}",
                        planned_names, search_names, tree_names,
                    );
                    drop(manifest_snap);
                }
                if let Some(pool) = &self.prefetch_pool {
                    let manifest_snap = self.manifest.read();
                    let sub_ppf = manifest_snap.sub_pages_per_frame;
                    let cache_ref = self.cache.as_ref().expect("disk cache required");
                    for access in &planned {
                        // Only bulk prefetch for SCAN. SEARCH uses prefetch_search
                        // via trigger_prefetch (per-tree miss counters).
                        if access.access_type != query_plan::AccessType::Scan {
                            continue;
                        }
                        if let Some(group_ids) = manifest_snap.tree_name_to_groups.get(&access.tree_name) {
                            for &plan_gid in group_ids {
                                if cache_ref.group_state(plan_gid) == GroupState::None
                                    && cache_ref.try_claim_group(plan_gid)
                                {
                                    if let Some(key) = manifest_snap.page_group_keys.get(plan_gid as usize) {
                                        if !key.is_empty() {
                                            let ft = manifest_snap.frame_tables.get(plan_gid as usize).cloned().unwrap_or_default();
                                            let gp = manifest_snap.group_page_nums(plan_gid).into_owned();
                                            if !pool.submit(plan_gid, key.clone(), ft, manifest_snap.page_size, sub_ppf, gp) {
                                                cache_ref.unclaim_group(plan_gid);
                                            }
                                        } else {
                                            cache_ref.unclaim_group(plan_gid);
                                        }
                                    } else {
                                        cache_ref.unclaim_group(plan_gid);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // 4. Check page bitmap (cache hit = direct pread, no decompression)
        let cache_arc = Arc::clone(self.cache.as_ref().expect("disk cache required"));
        let cache = cache_arc.as_ref();
        if cache.is_present(page_num) {
            self.reset_misses(current_tree_name.as_ref());
            cache.read_page(page_num, buf)?;
            self.detect_interior_page(buf, page_num, cache);
            cache.touch_group(gid);
            return Ok(());
        }

        // 5. Cache miss - fetch from S3.
        let s3_arc = Arc::clone(self.s3.as_ref().expect("s3 client required"));
        let manifest = self.manifest.read().clone();
        let miss_start = Instant::now();

        // 5a. Re-check cache: a sibling prefetch may have completed since step 4.
        if cache.is_present(page_num) {
            cache.read_page(page_num, buf)?;
            self.detect_interior_page(buf, page_num, cache);
            cache.touch_group(gid);
            self.reset_misses(current_tree_name.as_ref());
            return Ok(());
        }

        // Check if seekable format is available for sub-chunk range GETs.
        let frame_table = manifest.frame_tables.get(gid as usize);
        let has_frames = manifest.sub_pages_per_frame > 0
            && frame_table.map(|ft| !ft.is_empty()).unwrap_or(false);

        if has_frames {
            // ── SEEKABLE PATH: range GET just the sub-chunk containing the needed page ──
            let sub_ppg = manifest.sub_pages_per_frame;
            let page_in_group = page_in_group_idx;
            let frame_idx = page_in_group / sub_ppg as usize;
            let ft = frame_table.unwrap();

            if frame_idx < ft.len() {
                let key = manifest.page_group_keys.get(gid as usize)
                    .expect("gid must be valid index into page_group_keys");
                let entry = &ft[frame_idx];

                // Submit the CURRENT group to prefetch pool (full group fetch in background).
                self.increment_misses(current_tree_name.as_ref());
                if let Some(pool) = &self.prefetch_pool {
                    if cache.group_state(gid) == GroupState::None
                        && cache.try_claim_group(gid)
                    {
                        if let Some(key) = manifest.page_group_keys.get(gid as usize) {
                            if !key.is_empty() {
                                let gp_owned = manifest.group_page_nums(gid).into_owned();
                                if !pool.submit(gid, key.clone(), ft.to_vec(), manifest.page_size, sub_ppg, gp_owned) {
                                    cache.unclaim_group(gid);
                                }
                            } else {
                                cache.unclaim_group(gid);
                            }
                        } else {
                            cache.unclaim_group(gid);
                        }
                    }
                }
                self.trigger_prefetch(gid, &manifest, &cache_arc, &s3_arc);

                let s3_start = Instant::now();
                match s3_arc.range_get(key, entry.offset, entry.len) {
                    Ok(Some(compressed_frame)) => {
                        let s3_ms = s3_start.elapsed().as_millis();
                        let decode_start = Instant::now();
                        let decompressed = decode_seekable_subchunk(
                            &compressed_frame,
                            #[cfg(feature = "zstd")]
                            self.decoder_dict.as_ref(),
                            self.encryption_key.as_ref(),
                        )?;
                        let decode_ms = decode_start.elapsed().as_millis();

                        let ps = manifest.page_size as usize;

                        // Extract needed page directly into buf (no cache round-trip)
                        let page_offset_in_frame = page_in_group % sub_ppg as usize;
                        let src_start = page_offset_in_frame * ps;
                        let src_end = src_start + buf.len();
                        if src_end <= decompressed.len() {
                            buf.copy_from_slice(&decompressed[src_start..src_end]);
                        } else {
                            buf.fill(0);
                        }

                        // Write sub-chunk pages to cache
                        let frame_start_idx = frame_idx * sub_ppg as usize;
                        let gp = manifest.group_page_nums(gid);
                        {
                            let frame_end_idx = std::cmp::min(frame_start_idx + sub_ppg as usize, gp.len());
                            let frame_page_nums = &gp[frame_start_idx..frame_end_idx];
                            let pages_in_frame = frame_page_nums.len() as u64;
                            if pages_in_frame > 0 {
                                let data_len = pages_in_frame as usize * ps;
                                if data_len <= decompressed.len() {
                                    cache.write_pages_scattered(
                                        frame_page_nums,
                                        &decompressed[..data_len],
                                        gid,
                                        frame_start_idx as u32,
                                    )?;
                                }
                            }
                            // Scan for page types in the sub-chunk
                            for (i, &pnum) in frame_page_nums.iter().enumerate() {
                                let hdr_off = if pnum == 0 { 100 } else { 0 };
                                let page_start = i * ps;
                                let idx_in_group = (frame_start_idx + i) as u32;
                                let type_byte = decompressed.get(page_start + hdr_off).copied();
                                if let Some(b) = type_byte {
                                    if b == 0x05 || b == 0x02 {
                                        cache.mark_interior_group(gid, pnum, idx_in_group);
                                    } else if b == 0x0A {
                                        if let Some(page_slice) = decompressed.get(page_start..page_start + ps) {
                                            if is_valid_btree_page(page_slice, hdr_off) {
                                                cache.mark_index_page(pnum, gid, idx_in_group);
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        self.detect_interior_page(buf, page_num, cache);
                        cache.touch_group(gid);
                        self.reset_misses(current_tree_name.as_ref());

                        if std::env::var("BENCH_VERBOSE").is_ok() {
                            eprintln!(
                                "  [range-get] page={} gid={} frame={}/{} s3={}ms decode={}ms total={}ms ({:.1}KB)",
                                page_num, gid, frame_idx, ft.len(), s3_ms, decode_ms,
                                miss_start.elapsed().as_millis(),
                                compressed_frame.len() as f64 / 1024.0,
                            );
                        }
                        return Ok(());
                    }
                    Ok(None) | Err(_) => {
                        // Fall through to legacy path
                        if std::env::var("BENCH_VERBOSE").is_ok() {
                            eprintln!(
                                "  [range-get] page={} gid={} frame={} failed, falling through to legacy path",
                                page_num, gid, frame_idx,
                            );
                        }
                    }
                }
            }
            // Fall through to legacy path if sub-chunk fetch failed
        }

        // ── LEGACY PATH: full group download ──
        let state = cache.group_state(gid);
        if state == GroupState::Fetching {
            let wait_start = Instant::now();
            cache.wait_for_group(gid);
            if std::env::var("BENCH_VERBOSE").is_ok() {
                eprintln!(
                    "  [inline] page={} gid={} WAITED for prefetch worker {}ms",
                    page_num, gid, wait_start.elapsed().as_millis(),
                );
            }
        } else if state != GroupState::Present {
            if cache.try_claim_group(gid) {
                self.increment_misses(current_tree_name.as_ref());
                self.trigger_prefetch(gid, &manifest, &cache_arc, &s3_arc);
                if let Some(key) = manifest.page_group_keys.get(gid as usize) {
                    if !key.is_empty() {
                        let s3_start = Instant::now();
                        match s3_arc.get_page_group(key) {
                            Ok(Some(pg_data)) => {
                                let s3_ms = s3_start.elapsed().as_millis();
                                let decode_start = Instant::now();
                                // Use seekable full decode if frame table available, else legacy
                                let gp = manifest.group_page_nums(gid);
                                let decode_result = if has_frames {
                                    let ft = frame_table.unwrap();
                                    decode_page_group_seekable_full(
                                        &pg_data,
                                        ft,
                                        manifest.page_size,
                                        gp.len() as u32,
                                        manifest.page_count,
                                        0, // not used for scattered writes
                                        #[cfg(feature = "zstd")]
                                        self.decoder_dict.as_ref(),
                                        self.encryption_key.as_ref(),
                                    )
                                } else {
                                    decode_page_group_bulk(
                                        &pg_data,
                                        #[cfg(feature = "zstd")]
                                        self.decoder_dict.as_ref(),
                                        self.encryption_key.as_ref(),
                                    )
                                };
                                let (_pg_count, _pg_size, page_data) = decode_result?;
                                let decode_ms = decode_start.elapsed().as_millis();
                                let ps = manifest.page_size as usize;
                                let actual_pages = gp.len();
                                let write_start = Instant::now();
                                if actual_pages > 0 {
                                    let data_len = actual_pages * ps;
                                    if data_len <= page_data.len() {
                                        cache.write_pages_scattered(
                                            &gp,
                                            &page_data[..data_len],
                                            gid,
                                            0,
                                        )?;
                                    }
                                }
                                let write_ms = write_start.elapsed().as_millis();
                                // Scan for interior/index pages
                                for (i, &pnum) in gp.iter().enumerate() {
                                    let hdr_off = if pnum == 0 { 100 } else { 0 };
                                    let type_byte = page_data.get(i * ps + hdr_off).copied();
                                    if let Some(b) = type_byte {
                                        if b == 0x05 || b == 0x02 {
                                            cache.mark_interior_group(gid, pnum, i as u32);
                                        }
                                    }
                                }
                                cache.mark_group_present(gid);
                                cache.touch_group(gid);
                                if std::env::var("BENCH_VERBOSE").is_ok() {
                                    eprintln!(
                                        "  [inline] page={} gid={} s3={}ms decode={}ms write={}ms total={}ms ({:.1}KB)",
                                        page_num, gid, s3_ms, decode_ms, write_ms,
                                        miss_start.elapsed().as_millis(),
                                        pg_data.len() as f64 / 1024.0,
                                    );
                                }
                            }
                            Ok(None) => {
                                let states = cache.group_states.lock();
                                if let Some(s) = states.get(gid as usize) {
                                    s.store(GroupState::None as u8, Ordering::Release);
                                }
                                cache.group_condvar.notify_all();
                            }
                            Err(e) => {
                                let states = cache.group_states.lock();
                                if let Some(s) = states.get(gid as usize) {
                                    s.store(GroupState::None as u8, Ordering::Release);
                                }
                                cache.group_condvar.notify_all();
                                return Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    format!("S3 GET failed for gid={}: {}", gid, e),
                                ));
                            }
                        }
                    }
                }
            } else {
                let wait_start = Instant::now();
                cache.wait_for_group(gid);
                if std::env::var("BENCH_VERBOSE").is_ok() {
                    eprintln!(
                        "  [inline] page={} gid={} WAITED (race) {}ms",
                        page_num, gid, wait_start.elapsed().as_millis(),
                    );
                }
            }
        }

        // Read the page from cache (should be present now after legacy download).
        if cache.is_present(page_num) {
            self.reset_misses(current_tree_name.as_ref());
            cache.read_page(page_num, buf)?;
            self.detect_interior_page(buf, page_num, cache);
            cache.touch_group(gid);
            return Ok(());
        }
        buf.fill(0);
        Ok(())
    }

    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> Result<(), io::Error> {
        if self.is_passthrough() {
            use std::os::unix::fs::FileExt;
            let file = self.passthrough_file.as_ref().unwrap().read();
            #[cfg(feature = "encryption")]
            if let Some(ref key) = self.encryption_key {
                let encrypted = compress::encrypt_ctr(buf, offset, key)?;
                return file.write_all_at(&encrypted, offset);
            }
            return file.write_all_at(buf, offset);
        }

        if self.read_only {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "TieredHandle is read-only",
            ));
        }

        // Set page size from first write
        {
            let mut ps = self.page_size.write();
            if *ps == 0 {
                *ps = buf.len() as u32;
                // Also update DiskCache's page_size so cache offsets are correct
                if let Some(cache) = &self.cache {
                    cache.set_page_size(buf.len() as u32);
                }
            }
        }

        let page_size = *self.page_size.read() as u64;
        let page_num = offset / page_size;

        // Phase Verdun-i: track dirty B-tree name for write decay (once per tree per session)
        if self.prediction.is_some() && self.lock_session.active {
            if let Some(tree_name) = self.manifest.read().page_to_tree_name.get(&page_num) {
                self.dirty_btrees.insert(tree_name.clone());
            }
        }

        // Write to local cache and track as dirty (Phase Marne: data only in cache, not in memory)
        if let Some(cache) = &self.cache {
            cache.write_page(page_num, buf)?;
        }
        self.dirty_page_nums.write().insert(page_num);

        // Update manifest page_count if this page extends the database
        {
            let mut manifest = self.manifest.write();
            let new_count = page_num + 1;
            if new_count > manifest.page_count {
                manifest.page_count = new_count;
            }
            if manifest.page_size == 0 {
                manifest.page_size = buf.len() as u32;
            }
            if manifest.pages_per_group == 0 {
                manifest.pages_per_group = self.pages_per_group;
            }

            // Ensure group states capacity for new groups
            if let Some(cache) = &self.cache {
                let total_groups = manifest.total_groups() as usize;
                cache.ensure_group_capacity(total_groups);
            }
        }

        Ok(())
    }

    fn sync(&mut self, _data_only: bool) -> Result<(), io::Error> {
        if self.is_passthrough() {
            return self
                .passthrough_file
                .as_ref()
                .unwrap()
                .write()
                .sync_all();
        }

        if self.read_only {
            return Ok(());
        }

        // Phase Marne: snapshot is just page numbers, not page data.
        let dirty_snapshot: HashSet<u64> = {
            let dirty = self.dirty_page_nums.read();
            let has_pending_groups = !self.s3_dirty_groups.lock().unwrap().is_empty();
            if dirty.is_empty() && !has_pending_groups {
                return Ok(());
            }
            dirty.clone()
        };

        let ppg = self.pages_per_group;

        // Local-checkpoint-only: record dirty group IDs, scan interior pages,
        // free in-memory dirty map, but skip S3 upload entirely.
        // Pages are already in disk cache from write_all_at().
        if self.sync_mode == SyncMode::LocalThenFlush || LOCAL_CHECKPOINT_ONLY.load(Ordering::Acquire) {
            let cache = self.disk_cache();
            let mut manifest = self.manifest.write();
            let mut pending = self.s3_dirty_groups.lock().unwrap();
            let mut interior_found = 0usize;
            // Assign new pages (not in page_index) to new groups before recording dirty groups
            let unassigned: Vec<u64> = dirty_snapshot.iter()
                .filter(|&&pn| manifest.page_location(pn).is_none())
                .copied()
                .collect();
            if !unassigned.is_empty() {
                Self::assign_new_pages_to_groups(&mut manifest, &unassigned, ppg);
            }
            let page_size = *self.page_size.read() as usize;
            let mut type_buf = vec![0u8; page_size];
            for &page_num in &dirty_snapshot {
                let loc = manifest.page_location(page_num)
                    .expect("page must have group assignment after new-page assignment");
                pending.insert(loc.group_id);
                // Track interior pages so final sync builds correct interior chunks.
                // Phase Marne: read page type byte from cache instead of in-memory buffer.
                match cache.read_page(page_num, &mut type_buf) {
                    Ok(()) => {
                        let type_byte = if page_num == 0 { type_buf.get(100) } else { type_buf.get(0) };
                        if let Some(&b) = type_byte {
                            if b == 0x05 || b == 0x02 {
                                cache.mark_interior_group(loc.group_id, page_num, loc.index);
                                interior_found += 1;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("[sync] ERROR: cache.read_page({}) failed during local checkpoint interior scan: {}", page_num, e);
                    }
                }
            }
            drop(manifest);
            let n = dirty_snapshot.len();
            let total_interior = cache.interior_pages.lock().len();
            // Free dirty page tracking
            self.dirty_page_nums.write().clear();
            eprintln!("[sync] local-only checkpoint: {} pages, {} interior this batch, {} interior total, {} groups pending S3", n, interior_found, total_interior, pending.len());
            return Ok(());
        }

        let s3 = self.s3();
        let cache = self.disk_cache();

        // Assign new pages (not in page_index) to new groups before grouping
        {
            let mut manifest = self.manifest.write();
            let unassigned: Vec<u64> = dirty_snapshot.iter()
                .filter(|&&pn| manifest.page_location(pn).is_none())
                .copied()
                .collect();
            if !unassigned.is_empty() {
                Self::assign_new_pages_to_groups(&mut manifest, &unassigned, ppg);
            }
        }

        let manifest_snap = self.manifest.read().clone();
        let page_count = manifest_snap.page_count;

        // Group dirty pages by page group
        let mut groups_dirty: HashMap<u64, Vec<u64>> = HashMap::new();
        for &page_num in &dirty_snapshot {
            let gid = manifest_snap.page_location(page_num)
                .expect("page must have group assignment after new-page assignment")
                .group_id;
            groups_dirty.entry(gid).or_default().push(page_num);
        }

        // Merge in page groups from previous local-only checkpoints
        {
            let mut pending = self.s3_dirty_groups.lock().unwrap();
            for gid in pending.drain() {
                groups_dirty.entry(gid).or_default();
            }
        }

        let next_version = self.manifest.read().version + 1;
        let mut uploads: Vec<(String, Vec<u8>)> = Vec::new();
        let mut new_keys = self.manifest.read().page_group_keys.clone();
        // Track old keys being replaced (for post-checkpoint GC)
        let mut replaced_keys: Vec<String> = Vec::new();

        let page_size = *self.page_size.read();

        // Carry forward seekable encoding from the manifest.
        // If the manifest was imported with seekable format, sync re-encodes dirty
        // groups as seekable and carries forward frame_tables for untouched groups.
        let old_sub_ppf = manifest_snap.sub_pages_per_frame;
        let use_seekable = old_sub_ppf > 0;
        let mut new_frame_tables: Vec<Vec<FrameEntry>> = manifest_snap.frame_tables.clone();

        // Upload ALL dirty page groups. Interior/index pages are also stored in
        // bundles for eager loading, but every page group must have a valid S3 key
        // so cold readers can fetch pages before background bundle loading completes.
        // (Skipping groups with only interior/index pages caused index corruption
        // with small pages_per_group values like ppg=8.)
        let groups_needing_upload: Vec<u64> = groups_dirty.keys().copied().collect();

        // For each dirty page group that needs uploading: read all raw pages, encode as whole-group compressed
        for &gid in &groups_needing_upload {
            let mut need_s3_merge = false;

            // Get explicit page list from B-tree-aware manifest (Phase Midway)
            let pages_in_group = manifest_snap.group_page_nums(gid);
            let group_size = pages_in_group.len();

            let mut pages: Vec<Option<Vec<u8>>> = vec![None; group_size];

            // First, try to read all pages from local cache (raw, uncompressed)
            for (i, &pnum) in pages_in_group.iter().enumerate() {
                if pnum >= page_count {
                    break;
                }
                if cache.is_present(pnum) {
                    let mut page_buf = vec![0u8; page_size as usize];
                    if cache.read_page(pnum, &mut page_buf).is_ok() {
                        pages[i] = Some(page_buf);
                    }
                } else {
                    need_s3_merge = true;
                }
            }

            // If some pages aren't in cache, fetch existing group from S3 and merge
            if need_s3_merge {
                if let Some(existing_key) = new_keys.get(gid as usize) {
                    if !existing_key.is_empty() {
                        if let Ok(Some(pg_data)) = s3.get_page_group(existing_key) {
                            // Use seekable decode if the existing group has frame tables
                            let existing_ft = manifest_snap.frame_tables.get(gid as usize);
                            let has_ft = use_seekable
                                && existing_ft.map(|ft| !ft.is_empty()).unwrap_or(false);
                            if has_ft {
                                let ft = existing_ft.unwrap();
                                if let Ok((_pc, _ps, bulk_data)) = decode_page_group_seekable_full(
                                    &pg_data,
                                    ft,
                                    page_size,
                                    pages_in_group.len() as u32,
                                    page_count,
                                    0, // B-tree groups: not positional offset
                                    #[cfg(feature = "zstd")]
                                    self.decoder_dict.as_ref(),
                                    self.encryption_key.as_ref(),
                                ) {
                                    let ps = page_size as usize;
                                    for j in 0..pages_in_group.len() {
                                        if pages_in_group[j] >= page_count { break; }
                                        if pages[j].is_none() {
                                            let start = j * ps;
                                            let end = start + ps;
                                            if end <= bulk_data.len() {
                                                pages[j] = Some(bulk_data[start..end].to_vec());
                                            }
                                        }
                                    }
                                }
                            } else if let Ok((_pc, _ps, existing_pages)) = decode_page_group(
                                &pg_data,
                                #[cfg(feature = "zstd")]
                                self.decoder_dict.as_ref(),
                                self.encryption_key.as_ref(),
                            ) {
                                for (j, existing_page) in existing_pages.into_iter().enumerate() {
                                    if j >= pages_in_group.len() { break; }
                                    if pages_in_group[j] >= page_count { break; }
                                    if pages[j].is_none() {
                                        pages[j] = Some(existing_page);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Encode: use seekable format if the manifest has it, otherwise legacy
            let key = s3.page_group_key(gid, next_version);
            if use_seekable {
                let (encoded, ft) = encode_page_group_seekable(
                    &pages,
                    page_size,
                    old_sub_ppf,
                    self.compression_level,
                    #[cfg(feature = "zstd")]
                    self.encoder_dict.as_ref(),
                    self.encryption_key.as_ref(),
                )?;
                uploads.push((key.clone(), encoded));
                // Update frame table for this group
                while new_frame_tables.len() <= gid as usize {
                    new_frame_tables.push(Vec::new());
                }
                new_frame_tables[gid as usize] = ft;
            } else {
                let encoded = encode_page_group(
                    &pages,
                    page_size,
                    self.compression_level,
                    #[cfg(feature = "zstd")]
                    self.encoder_dict.as_ref(),
                    self.encryption_key.as_ref(),
                )?;
                uploads.push((key.clone(), encoded));
            }

            // Extend keys vector if needed
            while new_keys.len() <= gid as usize {
                new_keys.push(String::new());
            }
            // Track the old key being replaced (for GC)
            if let Some(old_key) = new_keys.get(gid as usize) {
                if !old_key.is_empty() {
                    replaced_keys.push(old_key.clone());
                }
            }
            new_keys[gid as usize] = key;
        }

        // Parallel upload all dirty page groups
        eprintln!("[sync] uploading {} dirty page groups...", uploads.len());
        s3.put_page_groups(&uploads)?;
        eprintln!("[sync] page groups uploaded");

        eprintln!("[sync] building interior chunks...");
        // Build chunked interior bundles: group interior pages by fixed page-number ranges.
        // Only re-upload chunks that contain dirty interior pages.
        let mut all_interior: HashMap<u64, Vec<u8>> = HashMap::new(); // pnum → data
        {
            // Phase Marne: collect interior pages from cache (dirty_snapshot is just page numbers)
            let mut dirty_interior_count = 0usize;
            let mut read_buf = vec![0u8; page_size as usize];
            for &pnum in &dirty_snapshot {
                match cache.read_page(pnum, &mut read_buf) {
                    Ok(()) => {
                        let type_byte = if pnum == 0 { read_buf.get(100) } else { read_buf.get(0) };
                        if let Some(&b) = type_byte {
                            if b == 0x05 || b == 0x02 {
                                if let Some(loc) = manifest_snap.page_location(pnum) {
                                    cache.mark_interior_group(loc.group_id, pnum, loc.index);
                                }
                                all_interior.insert(pnum, read_buf.clone());
                                dirty_interior_count += 1;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("[sync] ERROR: cache.read_page({}) failed during interior collection: {}", pnum, e);
                    }
                }
            }
            // Also include previously-known interior pages not in the dirty set
            let known_interior = cache.interior_pages.lock().clone();
            let mut cache_read_ok = 0usize;
            let mut cache_read_fail = 0usize;
            let mut cache_skipped_dup = 0usize;
            let mut cache_skipped_bounds = 0usize;
            for &pnum in &known_interior {
                if pnum >= page_count {
                    cache_skipped_bounds += 1;
                } else if dirty_snapshot.contains(&pnum) || all_interior.contains_key(&pnum) {
                    cache_skipped_dup += 1;
                } else {
                    let mut buf = vec![0u8; page_size as usize];
                    if cache.read_page(pnum, &mut buf).is_ok() {
                        all_interior.insert(pnum, buf);
                        cache_read_ok += 1;
                    } else {
                        cache_read_fail += 1;
                        eprintln!("[sync] WARN: cache.read_page({}) failed for known interior page", pnum);
                    }
                }
            }
            eprintln!(
                "[sync] interior collection: known_interior={}, dirty_snapshot_interior={}, cache_read_ok={}, cache_read_fail={}, skipped_dup={}, skipped_bounds={}, total={}",
                known_interior.len(), dirty_interior_count, cache_read_ok, cache_read_fail, cache_skipped_dup, cache_skipped_bounds, all_interior.len(),
            );
        }

        // Determine which chunks are dirty BEFORE moving all_interior into chunks.
        let dirty_chunk_ids: HashSet<u32> = all_interior.keys()
            .filter(|pnum| dirty_snapshot.contains(pnum))
            .map(|&pnum| (pnum / bundle_chunk_range(page_size)) as u32)
            .collect();

        // Group interior pages by chunk_id
        let mut chunks: HashMap<u32, Vec<(u64, Vec<u8>)>> = HashMap::new();
        for (pnum, data) in all_interior {
            let chunk_id = (pnum / bundle_chunk_range(page_size)) as u32;
            chunks.entry(chunk_id).or_default().push((pnum, data));
        }
        // Sort pages within each chunk by page number
        for pages in chunks.values_mut() {
            pages.sort_by_key(|(pnum, _)| *pnum);
        }

        // Build new chunk keys: dirty chunks get re-uploaded, clean chunks carry forward
        let old_chunk_keys = self.manifest.read().interior_chunk_keys.clone();
        let mut new_chunk_keys: HashMap<u32, String> = HashMap::new();
        let mut chunk_uploads: Vec<(String, Vec<u8>)> = Vec::new();

        for (&chunk_id, pages) in &chunks {
            if dirty_chunk_ids.contains(&chunk_id) || !old_chunk_keys.contains_key(&chunk_id) {
                // Dirty or new chunk — encode and upload
                let refs: Vec<(u64, &[u8])> = pages.iter().map(|(p, d)| (*p, d.as_slice())).collect();
                let encoded = encode_interior_bundle(
                    &refs,
                    page_size,
                    self.compression_level,
                    #[cfg(feature = "zstd")]
                    self.encoder_dict.as_ref(),
                    self.encryption_key.as_ref(),
                )?;
                let key = s3.interior_chunk_key(chunk_id, next_version);
                eprintln!(
                    "[sync] interior chunk {}: {} pages, {:.1}KB compressed",
                    chunk_id, pages.len(), encoded.len() as f64 / 1024.0,
                );
                chunk_uploads.push((key.clone(), encoded));
                // Track old interior chunk key being replaced (for GC)
                if let Some(old_key) = old_chunk_keys.get(&chunk_id) {
                    replaced_keys.push(old_key.clone());
                }
                new_chunk_keys.insert(chunk_id, key);
            } else {
                // Clean chunk — carry forward existing key
                new_chunk_keys.insert(chunk_id, old_chunk_keys[&chunk_id].clone());
            }
        }

        // GC orphaned interior chunks: old chunks with no current interior pages
        for (old_chunk_id, old_key) in &old_chunk_keys {
            if !chunks.contains_key(old_chunk_id) {
                // This chunk had interior pages before but has none now (e.g., after VACUUM/REINDEX)
                replaced_keys.push(old_key.clone());
                eprintln!("[sync] orphaned interior chunk {} scheduled for GC", old_chunk_id);
            }
        }

        // Parallel upload all dirty interior chunks
        if !chunk_uploads.is_empty() {
            eprintln!("[sync] uploading {} interior chunks...", chunk_uploads.len());
            s3.put_page_groups(&chunk_uploads)?;
            eprintln!("[sync] interior chunks uploaded");
        }

        // ── Index leaf bundles (same pattern as interior) ──
        eprintln!("[sync] building index leaf bundles...");
        let mut all_index_leaves: HashMap<u64, Vec<u8>> = HashMap::new();
        {
            // Phase Marne: read dirty pages from cache for index leaf classification
            let mut dirty_index_count = 0usize;
            let mut read_buf = vec![0u8; page_size as usize];
            for &pnum in &dirty_snapshot {
                match cache.read_page(pnum, &mut read_buf) {
                    Ok(()) => {
                        let hdr_off = if pnum == 0 { 100 } else { 0 };
                        let type_byte = read_buf.get(hdr_off);
                        if let Some(&b) = type_byte {
                            if b == 0x0A && is_valid_btree_page(&read_buf, hdr_off) {
                                all_index_leaves.insert(pnum, read_buf.clone());
                                dirty_index_count += 1;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("[sync] ERROR: cache.read_page({}) failed during index leaf collection: {}", pnum, e);
                    }
                }
            }
            // Also include previously-cached index leaf pages (read from cache)
            // We don't have a separate "known index pages" set like interior, so
            // we rely on the sub-chunk tracker's Index tier entries.
            let tracker = cache.tracker.lock();
            let index_sub_chunks: Vec<SubChunkId> = tracker.present.iter()
                .filter(|id| tracker.tiers.get(id).copied() == Some(SubChunkTier::Index))
                .copied()
                .collect();
            drop(tracker);

            let mut cache_read_ok = 0usize;
            for sc in &index_sub_chunks {
                let tracker = cache.tracker.lock();
                let page_range = tracker.pages_for_sub_chunk(*sc, page_count);
                drop(tracker);
                for pnum in page_range {
                    if pnum >= page_count || all_index_leaves.contains_key(&pnum) || dirty_snapshot.contains(&pnum) {
                        continue;
                    }
                    let mut buf = vec![0u8; page_size as usize];
                    if cache.read_page(pnum, &mut buf).is_ok() {
                        // Verify it's actually an index leaf page (not overflow with 0x0A first byte)
                        let hdr_off = if pnum == 0 { 100 } else { 0 };
                        let tb = buf.get(hdr_off).copied();
                        if tb == Some(0x0A) && is_valid_btree_page(&buf, hdr_off) {
                            all_index_leaves.insert(pnum, buf);
                            cache_read_ok += 1;
                        }
                    }
                }
            }
            eprintln!(
                "[sync] index leaf collection: dirty={}, cache_read_ok={}, total={}",
                dirty_index_count, cache_read_ok, all_index_leaves.len(),
            );
        }

        // Determine dirty index chunks BEFORE moving all_index_leaves into index_chunks.
        let dirty_index_chunk_ids: HashSet<u32> = all_index_leaves.keys()
            .filter(|pnum| dirty_snapshot.contains(pnum))
            .map(|&pnum| (pnum / bundle_chunk_range(page_size)) as u32)
            .collect();

        // Group index leaf pages by chunk_id
        let mut index_chunks: HashMap<u32, Vec<(u64, Vec<u8>)>> = HashMap::new();
        for (pnum, data) in all_index_leaves {
            let chunk_id = (pnum / bundle_chunk_range(page_size)) as u32;
            index_chunks.entry(chunk_id).or_default().push((pnum, data));
        }
        for pages in index_chunks.values_mut() {
            pages.sort_by_key(|(pnum, _)| *pnum);
        }

        let old_index_chunk_keys = self.manifest.read().index_chunk_keys.clone();
        let mut new_index_chunk_keys: HashMap<u32, String> = HashMap::new();
        let mut index_chunk_uploads: Vec<(String, Vec<u8>)> = Vec::new();

        for (&chunk_id, pages) in &index_chunks {
            if dirty_index_chunk_ids.contains(&chunk_id) || !old_index_chunk_keys.contains_key(&chunk_id) {
                let refs: Vec<(u64, &[u8])> = pages.iter().map(|(p, d)| (*p, d.as_slice())).collect();
                let encoded = encode_interior_bundle(
                    &refs,
                    page_size,
                    self.compression_level,
                    #[cfg(feature = "zstd")]
                    self.encoder_dict.as_ref(),
                    self.encryption_key.as_ref(),
                )?;
                let key = s3.index_chunk_key(chunk_id, next_version);
                eprintln!(
                    "[sync] index chunk {}: {} pages, {:.1}KB compressed",
                    chunk_id, pages.len(), encoded.len() as f64 / 1024.0,
                );
                index_chunk_uploads.push((key.clone(), encoded));
                if let Some(old_key) = old_index_chunk_keys.get(&chunk_id) {
                    replaced_keys.push(old_key.clone());
                }
                new_index_chunk_keys.insert(chunk_id, key);
            } else {
                new_index_chunk_keys.insert(chunk_id, old_index_chunk_keys[&chunk_id].clone());
            }
        }

        // GC orphaned index chunks: old chunks with no current index leaf pages
        for (old_chunk_id, old_key) in &old_index_chunk_keys {
            if !index_chunks.contains_key(old_chunk_id) {
                replaced_keys.push(old_key.clone());
                eprintln!("[sync] orphaned index chunk {} scheduled for GC", old_chunk_id);
            }
        }

        if !index_chunk_uploads.is_empty() {
            eprintln!("[sync] uploading {} index chunks...", index_chunk_uploads.len());
            s3.put_page_groups(&index_chunk_uploads)?;
            eprintln!("[sync] index chunks uploaded");
        }

        // Update manifest atomically
        let old_manifest = self.manifest.read().clone();
        let mut new_manifest = Manifest {
            version: next_version,
            page_count: old_manifest.page_count,
            page_size: old_manifest.page_size,
            pages_per_group: ppg,
            page_group_keys: new_keys,
            interior_chunk_keys: new_chunk_keys,
            index_chunk_keys: new_index_chunk_keys,
            // Carry forward seekable encoding: re-encoded groups have new frame tables,
            // untouched groups keep their existing frame tables from import.
            frame_tables: new_frame_tables,
            sub_pages_per_frame: old_sub_ppf,
            // Carry forward strategy + B-tree-aware fields
            strategy: old_manifest.strategy,
            group_pages: old_manifest.group_pages.clone(),
            btrees: old_manifest.btrees.clone(),
            page_index: HashMap::new(),
            btree_groups: HashMap::new(),
            page_to_tree_name: HashMap::new(),
            tree_name_to_groups: HashMap::new(),
            group_to_tree_name: HashMap::new(),
            // Phase Verdun (c): serialize access history + prediction patterns
            btree_access_freq: self.access_history.as_ref().map(|ah| {
                let mut h = ah.write();
                h.decay_and_prune();
                h.freq.clone()
            }).unwrap_or_else(|| old_manifest.btree_access_freq.clone()),
            prediction_patterns: self.prediction.as_ref().map(|p| {
                let mut t = p.write();
                t.prune();
                t.to_persisted()
            }).unwrap_or_else(|| old_manifest.prediction_patterns.clone()),
        };
        new_manifest.build_page_index();
        s3.put_manifest(&new_manifest)?;

        // Commit local state
        {
            let mut m = self.manifest.write();
            // Update cache's group_pages to match new manifest
            cache.set_group_pages(new_manifest.group_pages.clone());
            *m = new_manifest;
        }
        {
            let mut dirty = self.dirty_page_nums.write();
            for &page_num in &dirty_snapshot {
                dirty.remove(&page_num);
            }
        }

        // Persist bitmap
        let _ = cache.persist_bitmap();

        // Post-checkpoint GC: delete old page group/interior chunk versions
        if self.gc_enabled && !replaced_keys.is_empty() {
            eprintln!("[gc] deleting {} replaced S3 objects...", replaced_keys.len());
            if let Err(e) = s3.delete_objects(&replaced_keys) {
                eprintln!("[gc] ERROR: failed to delete old objects: {}", e);
            } else {
                eprintln!("[gc] deleted {} old versions", replaced_keys.len());
            }
        }

        Ok(())
    }

    fn set_len(&mut self, size: u64) -> Result<(), io::Error> {
        if self.is_passthrough() {
            return self
                .passthrough_file
                .as_ref()
                .unwrap()
                .write()
                .set_len(size);
        }

        let page_size = *self.page_size.read();
        if page_size == 0 {
            return Ok(());
        }

        let new_page_count = if size == 0 {
            0
        } else {
            (size + page_size as u64 - 1) / page_size as u64
        };

        let mut manifest = self.manifest.write();
        manifest.page_count = new_page_count;

        // Remove dirty pages beyond new size
        let mut dirty = self.dirty_page_nums.write();
        dirty.retain(|&pn| pn < new_page_count);

        Ok(())
    }

    fn lock(&mut self, lock: LockKind) -> Result<bool, io::Error> {
        let current = *self.lock.read();

        if current == lock {
            return Ok(true);
        }

        let lock_file = self.ensure_lock_file()?;

        match lock {
            LockKind::None => {
                self.active_db_locks.clear();
            }
            LockKind::Shared => {
                self.active_db_locks.clear();

                match file_guard::try_lock(
                    std::sync::Arc::clone(&lock_file),
                    file_guard::Lock::Shared,
                    PENDING_BYTE as usize,
                    1,
                ) {
                    Ok(_) => {}
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        return Ok(false);
                    }
                    Err(e) => return Err(e),
                }

                match file_guard::try_lock(
                    std::sync::Arc::clone(&lock_file),
                    file_guard::Lock::Shared,
                    SHARED_FIRST as usize,
                    1,
                ) {
                    Ok(guard) => {
                        self.active_db_locks
                            .insert("shared".to_string(), Box::new(guard));
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        return Ok(false);
                    }
                    Err(e) => return Err(e),
                }
            }
            LockKind::Reserved => {
                if !matches!(
                    current,
                    LockKind::Shared
                        | LockKind::Reserved
                        | LockKind::Pending
                        | LockKind::Exclusive
                ) {
                    return Ok(false);
                }
                match file_guard::try_lock(
                    std::sync::Arc::clone(&lock_file),
                    file_guard::Lock::Exclusive,
                    RESERVED_BYTE as usize,
                    1,
                ) {
                    Ok(guard) => {
                        self.active_db_locks
                            .insert("reserved".to_string(), Box::new(guard));
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        return Ok(false);
                    }
                    Err(e) => return Err(e),
                }
            }
            LockKind::Pending => {
                if !matches!(
                    current,
                    LockKind::Reserved | LockKind::Pending | LockKind::Exclusive
                ) {
                    return Ok(false);
                }
                match file_guard::try_lock(
                    std::sync::Arc::clone(&lock_file),
                    file_guard::Lock::Exclusive,
                    PENDING_BYTE as usize,
                    1,
                ) {
                    Ok(guard) => {
                        self.active_db_locks
                            .insert("pending".to_string(), Box::new(guard));
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        return Ok(false);
                    }
                    Err(e) => return Err(e),
                }
            }
            LockKind::Exclusive => {
                if !matches!(
                    current,
                    LockKind::Reserved | LockKind::Pending | LockKind::Exclusive
                ) {
                    return Ok(false);
                }
                if !self.active_db_locks.contains_key("pending") {
                    match file_guard::try_lock(
                        std::sync::Arc::clone(&lock_file),
                        file_guard::Lock::Exclusive,
                        PENDING_BYTE as usize,
                        1,
                    ) {
                        Ok(guard) => {
                            self.active_db_locks
                                .insert("pending".to_string(), Box::new(guard));
                        }
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            return Ok(false);
                        }
                        Err(e) => return Err(e),
                    }
                }

                self.active_db_locks.remove("shared");

                match file_guard::try_lock(
                    std::sync::Arc::clone(&lock_file),
                    file_guard::Lock::Exclusive,
                    SHARED_FIRST as usize,
                    SHARED_SIZE as usize,
                ) {
                    Ok(guard) => {
                        self.active_db_locks
                            .insert("exclusive".to_string(), Box::new(guard));
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        match file_guard::try_lock(
                            std::sync::Arc::clone(&lock_file),
                            file_guard::Lock::Shared,
                            SHARED_FIRST as usize,
                            1,
                        ) {
                            Ok(guard) => {
                                self.active_db_locks
                                    .insert("shared".to_string(), Box::new(guard));
                            }
                            Err(restore_err) => {
                                return Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    format!("Lock restore failed: {}", restore_err),
                                ));
                            }
                        }
                        return Ok(false);
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        // Phase Verdun: lock session tracking
        if self.prediction.is_some() {
            let was_none = current == LockKind::None;
            let now_none = lock == LockKind::None;

            if was_none && !now_none {
                // Acquiring lock: start a new session
                self.lock_session.start();
            } else if !was_none && now_none {
                // Releasing lock: flush the session
                self.flush_lock_session();
            }
        }

        *self.lock.write() = lock;
        Ok(true)
    }

    fn reserved(&mut self) -> Result<bool, io::Error> {
        let lock = *self.lock.read();
        Ok(matches!(
            lock,
            LockKind::Reserved | LockKind::Pending | LockKind::Exclusive
        ))
    }

    fn current_lock(&self) -> Result<LockKind, io::Error> {
        Ok(*self.lock.read())
    }

    fn wal_index(&self, _readonly: bool) -> Result<Self::WalIndex, io::Error> {
        let shm_path = self.db_path.with_extension("db-shm");
        Ok(FileWalIndex::new(shm_path))
    }
}

#[cfg(test)]
#[path = "test_handle.rs"]
mod tests;

