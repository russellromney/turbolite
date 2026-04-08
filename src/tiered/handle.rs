use super::*;

// ===== TurboliteHandle =====

/// Database handle for tiered S3-backed storage.
///
/// MainDb files are backed by S3 with a local page-level cache.
/// WAL/journal files are passthrough to local disk.
pub struct TurboliteHandle {
    // --- Tiered mode (MainDb) ---
    s3: Option<Arc<S3Client>>,
    /// Unified storage client for local page group flush (local mode only).
    storage: Option<Arc<StorageClient>>,
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
    /// Raw dictionary bytes for local flush (EncoderDictionary is not Clone).
    #[cfg(feature = "zstd")]
    dictionary_bytes: Option<Vec<u8>>,

    /// Phase Zenith-c: true if dirty pages have been written since the last
    /// successful sync. Used to detect transaction rollback (lock downgrade
    /// from EXCLUSIVE/RESERVED without sync having been called).
    dirty_since_sync: bool,

    /// Auto-GC: delete old page group versions after checkpoint.
    gc_enabled: bool,
    override_threshold: u32,
    compaction_threshold: u32,

    /// AES-256-GCM encryption key for S3 data and local cache.
    encryption_key: Option<[u8; 32]>,

    // --- Phase Marne: query-plan-aware prefetch ---
    /// When true, the VFS drains the global plan queue on first read after step().
    query_plan_prefetch: bool,

    // --- Phase Midway: VACUUM detection ---
    /// Schema cookie from page 0 offset 24 (4 bytes BE). Changes on schema modifications
    /// and VACUUM. Used to detect VACUUM and trigger B-tree re-walk at checkpoint.
    last_schema_cookie: Option<u32>,
    /// Old S3 keys to GC after VACUUM re-walk. Populated during VACUUM detection,
    /// consumed during the upload phase.
    vacuum_replaced_keys: Option<Vec<String>>,

    // --- Phase Stalingrad: cache eviction ---
    /// Maximum cache size in bytes. None = unlimited.
    cache_limit: Option<u64>,
    /// Evict data tier after successful checkpoint S3 upload.
    evict_on_checkpoint: bool,

    // --- Phase Kursk: staging log for two-phase checkpoint ---
    /// Append-only staging log writer (open during LocalThenFlush checkpoint).
    /// Lazily opened on first dirty write; closed+fsynced in sync().
    staging_writer: Option<staging::StagingWriter>,
    /// Shared pending flushes. Populated by sync(), drained by flush_to_s3().
    pending_flushes: Arc<Mutex<Vec<staging::PendingFlush>>>,
    /// Monotonic counter for staging log filenames (avoids version collisions).
    staging_seq: Arc<AtomicU64>,
    /// Staging directory path (cache_dir/staging).
    staging_dir: PathBuf,

    // --- Phase Jena: interior map for precise prefetch (experimental, off by default) ---
    jena_enabled: bool,
    interior_map: interior_map::InteriorMap,
    interior_writes_since_rebuild: u32,
    chase_rules: Vec<leaf_chaser::ChaseRule>,
    schema_info: Option<schema::SchemaInfo>,
    bench_verbose: bool,

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

impl TurboliteHandle {
    /// Create a tiered handle backed by S3 + local page cache.
    pub(crate) fn new_tiered(
        s3: Option<Arc<S3Client>>,
        storage: Option<Arc<StorageClient>>,
        cache: Arc<DiskCache>,
        shared_manifest: Arc<RwLock<Manifest>>,
        shared_dirty_groups: Arc<Mutex<HashSet<u64>>>,
        pending_flushes: Arc<Mutex<Vec<staging::PendingFlush>>>,
        staging_seq: Arc<AtomicU64>,
        db_path: PathBuf,
        pages_per_group: u32,
        compression_level: i32,
        read_only: bool,
        sync_mode: SyncMode,
        prefetch_search: Vec<f32>,
        prefetch_lookup: Vec<f32>,
        prefetch_pool: Option<Arc<PrefetchPool>>,
        gc_enabled: bool,
        eager_index_load: bool,
        #[cfg(feature = "zstd")] dictionary: Option<&[u8]>,
        encryption_key: Option<[u8; 32]>,
        query_plan_prefetch: bool,
        max_cache_bytes: Option<u64>,
        evict_on_checkpoint: bool,
        jena_enabled: bool,
    ) -> io::Result<Self> {
        // Snapshot the shared manifest for initialization (avoid holding lock during S3 I/O)
        let manifest = shared_manifest.read().clone();
        let page_size = manifest.page_size;

        // Eagerly fetch page group 0 and interior/index chunks from S3.
        // In local mode (s3=None), these are not fetched eagerly; they're fetched
        // on demand from local page groups when first accessed.
        #[cfg(feature = "cloud")]
        if let Some(ref s3_ref) = s3 {
            // Eagerly fetch page group 0 (contains schema + root page, hit on every query)
            if manifest.page_count > 0 && cache.group_state(0) != GroupState::Present {
                if let Some(key) = manifest.page_group_keys.first() {
                    if !key.is_empty() {
                        if cache.try_claim_group(0) {
                            if let Ok(Some(pg_data)) = s3_ref.get_page_group(key) {
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
                                // Phase Drift-c: apply overrides for group 0
                                if let Some(overrides) = manifest.subframe_overrides.get(0) {
                                    if !overrides.is_empty() && manifest.sub_pages_per_frame > 0 {
                                        let spf = manifest.sub_pages_per_frame as usize;
                                        for (&frame_idx, ovr) in overrides {
                                            if let Ok(Some(ovr_data)) = s3_ref.get_page_group(&ovr.key) {
                                                if let Ok(decompressed) = decode_seekable_subchunk(
                                                    &ovr_data,
                                                    #[cfg(feature = "zstd")]
                                                    dictionary.map(|d| zstd::dict::DecoderDictionary::copy(d)).as_ref(),
                                                    encryption_key.as_ref(),
                                                ) {
                                                    let frame_start = frame_idx * spf;
                                                    let frame_end = std::cmp::min(frame_start + spf, gp0.len());
                                                    if frame_end > frame_start {
                                                        let frame_page_nums = &gp0[frame_start..frame_end];
                                                        let data_len = frame_page_nums.len() * manifest.page_size as usize;
                                                        if data_len <= decompressed.len() {
                                                            let _ = cache.write_pages_scattered(
                                                                frame_page_nums, &decompressed[..data_len],
                                                                0, frame_start as u32,
                                                            );
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                cache.mark_group_present(0);
                                cache.touch_group(0);
                            }
                        }
                    }
                }
            }

            // Eagerly fetch interior chunks (B-tree interior pages split across S3 objects).
            let interior_already_cached = !cache.interior_pages.lock().is_empty();
            if interior_already_cached {
                eprintln!(
                    "[tiered] interior pages already cached ({} pages), skipping chunk fetch",
                    cache.interior_pages.lock().len(),
                );
            } else if !manifest.interior_chunk_keys.is_empty() {
                let chunk_keys: Vec<String> = manifest.interior_chunk_keys.values().cloned().collect();
                eprintln!("[tiered] fetching {} interior chunks in parallel...", chunk_keys.len());
                match s3_ref.get_page_groups_by_key(&chunk_keys) {
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
                                Err(e) => {
                                    return Err(io::Error::new(io::ErrorKind::InvalidData,
                                        format!("interior chunk {} decode failed: {}", key, e)));
                                }
                            }
                        }
                        eprintln!(
                            "[tiered] interior chunks loaded: {} pages from {} chunks ({:.1}KB total)",
                            total_pages, results.len(), total_bytes as f64 / 1024.0,
                        );
                    }
                    Err(e) => {
                        return Err(io::Error::new(io::ErrorKind::Other,
                            format!("interior chunk fetch failed: {}", e)));
                    }
                }
            }

            // Index leaf bundles: synchronous fetch (errors must surface on open)
            let index_already_cached = !cache.index_pages.lock().is_empty();
            if eager_index_load && !manifest.index_chunk_keys.is_empty() && !index_already_cached {
                let chunk_keys: Vec<String> = manifest.index_chunk_keys.values().cloned().collect();
                let n_chunks = chunk_keys.len();
                eprintln!("[tiered] fetching {} index leaf chunks...", n_chunks);
                match s3_ref.get_page_groups_by_key(&chunk_keys) {
                    Ok(results) => {
                        #[cfg(feature = "zstd")]
                        let ix_decoder = dictionary.map(zstd::dict::DecoderDictionary::copy);
                        let mut total_pages = 0usize;
                        let mut total_bytes = 0usize;
                        for (key, data) in &results {
                            total_bytes += data.len();
                            match decode_interior_bundle(
                                data,
                                #[cfg(feature = "zstd")]
                                ix_decoder.as_ref(),
                                encryption_key.as_ref(),
                            ) {
                                Ok(pages) => {
                                    total_pages += pages.len();
                                    for (pnum, pdata) in &pages {
                                        let _ = cache.write_page(*pnum, pdata);
                                        cache.index_pages.lock().insert(*pnum);
                                    }
                                }
                                Err(e) => {
                                    return Err(io::Error::new(io::ErrorKind::InvalidData,
                                        format!("index chunk {} decode failed: {}", key, e)));
                                }
                            }
                        }
                        eprintln!(
                            "[tiered] index leaf chunks loaded: {} pages from {} chunks ({:.1}KB total)",
                            total_pages, results.len(), total_bytes as f64 / 1024.0,
                        );
                    }
                    Err(e) => {
                        return Err(io::Error::new(io::ErrorKind::Other,
                            format!("index chunk fetch failed: {}", e)));
                    }
                }
            } else if index_already_cached {
                eprintln!(
                    "[tiered] index pages already cached ({} pages), skipping chunk fetch",
                    cache.index_pages.lock().len(),
                );
            }
        } // end #[cfg(feature = "cloud")] S3 eager fetch block

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

        let staging_dir = cache.cache_dir.join("staging");

        // Phase Jena: build interior map from cached interior pages (if enabled).
        let interior_map = if jena_enabled {
            let m = shared_manifest.read();
            let map = interior_map::InteriorMap::build(&cache, &m);
            if !map.is_empty() {
                eprintln!(
                    "[tiered] Jena: built interior map ({} interior pages, {} children)",
                    map.interior_count(),
                    map.child_count(),
                );
            }
            map
        } else {
            interior_map::InteriorMap::default()
        };

        Ok(Self {
            s3,
            storage,
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
            prefetch_search,
            prefetch_lookup,
            search_trees: HashSet::new(),
            prefetch_pool,
            dirty_since_sync: false,
            gc_enabled,
            override_threshold: 0,
            compaction_threshold: 0,
            encryption_key,
            #[cfg(feature = "zstd")]
            encoder_dict,
            #[cfg(feature = "zstd")]
            decoder_dict,
            #[cfg(feature = "zstd")]
            dictionary_bytes: dictionary.map(|d| d.to_vec()),
            query_plan_prefetch,
            last_schema_cookie: None,
            vacuum_replaced_keys: None,
            cache_limit: max_cache_bytes.and_then(|n| if n == 0 { None } else { Some(n) }),
            evict_on_checkpoint,
            staging_writer: None,
            pending_flushes,
            staging_seq,
            staging_dir,
            jena_enabled,
            interior_map,
            interior_writes_since_rebuild: 0,
            chase_rules: Vec::new(),
            schema_info: None,
            bench_verbose: std::env::var("BENCH_VERBOSE").is_ok(),
            passthrough_file: None,
            lock: RwLock::new(LockKind::None),
            db_path,
            lock_file: None,
            active_db_locks: HashMap::new(),
        })
    }

    /// Create a passthrough handle for WAL/journal files (local file I/O).
    pub(crate) fn new_passthrough(file: File, db_path: PathBuf, encryption_key: Option<[u8; 32]>) -> Self {
        Self {
            s3: None,
            storage: None,
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
            prefetch_search: vec![0.3, 0.3, 0.4],
            prefetch_lookup: vec![0.0, 0.0, 0.0],
            search_trees: HashSet::new(),
            prefetch_pool: None,
            dirty_since_sync: false,
            gc_enabled: false,
            override_threshold: 0,
            compaction_threshold: 0,
            encryption_key,
            #[cfg(feature = "zstd")]
            encoder_dict: None,
            #[cfg(feature = "zstd")]
            decoder_dict: None,
            #[cfg(feature = "zstd")]
            dictionary_bytes: None,
            query_plan_prefetch: false,
            last_schema_cookie: None,
            vacuum_replaced_keys: None,
            cache_limit: None,
            evict_on_checkpoint: false,
            staging_writer: None,
            pending_flushes: Arc::new(Mutex::new(Vec::new())),
            staging_seq: Arc::new(AtomicU64::new(0)),
            staging_dir: PathBuf::new(),
            jena_enabled: false,
            interior_map: interior_map::InteriorMap::default(),
            interior_writes_since_rebuild: 0,
            chase_rules: Vec::new(),
            schema_info: None,
            bench_verbose: false,
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

    /// Get miss count for a tree. Returns 0 for unknown trees.
    fn miss_count(&self, tree_name: Option<&String>) -> u8 {
        match tree_name {
            Some(name) => *self.tree_miss_counts.get(name).unwrap_or(&0),
            None => 0,
        }
    }

    /// Increment miss count for a tree.
    fn increment_misses(&mut self, tree_name: Option<&String>) {
        if let Some(name) = tree_name {
            let count = self.tree_miss_counts.entry(name.clone()).or_insert(0);
            *count = count.saturating_add(1);
        }
    }

    /// Reset miss count for a tree on cache hit.
    fn reset_misses(&mut self, tree_name: Option<&String>) {
        if let Some(name) = tree_name {
            self.tree_miss_counts.remove(name);
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

    /// Phase Jena-d: attempt cross-tree leaf chasing after reading a leaf page.
    ///
    /// If chase rules exist for the current tree, parse the leaf page,
    /// extract join column values, predict target leaf groups, and submit
    /// them for prefetch.
    fn try_leaf_chase(&mut self, buf: &[u8], page_num: u64, cache: &Arc<DiskCache>) {
        if !self.jena_enabled || self.chase_rules.is_empty() {
            return;
        }

        let hdr_off = if page_num == 0 { 100 } else { 0 };
        let type_byte = buf.get(hdr_off).copied().unwrap_or(0);

        // Only chase from leaf pages (0x0D = table leaf, 0x0A = index leaf)
        let is_table_leaf = type_byte == 0x0D;
        let is_index_leaf = type_byte == 0x0A;
        if !is_table_leaf && !is_index_leaf {
            return;
        }

        // Find which tree this page belongs to
        let manifest = self.manifest.read();
        let tree_name = manifest.group_to_tree_name
            .get(&manifest.page_location(page_num).map(|l| l.group_id).unwrap_or(u64::MAX))
            .cloned();

        let tree_name = match tree_name {
            Some(n) => n,
            None => { drop(manifest); return; },
        };

        // Check all chase rules for this source tree
        let pool = match &self.prefetch_pool {
            Some(p) => Arc::clone(p),
            None => { drop(manifest); return; },
        };
        let ps = manifest.page_size;
        let sub_ppf = manifest.sub_pages_per_frame;

        for rule in &self.chase_rules {
            if rule.source_tree != tree_name {
                continue;
            }

            // Extract chase keys from the leaf page
            let keys = leaf_chaser::extract_chase_keys(
                buf, hdr_off, rule.col_index, is_table_leaf, 64, // max 64 keys per page
            );
            if keys.is_empty() {
                continue;
            }

            // Predict target groups
            let groups = leaf_chaser::chase_predict_groups(
                &keys, rule.target_root_page, &self.interior_map,
            );

            // Submit for prefetch
            let mut submitted = 0usize;
            for gid in &groups {
                if cache.group_state(*gid) != GroupState::None {
                    continue;
                }
                if !cache.try_claim_group(*gid) {
                    continue;
                }
                if let Some(key) = manifest.page_group_keys.get(*gid as usize) {
                    if !key.is_empty() {
                        let ft = manifest.frame_tables.get(*gid as usize).cloned().unwrap_or_default();
                        let gp = manifest.group_page_nums(*gid).into_owned();
                        let ovrs = manifest.subframe_overrides.get(*gid as usize).cloned().unwrap_or_default();
                        if pool.submit(*gid, key.clone(), ft, ps, sub_ppf, gp, ovrs, manifest.version) {
                            submitted += 1;
                        } else {
                            cache.unclaim_group(*gid);
                        }
                    } else {
                        cache.unclaim_group(*gid);
                    }
                } else {
                    cache.unclaim_group(*gid);
                }
            }

            if submitted > 0 && self.bench_verbose {
                eprintln!(
                    "  [jena-chase] {} -> {}: {} keys, {} groups predicted, {} submitted",
                    rule.source_tree, rule.target_tree, keys.len(), groups.len(), submitted,
                );
            }
        }
        drop(manifest);
    }

    /// Phase Jena-f: on interior page read, prefetch sibling interior page groups.
    ///
    /// For very large databases where interior pages span multiple groups,
    /// this prevents a blocking fault when SQLite descends to the next
    /// interior page. Only fires for pages that ARE interior pages and
    /// whose siblings are in different groups.
    fn try_interior_lookahead(&self, buf: &[u8], page_num: u64, cache: &Arc<DiskCache>) {
        if !self.jena_enabled { return; }
        let hdr_off = if page_num == 0 { 100 } else { 0 };
        let type_byte = buf.get(hdr_off).copied().unwrap_or(0);
        if type_byte != 0x05 && type_byte != 0x02 {
            return; // not an interior page
        }

        // Find siblings of this interior page (they share the same parent)
        let sibling_groups = self.interior_map.sibling_groups(page_num);
        if sibling_groups.is_empty() {
            return;
        }

        let pool = match &self.prefetch_pool {
            Some(p) => p,
            None => return,
        };
        let manifest = self.manifest.read();
        let ps = manifest.page_size;
        let sub_ppf = manifest.sub_pages_per_frame;

        let mut submitted = 0usize;
        for &gid in &sibling_groups {
            if cache.group_state(gid) != GroupState::None { continue; }
            if !cache.try_claim_group(gid) { continue; }
            if let Some(key) = manifest.page_group_keys.get(gid as usize) {
                if !key.is_empty() {
                    let ft = manifest.frame_tables.get(gid as usize).cloned().unwrap_or_default();
                    let gp = manifest.group_page_nums(gid).into_owned();
                    let ovrs = manifest.subframe_overrides.get(gid as usize).cloned().unwrap_or_default();
                    if pool.submit(gid, key.clone(), ft, ps, sub_ppf, gp, ovrs, manifest.version) {
                        submitted += 1;
                    } else {
                        cache.unclaim_group(gid);
                    }
                } else {
                    cache.unclaim_group(gid);
                }
            } else {
                cache.unclaim_group(gid);
            }
        }

        if submitted > 0 && self.bench_verbose {
            eprintln!(
                "  [jena-lookahead] interior page={} prefetched {} sibling interior groups",
                page_num, submitted,
            );
        }
    }

    /// Phase Jena-e: detect overflow in a leaf page and prefetch overflow page groups.
    fn try_overflow_prefetch(&self, buf: &[u8], page_num: u64, cache: &Arc<DiskCache>) {
        if !self.jena_enabled { return; }
        let page_size = *self.page_size.read();
        if page_size == 0 { return; }

        let overflow_pages = overflow::detect_overflow_pages(buf, page_num, page_size);
        if overflow_pages.is_empty() { return; }

        let pool = match &self.prefetch_pool {
            Some(p) => p,
            None => return,
        };
        let manifest = self.manifest.read();
        let ps = manifest.page_size;
        let sub_ppf = manifest.sub_pages_per_frame;

        let mut submitted = 0usize;
        for ovfl_page in &overflow_pages {
            if let Some(loc) = manifest.page_location(*ovfl_page) {
                let gid = loc.group_id;
                if cache.group_state(gid) != GroupState::None { continue; }
                if !cache.try_claim_group(gid) { continue; }
                if let Some(key) = manifest.page_group_keys.get(gid as usize) {
                    if !key.is_empty() {
                        let ft = manifest.frame_tables.get(gid as usize).cloned().unwrap_or_default();
                        let gp = manifest.group_page_nums(gid).into_owned();
                        let ovrs = manifest.subframe_overrides.get(gid as usize).cloned().unwrap_or_default();
                        if pool.submit(gid, key.clone(), ft, ps, sub_ppf, gp, ovrs, manifest.version) {
                            submitted += 1;
                        } else {
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

        if submitted > 0 && self.bench_verbose {
            eprintln!(
                "  [jena-overflow] page={} detected {} overflow pages, submitted {} groups",
                page_num, overflow_pages.len(), submitted,
            );
        }
    }

    /// Ensure interior map is up to date before making prefetch decisions.
    /// Rebuilds if interior pages were written since last rebuild.
    fn ensure_interior_map_fresh(&mut self) {
        if self.interior_writes_since_rebuild > 0 {
            if let Some(cache) = &self.cache {
                let manifest = self.manifest.read();
                self.interior_map = interior_map::InteriorMap::build(cache, &manifest);
                self.interior_writes_since_rebuild = 0;
            }
        }
    }

    /// Phase Jena: precise prefetch using interior map.
    ///
    /// On cache miss, uses the B-tree child pointer map to identify exact
    /// sibling groups instead of guessing with hop-schedule fractions.
    /// Falls back to manifest-based sibling prefetch if interior map is empty.
    pub(crate) fn trigger_prefetch(
        &mut self,
        page_num: u64,
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
                    let ovrs = manifest.subframe_overrides.get(gid as usize).cloned().unwrap_or_default();
                    if pool.submit(gid, key.clone(), ft, ps, sub_ppf, gp, ovrs, manifest.version) {
                        *submitted += 1;
                        return true;
                    }
                }
            }
            cache.unclaim_group(gid);
            false
        };

        // Phase Jena: use interior map for precise sibling selection (if enabled).
        if self.jena_enabled && !self.interior_map.is_empty() {
            let tree_name = manifest.group_to_tree_name.get(&current_gid);
            let is_search = tree_name.map(|n| self.search_trees.contains(n)).unwrap_or(false);

            // For SEARCH: prefetch exact siblings from interior map
            // For SCAN: handled by plan-aware bulk prefetch (not here)
            // For lookup: conservative, prefetch 0-1 siblings
            let sibling_groups = self.interior_map.sibling_groups(page_num);

            if sibling_groups.is_empty() {
                return;
            }

            let max_submit = if is_search {
                sibling_groups.len() // SEARCH: prefetch all siblings
            } else {
                // Lookup: conservative, use hop schedule fraction
                let hops = &self.prefetch_lookup;
                let miss_count = self.miss_count(tree_name);
                let hop_idx = miss_count.saturating_sub(1) as usize;
                let fraction = if hop_idx < hops.len() { hops[hop_idx] } else { 1.0 };
                if fraction <= 0.0 { return; }
                ((sibling_groups.len() as f32) * fraction).ceil() as usize
            };

            let mut submitted = 0usize;
            for &gid in &sibling_groups {
                if submitted >= max_submit { break; }
                try_submit(gid, &mut submitted);
            }

            if std::env::var("BENCH_VERBOSE").is_ok() {
                eprintln!(
                    "  [jena-prefetch] page={} gid={} siblings={} submitted={} mode={}",
                    page_num, current_gid, sibling_groups.len(), submitted,
                    if is_search { "search" } else { "lookup" },
                );
            }
            return;
        }

        // Fallback: manifest-based sibling prefetch (legacy hop schedule)
        let siblings = manifest.prefetch_siblings(current_gid);
        let eligible: Vec<u64> = siblings.iter()
            .copied()
            .filter(|&gid| gid != current_gid && cache.group_state(gid) == GroupState::None)
            .collect();
        if eligible.is_empty() {
            return;
        }

        let tree_name = manifest.group_to_tree_name.get(&current_gid);
        let is_search = tree_name.map(|n| self.search_trees.contains(n)).unwrap_or(false);
        let hops = if is_search { &self.prefetch_search } else { &self.prefetch_lookup };

        let miss_count = self.miss_count(tree_name);
        let hop_idx = miss_count.saturating_sub(1) as usize;
        let fraction = if hop_idx < hops.len() { hops[hop_idx] } else { 1.0 };
        if fraction <= 0.0 { return; }

        let max_submit = ((eligible.len() as f32) * fraction).ceil() as usize;
        let mut submitted = 0usize;
        for &gid in &eligible {
            if submitted >= max_submit { break; }
            try_submit(gid, &mut submitted);
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

}

impl DatabaseHandle for TurboliteHandle {
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

        // 3. Look up page location (Phase Midway).
        // Safe to .expect() here because:
        //    - New pages (not in manifest) are always in dirty_page_nums (returned above)
        //    - Pages beyond page_count are zero-filled (returned above)
        //    - sync() assigns new pages to groups before clearing dirty_page_nums
        let manifest_ref = self.manifest.read();
        let loc = manifest_ref.page_location(page_num)
            .expect("page within manifest bounds must have group assignment");

        let gid = loc.group_id;
        let current_tree_name = manifest_ref.group_to_tree_name.get(&gid).cloned();
        drop(manifest_ref);
        let page_in_group_idx = loc.index as usize;

        // 3c. Phase Stalingrad: between-query eviction trigger.
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
                    "evict_on_checkpoint" => {
                        self.evict_on_checkpoint = matches!(update.value.as_str(), "true" | "1");
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

                // Phase Jena-d: build chase rules from plan + schema.
                // Take schema info from global cache (pushed by extension trace callback).
                // Re-take if schema was invalidated (DDL/VACUUM changes schema cookie).
                if self.schema_info.is_none() {
                    self.schema_info = schema::take_schema();
                }
                // Check for fresh schema (DDL may have pushed a new one)
                if let Some(fresh) = schema::take_schema() {
                    self.schema_info = Some(fresh);
                }
                if let Some(ref schema) = self.schema_info {
                    self.chase_rules = leaf_chaser::build_chase_rules(
                        &planned,
                        &schema.tree_roots,
                        &schema.table_columns,
                    );
                    if !self.chase_rules.is_empty() && self.bench_verbose {
                        eprintln!(
                            "  [jena-chase] built {} chase rules from {} planned accesses",
                            self.chase_rules.len(), planned.len(),
                        );
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
                                            let ovrs = manifest_snap.subframe_overrides.get(plan_gid as usize).cloned().unwrap_or_default();
                                            if !pool.submit(plan_gid, key.clone(), ft, manifest_snap.page_size, sub_ppf, gp, ovrs, manifest_snap.version) {
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
            // Debug: log reads of first 5 pages to trace HA promotion data visibility
            if page_num < 5 {
                let nonzero = buf.iter().filter(|&&b| b != 0).count();
                eprintln!("[read_exact_at] page {} CACHE HIT (gid={}, {}/{} nonzero bytes)",
                    page_num, gid, nonzero, buf.len());
            }
            self.detect_interior_page(buf, page_num, cache);
            self.try_leaf_chase(buf, page_num, &cache_arc);
            self.try_overflow_prefetch(buf, page_num, &cache_arc);
            self.try_interior_lookahead(buf, page_num, &cache_arc);
            cache.touch_group(gid);
            cache.stat_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }

        // 5. Cache miss - fetch from storage (S3 or local page groups).
        if page_num < 5 {
            let manifest_snap = self.manifest.read();
            let has_overrides = manifest_snap.subframe_overrides
                .get(gid as usize)
                .map(|ovs| !ovs.is_empty())
                .unwrap_or(false);
            let override_count = manifest_snap.subframe_overrides
                .get(gid as usize)
                .map(|ovs| ovs.len())
                .unwrap_or(0);
            eprintln!("[read_exact_at] page {} CACHE MISS (gid={}, manifest_v={}, overrides_for_gid={}, has_overrides={})",
                page_num, gid, manifest_snap.version, override_count, has_overrides);
            drop(manifest_snap);
        }
        cache.stat_misses.fetch_add(1, Ordering::Relaxed);

        // 5.local: For local-only mode, fetch the page group from local pg/ directory,
        // decode it, populate the cache, then read the page.
        if self.s3.is_none() {
            if let Some(ref storage) = self.storage {
                let manifest = self.manifest.read().clone();
                if let Some(key) = manifest.page_group_keys.get(gid as usize) {
                    if !key.is_empty() {
                        if let Ok(Some(pg_data)) = storage.get_page_group(key) {
                            let pages_in_group = manifest.group_page_nums(gid);
                            let ft = manifest.frame_tables.get(gid as usize);
                            let has_ft = manifest.sub_pages_per_frame > 0
                                && ft.map(|f| !f.is_empty()).unwrap_or(false);

                            let decoded_ok = if has_ft {
                                if let Ok((_pc, _ps, bulk_data)) = decode_page_group_seekable_full(
                                    &pg_data,
                                    ft.expect("checked above"),
                                    manifest.page_size,
                                    pages_in_group.len() as u32,
                                    manifest.page_count,
                                    0,
                                    #[cfg(feature = "zstd")]
                                    self.decoder_dict.as_ref(),
                                    self.encryption_key.as_ref(),
                                ) {
                                    cache.write_pages_scattered(
                                        &pages_in_group, &bulk_data, gid, 0,
                                    )?;
                                    true
                                } else { false }
                            } else if let Ok((_pc, _ps, bulk_data)) = decode_page_group_bulk(
                                &pg_data,
                                #[cfg(feature = "zstd")]
                                self.decoder_dict.as_ref(),
                                self.encryption_key.as_ref(),
                            ) {
                                cache.write_pages_scattered(
                                    &pages_in_group, &bulk_data, gid, 0,
                                )?;
                                true
                            } else { false };

                            if decoded_ok {
                                // Phase Drift-c: apply override frames
                                if let Some(overrides) = manifest.subframe_overrides.get(gid as usize) {
                                    if !overrides.is_empty() && manifest.sub_pages_per_frame > 0 {
                                        let spf = manifest.sub_pages_per_frame as usize;
                                        for (&frame_idx, ovr) in overrides {
                                            if let Ok(Some(ovr_data)) = storage.get_page_group(&ovr.key) {
                                                if let Ok(decompressed) = decode_seekable_subchunk(
                                                    &ovr_data,
                                                    #[cfg(feature = "zstd")]
                                                    self.decoder_dict.as_ref(),
                                                    self.encryption_key.as_ref(),
                                                ) {
                                                    let frame_start = frame_idx * spf;
                                                    let frame_end = std::cmp::min(frame_start + spf, pages_in_group.len());
                                                    let frame_page_nums = &pages_in_group[frame_start..frame_end];
                                                    let data_len = frame_page_nums.len() * manifest.page_size as usize;
                                                    if data_len <= decompressed.len() {
                                                        let _ = cache.write_pages_scattered(
                                                            frame_page_nums, &decompressed[..data_len],
                                                            gid, frame_start as u32,
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                cache.mark_group_present(gid);
                                cache.touch_group(gid);
                            }

                            // Now read the page from cache
                            if cache.is_present(page_num) {
                                cache.read_page(page_num, buf)?;
                                return Ok(());
                            }
                        }
                    }
                }
            }

            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("page {} not recoverable from local page groups", page_num),
            ));
        }

        let manifest = self.manifest.read().clone();

        // 5a. Re-check cache: a sibling prefetch may have completed since step 4.
        if cache.is_present(page_num) {
            cache.read_page(page_num, buf)?;
            self.detect_interior_page(buf, page_num, cache);
            self.try_leaf_chase(buf, page_num, &cache_arc);
            self.try_overflow_prefetch(buf, page_num, &cache_arc);
            self.try_interior_lookahead(buf, page_num, &cache_arc);
            cache.touch_group(gid);
            self.reset_misses(current_tree_name.as_ref());
            cache.stat_misses.fetch_sub(1, Ordering::Relaxed);
            cache.stat_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }

        // S3 fetch paths (seekable + legacy) only available with cloud feature.
        #[cfg(feature = "cloud")]
        {
        let s3_arc = Arc::clone(self.s3.as_ref().expect("s3 checked above"));
        let miss_start = Instant::now();

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
                let base_key = manifest.page_group_keys.get(gid as usize)
                    .expect("gid must be valid index into page_group_keys");
                let entry = &ft[frame_idx];

                // Phase Drift-c: check for override frame
                let override_entry = manifest.subframe_overrides
                    .get(gid as usize)
                    .and_then(|ovs| ovs.get(&frame_idx));


                // Submit the CURRENT group to prefetch pool (full group fetch in background).
                self.increment_misses(current_tree_name.as_ref());
                if let Some(pool) = &self.prefetch_pool {
                    if cache.group_state(gid) == GroupState::None
                        && cache.try_claim_group(gid)
                    {
                        if let Some(key) = manifest.page_group_keys.get(gid as usize) {
                            if !key.is_empty() {
                                let gp_owned = manifest.group_page_nums(gid).into_owned();
                                let ovrs = manifest.subframe_overrides.get(gid as usize).cloned().unwrap_or_default();
                                if !pool.submit(gid, key.clone(), ft.to_vec(), manifest.page_size, sub_ppg, gp_owned, ovrs, manifest.version) {
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
                self.ensure_interior_map_fresh();
                self.trigger_prefetch(page_num, gid, &manifest, &cache_arc, &s3_arc);

                let s3_start = Instant::now();
                let fetch_result = if let Some(ovr) = override_entry {
                    s3_arc.get_page_group(&ovr.key).map(|opt| opt.map(|d| d))
                } else {
                    s3_arc.range_get(base_key, entry.offset, entry.len)
                };
                match fetch_result {
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

                        // Debug: log foreground S3 fetch details for first pages
                        if page_num < 5 {
                            let is_override = override_entry.is_some();
                            let nonzero_in_page = if src_end <= decompressed.len() {
                                decompressed[src_start..src_end].iter().filter(|&&b| b != 0).count()
                            } else { 0 };
                            eprintln!("[read_exact_at] page {} S3 fetch: {}B compressed, {}B decompressed, override={}, page has {}/{} nonzero bytes",
                                page_num, compressed_frame.len(), decompressed.len(), is_override, nonzero_in_page, ps);
                        }
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
                        self.try_leaf_chase(buf, page_num, &cache_arc);
                        self.try_overflow_prefetch(buf, page_num, &cache_arc);
                        self.try_interior_lookahead(buf, page_num, &cache_arc);
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
                self.ensure_interior_map_fresh();
                self.trigger_prefetch(page_num, gid, &manifest, &cache_arc, &s3_arc);
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
        } // end #[cfg(feature = "cloud")] S3 fetch block

        // Read the page from cache (should be present now after legacy download).
        if cache.is_present(page_num) {
            self.reset_misses(current_tree_name.as_ref());
            cache.read_page(page_num, buf)?;
            self.detect_interior_page(buf, page_num, cache);
            self.try_leaf_chase(buf, page_num, &cache_arc);
            self.try_overflow_prefetch(buf, page_num, &cache_arc);
            self.try_interior_lookahead(buf, page_num, &cache_arc);
            cache.touch_group(gid);
            return Ok(());
        }

        // Phase Kursk: fallback read from cache file even if bitmap says absent.
        if cache.read_page(page_num, buf).is_ok() && buf.iter().any(|&b| b != 0) {
            cache.bitmap.lock().mark_present(page_num);
            cache.mark_group_present(gid);
            self.detect_interior_page(buf, page_num, cache);
            self.try_leaf_chase(buf, page_num, &cache_arc);
            self.try_overflow_prefetch(buf, page_num, &cache_arc);
            self.try_interior_lookahead(buf, page_num, &cache_arc);
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
                "TurboliteHandle is read-only",
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

        // Phase Midway: capture schema cookie from page 0 for VACUUM detection.
        // Read the EXISTING cookie before this write overwrites it.
        if page_num == 0 && self.last_schema_cookie.is_none() {
            if let Some(cache) = &self.cache {
                let mut old_page0 = vec![0u8; page_size as usize];
                if cache.read_page(0, &mut old_page0).is_ok() && old_page0.len() >= 28 {
                    let cookie = u32::from_be_bytes([old_page0[24], old_page0[25], old_page0[26], old_page0[27]]);
                    if cookie != 0 {
                        self.last_schema_cookie = Some(cookie);
                    }
                }
            }
            // If cache didn't have page 0 yet, read from the incoming write
            if self.last_schema_cookie.is_none() && buf.len() >= 28 {
                let cookie = u32::from_be_bytes([buf[24], buf[25], buf[26], buf[27]]);
                if cookie != 0 {
                    self.last_schema_cookie = Some(cookie);
                }
            }
        }

        // Write to local cache and track as dirty (Phase Marne: data only in cache, not in memory)
        if let Some(cache) = &self.cache {
            cache.write_page(page_num, buf)?;
        }
        self.dirty_page_nums.write().insert(page_num);
        self.dirty_since_sync = true;

        // Phase Jena: detect interior page writes for map rebuild (if enabled).
        if self.jena_enabled {
            let hdr_off = if page_num == 0 { 100 } else { 0 };
            let type_byte = buf.get(hdr_off).copied().unwrap_or(0);
            if type_byte == 0x05 || type_byte == 0x02 {
                self.interior_writes_since_rebuild += 1;
            }
        }

        // Phase Kursk: append to staging log for LocalThenFlush mode.
        // Captures exact page contents at write time, immune to overwrite
        // by subsequent checkpoints.
        if self.sync_mode == SyncMode::LocalThenFlush {
            let ps = *self.page_size.read();
            if self.staging_writer.is_none() && ps > 0 {
                let seq = self.staging_seq.fetch_add(1, Ordering::Relaxed);
                self.staging_writer = Some(
                    staging::StagingWriter::open(&self.staging_dir, seq, ps)?
                );
            }
            if let Some(ref mut writer) = self.staging_writer {
                writer.append(page_num, buf)?;
            }
        }

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
            // Seed sub_pages_per_frame from DiskCache config so local and cloud
            // produce identical page group encoding (seekable multi-frame format).
            if manifest.sub_pages_per_frame == 0 {
                if let Some(cache) = &self.cache {
                    let spf = cache.sub_pages_per_frame;
                    if spf > 0 {
                        manifest.sub_pages_per_frame = spf;
                    }
                }
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
        let dirty_count = self.dirty_page_nums.read().len();
        eprintln!("[vfs::sync] called (dirty={}, read_only={}, sync_mode={:?})", dirty_count, self.read_only, self.sync_mode);
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
                self.dirty_since_sync = false;
                eprintln!("[sync] early return: 0 dirty pages, 0 pending groups (sync_mode={:?})", self.sync_mode);
                return Ok(());
            }
            eprintln!("[sync] dirty={}, pending={}, sync_mode={:?}", dirty.len(), has_pending_groups, self.sync_mode);
            dirty.clone()
        };

        let ppg = self.pages_per_group;

        // Local-checkpoint-only: record dirty group IDs, scan interior pages,
        // free in-memory dirty map, but skip S3 upload entirely.
        // Pages are already in disk cache from write_all_at().
        if self.sync_mode == SyncMode::LocalThenFlush || LOCAL_CHECKPOINT_ONLY.load(Ordering::Acquire) {
            let cache_arc = self.cache.as_ref().expect("cache required for tiered handle").clone();
            let cache = &*cache_arc;
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
            let page_size = *self.page_size.read();
            let mut type_buf = vec![0u8; page_size as usize];
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
                        return Err(io::Error::new(io::ErrorKind::Other,
                            format!("cache.read_page({}) failed during local checkpoint interior scan: {}", page_num, e)));
                    }
                }
            }
            drop(manifest);
            let n = dirty_snapshot.len();
            let total_interior = cache.interior_pages.lock().len();
            // Free dirty page tracking
            self.dirty_page_nums.write().clear();
            self.dirty_since_sync = false;
            let cache_dir = cache.cache_dir.clone();
            let pending_groups_snapshot: Vec<u64> = pending.iter().copied().collect();
            drop(pending);
            eprintln!("[sync] local-only checkpoint: {} pages, {} interior this batch, {} interior total, {} dirty groups", n, interior_found, total_interior, pending_groups_snapshot.len());

            // Phase Kursk: finalize staging log (fsync + close) and push PendingFlush.
            // The staging log captured exact page contents during write_all_at(),
            // immune to overwrite by subsequent checkpoints.
            if let Some(writer) = self.staging_writer.take() {
                let (staging_path, pages_written) = writer.finalize()?;
                // Version from filename (seq counter assigned at open time)
                let version = staging_path.file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);
                eprintln!(
                    "[sync] staging log finalized: {} pages, version {}, path {}",
                    pages_written, version, staging_path.display(),
                );
                self.pending_flushes.lock().unwrap().push(staging::PendingFlush {
                    staging_path,
                    version,
                    page_size,
                });
            }

            // Phase Gallipoli: persist manifest + dirty groups locally for crash recovery.
            // Errors MUST propagate: if persist fails, SQLite must not discard the WAL.
            {
                let m = self.manifest.read().clone();
                let local = manifest::LocalManifest { manifest: m, dirty_groups: pending_groups_snapshot };
                local.persist(&cache_dir)?;
            }

            // Persist bitmap so cached pages survive process restart
            cache.persist_bitmap()?;

            // Local mode: flush dirty page groups to local pg/ directory immediately.
            // This encodes page groups from the cache and writes them alongside the manifest,
            // so data can be recovered even if the cache file is deleted.
            if let Some(ref storage) = self.storage {
                flush::flush_local_groups(
                    storage,
                    cache,
                    &self.manifest,
                    &self.s3_dirty_groups,
                    &self.pending_flushes,
                    self.compression_level,
                    #[cfg(feature = "zstd")]
                    self.dictionary_bytes.as_deref(),
                    self.encryption_key,
                    self.override_threshold,
                    self.compaction_threshold,
                )?;
            }

            return Ok(());
        }

        // ── S3Primary sync path: upload dirty frames as overrides + publish manifest ──
        // Every xSync is an S3 commit. No staging logs, no deferred flush.
        // Override-only (no full group rewrites) to keep per-commit latency bounded.
        #[cfg(feature = "cloud")]
        if self.sync_mode == SyncMode::S3Primary {
            let page_size = *self.page_size.read();
            let s3 = self.s3.as_ref().expect("S3Primary requires cloud feature with S3 client").clone();
            let cache = self.cache.as_ref().expect("cache required").clone();

            // Enforce journal_mode != WAL. SQLite stores journal mode at page 0
            // offset 18-19. WAL mode = 2 at offset 18. S3Primary is incompatible
            // with WAL because xSync must be the atomic commit point.
            if dirty_snapshot.contains(&0) || self.manifest.read().version == 0 {
                let mut page0 = vec![0u8; page_size as usize];
                if cache.read_page(0, &mut page0).is_ok() && page0.len() > 19 {
                    let journal_mode = page0[18];
                    if journal_mode == 2 {
                        return Err(io::Error::new(
                            io::ErrorKind::Unsupported,
                            "S3Primary mode requires journal_mode=OFF or MEMORY, not WAL. \
                             Run PRAGMA journal_mode=OFF before enabling S3Primary.",
                        ));
                    }
                }
            }

            // Assign new pages to groups
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
            let next_version = manifest_snap.version + 1;
            let ovr_count: usize = manifest_snap.subframe_overrides.iter()
                .map(|ovs| ovs.len()).sum();
            eprintln!("[sync:s3primary] building v{} from v{} (page_count={}, dirty={}, overrides={}, pg_keys={})",
                next_version, manifest_snap.version, page_count, dirty_snapshot.len(),
                ovr_count, manifest_snap.page_group_keys.len());
            let change_counter = read_change_counter_from_cache(&cache, page_size);
            let old_sub_ppf = manifest_snap.sub_pages_per_frame;
            let use_seekable = old_sub_ppf > 0;

            // Group dirty pages by group ID
            let mut groups_dirty: HashMap<u64, Vec<u64>> = HashMap::new();
            for &page_num in &dirty_snapshot {
                let gid = manifest_snap.page_location(page_num)
                    .expect("page must have group assignment after new-page assignment")
                    .group_id;
                groups_dirty.entry(gid).or_default().push(page_num);
            }

            let mut uploads: Vec<(String, Vec<u8>)> = Vec::new();
            let mut new_subframe_overrides = manifest_snap.subframe_overrides.clone();
            let mut new_keys = manifest_snap.page_group_keys.clone();
            let mut new_frame_tables = manifest_snap.frame_tables.clone();
            let mut replaced_keys: Vec<String> = Vec::new();

            // Ensure override vectors are large enough
            while new_subframe_overrides.len() <= manifest_snap.group_pages.len() {
                new_subframe_overrides.push(HashMap::new());
            }

            for (&gid, dirty_pnums) in &groups_dirty {
                let pages_in_group = manifest_snap.group_page_nums(gid);
                let group_size = pages_in_group.len();
                let frame_table_ref = manifest_snap.frame_tables.get(gid as usize);
                let has_frame_table = use_seekable
                    && frame_table_ref.map(|ft| !ft.is_empty()).unwrap_or(false);

                if has_frame_table {
                    // Override path: encode only dirty frames
                    eprintln!("[sync:s3primary] gid={}: override path (frame_table has {} entries)", gid, frame_table_ref.map(|ft| ft.len()).unwrap_or(0));
                    let dirty_frames = manifest::dirty_frames_for_group(
                        dirty_pnums,
                        &pages_in_group,
                        frame_table_ref.expect("checked above"),
                        old_sub_ppf,
                    );

                    for &frame_idx in &dirty_frames {
                        let frame_start = frame_idx * old_sub_ppf as usize;
                        let frame_end = std::cmp::min(frame_start + old_sub_ppf as usize, group_size);
                        let mut frame_pages: Vec<(u64, Vec<u8>)> = Vec::new();

                        for pos in frame_start..frame_end {
                            if pos >= pages_in_group.len() { break; }
                            let pnum = pages_in_group[pos];
                            if pnum >= page_count { break; }
                            let mut buf = vec![0u8; page_size as usize];
                            if cache.read_page(pnum, &mut buf).is_ok() {
                                frame_pages.push((pnum, buf));
                            } else {
                                frame_pages.push((pnum, vec![0u8; page_size as usize]));
                            }
                        }

                        let override_key = s3.override_frame_key(gid, frame_idx, next_version);
                        let encoded = encode_override_frame(
                            &frame_pages,
                            page_size,
                            self.compression_level,
                            #[cfg(feature = "zstd")]
                            self.encoder_dict.as_ref(),
                            self.encryption_key.as_ref(),
                        )?;

                        // Track old override being replaced
                        if let Some(old_ov) = new_subframe_overrides
                            .get(gid as usize)
                            .and_then(|ovs| ovs.get(&frame_idx))
                        {
                            replaced_keys.push(old_ov.key.clone());
                        }

                        uploads.push((override_key.clone(), encoded.clone()));
                        while new_subframe_overrides.len() <= gid as usize {
                            new_subframe_overrides.push(HashMap::new());
                        }
                        new_subframe_overrides[gid as usize].insert(
                            frame_idx,
                            SubframeOverride {
                                key: override_key,
                                entry: FrameEntry { offset: 0, len: encoded.len() as u32 },
                            },
                        );
                    }
                } else {
                    // No frame table (legacy format or new group): full group rewrite
                    let existing_ovrs = new_subframe_overrides.get(gid as usize).map(|o| o.len()).unwrap_or(0);
                    if existing_ovrs > 0 {
                        eprintln!("[sync:s3primary] gid={}: FULL REWRITE draining {} overrides!", gid, existing_ovrs);
                    } else {
                        eprintln!("[sync:s3primary] gid={}: full rewrite (no overrides to drain)", gid);
                    }
                    let mut pages: Vec<Option<Vec<u8>>> = vec![None; group_size];
                    let mut need_s3_merge = false;

                    for (i, &pnum) in pages_in_group.iter().enumerate() {
                        if pnum >= page_count { break; }
                        if cache.is_present(pnum) {
                            let mut buf = vec![0u8; page_size as usize];
                            if cache.read_page(pnum, &mut buf).is_ok() {
                                pages[i] = Some(buf);
                            }
                        } else {
                            need_s3_merge = true;
                        }
                    }

                    if need_s3_merge {
                        if let Some(existing_key) = new_keys.get(gid as usize) {
                            if !existing_key.is_empty() {
                                if let Ok(Some(pg_data)) = s3.get_page_group(existing_key) {
                                    let existing_ft = manifest_snap.frame_tables.get(gid as usize);
                                    let has_ft = use_seekable
                                        && existing_ft.map(|ft| !ft.is_empty()).unwrap_or(false);
                                    if has_ft {
                                        let ft = existing_ft.expect("checked");
                                        if let Ok((_pc, _ps, bulk)) = decode_page_group_seekable_full(
                                            &pg_data, ft, page_size,
                                            pages_in_group.len() as u32, page_count, 0,
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
                                                    if end <= bulk.len() {
                                                        pages[j] = Some(bulk[start..end].to_vec());
                                                    }
                                                }
                                            }
                                        }
                                    } else if let Ok((_pc, _ps, existing)) = decode_page_group(
                                        &pg_data,
                                        #[cfg(feature = "zstd")]
                                        self.decoder_dict.as_ref(),
                                        self.encryption_key.as_ref(),
                                    ) {
                                        for (j, ep) in existing.into_iter().enumerate() {
                                            if j >= pages_in_group.len() { break; }
                                            if pages_in_group[j] >= page_count { break; }
                                            if pages[j].is_none() { pages[j] = Some(ep); }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    let key = s3.page_group_key(gid, next_version);
                    if use_seekable {
                        let (encoded, ft) = encode_page_group_seekable(
                            &pages, page_size, old_sub_ppf, self.compression_level,
                            #[cfg(feature = "zstd")] self.encoder_dict.as_ref(),
                            self.encryption_key.as_ref(),
                        )?;
                        uploads.push((key.clone(), encoded));
                        while new_frame_tables.len() <= gid as usize {
                            new_frame_tables.push(Vec::new());
                        }
                        new_frame_tables[gid as usize] = ft;
                    } else {
                        let encoded = encode_page_group(
                            &pages, page_size, self.compression_level,
                            #[cfg(feature = "zstd")] self.encoder_dict.as_ref(),
                            self.encryption_key.as_ref(),
                        )?;
                        uploads.push((key.clone(), encoded));
                    }

                    while new_keys.len() <= gid as usize { new_keys.push(String::new()); }
                    if let Some(old_key) = new_keys.get(gid as usize) {
                        if !old_key.is_empty() { replaced_keys.push(old_key.clone()); }
                    }
                    new_keys[gid as usize] = key;

                    // Clear overrides for this group (full rewrite supersedes them)
                    if let Some(group_ovs) = new_subframe_overrides.get_mut(gid as usize) {
                        for (_, ov) in group_ovs.drain() {
                            replaced_keys.push(ov.key);
                        }
                    }
                }
            }

            // Upload all overrides + page groups
            if !uploads.is_empty() {
                eprintln!("[sync:s3primary] uploading {} objects...", uploads.len());
                s3.put_page_groups(&uploads)?;
            }

            // Build and publish new manifest
            let old_manifest = manifest_snap;
            // Capture full page 0 for multiwriter catch-up
            let ps = *self.page_size.read();
            let db_header = if ps > 0 {
                let mut page0 = vec![0u8; ps as usize];
                if cache.read_page(0, &mut page0).is_ok() {
                    Some(page0)
                } else {
                    old_manifest.db_header.clone()
                }
            } else {
                None
            };

            let mut new_manifest = Manifest {
                version: next_version,
                change_counter,
                page_count: old_manifest.page_count,
                page_size: old_manifest.page_size,
                pages_per_group: ppg,
                page_group_keys: new_keys,
                interior_chunk_keys: old_manifest.interior_chunk_keys.clone(),
                index_chunk_keys: old_manifest.index_chunk_keys.clone(),
                frame_tables: new_frame_tables,
                sub_pages_per_frame: old_sub_ppf,
                subframe_overrides: new_subframe_overrides,
                strategy: old_manifest.strategy,
                group_pages: old_manifest.group_pages.clone(),
                btrees: old_manifest.btrees.clone(),
                page_index: HashMap::new(),
                btree_groups: HashMap::new(),
                page_to_tree_name: HashMap::new(),
                tree_name_to_groups: HashMap::new(),
                group_to_tree_name: HashMap::new(),
                db_header,
            };
            new_manifest.build_page_index();
            s3.put_manifest(&new_manifest)?;

            // Commit local state
            {
                let mut m = self.manifest.write();
                cache.set_group_pages(new_manifest.group_pages.clone());
                *m = new_manifest.clone();
            }
            {
                let mut dirty = self.dirty_page_nums.write();
                for &page_num in &dirty_snapshot {
                    dirty.remove(&page_num);
                }
            }
            self.dirty_since_sync = false;

            // Persist local manifest for crash recovery
            let local = manifest::LocalManifest {
                manifest: new_manifest,
                dirty_groups: Vec::new(),
            };
            local.persist(&cache.cache_dir)?;
            let _ = cache.persist_bitmap();

            // Async GC of replaced keys
            if self.gc_enabled && !replaced_keys.is_empty() {
                let gc_s3 = Arc::clone(&s3);
                let runtime = gc_s3.runtime.clone();
                runtime.spawn(async move {
                    gc_s3.delete_objects_async_owned(replaced_keys).await;
                });
            }

            eprintln!(
                "[sync:s3primary] committed v{} ({} dirty pages, {} uploads)",
                next_version, dirty_snapshot.len(), uploads.len(),
            );
            return Ok(());
        }

        // Durable sync path: full S3 upload. Only available with cloud feature.
        #[cfg(not(feature = "cloud"))]
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Durable sync requires cloud feature",
        ));

        #[cfg(feature = "cloud")]
        {

        let page_size = *self.page_size.read();

        // Phase Midway: detect VACUUM by checking schema cookie change.
        // VACUUM rewrites the entire database with new page numbers.
        // When detected, re-walk B-trees and rebuild group_pages from scratch.
        // Done before borrowing cache/s3 to avoid borrow conflicts with self mutation.
        {
            let cache_arc = self.cache.as_ref().expect("cache required").clone();
            let cache_ref = &*cache_arc;
            if dirty_snapshot.contains(&0) && self.manifest.read().strategy == GroupingStrategy::BTreeAware {
                let mut page0 = vec![0u8; page_size as usize];
                if cache_ref.read_page(0, &mut page0).is_ok() && page0.len() >= 28 {
                    let cookie = u32::from_be_bytes([page0[24], page0[25], page0[26], page0[27]]);
                    let mut do_rewalk = false;
                    if let Some(prev) = self.last_schema_cookie {
                        if cookie != prev {
                            let manifest = self.manifest.read();
                            let dirty_ratio = dirty_snapshot.len() as f64 / manifest.page_count.max(1) as f64;
                            if dirty_ratio > 0.5 {
                                eprintln!(
                                    "[sync] VACUUM detected: schema cookie {} -> {}, {:.0}% pages dirty, re-walking B-trees",
                                    prev, cookie, dirty_ratio * 100.0,
                                );
                                do_rewalk = true;
                            }
                        }
                    }
                    self.last_schema_cookie = Some(cookie);

                    if do_rewalk {
                        let mut manifest = self.manifest.write();
                        let page_count = manifest.page_count;

                        let walk = crate::btree_walker::walk_all_btrees(page_count, page_size, &|pnum| {
                            let mut buf = vec![0u8; page_size as usize];
                            cache_ref.read_page(pnum, &mut buf).ok()?;
                            Some(buf)
                        });

                        eprintln!(
                            "[sync] VACUUM re-walk: {} B-trees, {} unowned pages",
                            walk.btrees.len(), walk.unowned_pages.len(),
                        );

                        // Rebuild group_pages using import's packing logic
                        let mut new_group_pages: Vec<Vec<u64>> = Vec::new();
                        let mut btree_list: Vec<(&u64, &crate::btree_walker::BTreeEntry)> =
                            walk.btrees.iter().collect();
                        btree_list.sort_by(|a, b| b.1.pages.len().cmp(&a.1.pages.len()));

                        let threshold = std::cmp::max(ppg as usize / 4, 1);
                        let mut small_pages: Vec<u64> = Vec::new();

                        for (_, entry) in &btree_list {
                            let mut sorted_pages = entry.pages.clone();
                            sorted_pages.sort_unstable();
                            if sorted_pages.len() >= threshold {
                                for chunk in sorted_pages.chunks(ppg as usize) {
                                    new_group_pages.push(chunk.to_vec());
                                }
                            } else {
                                small_pages.extend_from_slice(&sorted_pages);
                            }
                        }

                        let mut sorted_unowned = walk.unowned_pages.clone();
                        sorted_unowned.sort_unstable();
                        small_pages.extend_from_slice(&sorted_unowned);

                        for chunk in small_pages.chunks(ppg as usize) {
                            new_group_pages.push(chunk.to_vec());
                        }

                        // Rebuild btrees manifest entries
                        let mut page_to_gid: HashMap<u64, u64> = HashMap::new();
                        for (gid, pages) in new_group_pages.iter().enumerate() {
                            for &p in pages {
                                page_to_gid.insert(p, gid as u64);
                            }
                        }

                        let mut new_btrees: HashMap<u64, BTreeManifestEntry> = HashMap::new();
                        for (&root_page, entry) in &walk.btrees {
                            let mut gid_set: HashSet<u64> = HashSet::new();
                            for &p in &entry.pages {
                                if let Some(&gid) = page_to_gid.get(&p) {
                                    gid_set.insert(gid);
                                }
                            }
                            let mut gids: Vec<u64> = gid_set.into_iter().collect();
                            gids.sort_unstable();
                            new_btrees.insert(root_page, BTreeManifestEntry {
                                name: entry.name.clone(),
                                obj_type: entry.obj_type.clone(),
                                group_ids: gids,
                            });
                        }

                        eprintln!(
                            "[sync] VACUUM: repacked {} pages into {} groups (was {} groups)",
                            page_count, new_group_pages.len(), manifest.group_pages.len(),
                        );

                        // Collect old keys for GC
                        let mut vacuum_keys: Vec<String> = manifest.page_group_keys.iter()
                            .filter(|k| !k.is_empty()).cloned().collect();
                        vacuum_keys.extend(manifest.interior_chunk_keys.values().cloned());
                        vacuum_keys.extend(manifest.index_chunk_keys.values().cloned());

                        // Replace manifest group state
                        manifest.group_pages = new_group_pages;
                        manifest.btrees = new_btrees;
                        manifest.page_group_keys = Vec::new();
                        manifest.interior_chunk_keys.clear();
                        manifest.index_chunk_keys.clear();
                        manifest.frame_tables.clear();
                        // Phase Drift: clear overrides on VACUUM
                        for overrides in &manifest.subframe_overrides {
                            for ovr in overrides.values() {
                                vacuum_keys.push(ovr.key.clone());
                            }
                        }
                        manifest.subframe_overrides.clear();
                        manifest.build_page_index();

                        self.vacuum_replaced_keys = Some(vacuum_keys);
                    }
                }
            }
        }

        // Clone Arcs to avoid borrow conflicts with self.vacuum_replaced_keys
        let s3 = self.s3.as_ref().expect("s3 required").clone();
        let cache = self.cache.as_ref().expect("cache required").clone();

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

        // Dual counter (Phase Borodino):
        // - version: monotonic +1, for S3 key uniqueness
        // - change_counter: SQLite file change counter, for walrust WAL replay
        let next_version = self.manifest.read().version + 1;
        let change_counter = read_change_counter_from_cache(&cache, page_size);
        let mut uploads: Vec<(String, Vec<u8>)> = Vec::new();
        let mut new_keys = self.manifest.read().page_group_keys.clone();
        // Track old keys being replaced (for post-checkpoint GC)
        let mut replaced_keys: Vec<String> = Vec::new();

        // Consume VACUUM old keys (populated during VACUUM detection above)
        if let Some(vacuum_keys) = self.vacuum_replaced_keys.take() {
            replaced_keys.extend(vacuum_keys);
        }

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
                        return Err(io::Error::new(io::ErrorKind::Other,
                            format!("cache.read_page({}) failed during interior collection: {}", pnum, e)));
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
                        return Err(io::Error::new(io::ErrorKind::Other,
                            format!("cache.read_page({}) failed during index leaf collection: {}", pnum, e)));
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
        // Capture full page 0 for multiwriter catch-up
        let ps = *self.page_size.read();
        let db_header = if ps > 0 {
            let mut page0 = vec![0u8; ps as usize];
            if cache.read_page(0, &mut page0).is_ok() {
                Some(page0)
            } else {
                old_manifest.db_header.clone()
            }
        } else {
            None
        };

        let mut new_manifest = Manifest {
            version: next_version,
            change_counter,
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
            subframe_overrides: {
                let mut ovs = old_manifest.subframe_overrides.clone();
                for &gid in groups_dirty.keys() {
                    if let Some(group_ovs) = ovs.get_mut(gid as usize) {
                        for (_, ov) in group_ovs.drain() {
                            replaced_keys.push(ov.key);
                        }
                    }
                }
                ovs
            },
            // Carry forward strategy + B-tree-aware fields
            strategy: old_manifest.strategy,
            group_pages: old_manifest.group_pages.clone(),
            btrees: old_manifest.btrees.clone(),
            page_index: HashMap::new(),
            btree_groups: HashMap::new(),
            page_to_tree_name: HashMap::new(),
            tree_name_to_groups: HashMap::new(),
            group_to_tree_name: HashMap::new(),
            db_header,
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
        self.dirty_since_sync = false;

        // Persist bitmap
        let _ = cache.persist_bitmap();

        // Phase Marathon: truncate cache file if it's larger than current page_count.
        // After VACUUM, page_count decreases but the sparse cache file retains its old size.
        {
            let current_page_count = self.manifest.read().page_count;
            let ps = *self.page_size.read() as u64;
            if current_page_count > 0 && ps > 0 {
                let target_size = current_page_count * ps;
                let file = cache.cache_file.write();
                if let Ok(meta) = file.metadata() {
                    if meta.len() > target_size {
                        if let Err(e) = file.set_len(target_size) {
                            eprintln!("[sync] WARN: cache truncation failed: {}", e);
                        } else {
                            eprintln!("[sync] cache truncated: {}B -> {}B ({} pages)",
                                meta.len(), target_size, current_page_count);
                        }
                    }
                }
            }
        }

        // Post-checkpoint GC: delete old page group/interior chunk versions asynchronously.
        // Phase Thermopylae: fire-and-forget on tokio runtime so checkpoint doesn't block on deletes.
        if self.gc_enabled && !replaced_keys.is_empty() {
            let gc_s3 = Arc::clone(self.s3.as_ref().expect("s3 client required"));
            let runtime = gc_s3.runtime.clone();
            eprintln!("[gc] spawning async delete of {} replaced S3 objects", replaced_keys.len());
            runtime.spawn(async move {
                gc_s3.delete_objects_async_owned(replaced_keys).await;
            });
        }

        // Phase Somme-e: GC old WAL segments after checkpoint.
        // WAL segments with txid <= change_counter are in the page groups (redundant).
        // Uses change_counter (not version) because walrust txids are file change counters.
        #[cfg(feature = "wal")]
        if self.gc_enabled {
            let gc_s3 = self.s3.as_ref().expect("s3 required").clone();
            let wal_prefix = format!("{}/wal/", gc_s3.prefix);
            let version = change_counter;
            let runtime = gc_s3.runtime.clone();
            runtime.spawn(async move {
                // List all WAL segment keys under the wal prefix
                let keys = match gc_s3.list_all_keys_with_prefix(&wal_prefix).await {
                    Ok(k) => k,
                    Err(e) => {
                        eprintln!("[wal-gc] ERROR listing WAL segments: {}", e);
                        return;
                    }
                };
                // Filter to .hadbp files with max_txid <= version
                let to_delete: Vec<String> = keys.into_iter()
                    .filter(|k| {
                        k.ends_with(".hadbp")
                            && k.rsplit('/').next()
                                .and_then(|f| f.strip_suffix(".hadbp"))
                                .and_then(|f| f.split('-').last())
                                .and_then(|hex| u64::from_str_radix(hex, 16).ok())
                                .map(|max_txid| max_txid <= version)
                                .unwrap_or(false)
                    })
                    .collect();
                if !to_delete.is_empty() {
                    eprintln!("[wal-gc] deleting {} WAL segments with txid <= {}", to_delete.len(), version);
                    gc_s3.delete_objects_async_owned(to_delete).await;
                }
            });
        }

        // Phase Stalingrad-e: evict data tier after successful checkpoint upload.
        if self.evict_on_checkpoint {
            if let Some(cache) = &self.cache {
                let mut tracker = cache.tracker.lock();
                let to_evict: Vec<SubChunkId> = tracker.present.iter()
                    .filter(|id| {
                        let t = tracker.tiers.get(id).copied().unwrap_or(cache_tracking::SubChunkTier::Data);
                        t == cache_tracking::SubChunkTier::Data
                    })
                    .copied()
                    .collect();
                let scbs = tracker.sub_chunk_byte_size;
                let count = to_evict.len();
                for id in &to_evict {
                    tracker.remove(*id);
                }
                drop(tracker);
                for id in &to_evict {
                    let page_nums = cache.sub_chunk_page_nums(*id);
                    cache.clear_pages_from_disk(&page_nums);
                }
                cache.stat_evictions.fetch_add(count as u64, Ordering::Relaxed);
                cache.stat_bytes_evicted.fetch_add(count as u64 * scbs, Ordering::Relaxed);
                eprintln!("[sync] evict_on_checkpoint: evicted {} data sub-chunks", count);
            }
        }

        // Phase Gallipoli: persist local manifest (no dirty groups in Durable mode)
        if let Some(cache) = &self.cache {
            let m = self.manifest.read().clone();
            let local = manifest::LocalManifest { manifest: m, dirty_groups: Vec::new() };
            local.persist(&cache.cache_dir).map_err(|e| {
                io::Error::new(io::ErrorKind::Other,
                    format!("local manifest persist failed after S3 sync: {}", e))
            })?;
        }

        Ok(())
        } // end #[cfg(feature = "cloud")] durable sync block
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

        // Phase Zenith-c: detect transaction rollback.
        // If lock downgrades from EXCLUSIVE/RESERVED and we have unsynced dirty
        // pages, the transaction was rolled back. Clear dirty pages and evict
        // them from the disk cache so subsequent reads re-fetch from the source
        // of truth (S3 or local pg/).
        if (current == LockKind::Exclusive || current == LockKind::Reserved)
            && (lock == LockKind::Shared || lock == LockKind::None)
            && self.dirty_since_sync
        {
            let mut dirty = self.dirty_page_nums.write();
            if !dirty.is_empty() {
                let stale_pages: Vec<u64> = dirty.iter().copied().collect();
                eprintln!(
                    "[turbolite] lock downgrade without sync: clearing {} dirty pages (transaction rollback)",
                    stale_pages.len(),
                );
                dirty.clear();
                drop(dirty);
                // Evict stale pages from disk cache so reads go back to source.
                if let Some(cache) = &self.cache {
                    cache.clear_pages_from_disk(&stale_pages);
                }
            }
            self.dirty_since_sync = false;
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

