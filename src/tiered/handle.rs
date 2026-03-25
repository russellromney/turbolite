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
    manifest: RwLock<Manifest>,
    /// Dirty pages buffered in memory: page_num → raw (uncompressed) data
    dirty_pages: RwLock<HashMap<u64, Vec<u8>>>,
    /// Page group IDs that were locally checkpointed but not yet synced to S3.
    /// Populated during local-checkpoint-only mode; drained on the next real sync.
    s3_dirty_groups: Mutex<HashSet<u64>>,
    page_size: RwLock<u32>,
    pages_per_group: u32,
    compression_level: i32,
    read_only: bool,
    /// Consecutive cache misses (for fraction-based prefetch).
    consecutive_misses: u8,
    /// Fraction-based prefetch schedule.
    prefetch_hops: Vec<f32>,
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
        manifest: Manifest,
        db_path: PathBuf,
        pages_per_group: u32,
        compression_level: i32,
        read_only: bool,
        prefetch_hops: Vec<f32>,
        prefetch_pool: Option<Arc<PrefetchPool>>,
        gc_enabled: bool,
        eager_index_load: bool,
        #[cfg(feature = "zstd")] dictionary: Option<&[u8]>,
        encryption_key: Option<[u8; 32]>,
    ) -> Self {
        let page_size = manifest.page_size;

        // Eagerly fetch page group 0 (contains schema + root page, hit on every query)
        if manifest.page_count > 0 && cache.group_state(0) != GroupState::Present {
            if let Some(key) = manifest.page_group_keys.first() {
                if !key.is_empty() {
                    if cache.try_claim_group(0) {
                        if let Ok(Some(pg_data)) = s3.get_page_group(key) {
                            let ft = manifest.frame_tables.first().map(|v| v.as_slice());
                            let gp0 = manifest.group_pages.get(0)
                                .expect("group 0 must exist in group_pages");
                            let _ = Self::decode_and_cache_group_static(
                                &cache,
                                &pg_data,
                                gp0,
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

        Self {
            s3: Some(s3),
            cache: Some(cache),
            manifest: RwLock::new(manifest),
            dirty_pages: RwLock::new(HashMap::new()),
            s3_dirty_groups: Mutex::new(HashSet::new()),
            page_size: RwLock::new(page_size),
            pages_per_group,
            compression_level,
            read_only,
            consecutive_misses: 0,
            prefetch_hops,
            prefetch_pool,
            gc_enabled,
            encryption_key,
            #[cfg(feature = "zstd")]
            encoder_dict,
            #[cfg(feature = "zstd")]
            decoder_dict,
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
            manifest: RwLock::new(Manifest::empty()),
            dirty_pages: RwLock::new(HashMap::new()),
            s3_dirty_groups: Mutex::new(HashSet::new()),
            page_size: RwLock::new(0),
            pages_per_group: DEFAULT_PAGES_PER_GROUP,
            compression_level: 0,
            read_only: false,
            consecutive_misses: 0,
            prefetch_hops: vec![0.33, 0.33],
            prefetch_pool: None,
            gc_enabled: false,
            encryption_key,
            #[cfg(feature = "zstd")]
            encoder_dict: None,
            #[cfg(feature = "zstd")]
            decoder_dict: None,
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
        let group_page_nums = manifest.group_pages.get(gid as usize)
            .expect("group must exist in group_pages");
        let group_page_nums = group_page_nums.clone();
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

    /// Demand-driven, fraction-based prefetch of B-tree sibling groups.
    ///
    /// When we miss on group G, prefetch a FRACTION of sibling groups from
    /// the same B-tree. The fraction escalates with consecutive misses:
    ///   miss 1 -> hops[0] (33%) of siblings
    ///   miss 2 -> hops[1] (33%) more
    ///   miss 3+ -> all remaining
    ///
    /// Each sibling is claimed (CAS None->Fetching) before submission to
    /// guarantee at most one download per group.
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

        let siblings = match manifest.btree_groups.get(&current_gid) {
            Some(s) => s,
            None => return,
        };

        // Compute how many siblings to prefetch (excluding self and already-fetching)
        let eligible: Vec<u64> = siblings.iter()
            .copied()
            .filter(|&gid| gid != current_gid && cache.group_state(gid) == GroupState::None)
            .collect();

        if eligible.is_empty() {
            return;
        }

        // Fraction-based escalation: consecutive_misses indexes into prefetch_hops
        let hop_idx = self.consecutive_misses.saturating_sub(1) as usize;
        let fraction = if hop_idx < self.prefetch_hops.len() {
            self.prefetch_hops[hop_idx]
        } else {
            1.0 // beyond all hops: fetch everything
        };
        let max_submit = ((eligible.len() as f32) * fraction).ceil() as usize;

        let ps = manifest.page_size;
        let sub_ppf = manifest.sub_pages_per_frame;
        let mut submitted = 0usize;

        for &gid in &eligible {
            if submitted >= max_submit {
                break;
            }
            // Claim before submit: guarantees at most one download per group
            if cache.try_claim_group(gid) {
                if let Some(key) = manifest.page_group_keys.get(gid as usize) {
                    if !key.is_empty() {
                        let ft = manifest.frame_tables.get(gid as usize).cloned().unwrap_or_default();
                        let gp = manifest.group_pages.get(gid as usize)
                            .expect("group must exist in group_pages")
                            .clone();
                        if pool.submit(gid, key.clone(), ft, ps, sub_ppf, gp) {
                            submitted += 1;
                        } else {
                            // Submit failed (channel closed), reset state
                            let states = cache.group_states.lock();
                            if let Some(s) = states.get(gid as usize) {
                                s.store(GroupState::None as u8, Ordering::Release);
                            }
                            cache.group_condvar.notify_all();
                        }
                    } else {
                        // Empty key, reset state
                        let states = cache.group_states.lock();
                        if let Some(s) = states.get(gid as usize) {
                            s.store(GroupState::None as u8, Ordering::Release);
                        }
                        cache.group_condvar.notify_all();
                    }
                } else {
                    // No key for gid, reset state
                    let states = cache.group_states.lock();
                    if let Some(s) = states.get(gid as usize) {
                        s.store(GroupState::None as u8, Ordering::Release);
                    }
                    cache.group_condvar.notify_all();
                }
            }
        }

        if std::env::var("BENCH_VERBOSE").is_ok() {
            eprintln!(
                "  [prefetch] gid={} misses={} fraction={:.0}% eligible={} submitted={}",
                current_gid, self.consecutive_misses, fraction * 100.0,
                eligible.len(), submitted,
            );
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

        // 1. Check dirty pages first (new pages may not be in manifest yet)
        {
            let dirty = self.dirty_pages.read();
            if let Some(raw) = dirty.get(&page_num) {
                self.consecutive_misses = 0;
                Self::copy_raw_into_buf(raw, buf, offset, page_size);
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
        //    - New pages (not in manifest) are always in dirty_pages (returned above)
        //    - Pages beyond page_count are zero-filled (returned above)
        //    - sync() assigns new pages to groups before clearing dirty_pages
        let manifest_ref = self.manifest.read();
        let loc = manifest_ref.page_location(page_num)
            .expect("page within manifest bounds must have group assignment");
        drop(manifest_ref);
        let gid = loc.group_id;
        let page_in_group_idx = loc.index as usize;

        // 4. Check page bitmap (cache hit = direct pread, no decompression)
        let cache_arc = Arc::clone(self.cache.as_ref().expect("disk cache required"));
        let cache = cache_arc.as_ref();
        if cache.is_present(page_num) {
            self.consecutive_misses = 0;
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
            self.consecutive_misses = 0;
            return Ok(());
        }

        // 5b. Inline range GET: fastest path, serves the page immediately
        let ft = manifest.frame_tables.get(gid as usize)
            .expect("group must have seekable frame table");
        assert!(!ft.is_empty(), "frame table must not be empty for gid={}", gid);
        let sub_ppg = manifest.sub_pages_per_frame;
        assert!(sub_ppg > 0, "sub_pages_per_frame must be > 0");
        let page_in_group = page_in_group_idx;
        let frame_idx = page_in_group / sub_ppg as usize;
        assert!(frame_idx < ft.len(),
            "frame_idx {} must be < frame_table.len() {} for gid={} page={}",
            frame_idx, ft.len(), gid, page_num);

        let key = manifest.page_group_keys.get(gid as usize)
            .expect("gid must be valid index into page_group_keys");

        let entry = &ft[frame_idx];
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
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("sub-chunk too small: need {}..{} but got {} bytes (page={} gid={})",
                            src_start, src_end, decompressed.len(), page_num, gid),
                    ));
                }

                // Write sub-chunk pages to cache
                let frame_start_idx = frame_idx * sub_ppg as usize;
                let gp = manifest.group_pages.get(gid as usize)
                    .expect("group must exist in group_pages");
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

                if std::env::var("BENCH_VERBOSE").is_ok() {
                    eprintln!(
                        "  [range-get] page={} gid={} frame={}/{} s3={}ms decode={}ms total={}ms ({:.1}KB)",
                        page_num, gid, frame_idx, ft.len(), s3_ms, decode_ms,
                        miss_start.elapsed().as_millis(),
                        compressed_frame.len() as f64 / 1024.0,
                    );
                }

                // 5c. AFTER serving the page: submit background cache warming.
                // Range GET already returned the data we need. These are non-blocking
                // channel sends that warm the cache for future reads.
                self.consecutive_misses = self.consecutive_misses.saturating_add(1);
                if cache.try_claim_group(gid) {
                    let mut submitted = false;
                    if let Some(pool) = &self.prefetch_pool {
                        if !key.is_empty() {
                            let gp_clone = gp.clone();
                            submitted = pool.submit(gid, key.clone(), ft.to_vec(), manifest.page_size, sub_ppg, gp_clone);
                        }
                    }
                    if !submitted {
                        // Reset state: no pool or empty key, don't leave stuck in Fetching
                        let states = cache.group_states.lock();
                        if let Some(s) = states.get(gid as usize) {
                            s.store(GroupState::None as u8, Ordering::Release);
                        }
                        cache.group_condvar.notify_all();
                    }
                    // Demand-driven prefetch: fraction of B-tree siblings
                    self.trigger_prefetch(gid, &manifest, &cache_arc, &s3_arc);
                }
                self.consecutive_misses = 0;

                return Ok(());
            }
            Ok(None) => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("S3 object not found for page group gid={} key={}", gid, key),
                ));
            }
            Err(e) => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("S3 range GET failed for gid={} frame={}: {}", gid, frame_idx, e),
                ));
            }
        }
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

        // Buffer raw page as dirty (compression happens at sync time, whole-group)
        let mut dirty = self.dirty_pages.write();
        dirty.insert(page_num, buf.to_vec());

        // Also write to local cache for fast reads
        if let Some(cache) = &self.cache {
            let _ = cache.write_page(page_num, buf);
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

        // Clone dirty pages — keep them readable during upload.
        let dirty_snapshot: HashMap<u64, Vec<u8>> = {
            let dirty = self.dirty_pages.read();
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
        if LOCAL_CHECKPOINT_ONLY.load(Ordering::Acquire) {
            let cache = self.disk_cache();
            let mut manifest = self.manifest.write();
            let mut pending = self.s3_dirty_groups.lock().unwrap();
            let mut interior_found = 0usize;
            // Assign new pages (not in page_index) to new groups before recording dirty groups
            let unassigned: Vec<u64> = dirty_snapshot.keys()
                .filter(|&&pn| manifest.page_location(pn).is_none())
                .copied()
                .collect();
            if !unassigned.is_empty() {
                Self::assign_new_pages_to_groups(&mut manifest, &unassigned, ppg);
            }
            for (&page_num, data) in &dirty_snapshot {
                let loc = manifest.page_location(page_num)
                    .expect("page must have group assignment after new-page assignment");
                pending.insert(loc.group_id);
                // Track interior pages so final sync builds correct interior chunks
                let type_byte = if page_num == 0 { data.get(100) } else { data.get(0) };
                if let Some(&b) = type_byte {
                    if b == 0x05 || b == 0x02 {
                        cache.mark_interior_group(loc.group_id, page_num, loc.index);
                        interior_found += 1;
                    }
                }
            }
            drop(manifest);
            let n = dirty_snapshot.len();
            let total_interior = cache.interior_pages.lock().len();
            // Free in-memory dirty pages — they're already on local disk
            self.dirty_pages.write().clear();
            eprintln!("[sync] local-only checkpoint: {} pages, {} interior this batch, {} interior total, {} groups pending S3", n, interior_found, total_interior, pending.len());
            return Ok(());
        }

        let s3 = self.s3();
        let cache = self.disk_cache();

        // Assign new pages (not in page_index) to new groups before grouping
        {
            let mut manifest = self.manifest.write();
            let unassigned: Vec<u64> = dirty_snapshot.keys()
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
        for &page_num in dirty_snapshot.keys() {
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

        // Skip page groups where ALL dirty pages are interior (0x05/0x02) or index leaf (0x0A).
        // Those pages are served from their respective bundles — no need to re-upload the data group.
        let mut skipped_groups = 0usize;
        let groups_needing_upload: Vec<u64> = groups_dirty.keys()
            .filter(|&&gid| {
                let dirty_pages = &groups_dirty[&gid];
                if dirty_pages.is_empty() {
                    return true; // s3_dirty_groups entry — must upload (stale from prior local checkpoint)
                }
                let all_bundle_pages = dirty_pages.iter().all(|&pnum| {
                    if let Some(data) = dirty_snapshot.get(&pnum) {
                        let hdr_off = if pnum == 0 { 100 } else { 0 };
                        let type_byte = data.get(hdr_off).copied();
                        match type_byte {
                            Some(0x05) | Some(0x02) => true,
                            Some(0x0A) => is_valid_btree_page(data, hdr_off),
                            _ => false,
                        }
                    } else {
                        false // can't determine type — upload to be safe
                    }
                });
                if all_bundle_pages {
                    skipped_groups += 1;
                    false // skip: all dirty pages are in bundles
                } else {
                    true // has table leaf or other data pages — must upload
                }
            })
            .copied()
            .collect();
        if skipped_groups > 0 {
            eprintln!(
                "[sync] skipping {} page groups (all dirty pages are interior/index, served from bundles)",
                skipped_groups,
            );
        }

        // For each dirty page group that needs uploading: read all raw pages, encode as whole-group compressed
        for &gid in &groups_needing_upload {
            let mut need_s3_merge = false;

            // Get explicit page list from B-tree-aware manifest (Phase Midway)
            let pages_in_group = manifest_snap.group_pages.get(gid as usize)
                .expect("group must exist in group_pages")
                .clone();
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
                            if let Ok((_pc, _ps, existing_pages)) = decode_page_group(
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

            // Encode as whole-group compressed blob
            let encoded = encode_page_group(
                &pages,
                page_size,
                self.compression_level,
                #[cfg(feature = "zstd")]
                self.encoder_dict.as_ref(),
                self.encryption_key.as_ref(),
            )?;
            let key = s3.page_group_key(gid, next_version);
            uploads.push((key.clone(), encoded));

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
            // Collect interior pages from dirty snapshot (fastest — already in memory)
            let mut dirty_interior_count = 0usize;
            for (&pnum, data) in &dirty_snapshot {
                let type_byte = if pnum == 0 { data.get(100) } else { data.get(0) };
                if let Some(&b) = type_byte {
                    if b == 0x05 || b == 0x02 {
                        all_interior.insert(pnum, data.clone());
                        if let Some(loc) = manifest_snap.page_location(pnum) {
                            cache.mark_interior_group(loc.group_id, pnum, loc.index);
                        }
                        dirty_interior_count += 1;
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
                } else if dirty_snapshot.contains_key(&pnum) || all_interior.contains_key(&pnum) {
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

        // Determine which chunks are dirty (have at least one dirty interior page)
        let dirty_chunk_ids: HashSet<u32> = dirty_snapshot.keys()
            .filter(|&&pnum| {
                let type_byte = if pnum == 0 { dirty_snapshot[&pnum].get(100) } else { dirty_snapshot[&pnum].get(0) };
                type_byte.map_or(false, |&b| b == 0x05 || b == 0x02)
            })
            .map(|&pnum| (pnum / bundle_chunk_range(page_size)) as u32)
            .collect();

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
            let mut dirty_index_count = 0usize;
            for (&pnum, data) in &dirty_snapshot {
                let hdr_off = if pnum == 0 { 100 } else { 0 };
                let type_byte = data.get(hdr_off);
                if let Some(&b) = type_byte {
                    if b == 0x0A && is_valid_btree_page(data, hdr_off) {
                        all_index_leaves.insert(pnum, data.clone());
                        dirty_index_count += 1;
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
                    if pnum >= page_count || all_index_leaves.contains_key(&pnum) || dirty_snapshot.contains_key(&pnum) {
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

        // Group index leaf pages by chunk_id
        let mut index_chunks: HashMap<u32, Vec<(u64, Vec<u8>)>> = HashMap::new();
        for (pnum, data) in all_index_leaves {
            let chunk_id = (pnum / bundle_chunk_range(page_size)) as u32;
            index_chunks.entry(chunk_id).or_default().push((pnum, data));
        }
        for pages in index_chunks.values_mut() {
            pages.sort_by_key(|(pnum, _)| *pnum);
        }

        // Determine dirty index chunks
        let dirty_index_chunk_ids: HashSet<u32> = dirty_snapshot.keys()
            .filter(|&&pnum| {
                let data = &dirty_snapshot[&pnum];
                let hdr_off = if pnum == 0 { 100 } else { 0 };
                let type_byte = data.get(hdr_off);
                type_byte.map_or(false, |&b| b == 0x0A && is_valid_btree_page(data, hdr_off))
            })
            .map(|&pnum| (pnum / bundle_chunk_range(page_size)) as u32)
            .collect();

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
            // VFS sync path uses legacy single-frame encoding for now
            frame_tables: Vec::new(),
            sub_pages_per_frame: 0,
            // Carry forward B-tree-aware fields (Phase Midway)
            group_pages: old_manifest.group_pages.clone(),
            btrees: old_manifest.btrees.clone(),
            page_index: HashMap::new(),
            btree_groups: HashMap::new(),
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
            let mut dirty = self.dirty_pages.write();
            for page_num in dirty_snapshot.keys() {
                dirty.remove(page_num);
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
        let mut dirty = self.dirty_pages.write();
        dirty.retain(|&pn, _| pn < new_page_count);

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

