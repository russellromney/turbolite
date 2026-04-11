use super::*;

// ===== CacheIndex (compressed cache page offset tracking) =====

/// Entry in the compressed cache index: where a page lives in the cache file.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) struct CacheIndexEntry {
    /// Byte offset in the compressed cache file.
    pub(crate) offset: u64,
    /// Compressed (and optionally encrypted) length in bytes.
    pub(crate) compressed_len: u32,
}

/// Maps page numbers to their location in the compressed cache file.
/// When cache_compression is enabled, pages are zstd-compressed (then optionally
/// CTR-encrypted) and appended sequentially. The index tracks each page's offset
/// and compressed length so reads can pread the exact byte range.
///
/// Persisted as JSON alongside the bitmap for crash recovery.
pub(crate) struct CacheIndex {
    /// page_num -> (offset, compressed_len)
    pub(crate) entries: HashMap<u64, CacheIndexEntry>,
    /// Next append offset in the compressed cache file.
    pub(crate) next_offset: u64,
    /// Path for persistence.
    pub(crate) path: PathBuf,
}

impl CacheIndex {
    pub(crate) fn new(path: PathBuf) -> Self {
        match Self::load_from_disk(&path) {
            Some(idx) => idx,
            None => Self {
                entries: HashMap::new(),
                next_offset: 0,
                path,
            },
        }
    }

    /// Look up a page's location in the compressed cache.
    pub(crate) fn get(&self, page_num: u64) -> Option<&CacheIndexEntry> {
        self.entries.get(&page_num)
    }

    /// Record a page written at the current append offset.
    /// Returns the offset where the page was written.
    pub(crate) fn insert(&mut self, page_num: u64, compressed_len: u32) -> u64 {
        let offset = self.next_offset;
        self.entries.insert(page_num, CacheIndexEntry { offset, compressed_len });
        self.next_offset = offset + compressed_len as u64;
        offset
    }

    /// Record a page at a specific offset (for bulk writes where offset is pre-computed).
    pub(crate) fn insert_at(&mut self, page_num: u64, offset: u64, compressed_len: u32) {
        self.entries.insert(page_num, CacheIndexEntry { offset, compressed_len });
        let end = offset + compressed_len as u64;
        if end > self.next_offset {
            self.next_offset = end;
        }
    }

    /// Check if a page is in the index.
    pub(crate) fn contains(&self, page_num: u64) -> bool {
        self.entries.contains_key(&page_num)
    }

    /// Remove a page from the index.
    pub(crate) fn remove(&mut self, page_num: u64) {
        self.entries.remove(&page_num);
    }

    /// Remove all pages from the index and reset append offset.
    pub(crate) fn clear(&mut self) {
        self.entries.clear();
        self.next_offset = 0;
    }

    /// Persist index to disk (atomic tmp+rename).
    pub(crate) fn persist(&self) -> io::Result<()> {
        let data = serde_json::to_vec(&PersistableCacheIndex {
            entries: &self.entries,
            next_offset: self.next_offset,
        }).map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("serialize cache index: {}", e))
        })?;
        let tmp = self.path.with_extension("tmp");
        fs::write(&tmp, &data)?;
        fs::rename(&tmp, &self.path)?;
        Ok(())
    }

    /// Load index from disk. Returns None if missing or corrupt.
    fn load_from_disk(path: &Path) -> Option<Self> {
        let data = fs::read(path).ok()?;
        let parsed: LoadableCacheIndex = serde_json::from_slice(&data).ok()?;
        Some(Self {
            entries: parsed.entries,
            next_offset: parsed.next_offset,
            path: path.to_path_buf(),
        })
    }
}

#[derive(Serialize)]
struct PersistableCacheIndex<'a> {
    entries: &'a HashMap<u64, CacheIndexEntry>,
    next_offset: u64,
}

#[derive(Deserialize)]
struct LoadableCacheIndex {
    entries: HashMap<u64, CacheIndexEntry>,
    next_offset: u64,
}

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
    /// pread/pwrite are thread-safe on Unix, no lock needed for I/O.
    /// Only set_len (extending) is serialized via cache_file_extend.
    pub(crate) cache_file: File,
    /// Tracked file length to avoid metadata() syscall on every write.
    pub(crate) cache_file_len: std::sync::atomic::AtomicU64,
    /// Serializes set_len calls (rare: only when DB grows beyond current cache file).
    pub(crate) cache_file_extend: parking_lot::Mutex<()>,
    /// In-memory page cache: flat array of page data indexed by page_num.
    /// AtomicPtr per page: null = not cached, non-null = pointer to page_size bytes.
    /// Zero-lock reads: just atomic load + memcpy.
    pub(crate) mem_cache: Option<Vec<std::sync::atomic::AtomicPtr<u8>>>,
    /// Memory budget in bytes (0 = disabled). User-controlled via TurboliteConfig.
    pub(crate) mem_cache_budget: u64,
    /// Current usage in bytes.
    pub(crate) mem_cache_bytes: std::sync::atomic::AtomicU64,
    /// Sub-chunk-level tracking: which sub-chunks are cached + eviction tiers.
    pub(crate) tracker: parking_lot::Mutex<SubChunkTracker>,
    /// Page bitmap: AtomicU8-backed, lock-free for is_present/mark_present/clear.
    /// RwLock only protects resize (Vec reallocation). Read lock is ~free.
    pub(crate) bitmap: parking_lot::RwLock<PageBitmap>,
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
    /// When true, pages are zstd-compressed (and optionally CTR-encrypted) in the cache file.
    /// The CacheIndex tracks each page's offset and compressed length.
    pub(crate) cache_compression: bool,
    /// Zstd compression level for cache pages (only used when cache_compression is true).
    pub(crate) cache_compression_level: i32,
    /// Index mapping page_num -> (offset, compressed_len) in the compressed cache file.
    /// Only populated when cache_compression is true.
    pub(crate) cache_index: parking_lot::Mutex<CacheIndex>,
    /// Raw zstd dictionary bytes for cache compression/decompression.
    /// Shared via Arc so DiskCache (which is Arc<DiskCache>) can be used from multiple threads.
    /// EncoderDictionary/DecoderDictionary are created on each use (cheap, same pattern as PrefetchPool).
    #[cfg(feature = "zstd")]
    pub(crate) dictionary: Option<Arc<Vec<u8>>>,

    // ── Phase Stalingrad-c: cache stats counters ──
    /// Cache hits (page was in bitmap/cache, served from local disk).
    pub(crate) stat_hits: AtomicU64,
    /// Cache misses (page not cached, triggered S3 fetch).
    pub(crate) stat_misses: AtomicU64,
    /// Sub-chunks evicted (by budget enforcement, TTL, or manual eviction).
    pub(crate) stat_evictions: AtomicU64,
    /// Bytes evicted from cache.
    pub(crate) stat_bytes_evicted: AtomicU64,
    /// Peak cache size observed (bytes). Updated on every mark_present.
    pub(crate) stat_peak_cache_bytes: AtomicU64,
    /// Sub-chunks evicted in the last between-query eviction pass.
    /// Used for churn detection (>50% of cache evicted = high churn).
    pub(crate) stat_last_eviction_count: AtomicU64,
}

/// Counter for lazy eviction (every 64 group fetches).
pub(crate) static EVICTION_COUNTER: AtomicU64 = AtomicU64::new(0);

impl DiskCache {
    pub(crate) fn new(cache_dir: &Path, ttl_secs: u64, pages_per_group: u32, sub_pages_per_frame: u32, page_size: u32, page_count: u64, encryption_key: Option<[u8; 32]>, group_pages: Vec<Vec<u64>>) -> io::Result<Self> {
        Self::new_with_compression(
            cache_dir, ttl_secs, pages_per_group, sub_pages_per_frame,
            page_size, page_count, encryption_key, group_pages,
            false, 3,
            #[cfg(feature = "zstd")]
            None,
            0, // mem_cache_budget: disabled by default
        )
    }

    pub(crate) fn new_with_compression(
        cache_dir: &Path, ttl_secs: u64, pages_per_group: u32, sub_pages_per_frame: u32,
        page_size: u32, page_count: u64, encryption_key: Option<[u8; 32]>, group_pages: Vec<Vec<u64>>,
        cache_compression: bool, cache_compression_level: i32,
        #[cfg(feature = "zstd")] dictionary: Option<Vec<u8>>,
        mem_cache_budget: u64,
    ) -> io::Result<Self> {
        fs::create_dir_all(cache_dir)?;

        let cache_file_path = cache_dir.join("data.cache");
        let cache_file = FsOpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&cache_file_path)?;

        // For uncompressed mode: extend to full size (sparse file).
        // For compressed mode: the file grows via append, no pre-allocation needed.
        if !cache_compression && page_count > 0 && page_size > 0 {
            let target_size = page_count * page_size as u64;
            let meta = cache_file.metadata()?;
            if meta.len() < target_size {
                cache_file.set_len(target_size)?;
            }
        }

        // Load or create compressed cache index
        let index_path = cache_dir.join("cache_index.json");
        let mut cache_index = CacheIndex::new(index_path);

        // If compression mode changed (index exists but compression off, or vice versa),
        // or if index is present but cache file is empty/missing, reset both.
        if cache_compression {
            let file_len = cache_file.metadata()?.len();
            if file_len == 0 && !cache_index.entries.is_empty() {
                // Cache file was cleared but index survived, reset index
                cache_index.clear();
            }
        } else {
            // Not using compression, clear any stale index
            if !cache_index.entries.is_empty() {
                cache_index.clear();
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
            cache_file_len: std::sync::atomic::AtomicU64::new(cache_file.metadata().map(|m| m.len()).unwrap_or(0)),
            cache_file,
            cache_file_extend: parking_lot::Mutex::new(()),
            mem_cache: if mem_cache_budget > 0 && page_count > 0 && page_size > 0 {
                let count = page_count as usize;
                let mut v = Vec::with_capacity(count);
                for _ in 0..count {
                    v.push(std::sync::atomic::AtomicPtr::new(std::ptr::null_mut()));
                }
                Some(v)
            } else {
                None
            },
            mem_cache_budget,
            mem_cache_bytes: std::sync::atomic::AtomicU64::new(0),
            tracker: parking_lot::Mutex::new(tracker),
            bitmap: parking_lot::RwLock::new(bitmap),
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
            cache_compression,
            cache_compression_level,
            cache_index: parking_lot::Mutex::new(cache_index),
            #[cfg(feature = "zstd")]
            dictionary: dictionary.map(|d| Arc::new(d)),
            stat_hits: AtomicU64::new(0),
            stat_misses: AtomicU64::new(0),
            stat_evictions: AtomicU64::new(0),
            stat_bytes_evicted: AtomicU64::new(0),
            stat_peak_cache_bytes: AtomicU64::new(0),
            stat_last_eviction_count: AtomicU64::new(0),
        })
    }

    /// Create a zstd encoder dictionary from raw bytes (if dictionary is set).
    #[cfg(feature = "zstd")]
    fn encoder_dict(&self) -> Option<zstd::dict::EncoderDictionary<'static>> {
        self.dictionary.as_ref().map(|d| {
            zstd::dict::EncoderDictionary::copy(d, self.cache_compression_level)
        })
    }

    /// Ensure the cache file is at least `needed` bytes. Lock-free fast path
    /// when file is already large enough; mutex only for the rare set_len.
    fn ensure_file_len(&self, needed: u64) -> io::Result<()> {
        if self.cache_file_len.load(Ordering::Relaxed) >= needed {
            return Ok(());
        }
        let _guard = self.cache_file_extend.lock();
        // Re-check after lock
        let current = self.cache_file_len.load(Ordering::Relaxed);
        if current < needed {
            self.cache_file.set_len(needed)?;
            self.cache_file_len.store(needed, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Create a zstd decoder dictionary from raw bytes (if dictionary is set).
    #[cfg(feature = "zstd")]
    fn decoder_dict(&self) -> Option<zstd::dict::DecoderDictionary<'static>> {
        self.dictionary.as_ref().map(|d| {
            zstd::dict::DecoderDictionary::copy(d)
        })
    }

    /// Read a single page from the cache file.
    /// Uncompressed mode: pread at fixed offset, decrypt with CTR if encrypted.
    /// Compressed mode: look up offset+len in index, pread, decrypt CTR, zstd decompress.
    pub(crate) fn read_page(&self, page_num: u64, buf: &mut [u8]) -> io::Result<()> {
        use std::os::unix::fs::FileExt;

        if self.cache_compression {
            return self.read_page_compressed(page_num, buf);
        }

        // In-memory page cache: zero-lock read via AtomicPtr.
        if let Some(ref mc) = self.mem_cache {
            if let Some(slot) = mc.get(page_num as usize) {
                let ptr = slot.load(Ordering::Acquire);
                if !ptr.is_null() {
                    let ps = self.page_size.load(Ordering::Relaxed) as usize;
                    let copy_len = buf.len().min(ps);
                    unsafe { std::ptr::copy_nonoverlapping(ptr, buf.as_mut_ptr(), copy_len); }
                    return Ok(());
                }
            }
            // Miss: read from disk, then promote
            let ps = self.page_size.load(Ordering::Acquire) as usize;
            let offset = page_num * ps as u64;
            self.cache_file.read_exact_at(buf, offset)?;
            #[cfg(feature = "encryption")]
            if let Some(ref key) = self.encryption_key {
                let decrypted = compress::decrypt_ctr(buf, page_num, key)?;
                buf.copy_from_slice(&decrypted);
            }
            // Promote: allocate page buffer + store pointer atomically
            if let Some(slot) = mc.get(page_num as usize) {
                if slot.load(Ordering::Relaxed).is_null() {
                    let current = self.mem_cache_bytes.load(Ordering::Relaxed);
                    if current + ps as u64 <= self.mem_cache_budget {
                        let page_buf = vec![0u8; ps].into_boxed_slice();
                        let ptr = Box::into_raw(page_buf) as *mut u8;
                        unsafe { std::ptr::copy_nonoverlapping(buf.as_ptr(), ptr, buf.len().min(ps)); }
                        // CAS to avoid double-allocation on race
                        if slot.compare_exchange(
                            std::ptr::null_mut(), ptr, Ordering::Release, Ordering::Relaxed
                        ).is_err() {
                            // Another thread beat us, free our allocation
                            unsafe { drop(Box::from_raw(std::slice::from_raw_parts_mut(ptr, ps))); }
                        } else {
                            self.mem_cache_bytes.fetch_add(ps as u64, Ordering::Relaxed);
                        }
                    }
                }
            }
            return Ok(());
        }

        let offset = page_num * self.page_size.load(Ordering::Acquire) as u64;
        self.cache_file.read_exact_at(buf, offset)?;
        #[cfg(feature = "encryption")]
        if let Some(ref key) = self.encryption_key {
            let decrypted = compress::decrypt_ctr(buf, page_num, key)?;
            buf.copy_from_slice(&decrypted);
        }
        Ok(())
    }

    /// Read a page from the compressed cache: index lookup -> pread -> decrypt -> decompress.
    fn read_page_compressed(&self, page_num: u64, buf: &mut [u8]) -> io::Result<()> {
        use std::os::unix::fs::FileExt;

        let entry = {
            let index = self.cache_index.lock();
            match index.get(page_num) {
                Some(e) => *e,
                None => return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("page {} not in compressed cache index", page_num),
                )),
            }
        };

        // Read the compressed (and optionally encrypted) blob
        let mut compressed = vec![0u8; entry.compressed_len as usize];
        self.cache_file.read_exact_at(&mut compressed, entry.offset)?;

        // Decrypt if encrypted (CTR, same size)
        #[cfg(feature = "encryption")]
        if let Some(ref key) = self.encryption_key {
            let decrypted = compress::decrypt_ctr(&compressed, page_num, key)?;
            compressed = decrypted;
        }

        // Decompress (with dictionary if available)
        {
            #[cfg(feature = "zstd")]
            let dd = self.decoder_dict();
            let decompressed = compress::decompress(
                &compressed,
                #[cfg(feature = "zstd")]
                dd.as_ref(),
                #[cfg(not(feature = "zstd"))]
                None,
            )?;
            // SQLite may request partial reads (e.g., 16-byte header read).
            // Decompressed data is always a full page. Copy the requested portion.
            if buf.len() <= decompressed.len() {
                buf.copy_from_slice(&decompressed[..buf.len()]);
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "decompressed page {} too small: need {} bytes, got {}",
                        page_num, buf.len(), decompressed.len()
                    ),
                ));
            }
        }

        Ok(())
    }

    /// Write a single page to the cache file.
    /// Uncompressed mode: encrypt with CTR if key set, pwrite at fixed offset.
    /// Compressed mode: zstd compress, CTR encrypt, append at index offset.
    pub(crate) fn write_page(&self, page_num: u64, data: &[u8]) -> io::Result<()> {
        use std::os::unix::fs::FileExt;

        if self.cache_compression {
            return self.write_page_compressed(page_num, data);
        }

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

        // Extend file if needed (serialized via mutex), then pwrite (lock-free)
        self.ensure_file_len(needed)?;
        self.cache_file.write_all_at(data, offset)?;
        self.bitmap_mark(page_num);
        // Update in-memory cache if page is already cached (keep dirty data consistent).
        // Don't promote on write -- read path handles promotion.
        if let Some(ref mc) = self.mem_cache {
            if let Some(slot) = mc.get(page_num as usize) {
                let ptr = slot.load(Ordering::Relaxed);
                if !ptr.is_null() {
                    let copy_len = data.len().min(self.page_size.load(Ordering::Relaxed) as usize);
                    unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, copy_len); }
                }
            }
        }
        Ok(())
    }

    /// Write a single page in compressed mode: compress -> encrypt -> append -> update index.
    fn write_page_compressed(&self, page_num: u64, data: &[u8]) -> io::Result<()> {
        use std::os::unix::fs::FileExt;

        // Compress (with dictionary if available)
        #[cfg(feature = "zstd")]
        let ed = self.encoder_dict();
        let mut blob = compress::compress(
            data, self.cache_compression_level,
            #[cfg(feature = "zstd")]
            ed.as_ref(),
            #[cfg(not(feature = "zstd"))]
            None,
        )?;

        // Encrypt compressed blob
        #[cfg(feature = "encryption")]
        if let Some(ref key) = self.encryption_key {
            blob = compress::encrypt_ctr(&blob, page_num, key)?;
        }

        let blob_len = blob.len() as u32;

        // Reserve offset in index and write
        let offset = {
            let mut index = self.cache_index.lock();
            index.insert(page_num, blob_len)
        };

        let needed = offset + blob_len as u64;
        self.ensure_file_len(needed)?;
        self.cache_file.write_all_at(&blob, offset)?;

        self.bitmap_mark(page_num);
        Ok(())
    }

    /// Write a contiguous range of pages to the cache file in a single I/O operation.
    /// `start_page` is the first page number, `data` is the raw concatenated page data.
    /// Uses RwLock: read lock for pwrite (concurrent), write lock only if file needs extending.
    pub(crate) fn write_pages_bulk(&self, start_page: u64, data: &[u8], num_pages: u64) -> io::Result<()> {
        use std::os::unix::fs::FileExt;
        let page_sz = self.page_size.load(Ordering::Acquire) as usize;

        if self.cache_compression {
            return self.write_pages_bulk_compressed(start_page, data, num_pages);
        }

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

        self.ensure_file_len(needed)?;
        self.cache_file.write_all_at(data, offset)?;

        // Mark all pages present in bitmap (ensure capacity once, then atomic marks)
        {
            let last_page = start_page + num_pages - 1;
            let needed = last_page as usize / 8 + 1;
            let bm = self.bitmap.read();
            if needed > bm.bits.len() {
                drop(bm);
                self.bitmap.write().ensure_capacity(last_page);
            }
        }
        let bitmap = self.bitmap.read();
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

    /// Compressed bulk write: compress each page individually, concatenate, single pwrite.
    fn write_pages_bulk_compressed(&self, start_page: u64, data: &[u8], num_pages: u64) -> io::Result<()> {
        use std::os::unix::fs::FileExt;
        let page_sz = self.page_size.load(Ordering::Acquire) as usize;

        // Create encoder dictionary once for all pages in this batch
        #[cfg(feature = "zstd")]
        let ed = self.encoder_dict();

        // Compress each page, build a single contiguous blob and record offsets
        let mut blob = Vec::new();
        let mut page_entries: Vec<(u64, u64, u32)> = Vec::with_capacity(num_pages as usize); // (page_num, offset_in_blob, compressed_len)

        for i in 0..num_pages {
            let start = i as usize * page_sz;
            let end = (start + page_sz).min(data.len());
            let page_data = &data[start..end];

            let mut compressed = compress::compress(
                page_data, self.cache_compression_level,
                #[cfg(feature = "zstd")]
                ed.as_ref(),
                #[cfg(not(feature = "zstd"))]
                None,
            )?;

            #[cfg(feature = "encryption")]
            if let Some(ref key) = self.encryption_key {
                compressed = compress::encrypt_ctr(&compressed, start_page + i, key)?;
            }

            let blob_offset = blob.len() as u64;
            let compressed_len = compressed.len() as u32;
            blob.extend_from_slice(&compressed);
            page_entries.push((start_page + i, blob_offset, compressed_len));
        }

        // Reserve contiguous range in the index and write blob
        let base_offset = {
            let mut index = self.cache_index.lock();
            let base = index.next_offset;
            for &(page_num, offset_in_blob, compressed_len) in &page_entries {
                index.insert_at(page_num, base + offset_in_blob, compressed_len);
            }
            base
        };

        let needed = base_offset + blob.len() as u64;
        self.ensure_file_len(needed)?;
        self.cache_file.write_all_at(&blob, base_offset)?;

        // Mark all pages present in bitmap (ensure capacity once, then atomic marks)
        {
            let last_page = start_page + num_pages - 1;
            let needed = last_page as usize / 8 + 1;
            let bm = self.bitmap.read();
            if needed > bm.bits.len() {
                drop(bm);
                self.bitmap.write().ensure_capacity(last_page);
            }
        }
        let bitmap = self.bitmap.read();
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

        if self.cache_compression {
            return self.write_pages_scattered_compressed(written_pages, data, page_sz, gid, start_index_in_group);
        }

        // Find max page to size the cache file
        let max_page = written_pages.iter().copied().max().unwrap_or(0);
        let needed = (max_page + 1) * page_sz as u64;

        self.ensure_file_len(needed)?;
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
            self.cache_file.write_all_at(page_data, offset)?;
        }

        // Mark bitmap for per-page presence (ensure capacity, then atomic marks)
        if let Some(&max_page) = written_pages.iter().max() {
            let needed = max_page as usize / 8 + 1;
            let bm = self.bitmap.read();
            if needed > bm.bits.len() {
                drop(bm);
                self.bitmap.write().ensure_capacity(max_page);
            }
        }
        let bitmap = self.bitmap.read();
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

    /// Compressed scattered write: compress each page, append as contiguous blob.
    fn write_pages_scattered_compressed(
        &self,
        written_pages: &[u64],
        data: &[u8],
        page_sz: usize,
        gid: u64,
        start_index_in_group: u32,
    ) -> io::Result<()> {
        use std::os::unix::fs::FileExt;

        // Create encoder dictionary once for all pages in this batch
        #[cfg(feature = "zstd")]
        let ed = self.encoder_dict();

        let mut blob = Vec::new();
        let mut page_entries: Vec<(u64, u64, u32)> = Vec::with_capacity(written_pages.len());

        for (i, &pnum) in written_pages.iter().enumerate() {
            let src_start = i * page_sz;
            let page_data = &data[src_start..src_start + page_sz];

            let mut compressed = compress::compress(
                page_data, self.cache_compression_level,
                #[cfg(feature = "zstd")]
                ed.as_ref(),
                #[cfg(not(feature = "zstd"))]
                None,
            )?;

            #[cfg(feature = "encryption")]
            if let Some(ref key) = self.encryption_key {
                compressed = compress::encrypt_ctr(&compressed, pnum, key)?;
            }

            let blob_offset = blob.len() as u64;
            let compressed_len = compressed.len() as u32;
            blob.extend_from_slice(&compressed);
            page_entries.push((pnum, blob_offset, compressed_len));
        }

        let base_offset = {
            let mut index = self.cache_index.lock();
            let base = index.next_offset;
            for &(page_num, offset_in_blob, compressed_len) in &page_entries {
                index.insert_at(page_num, base + offset_in_blob, compressed_len);
            }
            base
        };

        let needed = base_offset + blob.len() as u64;
        self.ensure_file_len(needed)?;
        self.cache_file.write_all_at(&blob, base_offset)?;

        // Mark bitmap for per-page presence (ensure capacity, then atomic marks)
        if let Some(&max_page) = written_pages.iter().max() {
            let needed = max_page as usize / 8 + 1;
            let bm = self.bitmap.read();
            if needed > bm.bits.len() {
                drop(bm);
                self.bitmap.write().ensure_capacity(max_page);
            }
        }
        let bitmap = self.bitmap.read();
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

    /// Mark a page present, auto-growing the bitmap if needed.
    /// Fast path (read lock) if capacity is sufficient; slow path (write lock) to grow.
    pub(crate) fn bitmap_mark(&self, page_num: u64) {
        let needed = page_num as usize / 8 + 1;
        {
            let bm = self.bitmap.read();
            if needed <= bm.bits.len() {
                bm.mark_present(page_num);
                return;
            }
        }
        // Need to grow
        let mut bm = self.bitmap.write();
        bm.ensure_capacity(page_num);
        bm.mark_present(page_num);
    }

    /// Check if a page is present in the local cache.
    /// Uses bitmap (per-page accurate). SubChunkTracker is not consulted here
    /// because it uses positional mapping which is wrong for B-tree-aware groups.
    /// In compressed mode, also verifies the page is in the cache index.
    pub(crate) fn is_present(&self, page_num: u64) -> bool {
        let in_bitmap = self.bitmap.read().is_present(page_num);
        if self.cache_compression && in_bitmap {
            // Double-check: bitmap says present, but index must also have it
            return self.cache_index.lock().contains(page_num);
        }
        in_bitmap
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
        // Track peak cache size
        let current = self.tracker.lock().current_cache_bytes;
        self.stat_peak_cache_bytes.fetch_max(current, Ordering::Relaxed);
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

    /// Clear bitmap bits, remove from cache index, and hole-punch pages on Linux.
    pub(crate) fn clear_pages_from_disk(&self, page_nums: &[u64]) {
        {
            let bitmap = self.bitmap.read();
            for &pnum in page_nums {
                bitmap.clear(pnum);
            }
        }
        // Remove from compressed cache index
        if self.cache_compression {
            let mut index = self.cache_index.lock();
            for &pnum in page_nums {
                index.remove(pnum);
            }
        }
        // Hole-punching only applies to uncompressed mode (fixed offsets)
        #[cfg(target_os = "linux")]
        if !self.cache_compression {
            use std::os::unix::io::AsRawFd;
            let ps = self.page_size.load(Ordering::Acquire) as u64;
            for &pnum in page_nums {
                let offset = (pnum * ps) as libc::off_t;
                let len = ps as libc::off_t;
                unsafe {
                    libc::fallocate(
                        self.cache_file.as_raw_fd(),
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
        self.stat_last_eviction_count.store(evicted as u64, Ordering::Relaxed);

        // Churn detection: if >50% of cache was evicted, warn
        if evicted > 0 {
            let total_present = self.tracker.lock().present.len() as u64;
            let total_before = total_present + evicted as u64;
            if total_before > 0 && (evicted as u64 * 100 / total_before) > 50 {
                eprintln!(
                    "[cache] WARNING: high churn detected. Evicted {} of {} sub-chunks ({}%). \
                     Consider increasing cache_limit.",
                    evicted, total_before, evicted as u64 * 100 / total_before,
                );
            }
        }
        evicted
    }

    /// Prune the compressed cache index: remove all entries NOT in `keep_pages`.
    /// No-op if cache_compression is false. Persists the index afterward.
    pub(crate) fn prune_cache_index(&self, keep_pages: &HashSet<u64>) {
        if !self.cache_compression {
            return;
        }
        let mut index = self.cache_index.lock();
        let to_remove: Vec<u64> = index.entries.keys()
            .copied()
            .filter(|p| !keep_pages.contains(p))
            .collect();
        for p in to_remove {
            index.remove(p);
        }
        let _ = index.persist();
    }

    /// Clear the compressed cache index entirely (for full cache reset).
    /// No-op if cache_compression is false.
    pub(crate) fn clear_cache_index(&self) {
        if !self.cache_compression {
            return;
        }
        let mut index = self.cache_index.lock();
        index.clear();
        let _ = index.persist();
    }

    /// Mark all pages as present in the bitmap and all groups as Present.
    /// Called after an external process (walrust restore) writes pages directly
    /// to the cache file without going through the VFS.
    pub(crate) fn mark_all_pages_present(&self, page_count: u64) {
        let mut bitmap = self.bitmap.write();
        bitmap.resize(page_count);
        for p in 0..page_count {
            bitmap.mark_present(p);
        }
        let ppg = self.pages_per_group as u64;
        if ppg > 0 {
            let group_count = (page_count + ppg - 1) / ppg;
            let states = self.group_states.lock();
            self.ensure_group_states_capacity(&states, group_count.saturating_sub(1));
            for gid in 0..group_count as usize {
                if let Some(s) = states.get(gid) {
                    s.store(GroupState::Present as u8, Ordering::Release);
                }
            }
        }
    }

    /// Persist the page bitmap, sub-chunk tracker, and cache index to disk.
    pub(crate) fn persist_bitmap(&self) -> io::Result<()> {
        self.bitmap.read().persist()?;
        self.tracker.lock().persist()?;
        if self.cache_compression {
            self.cache_index.lock().persist()?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "test_disk_cache.rs"]
mod tests;

