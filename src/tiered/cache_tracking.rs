use super::*;

// ===== Page Bitmap =====

/// 1-bit-per-page tracking of which pages are present in the local cache file.
/// Persisted to disk for crash recovery.
pub(crate) struct PageBitmap {
    pub(crate) bits: Vec<u8>,
    pub(crate) path: PathBuf,
}

impl PageBitmap {
    pub(crate) fn new(path: PathBuf) -> Self {
        // Try to load from disk
        let bits = fs::read(&path).unwrap_or_default();
        Self { bits, path }
    }

    pub(crate) fn is_present(&self, page: u64) -> bool {
        let byte_idx = page as usize / 8;
        let bit_idx = page as usize % 8;
        if byte_idx >= self.bits.len() {
            return false;
        }
        self.bits[byte_idx] & (1 << bit_idx) != 0
    }

    pub(crate) fn mark_present(&mut self, page: u64) {
        self.ensure_capacity(page);
        let byte_idx = page as usize / 8;
        let bit_idx = page as usize % 8;
        self.bits[byte_idx] |= 1 << bit_idx;
    }

    #[cfg(test)]
    pub(crate) fn mark_range(&mut self, start: u64, count: u64) {
        for p in start..start + count {
            self.mark_present(p);
        }
    }

    pub(crate) fn clear(&mut self, page: u64) {
        let byte_idx = page as usize / 8;
        let bit_idx = page as usize % 8;
        if byte_idx < self.bits.len() {
            self.bits[byte_idx] &= !(1 << bit_idx);
        }
    }

    pub(crate) fn clear_range(&mut self, start: u64, count: u64) {
        for p in start..start + count {
            let byte_idx = p as usize / 8;
            let bit_idx = p as usize % 8;
            if byte_idx < self.bits.len() {
                self.bits[byte_idx] &= !(1 << bit_idx);
            }
        }
    }

    pub(crate) fn resize(&mut self, page_count: u64) {
        let needed_bytes = (page_count as usize + 7) / 8;
        if self.bits.len() < needed_bytes {
            self.bits.resize(needed_bytes, 0);
        }
    }

    pub(crate) fn persist(&self) -> io::Result<()> {
        let tmp = self.path.with_extension("tmp");
        fs::write(&tmp, &self.bits)?;
        fs::rename(&tmp, &self.path)?;
        Ok(())
    }

    pub(crate) fn ensure_capacity(&mut self, page: u64) {
        let needed = page as usize / 8 + 1;
        if self.bits.len() < needed {
            self.bits.resize(needed, 0);
        }
    }
}

// ===== Sub-chunk cache tracking =====

/// Identifies a sub-chunk within a page group: (group_id, frame_index).
/// The sub-chunk is the atomic unit of S3 cost — one range GET per sub-chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct SubChunkId {
    pub(crate) group_id: u32,
    pub(crate) frame_index: u16,
}

/// Priority tier for cache eviction. Lower number = higher priority (evicted last).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub(crate) enum SubChunkTier {
    /// Interior page sub-chunks — never evicted.
    Pinned = 0,
    /// Index leaf sub-chunks — evicted only when all Data sub-chunks are gone.
    Index = 1,
    /// Table leaf / data sub-chunks — standard LRU, evicted first.
    Data = 2,
}

/// Tracks which sub-chunks are present in the local cache file.
/// Replaces per-page PageBitmap with per-sub-chunk granularity.
/// Eviction operates on sub-chunks with tiered priority (Pinned > Index > Data).
pub(crate) struct SubChunkTracker {
    /// Which sub-chunks are currently cached on disk.
    pub(crate) present: HashSet<SubChunkId>,
    /// Priority tier per sub-chunk (determines eviction order).
    pub(crate) tiers: HashMap<SubChunkId, SubChunkTier>,
    /// Last access time per sub-chunk (for LRU within a tier).
    pub(crate) access_times: HashMap<SubChunkId, Instant>,
    /// Config: pages per group.
    pub(crate) pages_per_group: u32,
    /// Config: pages per sub-chunk frame.
    pub(crate) sub_pages_per_frame: u32,
    /// Path for persistence.
    pub(crate) path: PathBuf,
    /// Optional encryption key for CTR-encrypting the persistence file.
    #[cfg(feature = "encryption")]
    pub(crate) encryption_key: Option<[u8; 32]>,
}

impl SubChunkTracker {
    pub(crate) fn new(path: PathBuf, pages_per_group: u32, sub_pages_per_frame: u32) -> Self {
        // Try to load from disk (no encryption key yet — loaded as plaintext or fails gracefully)
        let (present, tiers) = Self::load_from_disk(&path, None);
        let now = Instant::now();
        let access_times: HashMap<SubChunkId, Instant> = present.iter().map(|id| (*id, now)).collect();
        Self {
            present,
            tiers,
            access_times,
            pages_per_group,
            sub_pages_per_frame,
            path,
            #[cfg(feature = "encryption")]
            encryption_key: None,
        }
    }

    #[cfg(feature = "encryption")]
    pub(crate) fn new_encrypted(path: PathBuf, pages_per_group: u32, sub_pages_per_frame: u32, encryption_key: Option<[u8; 32]>) -> Self {
        let (present, tiers) = Self::load_from_disk(&path, encryption_key.as_ref());
        let now = Instant::now();
        let access_times: HashMap<SubChunkId, Instant> = present.iter().map(|id| (*id, now)).collect();
        Self {
            present,
            tiers,
            access_times,
            pages_per_group,
            sub_pages_per_frame,
            path,
            encryption_key,
        }
    }

    /// Compute which sub-chunk a page belongs to (LEGACY: positional mapping).
    /// Only valid for positional groups. For B-tree-aware groups, use sub_chunk_id_for().
    pub(crate) fn sub_chunk_for_page(&self, page_num: u64) -> SubChunkId {
        let ppg = self.pages_per_group;
        let spf = self.sub_pages_per_frame;
        if ppg == 0 || spf == 0 {
            return SubChunkId { group_id: 0, frame_index: 0 };
        }
        let gid = (page_num / ppg as u64) as u32;
        let page_in_group = (page_num % ppg as u64) as u32;
        let frame_idx = page_in_group / spf;
        SubChunkId { group_id: gid, frame_index: frame_idx as u16 }
    }

    /// Compute SubChunkId from manifest-aware group ID and index within group.
    /// Correct for B-tree-aware groups where pages are non-positional.
    pub(crate) fn sub_chunk_id_for(&self, gid: u64, index_in_group: u32) -> SubChunkId {
        let spf = self.sub_pages_per_frame;
        let frame_idx = if spf > 0 { index_in_group / spf } else { 0 };
        SubChunkId { group_id: gid as u32, frame_index: frame_idx as u16 }
    }

    /// Check if the sub-chunk containing this page is cached.
    pub(crate) fn is_present(&self, page_num: u64) -> bool {
        let id = self.sub_chunk_for_page(page_num);
        self.present.contains(&id)
    }

    /// Check if a specific sub-chunk is cached.
    pub(crate) fn is_sub_chunk_present(&self, id: &SubChunkId) -> bool {
        self.present.contains(id)
    }

    /// Record a sub-chunk as cached with the given tier.
    /// If already present at a higher priority (lower tier number), keeps the higher priority.
    pub(crate) fn mark_present(&mut self, id: SubChunkId, tier: SubChunkTier) {
        self.present.insert(id);
        let existing = self.tiers.get(&id).copied();
        match existing {
            Some(t) if t <= tier => {} // already at equal or higher priority
            _ => { self.tiers.insert(id, tier); }
        }
        self.access_times.insert(id, Instant::now());
    }

    /// Promote a sub-chunk to Pinned tier (interior pages).
    /// Also ensures the sub-chunk is in the present set — pages may have been
    /// written via write_page (bitmap-only), so tracker may not know about them yet.
    pub(crate) fn mark_pinned(&mut self, id: SubChunkId) {
        self.present.insert(id);
        self.tiers.insert(id, SubChunkTier::Pinned);
        // Ensure access time exists
        self.access_times.entry(id).or_insert_with(Instant::now);
    }

    /// Promote a sub-chunk to Index tier (index leaf pages).
    /// Also ensures the sub-chunk is in the present set.
    pub(crate) fn mark_index(&mut self, id: SubChunkId) {
        self.present.insert(id);
        let existing = self.tiers.get(&id).copied();
        match existing {
            Some(SubChunkTier::Pinned) => {} // don't demote from pinned
            _ => { self.tiers.insert(id, SubChunkTier::Index); }
        }
        // Ensure access time exists
        self.access_times.entry(id).or_insert_with(Instant::now);
    }

    /// Touch a sub-chunk's access time (for LRU).
    pub(crate) fn touch(&mut self, id: SubChunkId) {
        self.access_times.insert(id, Instant::now());
    }

    /// Touch the sub-chunk containing a page.
    pub(crate) fn touch_page(&mut self, page_num: u64) {
        let id = self.sub_chunk_for_page(page_num);
        self.touch(id);
    }

    /// Evict one sub-chunk: pick the lowest-priority (highest tier number),
    /// least-recently-used sub-chunk. Returns the evicted sub-chunk or None if empty.
    /// Never evicts Pinned (tier 0) sub-chunks.
    pub(crate) fn evict_one(&mut self) -> Option<SubChunkId> {
        // Find eviction candidate: highest tier number, oldest access time
        // Use a single "epoch" instant for sub-chunks missing access times,
        // so they are treated as the oldest (most evictable) entries.
        let epoch = Instant::now() - Duration::from_secs(3600);
        let mut candidate: Option<(SubChunkId, SubChunkTier, Instant)> = None;
        for id in &self.present {
            let tier = self.tiers.get(id).copied().unwrap_or(SubChunkTier::Data);
            if tier == SubChunkTier::Pinned {
                continue; // never evict pinned
            }
            let access = self.access_times.get(id).copied().unwrap_or(epoch);
            match &candidate {
                None => candidate = Some((*id, tier, access)),
                Some((_, ct, ca)) => {
                    // Prefer higher tier number (Data > Index), then older access
                    if tier > *ct || (tier == *ct && access < *ca) {
                        candidate = Some((*id, tier, access));
                    }
                }
            }
        }
        if let Some((id, _, _)) = candidate {
            self.remove(id);
            Some(id)
        } else {
            None
        }
    }

    /// Evict all sub-chunks in a specific tier that are older than `ttl`.
    pub(crate) fn evict_expired_in_tier(&mut self, tier: SubChunkTier, ttl: Duration) -> Vec<SubChunkId> {
        let now = Instant::now();
        let expired: Vec<SubChunkId> = self.present.iter()
            .filter(|id| {
                let t = self.tiers.get(id).copied().unwrap_or(SubChunkTier::Data);
                if t != tier { return false; }
                let access = self.access_times.get(id).copied().unwrap_or(now);
                now.duration_since(access) > ttl
            })
            .copied()
            .collect();
        for id in &expired {
            self.remove(*id);
        }
        expired
    }

    /// Remove a sub-chunk from tracking (does NOT zero disk).
    pub(crate) fn remove(&mut self, id: SubChunkId) {
        self.present.remove(&id);
        self.tiers.remove(&id);
        self.access_times.remove(&id);
    }

    /// Remove all sub-chunks for a given group.
    pub(crate) fn remove_group(&mut self, group_id: u32) {
        let to_remove: Vec<SubChunkId> = self.present.iter()
            .filter(|id| id.group_id == group_id)
            .copied()
            .collect();
        for id in to_remove {
            self.remove(id);
        }
    }

    /// Clear all non-pinned sub-chunks (for cold benchmarks).
    pub(crate) fn clear_data(&mut self) {
        let to_remove: Vec<SubChunkId> = self.present.iter()
            .filter(|id| {
                let tier = self.tiers.get(id).copied().unwrap_or(SubChunkTier::Data);
                tier != SubChunkTier::Pinned
            })
            .copied()
            .collect();
        for id in to_remove {
            self.remove(id);
        }
    }

    /// Clear all non-pinned, non-index sub-chunks (data only).
    pub(crate) fn clear_data_only(&mut self) {
        let to_remove: Vec<SubChunkId> = self.present.iter()
            .filter(|id| {
                let tier = self.tiers.get(id).copied().unwrap_or(SubChunkTier::Data);
                tier == SubChunkTier::Data
            })
            .copied()
            .collect();
        for id in to_remove {
            self.remove(id);
        }
    }

    /// Clear Index + Data sub-chunks, keep Pinned only.
    pub(crate) fn clear_index_and_data(&mut self) {
        let to_remove: Vec<SubChunkId> = self.present.iter()
            .filter(|id| {
                let tier = self.tiers.get(id).copied().unwrap_or(SubChunkTier::Data);
                tier != SubChunkTier::Pinned
            })
            .copied()
            .collect();
        for id in to_remove {
            self.remove(id);
        }
    }

    /// Clear everything including pinned (full reset).
    pub(crate) fn clear_all(&mut self) {
        self.present.clear();
        self.tiers.clear();
        self.access_times.clear();
    }

    /// Number of tracked sub-chunks.
    pub(crate) fn len(&self) -> usize {
        self.present.len()
    }

    /// Number of sub-chunks in a specific tier.
    pub(crate) fn count_tier(&self, tier: SubChunkTier) -> usize {
        self.present.iter()
            .filter(|id| self.tiers.get(id).copied().unwrap_or(SubChunkTier::Data) == tier)
            .count()
    }

    /// Persist tracker state to disk for crash recovery.
    /// Serializes as JSON: vec of (SubChunkId, tier). CTR-encrypted with random nonce if key present.
    /// File format when encrypted: [8-byte random nonce][CTR-encrypted JSON]
    pub(crate) fn persist(&self) -> io::Result<()> {
        let entries: Vec<(SubChunkId, u8)> = self.present.iter()
            .map(|id| {
                let tier = self.tiers.get(id).copied().unwrap_or(SubChunkTier::Data) as u8;
                (*id, tier)
            })
            .collect();
        let data = serde_json::to_vec(&entries).map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("serialize sub-chunk tracker: {}", e))
        })?;
        #[cfg(feature = "encryption")]
        let data = if let Some(ref key) = self.encryption_key {
            use rand::RngCore;
            // Random nonce per persist — prevents CTR nonce reuse across updates
            let mut nonce_bytes = [0u8; 8];
            rand::thread_rng().fill_bytes(&mut nonce_bytes);
            let nonce_u64 = u64::from_le_bytes(nonce_bytes);
            let encrypted = compress::encrypt_ctr(&data, nonce_u64, key)?;
            let mut result = Vec::with_capacity(8 + encrypted.len());
            result.extend_from_slice(&nonce_bytes);
            result.extend_from_slice(&encrypted);
            result
        } else {
            data
        };
        let tmp = self.path.with_extension("tmp");
        fs::write(&tmp, &data)?;
        fs::rename(&tmp, &self.path)?;
        Ok(())
    }

    /// Load tracker state from disk. Decrypts with CTR if key provided.
    /// Encrypted format: [8-byte random nonce][CTR-encrypted JSON]
    pub(crate) fn load_from_disk(path: &Path, _encryption_key: Option<&[u8; 32]>) -> (HashSet<SubChunkId>, HashMap<SubChunkId, SubChunkTier>) {
        let data = match fs::read(path) {
            Ok(d) => d,
            Err(_) => return (HashSet::new(), HashMap::new()),
        };
        #[cfg(feature = "encryption")]
        let data = if let Some(key) = _encryption_key {
            if data.len() < 8 {
                return (HashSet::new(), HashMap::new());
            }
            let nonce_u64 = u64::from_le_bytes(data[..8].try_into().unwrap());
            match compress::decrypt_ctr(&data[8..], nonce_u64, key) {
                Ok(d) => d,
                Err(_) => return (HashSet::new(), HashMap::new()),
            }
        } else {
            data
        };
        let entries: Vec<(SubChunkId, u8)> = match serde_json::from_slice(&data) {
            Ok(e) => e,
            Err(_) => return (HashSet::new(), HashMap::new()),
        };
        let mut present = HashSet::new();
        let mut tiers = HashMap::new();
        for (id, tier_byte) in entries {
            present.insert(id);
            let tier = match tier_byte {
                0 => SubChunkTier::Pinned,
                1 => SubChunkTier::Index,
                _ => SubChunkTier::Data,
            };
            tiers.insert(id, tier);
        }
        (present, tiers)
    }

    /// Get all pages covered by a sub-chunk (for cache invalidation / hole punching).
    pub(crate) fn pages_for_sub_chunk(&self, id: SubChunkId, page_count: u64) -> std::ops::Range<u64> {
        let start = id.group_id as u64 * self.pages_per_group as u64
            + id.frame_index as u64 * self.sub_pages_per_frame as u64;
        let end = std::cmp::min(
            start + self.sub_pages_per_frame as u64,
            page_count,
        );
        start..end
    }
}

