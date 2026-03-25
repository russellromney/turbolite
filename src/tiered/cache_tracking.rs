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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tiered::*;
    use tempfile::TempDir;
    use std::time::Duration;

    #[cfg(feature = "encryption")]
    fn test_key() -> [u8; 32] {
        let mut key = [0u8; 32];
        for (i, b) in key.iter_mut().enumerate() { *b = i as u8; }
        key
    }

    #[cfg(feature = "encryption")]
    fn wrong_key() -> [u8; 32] {
        [0xFFu8; 32]
    }

    // =========================================================================
    // SubChunkTracker — Comprehensive Tests
    // =========================================================================

    #[test]
    fn test_sub_chunk_for_page_basic() {
        let dir = TempDir::new().unwrap();
        let t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
        // ppg=8, spf=2: group has 4 frames (0..3), each covering 2 pages
        // Page 0,1 → group 0, frame 0
        assert_eq!(t.sub_chunk_for_page(0), SubChunkId { group_id: 0, frame_index: 0 });
        assert_eq!(t.sub_chunk_for_page(1), SubChunkId { group_id: 0, frame_index: 0 });
        // Page 2,3 → group 0, frame 1
        assert_eq!(t.sub_chunk_for_page(2), SubChunkId { group_id: 0, frame_index: 1 });
        assert_eq!(t.sub_chunk_for_page(3), SubChunkId { group_id: 0, frame_index: 1 });
        // Page 6,7 → group 0, frame 3
        assert_eq!(t.sub_chunk_for_page(6), SubChunkId { group_id: 0, frame_index: 3 });
        assert_eq!(t.sub_chunk_for_page(7), SubChunkId { group_id: 0, frame_index: 3 });
        // Page 8 → group 1, frame 0
        assert_eq!(t.sub_chunk_for_page(8), SubChunkId { group_id: 1, frame_index: 0 });
        // Page 15 → group 1, frame 3
        assert_eq!(t.sub_chunk_for_page(15), SubChunkId { group_id: 1, frame_index: 3 });
    }

    #[test]
    fn test_sub_chunk_for_page_large_config() {
        let dir = TempDir::new().unwrap();
        // 64KB pages: ppg=256, spf=4 (production defaults)
        let t = SubChunkTracker::new(dir.path().join("t"), 256, 4);
        // Page 0 → group 0, frame 0
        assert_eq!(t.sub_chunk_for_page(0), SubChunkId { group_id: 0, frame_index: 0 });
        // Page 3 → group 0, frame 0 (still within first 4 pages)
        assert_eq!(t.sub_chunk_for_page(3), SubChunkId { group_id: 0, frame_index: 0 });
        // Page 4 → group 0, frame 1
        assert_eq!(t.sub_chunk_for_page(4), SubChunkId { group_id: 0, frame_index: 1 });
        // Page 255 → group 0, frame 63
        assert_eq!(t.sub_chunk_for_page(255), SubChunkId { group_id: 0, frame_index: 63 });
        // Page 256 → group 1, frame 0
        assert_eq!(t.sub_chunk_for_page(256), SubChunkId { group_id: 1, frame_index: 0 });
    }

    #[test]
    fn test_sub_chunk_for_page_zero_config() {
        let dir = TempDir::new().unwrap();
        let t = SubChunkTracker::new(dir.path().join("t"), 0, 0);
        // Edge: zero config should not panic, returns (0, 0)
        assert_eq!(t.sub_chunk_for_page(100), SubChunkId { group_id: 0, frame_index: 0 });
    }

    #[test]
    fn test_sub_chunk_tracker_mark_and_present() {
        let dir = TempDir::new().unwrap();
        let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
        let id = SubChunkId { group_id: 0, frame_index: 1 };
        assert!(!t.is_sub_chunk_present(&id));
        assert!(!t.is_present(2)); // page 2 maps to this sub-chunk

        t.mark_present(id, SubChunkTier::Data);
        assert!(t.is_sub_chunk_present(&id));
        assert!(t.is_present(2));
        assert!(t.is_present(3)); // same sub-chunk
        assert!(!t.is_present(0)); // different sub-chunk (frame 0)
        assert!(!t.is_present(4)); // different sub-chunk (frame 2)
    }

    #[test]
    fn test_sub_chunk_tracker_tier_promotion_respects_priority() {
        let dir = TempDir::new().unwrap();
        let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
        let id = SubChunkId { group_id: 0, frame_index: 0 };

        // Start as Data
        t.mark_present(id, SubChunkTier::Data);
        assert_eq!(t.tiers.get(&id), Some(&SubChunkTier::Data));

        // Promote to Index
        t.mark_index(id);
        assert_eq!(t.tiers.get(&id), Some(&SubChunkTier::Index));

        // Promote to Pinned
        t.mark_pinned(id);
        assert_eq!(t.tiers.get(&id), Some(&SubChunkTier::Pinned));

        // Cannot demote from Pinned via mark_present
        t.mark_present(id, SubChunkTier::Data);
        assert_eq!(t.tiers.get(&id), Some(&SubChunkTier::Pinned));

        // Cannot demote from Pinned via mark_index
        t.mark_index(id);
        assert_eq!(t.tiers.get(&id), Some(&SubChunkTier::Pinned));
    }

    #[test]
    fn test_sub_chunk_tracker_mark_present_does_not_demote() {
        let dir = TempDir::new().unwrap();
        let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
        let id = SubChunkId { group_id: 0, frame_index: 0 };

        // Set as Index
        t.mark_present(id, SubChunkTier::Index);
        assert_eq!(t.tiers.get(&id), Some(&SubChunkTier::Index));

        // mark_present with Data should NOT demote to Data
        t.mark_present(id, SubChunkTier::Data);
        assert_eq!(t.tiers.get(&id), Some(&SubChunkTier::Index));

        // mark_present with Index (same level) should keep Index
        t.mark_present(id, SubChunkTier::Index);
        assert_eq!(t.tiers.get(&id), Some(&SubChunkTier::Index));
    }

    #[test]
    fn test_sub_chunk_tracker_evict_one_prefers_data_over_index() {
        let dir = TempDir::new().unwrap();
        let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
        let data_id = SubChunkId { group_id: 0, frame_index: 0 };
        let index_id = SubChunkId { group_id: 0, frame_index: 1 };

        t.mark_present(index_id, SubChunkTier::Index);
        t.mark_present(data_id, SubChunkTier::Data);

        // Should evict Data first (higher tier number)
        let evicted = t.evict_one().unwrap();
        assert_eq!(evicted, data_id);
        assert!(t.is_sub_chunk_present(&index_id));
        assert!(!t.is_sub_chunk_present(&data_id));
    }

    #[test]
    fn test_sub_chunk_tracker_evict_one_never_evicts_pinned() {
        let dir = TempDir::new().unwrap();
        let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
        let pinned = SubChunkId { group_id: 0, frame_index: 0 };
        let data = SubChunkId { group_id: 0, frame_index: 1 };

        t.mark_present(pinned, SubChunkTier::Data);
        t.mark_pinned(pinned);
        t.mark_present(data, SubChunkTier::Data);

        // Should evict data, not pinned
        let evicted = t.evict_one().unwrap();
        assert_eq!(evicted, data);
        assert!(t.is_sub_chunk_present(&pinned));

        // With only pinned left, evict_one returns None
        assert!(t.evict_one().is_none());
    }

    #[test]
    fn test_sub_chunk_tracker_evict_one_lru_within_tier() {
        let dir = TempDir::new().unwrap();
        let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
        let old = SubChunkId { group_id: 0, frame_index: 0 };
        let new = SubChunkId { group_id: 0, frame_index: 1 };

        t.mark_present(old, SubChunkTier::Data);
        std::thread::sleep(Duration::from_millis(10));
        t.mark_present(new, SubChunkTier::Data);

        // Should evict the older one
        let evicted = t.evict_one().unwrap();
        assert_eq!(evicted, old);
    }

    #[test]
    fn test_sub_chunk_tracker_evict_cascade_data_then_index() {
        let dir = TempDir::new().unwrap();
        let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
        let pinned = SubChunkId { group_id: 0, frame_index: 0 };
        let index = SubChunkId { group_id: 0, frame_index: 1 };
        let data1 = SubChunkId { group_id: 0, frame_index: 2 };
        let data2 = SubChunkId { group_id: 0, frame_index: 3 };

        t.mark_present(pinned, SubChunkTier::Data);
        t.mark_pinned(pinned);
        t.mark_present(index, SubChunkTier::Index);
        std::thread::sleep(Duration::from_millis(5));
        t.mark_present(data1, SubChunkTier::Data);
        std::thread::sleep(Duration::from_millis(5));
        t.mark_present(data2, SubChunkTier::Data);

        // First two evictions should be Data tier (oldest first)
        assert_eq!(t.evict_one().unwrap(), data1);
        assert_eq!(t.evict_one().unwrap(), data2);
        // Then Index tier
        assert_eq!(t.evict_one().unwrap(), index);
        // Pinned never evicted
        assert!(t.evict_one().is_none());
        assert!(t.is_sub_chunk_present(&pinned));
    }

    #[test]
    fn test_sub_chunk_tracker_evict_empty() {
        let dir = TempDir::new().unwrap();
        let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
        assert!(t.evict_one().is_none());
    }

    #[test]
    fn test_sub_chunk_tracker_remove_group() {
        let dir = TempDir::new().unwrap();
        let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
        // Add sub-chunks across two groups
        t.mark_present(SubChunkId { group_id: 0, frame_index: 0 }, SubChunkTier::Data);
        t.mark_present(SubChunkId { group_id: 0, frame_index: 1 }, SubChunkTier::Pinned);
        t.mark_present(SubChunkId { group_id: 1, frame_index: 0 }, SubChunkTier::Data);

        t.remove_group(0);
        // Group 0 sub-chunks gone (even pinned)
        assert!(!t.is_sub_chunk_present(&SubChunkId { group_id: 0, frame_index: 0 }));
        assert!(!t.is_sub_chunk_present(&SubChunkId { group_id: 0, frame_index: 1 }));
        // Group 1 unaffected
        assert!(t.is_sub_chunk_present(&SubChunkId { group_id: 1, frame_index: 0 }));
    }

    #[test]
    fn test_sub_chunk_tracker_clear_data_keeps_pinned() {
        let dir = TempDir::new().unwrap();
        let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
        let pinned = SubChunkId { group_id: 0, frame_index: 0 };
        let index = SubChunkId { group_id: 0, frame_index: 1 };
        let data = SubChunkId { group_id: 0, frame_index: 2 };

        t.mark_present(pinned, SubChunkTier::Data);
        t.mark_pinned(pinned);
        t.mark_present(index, SubChunkTier::Index);
        t.mark_present(data, SubChunkTier::Data);

        t.clear_data();
        // Pinned survives
        assert!(t.is_sub_chunk_present(&pinned));
        // Index and Data are gone
        assert!(!t.is_sub_chunk_present(&index));
        assert!(!t.is_sub_chunk_present(&data));
    }

    #[test]
    fn test_sub_chunk_tracker_clear_data_only_keeps_index_and_pinned() {
        let dir = TempDir::new().unwrap();
        let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
        let pinned = SubChunkId { group_id: 0, frame_index: 0 };
        let index = SubChunkId { group_id: 0, frame_index: 1 };
        let data = SubChunkId { group_id: 0, frame_index: 2 };

        t.mark_present(pinned, SubChunkTier::Data);
        t.mark_pinned(pinned);
        t.mark_present(index, SubChunkTier::Index);
        t.mark_present(data, SubChunkTier::Data);

        t.clear_data_only();
        // Pinned and Index survive
        assert!(t.is_sub_chunk_present(&pinned));
        assert!(t.is_sub_chunk_present(&index));
        // Data is gone
        assert!(!t.is_sub_chunk_present(&data));
    }

    #[test]
    fn test_sub_chunk_tracker_clear_all() {
        let dir = TempDir::new().unwrap();
        let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
        t.mark_present(SubChunkId { group_id: 0, frame_index: 0 }, SubChunkTier::Data);
        t.mark_pinned(SubChunkId { group_id: 0, frame_index: 0 });
        t.mark_present(SubChunkId { group_id: 0, frame_index: 1 }, SubChunkTier::Index);
        t.mark_present(SubChunkId { group_id: 0, frame_index: 2 }, SubChunkTier::Data);

        t.clear_all();
        assert_eq!(t.present.len(), 0);
        assert_eq!(t.tiers.len(), 0);
        assert_eq!(t.access_times.len(), 0);
    }

    #[test]
    fn test_sub_chunk_tracker_clear_index_and_data_keeps_pinned() {
        let dir = TempDir::new().unwrap();
        let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
        let pinned = SubChunkId { group_id: 0, frame_index: 0 };
        let index = SubChunkId { group_id: 0, frame_index: 1 };
        let data = SubChunkId { group_id: 0, frame_index: 2 };

        t.mark_present(pinned, SubChunkTier::Data);
        t.mark_pinned(pinned);
        t.mark_present(index, SubChunkTier::Index);
        t.mark_present(data, SubChunkTier::Data);

        t.clear_index_and_data();
        // Pinned survives
        assert!(t.is_sub_chunk_present(&pinned));
        // Index and Data are gone
        assert!(!t.is_sub_chunk_present(&index));
        assert!(!t.is_sub_chunk_present(&data));
    }

    #[test]
    fn test_sub_chunk_tracker_persist_and_reload() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("tracker_persist");
        {
            let mut t = SubChunkTracker::new(path.clone(), 8, 2);
            t.mark_present(SubChunkId { group_id: 0, frame_index: 0 }, SubChunkTier::Pinned);
            t.mark_present(SubChunkId { group_id: 0, frame_index: 1 }, SubChunkTier::Index);
            t.mark_present(SubChunkId { group_id: 1, frame_index: 2 }, SubChunkTier::Data);
            t.persist().unwrap();
        }
        // Reload
        let t2 = SubChunkTracker::new(path, 8, 2);
        assert!(t2.is_sub_chunk_present(&SubChunkId { group_id: 0, frame_index: 0 }));
        assert!(t2.is_sub_chunk_present(&SubChunkId { group_id: 0, frame_index: 1 }));
        assert!(t2.is_sub_chunk_present(&SubChunkId { group_id: 1, frame_index: 2 }));
        // Tiers survive persistence
        assert_eq!(t2.tiers.get(&SubChunkId { group_id: 0, frame_index: 0 }), Some(&SubChunkTier::Pinned));
        assert_eq!(t2.tiers.get(&SubChunkId { group_id: 0, frame_index: 1 }), Some(&SubChunkTier::Index));
        assert_eq!(t2.tiers.get(&SubChunkId { group_id: 1, frame_index: 2 }), Some(&SubChunkTier::Data));
    }

    #[test]
    fn test_sub_chunk_tracker_len_and_count_tier() {
        let dir = TempDir::new().unwrap();
        let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
        assert_eq!(t.len(), 0);

        t.mark_present(SubChunkId { group_id: 0, frame_index: 0 }, SubChunkTier::Pinned);
        t.mark_present(SubChunkId { group_id: 0, frame_index: 1 }, SubChunkTier::Index);
        t.mark_present(SubChunkId { group_id: 0, frame_index: 2 }, SubChunkTier::Data);
        t.mark_present(SubChunkId { group_id: 0, frame_index: 3 }, SubChunkTier::Data);

        assert_eq!(t.len(), 4);
        assert_eq!(t.count_tier(SubChunkTier::Pinned), 1);
        assert_eq!(t.count_tier(SubChunkTier::Index), 1);
        assert_eq!(t.count_tier(SubChunkTier::Data), 2);
    }

    #[test]
    fn test_sub_chunk_tracker_touch_updates_lru_order() {
        let dir = TempDir::new().unwrap();
        let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
        let old = SubChunkId { group_id: 0, frame_index: 0 };
        let new = SubChunkId { group_id: 0, frame_index: 1 };

        t.mark_present(old, SubChunkTier::Data);
        std::thread::sleep(Duration::from_millis(10));
        t.mark_present(new, SubChunkTier::Data);

        // Touch the old one to make it "new"
        std::thread::sleep(Duration::from_millis(10));
        t.touch(old);

        // Now `new` is the least recently used
        let evicted = t.evict_one().unwrap();
        assert_eq!(evicted, new);
    }

    #[test]
    fn test_sub_chunk_tracker_pages_for_sub_chunk() {
        let dir = TempDir::new().unwrap();
        let t = SubChunkTracker::new(dir.path().join("t"), 8, 2);
        let page_count = 16u64;
        // Frame 0 of group 0: pages 0..2
        let pages = t.pages_for_sub_chunk(SubChunkId { group_id: 0, frame_index: 0 }, page_count);
        assert_eq!(pages, 0..2);
        // Frame 2 of group 0: pages 4..6
        let pages = t.pages_for_sub_chunk(SubChunkId { group_id: 0, frame_index: 2 }, page_count);
        assert_eq!(pages, 4..6);
        // Frame 0 of group 1: pages 8..10
        let pages = t.pages_for_sub_chunk(SubChunkId { group_id: 1, frame_index: 0 }, page_count);
        assert_eq!(pages, 8..10);
        // Boundary: last frame clamped to page_count
        let pages = t.pages_for_sub_chunk(SubChunkId { group_id: 1, frame_index: 3 }, page_count);
        assert_eq!(pages, 14..16);
    }

    // ===== Regression tests for review bugs =====

    #[test]
    fn test_evict_one_no_access_time_treated_as_oldest() {
        // Regression: evict_one() used Instant::now() per-iteration for missing access times,
        // making those entries effectively unevictable. Now uses a fixed epoch (1hr ago).
        let dir = TempDir::new().unwrap();
        let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);

        // Insert a sub-chunk via present set and tier, but no access time
        let orphan = SubChunkId { group_id: 0, frame_index: 0 };
        t.present.insert(orphan);
        t.tiers.insert(orphan, SubChunkTier::Data);
        // Intentionally no access_times entry

        // Insert a normal sub-chunk with access time
        let normal = SubChunkId { group_id: 0, frame_index: 1 };
        t.mark_present(normal, SubChunkTier::Data);

        // The orphan (no access time) should be evicted first — it gets epoch (oldest)
        let evicted = t.evict_one().unwrap();
        assert_eq!(evicted, orphan, "sub-chunk without access time should be evicted first");
        // Normal one should still be present
        assert!(t.is_sub_chunk_present(&normal));
    }

    #[test]
    fn test_evict_one_multiple_missing_access_times_all_evictable() {
        // Regression: with per-iteration Instant::now(), multiple missing-access-time entries
        // would compete unfairly. With fixed epoch, they're all equally old.
        let dir = TempDir::new().unwrap();
        let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);

        // Insert 3 sub-chunks with no access times
        for i in 0..3u16 {
            let id = SubChunkId { group_id: 0, frame_index: i };
            t.present.insert(id);
            t.tiers.insert(id, SubChunkTier::Data);
        }

        // All 3 should be evictable
        assert!(t.evict_one().is_some());
        assert!(t.evict_one().is_some());
        assert!(t.evict_one().is_some());
        assert!(t.evict_one().is_none());
    }

    #[test]
    fn test_mark_pinned_adds_to_present_set() {
        // Regression: mark_pinned() only set tier without adding to present set.
        // This meant pinned sub-chunks from write_page path weren't tracked.
        let dir = TempDir::new().unwrap();
        let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);

        let id = SubChunkId { group_id: 0, frame_index: 0 };

        // Before fix: mark_pinned without prior mark_present left sub-chunk not in present
        t.mark_pinned(id);

        assert!(t.is_sub_chunk_present(&id), "mark_pinned must add to present set");
        assert_eq!(t.tiers[&id], SubChunkTier::Pinned);
        assert!(t.access_times.contains_key(&id), "mark_pinned must set access time");
        assert_eq!(t.len(), 1);
        assert_eq!(t.count_tier(SubChunkTier::Pinned), 1);
    }

    #[test]
    fn test_mark_index_adds_to_present_set() {
        // Regression: mark_index() only set tier without adding to present set.
        let dir = TempDir::new().unwrap();
        let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);

        let id = SubChunkId { group_id: 0, frame_index: 0 };

        t.mark_index(id);

        assert!(t.is_sub_chunk_present(&id), "mark_index must add to present set");
        assert_eq!(t.tiers[&id], SubChunkTier::Index);
        assert!(t.access_times.contains_key(&id), "mark_index must set access time");
        assert_eq!(t.len(), 1);
        assert_eq!(t.count_tier(SubChunkTier::Index), 1);
    }

    #[test]
    fn test_mark_pinned_does_not_duplicate_when_already_present() {
        // mark_pinned on an already-present sub-chunk should promote tier, not add duplicate.
        let dir = TempDir::new().unwrap();
        let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);

        let id = SubChunkId { group_id: 0, frame_index: 0 };
        t.mark_present(id, SubChunkTier::Data);
        assert_eq!(t.len(), 1);

        t.mark_pinned(id);
        assert_eq!(t.len(), 1); // still just 1 entry
        assert_eq!(t.tiers[&id], SubChunkTier::Pinned);
    }

    #[test]
    fn test_mark_index_respects_pinned_priority() {
        // mark_index on a Pinned sub-chunk should NOT demote it.
        let dir = TempDir::new().unwrap();
        let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);

        let id = SubChunkId { group_id: 0, frame_index: 0 };
        t.mark_pinned(id);
        t.mark_index(id);

        assert_eq!(t.tiers[&id], SubChunkTier::Pinned, "mark_index must not demote from Pinned");
    }

    #[test]
    fn test_clear_data_removes_index_and_data() {
        // Verify clear_data() removes both Data and Index, keeps only Pinned.
        // (Used by clear_cache_all-like scenarios, not by clear_cache.)
        let dir = TempDir::new().unwrap();
        let mut t = SubChunkTracker::new(dir.path().join("t"), 8, 2);

        let pinned = SubChunkId { group_id: 0, frame_index: 0 };
        let index = SubChunkId { group_id: 0, frame_index: 1 };
        let data = SubChunkId { group_id: 0, frame_index: 2 };

        t.mark_present(pinned, SubChunkTier::Pinned);
        t.mark_present(index, SubChunkTier::Index);
        t.mark_present(data, SubChunkTier::Data);

        t.clear_data();

        assert!(t.is_sub_chunk_present(&pinned), "Pinned must survive clear_data");
        assert!(!t.is_sub_chunk_present(&index), "Index must be removed by clear_data");
        assert!(!t.is_sub_chunk_present(&data), "Data must be removed by clear_data");
    }

    // ===== SubChunkTracker persistence encryption tests =====

    #[test]
    #[cfg(feature = "encryption")]
    fn test_sub_chunk_tracker_encrypted_persist_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("tracker_enc");
        let key = test_key();

        // Create tracker with encryption, add some sub-chunks
        {
            let mut t = SubChunkTracker::new_encrypted(path.clone(), 8, 2, Some(key));
            let id1 = SubChunkId { group_id: 0, frame_index: 0 };
            let id2 = SubChunkId { group_id: 1, frame_index: 2 };
            t.mark_present(id1, SubChunkTier::Pinned);
            t.mark_present(id2, SubChunkTier::Data);
            t.persist().unwrap();
        }

        // Reload with same key — data must survive
        let t2 = SubChunkTracker::new_encrypted(path, 8, 2, Some(key));
        let id1 = SubChunkId { group_id: 0, frame_index: 0 };
        let id2 = SubChunkId { group_id: 1, frame_index: 2 };
        assert!(t2.is_sub_chunk_present(&id1), "sub-chunk 0:0 must survive persist+reload");
        assert!(t2.is_sub_chunk_present(&id2), "sub-chunk 1:2 must survive persist+reload");
        assert_eq!(t2.tiers.get(&id1).copied(), Some(SubChunkTier::Pinned), "tier must survive");
        assert_eq!(t2.tiers.get(&id2).copied(), Some(SubChunkTier::Data), "tier must survive");
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn test_sub_chunk_tracker_encrypted_on_disk_not_plaintext() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("tracker_enc");
        let key = test_key();

        let mut t = SubChunkTracker::new_encrypted(path.clone(), 8, 2, Some(key));
        let id = SubChunkId { group_id: 5, frame_index: 3 };
        t.mark_present(id, SubChunkTier::Index);
        t.persist().unwrap();

        // Raw file should not be valid JSON (it's encrypted)
        let raw = std::fs::read(&path).unwrap();
        let parse_result: Result<Vec<(SubChunkId, u8)>, _> = serde_json::from_slice(&raw);
        assert!(parse_result.is_err(), "encrypted tracker file must not be valid JSON");
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn test_sub_chunk_tracker_wrong_key_returns_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("tracker_enc");
        let key = test_key();

        // Write with correct key
        {
            let mut t = SubChunkTracker::new_encrypted(path.clone(), 8, 2, Some(key));
            let id = SubChunkId { group_id: 0, frame_index: 0 };
            t.mark_present(id, SubChunkTier::Pinned);
            t.persist().unwrap();
        }

        // Load with wrong key — should fail gracefully (empty tracker, not crash)
        let t2 = SubChunkTracker::new_encrypted(path, 8, 2, Some(wrong_key()));
        let id = SubChunkId { group_id: 0, frame_index: 0 };
        assert!(!t2.is_sub_chunk_present(&id), "wrong key must not load data");
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn test_sub_chunk_tracker_no_encryption_stays_plaintext() {
        // Without encryption, tracker persist is still plain JSON
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("tracker_plain");

        let mut t = SubChunkTracker::new(path.clone(), 8, 2);
        let id = SubChunkId { group_id: 2, frame_index: 1 };
        t.mark_present(id, SubChunkTier::Data);
        t.persist().unwrap();

        // Raw file should be valid JSON
        let raw = std::fs::read(&path).unwrap();
        let parse_result: Result<Vec<(SubChunkId, u8)>, _> = serde_json::from_slice(&raw);
        assert!(parse_result.is_ok(), "unencrypted tracker file must be valid JSON");
    }
}

