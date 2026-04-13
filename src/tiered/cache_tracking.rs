use super::*;
use std::sync::atomic::{AtomicU8, Ordering};

// ===== Page Bitmap =====

/// 1-bit-per-page tracking of which pages are present in the local cache file.
/// Persisted to disk for crash recovery.
///
/// Uses AtomicU8 storage so `is_present()` is lock-free. Readers never block
/// writers and vice versa. Only `ensure_capacity()` (which reallocates) needs
/// external synchronization, and that only happens during checkpoint when
/// the page count grows.
pub(crate) struct PageBitmap {
    pub(crate) bits: Vec<AtomicU8>,
    pub(crate) path: PathBuf,
}

// Safety: AtomicU8 is Send+Sync, PathBuf is Send+Sync.
unsafe impl Send for PageBitmap {}
unsafe impl Sync for PageBitmap {}

impl PageBitmap {
    pub(crate) fn new(path: PathBuf) -> Self {
        let raw = fs::read(&path).unwrap_or_default();
        let bits = raw.into_iter().map(AtomicU8::new).collect();
        Self { bits, path }
    }

    /// Lock-free read. Safe to call from any thread without holding a lock.
    pub(crate) fn is_present(&self, page: u64) -> bool {
        let byte_idx = page as usize / 8;
        let bit_idx = page as usize % 8;
        if byte_idx >= self.bits.len() {
            return false;
        }
        self.bits[byte_idx].load(Ordering::Relaxed) & (1 << bit_idx) != 0
    }

    /// Atomic set-bit. Safe to call concurrently (bit-OR is idempotent).
    /// Caller must ensure capacity via `ensure_capacity()` first.
    pub(crate) fn mark_present(&self, page: u64) {
        let byte_idx = page as usize / 8;
        let bit_idx = page as usize % 8;
        if byte_idx < self.bits.len() {
            self.bits[byte_idx].fetch_or(1 << bit_idx, Ordering::Relaxed);
        }
    }

    #[cfg(test)]
    pub(crate) fn mark_range(&self, start: u64, count: u64) {
        for p in start..start + count {
            self.mark_present(p);
        }
    }

    /// Atomic clear-bit.
    pub(crate) fn clear(&self, page: u64) {
        let byte_idx = page as usize / 8;
        let bit_idx = page as usize % 8;
        if byte_idx < self.bits.len() {
            self.bits[byte_idx].fetch_and(!(1 << bit_idx), Ordering::Relaxed);
        }
    }

    #[allow(dead_code)]
    pub(crate) fn clear_range(&self, start: u64, count: u64) {
        for p in start..start + count {
            self.clear(p);
        }
    }

    /// Grow the bitmap if needed. NOT thread-safe for concurrent resize,
    /// but safe to call while readers use `is_present()` on existing pages
    /// (they only access indices < current len).
    pub(crate) fn resize(&mut self, page_count: u64) {
        let needed_bytes = (page_count as usize + 7) / 8;
        if self.bits.len() < needed_bytes {
            self.bits.resize_with(needed_bytes, || AtomicU8::new(0));
        }
    }

    pub(crate) fn persist(&self) -> io::Result<()> {
        let tmp = self.path.with_extension("tmp");
        let raw: Vec<u8> = self.bits.iter().map(|a| a.load(Ordering::Relaxed)).collect();
        fs::write(&tmp, &raw)?;
        fs::rename(&tmp, &self.path)?;
        Ok(())
    }

    /// Grow if needed. Same caveats as `resize()`.
    pub(crate) fn ensure_capacity(&mut self, page: u64) {
        let needed = page as usize / 8 + 1;
        if self.bits.len() < needed {
            self.bits.resize_with(needed, || AtomicU8::new(0));
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

/// Maximum access count before capping. Prevents a heavily-accessed sub-chunk
/// from becoming effectively immortal. At cap=64, a sub-chunk accessed 64 times
/// gets max frequency score but can still be evicted if tier + recency say so.
const ACCESS_COUNT_CAP: u32 = 64;

/// Recency window in seconds. Sub-chunks older than this all get recency_score=0.
/// 1 hour is a reasonable default: most query workloads have a working set that
/// cycles within an hour. Sub-chunks untouched for >1hr are equally "cold."
const RECENCY_WINDOW_SECS: u64 = 3600;

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
    /// Access count per sub-chunk (for frequency-weighted eviction).
    /// Capped at ACCESS_COUNT_CAP to prevent immortal sub-chunks.
    pub(crate) access_counts: HashMap<SubChunkId, u32>,
    /// Current total cached bytes (sub-chunk granularity). Updated inside mutex
    /// alongside `present` for perfect consistency with tracker state.
    pub(crate) current_cache_bytes: u64,
    /// Bytes per sub-chunk (sub_pages_per_frame * page_size). Set at construction
    /// or updated via set_sub_chunk_byte_size() when page_size becomes known.
    pub(crate) sub_chunk_byte_size: u64,
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
        let (present, tiers, access_counts) = Self::load_from_disk(&path, None);
        let now = Instant::now();
        let access_times: HashMap<SubChunkId, Instant> = present.iter().map(|id| (*id, now)).collect();
        Self {
            present,
            tiers,
            access_times,
            access_counts,
            current_cache_bytes: 0, // recomputed once sub_chunk_byte_size is set
            sub_chunk_byte_size: 0,
            pages_per_group,
            sub_pages_per_frame,
            path,
            #[cfg(feature = "encryption")]
            encryption_key: None,
        }
    }

    #[cfg(feature = "encryption")]
    pub(crate) fn new_encrypted(path: PathBuf, pages_per_group: u32, sub_pages_per_frame: u32, encryption_key: Option<[u8; 32]>) -> Self {
        let (present, tiers, access_counts) = Self::load_from_disk(&path, encryption_key.as_ref());
        let now = Instant::now();
        let access_times: HashMap<SubChunkId, Instant> = present.iter().map(|id| (*id, now)).collect();
        Self {
            present,
            tiers,
            access_times,
            access_counts,
            current_cache_bytes: 0,
            sub_chunk_byte_size: 0,
            pages_per_group,
            sub_pages_per_frame,
            path,
            encryption_key,
        }
    }

    /// Compute which sub-chunk a page belongs to (LEGACY: positional mapping).
    /// Only valid for positional groups. For B-tree-aware groups, use sub_chunk_id_for().
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    pub(crate) fn is_present(&self, page_num: u64) -> bool {
        let id = self.sub_chunk_for_page(page_num);
        self.present.contains(&id)
    }

    /// Check if a specific sub-chunk is cached.
    pub(crate) fn is_sub_chunk_present(&self, id: &SubChunkId) -> bool {
        self.present.contains(id)
    }

    /// Set the sub-chunk byte size (sub_pages_per_frame * page_size).
    /// Called once page_size is known. Recomputes current_cache_bytes from present count.
    pub(crate) fn set_sub_chunk_byte_size(&mut self, byte_size: u64) {
        self.sub_chunk_byte_size = byte_size;
        self.current_cache_bytes = self.present.len() as u64 * byte_size;
    }

    /// Bytes used by Pinned sub-chunks (cache floor, never evicted).
    pub(crate) fn pinned_bytes(&self) -> u64 {
        self.count_tier(SubChunkTier::Pinned) as u64 * self.sub_chunk_byte_size
    }

    /// Record a sub-chunk as cached with the given tier.
    /// If already present at a higher priority (lower tier number), keeps the higher priority.
    pub(crate) fn mark_present(&mut self, id: SubChunkId, tier: SubChunkTier) {
        let is_new = self.present.insert(id);
        if is_new {
            self.current_cache_bytes += self.sub_chunk_byte_size;
        }
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
        let is_new = self.present.insert(id);
        if is_new {
            self.current_cache_bytes += self.sub_chunk_byte_size;
        }
        self.tiers.insert(id, SubChunkTier::Pinned);
        self.access_times.entry(id).or_insert_with(Instant::now);
    }

    /// Promote a sub-chunk to Index tier (index leaf pages).
    /// Also ensures the sub-chunk is in the present set.
    pub(crate) fn mark_index(&mut self, id: SubChunkId) {
        let is_new = self.present.insert(id);
        if is_new {
            self.current_cache_bytes += self.sub_chunk_byte_size;
        }
        let existing = self.tiers.get(&id).copied();
        match existing {
            Some(SubChunkTier::Pinned) => {} // don't demote from pinned
            _ => { self.tiers.insert(id, SubChunkTier::Index); }
        }
        self.access_times.entry(id).or_insert_with(Instant::now);
    }

    /// Touch a sub-chunk: update access time and increment access count (capped).
    pub(crate) fn touch(&mut self, id: SubChunkId) {
        self.access_times.insert(id, Instant::now());
        let count = self.access_counts.entry(id).or_insert(0);
        if *count < ACCESS_COUNT_CAP {
            *count += 1;
        }
    }

    /// Touch the sub-chunk containing a page.
    #[allow(dead_code)]
    pub(crate) fn touch_page(&mut self, page_num: u64) {
        let id = self.sub_chunk_for_page(page_num);
        self.touch(id);
    }

    /// Evict one sub-chunk: pick the lowest-priority (highest tier number),
    /// least-recently-used sub-chunk. Returns the evicted sub-chunk or None if empty.
    /// Never evicts Pinned (tier 0) sub-chunks.
    #[allow(dead_code)]
    pub(crate) fn evict_one(&mut self) -> Option<SubChunkId> {
        self.evict_one_skipping(&HashSet::new())
    }

    /// Evict one sub-chunk, skipping groups in skip_groups (dirty/pending/fetching).
    /// Uses weighted scoring: tier bonus (dominant) + recency + frequency.
    /// Lower score = more evictable. Never evicts Pinned sub-chunks.
    ///
    /// Score = tier_bonus + recency_score + frequency_score
    /// - tier_bonus: Data=0.0, Index=10.0 (additive, so ANY Index always beats ANY Data)
    /// - recency_score: 0.0 (oldest, 1hr+) to 1.0 (just accessed)
    /// - frequency_score: 0.0 (never touched) to 1.0 (hit ACCESS_COUNT_CAP times)
    ///
    /// Data scores range [0.0, 2.0]. Index scores range [10.0, 12.0].
    /// Tier always dominates: the hottest Data (2.0) is evicted before the coldest Index (10.0).
    /// Within a tier, frequency + recency break ties.
    #[allow(dead_code)]
    pub(crate) fn evict_one_skipping(&mut self, skip_groups: &HashSet<u64>) -> Option<SubChunkId> {
        let now = Instant::now();
        let max_age = Duration::from_secs(RECENCY_WINDOW_SECS);
        let epoch = now - max_age;
        let cap = ACCESS_COUNT_CAP as f64;

        let mut candidate: Option<(SubChunkId, f64)> = None; // (id, score)
        for id in &self.present {
            let tier = self.tiers.get(id).copied().unwrap_or(SubChunkTier::Data);
            if tier == SubChunkTier::Pinned {
                continue;
            }
            if skip_groups.contains(&(id.group_id as u64)) {
                continue;
            }

            let tier_bonus = match tier {
                SubChunkTier::Data => 0.0,
                SubChunkTier::Index => 10.0,
                SubChunkTier::Pinned => unreachable!(),
            };

            let access = self.access_times.get(id).copied().unwrap_or(epoch);
            let age_secs = now.duration_since(access).as_secs_f64();
            let recency_score = 1.0 - (age_secs / max_age.as_secs_f64()).min(1.0);

            let count = self.access_counts.get(id).copied().unwrap_or(0) as f64;
            let frequency_score = (count / cap).min(1.0);

            let score = tier_bonus + recency_score + frequency_score;

            match &candidate {
                None => candidate = Some((*id, score)),
                Some((_, cs)) => {
                    if score < *cs {
                        candidate = Some((*id, score));
                    }
                }
            }
        }
        if let Some((id, _)) = candidate {
            self.remove(id);
            Some(id)
        } else {
            None
        }
    }

    /// Score all evictable sub-chunks without removing them. Returns (id, score) pairs.
    /// Used by evict_to_budget to sort once and evict in order (O(n log n) vs O(n^2)).
    pub(crate) fn score_evictable(&self, skip_groups: &HashSet<u64>) -> Vec<(SubChunkId, f64)> {
        let now = Instant::now();
        let max_age = Duration::from_secs(RECENCY_WINDOW_SECS);
        let epoch = now - max_age;
        let cap = ACCESS_COUNT_CAP as f64;

        let mut scored = Vec::new();
        for id in &self.present {
            let tier = self.tiers.get(id).copied().unwrap_or(SubChunkTier::Data);
            if tier == SubChunkTier::Pinned {
                continue;
            }
            if skip_groups.contains(&(id.group_id as u64)) {
                continue;
            }

            let tier_bonus = match tier {
                SubChunkTier::Data => 0.0,
                SubChunkTier::Index => 10.0,
                SubChunkTier::Pinned => unreachable!(),
            };

            let access = self.access_times.get(id).copied().unwrap_or(epoch);
            let age_secs = now.duration_since(access).as_secs_f64();
            let recency_score = 1.0 - (age_secs / max_age.as_secs_f64()).min(1.0);

            let count = self.access_counts.get(id).copied().unwrap_or(0) as f64;
            let frequency_score = (count / cap).min(1.0);

            scored.push((*id, tier_bonus + recency_score + frequency_score));
        }
        scored
    }

    /// Evict all sub-chunks in a specific tier that are older than `ttl`.
    #[allow(dead_code)]
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
        if self.present.remove(&id) {
            self.current_cache_bytes = self.current_cache_bytes.saturating_sub(self.sub_chunk_byte_size);
        }
        self.tiers.remove(&id);
        self.access_times.remove(&id);
        self.access_counts.remove(&id);
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
    #[allow(dead_code)]
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
        self.access_counts.clear();
        self.current_cache_bytes = 0;
    }

    /// Number of tracked sub-chunks.
    #[allow(dead_code)]
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
    /// Format v2: JSON vec of (SubChunkId, tier_u8, access_count_u32).
    /// Backward-compatible: load_from_disk also reads v1 format (SubChunkId, tier_u8).
    /// CTR-encrypted with random nonce if key present.
    pub(crate) fn persist(&self) -> io::Result<()> {
        let entries: Vec<(SubChunkId, u8, u32)> = self.present.iter()
            .map(|id| {
                let tier = self.tiers.get(id).copied().unwrap_or(SubChunkTier::Data) as u8;
                let count = self.access_counts.get(id).copied().unwrap_or(0);
                (*id, tier, count)
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
    /// Tries v2 format (id, tier, count) first, falls back to v1 (id, tier).
    /// Encrypted format: [8-byte random nonce][CTR-encrypted JSON]
    pub(crate) fn load_from_disk(path: &Path, _encryption_key: Option<&[u8; 32]>) -> (HashSet<SubChunkId>, HashMap<SubChunkId, SubChunkTier>, HashMap<SubChunkId, u32>) {
        let data = match fs::read(path) {
            Ok(d) => d,
            Err(_) => return (HashSet::new(), HashMap::new(), HashMap::new()),
        };
        #[cfg(feature = "encryption")]
        let data = if let Some(key) = _encryption_key {
            if data.len() < 8 {
                return (HashSet::new(), HashMap::new(), HashMap::new());
            }
            let nonce_u64 = u64::from_le_bytes(data[..8].try_into().unwrap());
            match compress::decrypt_ctr(&data[8..], nonce_u64, key) {
                Ok(d) => d,
                Err(_) => return (HashSet::new(), HashMap::new(), HashMap::new()),
            }
        } else {
            data
        };

        // Try v2 format: (SubChunkId, tier_u8, access_count_u32)
        if let Ok(entries) = serde_json::from_slice::<Vec<(SubChunkId, u8, u32)>>(&data) {
            let mut present = HashSet::new();
            let mut tiers = HashMap::new();
            let mut counts = HashMap::new();
            for (id, tier_byte, count) in entries {
                present.insert(id);
                let tier = match tier_byte {
                    0 => SubChunkTier::Pinned,
                    1 => SubChunkTier::Index,
                    _ => SubChunkTier::Data,
                };
                tiers.insert(id, tier);
                if count > 0 {
                    counts.insert(id, count);
                }
            }
            return (present, tiers, counts);
        }

        // Fall back to v1 format: (SubChunkId, tier_u8)
        if let Ok(entries) = serde_json::from_slice::<Vec<(SubChunkId, u8)>>(&data) {
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
            return (present, tiers, HashMap::new());
        }

        (HashSet::new(), HashMap::new(), HashMap::new())
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
#[path = "test_cache_tracking.rs"]
mod tests;

