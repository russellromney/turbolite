use std::borrow::Cow;

use super::*;

/// Remote manifest, updated atomically after all page group uploads.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Monotonically increasing version (bumped +1 on each checkpoint).
    /// Used for S3 key uniqueness: `pg/{gid}_v{version}`.
    pub version: u64,
    /// SQLite file change counter (page 0, offset 24) at checkpoint time.
    /// Used by walrust for WAL segment replay: replay segments with txid > change_counter.
    /// Default 0 for backward compat (walrust replays everything).
    #[serde(default)]
    pub change_counter: u64,
    /// Number of pages in the database
    pub page_count: u64,
    /// Page size in bytes
    pub page_size: u32,
    /// Pages per page group
    #[serde(default = "default_pages_per_group")]
    pub pages_per_group: u32,
    /// Map groupId → S3 key. Versioned: "p/d/{gid}_v{version}"
    #[serde(default)]
    pub page_group_keys: Vec<String>,
    /// Chunked interior bundle: chunk_id → S3 key. Each chunk covers bundle_chunk_range(page_size) page numbers.
    /// Fetched in parallel on connection open so B-tree traversal is all cache hits.
    #[serde(default)]
    pub interior_chunk_keys: HashMap<u32, String>,
    /// Chunked index leaf bundle: chunk_id → S3 key. Same chunking as interior bundles.
    /// Index leaf pages (0x0A) stored separately from data page groups for eager parallel fetch.
    #[serde(default)]
    pub index_chunk_keys: HashMap<u32, String>,
    /// Per-group frame table for seekable page groups (multi-frame encoding).
    /// frame_tables[gid] = vec of FrameEntry for each sub-chunk.
    /// Empty or missing means legacy single-frame format (full download required).
    #[serde(default)]
    pub frame_tables: Vec<Vec<FrameEntry>>,
    /// Pages per sub-chunk frame (for seekable page groups). Default 0 = legacy format.
    #[serde(default)]
    pub sub_pages_per_frame: u32,

    /// Per-group subframe overrides. Indexed by group_id, keyed by frame_index.
    #[serde(default)]
    pub subframe_overrides: Vec<HashMap<usize, SubframeOverride>>,

    /// Grouping strategy used to build this manifest.
    /// Positional (legacy): gid = page_num / ppg. BTreeAware: explicit mapping.
    #[serde(default = "default_strategy")]
    pub strategy: GroupingStrategy,

    // B-tree-aware page groups
    /// Explicit page-to-group mapping. group_pages[gid] = ordered list of page numbers
    /// in that group. Empty for Positional strategy (computed on the fly).
    #[serde(default)]
    pub group_pages: Vec<Vec<u64>>,

    /// B-tree map: root_page (0-based) -> B-tree info + group IDs.
    #[serde(default)]
    pub btrees: HashMap<u64, BTreeManifestEntry>,

    /// Reverse index: page_num -> (group_id, position). Built on load, not serialized.
    #[serde(skip)]
    pub page_index: HashMap<u64, PageLocation>,

    /// Demand-driven prefetch: group_id -> sibling group_ids from the same B-tree.
    /// Built on load from `btrees`, not serialized.
    #[serde(skip)]
    pub btree_groups: HashMap<u64, Vec<u64>>,

    /// Reverse index page_num -> B-tree name (table/index name).
    /// Built on load from `btrees`, not serialized. Survives VACUUM (names stable).
    #[serde(skip)]
    pub page_to_tree_name: HashMap<u64, String>,

    /// Reverse index tree_name -> group IDs.
    /// Built on load from `btrees`, not serialized.
    #[serde(skip)]
    pub tree_name_to_groups: HashMap<String, Vec<u64>>,

    /// Reverse index: group_id -> B-tree name. Built on load, not serialized.
    /// Used by per-query prefetch schedule selection (SEARCH vs default hops).
    #[serde(skip)]
    pub group_to_tree_name: HashMap<u64, String>,

    /// Full content of page 0 (SQLite's page 1: database header + root table).
    /// Stored in manifest so multiwriter catch-up can write it to local cache,
    /// giving SQLite the correct database header (page count, schema cookie)
    /// without fetching from S3 or reopening the connection.
    /// None for manifests created before this field was added.
    ///
    /// Phase Strata note: this field intentionally has no `skip_serializing_if`.
    /// Under rmp_serde's positional encoding, a conditionally-skipped field in
    /// the middle of the struct shifts every subsequent field's array index,
    /// which breaks deserialization whenever a new field is added after it.
    /// Always-serialize keeps the positional layout stable; `None` is encoded
    /// as a single nil byte in msgpack, so the cost is negligible.
    #[serde(default)]
    pub db_header: Option<Vec<u8>>,

    /// Discontinuity stamp. Bumped ONLY by out-of-band operations that make
    /// the prior cache invalid (admin-driven fork/rollback/restore). Normal
    /// checkpoints preserve it. Consumers that see a remote manifest whose
    /// epoch differs from their cached manifest's epoch treat their local
    /// cache as stale and cold-start from the remote.
    ///
    /// Placed at the end of the struct so adding it doesn't shift any prior
    /// field's positional index — pre-Strata manifest.msgpack bytes already
    /// on disk deserialize cleanly (missing trailing element → serde default
    /// fills `epoch = 0`).
    #[serde(default)]
    pub epoch: u64,
}

fn default_strategy() -> GroupingStrategy {
    // Default to Positional for backward compat with manifests that lack this field.
    GroupingStrategy::Positional
}

/// A single frame entry in a seekable page group. Points to a byte range within the S3 object
/// containing an independently-decompressible sub-chunk of pages.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FrameEntry {
    /// Byte offset from start of the S3 object
    pub offset: u64,
    /// Compressed length in bytes
    pub len: u32,
}

/// A subframe override entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SubframeOverride {
    /// S3 key for the override object
    pub key: String,
    /// Frame entry (offset always 0, len is full object size)
    pub entry: FrameEntry,
}

pub(crate) fn default_pages_per_group() -> u32 {
    DEFAULT_PAGES_PER_GROUP
}

// Local manifest cache + dirty-group recovery state
//
// Two files, two concerns.
//
//   {cache_dir}/manifest.msgpack        msgpack(Manifest)     -- warm cache of the
//                                                                remote manifest
//   {cache_dir}/dirty_groups.msgpack    msgpack(Vec<u64>)     -- groups staged for
//                                                                upload, survives
//                                                                process crash
//
// Splitting them means the backend's own `manifest.msgpack` object is a raw
// Manifest (not a turbolite-specific wrapper), and dirty_groups stops leaking
// into what the backend sees.

/// Persist a `Manifest` to the local cache directory as a warm cache for cold
/// reopens. Atomic write (tmp + rename). Used by local mode as the
/// authoritative source and by remote mode as a hint for warm reconnect.
pub(crate) fn persist_manifest_local(cache_dir: &Path, manifest: &Manifest) -> io::Result<()> {
    let path = cache_dir.join("manifest.msgpack");
    let tmp = cache_dir.join("manifest.msgpack.tmp");
    let data = rmp_serde::to_vec(manifest)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("serialize manifest: {e}")))?;
    fs::write(&tmp, &data)?;
    fs::rename(&tmp, &path)?;
    Ok(())
}

/// Pre-Anvil-g wrapper: the old on-disk / on-S3 layout bundled the manifest
/// with the uploaded-but-unflushed dirty_groups into a single msgpack
/// object under `manifest.msgpack`. We split them in Anvil g so the
/// backend sees only the Manifest bytes and dirty_groups stays local, but
/// upgrades from an older stack may still encounter wrapper-format files.
/// `decode_manifest_bytes` handles both formats transparently.
#[derive(serde::Deserialize)]
struct LegacyLocalManifestWrapper {
    manifest: Manifest,
    #[serde(default)]
    #[allow(dead_code)]
    dirty_groups: Vec<u64>,
}

/// Decode a raw manifest.msgpack byte blob. Tries the Anvil-g raw
/// `Manifest` layout first; on failure, falls back to the pre-Anvil-g
/// `LocalManifest { manifest, dirty_groups }` wrapper and extracts the
/// inner manifest. Returns Err only if neither format decodes.
///
/// This is the single choke point for manifest deserialisation; every
/// caller (local cache load, backend get, staging-log trailer) routes
/// through here so the compat path exists exactly once.
pub(crate) fn decode_manifest_bytes(data: &[u8]) -> io::Result<Manifest> {
    if let Ok(m) = rmp_serde::from_slice::<Manifest>(data) {
        return Ok(m);
    }
    match rmp_serde::from_slice::<LegacyLocalManifestWrapper>(data) {
        Ok(wrapper) => {
            eprintln!(
                "[turbolite] manifest.msgpack is in pre-Anvil-g LocalManifest wrapper format; \
                 decoded and will be rewritten in the new format on next persist"
            );
            Ok(wrapper.manifest)
        }
        Err(e) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("deserialize manifest (neither Anvil-g nor legacy format): {e}"),
        )),
    }
}

/// Load a locally-cached `Manifest`. `Ok(None)` if absent. Callers that need
/// crash-recovery dirty groups read them from [`load_dirty_groups`] separately.
pub(crate) fn load_manifest_local(cache_dir: &Path) -> io::Result<Option<Manifest>> {
    let path = cache_dir.join("manifest.msgpack");
    let data = match fs::read(&path) {
        Ok(d) => d,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    Ok(Some(decode_manifest_bytes(&data)?))
}

/// Persist the set of page-group ids that have been checkpointed locally but
/// not yet flushed to the remote backend. Empty list means "no pending work";
/// file is removed in that case to keep the cache dir tidy.
pub(crate) fn persist_dirty_groups(cache_dir: &Path, dirty: &[u64]) -> io::Result<()> {
    let path = cache_dir.join("dirty_groups.msgpack");
    if dirty.is_empty() {
        match fs::remove_file(&path) {
            Ok(()) => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        }
    }
    let tmp = cache_dir.join("dirty_groups.msgpack.tmp");
    let data = rmp_serde::to_vec(&dirty.to_vec()).map_err(|e| {
        io::Error::new(io::ErrorKind::Other, format!("serialize dirty_groups: {e}"))
    })?;
    fs::write(&tmp, &data)?;
    fs::rename(&tmp, &path)?;
    Ok(())
}

/// Load dirty-group recovery state. `Ok(Vec::new())` if no file exists.
pub(crate) fn load_dirty_groups(cache_dir: &Path) -> io::Result<Vec<u64>> {
    let path = cache_dir.join("dirty_groups.msgpack");
    let data = match fs::read(&path) {
        Ok(d) => d,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let dirty: Vec<u64> = rmp_serde::from_slice(&data).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("deserialize dirty_groups: {e}"),
        )
    })?;
    Ok(dirty)
}

impl Manifest {
    pub(crate) fn empty() -> Self {
        Self {
            version: 0,
            change_counter: 0,
            page_count: 0,
            page_size: 0,
            pages_per_group: 0,
            page_group_keys: Vec::new(),
            interior_chunk_keys: HashMap::new(),
            index_chunk_keys: HashMap::new(),
            frame_tables: Vec::new(),
            sub_pages_per_frame: 0,
            subframe_overrides: Vec::new(),
            strategy: GroupingStrategy::Positional,
            group_pages: Vec::new(),
            btrees: HashMap::new(),
            page_index: HashMap::new(),
            btree_groups: HashMap::new(),
            page_to_tree_name: HashMap::new(),
            tree_name_to_groups: HashMap::new(),
            group_to_tree_name: HashMap::new(),
            db_header: None,
            epoch: 0,
        }
    }

    pub(crate) fn total_groups(&self) -> u64 {
        if !self.group_pages.is_empty() {
            return self.group_pages.len() as u64;
        }
        if self.pages_per_group == 0 || self.page_count == 0 {
            return 0;
        }
        (self.page_count + self.pages_per_group as u64 - 1) / self.pages_per_group as u64
    }

    /// Build the reverse index (page_num -> PageLocation) from group_pages.
    /// No-op for Positional strategy (page_location computed arithmetically).
    /// Auto-detects BTreeAware if group_pages is populated (backward compat).
    pub fn build_page_index(&mut self) {
        self.page_index.clear();
        self.btree_groups.clear();
        self.page_to_tree_name.clear();
        self.tree_name_to_groups.clear();
        self.group_to_tree_name.clear();
        // Auto-detect: if group_pages is populated, this is BTreeAware
        if !self.group_pages.is_empty() && self.strategy == GroupingStrategy::Positional {
            self.strategy = GroupingStrategy::BTreeAware;
        }

        if self.strategy == GroupingStrategy::Positional {
            return;
        }

        for (gid, pages) in self.group_pages.iter().enumerate() {
            for (idx, &page_num) in pages.iter().enumerate() {
                self.page_index.insert(
                    page_num,
                    PageLocation {
                        group_id: gid as u64,
                        index: idx as u32,
                    },
                );
            }
        }
        // Build btree_groups + page_to_tree_name + tree_name_to_groups from B-tree manifest entries
        for (_, entry) in &self.btrees {
            for &gid in &entry.group_ids {
                self.btree_groups.insert(gid, entry.group_ids.clone());
                self.group_to_tree_name.insert(gid, entry.name.clone());
            }
            // Reverse index from pages -> tree name
            for &gid in &entry.group_ids {
                if let Some(pages) = self.group_pages.get(gid as usize) {
                    for &page_num in pages {
                        self.page_to_tree_name.insert(page_num, entry.name.clone());
                    }
                }
            }
            // Tree name -> group IDs
            self.tree_name_to_groups
                .insert(entry.name.clone(), entry.group_ids.clone());
        }
    }

    /// Look up where a page lives. Dispatches by strategy:
    /// Positional: arithmetic (gid = page_num / ppg, idx = page_num % ppg).
    /// BTreeAware: HashMap lookup in page_index.
    pub fn page_location(&self, page_num: u64) -> Option<PageLocation> {
        match self.strategy {
            GroupingStrategy::Positional => {
                if self.pages_per_group == 0 || page_num >= self.page_count {
                    return None;
                }
                let ppg = self.pages_per_group as u64;
                Some(PageLocation {
                    group_id: page_num / ppg,
                    index: (page_num % ppg) as u32,
                })
            }
            GroupingStrategy::BTreeAware => self.page_index.get(&page_num).copied(),
        }
    }

    /// Get the list of page numbers in a group. Dispatches by strategy:
    /// Positional: returns [gid*ppg .. min((gid+1)*ppg, page_count)].
    /// BTreeAware: borrows from group_pages[gid] (zero-allocation).
    pub fn group_page_nums(&self, gid: u64) -> Cow<'_, [u64]> {
        match self.strategy {
            GroupingStrategy::Positional => {
                let ppg = self.pages_per_group as u64;
                let start = gid * ppg;
                let end = std::cmp::min(start + ppg, self.page_count);
                Cow::Owned((start..end).collect())
            }
            GroupingStrategy::BTreeAware => Cow::Borrowed(
                self.group_pages
                    .get(gid as usize)
                    .expect("BTreeAware group must exist in group_pages")
                    .as_slice(),
            ),
        }
    }

    /// Number of pages in a group (without allocating a Vec).
    pub fn group_size(&self, gid: u64) -> usize {
        match self.strategy {
            GroupingStrategy::Positional => {
                let ppg = self.pages_per_group as u64;
                let start = gid * ppg;
                std::cmp::min(ppg, self.page_count.saturating_sub(start)) as usize
            }
            GroupingStrategy::BTreeAware => self
                .group_pages
                .get(gid as usize)
                .map(|v| v.len())
                .unwrap_or(0),
        }
    }

    /// Get sibling groups from the same B-tree for prefetch.
    pub fn prefetch_siblings(&self, gid: u64) -> Vec<u64> {
        self.btree_groups.get(&gid).cloned().unwrap_or_default()
    }

    /// Ensure subframe_overrides vec length matches page_group_keys.
    pub fn normalize_overrides(&mut self) {
        while self.subframe_overrides.len() < self.page_group_keys.len() {
            self.subframe_overrides.push(HashMap::new());
        }
    }

    /// Detect strategy from manifest contents (for backward compat with manifests
    /// that lack the strategy field). Call after deserialization.
    pub fn detect_and_normalize_strategy(&mut self) {
        if !self.group_pages.is_empty() && self.strategy == GroupingStrategy::Positional {
            self.strategy = GroupingStrategy::BTreeAware;
        }
        self.normalize_overrides();
        self.build_page_index();
    }
}

/// Given dirty page numbers and a group's page list, return which
/// frame indices contain at least one dirty page.
pub(crate) fn dirty_frames_for_group(
    dirty_page_nums: &[u64],
    group_pages: &[u64],
    frame_table: &[FrameEntry],
    sub_pages_per_frame: u32,
) -> Vec<usize> {
    if sub_pages_per_frame == 0 || frame_table.is_empty() {
        return Vec::new();
    }
    let mut dirty_frame_set: HashSet<usize> = HashSet::new();
    for &dirty_pnum in dirty_page_nums {
        if let Some(pos) = group_pages.iter().position(|&p| p == dirty_pnum) {
            let frame_idx = pos / sub_pages_per_frame as usize;
            // Include frames beyond the base frame_table: new pages assigned
            // to this group may land in frames that don't exist in the base
            // page group yet. Override frames are standalone S3 objects, so
            // they don't require a base frame_table entry.
            dirty_frame_set.insert(frame_idx);
        }
    }
    let mut result: Vec<usize> = dirty_frame_set.into_iter().collect();
    result.sort_unstable();
    result
}

#[cfg(test)]
#[path = "test_manifest.rs"]
mod tests;
