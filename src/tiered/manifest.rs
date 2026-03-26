use std::borrow::Cow;

use super::*;

/// S3 manifest — updated atomically after all page group uploads.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Monotonically increasing version (bumped on each checkpoint)
    pub version: u64,
    /// Number of pages in the database
    pub page_count: u64,
    /// Page size in bytes
    pub page_size: u32,
    /// Pages per page group
    #[serde(default = "default_pages_per_group")]
    pub pages_per_group: u32,
    /// Map groupId → S3 key. Versioned: "pg/{gid}_v{version}"
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

    /// Grouping strategy used to build this manifest.
    /// Positional (legacy): gid = page_num / ppg. BTreeAware: explicit mapping.
    #[serde(default = "default_strategy")]
    pub strategy: GroupingStrategy,

    // --- Phase Midway: B-tree-aware page groups ---

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

    /// Phase Verdun-i: reverse index page_num -> B-tree name (table/index name).
    /// Built on load from `btrees`, not serialized. Survives VACUUM (names stable).
    #[serde(skip)]
    pub page_to_tree_name: HashMap<u64, String>,

    /// Phase Verdun-i: reverse index tree_name -> group IDs.
    /// Built on load from `btrees`, not serialized.
    #[serde(skip)]
    pub tree_name_to_groups: HashMap<String, Vec<u64>>,

    /// Reverse index: group_id -> B-tree name. Built on load, not serialized.
    /// Used by per-query prefetch schedule selection (SEARCH vs default hops).
    #[serde(skip)]
    pub group_to_tree_name: HashMap<u64, String>,

    /// Phase Verdun: B-tree access frequency for prediction confidence and decay.
    /// Keyed by tree name (survives VACUUM).
    #[serde(default)]
    pub btree_access_freq: HashMap<String, f32>,

    /// Phase Verdun-i: persisted prediction patterns (name set, confidence).
    #[serde(default)]
    pub prediction_patterns: Vec<(std::collections::BTreeSet<String>, f32)>,
}

fn default_strategy() -> GroupingStrategy {
    // Default to Positional for backward compat with manifests that lack this field.
    GroupingStrategy::Positional
}

/// A single frame entry in a seekable page group. Points to a byte range within the S3 object
/// containing an independently-decompressible sub-chunk of pages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrameEntry {
    /// Byte offset from start of the S3 object
    pub offset: u64,
    /// Compressed length in bytes
    pub len: u32,
}

pub(crate) fn default_pages_per_group() -> u32 {
    DEFAULT_PAGES_PER_GROUP
}

impl Manifest {
    pub(crate) fn empty() -> Self {
        Self {
            version: 0,
            page_count: 0,
            page_size: 0,
            pages_per_group: 0,
            page_group_keys: Vec::new(),
            interior_chunk_keys: HashMap::new(),
            index_chunk_keys: HashMap::new(),
            frame_tables: Vec::new(),
            sub_pages_per_frame: 0,
            strategy: GroupingStrategy::Positional,
            group_pages: Vec::new(),
            btrees: HashMap::new(),
            page_index: HashMap::new(),
            btree_groups: HashMap::new(),
            page_to_tree_name: HashMap::new(),
            tree_name_to_groups: HashMap::new(),
            group_to_tree_name: HashMap::new(),
            btree_access_freq: HashMap::new(),
            prediction_patterns: Vec::new(),
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
                self.page_index.insert(page_num, PageLocation {
                    group_id: gid as u64,
                    index: idx as u32,
                });
            }
        }
        // Build btree_groups + page_to_tree_name + tree_name_to_groups from B-tree manifest entries
        for (_, entry) in &self.btrees {
            for &gid in &entry.group_ids {
                self.btree_groups.insert(gid, entry.group_ids.clone());
                self.group_to_tree_name.insert(gid, entry.name.clone());
            }
            // Phase Verdun-i: reverse index from pages -> tree name
            for &gid in &entry.group_ids {
                if let Some(pages) = self.group_pages.get(gid as usize) {
                    for &page_num in pages {
                        self.page_to_tree_name.insert(page_num, entry.name.clone());
                    }
                }
            }
            // Phase Verdun-i: tree name -> group IDs
            self.tree_name_to_groups.insert(entry.name.clone(), entry.group_ids.clone());
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
            GroupingStrategy::BTreeAware => {
                self.page_index.get(&page_num).copied()
            }
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
            GroupingStrategy::BTreeAware => {
                Cow::Borrowed(self.group_pages.get(gid as usize)
                    .expect("BTreeAware group must exist in group_pages")
                    .as_slice())
            }
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
            GroupingStrategy::BTreeAware => {
                self.group_pages.get(gid as usize).map(|v| v.len()).unwrap_or(0)
            }
        }
    }

    /// Select prefetch neighbors based on strategy.
    /// Positional: radial fan-out from current gid.
    /// BTreeAware: sibling groups from the same B-tree.
    pub fn prefetch_neighbors(&self, gid: u64) -> PrefetchNeighbors {
        match self.strategy {
            GroupingStrategy::Positional => {
                PrefetchNeighbors::RadialFanout { total_groups: self.total_groups() }
            }
            GroupingStrategy::BTreeAware => {
                PrefetchNeighbors::BTreeSiblings(
                    self.btree_groups.get(&gid).cloned().unwrap_or_default()
                )
            }
        }
    }

    /// Detect strategy from manifest contents (for backward compat with manifests
    /// that lack the strategy field). Call after deserialization.
    pub fn detect_and_normalize_strategy(&mut self) {
        if !self.group_pages.is_empty() && self.strategy == GroupingStrategy::Positional {
            self.strategy = GroupingStrategy::BTreeAware;
        }
        self.build_page_index();
    }
}

#[cfg(test)]
#[path = "test_manifest.rs"]
mod tests;

