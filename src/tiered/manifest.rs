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

    // --- Phase Midway: B-tree-aware page groups ---

    /// Explicit page-to-group mapping. group_pages[gid] = ordered list of page numbers
    /// in that group. Empty = legacy positional mapping (gid = page_num / ppg).
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
            group_pages: Vec::new(),
            btrees: HashMap::new(),
            page_index: HashMap::new(),
            btree_groups: HashMap::new(),
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
    pub fn build_page_index(&mut self) {
        self.page_index.clear();
        for (gid, pages) in self.group_pages.iter().enumerate() {
            for (idx, &page_num) in pages.iter().enumerate() {
                self.page_index.insert(page_num, PageLocation {
                    group_id: gid as u64,
                    index: idx as u32,
                });
            }
        }
        // Build btree_groups: for each group, store all sibling groups from the same B-tree
        self.btree_groups.clear();
        for (_root, entry) in &self.btrees {
            for &gid in &entry.group_ids {
                self.btree_groups.insert(gid, entry.group_ids.clone());
            }
        }
    }

    /// Look up where a page lives. Returns None only if page_num is beyond manifest.
    pub fn page_location(&self, page_num: u64) -> Option<PageLocation> {
        self.page_index.get(&page_num).copied()
    }
}

