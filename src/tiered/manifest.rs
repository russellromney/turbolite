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

    /// Phase Midway-i: reverse index group_id -> B-tree name.
    /// Built on load from `btrees`, not serialized. Used by range-GET budget per tree.
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
            // Phase Midway-i: group ID -> tree name (for range-GET budget per tree)
            for &gid in &entry.group_ids {
                self.group_to_tree_name.insert(gid, entry.name.clone());
            }
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
mod tests {
    use super::*;
    use crate::tiered::*;
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};
    use parking_lot::RwLock;

    // =========================================================================
    // Manifest
    // =========================================================================

    #[test]
    fn test_manifest_total_groups() {
        let m = Manifest {
            version: 1,
            page_count: 10000,
            page_size: 4096,
            pages_per_group: 2048,
            ..Manifest::empty()
        };
        assert_eq!(m.total_groups(), 5); // ceil(10000/2048)

        let m2 = Manifest {
            page_count: 2048,
            pages_per_group: 2048,
            ..Manifest::empty()
        };
        assert_eq!(m2.total_groups(), 1); // exact fit

        let m3 = Manifest {
            page_count: 2049,
            pages_per_group: 2048,
            ..Manifest::empty()
        };
        assert_eq!(m3.total_groups(), 2); // one extra page → 2 groups
    }

    #[test]
    fn test_manifest_total_groups_edge_cases() {
        assert_eq!(Manifest::empty().total_groups(), 0);
        let m = Manifest {
            page_count: 1,
            pages_per_group: 2048,
            ..Manifest::empty()
        };
        assert_eq!(m.total_groups(), 1); // 1 page → 1 group

        let m2 = Manifest {
            page_count: 0,
            pages_per_group: 2048,
            ..Manifest::empty()
        };
        assert_eq!(m2.total_groups(), 0);
    }

    #[test]
    fn test_manifest_empty() {
        let m = Manifest::empty();
        assert_eq!(m.version, 0);
        assert_eq!(m.page_count, 0);
        assert_eq!(m.page_size, 0);
        assert_eq!(m.pages_per_group, 0);
        assert!(m.page_group_keys.is_empty());
    }

    #[test]
    fn test_manifest_serde_roundtrip() {
        let m = Manifest {
            version: 42,
            page_count: 10000,
            page_size: 4096,
            pages_per_group: 2048,
            page_group_keys: vec![
                "pg/0_v1".to_string(),
                "pg/1_v1".to_string(),
                "pg/2_v1".to_string(),
            ],
            ..Manifest::empty()
        };
        let json = serde_json::to_string(&m).unwrap();
        let m2: Manifest = serde_json::from_str(&json).unwrap();
        assert_eq!(m.version, m2.version);
        assert_eq!(m.page_count, m2.page_count);
        assert_eq!(m.page_size, m2.page_size);
        assert_eq!(m.pages_per_group, m2.pages_per_group);
        assert_eq!(m.page_group_keys, m2.page_group_keys);
        assert_eq!(m.interior_chunk_keys, m2.interior_chunk_keys);
    }

    #[test]
    fn test_manifest_deserialize_missing_pages_per_group() {
        // Simulate old manifest without pages_per_group
        let json = r#"{"version":1,"page_count":100,"page_size":4096,"page_group_keys":[]}"#;
        let m: Manifest = serde_json::from_str(json).unwrap();
        assert_eq!(m.pages_per_group, DEFAULT_PAGES_PER_GROUP); // default
    }

    #[test]
    fn test_manifest_deserialize_missing_page_group_keys() {
        let json = r#"{"version":1,"page_count":100,"page_size":4096,"pages_per_group":512}"#;
        let m: Manifest = serde_json::from_str(json).unwrap();
        assert!(m.page_group_keys.is_empty()); // default
        assert_eq!(m.pages_per_group, 512);
    }

    #[test]
    fn test_manifest_deserialize_invalid_json() {
        let result: Result<Manifest, _> = serde_json::from_str("not valid json");
        assert!(result.is_err());
    }

    #[test]
    fn test_manifest_serialize_empty() {
        let m = Manifest::empty();
        let json = serde_json::to_string(&m).unwrap();
        assert!(json.contains("\"version\":0"));
        assert!(json.contains("\"page_count\":0"));
    }

    // =========================================================================
    // Seekable manifest serde
    // =========================================================================

    #[test]
    fn test_seekable_manifest_serde_with_frames() {
        let m = Manifest {
            version: 1,
            page_count: 128,
            page_size: 4096,
            pages_per_group: 64,
            page_group_keys: vec!["pg/0_v1".to_string(), "pg/1_v1".to_string()],
            frame_tables: vec![
                vec![
                    FrameEntry { offset: 0, len: 500 },
                    FrameEntry { offset: 500, len: 600 },
                ],
                vec![
                    FrameEntry { offset: 0, len: 450 },
                    FrameEntry { offset: 450, len: 550 },
                ],
            ],
            sub_pages_per_frame: 32,
            ..Manifest::empty()
        };
        let json = serde_json::to_string(&m).unwrap();
        let m2: Manifest = serde_json::from_str(&json).unwrap();
        assert_eq!(m2.sub_pages_per_frame, 32);
        assert_eq!(m2.frame_tables.len(), 2);
        assert_eq!(m2.frame_tables[0].len(), 2);
        assert_eq!(m2.frame_tables[0][0].offset, 0);
        assert_eq!(m2.frame_tables[0][0].len, 500);
    }

    #[test]
    fn test_seekable_manifest_backward_compat() {
        // Old manifest without frame_tables/sub_pages_per_frame
        let json = r#"{"version":1,"page_count":100,"page_size":4096,"pages_per_group":512,"page_group_keys":[]}"#;
        let m: Manifest = serde_json::from_str(json).unwrap();
        assert!(m.frame_tables.is_empty());
        assert_eq!(m.sub_pages_per_frame, 0);
    }

    // ── Phase Midway regression tests ──

    #[test]
    fn test_assign_new_pages_to_groups_basic() {
        let mut manifest = Manifest {
            page_count: 10,
            page_size: 4096,
            pages_per_group: 4,
            group_pages: vec![
                vec![0, 1, 2, 3],
                vec![4, 5, 6, 7],
            ],
            ..Manifest::empty()
        };
        manifest.build_page_index();

        // Pages 8 and 9 are new (not in page_index)
        let unassigned = vec![8, 9];
        TieredHandle::assign_new_pages_to_groups(&mut manifest, &unassigned, 4);

        // Should be added to a group and in page_index
        assert!(manifest.page_location(8).is_some());
        assert!(manifest.page_location(9).is_some());
        // All original pages still mapped correctly
        for p in 0..8 {
            assert!(manifest.page_location(p).is_some());
        }
    }

    #[test]
    fn test_assign_new_pages_fills_last_group_first() {
        let mut manifest = Manifest {
            page_count: 5,
            page_size: 4096,
            pages_per_group: 4,
            group_pages: vec![
                vec![0, 1, 2, 3], // full
                vec![4],          // room for 3 more
            ],
            ..Manifest::empty()
        };
        manifest.build_page_index();

        let unassigned = vec![5, 6];
        TieredHandle::assign_new_pages_to_groups(&mut manifest, &unassigned, 4);

        // Pages 5,6 should fill group 1 (had room)
        let loc5 = manifest.page_location(5).unwrap();
        let loc6 = manifest.page_location(6).unwrap();
        assert_eq!(loc5.group_id, 1);
        assert_eq!(loc6.group_id, 1);
        assert_eq!(manifest.group_pages.len(), 2); // no new groups needed
    }

    #[test]
    fn test_assign_new_pages_overflow_to_new_group() {
        let mut manifest = Manifest {
            page_count: 8,
            page_size: 4096,
            pages_per_group: 4,
            group_pages: vec![
                vec![0, 1, 2, 3], // full
                vec![4, 5, 6, 7], // full
            ],
            ..Manifest::empty()
        };
        manifest.build_page_index();

        let unassigned = vec![8, 9, 10, 11, 12];
        TieredHandle::assign_new_pages_to_groups(&mut manifest, &unassigned, 4);

        // 5 new pages, both groups full -> need new group(s)
        assert_eq!(manifest.group_pages.len(), 4); // 2 original + 2 new (4 + 1)
        for p in 8..=12 {
            assert!(manifest.page_location(p).is_some());
        }
        // First 4 go to group 2, last 1 to group 3
        assert_eq!(manifest.page_location(8).unwrap().group_id, 2);
        assert_eq!(manifest.page_location(12).unwrap().group_id, 3);
    }

    #[test]
    fn test_assign_new_pages_empty_input() {
        let mut manifest = Manifest {
            page_count: 4,
            page_size: 4096,
            pages_per_group: 4,
            group_pages: vec![vec![0, 1, 2, 3]],
            ..Manifest::empty()
        };
        manifest.build_page_index();
        let original_groups = manifest.group_pages.len();

        TieredHandle::assign_new_pages_to_groups(&mut manifest, &[], 4);

        assert_eq!(manifest.group_pages.len(), original_groups);
    }

    #[test]
    fn test_assign_new_pages_no_duplicate_assignments() {
        let mut manifest = Manifest {
            page_count: 4,
            page_size: 4096,
            pages_per_group: 4,
            group_pages: vec![vec![0, 1, 2, 3]],
            ..Manifest::empty()
        };
        manifest.build_page_index();

        let unassigned = vec![4, 5, 6, 7, 8, 9];
        TieredHandle::assign_new_pages_to_groups(&mut manifest, &unassigned, 4);

        // Each page should appear exactly once across all groups
        let mut all_pages: Vec<u64> = manifest.group_pages.iter().flatten().copied().collect();
        all_pages.sort_unstable();
        all_pages.dedup();
        let total: usize = manifest.group_pages.iter().map(|g| g.len()).sum();
        assert_eq!(all_pages.len(), total, "duplicate page in group_pages");
    }

    #[test]
    fn test_build_page_index_roundtrip() {
        let mut manifest = Manifest {
            page_count: 10,
            page_size: 4096,
            pages_per_group: 4,
            group_pages: vec![
                vec![5, 0, 3, 8],  // non-sequential
                vec![1, 7, 2],
                vec![9, 4, 6],
            ],
            ..Manifest::empty()
        };
        manifest.build_page_index();

        // Every page 0-9 should have a location
        for p in 0..10 {
            let loc = manifest.page_location(p).unwrap_or_else(|| panic!("missing page {}", p));
            // Verify the reverse: group_pages[gid][index] == p
            assert_eq!(manifest.group_pages[loc.group_id as usize][loc.index as usize], p);
        }
    }

    // ── Phase Midway: B-tree-aware page groups tests ──

    #[test]
    fn test_total_groups_btree_vs_positional() {
        // Positional: ceil(100/32) = 4
        let m = Manifest {
            page_count: 100,
            pages_per_group: 32,
            ..Manifest::empty()
        };
        assert_eq!(m.total_groups(), 4);

        // B-tree groups: explicit mapping with more groups than positional formula
        let m2 = Manifest {
            page_count: 100,
            pages_per_group: 32,
            group_pages: vec![
                vec![0, 1, 2], vec![3, 4, 5], vec![6, 7, 8], vec![9, 10, 11],
                vec![12, 13, 14, 15, 16, 17, 18, 19],  // extra group from B-tree packing
            ],
            ..Manifest::empty()
        };
        // B-tree mapping takes priority: 5 groups, not positional 4
        assert_eq!(m2.total_groups(), 5);
    }

    // =========================================================================
    // More B-tree manifest tests
    // =========================================================================

    #[test]
    fn test_manifest_btree_page_location_non_sequential() {
        // B-tree groups have non-sequential page numbers
        let mut manifest = Manifest {
            page_count: 20,
            page_size: 4096,
            pages_per_group: 8,
            group_pages: vec![
                vec![0, 5, 10, 15],     // gid=0: scattered pages from B-tree A
                vec![1, 2, 3, 4],       // gid=1: sequential pages from B-tree B
                vec![6, 7, 8, 9],       // gid=2: sequential from B-tree C
                vec![11, 12, 13, 14, 16, 17, 18, 19], // gid=3: remaining pages
            ],
            ..Manifest::empty()
        };
        manifest.build_page_index();

        // Verify page 10 is in gid=0 at index=2
        let loc = manifest.page_location(10).unwrap();
        assert_eq!(loc.group_id, 0);
        assert_eq!(loc.index, 2);

        // Page 3 is in gid=1 at index=2
        let loc = manifest.page_location(3).unwrap();
        assert_eq!(loc.group_id, 1);
        assert_eq!(loc.index, 2);

        // Page 18 is in gid=3 at index=6
        let loc = manifest.page_location(18).unwrap();
        assert_eq!(loc.group_id, 3);
        assert_eq!(loc.index, 6);
    }

    #[test]
    fn test_manifest_serde_roundtrip_btree_groups() {
        // group_pages and btrees must survive JSON serialization.
        // page_index is #[serde(skip)] and rebuilt from group_pages.
        let mut m = Manifest {
            version: 5,
            page_count: 20,
            page_size: 4096,
            pages_per_group: 8,
            page_group_keys: vec![
                "pg/0_v5".into(), "pg/1_v5".into(), "pg/2_v5".into(),
            ],
            group_pages: vec![
                vec![0, 5, 10, 15],     // B-tree A (scattered)
                vec![1, 2, 3, 4],       // B-tree B (sequential)
                vec![6, 7, 8, 9, 11, 12, 13, 14, 16, 17, 18, 19],
            ],
            btrees: {
                let mut h = HashMap::new();
                h.insert(0, BTreeManifestEntry {
                    name: "users".into(),
                    obj_type: "table".into(),
                    group_ids: vec![0],
                });
                h.insert(5, BTreeManifestEntry {
                    name: "idx_users_name".into(),
                    obj_type: "index".into(),
                    group_ids: vec![1],
                });
                h
            },
            ..Manifest::empty()
        };
        m.build_page_index();

        let json = serde_json::to_string(&m).unwrap();
        let mut m2: Manifest = serde_json::from_str(&json).unwrap();

        // group_pages survives
        assert_eq!(m.group_pages, m2.group_pages);
        // btrees survives
        assert_eq!(m.btrees.len(), m2.btrees.len());
        assert_eq!(m.btrees[&0].name, m2.btrees[&0].name);
        assert_eq!(m.btrees[&5].group_ids, m2.btrees[&5].group_ids);
        // page_index is NOT serialized (skip)
        assert!(m2.page_index.is_empty(), "page_index should be empty after deserialize");
        // Rebuild and verify
        m2.build_page_index();
        assert_eq!(m2.page_location(10).unwrap().group_id, 0);
        assert_eq!(m2.page_location(10).unwrap().index, 2);
        assert_eq!(m2.page_location(3).unwrap().group_id, 1);
        assert_eq!(m2.page_location(3).unwrap().index, 2);
        // total_groups uses group_pages, not positional
        assert_eq!(m2.total_groups(), 3);
        // btree_groups rebuilt correctly (group -> sibling group_ids)
        assert_eq!(m2.btree_groups.get(&0).unwrap(), &vec![0u64]);
        assert_eq!(m2.btree_groups.get(&1).unwrap(), &vec![1u64]);
        // group 2 has no btree entry, so no btree_groups mapping
        assert!(m2.btree_groups.get(&2).is_none());
        // Phase Verdun-i: page_to_tree_name reverse index rebuilt from btrees
        // B-tree root=0 ("users") owns group 0 with pages [0, 5, 10, 15]
        assert_eq!(m2.page_to_tree_name.get(&0).map(|s| s.as_str()), Some("users"));
        assert_eq!(m2.page_to_tree_name.get(&5).map(|s| s.as_str()), Some("users"));
        assert_eq!(m2.page_to_tree_name.get(&10).map(|s| s.as_str()), Some("users"));
        assert_eq!(m2.page_to_tree_name.get(&15).map(|s| s.as_str()), Some("users"));
        // B-tree root=5 ("idx_users_name") owns group 1 with pages [1, 2, 3, 4]
        assert_eq!(m2.page_to_tree_name.get(&1).map(|s| s.as_str()), Some("idx_users_name"));
        assert_eq!(m2.page_to_tree_name.get(&4).map(|s| s.as_str()), Some("idx_users_name"));
        // Pages in group 2 (no btree entry) should NOT be in page_to_tree_name
        assert!(m2.page_to_tree_name.get(&6).is_none());
        assert!(m2.page_to_tree_name.get(&19).is_none());
        // page_to_tree_name is skip-serialized (rebuilt, not persisted)
        assert!(!m2.page_to_tree_name.is_empty()); // rebuilt by build_page_index
        // tree_name_to_groups also rebuilt
        assert_eq!(m2.tree_name_to_groups.get("users").unwrap(), &vec![0u64]);
        assert_eq!(m2.tree_name_to_groups.get("idx_users_name").unwrap(), &vec![1u64]);
    }

    // =========================================================================
    // B-tree manifest tests continued
    // =========================================================================

    #[test]
    fn test_manifest_btree_group_partial_last_group() {
        // Last B-tree group may have fewer pages than ppg.
        // total_groups, page_location, frame calculations all must respect actual size.
        let mut manifest = Manifest {
            page_count: 10,
            page_size: 4096,
            pages_per_group: 4,
            group_pages: vec![
                vec![0, 1, 2, 3],   // full group
                vec![4, 5, 6, 7],   // full group
                vec![8, 9],         // partial group (2 of 4)
            ],
            ..Manifest::empty()
        };
        manifest.build_page_index();

        assert_eq!(manifest.total_groups(), 3);

        // Page 9 is in the partial group at index 1
        let loc = manifest.page_location(9).unwrap();
        assert_eq!(loc.group_id, 2);
        assert_eq!(loc.index, 1);

        // Group 2 has 2 pages, not 4
        assert_eq!(manifest.group_pages[2].len(), 2);
    }

    #[test]
    fn test_manifest_deserialize_btree_fields_default_when_missing() {
        // Old manifests without group_pages/btrees should deserialize cleanly
        let json = r#"{"version":1,"page_count":100,"page_size":4096,"pages_per_group":32,"page_group_keys":["pg/0_v1","pg/1_v1","pg/2_v1","pg/3_v1"]}"#;
        let m: Manifest = serde_json::from_str(json).unwrap();

        // B-tree fields default to empty
        assert!(m.group_pages.is_empty());
        assert!(m.btrees.is_empty());
        assert!(m.page_index.is_empty());

        // Falls back to positional total_groups
        assert_eq!(m.total_groups(), 4); // ceil(100/32)
    }

    // =========================================================================
    // GroupingStrategy dispatch: page_location, group_page_nums, group_size,
    //                           prefetch_neighbors
    // =========================================================================

    #[test]
    fn test_positional_page_location() {
        let m = Manifest {
            page_count: 100,
            pages_per_group: 32,
            strategy: GroupingStrategy::Positional,
            ..Manifest::empty()
        };
        // page 0 -> gid=0, idx=0
        let loc = m.page_location(0).unwrap();
        assert_eq!(loc.group_id, 0);
        assert_eq!(loc.index, 0);
        // page 31 -> gid=0, idx=31
        let loc = m.page_location(31).unwrap();
        assert_eq!(loc.group_id, 0);
        assert_eq!(loc.index, 31);
        // page 32 -> gid=1, idx=0
        let loc = m.page_location(32).unwrap();
        assert_eq!(loc.group_id, 1);
        assert_eq!(loc.index, 0);
        // page 99 -> gid=3, idx=3
        let loc = m.page_location(99).unwrap();
        assert_eq!(loc.group_id, 3);
        assert_eq!(loc.index, 3);
        // page 100 -> None (out of bounds)
        assert!(m.page_location(100).is_none());
    }

    #[test]
    fn test_btreeaware_page_location() {
        let mut m = Manifest {
            page_count: 10,
            pages_per_group: 4,
            group_pages: vec![
                vec![0, 5, 10],   // gid=0: scattered
                vec![1, 2, 3, 4], // gid=1: sequential
            ],
            ..Manifest::empty()
        };
        m.build_page_index();
        assert_eq!(m.strategy, GroupingStrategy::BTreeAware);

        let loc = m.page_location(5).unwrap();
        assert_eq!(loc.group_id, 0);
        assert_eq!(loc.index, 1);

        let loc = m.page_location(3).unwrap();
        assert_eq!(loc.group_id, 1);
        assert_eq!(loc.index, 2);

        // Page 6 not assigned
        assert!(m.page_location(6).is_none());
    }

    #[test]
    fn test_positional_group_page_nums() {
        let m = Manifest {
            page_count: 10,
            pages_per_group: 4,
            strategy: GroupingStrategy::Positional,
            ..Manifest::empty()
        };
        // gid 0: [0,1,2,3]
        assert_eq!(m.group_page_nums(0).as_ref(), &[0, 1, 2, 3]);
        // gid 1: [4,5,6,7]
        assert_eq!(m.group_page_nums(1).as_ref(), &[4, 5, 6, 7]);
        // gid 2: [8,9] (partial last group)
        assert_eq!(m.group_page_nums(2).as_ref(), &[8, 9]);
    }

    #[test]
    fn test_btreeaware_group_page_nums_borrows() {
        let mut m = Manifest {
            page_count: 10,
            pages_per_group: 4,
            group_pages: vec![
                vec![0, 5, 10],
                vec![1, 2, 3, 4],
            ],
            ..Manifest::empty()
        };
        m.build_page_index();

        let gp = m.group_page_nums(0);
        // BTreeAware should return Cow::Borrowed (zero-allocation)
        assert!(matches!(gp, std::borrow::Cow::Borrowed(_)),
            "BTreeAware should borrow, not allocate");
        assert_eq!(gp.as_ref(), &[0, 5, 10]);
    }

    #[test]
    fn test_positional_group_page_nums_owned() {
        let m = Manifest {
            page_count: 8,
            pages_per_group: 4,
            strategy: GroupingStrategy::Positional,
            ..Manifest::empty()
        };

        let gp = m.group_page_nums(0);
        // Positional must allocate (Cow::Owned)
        assert!(matches!(gp, std::borrow::Cow::Owned(_)),
            "Positional should allocate Cow::Owned");
        assert_eq!(gp.as_ref(), &[0, 1, 2, 3]);
    }

    #[test]
    fn test_positional_group_size() {
        let m = Manifest {
            page_count: 10,
            pages_per_group: 4,
            strategy: GroupingStrategy::Positional,
            ..Manifest::empty()
        };
        assert_eq!(m.group_size(0), 4);
        assert_eq!(m.group_size(1), 4);
        assert_eq!(m.group_size(2), 2); // partial
    }

    #[test]
    fn test_btreeaware_group_size() {
        let mut m = Manifest {
            page_count: 10,
            pages_per_group: 4,
            group_pages: vec![
                vec![0, 5, 10],
                vec![1, 2, 3, 4],
            ],
            ..Manifest::empty()
        };
        m.build_page_index();
        assert_eq!(m.group_size(0), 3);
        assert_eq!(m.group_size(1), 4);
    }

    #[test]
    fn test_positional_prefetch_neighbors() {
        let m = Manifest {
            page_count: 100,
            pages_per_group: 32,
            strategy: GroupingStrategy::Positional,
            ..Manifest::empty()
        };
        match m.prefetch_neighbors(1) {
            PrefetchNeighbors::RadialFanout { total_groups } => {
                assert_eq!(total_groups, 4); // ceil(100/32)
            }
            _ => panic!("Positional should return RadialFanout"),
        }
    }

    #[test]
    fn test_btreeaware_prefetch_neighbors() {
        let mut m = Manifest {
            page_count: 20,
            pages_per_group: 8,
            group_pages: vec![
                vec![0, 1, 2, 3],
                vec![4, 5, 6, 7],
                vec![8, 9, 10, 11],
            ],
            btrees: HashMap::from([(
                0,
                BTreeManifestEntry {
                    name: "test_table".into(),
                    obj_type: "table".into(),
                    group_ids: vec![0, 1],
                },
            )]),
            ..Manifest::empty()
        };
        m.build_page_index();

        // gid=0 is in a btree with siblings [0, 1]
        match m.prefetch_neighbors(0) {
            PrefetchNeighbors::BTreeSiblings(siblings) => {
                assert_eq!(siblings, vec![0, 1]);
            }
            _ => panic!("BTreeAware should return BTreeSiblings"),
        }

        // gid=2 is NOT in any btree
        match m.prefetch_neighbors(2) {
            PrefetchNeighbors::BTreeSiblings(siblings) => {
                assert!(siblings.is_empty(), "non-btree group should have empty siblings");
            }
            _ => panic!("BTreeAware should return BTreeSiblings"),
        }
    }

    #[test]
    fn test_detect_and_normalize_strategy() {
        // Manifest with group_pages but strategy=Positional (simulates old manifest)
        let mut m = Manifest {
            page_count: 10,
            pages_per_group: 4,
            strategy: GroupingStrategy::Positional,
            group_pages: vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7]],
            ..Manifest::empty()
        };
        m.detect_and_normalize_strategy();
        assert_eq!(m.strategy, GroupingStrategy::BTreeAware);
        // page_index should be built
        assert!(m.page_location(3).is_some());
    }

    #[test]
    fn test_build_page_index_auto_detects_btreeaware() {
        // build_page_index should also auto-detect BTreeAware from group_pages
        let mut m = Manifest {
            page_count: 10,
            pages_per_group: 4,
            group_pages: vec![vec![0, 1], vec![2, 3]],
            ..Manifest::empty() // strategy defaults to Positional
        };
        assert_eq!(m.strategy, GroupingStrategy::Positional);
        m.build_page_index();
        assert_eq!(m.strategy, GroupingStrategy::BTreeAware,
            "build_page_index should auto-detect BTreeAware from non-empty group_pages");
        assert!(m.page_location(0).is_some());
    }

    #[test]
    fn test_positional_empty_manifest() {
        // Edge case: empty Positional manifest
        let m = Manifest::empty();
        assert_eq!(m.strategy, GroupingStrategy::Positional);
        assert_eq!(m.total_groups(), 0);
        assert!(m.page_location(0).is_none());
    }

    #[test]
    fn test_positional_page_location_ppg_zero() {
        // Edge case: ppg=0 should not panic
        let m = Manifest {
            page_count: 10,
            pages_per_group: 0,
            strategy: GroupingStrategy::Positional,
            ..Manifest::empty()
        };
        assert!(m.page_location(0).is_none());
    }

    #[test]
    fn test_shared_dirty_groups_arc_semantics() {
        // Verify that Arc<Mutex<HashSet<u64>>> drains correctly (simulates
        // handle writing + flush_to_s3 draining the same shared state).
        let shared = Arc::new(Mutex::new(HashSet::new()));

        // Simulate handle writing during local checkpoint
        {
            let mut pending = shared.lock().unwrap();
            pending.insert(0);
            pending.insert(3);
            pending.insert(7);
        }

        // Simulate flush_to_s3 draining
        let drained: HashSet<u64> = {
            let mut pending = shared.lock().unwrap();
            std::mem::take(&mut *pending)
        };

        assert_eq!(drained.len(), 3);
        assert!(drained.contains(&0));
        assert!(drained.contains(&3));
        assert!(drained.contains(&7));
        assert!(shared.lock().unwrap().is_empty());
    }

    #[test]
    fn test_shared_manifest_arc_write_visible_to_readers() {
        // Verify that manifest updates from the handle are visible to flush_to_s3
        let shared = Arc::new(RwLock::new(Manifest::empty()));
        let reader = Arc::clone(&shared);

        // Simulate handle updating manifest during write
        {
            let mut m = shared.write();
            m.page_count = 42;
            m.page_size = 65536;
            m.version = 5;
        }

        // Simulate flush reading the updated manifest
        {
            let m = reader.read();
            assert_eq!(m.page_count, 42);
            assert_eq!(m.page_size, 65536);
            assert_eq!(m.version, 5);
        }
    }

    // =========================================================================
    // Range-GET budget per tree (Phase Midway-i)
    // =========================================================================

    #[test]
    fn test_group_to_tree_name_built_on_index() {
        // group_to_tree_name reverse index should be populated by build_page_index
        let mut m = Manifest {
            page_count: 20,
            pages_per_group: 4,
            group_pages: vec![
                vec![0, 1, 2, 3],   // group 0: posts
                vec![4, 5, 6, 7],   // group 1: posts
                vec![8, 9, 10, 11], // group 2: idx_posts_user
            ],
            btrees: {
                let mut bt = HashMap::new();
                bt.insert(0, BTreeManifestEntry {
                    name: "posts".to_string(),
                    obj_type: "table".to_string(),
                    group_ids: vec![0, 1],
                });
                bt.insert(8, BTreeManifestEntry {
                    name: "idx_posts_user".to_string(),
                    obj_type: "index".to_string(),
                    group_ids: vec![2],
                });
                bt
            },
            ..Manifest::empty()
        };
        m.build_page_index();

        // group 0 and 1 belong to "posts"
        assert_eq!(m.group_to_tree_name.get(&0), Some(&"posts".to_string()));
        assert_eq!(m.group_to_tree_name.get(&1), Some(&"posts".to_string()));
        // group 2 belongs to "idx_posts_user"
        assert_eq!(m.group_to_tree_name.get(&2), Some(&"idx_posts_user".to_string()));
        // group 3 doesn't exist
        assert_eq!(m.group_to_tree_name.get(&3), None);
    }

    #[test]
    fn test_group_to_tree_name_empty_for_positional() {
        // Positional strategy has no btrees, so group_to_tree_name should be empty
        let mut m = Manifest {
            page_count: 10,
            pages_per_group: 4,
            strategy: GroupingStrategy::Positional,
            ..Manifest::empty()
        };
        m.build_page_index();
        assert!(m.group_to_tree_name.is_empty());
    }

    #[test]
    fn test_group_to_tree_name_cleared_on_rebuild() {
        // Rebuilding the index should clear stale entries
        let mut m = Manifest {
            page_count: 10,
            pages_per_group: 4,
            group_pages: vec![vec![0, 1, 2, 3]],
            btrees: {
                let mut bt = HashMap::new();
                bt.insert(0, BTreeManifestEntry {
                    name: "posts".to_string(),
                    obj_type: "table".to_string(),
                    group_ids: vec![0],
                });
                bt
            },
            ..Manifest::empty()
        };
        m.build_page_index();
        assert_eq!(m.group_to_tree_name.len(), 1);

        // Now rebuild with empty btrees
        m.btrees.clear();
        m.group_pages.clear();
        m.strategy = GroupingStrategy::Positional;
        m.build_page_index();
        assert!(m.group_to_tree_name.is_empty());
    }

    #[test]
    fn test_group_to_tree_name_multi_tree() {
        // Verify per-tree independence: multiple trees map correctly
        let mut m = Manifest {
            page_count: 30,
            pages_per_group: 4,
            group_pages: vec![
                vec![0, 1, 2, 3],     // group 0: posts
                vec![4, 5, 6, 7],     // group 1: posts
                vec![8, 9, 10, 11],   // group 2: idx_posts_user
                vec![12, 13, 14, 15], // group 3: users
                vec![16, 17, 18, 19], // group 4: users
            ],
            btrees: {
                let mut bt = HashMap::new();
                bt.insert(0, BTreeManifestEntry {
                    name: "posts".to_string(),
                    obj_type: "table".to_string(),
                    group_ids: vec![0, 1],
                });
                bt.insert(8, BTreeManifestEntry {
                    name: "idx_posts_user".to_string(),
                    obj_type: "index".to_string(),
                    group_ids: vec![2],
                });
                bt.insert(12, BTreeManifestEntry {
                    name: "users".to_string(),
                    obj_type: "table".to_string(),
                    group_ids: vec![3, 4],
                });
                bt
            },
            ..Manifest::empty()
        };
        m.build_page_index();

        // Each group maps to its tree
        assert_eq!(m.group_to_tree_name.get(&0).unwrap(), "posts");
        assert_eq!(m.group_to_tree_name.get(&1).unwrap(), "posts");
        assert_eq!(m.group_to_tree_name.get(&2).unwrap(), "idx_posts_user");
        assert_eq!(m.group_to_tree_name.get(&3).unwrap(), "users");
        assert_eq!(m.group_to_tree_name.get(&4).unwrap(), "users");
        // Total entries = number of groups with tree assignments
        assert_eq!(m.group_to_tree_name.len(), 5);
    }
}

