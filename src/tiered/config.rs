use super::*;

// ===== Sync mode =====

/// Controls whether checkpoint uploads to S3 synchronously (blocking) or defers to flush_to_s3().
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// Default. `sync()` uploads dirty pages to S3 during checkpoint.
    /// The SQLite EXCLUSIVE lock is held for the entire S3 upload duration.
    /// Full S3 durability on every checkpoint.
    Durable,
    /// `sync()` writes to local disk cache only; dirty groups are recorded for
    /// later upload via `flush_to_s3()`. The EXCLUSIVE lock is held for ~1ms.
    ///
    /// Between checkpoint and flush, data exists only in local disk cache:
    /// - Process crash: data survives (on local disk)
    /// - Machine loss: data lost (not yet on S3)
    ///
    /// Call `flush_to_s3()` (on TieredVfs or TieredBenchHandle) to upload.
    LocalThenFlush,
}

impl Default for SyncMode {
    fn default() -> Self {
        SyncMode::Durable
    }
}

// ===== Grouping strategy =====

/// How pages are assigned to groups and how prefetch neighbors are selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupingStrategy {
    /// Legacy positional mapping: gid = page_num / ppg, idx = page_num % ppg.
    /// Group g contains pages [g*ppg .. min((g+1)*ppg, page_count)].
    /// Prefetch uses radial fan-out from current group.
    Positional,
    /// B-tree-aware: explicit page-to-group mapping from btree walking.
    /// group_pages[gid] = ordered list of page numbers.
    /// Prefetch uses sibling groups from the same B-tree.
    BTreeAware,
}

impl Default for GroupingStrategy {
    fn default() -> Self {
        GroupingStrategy::BTreeAware
    }
}

/// Prefetch neighbor selection returned by `Manifest::prefetch_neighbors()`.
pub enum PrefetchNeighbors {
    /// Radial fan-out from current gid (positional strategy).
    RadialFanout { total_groups: u64 },
    /// Sibling groups from the same B-tree (btree-aware strategy).
    BTreeSiblings(Vec<u64>),
}

// ===== Group state for prefetch coordination =====

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupState {
    None = 0,
    Fetching = 1,
    Present = 2,
}

// ===== Configuration =====

/// Configuration for tiered S3-backed storage.
pub struct TieredConfig {
    /// S3 bucket name
    pub bucket: String,
    /// S3 key prefix (e.g. "databases/tenant-123")
    pub prefix: String,
    /// Local cache directory
    pub cache_dir: PathBuf,
    /// Zstd compression level (1-22, default 3)
    pub compression_level: i32,
    /// Custom S3 endpoint URL (for MinIO/Tigris)
    pub endpoint_url: Option<String>,
    /// Open in read-only mode (no writes, no WAL)
    pub read_only: bool,
    /// Tokio runtime handle (pass in, or a new runtime is created)
    pub runtime_handle: Option<TokioHandle>,
    /// Pages per page group (default 256 = 16MB uncompressed at 64KB page size, ~8MB compressed).
    /// Each S3 object contains this many contiguous compressed pages.
    /// At 4KB page size this is 1MB per group — increase to 4096 for small pages.
    pub pages_per_group: u32,
    /// AWS region (default "us-east-1")
    pub region: Option<String>,
    /// TTL for cached page groups in seconds (default 3600 = 1 hour).
    /// Page groups not accessed within this window are evicted from local NVMe.
    /// Interior page groups (B-tree internal nodes) are pinned permanently.
    pub cache_ttl_secs: u64,
    /// Radial prefetch schedule (Positional strategy). Each element is the fraction of
    /// total page groups to prefetch on consecutive cache misses.
    /// Default [0.33, 0.33] = 3-hop: miss 1 fetches 33%, miss 2 fetches 33%, miss 3+ fetches all.
    pub prefetch_hops: Vec<f32>,
    /// B-tree sibling prefetch schedule (BTreeAware strategy). Each element is the fraction of
    /// eligible sibling groups to prefetch on consecutive cache misses.
    /// Default [0.0, 0.5, 0.5] = skip first miss (point queries get zero overhead),
    /// then 50% on miss 2, remaining 50% on miss 3, everything on miss 4+.
    /// Higher floor, lower volatility: scans pay one extra miss but point queries are clean.
    pub btree_prefetch_hops: Vec<f32>,
    /// Maximum inline range GETs per B-tree before waiting for full-group prefetch.
    /// Default 2: point queries (1 GET per tree in a join) stay fast; scans
    /// (2+ GETs to same tree) switch to bulk prefetch after 2 range GETs.
    /// Set to 0 for always-wait, u8::MAX to disable.
    pub max_range_gets_per_tree: u8,
    /// Number of prefetch worker threads (default: num_cpus + 1).
    /// N+1 keeps the pipeline full: when a thread blocks on S3 I/O,
    /// the extra thread uses that core for decompression/cache writes.
    pub prefetch_threads: u32,
    /// Zstd compression dictionary (for 2-5x better compression on structured data)
    #[cfg(feature = "zstd")]
    pub dictionary: Option<Vec<u8>>,
    /// Pages per sub-chunk frame for seekable page groups (default 4).
    /// Each page group is encoded as multiple independently-decompressible frames,
    /// enabling S3 byte-range GETs for point lookups (~128KB compressed per range GET at 64KB pages).
    /// Set to 0 to disable seekable encoding (legacy single-frame format).
    pub sub_pages_per_frame: u32,
    /// Enable automatic garbage collection after each checkpoint.
    /// When true, old page group versions replaced during checkpoint are
    /// deleted from S3 immediately after the new manifest is uploaded.
    /// Default: false (old versions accumulate, enabling point-in-time restore).
    pub gc_enabled: bool,
    /// Load all index leaf bundles on VFS open (default true).
    /// When true, index leaf pages are fetched in parallel during connection open,
    /// so the first indexed query pays zero index-fetch latency.
    /// Same pattern as interior bundle loading.
    pub eager_index_load: bool,
    /// AES-256-GCM encryption key. When set, all data is encrypted:
    /// S3 page groups (per-frame), interior/index bundles, and local cache pages.
    /// The manifest is NOT encrypted (it contains only S3 keys and byte offsets, no user data).
    /// Requires the `encryption` feature for actual encryption; without it, the key is ignored.
    pub encryption_key: Option<[u8; 32]>,
    /// Page grouping strategy for import. Default: BTreeAware.
    /// Positional: sequential chunking (page N -> group N/ppg).
    /// BTreeAware: B-tree walking + bin-packing by B-tree.
    pub grouping_strategy: GroupingStrategy,
    /// Phase Verdun: enable predictive cross-tree prefetch + access history.
    /// When true, the VFS learns which B-trees appear together in transactions
    /// and prefetches them in parallel on subsequent queries.
    /// Default: false (enable after testing).
    pub prediction_enabled: bool,
    /// Checkpoint sync mode. Controls whether S3 upload happens during checkpoint
    /// (Durable, blocking) or is deferred to flush_to_s3() (LocalThenFlush, non-blocking).
    /// Default: Durable.
    pub sync_mode: SyncMode,
}

impl Default for TieredConfig {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            prefix: String::new(),
            cache_dir: PathBuf::from("/tmp/sqlces-cache"),
            compression_level: 3,
            endpoint_url: None,
            read_only: false,
            runtime_handle: None,
            pages_per_group: DEFAULT_PAGES_PER_GROUP,
            region: None,
            cache_ttl_secs: 3600,
            prefetch_hops: vec![0.33, 0.33],
            btree_prefetch_hops: vec![0.0, 0.5, 0.5],
            max_range_gets_per_tree: 2,
            prefetch_threads: std::env::var("SQLCES_PREFETCH_THREADS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(|| {
                    let cpus = std::thread::available_parallelism()
                        .map(|n| n.get() as u32)
                        .unwrap_or(2);
                    cpus + 1
                }),
            #[cfg(feature = "zstd")]
            dictionary: None,
            sub_pages_per_frame: DEFAULT_SUB_PAGES_PER_FRAME,
            gc_enabled: false,
            eager_index_load: true,
            encryption_key: None,
            grouping_strategy: GroupingStrategy::default(),
            prediction_enabled: false,
            sync_mode: SyncMode::default(),
        }
    }
}

// ===== Manifest =====

/// Location of a page within the explicit group mapping (Phase Midway).
#[derive(Debug, Clone, Copy)]
pub struct PageLocation {
    pub group_id: u64,
    /// Position within the group's page list (index into group_pages[group_id])
    pub index: u32,
}

/// B-tree metadata stored in the manifest (Phase Midway).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BTreeManifestEntry {
    pub name: String,
    /// "table" or "index"
    pub obj_type: String,
    /// Group IDs containing this B-tree's pages
    pub group_ids: Vec<u64>,
}

