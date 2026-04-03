use super::*;

// ===== Storage backend =====

/// Where page groups and manifests are stored.
/// Local: everything on local disk. No S3, no tokio, no async deps.
/// S3: cloud-backed with local NVMe cache (requires `cloud` feature).
#[derive(Debug, Clone)]
pub enum StorageBackend {
    /// Local-only mode. Page groups stored at `{cache_dir}/pg/`, manifest at
    /// `{cache_dir}/manifest.msgpack`. No cloud dependencies.
    Local,
    /// S3-backed mode. Local disk is a cache; S3 is the source of truth.
    /// Requires the `cloud` feature for AWS SDK + tokio.
    #[cfg(feature = "cloud")]
    S3 {
        /// S3 bucket name
        bucket: String,
        /// S3 key prefix (e.g., "databases/tenant-123")
        prefix: String,
        /// Custom S3 endpoint URL (for MinIO/Tigris)
        endpoint_url: Option<String>,
        /// AWS region (default "us-east-1")
        region: Option<String>,
    },
}

impl Default for StorageBackend {
    fn default() -> Self {
        StorageBackend::Local
    }
}

// ===== Manifest source =====

/// Where to load the manifest on connection open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestSource {
    /// Use local manifest if present, fall back to S3. Correct for single-writer.
    /// Checkpoints keep the local manifest fresh. No S3 fetch on warm reconnect.
    Auto,
    /// Always fetch from S3 on open. For HA followers and multi-reader setups
    /// where another process may have checkpointed.
    S3,
}

impl Default for ManifestSource {
    fn default() -> Self {
        ManifestSource::Auto
    }
}

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
    /// Call `flush_to_s3()` (on TieredVfs or TieredSharedState) to upload.
    LocalThenFlush,
}

impl Default for SyncMode {
    fn default() -> Self {
        SyncMode::Durable
    }
}

// ===== Grouping strategy =====

/// Grouping strategy marker. Only BTreeAware is supported.
/// Kept as an enum for serde backward compatibility with existing manifests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupingStrategy {
    /// Legacy positional mapping. No longer used for new databases.
    /// Existing Positional manifests are read-only compatible.
    Positional,
    /// B-tree-aware: explicit page-to-group mapping from btree walking.
    BTreeAware,
}

impl Default for GroupingStrategy {
    fn default() -> Self {
        GroupingStrategy::BTreeAware
    }
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

/// Configuration for turbolite storage.
///
/// Use `storage_backend` to choose Local (default) or S3 mode:
/// - `StorageBackend::Local`: page groups on local disk, no cloud deps
/// - `StorageBackend::S3 { .. }`: S3-backed with local cache (requires `cloud` feature)
///
/// When `storage_backend` is `S3`, `bucket`/`prefix` are read from the variant.
/// The top-level `bucket`/`prefix` fields are kept for backward compatibility;
/// if `storage_backend` is Local and `bucket` is non-empty, the VFS will
/// auto-upgrade to S3 mode.
pub struct TieredConfig {
    /// Storage backend: Local (default) or S3.
    pub storage_backend: StorageBackend,
    /// S3 bucket name (legacy; prefer StorageBackend::S3)
    pub bucket: String,
    /// S3 key prefix (legacy; prefer StorageBackend::S3)
    pub prefix: String,
    /// Local cache/data directory
    pub cache_dir: PathBuf,
    /// Zstd compression level (1-22, default 3)
    pub compression_level: i32,
    /// **Cloud-only.** Custom S3 endpoint URL (for MinIO/Tigris). Ignored in local mode.
    pub endpoint_url: Option<String>,
    /// Open in read-only mode (no writes, no WAL)
    pub read_only: bool,
    /// Tokio runtime handle (pass in, or a new runtime is created)
    #[cfg(feature = "cloud")]
    pub runtime_handle: Option<TokioHandle>,
    /// Pages per page group (default 256 = 16MB uncompressed at 64KB page size, ~8MB compressed).
    /// Each S3 object contains this many contiguous compressed pages.
    /// At 4KB page size this is 1MB per group — increase to 4096 for small pages.
    pub pages_per_group: u32,
    /// **Cloud-only.** AWS region (default "us-east-1"). Ignored in local mode.
    pub region: Option<String>,
    /// TTL for cached page groups in seconds (default 3600 = 1 hour).
    /// Page groups not accessed within this window are evicted from local NVMe.
    /// Interior page groups (B-tree internal nodes) are pinned permanently.
    pub cache_ttl_secs: u64,
    /// **Cloud-only.** Prefetch schedule for SEARCH queries (BTreeAware strategy).
    /// Default [0.3, 0.3, 0.4]. Ignored in local mode.
    pub prefetch_search: Vec<f32>,
    /// **Cloud-only.** Prefetch schedule for index lookups / point queries.
    /// Default [0, 0, 0]. Ignored in local mode.
    pub prefetch_lookup: Vec<f32>,
    /// **Cloud-only.** Number of prefetch worker threads (default: num_cpus + 1).
    /// Ignored in local mode (no S3 I/O to parallelize).
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
    /// deleted from S3 asynchronously after the new manifest is uploaded.
    /// Default: true. Set to false only for debugging or if external tooling manages S3 lifecycle.
    pub gc_enabled: bool,
    /// **Cloud-only.** Load all index leaf bundles on VFS open (default true).
    /// Fetches index leaf pages in parallel from S3 during connection open.
    /// Ignored in local mode (index pages loaded on demand from local page groups).
    pub eager_index_load: bool,
    /// AES-256-GCM encryption key. When set, all data is encrypted:
    /// S3 page groups (per-frame), interior/index bundles, and local cache pages.
    /// The manifest is NOT encrypted (it contains only S3 keys and byte offsets, no user data).
    /// Requires the `encryption` feature for actual encryption; without it, the key is ignored.
    pub encryption_key: Option<[u8; 32]>,
    /// **Cloud-only.** Enable predictive cross-tree prefetch + access history.
    /// Default: false. Ignored in local mode.
    pub prediction_enabled: bool,
    /// Checkpoint sync mode. In cloud mode: Durable uploads to S3 during checkpoint,
    /// LocalThenFlush defers to flush_to_s3(). In local mode: always LocalThenFlush
    /// (this field is overridden). Default: Durable.
    pub sync_mode: SyncMode,
    /// **Cloud-only.** Enable query-plan-aware prefetch.
    /// When true, the VFS drains the global plan queue on first cache miss and
    /// submits all planned groups to the prefetch pool. The trace callback in
    /// the loadable extension populates the queue via EQP at start of step().
    /// Default: true (no-op if the extension trace callback is not installed).
    pub query_plan_prefetch: bool,
    /// Phase Stalingrad: maximum cache size in bytes (sub-chunk granularity).
    /// When set, the VFS evicts sub-chunks between queries to stay within budget.
    /// None = unlimited (default). 0 = unlimited.
    /// Active queries can temporarily exceed this limit; eviction only fires between queries.
    /// Also settable via TURBOLITE_CACHE_LIMIT env var or turbolite_config_set('cache_limit', '512MB').
    pub max_cache_bytes: Option<u64>,
    /// Phase Stalingrad-e: evict data tier after successful checkpoint S3 upload.
    /// Good for serverless/bursty workloads where you want a clean slate after committing.
    /// Only evicts Data tier (interior + index remain for next query's fast path).
    /// Default: false. Also settable via turbolite_config_set('evict_on_checkpoint', 'true').
    pub evict_on_checkpoint: bool,
    /// Phase Jena: enable interior map introspection + leaf chasing + overflow prefetch.
    /// Experimental. Parses B-tree interior pages for precise prefetch instead of
    /// hop-schedule guessing. Currently adds overhead without proven latency improvement.
    /// Default: false. Set `TURBOLITE_JENA=true` to enable.
    pub jena_enabled: bool,
    /// Compress pages in the local disk cache using zstd before writing.
    /// Saves disk space at the cost of CPU on cache hits (compress on write, decompress on read).
    /// When combined with encryption_key, order is: compress then encrypt on write,
    /// decrypt then decompress on read.
    /// Requires the `zstd` feature. Default: false (uncompressed, zero-CPU cache hits).
    pub cache_compression: bool,
    /// Zstd compression level for local cache pages (1-22, default 3).
    /// Only used when cache_compression is true. Lower = faster, higher = smaller.
    pub cache_compression_level: i32,
    /// **Cloud-only.** Where to load manifest on connection open.
    /// Auto (default): use local manifest if present, fall back to S3.
    /// S3: always fetch from S3 (for HA followers, multi-reader).
    /// In local mode, manifest is always loaded from local disk.
    pub manifest_source: ManifestSource,
    /// Phase Somme: enable WAL replication via walrust.
    /// Ships WAL frames to S3 for transaction-level durability between checkpoints.
    /// Requires the `wal` feature flag.
    /// Default: false. `TURBOLITE_WAL_REPLICATION=true` to enable.
    #[cfg(feature = "wal")]
    pub wal_replication: bool,
    /// Phase Somme: WAL sync interval (how often walrust ships WAL frames to S3).
    /// Default: 1 second.
    #[cfg(feature = "wal")]
    pub wal_sync_interval_ms: u64,
}

impl Default for TieredConfig {
    fn default() -> Self {
        Self {
            storage_backend: StorageBackend::default(),
            bucket: String::new(),
            prefix: String::new(),
            cache_dir: PathBuf::from("/tmp/sqlces-cache"),
            compression_level: 1,
            endpoint_url: None,
            read_only: false,
            #[cfg(feature = "cloud")]
            runtime_handle: None,
            pages_per_group: DEFAULT_PAGES_PER_GROUP,
            region: None,
            cache_ttl_secs: 3600,
            prefetch_search: vec![0.3, 0.3, 0.4],
            prefetch_lookup: vec![0.0, 0.0, 0.0],
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
            gc_enabled: true,
            eager_index_load: true,
            encryption_key: None,
            prediction_enabled: false,
            sync_mode: SyncMode::default(),
            query_plan_prefetch: true,
            max_cache_bytes: std::env::var("TURBOLITE_CACHE_LIMIT")
                .ok()
                .and_then(|v| crate::tiered::settings::parse_byte_size(&v))
                .and_then(|n| if n == 0 { None } else { Some(n) }),
            evict_on_checkpoint: std::env::var("TURBOLITE_EVICT_ON_CHECKPOINT")
                .map(|v| matches!(v.as_str(), "true" | "1"))
                .unwrap_or(false),
            jena_enabled: std::env::var("TURBOLITE_JENA")
                .map(|v| matches!(v.as_str(), "true" | "1"))
                .unwrap_or(false),
            cache_compression: std::env::var("TURBOLITE_CACHE_COMPRESSION")
                .map(|v| matches!(v.as_str(), "true" | "1"))
                .unwrap_or(false),
            cache_compression_level: std::env::var("TURBOLITE_CACHE_COMPRESSION_LEVEL")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(3),
            manifest_source: std::env::var("TURBOLITE_MANIFEST_SOURCE")
                .ok()
                .map(|v| match v.to_lowercase().as_str() {
                    "s3" => ManifestSource::S3,
                    _ => ManifestSource::Auto,
                })
                .unwrap_or_default(),
            #[cfg(feature = "wal")]
            wal_replication: std::env::var("TURBOLITE_WAL_REPLICATION")
                .map(|v| matches!(v.as_str(), "true" | "1"))
                .unwrap_or(false),
            #[cfg(feature = "wal")]
            wal_sync_interval_ms: std::env::var("TURBOLITE_WAL_SYNC_INTERVAL")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1000),
        }
    }
}

impl TieredConfig {
    /// Resolve the effective storage backend.
    /// If `storage_backend` is explicitly S3, use it.
    /// If Local but `bucket` is non-empty, auto-upgrade to S3 (backward compat).
    pub fn effective_backend(&self) -> StorageBackend {
        match &self.storage_backend {
            #[cfg(feature = "cloud")]
            StorageBackend::S3 { .. } => self.storage_backend.clone(),
            StorageBackend::Local => {
                #[cfg(feature = "cloud")]
                if !self.bucket.is_empty() {
                    return StorageBackend::S3 {
                        bucket: self.bucket.clone(),
                        prefix: self.prefix.clone(),
                        endpoint_url: self.endpoint_url.clone(),
                        region: self.region.clone(),
                    };
                }
                StorageBackend::Local
            }
        }
    }

    /// Whether the effective backend is local-only (no S3).
    pub fn is_local(&self) -> bool {
        matches!(self.effective_backend(), StorageBackend::Local)
    }
}

#[cfg(test)]
#[path = "test_config.rs"]
mod tests;

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

