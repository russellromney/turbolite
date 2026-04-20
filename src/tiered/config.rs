use super::*;

// ===== Manifest source =====

/// Where to load the manifest on connection open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ManifestSource {
    /// Use local manifest if present, fall back to remote. Correct for single-writer.
    /// Checkpoints keep the local cache fresh; no remote fetch on warm reconnect.
    Auto,
    /// Always fetch from the remote backend on open. For HA followers and
    /// multi-reader setups where another process may have checkpointed.
    Remote,
}

impl Default for ManifestSource {
    fn default() -> Self {
        ManifestSource::Auto
    }
}

// ===== Sync mode =====

/// Controls whether checkpoint uploads synchronously (blocking) or defers to flush_to_storage().
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncMode {
    /// Default. `sync()` uploads dirty pages to the backend during checkpoint.
    /// The SQLite EXCLUSIVE lock is held for the entire upload duration.
    /// Full remote durability on every checkpoint.
    Durable,
    /// `sync()` writes to local disk cache only; dirty groups are recorded for
    /// later upload via `flush_to_storage()`. The EXCLUSIVE lock is held for ~1ms.
    ///
    /// Between checkpoint and flush, data exists only in local disk cache:
    /// - Process crash: data survives (on local disk)
    /// - Machine loss: data lost (not yet pushed to remote)
    ///
    /// Call `flush_to_storage()` (on TurboliteVfs or TurboliteSharedState) to upload.
    LocalThenFlush,
    /// Remote is the database. Local disk is disposable cache. Every `xSync()`
    /// uploads dirty frames as subframe overrides and publishes the manifest.
    /// The manifest publish is the atomic commit.
    ///
    /// Requires `journal_mode=OFF` or `MEMORY` (no WAL, no rollback journal).
    /// Per-commit cost: ~256KB per dirty frame + manifest publish (~2-50ms).
    /// Best for ephemeral compute (Lambda, scale-to-zero) where local disk is
    /// not durable but the backend is.
    RemotePrimary,
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

// ===== Nested configuration groups =====
//
// Phase Cirrus b1: these grouped structs are added alongside the flat
// `TurboliteConfig` fields. `Default::default()` populates both in lockstep
// so the nested view is always consistent with the flat view. b2 will
// remove the flat fields and switch internal reads to the nested form.

/// Cache and durability-eviction knobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CacheConfig {
    /// TTL for cached page groups in seconds (0 = no automatic eviction).
    pub ttl_secs: u64,
    /// Maximum cache size in bytes (sub-chunk granularity). None = unlimited.
    pub max_bytes: Option<u64>,
    /// In-memory page cache budget in bytes. 0 disables the mem cache.
    pub mem_budget: u64,
    /// Evict data tier after successful checkpoint upload.
    pub evict_on_checkpoint: bool,
    /// Compress pages in the local disk cache using zstd.
    pub compression: bool,
    /// Zstd compression level for local cache pages (1-22).
    pub compression_level: i32,
    /// Pages per page group. Each backend object contains this many compressed pages.
    pub pages_per_group: u32,
    /// Pages per sub-chunk frame for seekable page groups. 0 disables seekable encoding.
    pub sub_pages_per_frame: u32,
    /// Automatic garbage collection after each checkpoint.
    pub gc_enabled: bool,
    /// Override threshold. 0 = auto (frames_per_group / 4).
    pub override_threshold: u32,
    /// Compaction threshold.
    pub compaction_threshold: u32,
}

/// Compression settings for remote page groups (distinct from cache compression).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CompressionConfig {
    /// Zstd compression level (1-22).
    pub level: i32,
    /// Zstd compression dictionary (2-5x better on structured data).
    #[cfg(feature = "zstd")]
    pub dictionary: Option<Vec<u8>>,
}

/// At-rest encryption settings.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct EncryptionConfig {
    /// AES-256-GCM key. When set, all page data is encrypted at rest.
    /// Requires the `encryption` feature.
    pub key: Option<[u8; 32]>,
}

/// Prefetch and query-plan-driven warmup knobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PrefetchConfig {
    /// Prefetch schedule for SEARCH queries.
    pub search: Vec<f32>,
    /// Prefetch schedule for index lookups / point queries.
    pub lookup: Vec<f32>,
    /// Number of prefetch worker threads.
    pub threads: u32,
    /// Enable query-plan-aware prefetch.
    pub query_plan: bool,
    /// Enable predictive cross-tree prefetch.
    pub prediction: bool,
    /// Manifest source — Auto (local fallback) vs Remote (always backend).
    pub manifest_source: ManifestSource,
}

/// WAL replication settings (walrust-backed durability between checkpoints).
#[cfg(feature = "wal")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WalConfig {
    /// Ship WAL frames to the backend for transaction-level durability.
    pub replication: bool,
    /// How often walrust ships WAL frames (milliseconds).
    pub sync_interval_ms: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            ttl_secs: 0,
            max_bytes: None,
            mem_budget: 64 * 1024 * 1024,
            evict_on_checkpoint: false,
            compression: false,
            compression_level: 3,
            pages_per_group: DEFAULT_PAGES_PER_GROUP,
            sub_pages_per_frame: DEFAULT_SUB_PAGES_PER_FRAME,
            gc_enabled: true,
            override_threshold: 0,
            compaction_threshold: 8,
        }
    }
}

impl CacheConfig {
    /// Construct a CacheConfig from the `TURBOLITE_*` environment variables,
    /// falling back to `Default` for any value not overridden.
    pub fn from_env() -> Self {
        let d = Self::default();
        Self {
            max_bytes: std::env::var("TURBOLITE_CACHE_LIMIT")
                .ok()
                .and_then(|v| crate::tiered::settings::parse_byte_size(&v))
                .and_then(|n| if n == 0 { None } else { Some(n) })
                .or(d.max_bytes),
            mem_budget: std::env::var("TURBOLITE_MEM_CACHE_BUDGET")
                .ok()
                .and_then(|v| crate::tiered::settings::parse_byte_size(&v))
                .unwrap_or(d.mem_budget),
            evict_on_checkpoint: std::env::var("TURBOLITE_EVICT_ON_CHECKPOINT")
                .map(|v| matches!(v.as_str(), "true" | "1"))
                .unwrap_or(d.evict_on_checkpoint),
            compression: std::env::var("TURBOLITE_CACHE_COMPRESSION")
                .map(|v| matches!(v.as_str(), "true" | "1"))
                .unwrap_or(d.compression),
            compression_level: std::env::var("TURBOLITE_CACHE_COMPRESSION_LEVEL")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(d.compression_level),
            override_threshold: std::env::var("TURBOLITE_OVERRIDE_THRESHOLD")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(d.override_threshold),
            compaction_threshold: std::env::var("TURBOLITE_COMPACTION_THRESHOLD")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(d.compaction_threshold),
            ..d
        }
    }
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            level: 3,
            #[cfg(feature = "zstd")]
            dictionary: None,
        }
    }
}

impl CompressionConfig {
    /// Read `TURBOLITE_COMPRESSION_LEVEL` (the loadable-extension historically
    /// used this name). Falls back to `Default` otherwise.
    pub fn from_env() -> Self {
        let d = Self::default();
        Self {
            level: std::env::var("TURBOLITE_COMPRESSION_LEVEL")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(d.level),
            ..d
        }
    }
}

impl Default for PrefetchConfig {
    fn default() -> Self {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get() as u32)
            .unwrap_or(2);
        Self {
            search: vec![0.3, 0.3, 0.4],
            lookup: vec![0.0, 0.0, 0.0],
            threads: cpus + 1,
            query_plan: true,
            prediction: false,
            manifest_source: ManifestSource::Auto,
        }
    }
}

impl PrefetchConfig {
    pub fn from_env() -> Self {
        let d = Self::default();
        Self {
            threads: std::env::var("TURBOLITE_PREFETCH_THREADS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(d.threads),
            manifest_source: std::env::var("TURBOLITE_MANIFEST_SOURCE")
                .ok()
                .map(|v| match v.to_lowercase().as_str() {
                    "remote" | "s3" => ManifestSource::Remote,
                    _ => ManifestSource::Auto,
                })
                .unwrap_or(d.manifest_source),
            ..d
        }
    }
}

#[cfg(feature = "wal")]
impl Default for WalConfig {
    fn default() -> Self {
        Self {
            replication: false,
            sync_interval_ms: 1000,
        }
    }
}

#[cfg(feature = "wal")]
impl WalConfig {
    pub fn from_env() -> Self {
        let d = Self::default();
        Self {
            replication: std::env::var("TURBOLITE_WAL_REPLICATION")
                .map(|v| matches!(v.as_str(), "true" | "1"))
                .unwrap_or(d.replication),
            sync_interval_ms: std::env::var("TURBOLITE_WAL_SYNC_INTERVAL")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(d.sync_interval_ms),
        }
    }
}

// ===== Configuration =====

/// Configuration for turbolite storage.
///
/// turbolite is a byte-level storage consumer; the concrete backend (local
/// filesystem, S3, Cinch HTTP, etc.) is an embedder's choice. Use
/// [`TurboliteVfs::new`] for local mode (page groups on `cache_dir`) or
/// [`TurboliteVfs::with_backend`] to inject any
/// `Arc<dyn hadb_storage::StorageBackend>` plus a tokio runtime handle.
#[derive(Serialize, Deserialize)]
#[serde(default)]
pub struct TurboliteConfig {
    /// Local cache/data directory. Always populated; for non-local backends
    /// this is where the sub-chunk cache, staging logs, and dirty-group
    /// recovery state live.
    pub cache_dir: PathBuf,
    /// Open in read-only mode (no writes, no WAL)
    pub read_only: bool,
    /// Checkpoint sync mode. Default: Durable (upload on every checkpoint).
    /// Local mode is always LocalThenFlush (this field is overridden).
    pub sync_mode: SyncMode,

    /// Cache and durability-eviction knobs.
    pub cache: CacheConfig,
    /// Compression settings for remote page groups.
    pub compression: CompressionConfig,
    /// At-rest encryption settings.
    pub encryption: EncryptionConfig,
    /// Prefetch and query-plan-driven warmup knobs.
    pub prefetch: PrefetchConfig,
    /// WAL replication settings (walrust-backed durability between checkpoints).
    #[cfg(feature = "wal")]
    pub wal: WalConfig,
}

impl Default for TurboliteConfig {
    fn default() -> Self {
        Self {
            cache_dir: PathBuf::from("/tmp/turbolite-cache"),
            read_only: false,
            sync_mode: SyncMode::default(),
            cache: CacheConfig::default(),
            compression: CompressionConfig::default(),
            encryption: EncryptionConfig::default(),
            prefetch: PrefetchConfig::default(),
            #[cfg(feature = "wal")]
            wal: WalConfig::default(),
        }
    }
}

impl TurboliteConfig {
    /// Construct a TurboliteConfig by reading `TURBOLITE_*` environment variables.
    /// Fields not overridden by env vars come from `Default`. `cache_dir` reads
    /// `TURBOLITE_CACHE_DIR` (default `/tmp/turbolite-cache`).
    pub fn from_env() -> Self {
        let cache_dir = std::env::var("TURBOLITE_CACHE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/tmp/turbolite-cache"));
        Self {
            cache_dir,
            read_only: std::env::var("TURBOLITE_READ_ONLY")
                .map(|v| matches!(v.as_str(), "true" | "1"))
                .unwrap_or(false),
            sync_mode: SyncMode::default(),
            cache: CacheConfig::from_env(),
            compression: CompressionConfig::from_env(),
            encryption: EncryptionConfig::default(),
            prefetch: PrefetchConfig::from_env(),
            #[cfg(feature = "wal")]
            wal: WalConfig::from_env(),
        }
    }
}

#[cfg(test)]
#[path = "test_config.rs"]
mod tests;

// ===== Manifest =====

/// Location of a page within the explicit group mapping.
#[derive(Debug, Clone, Copy)]
pub struct PageLocation {
    pub group_id: u64,
    /// Position within the group's page list (index into group_pages[group_id])
    pub index: u32,
}

/// B-tree metadata stored in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BTreeManifestEntry {
    pub name: String,
    /// "table" or "index"
    pub obj_type: String,
    /// Group IDs containing this B-tree's pages
    pub group_ids: Vec<u64>,
}
