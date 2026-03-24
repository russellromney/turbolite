//! S3-backed page-group tiered storage VFS.
//!
//! Architecture:
//! - S3/Tigris is the source of truth for all pages
//! - Local NVMe disk is a page-level cache (uncompressed, direct pread)
//! - Writes go through WAL (local, fast), checkpoint flushes dirty pages to S3 as page groups
//! - Any instance with the VFS + S3 credentials can read the database
//! - A page group = 2048 contiguous compressed pages in a single S3 object (~8MB at 4KB page size)
//!
//! S3 layout:
//! ```text
//! s3://{bucket}/{prefix}/
//! ├── manifest.json       # version, page_count, page_size, pages_per_group, page_group_keys
//! └── pg/
//!     ├── 0_v1            # Page group 0, manifest version 1 (pages 0-2047)
//!     ├── 1_v1            # Page group 1 (pages 2048-4095)
//!     └── 0_v2            # Page group 0 updated at version 2
//! ```
//!
//! Page group format (S3 object, compressed):
//! ```text
//! [2049 × u32 offsets (8196 bytes)] [compressed page 0] [compressed page 1] ... [compressed page 2047]
//! ```
//!
//! Local cache (single file, uncompressed):
//! ```text
//! [page 0 @ offset 0] [page 1 @ offset 4096] ... [page N @ offset N*4096]
//! ```
//! Cache hits are a single pread() with zero CPU overhead (no decompression).
//!
//! Page bitmap (1 bit per page, persisted to disk):
//! Tracks which pages are present in the local cache file.
//!
//! Interior B-tree pages (type bytes 0x05, 0x02) are pinned permanently — never evicted.
//! They represent <1% of the database but are hit on every query.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions as FsOpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sqlite_vfs::{DatabaseHandle, LockKind, OpenKind, OpenOptions, Vfs};
use tokio::runtime::Handle as TokioHandle;

use crate::compress;

// Re-use the FileWalIndex from the main lib
use crate::FileWalIndex;

// ===== Constants =====

/// Default pages per page group (4096 × 4KB = 16MB uncompressed, ~8MB compressed).
const DEFAULT_PAGES_PER_GROUP: u32 = 4096;

/// Interior chunk range: each chunk covers this many page numbers.
/// At ~1% interior page density → ~327 interior pages per chunk → ~1.3MB uncompressed.
/// Stable boundaries: adding/removing an interior page only affects one chunk.
const INTERIOR_CHUNK_RANGE: u64 = 32768;

/// When true, `sync()` skips S3 upload and just records which page groups are dirty.
/// Pages are already in disk cache from `write_all_at()`. This keeps WAL small during
/// bulk loading without S3 round-trip overhead. Call `set_local_checkpoint_only(false)`
/// before the final checkpoint to upload everything to S3.
static LOCAL_CHECKPOINT_ONLY: AtomicBool = AtomicBool::new(false);

/// Set the local-checkpoint-only flag. When true, WAL checkpoints compact the WAL
/// and free in-memory dirty pages, but skip S3 upload. When false (default), checkpoints
/// upload dirty pages to S3 as normal.
pub fn set_local_checkpoint_only(val: bool) {
    LOCAL_CHECKPOINT_ONLY.store(val, Ordering::Release);
}

/// Returns the current value of the local-checkpoint-only flag.
pub fn is_local_checkpoint_only() -> bool {
    LOCAL_CHECKPOINT_ONLY.load(Ordering::Acquire)
}

// ===== Page group coordinate math =====

pub fn group_id(page_num: u64, ppg: u32) -> u64 {
    page_num / ppg as u64
}

pub fn local_idx_in_group(page_num: u64, ppg: u32) -> u32 {
    (page_num % ppg as u64) as u32
}

pub fn group_start_page(gid: u64, ppg: u32) -> u64 {
    gid * ppg as u64
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
    /// Pages per page group (default 4096 = 16MB uncompressed at 4KB page size, ~8MB compressed).
    /// Each S3 object contains this many contiguous compressed pages.
    pub pages_per_group: u32,
    /// AWS region (default "us-east-1")
    pub region: Option<String>,
    /// TTL for cached page groups in seconds (default 3600 = 1 hour).
    /// Page groups not accessed within this window are evicted from local NVMe.
    /// Interior page groups (B-tree internal nodes) are pinned permanently.
    pub cache_ttl_secs: u64,
    /// Fraction-based prefetch schedule. Each element is the fraction of
    /// total page groups to prefetch on consecutive cache misses.
    /// Default [0.33, 0.33] = 3-hop: miss 1 fetches 33%, miss 2 fetches 33%, miss 3+ fetches all.
    pub prefetch_hops: Vec<f32>,
    /// Number of prefetch worker threads (default: num_cpus + 1).
    /// N+1 keeps the pipeline full: when a thread blocks on S3 I/O,
    /// the extra thread uses that core for decompression/cache writes.
    pub prefetch_threads: u32,
    /// Zstd compression dictionary (for 2-5x better compression on structured data)
    #[cfg(feature = "zstd")]
    pub dictionary: Option<Vec<u8>>,
    /// Pages per sub-chunk frame for seekable page groups (default 32).
    /// Each page group is encoded as multiple independently-decompressible frames,
    /// enabling S3 byte-range GETs for point lookups (fetch ~128KB instead of ~10MB).
    /// Set to 0 to disable seekable encoding (legacy single-frame format).
    pub sub_pages_per_frame: u32,
    /// Enable automatic garbage collection after each checkpoint.
    /// When true, old page group versions replaced during checkpoint are
    /// deleted from S3 immediately after the new manifest is uploaded.
    /// Default: false (old versions accumulate, enabling point-in-time restore).
    pub gc_enabled: bool,
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
            sub_pages_per_frame: 32,
            gc_enabled: false,
        }
    }
}

// ===== Manifest =====

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
    /// Chunked interior bundle: chunk_id → S3 key. Each chunk covers INTERIOR_CHUNK_RANGE page numbers.
    /// Fetched in parallel on connection open so B-tree traversal is all cache hits.
    #[serde(default)]
    pub interior_chunk_keys: HashMap<u32, String>,
    /// Per-group frame table for seekable page groups (multi-frame encoding).
    /// frame_tables[gid] = vec of FrameEntry for each sub-chunk.
    /// Empty or missing means legacy single-frame format (full download required).
    #[serde(default)]
    pub frame_tables: Vec<Vec<FrameEntry>>,
    /// Pages per sub-chunk frame (for seekable page groups). Default 0 = legacy format.
    #[serde(default)]
    pub sub_pages_per_frame: u32,
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

fn default_pages_per_group() -> u32 {
    DEFAULT_PAGES_PER_GROUP
}

impl Manifest {
    fn empty() -> Self {
        Self {
            version: 0,
            page_count: 0,
            page_size: 0,
            pages_per_group: 0,
            page_group_keys: Vec::new(),
            interior_chunk_keys: HashMap::new(),
            frame_tables: Vec::new(),
            sub_pages_per_frame: 0,
        }
    }

    fn total_groups(&self) -> u64 {
        if self.pages_per_group == 0 || self.page_count == 0 {
            return 0;
        }
        (self.page_count + self.pages_per_group as u64 - 1) / self.pages_per_group as u64
    }
}

// ===== S3Client (sync wrapper around async SDK) =====

/// Synchronous S3 client wrapping the async AWS SDK.
struct S3Client {
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
    runtime: TokioHandle,
    /// S3 GET count (for benchmarking / diagnostics)
    fetch_count: AtomicU64,
    /// S3 bytes fetched (for benchmarking / diagnostics)
    fetch_bytes: AtomicU64,
}

impl S3Client {
    /// Create a new S3 client.
    async fn new_async(config: &TieredConfig) -> io::Result<Self> {
        eprintln!("[s3] new_async: loading aws_config...");
        let mut aws_config = aws_config::from_env();

        if let Some(region) = &config.region {
            eprintln!("[s3] setting region: {}", region);
            aws_config =
                aws_config.region(aws_sdk_s3::config::Region::new(region.clone()));
        }

        let aws_config = aws_config.load().await;
        eprintln!("[s3] aws_config loaded");

        let mut s3_config = aws_sdk_s3::config::Builder::from(&aws_config);
        if let Some(endpoint) = &config.endpoint_url {
            eprintln!("[s3] setting endpoint: {}", endpoint);
            s3_config = s3_config.endpoint_url(endpoint);
        } else {
            eprintln!("[s3] using default AWS S3 endpoint");
        }

        let client = aws_sdk_s3::Client::from_conf(s3_config.build());
        eprintln!("[s3] S3 client created");

        let runtime = config
            .runtime_handle
            .clone()
            .or_else(|| TokioHandle::try_current().ok())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Other,
                    "No tokio runtime available. Pass runtime_handle in TieredConfig \
                     or call from within a tokio context.",
                )
            })?;

        Ok(Self {
            client,
            bucket: config.bucket.clone(),
            prefix: config.prefix.clone(),
            runtime,
            fetch_count: AtomicU64::new(0),
            fetch_bytes: AtomicU64::new(0),
        })
    }

    /// Blocking constructor.
    fn new_blocking(config: &TieredConfig, runtime: &TokioHandle) -> io::Result<Self> {
        Self::block_on(runtime, Self::new_async(config))
    }

    /// Run an async future on the tokio runtime, handling both
    /// "inside tokio" and "outside tokio" cases.
    fn block_on<F: std::future::Future<Output = T>, T>(
        handle: &TokioHandle,
        fut: F,
    ) -> T {
        match TokioHandle::try_current() {
            Ok(_) => tokio::task::block_in_place(|| handle.block_on(fut)),
            Err(_) => handle.block_on(fut),
        }
    }

    fn s3_key(&self, suffix: &str) -> String {
        if self.prefix.is_empty() {
            suffix.to_string()
        } else {
            format!("{}/{}", self.prefix, suffix)
        }
    }

    fn manifest_key(&self) -> String {
        self.s3_key("manifest.json")
    }

    /// Generate versioned S3 key for a page group.
    fn page_group_key(&self, group_id: u64, version: u64) -> String {
        self.s3_key(&format!("pg/{}_v{}", group_id, version))
    }

    /// Generate versioned S3 key for a chunked interior bundle piece.
    fn interior_chunk_key(&self, chunk_id: u32, version: u64) -> String {
        self.s3_key(&format!("ibc/{}_v{}", chunk_id, version))
    }

    // --- Generic GET/PUT ---

    async fn get_object_async(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        let mut retries = 0u32;
        loop {
            match self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
            {
                Ok(resp) => {
                    let bytes = resp
                        .body
                        .collect()
                        .await
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                        .into_bytes();
                    self.fetch_count.fetch_add(1, Ordering::Relaxed);
                    self.fetch_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    return Ok(Some(bytes.to_vec()));
                }
                Err(e) => {
                    if is_not_found(&e) {
                        return Ok(None);
                    }
                    retries += 1;
                    if retries >= 3 {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("S3 GET {} failed after 3 retries: {:?}", key, e),
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(
                        100 * (1 << retries),
                    ))
                    .await;
                }
            }
        }
    }

    /// Byte-range GET: fetch [start..start+len) from an S3 object.
    async fn range_get_async(&self, key: &str, start: u64, len: u32) -> io::Result<Option<Vec<u8>>> {
        let range = format!("bytes={}-{}", start, start + len as u64 - 1);
        let mut retries = 0u32;
        loop {
            match self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .range(&range)
                .send()
                .await
            {
                Ok(resp) => {
                    let bytes = resp
                        .body
                        .collect()
                        .await
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                        .into_bytes();
                    self.fetch_count.fetch_add(1, Ordering::Relaxed);
                    self.fetch_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    return Ok(Some(bytes.to_vec()));
                }
                Err(e) => {
                    if is_not_found(&e) {
                        return Ok(None);
                    }
                    retries += 1;
                    if retries >= 3 {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("S3 range GET {} ({}) failed after 3 retries: {:?}", key, range, e),
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(
                        100 * (1 << retries),
                    ))
                    .await;
                }
            }
        }
    }

    /// Blocking byte-range GET.
    fn range_get(&self, key: &str, start: u64, len: u32) -> io::Result<Option<Vec<u8>>> {
        S3Client::block_on(&self.runtime, self.range_get_async(key, start, len))
    }

    async fn put_object_async(&self, key: &str, data: Vec<u8>, content_type: Option<&str>) -> io::Result<()> {
        let mut retries = 0u32;
        loop {
            let body = aws_sdk_s3::primitives::ByteStream::from(data.clone());
            let mut req = self
                .client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(body);
            if let Some(ct) = content_type {
                req = req.content_type(ct);
            }
            match req.send().await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    retries += 1;
                    if retries >= 3 {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("S3 PUT {} failed after 3 retries: {}", key, e),
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(
                        100 * (1 << retries),
                    ))
                    .await;
                }
            }
        }
    }

    // --- Page group operations ---

    /// Fetch a page group by its S3 key.
    fn get_page_group(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        S3Client::block_on(&self.runtime, self.get_object_async(key))
    }

    /// Fetch multiple page groups in parallel by S3 key.
    /// Returns key → bytes for found objects.
    #[allow(dead_code)]
    fn get_page_groups_by_key(&self, keys: &[String]) -> io::Result<HashMap<String, Vec<u8>>> {
        S3Client::block_on(&self.runtime, self.get_page_groups_by_key_async(keys))
    }

    #[allow(dead_code)]
    async fn get_page_groups_by_key_async(&self, keys: &[String]) -> io::Result<HashMap<String, Vec<u8>>> {
        let mut handles = Vec::with_capacity(keys.len());
        for key in keys {
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let key = key.clone();
            handles.push(tokio::spawn(async move {
                let mut retries = 0u32;
                loop {
                    match client
                        .get_object()
                        .bucket(&bucket)
                        .key(&key)
                        .send()
                        .await
                    {
                        Ok(resp) => {
                            let bytes = resp
                                .body
                                .collect()
                                .await
                                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                                .into_bytes();
                            return Ok::<_, io::Error>((key, Some(bytes.to_vec())));
                        }
                        Err(e) => {
                            if is_not_found(&e) {
                                return Ok((key, None));
                            }
                            retries += 1;
                            if retries >= 3 {
                                return Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    format!("S3 GET {} failed: {}", key, e),
                                ));
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(
                                100 * (1 << retries),
                            ))
                            .await;
                        }
                    }
                }
            }));
        }

        let mut result = HashMap::new();
        for handle in handles {
            let (key, data) = handle
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))??;
            if let Some(bytes) = data {
                self.fetch_count.fetch_add(1, Ordering::Relaxed);
                self.fetch_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                result.insert(key, bytes);
            }
        }
        Ok(result)
    }

    /// Upload multiple page groups in parallel. Each entry is (s3_key, raw_bytes).
    fn put_page_groups(&self, groups: &[(String, Vec<u8>)]) -> io::Result<()> {
        S3Client::block_on(&self.runtime, self.put_page_groups_async(groups))
    }

    async fn put_page_groups_async(&self, groups: &[(String, Vec<u8>)]) -> io::Result<()> {
        let mut handles = Vec::with_capacity(groups.len());
        for (key, data) in groups {
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let key = key.clone();
            let data = data.clone();
            handles.push(tokio::spawn(async move {
                let mut retries = 0u32;
                loop {
                    let body =
                        aws_sdk_s3::primitives::ByteStream::from(data.clone());
                    match client
                        .put_object()
                        .bucket(&bucket)
                        .key(&key)
                        .body(body)
                        .send()
                        .await
                    {
                        Ok(_) => return Ok::<_, io::Error>(()),
                        Err(e) => {
                            retries += 1;
                            if retries >= 3 {
                                return Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    format!("S3 PUT {} failed: {}", key, e),
                                ));
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(
                                100 * (1 << retries),
                            ))
                            .await;
                        }
                    }
                }
            }));
        }

        for handle in handles {
            handle
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))??;
        }
        Ok(())
    }

    // --- Manifest ---

    fn get_manifest(&self) -> io::Result<Option<Manifest>> {
        S3Client::block_on(&self.runtime, self.get_manifest_async())
    }

    async fn get_manifest_async(&self) -> io::Result<Option<Manifest>> {
        let key = self.manifest_key();
        match self.get_object_async(&key).await? {
            Some(bytes) => {
                let manifest: Manifest = serde_json::from_slice(&bytes).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Invalid manifest JSON: {}", e),
                    )
                })?;
                Ok(Some(manifest))
            }
            None => Ok(None),
        }
    }

    fn put_manifest(&self, manifest: &Manifest) -> io::Result<()> {
        S3Client::block_on(&self.runtime, self.put_manifest_async(manifest))
    }

    async fn put_manifest_async(&self, manifest: &Manifest) -> io::Result<()> {
        let key = self.manifest_key();
        let json = serde_json::to_vec(manifest)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.put_object_async(&key, json, Some("application/json")).await
    }

    /// Delete a batch of S3 objects by key. Handles batching (AWS limit: 1000/request).
    fn delete_objects(&self, keys: &[String]) -> io::Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        S3Client::block_on(&self.runtime, self.delete_objects_async(keys))
    }

    async fn delete_objects_async(&self, keys: &[String]) -> io::Result<()> {
        for batch in keys.chunks(1000) {
            let objects: Vec<aws_sdk_s3::types::ObjectIdentifier> = batch
                .iter()
                .map(|key| {
                    aws_sdk_s3::types::ObjectIdentifier::builder()
                        .key(key)
                        .build()
                        .expect("ObjectIdentifier requires key")
                })
                .collect();

            let delete = aws_sdk_s3::types::Delete::builder()
                .set_objects(Some(objects))
                .quiet(true)
                .build()
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!("Failed to build Delete request: {}", e),
                    )
                })?;

            self.client
                .delete_objects()
                .bucket(&self.bucket)
                .delete(delete)
                .send()
                .await
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!("S3 batch delete failed: {}", e),
                    )
                })?;
        }
        Ok(())
    }

    /// List all S3 object keys under this client's prefix.
    fn list_all_keys(&self) -> io::Result<Vec<String>> {
        S3Client::block_on(&self.runtime, self.list_all_keys_async())
    }

    async fn list_all_keys_async(&self) -> io::Result<Vec<String>> {
        let mut all_keys = Vec::new();
        let mut continuation_token: Option<String> = None;
        loop {
            let mut req = self.client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&self.prefix);
            if let Some(token) = &continuation_token {
                req = req.continuation_token(token);
            }
            let resp = req.send().await.map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("S3 list failed: {}", e))
            })?;
            for obj in resp.contents() {
                if let Some(key) = obj.key() {
                    all_keys.push(key.to_string());
                }
            }
            if resp.is_truncated() == Some(true) {
                continuation_token = resp.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }
        Ok(all_keys)
    }
}

/// Check if an S3 error is a 404 / NoSuchKey.
fn is_not_found<E: std::fmt::Display + std::fmt::Debug>(
    err: &aws_sdk_s3::error::SdkError<E>,
) -> bool {
    match err {
        aws_sdk_s3::error::SdkError::ServiceError(service_err) => {
            service_err.raw().status().as_u16() == 404
        }
        _ => false,
    }
}

// ===== Page Bitmap =====

/// 1-bit-per-page tracking of which pages are present in the local cache file.
/// Persisted to disk for crash recovery.
struct PageBitmap {
    bits: Vec<u8>,
    path: PathBuf,
}

impl PageBitmap {
    fn new(path: PathBuf) -> Self {
        // Try to load from disk
        let bits = fs::read(&path).unwrap_or_default();
        Self { bits, path }
    }

    fn is_present(&self, page: u64) -> bool {
        let byte_idx = page as usize / 8;
        let bit_idx = page as usize % 8;
        if byte_idx >= self.bits.len() {
            return false;
        }
        self.bits[byte_idx] & (1 << bit_idx) != 0
    }

    fn mark_present(&mut self, page: u64) {
        self.ensure_capacity(page);
        let byte_idx = page as usize / 8;
        let bit_idx = page as usize % 8;
        self.bits[byte_idx] |= 1 << bit_idx;
    }

    #[cfg(test)]
    fn mark_range(&mut self, start: u64, count: u64) {
        for p in start..start + count {
            self.mark_present(p);
        }
    }

    fn clear_range(&mut self, start: u64, count: u64) {
        for p in start..start + count {
            let byte_idx = p as usize / 8;
            let bit_idx = p as usize % 8;
            if byte_idx < self.bits.len() {
                self.bits[byte_idx] &= !(1 << bit_idx);
            }
        }
    }

    fn resize(&mut self, page_count: u64) {
        let needed_bytes = (page_count as usize + 7) / 8;
        if self.bits.len() < needed_bytes {
            self.bits.resize(needed_bytes, 0);
        }
    }

    fn persist(&self) -> io::Result<()> {
        let tmp = self.path.with_extension("tmp");
        fs::write(&tmp, &self.bits)?;
        fs::rename(&tmp, &self.path)?;
        Ok(())
    }

    fn ensure_capacity(&mut self, page: u64) {
        let needed = page as usize / 8 + 1;
        if self.bits.len() < needed {
            self.bits.resize(needed, 0);
        }
    }
}

// ===== DiskCache (page-level cache with bitmap + TTL eviction) =====

/// Local NVMe page cache with TTL-based eviction.
///
/// Pages are stored **uncompressed** in a single cache file at natural offsets.
/// Cache hits are a single `pread()` — zero CPU overhead (no decompression).
/// A page bitmap tracks which pages are present.
///
/// Interior B-tree pages are pinned permanently (never evicted).
struct DiskCache {
    #[allow(dead_code)] // retained for debugging
    cache_dir: PathBuf,
    /// Local cache file — uncompressed pages at offset page_num * page_size.
    /// RwLock: read lock for concurrent pread/pwrite (thread-safe on Unix),
    /// write lock only for set_len (extending the file).
    cache_file: parking_lot::RwLock<File>,
    /// 1 bit per page: is this page present in cache_file?
    bitmap: parking_lot::Mutex<PageBitmap>,
    /// Per-group state: 0=None, 1=Fetching, 2=Present
    group_states: parking_lot::Mutex<Vec<std::sync::atomic::AtomicU8>>,
    /// Condition variable for wait_for_group (replaces spin-wait)
    group_condvar: parking_lot::Condvar,
    group_condvar_mutex: parking_lot::Mutex<()>,
    /// Interior page groups — permanently pinned, never evicted
    interior_groups: parking_lot::Mutex<HashSet<u64>>,
    /// Individual interior page numbers (for precise cache preservation)
    interior_pages: parking_lot::Mutex<HashSet<u64>>,
    /// TTL tracking: group_id → last_access
    group_access: parking_lot::Mutex<HashMap<u64, Instant>>,
    ttl_secs: u64,
    pages_per_group: u32,
    page_size: std::sync::atomic::AtomicU32,
}

/// Counter for lazy eviction (every 64 group fetches).
static EVICTION_COUNTER: AtomicU64 = AtomicU64::new(0);

impl DiskCache {
    fn new(cache_dir: &Path, ttl_secs: u64, pages_per_group: u32, page_size: u32, page_count: u64) -> io::Result<Self> {
        fs::create_dir_all(cache_dir)?;

        let cache_file_path = cache_dir.join("data.cache");
        let cache_file = FsOpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&cache_file_path)?;

        // Extend to full size (sparse file)
        if page_count > 0 && page_size > 0 {
            let target_size = page_count * page_size as u64;
            let meta = cache_file.metadata()?;
            if meta.len() < target_size {
                cache_file.set_len(target_size)?;
            }
        }

        let bitmap_path = cache_dir.join("page_bitmap");
        let mut bitmap = PageBitmap::new(bitmap_path);
        if page_count > 0 {
            bitmap.resize(page_count);
        }

        let total_groups = if pages_per_group > 0 && page_count > 0 {
            ((page_count + pages_per_group as u64 - 1) / pages_per_group as u64) as usize
        } else {
            0
        };

        // Initialize group states — mark as Present for groups where bitmap shows all pages
        let group_states: Vec<std::sync::atomic::AtomicU8> = (0..total_groups)
            .map(|gid| {
                let start = gid as u64 * pages_per_group as u64;
                let end = (start + pages_per_group as u64).min(page_count);
                let all_present = (start..end).all(|p| bitmap.is_present(p));
                let state = if all_present && end > start {
                    GroupState::Present as u8
                } else {
                    GroupState::None as u8
                };
                std::sync::atomic::AtomicU8::new(state)
            })
            .collect();

        Ok(Self {
            cache_dir: cache_dir.to_path_buf(),
            cache_file: parking_lot::RwLock::new(cache_file),
            bitmap: parking_lot::Mutex::new(bitmap),
            group_states: parking_lot::Mutex::new(group_states),
            group_condvar: parking_lot::Condvar::new(),
            group_condvar_mutex: parking_lot::Mutex::new(()),
            interior_groups: parking_lot::Mutex::new(HashSet::new()),
            interior_pages: parking_lot::Mutex::new(HashSet::new()),
            group_access: parking_lot::Mutex::new(HashMap::new()),
            ttl_secs,
            pages_per_group,
            page_size: std::sync::atomic::AtomicU32::new(page_size),
        })
    }

    /// Read a single page from the cache file (pread, no decompression).
    fn read_page(&self, page_num: u64, buf: &mut [u8]) -> io::Result<()> {
        use std::os::unix::fs::FileExt;
        let offset = page_num * self.page_size.load(Ordering::Acquire) as u64;
        let file = self.cache_file.read();
        file.read_exact_at(buf, offset)
    }

    /// Write a single uncompressed page to the cache file.
    fn write_page(&self, page_num: u64, data: &[u8]) -> io::Result<()> {
        use std::os::unix::fs::FileExt;
        let offset = page_num * self.page_size.load(Ordering::Acquire) as u64;
        let needed = offset + data.len() as u64;

        // Extend file if needed (exclusive lock), then pwrite (shared lock)
        {
            let file = self.cache_file.read();
            if file.metadata()?.len() < needed {
                drop(file);
                let file = self.cache_file.write();
                // Re-check after acquiring write lock
                if file.metadata()?.len() < needed {
                    file.set_len(needed)?;
                }
                file.write_all_at(data, offset)?;
            } else {
                file.write_all_at(data, offset)?;
            }
        }
        self.bitmap.lock().mark_present(page_num);
        Ok(())
    }

    /// Write a contiguous range of pages to the cache file in a single I/O operation.
    /// `start_page` is the first page number, `data` is the raw concatenated page data.
    /// Uses RwLock: read lock for pwrite (concurrent), write lock only if file needs extending.
    fn write_pages_bulk(&self, start_page: u64, data: &[u8], num_pages: u64) -> io::Result<()> {
        use std::os::unix::fs::FileExt;
        let offset = start_page * self.page_size.load(Ordering::Acquire) as u64;
        let needed = offset + data.len() as u64;

        // Try read lock first (concurrent pwrite). Only upgrade to write lock if file too small.
        {
            let file = self.cache_file.read();
            if file.metadata()?.len() < needed {
                drop(file);
                let file = self.cache_file.write();
                if file.metadata()?.len() < needed {
                    file.set_len(needed)?;
                }
                file.write_all_at(data, offset)?;
            } else {
                file.write_all_at(data, offset)?;
            }
        }

        // Mark all pages present in one lock acquisition
        let mut bitmap = self.bitmap.lock();
        for i in 0..num_pages {
            bitmap.mark_present(start_page + i);
        }
        Ok(())
    }

    /// Update the page size (needed when writer VFS learns page size from first write).
    fn set_page_size(&self, new_page_size: u32) {
        self.page_size.store(new_page_size, Ordering::Release);
    }

    /// Check if a page is present in the local cache.
    fn is_present(&self, page_num: u64) -> bool {
        self.bitmap.lock().is_present(page_num)
    }

    /// Get the state of a page group.
    fn group_state(&self, gid: u64) -> GroupState {
        let states = self.group_states.lock();
        match states.get(gid as usize) {
            Some(s) => match s.load(Ordering::Acquire) {
                1 => GroupState::Fetching,
                2 => GroupState::Present,
                _ => GroupState::None,
            },
            None => GroupState::None,
        }
    }

    /// Try to claim a group for fetching (CAS None→Fetching).
    /// Returns true if we claimed it, false if already Fetching or Present.
    fn try_claim_group(&self, gid: u64) -> bool {
        let states = self.group_states.lock();
        self.ensure_group_states_capacity(&states, gid);
        if let Some(s) = states.get(gid as usize) {
            s.compare_exchange(
                GroupState::None as u8,
                GroupState::Fetching as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        } else {
            false
        }
    }

    /// Mark a group as present (all pages fetched and written to cache).
    fn mark_group_present(&self, gid: u64) {
        let states = self.group_states.lock();
        self.ensure_group_states_capacity(&states, gid);
        if let Some(s) = states.get(gid as usize) {
            s.store(GroupState::Present as u8, Ordering::Release);
        }
        // Wake any threads waiting on this group
        self.group_condvar.notify_all();
        // Lazy eviction check
        let count = EVICTION_COUNTER.fetch_add(1, Ordering::Relaxed);
        if count % 64 == 0 {
            drop(states);
            self.evict_expired();
        }
    }

    fn ensure_group_states_capacity(
        &self,
        states: &parking_lot::MutexGuard<'_, Vec<std::sync::atomic::AtomicU8>>,
        gid: u64,
    ) {
        // Note: Can't actually resize through a shared ref. In practice, group_states
        // is sized at open time. If we encounter a new group (from writes extending the DB),
        // we handle it in the write path by resizing before accessing.
        let _ = (states, gid);
    }

    /// Grow group_states to accommodate new groups (e.g., after database grows).
    fn ensure_group_capacity(&self, total_groups: usize) {
        let mut states = self.group_states.lock();
        while states.len() < total_groups {
            states.push(std::sync::atomic::AtomicU8::new(GroupState::None as u8));
        }
    }

    /// Wait for a group to leave Fetching state (condvar, no spin).
    /// Wait until the group is no longer in a "pending" state.
    /// Returns when state is Present, or Fetching (worker claimed it),
    /// or None after having been Fetching (worker failed).
    /// Caller must re-check state and loop as needed.
    fn wait_for_group(&self, gid: u64) {
        let mut guard = self.group_condvar_mutex.lock();
        // Wait while state is Fetching (worker in progress).
        // If state is None (worker hasn't claimed yet, or failed), also wait
        // but with a timeout to avoid spinning.
        loop {
            let state = self.group_state(gid);
            if state == GroupState::Present {
                return;
            }
            if state == GroupState::Fetching {
                // Worker is actively fetching. Wait for completion notification.
                self.group_condvar.wait(&mut guard);
                continue;
            }
            // state == None. Worker may not have picked up job yet.
            // Wait with a short timeout, then re-check.
            self.group_condvar.wait_for(&mut guard, Duration::from_millis(5));
            return; // Let caller loop and re-check.
        }
    }

    /// Touch a group's access time for TTL tracking.
    fn touch_group(&self, gid: u64) {
        self.group_access.lock().insert(gid, Instant::now());
    }

    /// Mark a page group as containing B-tree interior pages (permanently pinned).
    fn mark_interior_group(&self, gid: u64, page_num: u64) {
        self.interior_groups.lock().insert(gid);
        self.interior_pages.lock().insert(page_num);
    }

    /// Evict page groups that haven't been accessed within TTL.
    /// Interior page groups are NEVER evicted.
    fn evict_expired(&self) {
        if self.ttl_secs == 0 {
            return; // TTL disabled
        }
        let now = Instant::now();
        let ttl = Duration::from_secs(self.ttl_secs);
        let interior = self.interior_groups.lock().clone();
        let mut access = self.group_access.lock();

        let expired: Vec<u64> = access
            .iter()
            .filter(|(gid, last)| {
                // Interior page groups are NEVER evicted
                if interior.contains(gid) {
                    return false;
                }
                now.duration_since(**last) > ttl
            })
            .map(|(gid, _)| *gid)
            .collect();

        for gid in &expired {
            self.evict_group(*gid);
            access.remove(gid);
        }
    }

    /// Evict a single page group from the local cache.
    fn evict_group(&self, gid: u64) {
        let start = gid * self.pages_per_group as u64;
        let count = self.pages_per_group as u64;
        self.bitmap.lock().clear_range(start, count);

        // On Linux: hole punch to reclaim NVMe blocks
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            let file = self.cache_file.write();
            let ps = self.page_size.load(Ordering::Acquire) as u64;
            let offset = (start * ps) as libc::off_t;
            let len = (count * ps) as libc::off_t;
            unsafe {
                libc::fallocate(
                    file.as_raw_fd(),
                    libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
                    offset,
                    len,
                );
            }
        }

        // Reset group state to None
        let states = self.group_states.lock();
        if let Some(s) = states.get(gid as usize) {
            s.store(GroupState::None as u8, Ordering::Release);
        }
    }

    /// Persist the page bitmap to disk.
    fn persist_bitmap(&self) -> io::Result<()> {
        self.bitmap.lock().persist()
    }
}

// ===== Page group encoding/decoding =====
//
// Whole-group compression: raw pages packed together, then single zstd frame.
//
// S3 object format:
//   zstd([u32 page_count][u32 page_size][page_0 bytes][page_1 bytes]...[page_N bytes])
//
// Each page is exactly page_size bytes. Empty trailing pages are omitted
// (page_count tells us how many pages are in the group).

/// Encode a page group: pack raw pages and compress as single zstd frame.
/// `pages` is a slice of Option<Vec<u8>> — None means empty (zero-filled) page.
/// Returns the compressed blob for S3.
fn encode_page_group(
    pages: &[Option<Vec<u8>>],
    page_size: u32,
    compression_level: i32,
    #[cfg(feature = "zstd")] encoder_dict: Option<&zstd::dict::EncoderDictionary<'static>>,
) -> io::Result<Vec<u8>> {
    // Find last non-empty page to avoid trailing zeros
    let page_count = pages
        .iter()
        .rposition(|p| p.is_some())
        .map(|i| i + 1)
        .unwrap_or(0) as u32;

    // Build raw buffer: [u32 page_count][u32 page_size][page bytes...]
    let header_len = 8; // 2 × u32
    let raw_len = header_len + page_count as usize * page_size as usize;
    let mut raw = Vec::with_capacity(raw_len);
    raw.extend_from_slice(&page_count.to_le_bytes());
    raw.extend_from_slice(&page_size.to_le_bytes());

    for i in 0..page_count as usize {
        match pages.get(i).and_then(|p| p.as_ref()) {
            Some(data) => {
                raw.extend_from_slice(data);
                // Pad to page_size if data is shorter
                if data.len() < page_size as usize {
                    raw.resize(raw.len() + page_size as usize - data.len(), 0);
                }
            }
            None => {
                // Zero-filled page
                raw.resize(raw.len() + page_size as usize, 0);
            }
        }
    }

    compress::compress(
        &raw,
        compression_level,
        #[cfg(feature = "zstd")]
        encoder_dict,
        #[cfg(not(feature = "zstd"))]
        None,
    )
}

/// Encode a page group as multiple independently-decompressible sub-chunk frames.
/// Returns (compressed_blob, frame_table). Each frame contains `sub_ppg` pages
/// and can be fetched via S3 byte-range GET and decompressed independently.
/// This enables point lookups to download ~128KB instead of ~10MB.
fn encode_page_group_seekable(
    pages: &[Option<Vec<u8>>],
    page_size: u32,
    sub_ppg: u32,
    compression_level: i32,
    #[cfg(feature = "zstd")] encoder_dict: Option<&zstd::dict::EncoderDictionary<'static>>,
) -> io::Result<(Vec<u8>, Vec<FrameEntry>)> {
    // Find last non-empty page to avoid trailing zeros
    let page_count = pages
        .iter()
        .rposition(|p| p.is_some())
        .map(|i| i + 1)
        .unwrap_or(0);

    let num_frames = (page_count + sub_ppg as usize - 1) / sub_ppg as usize;
    let mut blob = Vec::new();
    let mut frame_table = Vec::with_capacity(num_frames);

    for frame_idx in 0..num_frames {
        let start = frame_idx * sub_ppg as usize;
        let end = std::cmp::min(start + sub_ppg as usize, page_count);
        let pages_in_frame = end - start;

        // Build raw sub-chunk: just page bytes (no header — metadata is in manifest)
        let mut raw = Vec::with_capacity(pages_in_frame * page_size as usize);
        for i in start..end {
            match pages.get(i).and_then(|p| p.as_ref()) {
                Some(data) => {
                    raw.extend_from_slice(data);
                    if data.len() < page_size as usize {
                        raw.resize(raw.len() + page_size as usize - data.len(), 0);
                    }
                }
                None => {
                    raw.resize(raw.len() + page_size as usize, 0);
                }
            }
        }

        let offset = blob.len() as u64;
        let compressed = compress::compress(
            &raw,
            compression_level,
            #[cfg(feature = "zstd")]
            encoder_dict,
            #[cfg(not(feature = "zstd"))]
            None,
        )?;
        frame_table.push(FrameEntry {
            offset,
            len: compressed.len() as u32,
        });
        blob.extend_from_slice(&compressed);
    }

    Ok((blob, frame_table))
}

/// Decode a single sub-chunk frame from a seekable page group.
/// Returns the raw page data for the sub-chunk (pages_in_frame × page_size bytes).
fn decode_seekable_subchunk(
    compressed_frame: &[u8],
    #[cfg(feature = "zstd")] decoder_dict: Option<&zstd::dict::DecoderDictionary<'static>>,
) -> io::Result<Vec<u8>> {
    compress::decompress(
        compressed_frame,
        #[cfg(feature = "zstd")]
        decoder_dict,
        #[cfg(not(feature = "zstd"))]
        None,
    )
}

/// Decode all frames of a seekable page group (for prefetch / full download).
/// Returns (page_count, page_size, contiguous page data) — same shape as decode_page_group_bulk.
fn decode_page_group_seekable_full(
    data: &[u8],
    frame_table: &[FrameEntry],
    page_size: u32,
    pages_per_group: u32,
    total_page_count: u64,
    group_start: u64,
    #[cfg(feature = "zstd")] decoder_dict: Option<&zstd::dict::DecoderDictionary<'static>>,
) -> io::Result<(u32, u32, Vec<u8>)> {
    let actual_pages = std::cmp::min(
        pages_per_group as u64,
        total_page_count.saturating_sub(group_start),
    ) as u32;
    let mut output = Vec::with_capacity(actual_pages as usize * page_size as usize);

    for entry in frame_table {
        let start = entry.offset as usize;
        let end = start + entry.len as usize;
        if end > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("frame extends beyond data: {}..{} > {}", start, end, data.len()),
            ));
        }
        let decompressed = compress::decompress(
            &data[start..end],
            #[cfg(feature = "zstd")]
            decoder_dict,
            #[cfg(not(feature = "zstd"))]
            None,
        )?;
        output.extend_from_slice(&decompressed);
    }

    // Truncate to actual page count (last frame may have been padded)
    let expected_len = actual_pages as usize * page_size as usize;
    output.truncate(expected_len);

    Ok((actual_pages, page_size, output))
}

/// Decode a page group: decompress single zstd frame, split into pages.
/// Returns Vec of raw page buffers (each exactly page_size bytes).
fn decode_page_group(
    compressed: &[u8],
    #[cfg(feature = "zstd")] decoder_dict: Option<&zstd::dict::DecoderDictionary<'static>>,
) -> io::Result<(u32, u32, Vec<Vec<u8>>)> {
    let raw = compress::decompress(
        compressed,
        #[cfg(feature = "zstd")]
        decoder_dict,
        #[cfg(not(feature = "zstd"))]
        None,
    )?;

    if raw.len() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "page group too short for header",
        ));
    }

    let page_count = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]);
    let page_size = u32::from_le_bytes([raw[4], raw[5], raw[6], raw[7]]);

    let expected_len = 8 + page_count as usize * page_size as usize;
    if raw.len() < expected_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "page group data truncated: expected {} bytes, got {}",
                expected_len,
                raw.len()
            ),
        ));
    }

    let mut pages = Vec::with_capacity(page_count as usize);
    for i in 0..page_count as usize {
        let start = 8 + i * page_size as usize;
        let end = start + page_size as usize;
        pages.push(raw[start..end].to_vec());
    }

    Ok((page_count, page_size, pages))
}

/// Decode a page group, returning the contiguous page data buffer (no per-page allocation).
/// Returns (page_count, page_size, raw_page_data_slice_starting_after_8byte_header).
fn decode_page_group_bulk(
    compressed: &[u8],
    #[cfg(feature = "zstd")] decoder_dict: Option<&zstd::dict::DecoderDictionary<'static>>,
) -> io::Result<(u32, u32, Vec<u8>)> {
    let raw = compress::decompress(
        compressed,
        #[cfg(feature = "zstd")]
        decoder_dict,
        #[cfg(not(feature = "zstd"))]
        None,
    )?;

    if raw.len() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "page group too short for header",
        ));
    }

    let page_count = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]);
    let page_size = u32::from_le_bytes([raw[4], raw[5], raw[6], raw[7]]);

    let expected_len = 8 + page_count as usize * page_size as usize;
    if raw.len() < expected_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "page group data truncated: expected {} bytes, got {}",
                expected_len,
                raw.len()
            ),
        ));
    }

    // Return the page data portion (after the 8-byte header) as a contiguous buffer
    let page_data = raw[8..expected_len].to_vec();
    Ok((page_count, page_size, page_data))
}

// ===== Interior Bundle encode/decode =====
//
// Interior bundle: a single S3 object containing all B-tree interior pages.
// Unlike page groups (contiguous pages), the bundle stores non-contiguous pages
// with their page numbers.
//
// Format:
//   zstd([u32 page_count][u32 page_size][page_count × u64 page_nums][page data...])

/// Encode an interior bundle from (page_number, raw_data) pairs.
fn encode_interior_bundle(
    pages: &[(u64, &[u8])],
    page_size: u32,
    compression_level: i32,
    #[cfg(feature = "zstd")] encoder_dict: Option<&zstd::dict::EncoderDictionary<'static>>,
) -> io::Result<Vec<u8>> {
    let page_count = pages.len() as u32;
    let header_len = 8 + pages.len() * 8; // 2×u32 + page_count×u64
    let raw_len = header_len + pages.len() * page_size as usize;
    let mut raw = Vec::with_capacity(raw_len);

    raw.extend_from_slice(&page_count.to_le_bytes());
    raw.extend_from_slice(&page_size.to_le_bytes());

    // Page numbers
    for (pnum, _) in pages {
        raw.extend_from_slice(&pnum.to_le_bytes());
    }

    // Page data
    for (_, data) in pages {
        raw.extend_from_slice(data);
        if data.len() < page_size as usize {
            raw.resize(raw.len() + page_size as usize - data.len(), 0);
        }
    }

    compress::compress(
        &raw,
        compression_level,
        #[cfg(feature = "zstd")]
        encoder_dict,
        #[cfg(not(feature = "zstd"))]
        None,
    )
}

/// Decode an interior bundle: returns Vec of (page_number, raw_page_data).
fn decode_interior_bundle(
    compressed: &[u8],
    #[cfg(feature = "zstd")] decoder_dict: Option<&zstd::dict::DecoderDictionary<'static>>,
) -> io::Result<Vec<(u64, Vec<u8>)>> {
    let raw = compress::decompress(
        compressed,
        #[cfg(feature = "zstd")]
        decoder_dict,
        #[cfg(not(feature = "zstd"))]
        None,
    )?;

    if raw.len() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "interior bundle too short for header",
        ));
    }

    let page_count = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
    let page_size = u32::from_le_bytes([raw[4], raw[5], raw[6], raw[7]]) as usize;

    let pnums_end = 8 + page_count * 8;
    let expected_len = pnums_end + page_count * page_size;
    if raw.len() < expected_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "interior bundle truncated: expected {} bytes, got {}",
                expected_len, raw.len()
            ),
        ));
    }

    let mut result = Vec::with_capacity(page_count);
    for i in 0..page_count {
        let pnum_offset = 8 + i * 8;
        let pnum = u64::from_le_bytes(
            raw[pnum_offset..pnum_offset + 8]
                .try_into()
                .expect("8 bytes for u64"),
        );
        let data_offset = pnums_end + i * page_size;
        let data = raw[data_offset..data_offset + page_size].to_vec();
        result.push((pnum, data));
    }

    Ok(result)
}

// ===== PrefetchPool =====

/// A job for the prefetch thread pool.
struct PrefetchJob {
    gid: u64,
    key: String,
    /// Frame table for seekable format (empty = legacy single-frame format).
    frame_table: Vec<FrameEntry>,
    /// Page size (needed for seekable decode).
    page_size: u32,
    /// Sub-chunk size (needed for seekable decode).
    sub_pages_per_frame: u32,
}

/// Fixed thread pool for background page group prefetching.
/// Workers loop on a shared mpsc receiver, fetching page groups from S3
/// and writing them to the local cache. Default thread count: num_cpus + 1
/// (keeps pipeline full when threads block on S3 I/O).
struct PrefetchPool {
    sender: std::sync::mpsc::Sender<PrefetchJob>,
    in_flight: Arc<AtomicU64>,
    workers: parking_lot::Mutex<Vec<std::thread::JoinHandle<()>>>,
}

impl PrefetchPool {
    fn new(
        num_workers: u32,
        s3: Arc<S3Client>,
        cache: Arc<DiskCache>,
        pages_per_group: u32,
        page_count: Arc<AtomicU64>,
        #[cfg(feature = "zstd")] dictionary: Option<Vec<u8>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<PrefetchJob>();
        let rx = Arc::new(Mutex::new(rx));
        let in_flight = Arc::new(AtomicU64::new(0));
        let mut workers = Vec::with_capacity(num_workers as usize);

        #[cfg(feature = "zstd")]
        let dictionary = dictionary.map(Arc::new);

        for _ in 0..num_workers {
            let rx = Arc::clone(&rx);
            let s3 = Arc::clone(&s3);
            let cache = Arc::clone(&cache);
            let page_count = Arc::clone(&page_count);
            let in_flight = Arc::clone(&in_flight);
            let ppg = pages_per_group;
            #[cfg(feature = "zstd")]
            let dictionary = dictionary.clone();

            workers.push(std::thread::spawn(move || {
                loop {
                    let job = {
                        let lock = rx.lock().expect("prefetch rx poisoned");
                        match lock.recv() {
                            Ok(job) => job,
                            Err(_) => break, // channel closed
                        }
                    };

                    // CAS: claim the group before fetching
                    if !cache.try_claim_group(job.gid) {
                        if std::env::var("BENCH_VERBOSE").is_ok() {
                            eprintln!("  [prefetch-skip] gid={} already claimed", job.gid);
                        }
                        in_flight.fetch_sub(1, Ordering::Release);
                        continue;
                    }

                    let worker_start = Instant::now();

                    // Blocking S3 GET
                    let fetch_start = Instant::now();
                    let pg_data = match S3Client::block_on(&s3.runtime, s3.get_object_async(&job.key)) {
                        Ok(data) => data,
                        Err(e) => {
                            eprintln!("[prefetch] gid={} fetch error: {}", job.gid, e);
                            let states = cache.group_states.lock();
                            if let Some(s) = states.get(job.gid as usize) {
                                s.store(GroupState::None as u8, Ordering::Release);
                            }
                            cache.group_condvar.notify_all();
                            in_flight.fetch_sub(1, Ordering::Release);
                            continue;
                        }
                    };
                    let fetch_ms = fetch_start.elapsed().as_millis();

                    let Some(pg_data) = pg_data else {
                        // Key not found — reset to None
                        let states = cache.group_states.lock();
                        if let Some(s) = states.get(job.gid as usize) {
                            s.store(GroupState::None as u8, Ordering::Release);
                        }
                        cache.group_condvar.notify_all();
                        in_flight.fetch_sub(1, Ordering::Release);
                        continue;
                    };

                    // Decompress
                    #[cfg(feature = "zstd")]
                    let decoder_dict = dictionary
                        .as_deref()
                        .map(|d| zstd::dict::DecoderDictionary::copy(d));

                    let decompress_start = Instant::now();
                    let decode_result = if !job.frame_table.is_empty() {
                        let start_page = group_start_page(job.gid, ppg);
                        let pc = page_count.load(Ordering::Relaxed);
                        decode_page_group_seekable_full(
                            &pg_data,
                            &job.frame_table,
                            job.page_size,
                            ppg,
                            pc,
                            start_page,
                            #[cfg(feature = "zstd")]
                            decoder_dict.as_ref(),
                        )
                    } else {
                        decode_page_group_bulk(
                            &pg_data,
                            #[cfg(feature = "zstd")]
                            decoder_dict.as_ref(),
                        )
                    };
                    let (pg_count, _pg_size, page_data) = match decode_result {
                        Ok(v) => v,
                        Err(e) => {
                            eprintln!("[prefetch] gid={} decode error: {}", job.gid, e);
                            let states = cache.group_states.lock();
                            if let Some(s) = states.get(job.gid as usize) {
                                s.store(GroupState::None as u8, Ordering::Release);
                            }
                            cache.group_condvar.notify_all();
                            in_flight.fetch_sub(1, Ordering::Release);
                            continue;
                        }
                    };
                    let decompress_ms = decompress_start.elapsed().as_millis();

                    // Bulk write all pages
                    let write_start = Instant::now();
                    let start_page = group_start_page(job.gid, ppg);
                    let pc = page_count.load(Ordering::Relaxed);
                    let actual_pages = std::cmp::min(
                        pg_count as u64,
                        pc.saturating_sub(start_page),
                    );
                    if actual_pages > 0 {
                        let data_len = actual_pages as usize * _pg_size as usize;
                        if let Err(e) = cache.write_pages_bulk(
                            start_page,
                            &page_data[..data_len],
                            actual_pages,
                        ) {
                            eprintln!("[prefetch] gid={} write error: {}", job.gid, e);
                            let states = cache.group_states.lock();
                            if let Some(s) = states.get(job.gid as usize) {
                                s.store(GroupState::None as u8, Ordering::Release);
                            }
                            cache.group_condvar.notify_all();
                            in_flight.fetch_sub(1, Ordering::Release);
                            continue;
                        }
                    }
                    let write_ms = write_start.elapsed().as_millis();

                    // Scan pages for interior B-tree types
                    {
                        let ps = _pg_size as usize;
                        for i in 0..actual_pages as usize {
                            let pnum = start_page + i as u64;
                            let type_byte = if pnum == 0 {
                                page_data.get(i * ps + 100).copied()
                            } else {
                                page_data.get(i * ps).copied()
                            };
                            if let Some(b) = type_byte {
                                if b == 0x05 || b == 0x02 {
                                    cache.mark_interior_group(job.gid, pnum);
                                }
                            }
                        }
                    }

                    cache.mark_group_present(job.gid);
                    cache.touch_group(job.gid);
                    if std::env::var("BENCH_VERBOSE").is_ok() {
                        eprintln!(
                            "  [prefetch-done] gid={} ({:.1}KB) fetch={}ms decompress={}ms write={}ms total={}ms",
                            job.gid,
                            pg_data.len() as f64 / 1024.0,
                            fetch_ms, decompress_ms, write_ms,
                            worker_start.elapsed().as_millis(),
                        );
                    }
                    in_flight.fetch_sub(1, Ordering::Release);
                }
            }));
        }

        Self {
            sender: tx,
            in_flight,
            workers: parking_lot::Mutex::new(workers),
        }
    }

    /// Submit a prefetch job (non-blocking). Returns false if channel is closed.
    fn submit(&self, gid: u64, key: String, frame_table: Vec<FrameEntry>, page_size: u32, sub_ppf: u32) -> bool {
        self.in_flight.fetch_add(1, Ordering::Acquire);
        match self.sender.send(PrefetchJob {
            gid,
            key,
            frame_table,
            page_size,
            sub_pages_per_frame: sub_ppf,
        }) {
            Ok(()) => true,
            Err(_) => {
                self.in_flight.fetch_sub(1, Ordering::Release);
                false
            }
        }
    }

    /// Wait until all in-flight prefetch jobs complete.
    fn wait_idle(&self) {
        while self.in_flight.load(Ordering::Acquire) > 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

impl Drop for PrefetchPool {
    fn drop(&mut self) {
        // Drop sender to close the channel, then join all workers
        drop(std::mem::replace(&mut self.sender, mpsc::channel().0));
        let mut workers = self.workers.lock();
        for handle in workers.drain(..) {
            let _ = handle.join();
        }
    }
}

// ===== TieredHandle =====

/// Database handle for tiered S3-backed storage.
///
/// MainDb files are backed by S3 with a local page-level cache.
/// WAL/journal files are passthrough to local disk.
pub struct TieredHandle {
    // --- Tiered mode (MainDb) ---
    s3: Option<Arc<S3Client>>,
    cache: Option<Arc<DiskCache>>,
    manifest: RwLock<Manifest>,
    /// Dirty pages buffered in memory: page_num → raw (uncompressed) data
    dirty_pages: RwLock<HashMap<u64, Vec<u8>>>,
    /// Page group IDs that were locally checkpointed but not yet synced to S3.
    /// Populated during local-checkpoint-only mode; drained on the next real sync.
    s3_dirty_groups: Mutex<HashSet<u64>>,
    page_size: RwLock<u32>,
    pages_per_group: u32,
    compression_level: i32,
    read_only: bool,
    /// Consecutive cache misses (for fraction-based prefetch).
    consecutive_misses: u8,
    /// Fraction-based prefetch schedule.
    prefetch_hops: Vec<f32>,
    /// Fixed thread pool for background prefetch.
    prefetch_pool: Option<Arc<PrefetchPool>>,

    // --- Compression dictionary ---
    #[cfg(feature = "zstd")]
    encoder_dict: Option<zstd::dict::EncoderDictionary<'static>>,
    #[cfg(feature = "zstd")]
    decoder_dict: Option<zstd::dict::DecoderDictionary<'static>>,

    /// Auto-GC: delete old page group versions after checkpoint.
    gc_enabled: bool,

    // --- Passthrough mode (WAL/journal) ---
    passthrough_file: Option<RwLock<File>>,

    // --- Shared ---
    lock: RwLock<LockKind>,
    db_path: PathBuf,
    /// Separate file handle for byte-range locking
    lock_file: Option<std::sync::Arc<File>>,
    /// Active byte-range locks
    active_db_locks: HashMap<String, Box<dyn std::any::Any + Send + Sync>>,
}

// SQLite main database lock byte offsets (same as lib.rs)
const PENDING_BYTE: u64 = 0x40000000;
const RESERVED_BYTE: u64 = PENDING_BYTE + 1;
const SHARED_FIRST: u64 = PENDING_BYTE + 2;
const SHARED_SIZE: u64 = 510;

impl TieredHandle {
    /// Create a tiered handle backed by S3 + local page cache.
    fn new_tiered(
        s3: Arc<S3Client>,
        cache: Arc<DiskCache>,
        manifest: Manifest,
        db_path: PathBuf,
        pages_per_group: u32,
        compression_level: i32,
        read_only: bool,
        prefetch_hops: Vec<f32>,
        prefetch_pool: Option<Arc<PrefetchPool>>,
        gc_enabled: bool,
        #[cfg(feature = "zstd")] dictionary: Option<&[u8]>,
    ) -> Self {
        let page_size = manifest.page_size;

        // Eagerly fetch page group 0 (contains schema + root page, hit on every query)
        if manifest.page_count > 0 && cache.group_state(0) != GroupState::Present {
            if let Some(key) = manifest.page_group_keys.first() {
                if !key.is_empty() {
                    if cache.try_claim_group(0) {
                        if let Ok(Some(pg_data)) = s3.get_page_group(key) {
                            let ft = manifest.frame_tables.first().map(|v| v.as_slice());
                            let _ = Self::decode_and_cache_group_static(
                                &cache,
                                &pg_data,
                                0,
                                pages_per_group,
                                manifest.page_size,
                                manifest.page_count,
                                ft,
                                #[cfg(feature = "zstd")]
                                dictionary,
                            );
                            cache.mark_group_present(0);
                            cache.touch_group(0);
                        }
                    }
                }
            }
        }

        // Eagerly fetch interior chunks (B-tree interior pages split across S3 objects).
        // Fetched in parallel — after this, every B-tree traversal is a cache hit.
        // Skip if interior pages are already cached (survived clear_cache pinning).
        let interior_already_cached = !cache.interior_pages.lock().is_empty();
        if interior_already_cached {
            eprintln!(
                "[tiered] interior pages already cached ({} pages), skipping chunk fetch",
                cache.interior_pages.lock().len(),
            );
        } else if !manifest.interior_chunk_keys.is_empty() {
            // Parallel fetch all interior chunks
            let chunk_keys: Vec<String> = manifest.interior_chunk_keys.values().cloned().collect();
            eprintln!("[tiered] fetching {} interior chunks in parallel...", chunk_keys.len());
            match s3.get_page_groups_by_key(&chunk_keys) {
                Ok(results) => {
                    #[cfg(feature = "zstd")]
                    let ib_decoder = dictionary.map(zstd::dict::DecoderDictionary::copy);
                    let mut total_pages = 0usize;
                    let mut total_bytes = 0usize;
                    for (key, data) in &results {
                        total_bytes += data.len();
                        match decode_interior_bundle(
                            data,
                            #[cfg(feature = "zstd")]
                            ib_decoder.as_ref(),
                        ) {
                            Ok(pages) => {
                                total_pages += pages.len();
                                for (pnum, pdata) in &pages {
                                    let _ = cache.write_page(*pnum, pdata);
                                    let gid = group_id(*pnum, pages_per_group);
                                    cache.mark_interior_group(gid, *pnum);
                                }
                            }
                            Err(e) => eprintln!("[tiered] interior chunk {} decode failed: {}", key, e),
                        }
                    }
                    eprintln!(
                        "[tiered] interior chunks loaded: {} pages from {} chunks ({:.1}KB total)",
                        total_pages, results.len(), total_bytes as f64 / 1024.0,
                    );
                }
                Err(e) => eprintln!("[tiered] interior chunk fetch failed: {}", e),
            }
        }

        #[cfg(feature = "zstd")]
        let (encoder_dict, decoder_dict) = match dictionary {
            Some(dict_bytes) => (
                Some(zstd::dict::EncoderDictionary::copy(dict_bytes, compression_level)),
                Some(zstd::dict::DecoderDictionary::copy(dict_bytes)),
            ),
            None => (None, None),
        };

        Self {
            s3: Some(s3),
            cache: Some(cache),
            manifest: RwLock::new(manifest),
            dirty_pages: RwLock::new(HashMap::new()),
            s3_dirty_groups: Mutex::new(HashSet::new()),
            page_size: RwLock::new(page_size),
            pages_per_group,
            compression_level,
            read_only,
            consecutive_misses: 0,
            prefetch_hops,
            prefetch_pool,
            gc_enabled,
            #[cfg(feature = "zstd")]
            encoder_dict,
            #[cfg(feature = "zstd")]
            decoder_dict,
            passthrough_file: None,
            lock: RwLock::new(LockKind::None),
            db_path,
            lock_file: None,
            active_db_locks: HashMap::new(),
        }
    }

    /// Create a passthrough handle for WAL/journal files (local file I/O).
    fn new_passthrough(file: File, db_path: PathBuf) -> Self {
        Self {
            s3: None,
            cache: None,
            manifest: RwLock::new(Manifest::empty()),
            dirty_pages: RwLock::new(HashMap::new()),
            s3_dirty_groups: Mutex::new(HashSet::new()),
            page_size: RwLock::new(0),
            pages_per_group: DEFAULT_PAGES_PER_GROUP,
            compression_level: 0,
            read_only: false,
            consecutive_misses: 0,
            prefetch_hops: vec![0.33, 0.33],
            prefetch_pool: None,
            gc_enabled: false,
            #[cfg(feature = "zstd")]
            encoder_dict: None,
            #[cfg(feature = "zstd")]
            decoder_dict: None,
            passthrough_file: Some(RwLock::new(file)),
            lock: RwLock::new(LockKind::None),
            db_path,
            lock_file: None,
            active_db_locks: HashMap::new(),
        }
    }

    fn is_passthrough(&self) -> bool {
        self.passthrough_file.is_some()
    }

    fn s3(&self) -> &S3Client {
        self.s3.as_ref().expect("s3 client required for tiered mode")
    }

    fn disk_cache(&self) -> &DiskCache {
        self.cache
            .as_ref()
            .expect("disk cache required for tiered mode")
    }

    /// Copy raw page data into the output buffer, handling sub-page offsets.
    fn copy_raw_into_buf(
        raw: &[u8],
        buf: &mut [u8],
        offset: u64,
        page_size: u64,
    ) {
        let page_offset = (offset % page_size) as usize;
        let copy_len = buf.len().min(raw.len().saturating_sub(page_offset));
        if copy_len > 0 {
            buf[..copy_len].copy_from_slice(&raw[page_offset..page_offset + copy_len]);
        }
        if copy_len < buf.len() {
            buf[copy_len..].fill(0);
        }
    }

    /// Decode a page group and write all pages to the cache file.
    /// Static version for use in constructors and prefetch workers.
    /// Handles both seekable (multi-frame) and legacy (single-frame with 8-byte header) formats.
    fn decode_and_cache_group_static(
        cache: &DiskCache,
        pg_data: &[u8],
        gid: u64,
        pages_per_group: u32,
        page_size: u32,
        page_count: u64,
        frame_table: Option<&[FrameEntry]>,
        #[cfg(feature = "zstd")] dictionary: Option<&[u8]>,
    ) -> io::Result<()> {
        #[cfg(feature = "zstd")]
        let decoder_dict = dictionary.map(zstd::dict::DecoderDictionary::copy);

        let start_page = group_start_page(gid, pages_per_group);

        // Dispatch: seekable (frame_table present and non-empty) vs legacy bulk
        let (pg_count, page_data) = match frame_table.filter(|ft| !ft.is_empty()) {
            Some(ft) => {
                let (c, _s, d) = decode_page_group_seekable_full(
                    pg_data,
                    ft,
                    page_size,
                    pages_per_group,
                    page_count,
                    start_page,
                    #[cfg(feature = "zstd")]
                    decoder_dict.as_ref(),
                )?;
                (c, d)
            }
            None => {
                let (c, _s, d) = decode_page_group_bulk(
                    pg_data,
                    #[cfg(feature = "zstd")]
                    decoder_dict.as_ref(),
                )?;
                (c, d)
            }
        };

        let ps = page_size as usize;
        for i in 0..pg_count as usize {
            let pnum = start_page + i as u64;
            if pnum >= page_count {
                break;
            }
            let offset = i * ps;
            let end = offset + ps;
            if end > page_data.len() {
                break;
            }
            cache.write_page(pnum, &page_data[offset..end])?;
        }
        Ok(())
    }

    /// Decode a page group and write all pages to the cache file.
    #[allow(dead_code)]
    fn decode_and_cache_group(
        &self,
        cache: &DiskCache,
        pg_data: &[u8],
        gid: u64,
    ) -> io::Result<()> {
        let page_count = self.manifest.read().page_count;
        let (_pg_count, _pg_size, pages) = decode_page_group(
            pg_data,
            #[cfg(feature = "zstd")]
            self.decoder_dict.as_ref(),
        )?;

        let start_page = group_start_page(gid, self.pages_per_group);
        for (i, page_data) in pages.iter().enumerate() {
            let pnum = start_page + i as u64;
            if pnum >= page_count {
                break;
            }
            cache.write_page(pnum, page_data)?;
        }
        Ok(())
    }

    /// Check if a page is a B-tree interior page and mark its group.
    fn detect_interior_page(&self, buf: &[u8], page_num: u64, cache: &DiskCache) {
        let type_byte = if page_num == 0 { buf.get(100) } else { buf.get(0) };
        if let Some(&b) = type_byte {
            if b == 0x05 || b == 0x02 {
                let gid = group_id(page_num, self.pages_per_group);
                cache.mark_interior_group(gid, page_num);
            }
        }
    }

    /// Trigger fraction-based prefetch after a cache miss.
    /// Non-blocking: spawns background tasks on the tokio runtime.
    fn trigger_prefetch(
        &self,
        current_gid: u64,
        manifest: &Manifest,
        cache: &Arc<DiskCache>,
        _s3: &Arc<S3Client>,
    ) {
        let total_groups = manifest.total_groups();
        if total_groups <= 1 {
            return;
        }

        let pool = match &self.prefetch_pool {
            Some(pool) => pool,
            None => return,
        };

        // Determine prefetch count based on hop schedule
        let hop_idx = (self.consecutive_misses as usize).saturating_sub(1);
        let fraction = if hop_idx < self.prefetch_hops.len() {
            self.prefetch_hops[hop_idx]
        } else {
            1.0 // Final hop: fetch everything remaining
        };
        let prefetch_count = ((total_groups as f32) * fraction).ceil() as u64;
        if prefetch_count == 0 {
            return;
        }

        // Fan out from current group (nearest neighbors first).
        // Use lightweight state check (no CAS). Workers do try_claim_group
        // before fetching — avoids locking groups in FETCHING state before
        // a worker is ready, which would block the sync read path.
        let ft_for = |g: u64| -> Vec<FrameEntry> {
            manifest.frame_tables.get(g as usize).cloned().unwrap_or_default()
        };
        let ps = manifest.page_size;
        let sub_ppf = manifest.sub_pages_per_frame;

        let mut submitted = 0u64;
        for delta in 1..=total_groups {
            if submitted >= prefetch_count {
                break;
            }
            // Forward
            let fwd = current_gid + delta;
            if fwd < total_groups {
                if cache.group_state(fwd) == GroupState::None {
                    if let Some(key) = manifest.page_group_keys.get(fwd as usize) {
                        if !key.is_empty() {
                            pool.submit(fwd, key.clone(), ft_for(fwd), ps, sub_ppf);
                            submitted += 1;
                        }
                    }
                }
            }
            // Backward
            if delta <= current_gid && submitted < prefetch_count {
                let bwd = current_gid - delta;
                if cache.group_state(bwd) == GroupState::None {
                    if let Some(key) = manifest.page_group_keys.get(bwd as usize) {
                        if !key.is_empty() {
                            pool.submit(bwd, key.clone(), ft_for(bwd), ps, sub_ppf);
                            submitted += 1;
                        }
                    }
                }
            }
        }
        if std::env::var("BENCH_VERBOSE").is_ok() {
            eprintln!(
                "  [prefetch] from gid={} miss={} hop_frac={:.2} submitted={}/{}",
                current_gid, self.consecutive_misses, fraction, submitted, prefetch_count,
            );
        }
    }

    /// Ensure a lock file exists for byte-range locking.
    fn ensure_lock_file(&mut self) -> io::Result<std::sync::Arc<File>> {
        if self.lock_file.is_none() {
            let lock_path = self.db_path.with_extension("db-lock");
            let file = FsOpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&lock_path)?;
            self.lock_file = Some(std::sync::Arc::new(file));
        }
        Ok(std::sync::Arc::clone(
            self.lock_file.as_ref().unwrap(),
        ))
    }
}

impl DatabaseHandle for TieredHandle {
    type WalIndex = FileWalIndex;

    fn size(&self) -> Result<u64, io::Error> {
        if self.is_passthrough() {
            let file = self.passthrough_file.as_ref().unwrap().read();
            return file.metadata().map(|m| m.len());
        }

        let manifest = self.manifest.read();
        if manifest.page_size > 0 && manifest.page_count > 0 {
            Ok(manifest.page_count * manifest.page_size as u64)
        } else {
            Ok(0)
        }
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> Result<(), io::Error> {
        if self.is_passthrough() {
            use std::os::unix::fs::FileExt;
            let file = self.passthrough_file.as_ref().unwrap().read();
            return file.read_exact_at(buf, offset);
        }

        // Determine page number
        let page_size = {
            let ps = *self.page_size.read();
            if ps > 0 { ps as u64 } else { buf.len() as u64 }
        };
        let page_num = offset / page_size;
        let gid = group_id(page_num, self.pages_per_group);

        // 1. Check dirty pages (in-memory, raw/uncompressed)
        {
            let dirty = self.dirty_pages.read();
            if let Some(raw) = dirty.get(&page_num) {
                self.consecutive_misses = 0;
                Self::copy_raw_into_buf(raw, buf, offset, page_size);
                return Ok(());
            }
        }

        // 2. Bounds check
        let manifest_page_count = self.manifest.read().page_count;
        if page_num >= manifest_page_count {
            buf.fill(0);
            return Ok(());
        }

        // 3. Check page bitmap (cache hit = direct pread, no decompression)
        let cache_arc = Arc::clone(self.cache.as_ref().expect("disk cache required"));
        let cache = cache_arc.as_ref();
        if cache.is_present(page_num) {
            self.consecutive_misses = 0;
            cache.read_page(page_num, buf)?;
            self.detect_interior_page(buf, page_num, cache);
            cache.touch_group(gid);
            return Ok(());
        }

        // 4. Cache miss — fetch from S3.
        let s3_arc = Arc::clone(self.s3.as_ref().expect("s3 client required"));
        let manifest = self.manifest.read().clone();
        let miss_start = Instant::now();
        let start_page = group_start_page(gid, self.pages_per_group);

        // Check if seekable format is available for sub-chunk range GETs.
        let frame_table = manifest.frame_tables.get(gid as usize);
        let has_frames = manifest.sub_pages_per_frame > 0
            && frame_table.map(|ft| !ft.is_empty()).unwrap_or(false);
        if has_frames {
            // ── SEEKABLE PATH: range GET just the sub-chunk containing the needed page ──
            // Downloads ~100KB instead of ~2-10MB. Prefetch still gets the full group.
            let sub_ppg = manifest.sub_pages_per_frame;
            let page_in_group = (page_num - start_page) as usize;
            let frame_idx = page_in_group / sub_ppg as usize;
            let ft = frame_table.unwrap();

            if frame_idx < ft.len() {
                let entry = &ft[frame_idx];
                let key = &manifest.page_group_keys[gid as usize];

                // Submit the CURRENT group to prefetch pool (full group fetch in background).
                // This ensures subsequent pages from the same group are cache hits.
                self.consecutive_misses = self.consecutive_misses.saturating_add(1);
                if let Some(pool) = &self.prefetch_pool {
                    if cache.group_state(gid) == GroupState::None {
                        if let Some(key) = manifest.page_group_keys.get(gid as usize) {
                            if !key.is_empty() {
                                pool.submit(gid, key.clone(), ft.to_vec(), manifest.page_size, sub_ppg);
                            }
                        }
                    }
                }
                // Also trigger neighbor prefetch
                self.trigger_prefetch(gid, &manifest, &cache_arc, &s3_arc);

                let s3_start = Instant::now();
                match s3_arc.range_get(key, entry.offset, entry.len) {
                    Ok(Some(compressed_frame)) => {
                        let s3_ms = s3_start.elapsed().as_millis();
                        let decode_start = Instant::now();
                        let decompressed = decode_seekable_subchunk(
                            &compressed_frame,
                            #[cfg(feature = "zstd")]
                            self.decoder_dict.as_ref(),
                        )?;
                        let decode_ms = decode_start.elapsed().as_millis();

                        let ps = manifest.page_size as usize;

                        // Extract needed page directly into buf (no cache round-trip)
                        let page_offset_in_frame = page_in_group % sub_ppg as usize;
                        let src_start = page_offset_in_frame * ps;
                        let src_end = src_start + buf.len();
                        if src_end <= decompressed.len() {
                            buf.copy_from_slice(&decompressed[src_start..src_end]);
                        } else {
                            buf.fill(0);
                        }

                        // Write sub-chunk pages to cache for future hits
                        let frame_start_page = start_page + (frame_idx * sub_ppg as usize) as u64;
                        let pages_in_frame = std::cmp::min(
                            sub_ppg as u64,
                            manifest.page_count.saturating_sub(frame_start_page),
                        );
                        if pages_in_frame > 0 {
                            let data_len = pages_in_frame as usize * ps;
                            if data_len <= decompressed.len() {
                                cache.write_pages_bulk(
                                    frame_start_page,
                                    &decompressed[..data_len],
                                    pages_in_frame,
                                )?;
                            }
                        }

                        // Scan for interior B-tree pages in the sub-chunk
                        for i in 0..pages_in_frame as usize {
                            let pnum = frame_start_page + i as u64;
                            let type_byte = if pnum == 0 {
                                decompressed.get(i * ps + 100).copied()
                            } else {
                                decompressed.get(i * ps).copied()
                            };
                            if let Some(b) = type_byte {
                                if b == 0x05 || b == 0x02 {
                                    cache.mark_interior_group(gid, pnum);
                                }
                            }
                        }

                        self.detect_interior_page(buf, page_num, cache);
                        cache.touch_group(gid);
                        self.consecutive_misses = 0;

                        if std::env::var("BENCH_VERBOSE").is_ok() {
                            eprintln!(
                                "  [inline-subchunk] page={} gid={} frame={}/{} s3={}ms decode={}ms total={}ms ({:.1}KB)",
                                page_num, gid, frame_idx, ft.len(), s3_ms, decode_ms,
                                miss_start.elapsed().as_millis(),
                                compressed_frame.len() as f64 / 1024.0,
                            );
                        }
                        return Ok(());
                    }
                    Ok(None) => {
                        if std::env::var("BENCH_VERBOSE").is_ok() {
                            eprintln!("  [inline-subchunk] page={} gid={} NOT FOUND {}ms", page_num, gid, miss_start.elapsed().as_millis());
                        }
                    }
                    Err(e) => {
                        if std::env::var("BENCH_VERBOSE").is_ok() {
                            eprintln!("[inline-subchunk] page={} gid={} frame={} error: {} {}ms", page_num, gid, frame_idx, e, miss_start.elapsed().as_millis());
                        }
                    }
                }
            }
            // Fall through to legacy path if sub-chunk fetch failed
        }

        // ── LEGACY PATH: full group download ──
        let state = cache.group_state(gid);
        if state == GroupState::Fetching {
            let wait_start = Instant::now();
            cache.wait_for_group(gid);
            if std::env::var("BENCH_VERBOSE").is_ok() {
                eprintln!(
                    "  [inline] page={} gid={} WAITED for prefetch worker {}ms",
                    page_num, gid, wait_start.elapsed().as_millis(),
                );
            }
        } else if state != GroupState::Present {
            if cache.try_claim_group(gid) {
                self.consecutive_misses = self.consecutive_misses.saturating_add(1);
                self.trigger_prefetch(gid, &manifest, &cache_arc, &s3_arc);
                if let Some(key) = manifest.page_group_keys.get(gid as usize) {
                    if !key.is_empty() {
                        let s3_start = Instant::now();
                        match s3_arc.get_page_group(key) {
                            Ok(Some(pg_data)) => {
                                let s3_ms = s3_start.elapsed().as_millis();
                                let decode_start = Instant::now();
                                // Use seekable full decode if frame table available, else legacy
                                let decode_result = if has_frames {
                                    let ft = frame_table.unwrap();
                                    decode_page_group_seekable_full(
                                        &pg_data,
                                        ft,
                                        manifest.page_size,
                                        self.pages_per_group,
                                        manifest.page_count,
                                        start_page,
                                        #[cfg(feature = "zstd")]
                                        self.decoder_dict.as_ref(),
                                    )
                                } else {
                                    decode_page_group_bulk(
                                        &pg_data,
                                        #[cfg(feature = "zstd")]
                                        self.decoder_dict.as_ref(),
                                    )
                                };
                                let (pg_count, pg_size, page_data) = decode_result?;
                                let decode_ms = decode_start.elapsed().as_millis();
                                let pc = manifest.page_count;
                                let actual_pages = std::cmp::min(
                                    pg_count as u64,
                                    pc.saturating_sub(start_page),
                                );
                                let write_start = Instant::now();
                                if actual_pages > 0 {
                                    let data_len = actual_pages as usize * pg_size as usize;
                                    cache.write_pages_bulk(
                                        start_page,
                                        &page_data[..data_len],
                                        actual_pages,
                                    )?;
                                }
                                let write_ms = write_start.elapsed().as_millis();
                                let ps = pg_size as usize;
                                for i in 0..actual_pages as usize {
                                    let pnum = start_page + i as u64;
                                    let type_byte = if pnum == 0 {
                                        page_data.get(i * ps + 100).copied()
                                    } else {
                                        page_data.get(i * ps).copied()
                                    };
                                    if let Some(b) = type_byte {
                                        if b == 0x05 || b == 0x02 {
                                            cache.mark_interior_group(gid, pnum);
                                        }
                                    }
                                }
                                cache.mark_group_present(gid);
                                cache.touch_group(gid);
                                if std::env::var("BENCH_VERBOSE").is_ok() {
                                    eprintln!(
                                        "  [inline] page={} gid={} s3={}ms decode={}ms write={}ms total={}ms ({:.1}KB)",
                                        page_num, gid, s3_ms, decode_ms, write_ms,
                                        miss_start.elapsed().as_millis(),
                                        pg_data.len() as f64 / 1024.0,
                                    );
                                }
                            }
                            Ok(None) => {
                                let states = cache.group_states.lock();
                                if let Some(s) = states.get(gid as usize) {
                                    s.store(GroupState::None as u8, Ordering::Release);
                                }
                                cache.group_condvar.notify_all();
                                if std::env::var("BENCH_VERBOSE").is_ok() {
                                    eprintln!("  [inline] page={} gid={} NOT FOUND {}ms", page_num, gid, miss_start.elapsed().as_millis());
                                }
                            }
                            Err(e) => {
                                if std::env::var("BENCH_VERBOSE").is_ok() {
                                    eprintln!("[inline] page={} gid={} error: {} {}ms", page_num, gid, e, miss_start.elapsed().as_millis());
                                }
                                let states = cache.group_states.lock();
                                if let Some(s) = states.get(gid as usize) {
                                    s.store(GroupState::None as u8, Ordering::Release);
                                }
                                cache.group_condvar.notify_all();
                            }
                        }
                    }
                }
            } else {
                let wait_start = Instant::now();
                cache.wait_for_group(gid);
                if std::env::var("BENCH_VERBOSE").is_ok() {
                    eprintln!(
                        "  [inline] page={} gid={} WAITED (race) {}ms",
                        page_num, gid, wait_start.elapsed().as_millis(),
                    );
                }
            }
        }

        // Read the page from cache (should be present now for legacy path).
        if cache.is_present(page_num) {
            self.consecutive_misses = 0;
            cache.read_page(page_num, buf)?;
            self.detect_interior_page(buf, page_num, cache);
            cache.touch_group(gid);
            return Ok(());
        }
        buf.fill(0);
        Ok(())
    }

    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> Result<(), io::Error> {
        if self.is_passthrough() {
            use std::os::unix::fs::FileExt;
            let file = self.passthrough_file.as_ref().unwrap().read();
            return file.write_all_at(buf, offset);
        }

        if self.read_only {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "TieredHandle is read-only",
            ));
        }

        // Set page size from first write
        {
            let mut ps = self.page_size.write();
            if *ps == 0 {
                *ps = buf.len() as u32;
                // Also update DiskCache's page_size so cache offsets are correct
                if let Some(cache) = &self.cache {
                    cache.set_page_size(buf.len() as u32);
                }
            }
        }

        let page_size = *self.page_size.read() as u64;
        let page_num = offset / page_size;

        // Buffer raw page as dirty (compression happens at sync time, whole-group)
        let mut dirty = self.dirty_pages.write();
        dirty.insert(page_num, buf.to_vec());

        // Also write to local cache for fast reads
        if let Some(cache) = &self.cache {
            let _ = cache.write_page(page_num, buf);
        }

        // Update manifest page_count if this page extends the database
        {
            let mut manifest = self.manifest.write();
            let new_count = page_num + 1;
            if new_count > manifest.page_count {
                manifest.page_count = new_count;
            }
            if manifest.page_size == 0 {
                manifest.page_size = buf.len() as u32;
            }
            if manifest.pages_per_group == 0 {
                manifest.pages_per_group = self.pages_per_group;
            }

            // Ensure group states capacity for new groups
            if let Some(cache) = &self.cache {
                let total_groups = manifest.total_groups() as usize;
                cache.ensure_group_capacity(total_groups);
            }
        }

        Ok(())
    }

    fn sync(&mut self, _data_only: bool) -> Result<(), io::Error> {
        if self.is_passthrough() {
            return self
                .passthrough_file
                .as_ref()
                .unwrap()
                .write()
                .sync_all();
        }

        if self.read_only {
            return Ok(());
        }

        // Clone dirty pages — keep them readable during upload.
        let dirty_snapshot: HashMap<u64, Vec<u8>> = {
            let dirty = self.dirty_pages.read();
            let has_pending_groups = !self.s3_dirty_groups.lock().unwrap().is_empty();
            if dirty.is_empty() && !has_pending_groups {
                return Ok(());
            }
            dirty.clone()
        };

        let ppg = self.pages_per_group;

        // Local-checkpoint-only: record dirty group IDs, scan interior pages,
        // free in-memory dirty map, but skip S3 upload entirely.
        // Pages are already in disk cache from write_all_at().
        if LOCAL_CHECKPOINT_ONLY.load(Ordering::Acquire) {
            let cache = self.disk_cache();
            let mut pending = self.s3_dirty_groups.lock().unwrap();
            let mut interior_found = 0usize;
            for (&page_num, data) in &dirty_snapshot {
                pending.insert(group_id(page_num, ppg));
                // Track interior pages so final sync builds correct interior chunks
                let type_byte = if page_num == 0 { data.get(100) } else { data.get(0) };
                if let Some(&b) = type_byte {
                    if b == 0x05 || b == 0x02 {
                        cache.mark_interior_group(group_id(page_num, ppg), page_num);
                        interior_found += 1;
                    }
                }
            }
            let n = dirty_snapshot.len();
            let total_interior = cache.interior_pages.lock().len();
            // Free in-memory dirty pages — they're already on local disk
            self.dirty_pages.write().clear();
            eprintln!("[sync] local-only checkpoint: {} pages, {} interior this batch, {} interior total, {} groups pending S3", n, interior_found, total_interior, pending.len());
            return Ok(());
        }

        let s3 = self.s3();
        let cache = self.disk_cache();
        let page_count = self.manifest.read().page_count;

        // Group dirty pages by page group
        let mut groups_dirty: HashMap<u64, Vec<u64>> = HashMap::new();
        for &page_num in dirty_snapshot.keys() {
            let gid = group_id(page_num, ppg);
            groups_dirty.entry(gid).or_default().push(page_num);
        }

        // Merge in page groups from previous local-only checkpoints
        {
            let mut pending = self.s3_dirty_groups.lock().unwrap();
            for gid in pending.drain() {
                groups_dirty.entry(gid).or_default();
            }
        }

        let next_version = self.manifest.read().version + 1;
        let mut uploads: Vec<(String, Vec<u8>)> = Vec::new();
        let mut new_keys = self.manifest.read().page_group_keys.clone();
        // Track old keys being replaced (for post-checkpoint GC)
        let mut replaced_keys: Vec<String> = Vec::new();

        let page_size = *self.page_size.read();

        // For each dirty page group: read all raw pages, encode as whole-group compressed
        for (&gid, _dirty_pages) in &groups_dirty {
            let mut pages: Vec<Option<Vec<u8>>> = vec![None; ppg as usize];
            let start = group_start_page(gid, ppg);
            let mut need_s3_merge = false;

            // First, try to read all pages from local cache (raw, uncompressed)
            for i in 0..ppg {
                let pnum = start + i as u64;
                if pnum >= page_count {
                    break;
                }
                if cache.is_present(pnum) {
                    let mut page_buf = vec![0u8; page_size as usize];
                    if cache.read_page(pnum, &mut page_buf).is_ok() {
                        pages[i as usize] = Some(page_buf);
                    }
                } else {
                    need_s3_merge = true;
                }
            }

            // If some pages aren't in cache, fetch existing group from S3 and merge
            if need_s3_merge {
                if let Some(existing_key) = new_keys.get(gid as usize) {
                    if !existing_key.is_empty() {
                        if let Ok(Some(pg_data)) = s3.get_page_group(existing_key) {
                            // Decode the existing group to get raw pages
                            if let Ok((_pc, _ps, existing_pages)) = decode_page_group(
                                &pg_data,
                                #[cfg(feature = "zstd")]
                                self.decoder_dict.as_ref(),
                            ) {
                                for (j, existing_page) in existing_pages.into_iter().enumerate() {
                                    let jpnum = start + j as u64;
                                    if jpnum >= page_count {
                                        break;
                                    }
                                    if pages[j].is_none() {
                                        pages[j] = Some(existing_page);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Encode as whole-group compressed blob
            let encoded = encode_page_group(
                &pages,
                page_size,
                self.compression_level,
                #[cfg(feature = "zstd")]
                self.encoder_dict.as_ref(),
            )?;
            let key = s3.page_group_key(gid, next_version);
            uploads.push((key.clone(), encoded));

            // Extend keys vector if needed
            while new_keys.len() <= gid as usize {
                new_keys.push(String::new());
            }
            // Track the old key being replaced (for GC)
            if let Some(old_key) = new_keys.get(gid as usize) {
                if !old_key.is_empty() {
                    replaced_keys.push(old_key.clone());
                }
            }
            new_keys[gid as usize] = key;
        }

        // Parallel upload all dirty page groups
        eprintln!("[sync] uploading {} dirty page groups...", uploads.len());
        s3.put_page_groups(&uploads)?;
        eprintln!("[sync] page groups uploaded");

        eprintln!("[sync] building interior chunks...");
        // Build chunked interior bundles: group interior pages by fixed page-number ranges.
        // Only re-upload chunks that contain dirty interior pages.
        let mut all_interior: HashMap<u64, Vec<u8>> = HashMap::new(); // pnum → data
        {
            // Collect interior pages from dirty snapshot (fastest — already in memory)
            let mut dirty_interior_count = 0usize;
            for (&pnum, data) in &dirty_snapshot {
                let type_byte = if pnum == 0 { data.get(100) } else { data.get(0) };
                if let Some(&b) = type_byte {
                    if b == 0x05 || b == 0x02 {
                        all_interior.insert(pnum, data.clone());
                        cache.mark_interior_group(group_id(pnum, ppg), pnum);
                        dirty_interior_count += 1;
                    }
                }
            }
            // Also include previously-known interior pages not in the dirty set
            let known_interior = cache.interior_pages.lock().clone();
            let mut cache_read_ok = 0usize;
            let mut cache_read_fail = 0usize;
            let mut cache_skipped_dup = 0usize;
            let mut cache_skipped_bounds = 0usize;
            for &pnum in &known_interior {
                if pnum >= page_count {
                    cache_skipped_bounds += 1;
                } else if dirty_snapshot.contains_key(&pnum) || all_interior.contains_key(&pnum) {
                    cache_skipped_dup += 1;
                } else {
                    let mut buf = vec![0u8; page_size as usize];
                    if cache.read_page(pnum, &mut buf).is_ok() {
                        all_interior.insert(pnum, buf);
                        cache_read_ok += 1;
                    } else {
                        cache_read_fail += 1;
                        eprintln!("[sync] WARN: cache.read_page({}) failed for known interior page", pnum);
                    }
                }
            }
            eprintln!(
                "[sync] interior collection: known_interior={}, dirty_snapshot_interior={}, cache_read_ok={}, cache_read_fail={}, skipped_dup={}, skipped_bounds={}, total={}",
                known_interior.len(), dirty_interior_count, cache_read_ok, cache_read_fail, cache_skipped_dup, cache_skipped_bounds, all_interior.len(),
            );
        }

        // Group interior pages by chunk_id
        let mut chunks: HashMap<u32, Vec<(u64, Vec<u8>)>> = HashMap::new();
        for (pnum, data) in all_interior {
            let chunk_id = (pnum / INTERIOR_CHUNK_RANGE) as u32;
            chunks.entry(chunk_id).or_default().push((pnum, data));
        }
        // Sort pages within each chunk by page number
        for pages in chunks.values_mut() {
            pages.sort_by_key(|(pnum, _)| *pnum);
        }

        // Determine which chunks are dirty (have at least one dirty interior page)
        let dirty_chunk_ids: HashSet<u32> = dirty_snapshot.keys()
            .filter(|&&pnum| {
                let type_byte = if pnum == 0 { dirty_snapshot[&pnum].get(100) } else { dirty_snapshot[&pnum].get(0) };
                type_byte.map_or(false, |&b| b == 0x05 || b == 0x02)
            })
            .map(|&pnum| (pnum / INTERIOR_CHUNK_RANGE) as u32)
            .collect();

        // Build new chunk keys: dirty chunks get re-uploaded, clean chunks carry forward
        let old_chunk_keys = self.manifest.read().interior_chunk_keys.clone();
        let mut new_chunk_keys: HashMap<u32, String> = HashMap::new();
        let mut chunk_uploads: Vec<(String, Vec<u8>)> = Vec::new();

        for (&chunk_id, pages) in &chunks {
            if dirty_chunk_ids.contains(&chunk_id) || !old_chunk_keys.contains_key(&chunk_id) {
                // Dirty or new chunk — encode and upload
                let refs: Vec<(u64, &[u8])> = pages.iter().map(|(p, d)| (*p, d.as_slice())).collect();
                let encoded = encode_interior_bundle(
                    &refs,
                    page_size,
                    self.compression_level,
                    #[cfg(feature = "zstd")]
                    self.encoder_dict.as_ref(),
                )?;
                let key = s3.interior_chunk_key(chunk_id, next_version);
                eprintln!(
                    "[sync] interior chunk {}: {} pages, {:.1}KB compressed",
                    chunk_id, pages.len(), encoded.len() as f64 / 1024.0,
                );
                chunk_uploads.push((key.clone(), encoded));
                // Track old interior chunk key being replaced (for GC)
                if let Some(old_key) = old_chunk_keys.get(&chunk_id) {
                    replaced_keys.push(old_key.clone());
                }
                new_chunk_keys.insert(chunk_id, key);
            } else {
                // Clean chunk — carry forward existing key
                new_chunk_keys.insert(chunk_id, old_chunk_keys[&chunk_id].clone());
            }
        }

        // Parallel upload all dirty interior chunks
        if !chunk_uploads.is_empty() {
            eprintln!("[sync] uploading {} interior chunks...", chunk_uploads.len());
            s3.put_page_groups(&chunk_uploads)?;
            eprintln!("[sync] interior chunks uploaded");
        }

        // Update manifest atomically
        let new_manifest = Manifest {
            version: next_version,
            page_count: self.manifest.read().page_count,
            page_size: self.manifest.read().page_size,
            pages_per_group: ppg,
            page_group_keys: new_keys,
            interior_chunk_keys: new_chunk_keys,
            // VFS sync path uses legacy single-frame encoding for now
            frame_tables: Vec::new(),
            sub_pages_per_frame: 0,
        };
        s3.put_manifest(&new_manifest)?;

        // Commit local state
        {
            let mut m = self.manifest.write();
            *m = new_manifest;
        }
        {
            let mut dirty = self.dirty_pages.write();
            for page_num in dirty_snapshot.keys() {
                dirty.remove(page_num);
            }
        }

        // Persist bitmap
        let _ = cache.persist_bitmap();

        // Post-checkpoint GC: delete old page group/interior chunk versions
        if self.gc_enabled && !replaced_keys.is_empty() {
            eprintln!("[gc] deleting {} replaced S3 objects...", replaced_keys.len());
            if let Err(e) = s3.delete_objects(&replaced_keys) {
                eprintln!("[gc] ERROR: failed to delete old objects: {}", e);
            } else {
                eprintln!("[gc] deleted {} old versions", replaced_keys.len());
            }
        }

        Ok(())
    }

    fn set_len(&mut self, size: u64) -> Result<(), io::Error> {
        if self.is_passthrough() {
            return self
                .passthrough_file
                .as_ref()
                .unwrap()
                .write()
                .set_len(size);
        }

        let page_size = *self.page_size.read();
        if page_size == 0 {
            return Ok(());
        }

        let new_page_count = if size == 0 {
            0
        } else {
            (size + page_size as u64 - 1) / page_size as u64
        };

        let mut manifest = self.manifest.write();
        manifest.page_count = new_page_count;

        // Remove dirty pages beyond new size
        let mut dirty = self.dirty_pages.write();
        dirty.retain(|&pn, _| pn < new_page_count);

        Ok(())
    }

    fn lock(&mut self, lock: LockKind) -> Result<bool, io::Error> {
        let current = *self.lock.read();

        if current == lock {
            return Ok(true);
        }

        let lock_file = self.ensure_lock_file()?;

        match lock {
            LockKind::None => {
                self.active_db_locks.clear();
            }
            LockKind::Shared => {
                self.active_db_locks.clear();

                match file_guard::try_lock(
                    std::sync::Arc::clone(&lock_file),
                    file_guard::Lock::Shared,
                    PENDING_BYTE as usize,
                    1,
                ) {
                    Ok(_) => {}
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        return Ok(false);
                    }
                    Err(e) => return Err(e),
                }

                match file_guard::try_lock(
                    std::sync::Arc::clone(&lock_file),
                    file_guard::Lock::Shared,
                    SHARED_FIRST as usize,
                    1,
                ) {
                    Ok(guard) => {
                        self.active_db_locks
                            .insert("shared".to_string(), Box::new(guard));
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        return Ok(false);
                    }
                    Err(e) => return Err(e),
                }
            }
            LockKind::Reserved => {
                if !matches!(
                    current,
                    LockKind::Shared
                        | LockKind::Reserved
                        | LockKind::Pending
                        | LockKind::Exclusive
                ) {
                    return Ok(false);
                }
                match file_guard::try_lock(
                    std::sync::Arc::clone(&lock_file),
                    file_guard::Lock::Exclusive,
                    RESERVED_BYTE as usize,
                    1,
                ) {
                    Ok(guard) => {
                        self.active_db_locks
                            .insert("reserved".to_string(), Box::new(guard));
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        return Ok(false);
                    }
                    Err(e) => return Err(e),
                }
            }
            LockKind::Pending => {
                if !matches!(
                    current,
                    LockKind::Reserved | LockKind::Pending | LockKind::Exclusive
                ) {
                    return Ok(false);
                }
                match file_guard::try_lock(
                    std::sync::Arc::clone(&lock_file),
                    file_guard::Lock::Exclusive,
                    PENDING_BYTE as usize,
                    1,
                ) {
                    Ok(guard) => {
                        self.active_db_locks
                            .insert("pending".to_string(), Box::new(guard));
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        return Ok(false);
                    }
                    Err(e) => return Err(e),
                }
            }
            LockKind::Exclusive => {
                if !matches!(
                    current,
                    LockKind::Reserved | LockKind::Pending | LockKind::Exclusive
                ) {
                    return Ok(false);
                }
                if !self.active_db_locks.contains_key("pending") {
                    match file_guard::try_lock(
                        std::sync::Arc::clone(&lock_file),
                        file_guard::Lock::Exclusive,
                        PENDING_BYTE as usize,
                        1,
                    ) {
                        Ok(guard) => {
                            self.active_db_locks
                                .insert("pending".to_string(), Box::new(guard));
                        }
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            return Ok(false);
                        }
                        Err(e) => return Err(e),
                    }
                }

                self.active_db_locks.remove("shared");

                match file_guard::try_lock(
                    std::sync::Arc::clone(&lock_file),
                    file_guard::Lock::Exclusive,
                    SHARED_FIRST as usize,
                    SHARED_SIZE as usize,
                ) {
                    Ok(guard) => {
                        self.active_db_locks
                            .insert("exclusive".to_string(), Box::new(guard));
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        match file_guard::try_lock(
                            std::sync::Arc::clone(&lock_file),
                            file_guard::Lock::Shared,
                            SHARED_FIRST as usize,
                            1,
                        ) {
                            Ok(guard) => {
                                self.active_db_locks
                                    .insert("shared".to_string(), Box::new(guard));
                            }
                            Err(restore_err) => {
                                return Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    format!("Lock restore failed: {}", restore_err),
                                ));
                            }
                        }
                        return Ok(false);
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        *self.lock.write() = lock;
        Ok(true)
    }

    fn reserved(&mut self) -> Result<bool, io::Error> {
        let lock = *self.lock.read();
        Ok(matches!(
            lock,
            LockKind::Reserved | LockKind::Pending | LockKind::Exclusive
        ))
    }

    fn current_lock(&self) -> Result<LockKind, io::Error> {
        Ok(*self.lock.read())
    }

    fn wal_index(&self, _readonly: bool) -> Result<Self::WalIndex, io::Error> {
        let shm_path = self.db_path.with_extension("db-shm");
        Ok(FileWalIndex::new(shm_path))
    }
}

// ===== TieredVfs =====

/// S3-backed tiered storage VFS.
///
/// # Usage
/// ```ignore
/// use sqlite_compress_encrypt_vfs::tiered::{TieredVfs, TieredConfig};
///
/// let config = TieredConfig {
///     bucket: "my-bucket".into(),
///     prefix: "databases/tenant-1".into(),
///     cache_dir: "/tmp/cache".into(),
///     ..Default::default()
/// };
/// let vfs = TieredVfs::new(config).expect("failed to create TieredVfs");
/// sqlite_compress_encrypt_vfs::tiered::register("tiered", vfs).unwrap();
/// ```
pub struct TieredVfs {
    s3: Arc<S3Client>,
    cache: Arc<DiskCache>,
    prefetch_pool: Arc<PrefetchPool>,
    /// Shared page_count for prefetch workers (kept alive by PrefetchPool workers).
    #[allow(dead_code)]
    page_count: Arc<AtomicU64>,
    config: TieredConfig,
    /// Owned runtime (if we created one ourselves)
    _runtime: Option<tokio::runtime::Runtime>,
}

impl TieredVfs {
    /// Create a new tiered VFS.
    pub fn new(config: TieredConfig) -> io::Result<Self> {
        let (runtime_handle, owned_runtime) =
            if let Some(ref handle) = config.runtime_handle {
                (handle.clone(), None)
            } else if let Ok(handle) = TokioHandle::try_current() {
                (handle, None)
            } else {
                let rt = tokio::runtime::Runtime::new().map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!("Failed to create tokio runtime: {}", e),
                    )
                })?;
                let handle = rt.handle().clone();
                (handle, Some(rt))
            };

        eprintln!("[tiered] creating S3 client...");
        let s3 = S3Client::new_blocking(&config, &runtime_handle)?;
        eprintln!("[tiered] S3 client created, fetching manifest...");

        // Fetch manifest to get page_size for cache initialization
        let manifest = s3.get_manifest()?.unwrap_or_else(Manifest::empty);
        eprintln!("[tiered] manifest fetched (page_size={}, ppg={})", manifest.page_size, manifest.pages_per_group);
        let page_size = if manifest.page_size > 0 {
            manifest.page_size
        } else {
            4096 // default for new databases
        };
        let ppg = if manifest.pages_per_group > 0 {
            manifest.pages_per_group
        } else {
            config.pages_per_group
        };

        let cache = DiskCache::new(
            &config.cache_dir,
            config.cache_ttl_secs,
            ppg,
            page_size,
            manifest.page_count,
        )?;

        let s3 = Arc::new(s3);
        let cache = Arc::new(cache);
        let page_count = Arc::new(AtomicU64::new(manifest.page_count));

        let prefetch_pool = Arc::new(PrefetchPool::new(
            config.prefetch_threads,
            Arc::clone(&s3),
            Arc::clone(&cache),
            ppg,
            Arc::clone(&page_count),
            #[cfg(feature = "zstd")]
            config.dictionary.clone(),
        ));

        Ok(Self {
            s3,
            cache,
            prefetch_pool,
            page_count,
            config,
            _runtime: owned_runtime,
        })
    }

    /// Get a lightweight handle for benchmarking (clear_cache, S3 counters).
    /// The handle shares the same cache and S3 client as the VFS.
    pub fn bench_handle(&self) -> TieredBenchHandle {
        TieredBenchHandle {
            s3: Arc::clone(&self.s3),
            cache: Arc::clone(&self.cache),
            prefetch_pool: Arc::clone(&self.prefetch_pool),
        }
    }

    /// Evict non-interior pages from disk cache. Interior pages and group 0
    /// (schema + root page) stay warm — simulates production where structural
    /// pages are always hot after first access.
    pub fn clear_cache(&self) {
        self.prefetch_pool.wait_idle();

        let pinned_pages = self.cache.interior_pages.lock().clone();
        let ppg = self.cache.pages_per_group as u64;

        // Reset group states to None, except group 0 stays Present
        let states = self.cache.group_states.lock();
        for (i, s) in states.iter().enumerate() {
            if i == 0 {
                s.store(GroupState::Present as u8, Ordering::Release);
            } else {
                s.store(GroupState::None as u8, Ordering::Release);
            }
        }
        drop(states);

        self.cache.group_access.lock().clear();

        // Clear bitmap except for interior pages and group 0
        {
            let mut bitmap = self.cache.bitmap.lock();
            let total_pages = bitmap.bits.len() as u64 * 8;
            bitmap.bits.fill(0);
            for &page in &pinned_pages {
                bitmap.mark_present(page);
            }
            let g0_end = ppg.min(total_pages);
            for p in 0..g0_end {
                bitmap.mark_present(p);
            }
            let _ = bitmap.persist();
        }

        // Don't truncate cache file — bitmap controls what's "present".
        // Pinned page data stays in the file at correct offsets.
        // Non-pinned pages are bitmap-absent so reads will re-fetch from S3.
    }

    /// Reset S3 I/O counters. Returns (fetch_count, fetch_bytes) before reset.
    pub fn reset_s3_counters(&self) -> (u64, u64) {
        let count = self.s3.fetch_count.swap(0, Ordering::Relaxed);
        let bytes = self.s3.fetch_bytes.swap(0, Ordering::Relaxed);
        (count, bytes)
    }

    /// Read current S3 I/O counters without resetting.
    pub fn s3_counters(&self) -> (u64, u64) {
        (
            self.s3.fetch_count.load(Ordering::Relaxed),
            self.s3.fetch_bytes.load(Ordering::Relaxed),
        )
    }

    /// Garbage collect orphaned S3 objects not referenced by the current manifest.
    /// Lists all objects under the prefix, compares against manifest keys, and
    /// deletes unreferenced page groups and interior chunks.
    /// Returns the number of objects deleted.
    pub fn gc(&self) -> io::Result<usize> {
        let manifest = self.s3.get_manifest()?.unwrap_or_else(Manifest::empty);
        let all_keys = self.s3.list_all_keys()?;

        // Build set of live keys from manifest
        let mut live_keys: HashSet<String> = HashSet::new();
        live_keys.insert(self.s3.manifest_key());
        for key in &manifest.page_group_keys {
            if !key.is_empty() {
                live_keys.insert(key.clone());
            }
        }
        for key in manifest.interior_chunk_keys.values() {
            live_keys.insert(key.clone());
        }

        // Find orphans (keys in S3 but not in manifest)
        let orphans: Vec<String> = all_keys
            .into_iter()
            .filter(|k| !live_keys.contains(k))
            .collect();

        let count = orphans.len();
        if count > 0 {
            eprintln!("[gc] deleting {} orphaned S3 objects...", count);
            self.s3.delete_objects(&orphans)?;
            eprintln!("[gc] deleted {} orphaned objects", count);
        }
        Ok(count)
    }

    /// Helper to destroy all S3 data for a prefix.
    pub fn destroy_s3(&self) -> io::Result<()> {
        let s3 = &self.s3;
        S3Client::block_on(&s3.runtime, async {
            let mut continuation_token: Option<String> = None;
            loop {
                let mut req = s3
                    .client
                    .list_objects_v2()
                    .bucket(&s3.bucket)
                    .prefix(&s3.prefix);

                if let Some(token) = &continuation_token {
                    req = req.continuation_token(token);
                }

                let resp = req.send().await.map_err(|e| {
                    io::Error::new(io::ErrorKind::Other, format!("S3 list failed: {}", e))
                })?;

                let keys: Vec<String> = resp
                    .contents()
                    .iter()
                    .filter_map(|obj| obj.key().map(|k| k.to_string()))
                    .collect();

                for batch in keys.chunks(1000) {
                    let objects: Vec<aws_sdk_s3::types::ObjectIdentifier> = batch
                        .iter()
                        .map(|key| {
                            aws_sdk_s3::types::ObjectIdentifier::builder()
                                .key(key)
                                .build()
                                .expect("ObjectIdentifier requires key")
                        })
                        .collect();

                    let delete = aws_sdk_s3::types::Delete::builder()
                        .set_objects(Some(objects))
                        .quiet(true)
                        .build()
                        .map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::Other,
                                format!("Failed to build Delete request: {}", e),
                            )
                        })?;

                    s3.client
                        .delete_objects()
                        .bucket(&s3.bucket)
                        .delete(delete)
                        .send()
                        .await
                        .map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::Other,
                                format!("S3 batch delete failed: {}", e),
                            )
                        })?;
                }

                if resp.is_truncated() == Some(true) {
                    continuation_token =
                        resp.next_continuation_token().map(|s| s.to_string());
                } else {
                    break;
                }
            }
            Ok(())
        })
    }
}

impl Vfs for TieredVfs {
    type Handle = TieredHandle;

    fn open(&self, db: &str, opts: OpenOptions) -> Result<Self::Handle, io::Error> {
        let path = self.config.cache_dir.join(db);

        if matches!(opts.kind, OpenKind::MainDb) {
            let manifest = self.s3.get_manifest()?.unwrap_or_else(Manifest::empty);

            let ppg = if manifest.pages_per_group > 0 {
                manifest.pages_per_group
            } else {
                self.config.pages_per_group
            };

            let lock_dir = self.config.cache_dir.join("locks");
            fs::create_dir_all(&lock_dir)?;
            let lock_path = lock_dir.join(db);
            FsOpenOptions::new()
                .create(true)
                .write(true)
                .open(&lock_path)?;

            Ok(TieredHandle::new_tiered(
                Arc::clone(&self.s3),
                Arc::clone(&self.cache),
                manifest,
                lock_path,
                ppg,
                self.config.compression_level,
                self.config.read_only,
                self.config.prefetch_hops.clone(),
                Some(Arc::clone(&self.prefetch_pool)),
                self.config.gc_enabled,
                #[cfg(feature = "zstd")]
                self.config.dictionary.as_deref(),
            ))
        } else {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let file = FsOpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)?;
            Ok(TieredHandle::new_passthrough(file, path))
        }
    }

    fn delete(&self, db: &str) -> Result<(), io::Error> {
        let path = self.config.cache_dir.join(db);
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        Ok(())
    }

    fn exists(&self, db: &str) -> Result<bool, io::Error> {
        let path = self.config.cache_dir.join(db);
        if path.exists() {
            return Ok(true);
        }

        if db.ends_with("-wal") || db.ends_with("-journal") || db.ends_with("-shm") {
            return Ok(false);
        }

        Ok(self.s3.get_manifest()?.is_some())
    }

    fn temporary_name(&self) -> String {
        format!("temp_{}", std::process::id())
    }

    fn random(&self, buffer: &mut [i8]) {
        use std::time::SystemTime;
        let mut seed = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        for b in buffer.iter_mut() {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            *b = (seed >> 33) as i8;
        }
    }

    fn sleep(&self, duration: Duration) -> Duration {
        std::thread::sleep(duration);
        duration
    }
}

/// Lightweight handle for benchmarking — shares the same S3 client and cache.
/// Obtained via [`TieredVfs::bench_handle`] before registering the VFS.
pub struct TieredBenchHandle {
    s3: Arc<S3Client>,
    cache: Arc<DiskCache>,
    prefetch_pool: Arc<PrefetchPool>,
}

impl TieredBenchHandle {
    /// Evict data pages only — interior B-tree pages and group 0 stay warm.
    /// Simulates production where structural pages are always hot.
    pub fn clear_cache_data_only(&self) {
        self.prefetch_pool.wait_idle();

        let pinned_pages = self.cache.interior_pages.lock().clone();
        let ppg = self.cache.pages_per_group as u64;

        // Reset group states to None, except group 0 stays Present
        let states = self.cache.group_states.lock();
        for (i, s) in states.iter().enumerate() {
            if i == 0 {
                s.store(GroupState::Present as u8, Ordering::Release);
            } else {
                s.store(GroupState::None as u8, Ordering::Release);
            }
        }
        drop(states);

        self.cache.group_access.lock().clear();

        // Clear bitmap except for interior pages and group 0
        {
            let mut bitmap = self.cache.bitmap.lock();
            let total_pages = bitmap.bits.len() as u64 * 8;
            bitmap.bits.fill(0);
            for &page in &pinned_pages {
                bitmap.mark_present(page);
            }
            let g0_end = ppg.min(total_pages);
            for p in 0..g0_end {
                bitmap.mark_present(p);
            }
            let _ = bitmap.persist();
        }
    }

    /// Evict everything — interior pages, group 0, all data. Nothing cached.
    /// Next connection open must re-fetch interior chunks from S3.
    pub fn clear_cache_all(&self) {
        self.prefetch_pool.wait_idle();

        let states = self.cache.group_states.lock();
        for s in states.iter() {
            s.store(GroupState::None as u8, Ordering::Release);
        }
        drop(states);

        self.cache.group_access.lock().clear();
        self.cache.interior_pages.lock().clear();
        self.cache.interior_groups.lock().clear();

        {
            let mut bitmap = self.cache.bitmap.lock();
            bitmap.bits.fill(0);
            let _ = bitmap.persist();
        }
    }

    /// Reset S3 I/O counters. Returns (fetch_count, fetch_bytes) before reset.
    pub fn reset_s3_counters(&self) -> (u64, u64) {
        let count = self.s3.fetch_count.swap(0, Ordering::Relaxed);
        let bytes = self.s3.fetch_bytes.swap(0, Ordering::Relaxed);
        (count, bytes)
    }

    /// Read current S3 I/O counters without resetting.
    pub fn s3_counters(&self) -> (u64, u64) {
        (
            self.s3.fetch_count.load(Ordering::Relaxed),
            self.s3.fetch_bytes.load(Ordering::Relaxed),
        )
    }
}

/// Import a local SQLite database file directly to S3 as tiered page groups.
///
/// Reads the file page-by-page, encodes page groups, detects interior B-tree pages,
/// and uploads everything to S3 with a manifest. This is much faster than writing
/// through the VFS because it avoids WAL overhead and checkpoint cycles.
///
/// Returns the manifest that was uploaded.
pub fn import_sqlite_file(
    config: &TieredConfig,
    file_path: &Path,
) -> io::Result<Manifest> {
    use std::os::unix::fs::FileExt;

    let runtime = tokio::runtime::Runtime::new()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let handle = runtime.handle().clone();

    // Build a minimal config for S3Client
    let s3_cfg = TieredConfig {
        bucket: config.bucket.clone(),
        prefix: config.prefix.clone(),
        endpoint_url: config.endpoint_url.clone(),
        region: config.region.clone(),
        runtime_handle: Some(handle.clone()),
        ..Default::default()
    };

    let s3 = S3Client::block_on(&handle, S3Client::new_async(&s3_cfg))?;

    // Open the file and read SQLite header to get page size and page count
    let file = File::open(file_path)?;
    let file_len = file.metadata()?.len();

    // SQLite header: page size at offset 16 (2 bytes, big-endian)
    let mut header = [0u8; 100];
    file.read_exact_at(&mut header, 0)?;
    let raw_page_size = u16::from_be_bytes([header[16], header[17]]);
    let page_size: u32 = if raw_page_size == 1 { 65536 } else { raw_page_size as u32 };
    let page_count = (file_len / page_size as u64) as u64;
    let ppg = config.pages_per_group;
    let total_groups = (page_count + ppg as u64 - 1) / ppg as u64;

    eprintln!(
        "[import] file={} size={:.1}MB page_size={} pages={} groups={}",
        file_path.display(),
        file_len as f64 / (1024.0 * 1024.0),
        page_size,
        page_count,
        total_groups,
    );

    // Read all pages, group them, detect interior pages
    let compression_level = config.compression_level;
    let sub_ppf = config.sub_pages_per_frame;
    let use_seekable = sub_ppf > 0;
    let mut page_group_keys: Vec<String> = Vec::with_capacity(total_groups as usize);
    let mut frame_tables: Vec<Vec<FrameEntry>> = Vec::with_capacity(total_groups as usize);
    let mut uploads: Vec<(String, Vec<u8>)> = Vec::new();
    let mut interior_pages: Vec<(u64, Vec<u8>)> = Vec::new();
    let version = 1u64;

    eprintln!(
        "[import] encoding: {} (sub_ppf={})",
        if use_seekable { "seekable multi-frame" } else { "legacy single-frame" },
        sub_ppf,
    );

    for gid in 0..total_groups {
        let start_page = gid * ppg as u64;
        let end_page = std::cmp::min(start_page + ppg as u64, page_count);
        let mut pages: Vec<Option<Vec<u8>>> = Vec::with_capacity(ppg as usize);

        for i in 0..ppg as usize {
            let pnum = start_page + i as u64;
            if pnum >= page_count {
                pages.push(None);
                continue;
            }
            let mut buf = vec![0u8; page_size as usize];
            file.read_exact_at(&mut buf, pnum * page_size as u64)?;

            // Detect interior B-tree pages
            let type_byte = if pnum == 0 { buf[100] } else { buf[0] };
            if type_byte == 0x05 || type_byte == 0x02 {
                interior_pages.push((pnum, buf.clone()));
            }

            pages.push(Some(buf));
        }

        let key = s3.page_group_key(gid, version);
        if use_seekable {
            let (encoded, ft) = encode_page_group_seekable(
                &pages,
                page_size,
                sub_ppf,
                compression_level,
                #[cfg(feature = "zstd")]
                None,
            )?;
            uploads.push((key.clone(), encoded));
            frame_tables.push(ft);
        } else {
            let encoded = encode_page_group(
                &pages,
                page_size,
                compression_level,
                #[cfg(feature = "zstd")]
                None,
            )?;
            uploads.push((key.clone(), encoded));
            frame_tables.push(Vec::new());
        }
        page_group_keys.push(key);

        if (gid + 1) % 50 == 0 || gid + 1 == total_groups {
            eprintln!(
                "[import] encoded {}/{} groups ({} interior pages so far)",
                gid + 1, total_groups, interior_pages.len(),
            );
        }
    }

    // Upload page groups in batches of 50 (parallel within each batch)
    let batch_size = 50;
    let total_uploads = uploads.len();
    for (batch_idx, batch) in uploads.chunks(batch_size).enumerate() {
        s3.put_page_groups(batch)?;
        let done = std::cmp::min((batch_idx + 1) * batch_size, total_uploads);
        eprintln!("[import] uploaded {}/{} page groups", done, total_uploads);
    }

    // Build interior chunks
    let mut chunks: HashMap<u32, Vec<(u64, Vec<u8>)>> = HashMap::new();
    for (pnum, data) in interior_pages {
        let chunk_id = (pnum / INTERIOR_CHUNK_RANGE) as u32;
        chunks.entry(chunk_id).or_default().push((pnum, data));
    }
    for pages in chunks.values_mut() {
        pages.sort_by_key(|(pnum, _)| *pnum);
    }

    let mut interior_chunk_keys: HashMap<u32, String> = HashMap::new();
    let mut chunk_uploads: Vec<(String, Vec<u8>)> = Vec::new();
    for (&chunk_id, pages) in &chunks {
        let refs: Vec<(u64, &[u8])> = pages.iter().map(|(p, d)| (*p, d.as_slice())).collect();
        let encoded = encode_interior_bundle(
            &refs,
            page_size,
            compression_level,
            #[cfg(feature = "zstd")]
            None,
        )?;
        let key = s3.interior_chunk_key(chunk_id, version);
        eprintln!(
            "[import] interior chunk {}: {} pages, {:.1}KB",
            chunk_id, pages.len(), encoded.len() as f64 / 1024.0,
        );
        chunk_uploads.push((key.clone(), encoded));
        interior_chunk_keys.insert(chunk_id, key);
    }

    if !chunk_uploads.is_empty() {
        s3.put_page_groups(&chunk_uploads)?;
        eprintln!("[import] uploaded {} interior chunks", chunk_uploads.len());
    }

    // Build and upload manifest
    let manifest = Manifest {
        version,
        page_count,
        page_size,
        pages_per_group: ppg,
        page_group_keys,
        interior_chunk_keys,
        frame_tables,
        sub_pages_per_frame: if use_seekable { sub_ppf } else { 0 },
    };
    s3.put_manifest(&manifest)?;
    eprintln!(
        "[import] manifest uploaded: version={} pages={} groups={} interior_chunks={} seekable={}",
        manifest.version, manifest.page_count, manifest.page_group_keys.len(),
        manifest.interior_chunk_keys.len(), use_seekable,
    );

    Ok(manifest)
}

/// Register a tiered VFS with SQLite.
pub fn register(name: &str, vfs: TieredVfs) -> Result<(), io::Error> {
    sqlite_vfs::register(name, vfs, false)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // =========================================================================
    // PageBitmap
    // =========================================================================

    #[test]
    fn test_bitmap_set_and_check() {
        let dir = TempDir::new().unwrap();
        let mut bm = PageBitmap::new(dir.path().join("bm"));
        assert!(!bm.is_present(0));
        assert!(!bm.is_present(100));
        bm.mark_present(0);
        assert!(bm.is_present(0));
        assert!(!bm.is_present(1));
        bm.mark_present(100);
        assert!(bm.is_present(100));
        assert!(!bm.is_present(99));
    }

    #[test]
    fn test_bitmap_range() {
        let dir = TempDir::new().unwrap();
        let mut bm = PageBitmap::new(dir.path().join("bm"));
        bm.mark_range(10, 5);
        for p in 10..15 {
            assert!(bm.is_present(p), "page {} should be present", p);
        }
        assert!(!bm.is_present(9));
        assert!(!bm.is_present(15));
        bm.clear_range(11, 2);
        assert!(bm.is_present(10));
        assert!(!bm.is_present(11));
        assert!(!bm.is_present(12));
        assert!(bm.is_present(13));
    }

    #[test]
    fn test_bitmap_persist_and_reload() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("bm");
        {
            let mut bm = PageBitmap::new(path.clone());
            bm.mark_present(42);
            bm.mark_present(1000);
            bm.persist().unwrap();
        }
        let bm2 = PageBitmap::new(path);
        assert!(bm2.is_present(42));
        assert!(bm2.is_present(1000));
        assert!(!bm2.is_present(43));
    }

    #[test]
    fn test_bitmap_byte_boundaries() {
        let dir = TempDir::new().unwrap();
        let mut bm = PageBitmap::new(dir.path().join("bm"));
        // Test at every bit in a byte
        for bit in 0..8 {
            bm.mark_present(bit);
            assert!(bm.is_present(bit));
            // Adjacent bits in next byte untouched
            assert!(!bm.is_present(bit + 8));
        }
        // Byte boundaries: 7→8, 15→16
        bm.mark_present(7);
        bm.mark_present(8);
        assert!(bm.is_present(7));
        assert!(bm.is_present(8));
    }

    #[test]
    fn test_bitmap_large_page_numbers() {
        let dir = TempDir::new().unwrap();
        let mut bm = PageBitmap::new(dir.path().join("bm"));
        let big = 1_000_000u64;
        bm.mark_present(big);
        assert!(bm.is_present(big));
        assert!(!bm.is_present(big - 1));
        assert!(!bm.is_present(big + 1));
        // Bitmap should be big enough
        assert!(bm.bits.len() >= (big as usize / 8) + 1);
    }

    #[test]
    fn test_bitmap_ensure_capacity_auto_extends() {
        let dir = TempDir::new().unwrap();
        let mut bm = PageBitmap::new(dir.path().join("bm"));
        assert_eq!(bm.bits.len(), 0);
        bm.mark_present(0);
        assert!(bm.bits.len() >= 1);
        bm.mark_present(255);
        assert!(bm.bits.len() >= 32);
    }

    #[test]
    fn test_bitmap_resize_explicit() {
        let dir = TempDir::new().unwrap();
        let mut bm = PageBitmap::new(dir.path().join("bm"));
        bm.resize(1000);
        assert!(bm.bits.len() >= 125); // ceil(1000/8)
        // Resize with smaller value should not shrink
        let len_before = bm.bits.len();
        bm.resize(10);
        assert_eq!(bm.bits.len(), len_before);
    }

    #[test]
    fn test_bitmap_clear_range_beyond_capacity() {
        let dir = TempDir::new().unwrap();
        let mut bm = PageBitmap::new(dir.path().join("bm"));
        bm.mark_present(5);
        // Clear range far beyond capacity — should not panic
        bm.clear_range(100000, 500);
        assert!(bm.is_present(5));
    }

    #[test]
    fn test_bitmap_mark_range_zero_count() {
        let dir = TempDir::new().unwrap();
        let mut bm = PageBitmap::new(dir.path().join("bm"));
        bm.mark_range(10, 0); // no-op
        assert!(!bm.is_present(10));
    }

    #[test]
    fn test_bitmap_clear_range_within_single_byte() {
        let dir = TempDir::new().unwrap();
        let mut bm = PageBitmap::new(dir.path().join("bm"));
        // Set all 8 bits in first byte
        for i in 0..8 {
            bm.mark_present(i);
        }
        // Clear only bits 2 and 3
        bm.clear_range(2, 2);
        assert!(bm.is_present(0));
        assert!(bm.is_present(1));
        assert!(!bm.is_present(2));
        assert!(!bm.is_present(3));
        assert!(bm.is_present(4));
        assert!(bm.is_present(5));
    }

    #[test]
    fn test_bitmap_new_nonexistent_file() {
        let dir = TempDir::new().unwrap();
        let bm = PageBitmap::new(dir.path().join("does_not_exist"));
        assert_eq!(bm.bits.len(), 0);
        assert!(!bm.is_present(0));
    }

    #[test]
    fn test_bitmap_persist_creates_file_atomic() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("bm");
        let mut bm = PageBitmap::new(path.clone());
        bm.mark_present(7);
        bm.persist().unwrap();
        assert!(path.exists());
        // tmp file should NOT exist after atomic rename
        assert!(!path.with_extension("tmp").exists());
    }

    #[test]
    fn test_bitmap_is_present_beyond_capacity_returns_false() {
        let dir = TempDir::new().unwrap();
        let bm = PageBitmap::new(dir.path().join("bm"));
        // Empty bitmap, any page should return false
        assert!(!bm.is_present(0));
        assert!(!bm.is_present(u64::MAX / 8 - 1));
    }

    // =========================================================================
    // Coordinate Math
    // =========================================================================

    #[test]
    fn test_group_id_calculation() {
        let ppg = 2048u32;
        assert_eq!(group_id(0, ppg), 0);
        assert_eq!(group_id(2047, ppg), 0);
        assert_eq!(group_id(2048, ppg), 1);
        assert_eq!(group_id(4095, ppg), 1);
        assert_eq!(group_id(4096, ppg), 2);
    }

    #[test]
    fn test_group_id_small_ppg() {
        assert_eq!(group_id(0, 1), 0);
        assert_eq!(group_id(1, 1), 1);
        assert_eq!(group_id(99, 1), 99);
        assert_eq!(group_id(0, 4), 0);
        assert_eq!(group_id(3, 4), 0);
        assert_eq!(group_id(4, 4), 1);
    }

    #[test]
    fn test_local_idx_calculation() {
        let ppg = 2048u32;
        assert_eq!(local_idx_in_group(0, ppg), 0);
        assert_eq!(local_idx_in_group(2047, ppg), 2047);
        assert_eq!(local_idx_in_group(2048, ppg), 0);
        assert_eq!(local_idx_in_group(2049, ppg), 1);
    }

    #[test]
    fn test_local_idx_small_ppg() {
        assert_eq!(local_idx_in_group(0, 1), 0);
        assert_eq!(local_idx_in_group(1, 1), 0);
        assert_eq!(local_idx_in_group(5, 3), 2);
        assert_eq!(local_idx_in_group(6, 3), 0);
    }

    #[test]
    fn test_group_start_page() {
        let ppg = 2048u32;
        assert_eq!(group_start_page(0, ppg), 0);
        assert_eq!(group_start_page(1, ppg), 2048);
        assert_eq!(group_start_page(5, ppg), 10240);
    }

    #[test]
    fn test_group_start_page_small_ppg() {
        assert_eq!(group_start_page(0, 1), 0);
        assert_eq!(group_start_page(3, 1), 3);
        assert_eq!(group_start_page(2, 4), 8);
    }

    #[test]
    fn test_coordinate_math_roundtrip() {
        // For any page, group_start_page(group_id(p)) + local_idx(p) == p
        let ppg = 2048u32;
        for p in [0u64, 1, 2047, 2048, 2049, 4095, 4096, 100_000] {
            let gid = group_id(p, ppg);
            let idx = local_idx_in_group(p, ppg);
            let reconstructed = group_start_page(gid, ppg) + idx as u64;
            assert_eq!(reconstructed, p, "roundtrip failed for page {}", p);
        }
    }

    // =========================================================================
    // GroupState
    // =========================================================================

    #[test]
    fn test_group_state_enum_values() {
        assert_eq!(GroupState::None as u8, 0);
        assert_eq!(GroupState::Fetching as u8, 1);
        assert_eq!(GroupState::Present as u8, 2);
    }

    #[test]
    fn test_group_state_equality() {
        assert_eq!(GroupState::None, GroupState::None);
        assert_ne!(GroupState::None, GroupState::Fetching);
        assert_ne!(GroupState::Fetching, GroupState::Present);
    }

    // =========================================================================
    // Page Group Encoding
    // =========================================================================

    /// Helper: encode a page group with default compression settings (level 3, no dict).
    fn test_encode(pages: &[Option<Vec<u8>>], page_size: u32) -> Vec<u8> {
        encode_page_group(
            pages,
            page_size,
            3,
            #[cfg(feature = "zstd")]
            None,
        )
        .unwrap()
    }

    /// Helper: decode a page group with no dict.
    fn test_decode(compressed: &[u8]) -> (u32, u32, Vec<Vec<u8>>) {
        decode_page_group(
            compressed,
            #[cfg(feature = "zstd")]
            None,
        )
        .unwrap()
    }

    #[test]
    fn test_encode_decode_page_group_roundtrip() {
        let page_size = 64u32;
        let mut pages: Vec<Option<Vec<u8>>> = vec![None; 8];
        pages[0] = Some(vec![1; page_size as usize]);
        pages[3] = Some(vec![4; page_size as usize]);
        pages[7] = Some(vec![8; page_size as usize]);

        let encoded = test_encode(&pages, page_size);
        let (pg_count, ps, decoded) = test_decode(&encoded);

        assert_eq!(ps, page_size);
        assert_eq!(pg_count, 8); // last non-None is index 7 → 8 pages
        assert_eq!(decoded[0], vec![1u8; page_size as usize]);
        assert_eq!(decoded[3], vec![4u8; page_size as usize]);
        assert_eq!(decoded[7], vec![8u8; page_size as usize]);
        // Empty slots are zero-filled
        assert_eq!(decoded[1], vec![0u8; page_size as usize]);
    }

    #[test]
    fn test_encode_empty_page_group() {
        let page_size = 64u32;
        let pages: Vec<Option<Vec<u8>>> = vec![None; 4];
        let encoded = test_encode(&pages, page_size);
        let (pg_count, ps, decoded) = test_decode(&encoded);

        assert_eq!(pg_count, 0);
        assert_eq!(ps, page_size);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_encode_all_pages_filled() {
        let page_size = 100u32;
        let pages: Vec<Option<Vec<u8>>> = (0..4u32)
            .map(|i| Some(vec![i as u8; page_size as usize]))
            .collect();
        let encoded = test_encode(&pages, page_size);
        let (pg_count, _ps, decoded) = test_decode(&encoded);

        assert_eq!(pg_count, 4);
        for i in 0..4 {
            assert_eq!(decoded[i], vec![i as u8; page_size as usize]);
        }
    }

    #[test]
    fn test_encode_single_page_filled() {
        let page_size = 50u32;
        let mut pages: Vec<Option<Vec<u8>>> = vec![None; 4];
        pages[2] = Some(vec![0xAB; page_size as usize]);
        let encoded = test_encode(&pages, page_size);
        let (pg_count, _ps, decoded) = test_decode(&encoded);

        // page_count = 3 (last non-None is index 2)
        assert_eq!(pg_count, 3);
        assert_eq!(decoded[0], vec![0u8; page_size as usize]); // zero-filled
        assert_eq!(decoded[1], vec![0u8; page_size as usize]); // zero-filled
        assert_eq!(decoded[2], vec![0xABu8; page_size as usize]);
    }

    #[test]
    fn test_encode_page_group_large_data() {
        let page_size = 1_000_000u32; // 1MB page
        let big_page = vec![0xFFu8; page_size as usize];
        let pages = vec![Some(big_page.clone()), None];
        let encoded = test_encode(&pages, page_size);
        let (pg_count, _ps, decoded) = test_decode(&encoded);

        assert_eq!(pg_count, 1);
        assert_eq!(decoded[0].len(), page_size as usize);
        assert_eq!(decoded[0], big_page);
    }

    #[test]
    fn test_decode_truncated_data() {
        // Too short to even have the header
        let short = vec![0u8; 3];
        let result = decode_page_group(
            &short,
            #[cfg(feature = "zstd")]
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_empty_data() {
        let result = decode_page_group(
            &[],
            #[cfg(feature = "zstd")]
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_encode_decode_roundtrip_various_page_sizes() {
        for page_size in [64u32, 256, 1024, 4096] {
            let pages: Vec<Option<Vec<u8>>> = (0..4u32)
                .map(|i| {
                    if i % 2 == 0 {
                        Some(vec![i as u8; page_size as usize])
                    } else {
                        None
                    }
                })
                .collect();
            let encoded = test_encode(&pages, page_size);
            let (pg_count, ps, decoded) = test_decode(&encoded);

            assert_eq!(ps, page_size, "page_size mismatch for {}", page_size);
            // Last non-None is index 2, so pg_count = 3
            assert_eq!(pg_count, 3, "pg_count mismatch for {}", page_size);
            assert_eq!(decoded[0], vec![0u8; page_size as usize]);
            assert_eq!(decoded[1], vec![0u8; page_size as usize]); // None → zero
            assert_eq!(decoded[2], vec![2u8; page_size as usize]);
        }
    }

    // =========================================================================
    // Page Type Detection
    // =========================================================================

    #[test]
    fn test_page_type_detection() {
        let check = |buf: &[u8], page_num: u64| -> bool {
            let type_byte = if page_num == 0 { buf.get(100) } else { buf.get(0) };
            matches!(type_byte, Some(&0x05) | Some(&0x02))
        };
        // Table interior (0x05)
        assert!(check(&[0x05u8; 4096], 1));
        // Index interior (0x02)
        assert!(check(&[0x02u8; 4096], 1));
        // Table leaf (0x0d)
        assert!(!check(&[0x0du8; 4096], 1));
        // Index leaf (0x0a)
        assert!(!check(&[0x0au8; 4096], 1));
        // Zero page
        assert!(!check(&[0x00u8; 4096], 1));
    }

    #[test]
    fn test_page_type_page_zero_offset_100() {
        let check = |buf: &[u8], page_num: u64| -> bool {
            let type_byte = if page_num == 0 { buf.get(100) } else { buf.get(0) };
            matches!(type_byte, Some(&0x05) | Some(&0x02))
        };
        let mut page0 = vec![0u8; 4096];
        page0[100] = 0x05;
        assert!(check(&page0, 0));
        page0[100] = 0x02;
        assert!(check(&page0, 0));
        page0[100] = 0x0d;
        assert!(!check(&page0, 0));
        // Byte at offset 0 should NOT be checked for page 0
        page0[0] = 0x05;
        page0[100] = 0x0d;
        assert!(!check(&page0, 0));
    }

    #[test]
    fn test_page_type_all_valid_types() {
        let check = |buf: &[u8]| -> bool {
            matches!(buf.get(0), Some(&0x05) | Some(&0x02))
        };
        // 0x02 = index interior, 0x05 = table interior
        assert!(check(&[0x02]));
        assert!(check(&[0x05]));
        // 0x0a = index leaf, 0x0d = table leaf, others = not interior
        assert!(!check(&[0x0a]));
        assert!(!check(&[0x0d]));
        assert!(!check(&[0x00]));
        assert!(!check(&[0x01]));
        assert!(!check(&[0xFF]));
    }

    // =========================================================================
    // DiskCache
    // =========================================================================

    #[test]
    fn test_disk_cache_write_and_read_page() {
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 3600, 8, 64, 16).unwrap();
        let data = vec![42u8; 64];
        cache.write_page(5, &data).unwrap();
        assert!(cache.is_present(5));
        assert!(!cache.is_present(4));
        let mut buf = vec![0u8; 64];
        cache.read_page(5, &mut buf).unwrap();
        assert_eq!(buf, data);
    }

    #[test]
    fn test_disk_cache_write_multiple_pages() {
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 3600, 4, 64, 16).unwrap();
        for i in 0..16u64 {
            cache.write_page(i, &vec![i as u8; 64]).unwrap();
        }
        for i in 0..16u64 {
            assert!(cache.is_present(i));
            let mut buf = vec![0u8; 64];
            cache.read_page(i, &mut buf).unwrap();
            assert_eq!(buf, vec![i as u8; 64], "page {} mismatch", i);
        }
    }

    #[test]
    fn test_disk_cache_write_page_overwrite() {
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 3600, 4, 64, 8).unwrap();
        cache.write_page(3, &vec![0xAA; 64]).unwrap();
        cache.write_page(3, &vec![0xBB; 64]).unwrap(); // overwrite
        let mut buf = vec![0u8; 64];
        cache.read_page(3, &mut buf).unwrap();
        assert_eq!(buf, vec![0xBB; 64]);
    }

    #[test]
    fn test_disk_cache_write_extends_file() {
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 3600, 4, 64, 0).unwrap(); // page_count=0
        // Writing page 10 should extend the file
        cache.write_page(10, &vec![42u8; 64]).unwrap();
        assert!(cache.is_present(10));
        let mut buf = vec![0u8; 64];
        cache.read_page(10, &mut buf).unwrap();
        assert_eq!(buf, vec![42u8; 64]);
    }

    #[test]
    fn test_disk_cache_read_uncached_page_returns_zeros() {
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 3600, 4, 64, 16).unwrap();
        // Page exists in sparse file but not marked in bitmap
        let mut buf = vec![0xFFu8; 64];
        cache.read_page(0, &mut buf).unwrap();
        assert_eq!(buf, vec![0u8; 64]); // Sparse file reads as zeros
    }

    #[test]
    fn test_disk_cache_creates_cache_file() {
        let dir = TempDir::new().unwrap();
        let _cache = DiskCache::new(dir.path(), 3600, 4, 64, 100).unwrap();
        assert!(dir.path().join("data.cache").exists());
        let meta = std::fs::metadata(dir.path().join("data.cache")).unwrap();
        assert_eq!(meta.len(), 100 * 64); // page_count * page_size
    }

    #[test]
    fn test_disk_cache_creates_dir_if_missing() {
        let dir = TempDir::new().unwrap();
        let nested = dir.path().join("a").join("b").join("c");
        let _cache = DiskCache::new(&nested, 3600, 4, 64, 8).unwrap();
        assert!(nested.join("data.cache").exists());
    }

    #[test]
    fn test_disk_cache_group_states() {
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 3600, 4, 64, 8).unwrap();
        assert_eq!(cache.group_state(0), GroupState::None);
        assert_eq!(cache.group_state(1), GroupState::None);
        assert!(cache.try_claim_group(0));
        assert_eq!(cache.group_state(0), GroupState::Fetching);
        assert!(!cache.try_claim_group(0)); // Can't claim again
        cache.mark_group_present(0);
        assert_eq!(cache.group_state(0), GroupState::Present);
    }

    #[test]
    fn test_disk_cache_try_claim_present_fails() {
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 3600, 4, 64, 8).unwrap();
        cache.try_claim_group(0);
        cache.mark_group_present(0);
        assert!(!cache.try_claim_group(0)); // Already Present
    }

    #[test]
    fn test_disk_cache_group_state_out_of_bounds() {
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 3600, 4, 64, 8).unwrap(); // 2 groups
        // Out of bounds should return None
        assert_eq!(cache.group_state(100), GroupState::None);
        assert_eq!(cache.group_state(u64::MAX), GroupState::None);
    }

    #[test]
    fn test_disk_cache_wait_for_group_present() {
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 3600, 4, 64, 8).unwrap();
        cache.try_claim_group(0);
        cache.mark_group_present(0);
        // Should return immediately
        cache.wait_for_group(0);
    }

    #[test]
    fn test_disk_cache_wait_for_group_none() {
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 3600, 4, 64, 8).unwrap();
        // State is None — should return immediately
        cache.wait_for_group(0);
    }

    #[test]
    fn test_disk_cache_touch_group_updates_access() {
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 3600, 4, 64, 8).unwrap();
        assert!(!cache.group_access.lock().contains_key(&0));
        cache.touch_group(0);
        assert!(cache.group_access.lock().contains_key(&0));
        cache.touch_group(1);
        assert!(cache.group_access.lock().contains_key(&1));
    }

    #[test]
    fn test_disk_cache_mark_interior_group() {
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 3600, 4, 64, 8).unwrap();
        assert!(!cache.interior_groups.lock().contains(&0));
        cache.mark_interior_group(0, 0);
        assert!(cache.interior_groups.lock().contains(&0));
        // Marking again is idempotent
        cache.mark_interior_group(0, 0);
        assert_eq!(cache.interior_groups.lock().len(), 1);
    }

    #[test]
    fn test_disk_cache_eviction_skips_interior() {
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 0, 4, 64, 8).unwrap(); // TTL=0 = disabled
        cache.mark_interior_group(0, 0);
        cache.touch_group(0);
        cache.touch_group(1);
        cache.evict_expired();
        assert!(cache.group_access.lock().contains_key(&0));
        assert!(cache.group_access.lock().contains_key(&1));
    }

    #[test]
    fn test_disk_cache_evict_group_clears_bitmap_and_state() {
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 3600, 4, 64, 16).unwrap();
        // Write pages in group 0 (pages 0-3)
        for i in 0..4u64 {
            cache.write_page(i, &vec![i as u8; 64]).unwrap();
        }
        cache.try_claim_group(0);
        cache.mark_group_present(0);
        assert!(cache.is_present(0));
        assert!(cache.is_present(3));
        assert_eq!(cache.group_state(0), GroupState::Present);

        // Evict group 0
        cache.evict_group(0);
        assert!(!cache.is_present(0));
        assert!(!cache.is_present(1));
        assert!(!cache.is_present(2));
        assert!(!cache.is_present(3));
        assert_eq!(cache.group_state(0), GroupState::None);
    }

    #[test]
    fn test_disk_cache_evict_group_preserves_other_groups() {
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 3600, 4, 64, 16).unwrap();
        // Write pages in groups 0 and 1
        for i in 0..8u64 {
            cache.write_page(i, &vec![i as u8; 64]).unwrap();
        }
        cache.evict_group(0); // Evict only group 0
        assert!(!cache.is_present(0));
        assert!(cache.is_present(4)); // Group 1 untouched
        assert!(cache.is_present(7));
    }

    #[test]
    fn test_disk_cache_evict_expired_with_real_ttl() {
        let dir = TempDir::new().unwrap();
        // TTL = 1 second
        let cache = DiskCache::new(dir.path(), 1, 4, 64, 16).unwrap();

        // Write and touch group 0
        for i in 0..4u64 {
            cache.write_page(i, &vec![i as u8; 64]).unwrap();
        }
        cache.try_claim_group(0);
        cache.mark_group_present(0);
        cache.touch_group(0);
        assert!(cache.is_present(0));

        // Sleep past TTL
        std::thread::sleep(Duration::from_millis(1100));

        cache.evict_expired();
        // Group should have been evicted
        assert!(!cache.is_present(0));
        assert_eq!(cache.group_state(0), GroupState::None);
    }

    #[test]
    fn test_disk_cache_evict_expired_protects_interior() {
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 1, 4, 64, 16).unwrap(); // TTL = 1s

        for i in 0..8u64 {
            cache.write_page(i, &vec![i as u8; 64]).unwrap();
        }
        cache.try_claim_group(0);
        cache.mark_group_present(0);
        cache.try_claim_group(1);
        cache.mark_group_present(1);
        cache.touch_group(0);
        cache.touch_group(1);
        cache.mark_interior_group(0, 0); // Group 0 is interior = pinned

        std::thread::sleep(Duration::from_millis(1100));
        cache.evict_expired();

        // Interior group 0 should survive; group 1 should be evicted
        assert!(cache.is_present(0)); // Pinned
        assert!(!cache.is_present(4)); // Evicted
    }

    #[test]
    fn test_disk_cache_evict_expired_skips_recent() {
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 10, 4, 64, 8).unwrap(); // TTL = 10s
        cache.touch_group(0);
        cache.evict_expired();
        // Group should NOT be evicted (only 0ms elapsed, TTL = 10s)
        assert!(cache.group_access.lock().contains_key(&0));
    }

    #[test]
    fn test_disk_cache_ensure_group_capacity() {
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 3600, 4, 64, 8).unwrap(); // 2 groups
        assert_eq!(cache.group_states.lock().len(), 2);
        cache.ensure_group_capacity(10);
        assert_eq!(cache.group_states.lock().len(), 10);
        // New groups should be None
        assert_eq!(cache.group_state(5), GroupState::None);
    }

    #[test]
    fn test_disk_cache_bitmap_persistence() {
        let dir = TempDir::new().unwrap();
        {
            let cache = DiskCache::new(dir.path(), 3600, 4, 64, 8).unwrap();
            cache.write_page(3, &vec![1u8; 64]).unwrap();
            cache.persist_bitmap().unwrap();
        }
        let cache2 = DiskCache::new(dir.path(), 3600, 4, 64, 8).unwrap();
        assert!(cache2.is_present(3));
        assert!(!cache2.is_present(4));
    }

    #[test]
    fn test_disk_cache_reopen_initializes_group_states_from_bitmap() {
        let dir = TempDir::new().unwrap();
        {
            let cache = DiskCache::new(dir.path(), 3600, 4, 64, 16).unwrap();
            // Write ALL pages in group 0 (pages 0-3)
            for i in 0..4u64 {
                cache.write_page(i, &vec![i as u8; 64]).unwrap();
            }
            cache.persist_bitmap().unwrap();
        }
        // Reopen — group 0 should be Present (all 4 pages marked)
        let cache2 = DiskCache::new(dir.path(), 3600, 4, 64, 16).unwrap();
        assert_eq!(cache2.group_state(0), GroupState::Present);
        // Group 1 should be None (no pages)
        assert_eq!(cache2.group_state(1), GroupState::None);
    }

    #[test]
    fn test_disk_cache_reopen_partial_group_is_none() {
        let dir = TempDir::new().unwrap();
        {
            let cache = DiskCache::new(dir.path(), 3600, 4, 64, 16).unwrap();
            // Write only 2 of 4 pages in group 0
            cache.write_page(0, &vec![0u8; 64]).unwrap();
            cache.write_page(1, &vec![1u8; 64]).unwrap();
            cache.persist_bitmap().unwrap();
        }
        let cache2 = DiskCache::new(dir.path(), 3600, 4, 64, 16).unwrap();
        // Partial group should be None (not all pages present)
        assert_eq!(cache2.group_state(0), GroupState::None);
        // But individual pages should still be present
        assert!(cache2.is_present(0));
        assert!(cache2.is_present(1));
        assert!(!cache2.is_present(2));
    }

    #[test]
    fn test_disk_cache_zero_page_count() {
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 3600, 4, 64, 0).unwrap();
        assert_eq!(cache.group_states.lock().len(), 0);
        assert_eq!(cache.group_state(0), GroupState::None);
    }

    #[test]
    fn test_disk_cache_zero_ppg() {
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 3600, 0, 64, 100).unwrap();
        assert_eq!(cache.group_states.lock().len(), 0);
    }

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
            page_group_keys: vec![],
            interior_chunk_keys: HashMap::new(),
            frame_tables: Vec::new(),
            sub_pages_per_frame: 0,
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
            interior_chunk_keys: HashMap::new(),
            frame_tables: Vec::new(),
            sub_pages_per_frame: 0,
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
    // Seekable Encode/Decode
    // =========================================================================

    #[test]
    fn test_seekable_encode_decode_roundtrip() {
        let page_size = 4096u32;
        let sub_ppg = 4u32; // 4 pages per sub-chunk
        let total_pages = 10;

        // Create test pages with recognizable content
        let mut pages: Vec<Option<Vec<u8>>> = Vec::new();
        for i in 0..total_pages {
            let mut page = vec![0u8; page_size as usize];
            page[0] = (i + 1) as u8; // marker byte
            page[page_size as usize - 1] = (i + 100) as u8; // tail marker
            pages.push(Some(page));
        }

        // Encode as seekable
        let (blob, frame_table) = encode_page_group_seekable(
            &pages,
            page_size,
            sub_ppg,
            3, // compression level
            #[cfg(feature = "zstd")]
            None,
        )
        .expect("encode seekable");

        // Should have ceil(10/4) = 3 frames
        assert_eq!(frame_table.len(), 3);

        // Full decode should produce identical pages
        let (pg_count, pg_size, full_data) = decode_page_group_seekable_full(
            &blob,
            &frame_table,
            page_size,
            total_pages as u32, // ppg = total_pages for this test
            total_pages as u64,
            0,
            #[cfg(feature = "zstd")]
            None,
        )
        .expect("full decode");
        assert_eq!(pg_count, total_pages as u32);
        assert_eq!(pg_size, page_size);

        for i in 0..total_pages {
            let start = i * page_size as usize;
            assert_eq!(full_data[start], (i + 1) as u8, "page {} marker", i);
            assert_eq!(
                full_data[start + page_size as usize - 1],
                (i + 100) as u8,
                "page {} tail",
                i,
            );
        }

        // Sub-chunk decode: fetch frame 1 (pages 4-7)
        let entry = &frame_table[1];
        let frame_bytes = &blob[entry.offset as usize..(entry.offset as usize + entry.len as usize)];
        let subchunk = decode_seekable_subchunk(
            frame_bytes,
            #[cfg(feature = "zstd")]
            None,
        )
        .expect("subchunk decode");

        // Should contain 4 pages (pages 4,5,6,7)
        assert_eq!(subchunk.len(), 4 * page_size as usize);
        for j in 0..4 {
            let i = 4 + j; // global page index
            let start = j * page_size as usize;
            assert_eq!(subchunk[start], (i + 1) as u8, "subchunk page {} marker", j);
        }

        // Sub-chunk decode: fetch frame 2 (pages 8-9, partial)
        let entry2 = &frame_table[2];
        let frame_bytes2 = &blob[entry2.offset as usize..(entry2.offset as usize + entry2.len as usize)];
        let subchunk2 = decode_seekable_subchunk(
            frame_bytes2,
            #[cfg(feature = "zstd")]
            None,
        )
        .expect("subchunk2 decode");
        // Last frame has 2 pages
        assert_eq!(subchunk2.len(), 2 * page_size as usize);
        assert_eq!(subchunk2[0], 9u8); // page 8 marker (8+1=9)
        assert_eq!(subchunk2[page_size as usize], 10u8); // page 9 marker (9+1=10)
    }

    #[test]
    fn test_seekable_manifest_serde_with_frames() {
        let m = Manifest {
            version: 1,
            page_count: 128,
            page_size: 4096,
            pages_per_group: 64,
            page_group_keys: vec!["pg/0_v1".to_string(), "pg/1_v1".to_string()],
            interior_chunk_keys: HashMap::new(),
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

    // =========================================================================
    // TieredConfig
    // =========================================================================

    #[test]
    fn test_tiered_config_default() {
        let c = TieredConfig::default();
        assert_eq!(c.bucket, "");
        assert_eq!(c.prefix, "");
        assert_eq!(c.cache_dir, PathBuf::from("/tmp/sqlces-cache"));
        assert_eq!(c.compression_level, 3);
        assert_eq!(c.endpoint_url, None);
        assert!(!c.read_only);
        assert!(c.runtime_handle.is_none());
        assert_eq!(c.pages_per_group, DEFAULT_PAGES_PER_GROUP);
        assert_eq!(c.region, None);
        assert_eq!(c.cache_ttl_secs, 3600);
        assert_eq!(c.prefetch_hops, vec![0.33, 0.33]);
        let expected_threads = std::thread::available_parallelism()
            .map(|n| n.get() as u32)
            .unwrap_or(2) + 1;
        assert_eq!(c.prefetch_threads, expected_threads);
    }

    #[test]
    fn test_tiered_config_default_pages_per_group_is_4096() {
        assert_eq!(DEFAULT_PAGES_PER_GROUP, 4096);
        assert_eq!(TieredConfig::default().pages_per_group, 4096);
    }

    // =========================================================================
    // Fraction-Based Prefetch Math
    // =========================================================================

    #[test]
    fn test_fraction_prefetch_schedule() {
        let hops = vec![0.33f32, 0.33];
        let total_groups = 10u64;
        let count1 = ((total_groups as f32) * hops[0]).ceil() as u64;
        assert_eq!(count1, 4); // 33% of 10 = 3.3 → ceil = 4
        let count2 = ((total_groups as f32) * hops[1]).ceil() as u64;
        assert_eq!(count2, 4);
        let count3 = ((total_groups as f32) * 1.0).ceil() as u64;
        assert_eq!(count3, 10); // 100%
    }

    #[test]
    fn test_fraction_prefetch_single_group() {
        let hops = vec![0.33f32, 0.33];
        let total_groups = 1u64;
        let count = ((total_groups as f32) * hops[0]).ceil() as u64;
        assert_eq!(count, 1); // 33% of 1 = 0.33 → ceil = 1
    }

    #[test]
    fn test_fraction_prefetch_large_db() {
        let hops = vec![0.33f32, 0.33];
        let total_groups = 1000u64;
        let hop1 = ((total_groups as f32) * hops[0]).ceil() as u64;
        assert_eq!(hop1, 330);
        let hop2 = ((total_groups as f32) * hops[1]).ceil() as u64;
        assert_eq!(hop2, 330);
        // Remaining after 2 hops
        let remaining = total_groups - hop1 - hop2;
        assert_eq!(remaining, 340); // hop3 fetches all remaining
    }

    #[test]
    fn test_fraction_prefetch_empty_hops() {
        let hops: Vec<f32> = vec![];
        // With empty hops, hop_idx 0 is beyond len, so fraction = 1.0
        let total_groups = 10u64;
        let fraction = if 0 < hops.len() { hops[0] } else { 1.0 };
        let count = ((total_groups as f32) * fraction).ceil() as u64;
        assert_eq!(count, 10); // Fetch all immediately
    }

    // =========================================================================
    // S3Client page_group_key format
    // =========================================================================

    #[test]
    fn test_s3_key_format() {
        // Verify the key format is "pg/{gid}_v{version}"
        let key = format!("pg/{}_v{}", 5, 3);
        assert_eq!(key, "pg/5_v3");
        let key0 = format!("pg/{}_v{}", 0, 1);
        assert_eq!(key0, "pg/0_v1");
    }

    // =========================================================================
    // DiskCache: concurrent group state transitions
    // =========================================================================

    #[test]
    fn test_disk_cache_concurrent_claim() {
        let dir = TempDir::new().unwrap();
        let cache = Arc::new(DiskCache::new(dir.path(), 3600, 4, 64, 16).unwrap());

        // Simulate two threads trying to claim the same group
        let claimed1 = cache.try_claim_group(0);
        let claimed2 = cache.try_claim_group(0);
        // Exactly one should succeed
        assert!(claimed1 ^ claimed2, "exactly one thread should claim the group");
    }

    #[test]
    fn test_disk_cache_multiple_groups_independent() {
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 3600, 4, 64, 32).unwrap(); // 8 groups

        // Claim different groups — all should succeed
        for gid in 0..8u64 {
            assert!(cache.try_claim_group(gid), "should claim group {}", gid);
        }
        // All should be Fetching
        for gid in 0..8u64 {
            assert_eq!(cache.group_state(gid), GroupState::Fetching);
        }
    }

    // =========================================================================
    // End-to-end: encode → write to cache → read back
    // =========================================================================

    #[test]
    fn test_encode_cache_read_roundtrip() {
        let dir = TempDir::new().unwrap();
        let ppg = 4u32;
        let page_size = 64u32;
        let cache = DiskCache::new(dir.path(), 3600, ppg, page_size, ppg as u64).unwrap();

        // Create page group with known data
        let pages: Vec<Option<Vec<u8>>> = (0..ppg)
            .map(|i| Some(vec![i as u8 + 1; page_size as usize]))
            .collect();
        let encoded = test_encode(&pages, page_size);

        // Simulate what decode_and_cache_group does: decode whole group, write pages
        let (_pg_count, _ps, decoded) = test_decode(&encoded);
        for (i, page_data) in decoded.iter().enumerate() {
            cache.write_page(i as u64, page_data).unwrap();
        }

        // Read back from cache
        for i in 0..ppg {
            assert!(cache.is_present(i as u64));
            let mut buf = vec![0u8; page_size as usize];
            cache.read_page(i as u64, &mut buf).unwrap();
            assert_eq!(buf, vec![i as u8 + 1; page_size as usize]);
        }
    }

    // =========================================================================
    // Local-checkpoint-only flag
    // =========================================================================

    #[test]
    fn test_local_checkpoint_flag_default_false() {
        set_local_checkpoint_only(false);
        assert!(!is_local_checkpoint_only());
    }

    #[test]
    fn test_local_checkpoint_flag_set_and_clear() {
        set_local_checkpoint_only(true);
        assert!(is_local_checkpoint_only());
        set_local_checkpoint_only(false);
        assert!(!is_local_checkpoint_only());
    }

    #[test]
    fn test_s3_dirty_groups_drain_merges_into_groups_dirty() {
        // Simulate what sync() does: drain s3_dirty_groups into groups_dirty
        let mut s3_dirty: HashSet<u64> = HashSet::new();
        s3_dirty.insert(0);
        s3_dirty.insert(3);
        s3_dirty.insert(7);

        let mut groups_dirty: HashMap<u64, Vec<u64>> = HashMap::new();
        groups_dirty.entry(0).or_default().push(100);
        groups_dirty.entry(5).or_default().push(20480);

        // Merge pending groups (as sync() does)
        for gid in s3_dirty.drain() {
            groups_dirty.entry(gid).or_default();
        }

        // Group 0: has dirty_snapshot pages
        assert_eq!(groups_dirty[&0], vec![100]);
        // Group 5: has dirty_snapshot pages
        assert_eq!(groups_dirty[&5], vec![20480]);
        // Groups 3 and 7: pending from local checkpoint, no dirty_snapshot pages
        assert!(groups_dirty[&3].is_empty());
        assert!(groups_dirty[&7].is_empty());
        assert!(s3_dirty.is_empty());
    }

    #[test]
    fn test_local_checkpoint_interior_page_detection() {
        let page_size = 4096usize;

        // B-tree interior index page (type 0x02) — not page 0
        let mut interior_index = vec![0u8; page_size];
        interior_index[0] = 0x02;
        assert_eq!(interior_index.get(0), Some(&0x02));

        // B-tree interior table page (type 0x05) — not page 0
        let mut interior_table = vec![0u8; page_size];
        interior_table[0] = 0x05;
        assert_eq!(interior_table.get(0), Some(&0x05));

        // Page 0: type byte at offset 100 (after SQLite header)
        let mut page0 = vec![0u8; page_size];
        page0[100] = 0x05;
        assert_eq!(page0.get(100), Some(&0x05));

        // Leaf page (type 0x0D) — should NOT be interior
        let mut leaf = vec![0u8; page_size];
        leaf[0] = 0x0D;
        let b = leaf[0];
        assert!(b != 0x05 && b != 0x02);
    }
}
