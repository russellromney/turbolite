//! WAL replication via walrust integration.
//!
//! When `wal_replication=true`, the VFS starts a background walrust replication
//! loop that ships WAL frames to S3 on a timer. This provides transaction-level
//! durability between turbolite checkpoints.
//!
//! Architecture:
//! - turbolite page groups = snapshot (managed by sync/checkpoint)
//! - walrust WAL shipping = incremental durability (managed by this module)
//! - Both use SQLite's file change counter as version/txid
//!
//! The replication loop starts lazily on the first MainDb open (not in VFS::new)
//! because the WAL file path isn't known until SQLite calls open().

#![cfg(feature = "wal")]

use parking_lot::Mutex;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

/// block_on that works from inside or outside a tokio runtime.
/// Same pattern as S3Client::block_on.
fn safe_block_on<F: std::future::Future<Output = T>, T>(
    handle: &tokio::runtime::Handle,
    fut: F,
) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(_) => tokio::task::block_in_place(|| handle.block_on(fut)),
        Err(_) => handle.block_on(fut),
    }
}

/// State for the WAL replication background task.
pub(crate) struct WalReplicationState {
    /// Cancel signal sender. Send `true` to stop the background loop.
    cancel_tx: Option<tokio::sync::watch::Sender<bool>>,
    /// Whether replication has been started (prevents double-start).
    started: AtomicBool,
    /// Join handle for the background task (for graceful shutdown).
    join_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl WalReplicationState {
    pub(crate) fn new() -> Self {
        Self {
            cancel_tx: None,
            started: AtomicBool::new(false),
            join_handle: Mutex::new(None),
        }
    }

    /// Start WAL replication if not already started.
    pub(crate) fn start(
        &mut self,
        db_path: PathBuf,
        wal_prefix: String,
        initial_txid: u64,
        sync_interval_ms: u64,
        bucket: String,
        endpoint: Option<String>,
        _region: Option<String>,
        runtime_handle: tokio::runtime::Handle,
    ) -> io::Result<()> {
        if self.started.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        self.cancel_tx = Some(cancel_tx);

        let config = walrust_core::ReplicationConfig {
            sync_interval: std::time::Duration::from_millis(sync_interval_ms),
            snapshot_interval: std::time::Duration::from_secs(86400),
            retry_policy: walrust_core::RetryPolicy::new(walrust_core::RetryConfig::default()),
            db_name: None,
            ..Default::default()
        };

        let handle = runtime_handle.spawn(async move {
            let storage =
                match hadb_storage_s3::S3Storage::from_env(bucket, endpoint.as_deref()).await {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("[wal-replication] ERROR: S3 backend: {}", e);
                        return;
                    }
                };

            let mut state = match walrust_core::SyncState::new(db_path) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("[wal-replication] ERROR: SyncState: {}", e);
                    return;
                }
            };

            turbolite_debug!(
                "[wal-replication] starting (db={}, txid={}, interval={}ms)",
                state.name,
                initial_txid,
                sync_interval_ms,
            );

            if let Err(e) = walrust_core::run_wal_replication(
                &storage,
                &wal_prefix,
                &mut state,
                initial_txid,
                config,
                cancel_rx,
            )
            .await
            {
                eprintln!("[wal-replication] ERROR: {}", e);
            } else {
                turbolite_debug!(
                    "[wal-replication] stopped (final txid={})",
                    state.current_txid
                );
            }
        });

        *self.join_handle.lock() = Some(handle);
        Ok(())
    }

    pub(crate) fn stop(&mut self) {
        if let Some(tx) = self.cancel_tx.take() {
            let _ = tx.send(true);
            turbolite_debug!("[wal-replication] shutdown signal sent");
        }
    }

    pub(crate) fn is_started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }
}

impl Drop for WalReplicationState {
    fn drop(&mut self) {
        self.stop();
    }
}

// ============================================================================
// WAL recovery on cold start
// ============================================================================

/// Recover WAL segments from S3, apply to a materialized DB, load pages into cache.
///
/// Called from TurboliteVfs after construction (shared_state is available).
/// Returns number of pages loaded from WAL recovery, or 0 if no WAL to replay.
pub(crate) fn recover_wal_from_shared_state(
    shared_state: &super::bench::TurboliteSharedState,
    cache: &super::DiskCache,
    manifest_version: u64,
    page_size: u32,
    wal_prefix: &str,
    bucket: &str,
    endpoint: Option<&str>,
    runtime_handle: &tokio::runtime::Handle,
    cache_dir: &std::path::Path,
) -> io::Result<u64> {
    if manifest_version == 0 {
        return Ok(0);
    }

    // Step 1: check for WAL segments before doing expensive materialization
    let incr_keys = safe_block_on(runtime_handle, async {
        let storage = hadb_storage_s3::S3Storage::from_env(bucket.to_string(), endpoint)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("S3Backend: {}", e)))?;

        use hadb_storage::StorageBackend;
        let db_name = wal_prefix
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or("db");
        let incr_prefix = format!("{}{}/0000/", wal_prefix, db_name);
        let start_key = format!(
            "{}{:016x}-{:016x}.hadbp",
            incr_prefix, manifest_version, manifest_version
        );

        storage
            .list(&incr_prefix, Some(&start_key))
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("list WAL: {}", e)))
    })?;

    if incr_keys.is_empty() {
        turbolite_debug!(
            "[wal-recovery] no WAL segments newer than version {}",
            manifest_version
        );
        return Ok(0);
    }

    turbolite_debug!(
        "[wal-recovery] found {} WAL segments to replay",
        incr_keys.len()
    );

    // Step 2: materialize page groups to temp file
    let recovery_path = cache_dir.join("recovery.db");
    shared_state.materialize_to_file(&recovery_path)?;

    // Step 3: download and apply WAL segments
    let applied = safe_block_on(runtime_handle, async {
        let storage = hadb_storage_s3::S3Storage::from_env(bucket.to_string(), endpoint)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("S3Backend: {}", e)))?;

        use hadb_storage::StorageBackend;
        let mut count = 0u64;
        let mut prev_checksum = 0u64; // Start of chain; first changeset validates from 0
        for key in &incr_keys {
            // hadb_storage::StorageBackend::get returns Ok(None) for absent keys.
            // The list() above filtered by prefix + `after`, so a missing key here
            // means the object was deleted between list and get — treat as a hard
            // error so the caller knows recovery is incomplete.
            let data = storage
                .get(key)
                .await
                .map_err(|e| {
                    io::Error::new(io::ErrorKind::Other, format!("download {}: {}", key, e))
                })?
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!("WAL segment {} disappeared between list and get", key),
                    )
                })?;
            match walrust_core::ltx::apply_changeset_to_db(&data, &recovery_path, prev_checksum) {
                Ok(result) => {
                    prev_checksum = result.checksum;
                    count += 1;
                    turbolite_debug!("[wal-recovery] applied {}", key);
                }
                Err(e)
                    if e.to_string().contains("checksum") || e.to_string().contains("Checksum") =>
                {
                    turbolite_debug!("[wal-recovery] stopping at stale lineage: {}", e);
                    break;
                }
                Err(e) => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("apply {}: {}", key, e),
                    ));
                }
            }
        }
        Ok::<u64, io::Error>(count)
    })?;

    if applied == 0 {
        let _ = std::fs::remove_file(&recovery_path);
        return Ok(0);
    }

    // Step 4: read recovered pages into VFS cache
    turbolite_debug!(
        "[wal-recovery] loading {} WAL-recovered pages into cache...",
        applied
    );
    use std::os::unix::fs::FileExt;
    let file = std::fs::File::open(&recovery_path)?;
    let file_size = file.metadata()?.len();
    let recovered_page_count = file_size / page_size as u64;

    let mut pages_loaded = 0u64;
    for pnum in 0..recovered_page_count {
        let mut buf = vec![0u8; page_size as usize];
        if file
            .read_exact_at(&mut buf, pnum * page_size as u64)
            .is_ok()
        {
            if cache.write_page(pnum, &buf).is_ok() {
                pages_loaded += 1;
            }
        }
    }

    let _ = std::fs::remove_file(&recovery_path);
    turbolite_debug!(
        "[wal-recovery] loaded {} pages from WAL recovery",
        pages_loaded
    );
    Ok(pages_loaded)
}
