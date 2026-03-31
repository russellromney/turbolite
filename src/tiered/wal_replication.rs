//! Phase Somme: WAL replication via walrust integration.
//!
//! When `wal_replication=true`, the VFS starts a background walrust replication
//! loop that ships WAL frames to S3 on a timer. This provides transaction-level
//! durability between turbolite checkpoints.
//!
//! Architecture:
//! - turbolite page groups = snapshot (managed by sync/checkpoint)
//! - walrust WAL shipping = incremental durability (managed by this module)
//! - Both use SQLite's file change counter as version/txid (Phase Somme-b)
//!
//! The replication loop starts lazily on the first MainDb open (not in VFS::new)
//! because the WAL file path isn't known until SQLite calls open().

#![cfg(feature = "wal")]

use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use parking_lot::Mutex;

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
    /// Called from TieredVfs::open() on first MainDb open.
    ///
    /// `db_path`: path to the SQLite DB file (for walrust to read WAL)
    /// `wal_prefix`: S3 prefix for WAL segments (e.g., "{turbolite_prefix}/wal/")
    /// `initial_txid`: current manifest version (file change counter)
    /// `sync_interval_ms`: how often to ship WAL frames
    /// `bucket`: S3 bucket name
    /// `endpoint`: S3 endpoint URL
    /// `region`: AWS region
    pub(crate) fn start(
        &mut self,
        db_path: PathBuf,
        wal_prefix: String,
        initial_txid: u64,
        sync_interval_ms: u64,
        bucket: String,
        endpoint: Option<String>,
        region: Option<String>,
        runtime_handle: tokio::runtime::Handle,
    ) -> io::Result<()> {
        if self.started.swap(true, Ordering::SeqCst) {
            return Ok(()); // Already started
        }

        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        self.cancel_tx = Some(cancel_tx);

        let config = walrust_core::ReplicationConfig {
            sync_interval: std::time::Duration::from_millis(sync_interval_ms),
            snapshot_interval: std::time::Duration::from_secs(86400), // unused, no periodic snapshots
            retry_policy: walrust_core::RetryPolicy::new(walrust_core::RetryConfig::default()),
            db_name: None,
        };

        let handle = runtime_handle.spawn(async move {
            // Create S3 backend for walrust
            let storage = match walrust_core::S3Backend::from_env(
                bucket,
                endpoint.as_deref(),
            ).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("[wal-replication] ERROR: failed to create S3 backend: {}", e);
                    return;
                }
            };

            let mut state = match walrust_core::SyncState::new(db_path) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("[wal-replication] ERROR: failed to create SyncState: {}", e);
                    return;
                }
            };

            eprintln!(
                "[wal-replication] starting WAL replication (db={}, txid={}, interval={}ms)",
                state.name, initial_txid, sync_interval_ms,
            );

            if let Err(e) = walrust_core::run_wal_replication(
                &storage,
                &wal_prefix,
                &mut state,
                initial_txid,
                config,
                cancel_rx,
            ).await {
                eprintln!("[wal-replication] ERROR: replication loop exited: {}", e);
            } else {
                eprintln!("[wal-replication] replication stopped (final txid={})", state.current_txid);
            }
        });

        *self.join_handle.lock() = Some(handle);
        Ok(())
    }

    /// Signal the replication loop to stop and do a final WAL sync.
    pub(crate) fn stop(&mut self) {
        if let Some(tx) = self.cancel_tx.take() {
            let _ = tx.send(true);
            eprintln!("[wal-replication] shutdown signal sent");
        }
    }

    /// Check if replication has been started.
    pub(crate) fn is_started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }
}

impl Drop for WalReplicationState {
    fn drop(&mut self) {
        self.stop();
    }
}
