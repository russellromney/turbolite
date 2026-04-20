//! Phase Anvil g review tests: SyncMode semantics on remote backends.
//!
//! These tests pin the contract so future refactors can't silently flip the
//! behaviour. They exercise `TurboliteVfs::with_backend` + `hadb-storage-mem`
//! + a tokio runtime; no S3, no network.
//!
//! Invariants verified:
//!   - `SyncMode::LocalThenFlush`: checkpoint does NOT upload to a remote
//!     backend. `flush_to_storage()` is the drain.
//!   - `SyncMode::Durable`: checkpoint DOES upload to the backend inline.
//!     Without this, the review-fix regression for `with_backend`
//!     silently downgrading Durable -> LocalThenFlush can come back.

use std::sync::Arc;

use hadb_storage::StorageBackend;
use hadb_storage_mem::MemStorage;
use tempfile::TempDir;
use tokio::runtime::Runtime;

use turbolite::tiered::{
    CacheConfig, CompressionConfig, SharedTurboliteVfs, SyncMode, TurboliteConfig, TurboliteVfs,
};

const SCHEMA: &str = "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);";

fn unique_vfs_name(label: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    format!("anvil-sync-{label}-{nanos}")
}

/// Build a VFS with the given sync mode against an in-memory backend.
/// Returns everything the caller needs to drive it: the registered VFS
/// name, a handle to the shared VFS (for accessor methods), and the raw
/// backend so the test can inspect what was actually written.
fn build_vfs_with_mode(
    dir: &std::path::Path,
    sync_mode: SyncMode,
    label: &str,
) -> (String, SharedTurboliteVfs, Arc<MemStorage>, Runtime) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build rt");
    let handle = rt.handle().clone();

    let mem = Arc::new(MemStorage::new());
    let backend: Arc<dyn StorageBackend> = mem.clone();

    let cfg = TurboliteConfig {
        cache_dir: dir.to_path_buf(),
        sync_mode,
        compression: CompressionConfig { level: 1, ..Default::default() },
        cache: CacheConfig { pages_per_group: 4, ..Default::default() },
        ..Default::default()
    };

    let vfs =
        TurboliteVfs::with_backend(cfg, backend, handle).expect("build vfs");
    let shared = SharedTurboliteVfs::new(vfs);
    let vfs_name = unique_vfs_name(label);
    turbolite::tiered::register_shared(&vfs_name, shared.clone())
        .expect("register shared vfs");

    (vfs_name, shared, mem, rt)
}

/// Write one row through turbolite's VFS and run a SQLite checkpoint. The
/// checkpoint is what turbolite's xSync hooks into, so it's the thing that
/// should (or shouldn't) reach the backend depending on sync_mode.
fn write_and_checkpoint(vfs_name: &str) {
    let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
        | rusqlite::OpenFlags::SQLITE_OPEN_CREATE;
    let conn = rusqlite::Connection::open_with_flags_and_vfs("semantics.db", flags, vfs_name)
        .expect("open conn");
    conn.execute_batch("PRAGMA journal_mode=WAL;").expect("wal");
    conn.execute_batch(SCHEMA).expect("schema");
    conn.execute("INSERT INTO t (v) VALUES (?1)", rusqlite::params![1i64])
        .expect("insert");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("checkpoint");
}

/// Count how many keys the backend has seen. Since `MemStorage::list("", None)`
/// walks the whole map, this is a full census of what turbolite pushed during
/// the test.
fn backend_key_count(rt: &Runtime, mem: &MemStorage) -> usize {
    rt.block_on(async { mem.list("", None).await.expect("list").len() })
}

#[test]
fn local_then_flush_defers_upload_on_remote_backend() {
    let dir = TempDir::new().expect("tmpdir");
    let (vfs_name, shared, mem, rt) =
        build_vfs_with_mode(dir.path(), SyncMode::LocalThenFlush, "lthen");

    // Before any activity, backend is empty.
    assert_eq!(backend_key_count(&rt, &mem), 0, "backend starts empty");

    // Write + checkpoint. Under LocalThenFlush, the contract is: do NOT hit
    // the backend from inside sync. The staging log + pending_flushes queue
    // are the bookkeeping; `flush_to_storage()` is the drain.
    write_and_checkpoint(&vfs_name);

    let after_ckpt = backend_key_count(&rt, &mem);
    assert_eq!(
        after_ckpt, 0,
        "LocalThenFlush must not touch the remote backend during sync; \
         got {after_ckpt} backend keys. If this assertion fires, somebody \
         re-introduced the handle.rs sync() inline flush_dirty_groups call."
    );
    assert!(
        shared.has_pending_flush(),
        "LocalThenFlush must leave pending work for flush_to_storage()"
    );

    // Now drain. This is the call site that actually hits the backend.
    shared.flush_to_storage().expect("drain");
    let after_flush = backend_key_count(&rt, &mem);
    assert!(
        after_flush >= 1,
        "flush_to_storage() must push at least the manifest; got {after_flush}"
    );
    assert!(
        !shared.has_pending_flush(),
        "flush_to_storage() must drain pending work"
    );
}

#[test]
fn durable_uploads_inline_on_remote_backend() {
    let dir = TempDir::new().expect("tmpdir");
    let (vfs_name, shared, mem, rt) =
        build_vfs_with_mode(dir.path(), SyncMode::Durable, "durable");

    assert_eq!(backend_key_count(&rt, &mem), 0, "backend starts empty");

    // Durable's contract: every checkpoint pushes to the backend. Before
    // Phase Anvil g's review fix, with_backend silently rewrote this
    // to LocalThenFlush and the backend stayed empty. This test pins the
    // behaviour back.
    write_and_checkpoint(&vfs_name);

    let after_ckpt = backend_key_count(&rt, &mem);
    assert!(
        after_ckpt >= 1,
        "Durable checkpoint must push to the backend inline; got {after_ckpt} keys. \
         If this assertion fires, with_backend is silently downgrading Durable \
         again (see vfs.rs review fix)."
    );
    assert!(
        !shared.has_pending_flush(),
        "Durable must not leave pending work; all writes committed inline"
    );
}
