//! Phase Anvil g review tests: manifest + dirty_groups persistence split.
//!
//! Before Phase Anvil g, local recovery state was one atomic file
//! (`LocalManifest { manifest, dirty_groups }`). After, it's two
//! independent files (`manifest.msgpack` + `dirty_groups.msgpack`). These
//! tests pin the crash-recovery semantics so we notice if either file's
//! atomic-write invariant breaks or if the loader regresses.

use std::sync::Arc;

use hadb_storage::StorageBackend;
use hadb_storage_mem::MemStorage;
use tempfile::TempDir;
use turbolite::tiered::{
    register_shared, CacheConfig, CompressionConfig, SharedTurboliteVfs, SyncMode, TurboliteConfig, TurboliteVfs,
};

fn unique_vfs_name(label: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    format!("anvil-crash-{label}-{nanos}")
}

fn build_remote_vfs(
    dir: &std::path::Path,
    label: &str,
) -> (String, SharedTurboliteVfs, Arc<MemStorage>, tokio::runtime::Runtime) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("rt");
    let handle = rt.handle().clone();
    let mem = Arc::new(MemStorage::new());
    let backend: Arc<dyn StorageBackend> = mem.clone();
    let cfg = TurboliteConfig {
        cache_dir: dir.to_path_buf(),
        sync_mode: SyncMode::LocalThenFlush,
        compression: CompressionConfig { level: 1, ..Default::default() },
        cache: CacheConfig { pages_per_group: 4, ..Default::default() },
        ..Default::default()
    };
    let vfs = TurboliteVfs::with_backend(cfg, backend, handle).expect("vfs");
    let shared = SharedTurboliteVfs::new(vfs);
    let vfs_name = unique_vfs_name(label);
    register_shared(&vfs_name, shared.clone()).expect("register");
    (vfs_name, shared, mem, rt)
}

fn write_one_row(vfs_name: &str) {
    let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
        | rusqlite::OpenFlags::SQLITE_OPEN_CREATE;
    let conn = rusqlite::Connection::open_with_flags_and_vfs("recover.db", flags, vfs_name)
        .expect("open");
    conn.execute_batch("PRAGMA journal_mode=WAL;").expect("wal");
    conn.execute_batch("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v INTEGER);")
        .expect("schema");
    conn.execute("INSERT INTO t (v) VALUES (?1)", rusqlite::params![1i64])
        .expect("insert");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("ckpt");
}

/// After checkpoint in LocalThenFlush mode, `manifest.msgpack` and
/// `dirty_groups.msgpack` (or staging logs + trailers) are on disk and a
/// fresh VFS reopening the same `cache_dir` recovers pending flush state.
/// Critical: the recovered VFS knows there's work to do and
/// `flush_to_storage()` completes it.
#[test]
fn reopen_after_checkpoint_recovers_pending_flush() {
    let dir = TempDir::new().expect("tmpdir");

    // Session 1: write + checkpoint, then drop the VFS (simulates process exit).
    {
        let (vfs_name, shared, _mem, _rt) = build_remote_vfs(dir.path(), "s1");
        write_one_row(&vfs_name);
        assert!(
            shared.has_pending_flush(),
            "LocalThenFlush must leave pending work post-checkpoint"
        );
        // Intentionally do NOT flush_to_storage(). Drop everything. Session
        // 2 has to recover from disk state alone.
    }

    // Session 2: new VFS, new runtime, new backend (fresh MemStorage, so we
    // can tell backend writes are from the flush drain, not leftover state).
    let (vfs_name2, shared2, mem2, rt2) = build_remote_vfs(dir.path(), "s2");

    // Registration succeeds for a different name; the filesystem state is
    // shared because we point at the same cache_dir.
    assert_ne!(vfs_name2, "", "new vfs registered");

    // Open the connection just to force VFS initialisation (it should
    // replay/recover from disk state during construction).
    {
        let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE;
        let _conn =
            rusqlite::Connection::open_with_flags_and_vfs("recover.db", flags, &vfs_name2)
                .expect("reopen");
    }

    // The recovered VFS must know there are pending page groups to flush.
    // If this assertion fires, the manifest/dirty_groups split lost its
    // round-trip correctness OR the staging-log recovery path regressed.
    assert!(
        shared2.has_pending_flush(),
        "reopened VFS must recover pending flush state; \
         if this fires, crash-recovery is broken"
    );

    // Drain to the fresh backend. Before the drain, backend is empty (since
    // it's a new MemStorage); after, it must see the manifest + page groups.
    let before = rt2
        .block_on(async { mem2.list("", None).await.expect("list").len() });
    assert_eq!(
        before, 0,
        "fresh backend starts empty; baseline for measuring the drain"
    );

    shared2.flush_to_storage().expect("drain");

    let after = rt2
        .block_on(async { mem2.list("", None).await.expect("list").len() });
    assert!(
        after >= 1,
        "flush_to_storage() after recovery must push at least the manifest; got {after}"
    );
    assert!(
        !shared2.has_pending_flush(),
        "drain must clear pending flush state"
    );
}

/// Partial-crash scenario: `manifest.msgpack` persisted but
/// `dirty_groups.msgpack` did not (crash between the two atomic renames).
/// The reopened VFS must still start cleanly; losing the dirty_groups file
/// is acceptable (staging logs are the authoritative recovery mechanism
/// in the new scheme) but the manifest-level state must survive.
#[test]
fn reopen_with_missing_dirty_groups_file_does_not_panic() {
    let dir = TempDir::new().expect("tmpdir");

    {
        let (vfs_name, shared, _mem, _rt) = build_remote_vfs(dir.path(), "s1");
        write_one_row(&vfs_name);
        // Flush so the manifest reflects a real committed state, then
        // we'll nuke dirty_groups below to simulate the partial-write.
        shared.flush_to_storage().expect("drain");
    }

    // Simulate the partial-crash: dirty_groups.msgpack got removed between
    // manifest persist and dirty_groups persist. (In our case it's already
    // been removed by the drain; persist_dirty_groups(&[]) deletes the
    // file. Write some garbage state to confirm reopen survives absence.)
    let dirty_path = dir.path().join("dirty_groups.msgpack");
    if dirty_path.exists() {
        std::fs::remove_file(&dirty_path).expect("remove dirty_groups");
    }

    // Reopen. Must not panic, must surface a consistent manifest.
    let (vfs_name2, _shared2, _mem2, _rt2) = build_remote_vfs(dir.path(), "s2");
    let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE;
    let conn =
        rusqlite::Connection::open_with_flags_and_vfs("recover.db", flags, &vfs_name2)
            .expect("reopen after partial crash");
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .expect("query");
    assert_eq!(
        count, 1,
        "row from previous session must survive reopen without dirty_groups file"
    );
}
