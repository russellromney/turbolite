//! Regression tests for adversarial review A1:
//! set_len must keep BTree-aware group_pages/page_index consistent.

use std::sync::Arc;

use hadb_storage::StorageBackend;
use hadb_storage_mem::MemStorage;
use sqlite_plugin::flags::OpenOpts;
use sqlite_plugin::vfs::Vfs;
use tempfile::TempDir;
use turbolite::tiered::{
    import_sqlite_file, register_shared, CacheConfig, CompressionConfig, SharedTurboliteVfs,
    TurboliteConfig, TurboliteVfs,
};

fn unique_vfs_name(label: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    format!("setlen-btree-{label}-{nanos}")
}

/// Import a local SQLite file into a MemStorage-backed TurboliteVFS and return
/// the VFS plus the imported page size.
fn import_local_db(
    dir: &std::path::Path,
    backend: Arc<dyn StorageBackend>,
) -> (SharedTurboliteVfs, u32) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("rt");

    let local_path = dir.join("source.db");
    let conn = rusqlite::Connection::open(&local_path).unwrap();
    conn.execute_batch("PRAGMA page_size=4096; PRAGMA journal_mode=WAL;")
        .unwrap();
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);")
        .unwrap();
    for i in 0..100 {
        conn.execute(
            "INSERT INTO t (id, v) VALUES (?1, ?2)",
            rusqlite::params![i, format!("value-{i}")],
        )
        .unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    let page_size: i64 = conn
        .query_row("PRAGMA page_size", [], |r| r.get(0))
        .unwrap();
    drop(conn);

    let mut cfg = TurboliteConfig {
        cache_dir: dir.to_path_buf(),
        compression: CompressionConfig {
            level: 1,
            ..Default::default()
        },
        cache: CacheConfig {
            pages_per_group: 4,
            ..Default::default()
        },
        ..Default::default()
    };
    cfg.apply_legacy_flat_fields();
    let manifest = import_sqlite_file(&cfg, backend.clone(), rt.handle().clone(), &local_path)
        .expect("import");
    assert!(
        matches!(
            manifest.strategy,
            turbolite::tiered::GroupingStrategy::BTreeAware
        ),
        "import should produce BTree-aware grouping"
    );
    let vfs = TurboliteVfs::with_backend(cfg, backend, rt.handle().clone()).expect("vfs");
    (SharedTurboliteVfs::new(vfs), page_size as u32)
}

#[test]
fn set_len_truncate_and_regrow_keeps_btree_mapping_consistent() {
    let dir = TempDir::new().expect("tmpdir");
    let mem = Arc::new(MemStorage::new());
    let (vfs, page_size) = import_local_db(dir.path(), mem.clone());
    let vfs_name = unique_vfs_name("writer");
    // Keep a clone for VFS operations after registering the VFS with SQLite.
    let vfs_for_ops = vfs.clone();
    register_shared(&vfs_name, vfs).expect("register");

    let flags = sqlite_plugin::vars::SQLITE_OPEN_MAIN_DB
        | sqlite_plugin::vars::SQLITE_OPEN_READWRITE
        | sqlite_plugin::vars::SQLITE_OPEN_CREATE;
    let mut handle = vfs_for_ops
        .open(Some("test.db"), OpenOpts::new(flags))
        .expect("open handle");

    // Shrink to a single page.
    vfs_for_ops
        .truncate(&mut handle, page_size as usize)
        .expect("truncate shrink");

    // Regrow to four pages. With the bug, page_index still pointed at the old
    // pre-truncate group assignment, so reads either panicked or returned stale bytes.
    let new_size = 4 * page_size as usize;
    vfs_for_ops
        .truncate(&mut handle, new_size)
        .expect("truncate regrow");

    // Page 3 is unassigned after regrow; it must read as zeros (not panic).
    let mut buf = vec![0u8; page_size as usize];
    vfs_for_ops
        .read(&mut handle, 3 * page_size as usize, &mut buf)
        .expect("read regrown page");
    assert!(
        buf.iter().all(|&b| b == 0),
        "unassigned regrown page must be zero-filled"
    );

    // Write page 3 and read it back.
    let written = [0xABu8; 4096];
    vfs_for_ops
        .write(&mut handle, 3 * page_size as usize, &written)
        .expect("write regrown page");
    vfs_for_ops
        .read(&mut handle, 3 * page_size as usize, &mut buf)
        .expect("read written page");
    assert_eq!(buf, &written[..], "written regrown page must round-trip");

    vfs_for_ops.close(handle).expect("close");
}
