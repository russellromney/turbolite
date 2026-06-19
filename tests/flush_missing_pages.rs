//! Regression tests for adversarial review C2/C3:
//! missing/evicted pages must not be encoded as zeros during flush.

use std::sync::Arc;

use hadb_storage::StorageBackend;
use hadb_storage_mem::MemStorage;
use tempfile::TempDir;
use turbolite::tiered::{
    register_shared, CacheConfig, CompressionConfig, SharedTurboliteVfs, TurboliteConfig,
    TurboliteVfs,
};

fn unique_vfs_name(label: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    format!("flush-missing-{label}-{nanos}")
}

fn build_vfs(
    dir: &std::path::Path,
    label: &str,
    backend: Arc<dyn StorageBackend>,
    rt: tokio::runtime::Handle,
) -> (String, SharedTurboliteVfs) {
    let cfg = TurboliteConfig {
        cache_dir: dir.to_path_buf(),
        compression: CompressionConfig {
            level: 1,
            ..Default::default()
        },
        cache: CacheConfig {
            pages_per_group: 4,
            checkpoint_mode: turbolite::tiered::CheckpointMode::LocalThenFlush,
            ..Default::default()
        },
        ..Default::default()
    };
    let vfs = TurboliteVfs::with_backend(cfg, backend, rt).expect("vfs");
    let shared = SharedTurboliteVfs::new(vfs);
    let vfs_name = unique_vfs_name(label);
    register_shared(&vfs_name, shared.clone()).expect("register");
    (vfs_name, shared)
}

fn write_rows(vfs_name: &str, count: i64) {
    let flags =
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE;
    let conn =
        rusqlite::Connection::open_with_flags_and_vfs("missing.db", flags, vfs_name).expect("open");
    conn.execute_batch("PRAGMA journal_mode=WAL;").expect("wal");
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);")
        .expect("schema");
    let tx = conn.unchecked_transaction().unwrap();
    for i in 0..count {
        tx.execute(
            "INSERT INTO t (id, v) VALUES (?1, ?2)",
            rusqlite::params![i, i * 10],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("ckpt");
}

fn update_row(vfs_name: &str, id: i64, v: i64) {
    let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE;
    let conn =
        rusqlite::Connection::open_with_flags_and_vfs("missing.db", flags, vfs_name).expect("open");
    conn.execute(
        "UPDATE t SET v = ?1 WHERE id = ?2",
        rusqlite::params![v, id],
    )
    .expect("update");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("ckpt");
}

fn assert_rows(vfs_name: &str, count: i64, expected: impl Fn(i64) -> i64) {
    let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY;
    let conn =
        rusqlite::Connection::open_with_flags_and_vfs("missing.db", flags, vfs_name).expect("open");
    for i in 0..count {
        let got: i64 = conn
            .query_row("SELECT v FROM t WHERE id = ?1", rusqlite::params![i], |r| {
                r.get(0)
            })
            .unwrap_or_else(|e| panic!("read row {i}: {e}"));
        assert_eq!(got, expected(i), "row {i} mismatch");
    }
}

/// LocalThenFlush: write multiple groups, flush, clear cache, overwrite a page,
/// flush again. The second flush must fetch missing pages from the backend
/// rather than encoding zeros.
#[test]
fn local_then_flush_does_not_encode_missing_pages_as_zeros() {
    let dir = TempDir::new().expect("tmpdir");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("rt");
    let mem = Arc::new(MemStorage::new());

    let (vfs_name, shared) = build_vfs(dir.path(), "writer", mem.clone(), rt.handle().clone());
    // Write enough rows to span multiple page groups (pages_per_group=4).
    write_rows(&vfs_name, 1000);
    shared.flush_to_storage().expect("first flush");

    // Evict everything from the local cache so the next flush must merge.
    shared.vfs().clear_cache();

    // Overwrite one row. This dirties a single page but the rest of its group
    // is no longer in cache.
    update_row(&vfs_name, 42, 9999);
    shared.flush_to_storage().expect("second flush");

    // Cold reader with a fresh cache dir sharing the same backend.
    let cold_dir = TempDir::new().expect("cold tmpdir");
    let (cold_name, _) = build_vfs(cold_dir.path(), "cold", mem.clone(), rt.handle().clone());

    // If missing pages were encoded as zeros, this read returns corrupt/garbage
    // data or fails with a malformed database error.
    assert_rows(&cold_name, 1000, |i| if i == 42 { 9999 } else { i * 10 });
}

#[test]
fn local_then_flush_growth_after_import_does_not_lose_pages() {
    use std::sync::Arc;
    use hadb_storage_mem::MemStorage;
    use turbolite::tiered::{import_sqlite_file, TurboliteConfig, CompressionConfig, CacheConfig};

    let dir = TempDir::new().expect("tmpdir");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("rt");
    let mem = Arc::new(MemStorage::new());

    // Build an imported BTree-aware base database.
    let source = dir.path().join("source.db");
    {
        let conn = rusqlite::Connection::open(&source).unwrap();
        conn.execute_batch("PRAGMA page_size=4096; PRAGMA journal_mode=WAL;").unwrap();
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);").unwrap();
        for i in 0..100 {
            conn.execute("INSERT INTO t (id, v) VALUES (?1, ?2)", rusqlite::params![i, i]).unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    let cfg = TurboliteConfig {
        cache_dir: dir.path().join("cache"),
        compression: CompressionConfig { level: 1, ..Default::default() },
        cache: CacheConfig { pages_per_group: 4, checkpoint_mode: turbolite::tiered::CheckpointMode::LocalThenFlush, ..Default::default() },
        ..Default::default()
    };
    let _manifest = import_sqlite_file(&cfg, mem.clone(), rt.handle().clone(), &source).unwrap();

    let (vfs_name, shared) = build_vfs(dir.path().join("cache2").as_path(), "growth", mem.clone(), rt.handle().clone());
    // Growth write: add many more rows, which grows the file beyond imported page_count.
    {
        let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE;
        let conn = rusqlite::Connection::open_with_flags_and_vfs("growth.db", flags, &vfs_name).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        let tx = conn.unchecked_transaction().unwrap();
        for i in 1000..2000 {
            tx.execute("INSERT INTO t (id, v) VALUES (?1, ?2)", rusqlite::params![i, i]).unwrap();
        }
        tx.commit().unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    shared.flush_to_storage().expect("flush growth");

    // Cold reader should see all new rows.
    let cold_dir = TempDir::new().expect("cold tmpdir");
    let (cold_name, _) = build_vfs(cold_dir.path(), "cold-growth", mem.clone(), rt.handle().clone());
    let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY;
    let conn = rusqlite::Connection::open_with_flags_and_vfs("growth.db", flags, &cold_name).unwrap();
    for i in 1000..2000 {
        let got: i64 = conn
            .query_row("SELECT v FROM t WHERE id = ?1", rusqlite::params![i], |r| r.get(0))
            .unwrap_or_else(|e| panic!("read row {i}: {e}"));
        assert_eq!(got, i, "row {i} mismatch");
    }
}
