//! Integration tests for materialize_to_file (Phase Somme prerequisite).

use super::helpers::*;
use turbolite::tiered::*;

/// Materialize a B-tree-aware database to a local file. Verify SQLite can open it
/// and row counts match.
#[test]
fn test_materialize_roundtrip() {
    let cache_dir = tempfile::tempdir().unwrap();
    let config = test_config("mat_rt", cache_dir.path());

    // Create and import a multi-table database
    let local_db = cache_dir.path().join("source.db");
    {
        let conn = rusqlite::Connection::open(&local_db).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        conn.execute_batch(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
             CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, body TEXT);
             CREATE INDEX idx_posts_user ON posts(user_id);",
        ).unwrap();
        for i in 0..500 {
            conn.execute("INSERT INTO users VALUES (?1, ?2)",
                rusqlite::params![i, format!("user_{}", i)]).unwrap();
            conn.execute("INSERT INTO posts VALUES (?1, ?2, ?3)",
                rusqlite::params![i, i % 50, format!("post body {} with some padding data", i)]).unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
        drop(conn);
    }

    let manifest = import_sqlite_file(&config, &local_db).unwrap();
    eprintln!("[test] imported: {} pages, {} groups, version {}",
        manifest.page_count, manifest.page_group_keys.len(), manifest.version);

    // Create VFS to get shared_state
    let vfs_name = unique_vfs_name("mat_rt");
    let vfs = TieredVfs::new(config).unwrap();
    let bench = vfs.shared_state();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    // Materialize to a new file
    let output = cache_dir.path().join("materialized.db");
    let version = bench.materialize_to_file(&output).unwrap();
    assert_eq!(version, manifest.version, "materialized version should match manifest");

    // Open the materialized file with plain SQLite (no VFS) and verify data
    let conn = rusqlite::Connection::open(&output).unwrap();
    let user_count: i64 = conn.query_row("SELECT COUNT(*) FROM users", [], |row| row.get(0)).unwrap();
    let post_count: i64 = conn.query_row("SELECT COUNT(*) FROM posts", [], |row| row.get(0)).unwrap();
    assert_eq!(user_count, 500, "materialized DB should have 500 users");
    assert_eq!(post_count, 500, "materialized DB should have 500 posts");

    // Verify index works
    let idx_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM posts WHERE user_id = 5", [], |row| row.get(0),
    ).unwrap();
    assert_eq!(idx_count, 10, "index scan should return 10 posts for user_id=5");

    // Verify specific data
    let name: String = conn.query_row(
        "SELECT name FROM users WHERE id = 42", [], |row| row.get(0),
    ).unwrap();
    assert_eq!(name, "user_42");
}

/// Materialize after VFS writes + checkpoint. The materialized file should
/// include data from both the import and the VFS writes.
#[test]
fn test_materialize_after_vfs_writes() {
    let cache_dir = tempfile::tempdir().unwrap();
    let config = test_config("mat_vfs", cache_dir.path());

    let local_db = cache_dir.path().join("source.db");
    {
        let conn = rusqlite::Connection::open(&local_db).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        conn.execute_batch("CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT);").unwrap();
        for i in 0..100 {
            conn.execute("INSERT INTO data VALUES (?1, ?2)",
                rusqlite::params![i, format!("val_{}", i)]).unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
        drop(conn);
    }

    import_sqlite_file(&config, &local_db).unwrap();

    // Open via VFS, add more rows, checkpoint
    let vfs_name = unique_vfs_name("mat_vfs");
    let vfs = TieredVfs::new(config).unwrap();
    let bench = vfs.shared_state();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let db_path = format!("file:source.db?vfs={}", vfs_name);
    {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        for i in 100..200 {
            conn.execute("INSERT INTO data VALUES (?1, ?2)",
                rusqlite::params![i, format!("val_{}", i)]).unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    // Materialize should include all 200 rows
    let output = cache_dir.path().join("materialized.db");
    let version = bench.materialize_to_file(&output).unwrap();
    assert!(version >= 2, "version should be >= 2 after import + VFS checkpoint");

    let conn = rusqlite::Connection::open(&output).unwrap();
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM data", [], |row| row.get(0)).unwrap();
    assert_eq!(count, 200, "materialized DB should have 200 rows (100 imported + 100 VFS)");
}

/// Materialize returns correct version number.
#[test]
fn test_materialize_returns_manifest_version() {
    let cache_dir = tempfile::tempdir().unwrap();
    let config = test_config("mat_ver", cache_dir.path());

    let local_db = cache_dir.path().join("source.db");
    {
        let conn = rusqlite::Connection::open(&local_db).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY);").unwrap();
        for i in 0..50 { conn.execute("INSERT INTO t VALUES (?1)", [i]).unwrap(); }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
        drop(conn);
    }

    let manifest = import_sqlite_file(&config, &local_db).unwrap();

    let vfs_name = unique_vfs_name("mat_ver");
    let vfs = TieredVfs::new(config).unwrap();
    let bench = vfs.shared_state();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let output = cache_dir.path().join("materialized.db");
    let version = bench.materialize_to_file(&output).unwrap();
    assert_eq!(version, manifest.version);
}
