use super::*;
use tempfile::TempDir;

/// RED TEST: TieredVfs::new() with StorageBackend::Local should succeed
/// without any S3 credentials, tokio runtime, or cloud dependencies.
#[test]
fn test_local_vfs_construction() {
    let dir = TempDir::new().unwrap();
    let config = TieredConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };

    let vfs = TieredVfs::new(config).expect("local VFS construction should succeed");

    // Verify it's local
    assert!(vfs.storage.is_local());
}

/// RED TEST: Local VFS exists() returns false for new empty dir.
#[test]
fn test_local_vfs_exists_empty() {
    let dir = TempDir::new().unwrap();
    let config = TieredConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs = TieredVfs::new(config).expect("local VFS");
    assert!(!vfs.storage.exists().unwrap());
}

/// RED TEST: Local VFS can register with SQLite, open a db, and do CRUD.
#[test]
fn test_local_vfs_sqlite_roundtrip() {
    let dir = TempDir::new().unwrap();
    let config = TieredConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };

    let vfs_name = format!("local_rt_{}", std::process::id());
    let vfs = TieredVfs::new(config).expect("local VFS");
    crate::tiered::register(&vfs_name, vfs).expect("register");

    let db_path = format!("file:test.db?vfs={}", vfs_name);
    let conn = rusqlite::Connection::open(&db_path).expect("open");
    conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", []).unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'hello')", []).unwrap();

    let val: String = conn
        .query_row("SELECT val FROM t WHERE id = 1", [], |row| row.get(0))
        .unwrap();
    assert_eq!(val, "hello");
}

/// Local VFS with compression enabled: write + checkpoint + reopen.
#[test]
fn test_local_vfs_with_compression() {
    let dir = TempDir::new().unwrap();

    {
        let config = TieredConfig {
            storage_backend: StorageBackend::Local,
            cache_dir: dir.path().to_path_buf(),
            cache_compression: true,
            cache_compression_level: 3,
            ..Default::default()
        };
        let vfs_name = format!("local_cmp_{}", std::process::id());
        let vfs = TieredVfs::new(config).expect("local VFS with compression");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", []).unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'compressed')", []).unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    // Cold reopen with compression
    {
        let config = TieredConfig {
            storage_backend: StorageBackend::Local,
            cache_dir: dir.path().to_path_buf(),
            cache_compression: true,
            cache_compression_level: 3,
            ..Default::default()
        };
        let vfs_name = format!("local_cmp2_{}", std::process::id());
        let vfs = TieredVfs::new(config).expect("reopen with compression");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        let val: String = conn.query_row("SELECT val FROM t WHERE id = 1", [], |row| row.get(0)).unwrap();
        assert_eq!(val, "compressed");
    }
}

/// Local VFS with multiple tables and schema changes.
#[test]
fn test_local_vfs_schema_changes() {
    let dir = TempDir::new().unwrap();
    let config = TieredConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs_name = format!("local_schema_{}", std::process::id());
    let vfs = TieredVfs::new(config).expect("local VFS");
    crate::tiered::register(&vfs_name, vfs).expect("register");

    let db_path = format!("file:test.db?vfs={}", vfs_name);
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();

    // Multiple tables
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", []).unwrap();
    conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT)", []).unwrap();
    conn.execute("CREATE INDEX idx_posts_user ON posts(user_id)", []).unwrap();

    // Insert data
    conn.execute("INSERT INTO users VALUES (1, 'alice')", []).unwrap();
    conn.execute("INSERT INTO users VALUES (2, 'bob')", []).unwrap();
    conn.execute("INSERT INTO posts VALUES (1, 1, 'hello world')", []).unwrap();
    conn.execute("INSERT INTO posts VALUES (2, 1, 'second post')", []).unwrap();
    conn.execute("INSERT INTO posts VALUES (3, 2, 'bob post')", []).unwrap();

    // Query with index
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM posts WHERE user_id = 1", [], |row| row.get(0)
    ).unwrap();
    assert_eq!(count, 2);

    // Alter table
    conn.execute("ALTER TABLE users ADD COLUMN email TEXT", []).unwrap();
    conn.execute("UPDATE users SET email = 'alice@example.com' WHERE id = 1", []).unwrap();

    let email: String = conn.query_row(
        "SELECT email FROM users WHERE id = 1", [], |row| row.get(0)
    ).unwrap();
    assert_eq!(email, "alice@example.com");
}

/// Local VFS gc() and flush_to_s3() return appropriate errors.
#[test]
fn test_local_vfs_cloud_methods_error() {
    let dir = TempDir::new().unwrap();
    let config = TieredConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs = TieredVfs::new(config).expect("local VFS");

    // Cloud-only methods should return Unsupported errors, not panic
    let gc_err = vfs.gc();
    assert!(gc_err.is_err());

    let flush_err = vfs.flush_to_s3();
    assert!(flush_err.is_err());

    let destroy_err = vfs.destroy_s3();
    assert!(destroy_err.is_err());
}

/// Local VFS s3_counters return zeros.
#[test]
fn test_local_vfs_s3_counters_zero() {
    let dir = TempDir::new().unwrap();
    let config = TieredConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs = TieredVfs::new(config).expect("local VFS");

    assert_eq!(vfs.s3_counters(), (0, 0));
    assert_eq!(vfs.reset_s3_counters(), (0, 0));
}

/// Local VFS data survives checkpoint + cold reopen.
#[test]
fn test_local_vfs_checkpoint_reopen() {
    let dir = TempDir::new().unwrap();

    // Write + checkpoint
    {
        let config = TieredConfig {
            storage_backend: StorageBackend::Local,
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("local_ck1_{}", std::process::id());
        let vfs = TieredVfs::new(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", []).unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'persisted')", []).unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    // Cold reopen
    {
        let config = TieredConfig {
            storage_backend: StorageBackend::Local,
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("local_ck2_{}", std::process::id());
        let vfs = TieredVfs::new(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");

        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 1", [], |row| row.get(0))
            .unwrap();
        assert_eq!(val, "persisted");
    }
}
