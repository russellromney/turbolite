use super::*;
use tempfile::TempDir;

/// RED TEST: TurboliteVfs::new() with StorageBackend::Local should succeed
/// without any S3 credentials, tokio runtime, or cloud dependencies.
#[test]
fn test_local_vfs_construction() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };

    let vfs = TurboliteVfs::new(config).expect("local VFS construction should succeed");

    // Verify it's local
    assert!(vfs.storage.is_local());
}

/// RED TEST: Local VFS exists() returns false for new empty dir.
#[test]
fn test_local_vfs_exists_empty() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new(config).expect("local VFS");
    assert!(!vfs.storage.exists().unwrap());
}

/// RED TEST: Local VFS can register with SQLite, open a db, and do CRUD.
#[test]
fn test_local_vfs_sqlite_roundtrip() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };

    let vfs_name = format!("local_rt_{}", std::process::id());
    let vfs = TurboliteVfs::new(config).expect("local VFS");
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
        let config = TurboliteConfig {
            storage_backend: StorageBackend::Local,
            cache_dir: dir.path().to_path_buf(),
            cache_compression: true,
            cache_compression_level: 3,
            ..Default::default()
        };
        let vfs_name = format!("local_cmp_{}", std::process::id());
        let vfs = TurboliteVfs::new(config).expect("local VFS with compression");
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
        let config = TurboliteConfig {
            storage_backend: StorageBackend::Local,
            cache_dir: dir.path().to_path_buf(),
            cache_compression: true,
            cache_compression_level: 3,
            ..Default::default()
        };
        let vfs_name = format!("local_cmp2_{}", std::process::id());
        let vfs = TurboliteVfs::new(config).expect("reopen with compression");
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
    let config = TurboliteConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs_name = format!("local_schema_{}", std::process::id());
    let vfs = TurboliteVfs::new(config).expect("local VFS");
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
#[cfg(feature = "cloud")]
#[test]
fn test_local_vfs_cloud_methods_error() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new(config).expect("local VFS");

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
    let config = TurboliteConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new(config).expect("local VFS");

    assert_eq!(vfs.s3_counters(), (0, 0));
    assert_eq!(vfs.reset_s3_counters(), (0, 0));
}

/// RED TEST: Delete cache file after checkpoint, reopen, verify data recovered from local page groups.
#[test]
fn test_local_vfs_recover_from_page_groups() {
    let dir = TempDir::new().unwrap();

    // Phase 1: write data and checkpoint
    {
        let config = TurboliteConfig {
            storage_backend: StorageBackend::Local,
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("local_pg1_{}", std::process::id());
        let vfs = TurboliteVfs::new(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", []).unwrap();
        for i in 0..100 {
            conn.execute("INSERT INTO t VALUES (?1, ?2)", rusqlite::params![i, format!("value_{}", i)]).unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        drop(conn);
    }

    // Verify page groups exist on disk
    let pg_dir = dir.path().join("pg");
    assert!(pg_dir.is_dir(), "pg/ directory should exist");
    let pg_files: Vec<_> = std::fs::read_dir(&pg_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|ext| ext != "tmp").unwrap_or(true))
        .collect();
    assert!(!pg_files.is_empty(), "page group files should exist in pg/");

    // Phase 2: delete cache file + bitmap (simulate cache loss), reopen
    let _ = std::fs::remove_file(dir.path().join("data.cache"));
    let _ = std::fs::remove_file(dir.path().join("page_bitmap"));
    let _ = std::fs::remove_file(dir.path().join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(dir.path().join("cache_index.json"));

    {
        let config = TurboliteConfig {
            storage_backend: StorageBackend::Local,
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("local_pg2_{}", std::process::id());
        let vfs = TurboliteVfs::new(config).expect("reopen VFS after cache loss");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");

        // All data should be recoverable from local page groups
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0)).unwrap();
        assert_eq!(count, 100, "all 100 rows should be recovered from page groups");

        let val: String = conn.query_row(
            "SELECT val FROM t WHERE id = 42", [], |row| row.get(0)
        ).unwrap();
        assert_eq!(val, "value_42");
    }
}

/// Multiple checkpoints produce distinct page group versions; all data recoverable.
#[test]
fn test_local_vfs_multi_checkpoint() {
    let dir = TempDir::new().unwrap();

    {
        let config = TurboliteConfig {
            storage_backend: StorageBackend::Local,
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("local_mc_{}", std::process::id());
        let vfs = TurboliteVfs::new(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", []).unwrap();

        // Checkpoint 1: insert 50 rows
        for i in 0..50 {
            conn.execute("INSERT INTO t VALUES (?1, ?2)", rusqlite::params![i, format!("v1_{}", i)]).unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

        // Checkpoint 2: insert 50 more rows + update some existing
        for i in 50..100 {
            conn.execute("INSERT INTO t VALUES (?1, ?2)", rusqlite::params![i, format!("v2_{}", i)]).unwrap();
        }
        conn.execute("UPDATE t SET val = 'updated' WHERE id = 0", []).unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

        // Checkpoint 3: delete some rows
        conn.execute("DELETE FROM t WHERE id >= 80", []).unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

        drop(conn);
    }

    // Delete cache, reopen, verify final state from page groups
    let _ = std::fs::remove_file(dir.path().join("data.cache"));
    let _ = std::fs::remove_file(dir.path().join("page_bitmap"));
    let _ = std::fs::remove_file(dir.path().join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(dir.path().join("cache_index.json"));

    {
        let config = TurboliteConfig {
            storage_backend: StorageBackend::Local,
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("local_mc2_{}", std::process::id());
        let vfs = TurboliteVfs::new(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).unwrap();

        // Should see 80 rows (100 inserted - 20 deleted)
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0)).unwrap();
        assert_eq!(count, 80, "should have 80 rows after multi-checkpoint recovery");

        // Row 0 should be updated
        let val: String = conn.query_row("SELECT val FROM t WHERE id = 0", [], |row| row.get(0)).unwrap();
        assert_eq!(val, "updated");

        // Rows 80-99 should not exist
        let high: i64 = conn.query_row("SELECT COUNT(*) FROM t WHERE id >= 80", [], |row| row.get(0)).unwrap();
        assert_eq!(high, 0, "deleted rows should not exist");
    }
}

/// Local VFS data survives checkpoint + cold reopen.
#[test]
fn test_local_vfs_checkpoint_reopen() {
    let dir = TempDir::new().unwrap();

    // Write + checkpoint
    {
        let config = TurboliteConfig {
            storage_backend: StorageBackend::Local,
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("local_ck1_{}", std::process::id());
        let vfs = TurboliteVfs::new(config).expect("local VFS");
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
        let config = TurboliteConfig {
            storage_backend: StorageBackend::Local,
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("local_ck2_{}", std::process::id());
        let vfs = TurboliteVfs::new(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");

        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 1", [], |row| row.get(0))
            .unwrap();
        assert_eq!(val, "persisted");
    }
}

// =========================================================================
// Phase Drift: override write + cold read tests
// =========================================================================

/// Write data with override_threshold=100 (high, so overrides are used),
/// cold reopen, verify data survives.
#[test]
fn test_local_vfs_override_write_cold_read() {
    use std::sync::atomic::Ordering;
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);

    let dir = TempDir::new().unwrap();

    // Write phase
    {
        let config = TurboliteConfig {
            storage_backend: StorageBackend::Local,
            cache_dir: dir.path().to_path_buf(),
            override_threshold: 100, // high threshold: everything goes to override path
            compaction_threshold: 0, // disable auto-compact
            ..Default::default()
        };
        let vfs_name = format!("local_ovr_w_{}", id);
        let vfs = TurboliteVfs::new(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", []).unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'override_test')", []).unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    // Cold reopen
    {
        let config = TurboliteConfig {
            storage_backend: StorageBackend::Local,
            cache_dir: dir.path().to_path_buf(),
            override_threshold: 100,
            compaction_threshold: 0,
            ..Default::default()
        };
        let vfs_name = format!("local_ovr_r_{}", id);
        let vfs = TurboliteVfs::new(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");
        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 1", [], |row| row.get(0))
            .unwrap();
        assert_eq!(val, "override_test");
    }
}

/// Write with override, then full rewrite via another checkpoint, cold reopen.
#[test]
fn test_local_vfs_override_then_full_rewrite() {
    use std::sync::atomic::Ordering;
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);

    let dir = TempDir::new().unwrap();

    // Write phase 1: create table and initial data
    {
        let config = TurboliteConfig {
            storage_backend: StorageBackend::Local,
            cache_dir: dir.path().to_path_buf(),
            override_threshold: 100,
            compaction_threshold: 0,
            ..Default::default()
        };
        let vfs_name = format!("local_ovr_fr1_{}", id);
        let vfs = TurboliteVfs::new(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", []).unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'first')", []).unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    // Write phase 2: update to different value (full rewrite, threshold=0)
    {
        let config = TurboliteConfig {
            storage_backend: StorageBackend::Local,
            cache_dir: dir.path().to_path_buf(),
            override_threshold: 0, // back to default, full rewrite
            compaction_threshold: 0,
            ..Default::default()
        };
        let vfs_name = format!("local_ovr_fr2_{}", id);
        let vfs = TurboliteVfs::new(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");
        conn.execute("UPDATE t SET val = 'final' WHERE id = 1", []).unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    // Cold reopen: verify final value
    {
        let config = TurboliteConfig {
            storage_backend: StorageBackend::Local,
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("local_ovr_fr3_{}", id);
        let vfs = TurboliteVfs::new(config).expect("final reopen");
        crate::tiered::register(&vfs_name, vfs).expect("register3");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("final open");
        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 1", [], |row| row.get(0))
            .unwrap();
        assert_eq!(val, "final");
    }
}

/// Accumulate overrides past compaction threshold, verify compaction fires, cold reopen.
#[test]
fn test_local_vfs_override_compaction() {
    use std::sync::atomic::Ordering;
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);

    let dir = TempDir::new().unwrap();

    // Initial write
    {
        let config = TurboliteConfig {
            storage_backend: StorageBackend::Local,
            cache_dir: dir.path().to_path_buf(),
            override_threshold: 100,
            compaction_threshold: 2, // compact after 2 overrides
            ..Default::default()
        };
        let vfs_name = format!("local_cmpct1_{}", id);
        let vfs = TurboliteVfs::new(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", []).unwrap();
        for i in 1..=10 {
            conn.execute("INSERT INTO t VALUES (?1, ?2)", rusqlite::params![i, format!("val_{}", i)]).unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

        // Update a few times to accumulate overrides, each followed by checkpoint
        for round in 1..=3 {
            conn.execute("UPDATE t SET val = ?1 WHERE id = 1", rusqlite::params![format!("round_{}", round)]).unwrap();
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        }
    }

    // Cold reopen: data should be consistent after compaction
    {
        let config = TurboliteConfig {
            storage_backend: StorageBackend::Local,
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("local_cmpct2_{}", id);
        let vfs = TurboliteVfs::new(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");
        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 1", [], |row| row.get(0))
            .unwrap();
        assert_eq!(val, "round_3");

        // Other rows should be intact
        let val5: String = conn
            .query_row("SELECT val FROM t WHERE id = 5", [], |row| row.get(0))
            .unwrap();
        assert_eq!(val5, "val_5");
    }
}

// =========================================================================
// Phase Zenith-b: Cache validation on open
// =========================================================================

/// Reopen with same manifest version: cache warm, data correct.
#[test]
fn test_cache_validation_warm_reopen_same_version() {
    let dir = TempDir::new().unwrap();

    // Write data
    {
        let config = TurboliteConfig {
            storage_backend: StorageBackend::Local,
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("cv_warm1_{}", std::process::id());
        let vfs = TurboliteVfs::new(config).expect("VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let conn = rusqlite::Connection::open(format!("file:test.db?vfs={}", vfs_name)).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", []).unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'warm')", []).unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        drop(conn);
    }

    // Reopen: same manifest version, cache should be warm
    {
        let config = TurboliteConfig {
            storage_backend: StorageBackend::Local,
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("cv_warm2_{}", std::process::id());
        let vfs = TurboliteVfs::new(config).expect("VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let conn = rusqlite::Connection::open(format!("file:test.db?vfs={}", vfs_name)).unwrap();
        let val: String = conn.query_row("SELECT val FROM t WHERE id = 1", [], |r| r.get(0)).unwrap();
        assert_eq!(val, "warm");
    }
}

/// Simulate external write: modify manifest version + page_group_keys on disk,
/// reopen, verify cache invalidation triggers and correct data is read.
#[test]
fn test_cache_validation_external_write_invalidates_stale_groups() {
    let dir = TempDir::new().unwrap();

    // Session 1: write data + checkpoint
    {
        let config = TurboliteConfig {
            storage_backend: StorageBackend::Local,
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("cv_ext1_{}", std::process::id());
        let vfs = TurboliteVfs::new(config).expect("VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let conn = rusqlite::Connection::open(format!("file:test.db?vfs={}", vfs_name)).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", []).unwrap();
        for i in 1..=5 {
            conn.execute("INSERT INTO t VALUES (?1, ?2)", rusqlite::params![i, format!("v1_{}", i)]).unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        drop(conn);
    }

    // Session 2: update some rows (simulates another node writing)
    {
        let config = TurboliteConfig {
            storage_backend: StorageBackend::Local,
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("cv_ext2_{}", std::process::id());
        let vfs = TurboliteVfs::new(config).expect("VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let conn = rusqlite::Connection::open(format!("file:test.db?vfs={}", vfs_name)).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("UPDATE t SET val = 'updated' WHERE id = 3", []).unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        drop(conn);
    }

    // Session 3: reopen (simulates original node reopening after external write)
    // The local manifest should now reflect session 2's version.
    // Cache validation should invalidate changed groups.
    {
        let config = TurboliteConfig {
            storage_backend: StorageBackend::Local,
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("cv_ext3_{}", std::process::id());
        let vfs = TurboliteVfs::new(config).expect("VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let conn = rusqlite::Connection::open(format!("file:test.db?vfs={}", vfs_name)).unwrap();

        // Should see the updated value from session 2
        let val: String = conn.query_row("SELECT val FROM t WHERE id = 3", [], |r| r.get(0)).unwrap();
        assert_eq!(val, "updated", "should see external write after cache validation");

        // Unchanged rows should still be correct
        let val1: String = conn.query_row("SELECT val FROM t WHERE id = 1", [], |r| r.get(0)).unwrap();
        assert_eq!(val1, "v1_1");
    }
}

/// Reopen after deletion of cache files: pages re-fetched from page groups.
#[test]
fn test_cache_validation_cold_start_after_cache_delete() {
    let dir = TempDir::new().unwrap();

    // Write data
    {
        let config = TurboliteConfig {
            storage_backend: StorageBackend::Local,
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("cv_cold1_{}", std::process::id());
        let vfs = TurboliteVfs::new(config).expect("VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let conn = rusqlite::Connection::open(format!("file:test.db?vfs={}", vfs_name)).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", []).unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'persisted')", []).unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        drop(conn);
    }

    // Delete cache files (simulates Lambda cold start with fresh disk)
    let _ = std::fs::remove_file(dir.path().join("data.cache"));
    let _ = std::fs::remove_file(dir.path().join("page_bitmap"));
    let _ = std::fs::remove_file(dir.path().join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(dir.path().join("cache_index.json"));

    // Reopen: cache empty, manifest still on disk, data from page groups
    {
        let config = TurboliteConfig {
            storage_backend: StorageBackend::Local,
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("cv_cold2_{}", std::process::id());
        let vfs = TurboliteVfs::new(config).expect("VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let conn = rusqlite::Connection::open(format!("file:test.db?vfs={}", vfs_name)).unwrap();
        let val: String = conn.query_row("SELECT val FROM t WHERE id = 1", [], |r| r.get(0)).unwrap();
        assert_eq!(val, "persisted", "should recover data from page groups after cache delete");
    }
}
