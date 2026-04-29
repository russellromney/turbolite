use super::*;
use tempfile::TempDir;

/// RED TEST: TurboliteVfs::new_local() with StorageBackend::Local should succeed
/// without any S3 credentials, tokio runtime, or cloud dependencies.
#[test]
fn test_local_vfs_construction() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };

    let vfs = TurboliteVfs::new_local(config).expect("local VFS construction should succeed");

    // Verify it's local
    assert!(vfs.is_local);
}

/// Phase Cirrus e: shared_state() is total for local VFSes. The old
/// "expect(shared_state)" panic for non-S3 backends is gone; the field
/// types carry the invariant.
#[test]
fn test_local_vfs_shared_state_is_total() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new_local(config).expect("local VFS");
    let state = vfs.shared_state();
    assert!(state.is_local);
}

/// RED TEST: Local VFS exists() returns false for new empty dir.
#[test]
fn test_local_vfs_exists_empty() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new_local(config).expect("local VFS");
    // Empty dir: no manifest in the backend.
    use sqlite_vfs::Vfs;
    assert!(!vfs.exists("main.db").unwrap());
}

/// RED TEST: Local VFS can register with SQLite, open a db, and do CRUD.
#[test]
fn test_local_vfs_sqlite_roundtrip() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };

    let vfs_name = format!("local_rt_{}", std::process::id());
    let vfs = TurboliteVfs::new_local(config).expect("local VFS");
    crate::tiered::register(&vfs_name, vfs).expect("register");

    let db_path = format!("file:test.db?vfs={}", vfs_name);
    let conn = rusqlite::Connection::open(&db_path).expect("open");
    conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'hello')", [])
        .unwrap();

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
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                compression: true,
                compression_level: 3,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("local_cmp_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("local VFS with compression");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'compressed')", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
    }

    // Cold reopen with compression
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                compression: true,
                compression_level: 3,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("local_cmp2_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("reopen with compression");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 1", [], |row| row.get(0))
            .unwrap();
        assert_eq!(val, "compressed");
    }
}

/// Local VFS with multiple tables and schema changes.
#[test]
fn test_local_vfs_schema_changes() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs_name = format!("local_schema_{}", std::process::id());
    let vfs = TurboliteVfs::new_local(config).expect("local VFS");
    crate::tiered::register(&vfs_name, vfs).expect("register");

    let db_path = format!("file:test.db?vfs={}", vfs_name);
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();

    // Multiple tables
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])
        .unwrap();
    conn.execute(
        "CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT)",
        [],
    )
    .unwrap();
    conn.execute("CREATE INDEX idx_posts_user ON posts(user_id)", [])
        .unwrap();

    // Insert data
    conn.execute("INSERT INTO users VALUES (1, 'alice')", [])
        .unwrap();
    conn.execute("INSERT INTO users VALUES (2, 'bob')", [])
        .unwrap();
    conn.execute("INSERT INTO posts VALUES (1, 1, 'hello world')", [])
        .unwrap();
    conn.execute("INSERT INTO posts VALUES (2, 1, 'second post')", [])
        .unwrap();
    conn.execute("INSERT INTO posts VALUES (3, 2, 'bob post')", [])
        .unwrap();

    // Query with index
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM posts WHERE user_id = 1", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(count, 2);

    // Alter table
    conn.execute("ALTER TABLE users ADD COLUMN email TEXT", [])
        .unwrap();
    conn.execute(
        "UPDATE users SET email = 'alice@example.com' WHERE id = 1",
        [],
    )
    .unwrap();

    let email: String = conn
        .query_row("SELECT email FROM users WHERE id = 1", [], |row| row.get(0))
        .unwrap();
    assert_eq!(email, "alice@example.com");
}

/// Local VFS gc() and destroy_s3() return appropriate errors.
/// flush_to_storage() is a valid no-op on local VFS (returns Ok).
#[cfg(feature = "s3")]
#[test]
fn test_local_vfs_cloud_methods_error() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new_local(config).expect("local VFS");

    // Cloud-only methods should return Unsupported errors, not panic
    let gc_err = vfs.gc();
    assert!(gc_err.is_err());

    // flush_to_storage uses StorageClient abstraction, works on all backends
    let flush_result = vfs.flush_to_storage();
    assert!(
        flush_result.is_ok(),
        "flush_to_storage should be a no-op on local VFS"
    );

    let destroy_err = vfs.destroy_s3();
    assert!(destroy_err.is_err());
}

/// Local VFS can be constructed; I/O counters (previously exposed via
/// `s3_counters` / `reset_s3_counters`) now live on concrete backend
/// impls, not the generic VFS.
#[test]
fn test_local_vfs_smoke_construct() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let _vfs = TurboliteVfs::new_local(config).expect("local VFS");
}

/// RED TEST: Delete cache file after checkpoint, reopen, verify data recovered from local page groups.
#[test]
fn test_local_vfs_recover_from_page_groups() {
    let dir = TempDir::new().unwrap();

    // Phase 1: write data and checkpoint
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("local_pg1_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();
        for i in 0..100 {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("value_{}", i)],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        drop(conn);
    }

    // Verify page groups exist on disk
    let pg_dir = dir.path().join("p/d");
    assert!(pg_dir.is_dir(), "p/d/ directory should exist");
    let pg_files: Vec<_> = std::fs::read_dir(&pg_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|ext| ext != "tmp").unwrap_or(true))
        .collect();
    assert!(
        !pg_files.is_empty(),
        "page group files should exist in p/d/"
    );

    // Phase 2: delete cache file + bitmap (simulate cache loss), reopen
    let _ = std::fs::remove_file(dir.path().join("data.cache"));
    let _ = std::fs::remove_file(dir.path().join("page_bitmap"));
    let _ = std::fs::remove_file(dir.path().join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(dir.path().join("cache_index.json"));

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("local_pg2_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS after cache loss");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");

        // All data should be recoverable from local page groups
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            count, 100,
            "all 100 rows should be recovered from page groups"
        );

        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 42", [], |row| row.get(0))
            .unwrap();
        assert_eq!(val, "value_42");
    }
}

/// Multiple checkpoints produce distinct page group versions; all data recoverable.
#[test]
fn test_local_vfs_multi_checkpoint() {
    let dir = TempDir::new().unwrap();

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("local_mc_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();

        // Checkpoint 1: insert 50 rows
        for i in 0..50 {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("v1_{}", i)],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        // Checkpoint 2: insert 50 more rows + update some existing
        for i in 50..100 {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("v2_{}", i)],
            )
            .unwrap();
        }
        conn.execute("UPDATE t SET val = 'updated' WHERE id = 0", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        // Checkpoint 3: delete some rows
        conn.execute("DELETE FROM t WHERE id >= 80", []).unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        drop(conn);
    }

    // Delete cache, reopen, verify final state from page groups
    let _ = std::fs::remove_file(dir.path().join("data.cache"));
    let _ = std::fs::remove_file(dir.path().join("page_bitmap"));
    let _ = std::fs::remove_file(dir.path().join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(dir.path().join("cache_index.json"));

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("local_mc2_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).unwrap();

        // Should see 80 rows (100 inserted - 20 deleted)
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            count, 80,
            "should have 80 rows after multi-checkpoint recovery"
        );

        // Row 0 should be updated
        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 0", [], |row| row.get(0))
            .unwrap();
        assert_eq!(val, "updated");

        // Rows 80-99 should not exist
        let high: i64 = conn
            .query_row("SELECT COUNT(*) FROM t WHERE id >= 80", [], |row| {
                row.get(0)
            })
            .unwrap();
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
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("local_ck1_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'persisted')", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
    }

    // Cold reopen
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("local_ck2_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
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
// Override write + cold read tests
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
            cache_dir: dir.path().to_path_buf(),
            // high threshold: everything goes to override path
            // disable auto-compact
            cache: CacheConfig {
                override_threshold: 100,
                compaction_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("local_ovr_w_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'override_test')", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
    }

    // Cold reopen
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                override_threshold: 100,
                compaction_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("local_ovr_r_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
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
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                override_threshold: 100,
                compaction_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("local_ovr_fr1_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'first')", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
    }

    // Write phase 2: update to different value (full rewrite, threshold=0)
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            // back to default, full rewrite
            cache: CacheConfig {
                override_threshold: 0,
                compaction_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("local_ovr_fr2_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");
        conn.execute("UPDATE t SET val = 'final' WHERE id = 1", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
    }

    // Cold reopen: verify final value
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("local_ovr_fr3_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("final reopen");
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
            cache_dir: dir.path().to_path_buf(),
            // compact after 2 overrides
            cache: CacheConfig {
                override_threshold: 100,
                compaction_threshold: 2,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("local_cmpct1_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();
        for i in 1..=10 {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("val_{}", i)],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        // Update a few times to accumulate overrides, each followed by checkpoint
        for round in 1..=3 {
            conn.execute(
                "UPDATE t SET val = ?1 WHERE id = 1",
                rusqlite::params![format!("round_{}", round)],
            )
            .unwrap();
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
                .unwrap();
        }
    }

    // Cold reopen: data should be consistent after compaction
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("local_cmpct2_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
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
// Cache validation on open
// =========================================================================

/// Reopen with same manifest version: cache warm, data correct.
#[test]
fn test_cache_validation_warm_reopen_same_version() {
    let dir = TempDir::new().unwrap();

    // Write data
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("cv_warm1_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let conn = rusqlite::Connection::open(format!("file:test.db?vfs={}", vfs_name)).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'warm')", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        drop(conn);
    }

    // Reopen: same manifest version, cache should be warm
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("cv_warm2_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let conn = rusqlite::Connection::open(format!("file:test.db?vfs={}", vfs_name)).unwrap();
        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 1", [], |r| r.get(0))
            .unwrap();
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
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("cv_ext1_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let conn = rusqlite::Connection::open(format!("file:test.db?vfs={}", vfs_name)).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();
        for i in 1..=5 {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("v1_{}", i)],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        drop(conn);
    }

    // Session 2: update some rows (simulates another node writing)
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("cv_ext2_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let conn = rusqlite::Connection::open(format!("file:test.db?vfs={}", vfs_name)).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("UPDATE t SET val = 'updated' WHERE id = 3", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        drop(conn);
    }

    // Session 3: reopen (simulates original node reopening after external write)
    // The local manifest should now reflect session 2's version.
    // Cache validation should invalidate changed groups.
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("cv_ext3_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let conn = rusqlite::Connection::open(format!("file:test.db?vfs={}", vfs_name)).unwrap();

        // Should see the updated value from session 2
        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 3", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            val, "updated",
            "should see external write after cache validation"
        );

        // Unchanged rows should still be correct
        let val1: String = conn
            .query_row("SELECT val FROM t WHERE id = 1", [], |r| r.get(0))
            .unwrap();
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
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("cv_cold1_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let conn = rusqlite::Connection::open(format!("file:test.db?vfs={}", vfs_name)).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'persisted')", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
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
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("cv_cold2_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let conn = rusqlite::Connection::open(format!("file:test.db?vfs={}", vfs_name)).unwrap();
        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            val, "persisted",
            "should recover data from page groups after cache delete"
        );
    }
}

// ===== Transaction Rollback Handling =====

/// After a constraint violation (failed INSERT), subsequent reads must see
/// the correct data, not stale dirty pages from the rolled-back transaction.
#[test]
fn test_constraint_violation_does_not_corrupt_reads() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs_name = format!("zenith_c_constraint_{}", std::process::id());
    let vfs = TurboliteVfs::new_local(config).expect("VFS");
    crate::tiered::register(&vfs_name, vfs).expect("register");

    let conn = rusqlite::Connection::open(format!("file:test.db?vfs={}", vfs_name)).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT UNIQUE)",
        [],
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'original')", [])
        .unwrap();

    // Force a checkpoint so the data is committed to page groups
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();

    // Try to insert a duplicate value (should fail with UNIQUE constraint)
    let result = conn.execute("INSERT INTO t VALUES (2, 'original')", []);
    assert!(result.is_err(), "duplicate insert should fail");

    // Read should still see only the original row, not corrupted data
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1, "should have exactly 1 row after failed insert");

    let val: String = conn
        .query_row("SELECT val FROM t WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(val, "original");

    // Can still write after the failed transaction
    conn.execute("INSERT INTO t VALUES (2, 'second')", [])
        .unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 2);
}

/// Explicit BEGIN + rollback (via DROP or error) should not leave stale dirty pages.
#[test]
fn test_explicit_transaction_rollback() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs_name = format!("zenith_c_rollback_{}", std::process::id());
    let vfs = TurboliteVfs::new_local(config).expect("VFS");
    crate::tiered::register(&vfs_name, vfs).expect("register");

    let conn = rusqlite::Connection::open(format!("file:test.db?vfs={}", vfs_name)).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'committed')", [])
        .unwrap();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();

    // Begin a transaction, write something, then rollback
    conn.execute_batch("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'will_rollback')", [])
        .unwrap();
    conn.execute_batch("ROLLBACK").unwrap();

    // After rollback, we should see only the committed row
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1, "rolled-back row should not be visible");

    let val: String = conn
        .query_row("SELECT val FROM t WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(val, "committed");
}

/// Multiple constraint violations in a row should not accumulate stale pages.
#[test]
fn test_repeated_constraint_violations() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs_name = format!("zenith_c_repeated_{}", std::process::id());
    let vfs = TurboliteVfs::new_local(config).expect("VFS");
    crate::tiered::register(&vfs_name, vfs).expect("register");

    let conn = rusqlite::Connection::open(format!("file:test.db?vfs={}", vfs_name)).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT UNIQUE)",
        [],
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'one')", []).unwrap();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();

    // Several failing inserts
    for i in 0..5 {
        let result = conn.execute(&format!("INSERT INTO t VALUES ({}, 'one')", i + 10), []);
        assert!(result.is_err(), "duplicate should fail on iteration {}", i);
    }

    // Data should still be correct
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);

    // Successful write after repeated failures
    conn.execute("INSERT INTO t VALUES (2, 'two')", []).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 2);
}

// ===== WAL Migration Path =====

/// turbolite_migrate_to_s3_primary checkpoints WAL and prepares for journal_mode=OFF.
/// The turbolite VFS creates WAL stub files on open (unless S3Primary mode), so
/// the full migration from WAL to OFF requires:
/// 1. Call migrate (checkpoints WAL)
/// 2. Close the connection
/// 3. Reopen with a VFS configured for S3Primary or non-WAL mode
#[test]
fn test_migrate_to_s3_primary_from_wal() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs_name = format!("zenith_d_migrate_{}", std::process::id());
    let vfs = TurboliteVfs::new_local(config).expect("VFS");
    crate::tiered::register(&vfs_name, vfs).expect("register");

    let uri = format!("file:test.db?vfs={}", vfs_name);

    // Step 1: create data in WAL mode, then migrate (checkpoints WAL)
    {
        let conn = rusqlite::Connection::open(&uri).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'before_migrate')", [])
            .unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'second')", [])
            .unwrap();

        // Migration returns Err when PRAGMA journal_mode=OFF fails (expected with
        // turbolite VFS), but the WAL checkpoint has already completed successfully.
        let result = crate::tiered::turbolite_migrate_to_s3_primary(&conn);
        assert!(
            result.is_err(),
            "should return Err when PRAGMA journal_mode=OFF fails in WAL mode"
        );
    }

    // Step 2: verify data survived the checkpoint by reopening (still WAL mode
    // in local VFS, but all data is in the main database file, not the WAL)
    {
        let conn = rusqlite::Connection::open(&uri).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 2, "all rows should survive WAL checkpoint");

        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "before_migrate");
    }
}

/// Migrating a database that is already in DELETE mode switches to OFF directly.
#[test]
fn test_migrate_from_delete_to_off() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs_name = format!("zenith_d_delete_{}", std::process::id());
    let vfs = TurboliteVfs::new_local(config).expect("VFS");
    crate::tiered::register(&vfs_name, vfs).expect("register");

    let uri = format!("file:test.db?vfs={}", vfs_name);
    let conn = rusqlite::Connection::open(&uri).unwrap();
    // Start with DELETE mode (not WAL), then migrate to OFF
    conn.execute_batch("PRAGMA journal_mode=DELETE").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", [])
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1)", []).unwrap();

    // Migrate should succeed and switch to OFF
    crate::tiered::turbolite_migrate_to_s3_primary(&conn).unwrap();

    let mode: String = conn
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert!(mode == "off" || mode == "memory", "got: {}", mode);

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);
}

/// Migration is a no-op when already in journal_mode=OFF.
#[test]
fn test_migrate_already_off() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs_name = format!("zenith_d_already_{}", std::process::id());
    let vfs = TurboliteVfs::new_local(config).expect("VFS");
    crate::tiered::register(&vfs_name, vfs).expect("register");

    let uri = format!("file:test.db?vfs={}", vfs_name);
    let conn = rusqlite::Connection::open(&uri).unwrap();
    conn.execute_batch("PRAGMA journal_mode=DELETE").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", [])
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1)", []).unwrap();

    // First call: switches to OFF
    crate::tiered::turbolite_migrate_to_s3_primary(&conn).unwrap();
    let mode: String = conn
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert!(mode == "off" || mode == "memory", "got: {}", mode);

    // Second call: should be a no-op
    crate::tiered::turbolite_migrate_to_s3_primary(&conn).unwrap();
    let mode2: String = conn
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode, mode2);
}

/// Migration from WAL preserves data across many rows.
#[test]
fn test_migrate_preserves_large_dataset() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs_name = format!("zenith_d_large_{}", std::process::id());
    let vfs = TurboliteVfs::new_local(config).expect("VFS");
    crate::tiered::register(&vfs_name, vfs).expect("register");

    let uri = format!("file:test.db?vfs={}", vfs_name);
    let row_count = 500i64;

    // Create data in WAL mode and migrate
    {
        let conn = rusqlite::Connection::open(&uri).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();

        for i in 0..row_count {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("row_{}", i)],
            )
            .unwrap();
        }

        // Migration returns Err when PRAGMA journal_mode=OFF fails (expected with
        // turbolite VFS), but the WAL checkpoint has already completed successfully.
        let result = crate::tiered::turbolite_migrate_to_s3_primary(&conn);
        assert!(
            result.is_err(),
            "should return Err when PRAGMA journal_mode=OFF fails in WAL mode"
        );
    }

    // Verify all data survived
    {
        let conn = rusqlite::Connection::open(&uri).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, row_count);

        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 250", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "row_250");

        // Can still write after migration
        conn.execute(
            "INSERT INTO t VALUES (?1, 'new')",
            rusqlite::params![row_count],
        )
        .unwrap();
        let new_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(new_count, row_count + 1);
    }
}

// =========================================================================
// Stress tests
// =========================================================================

/// Large database: 10k rows across many page groups, checkpoint, cold reopen, verify all rows.
#[test]
fn test_stress_large_database_10k_rows() {
    let dir = TempDir::new().unwrap();
    let row_count = 10_000i64;

    // Write phase
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            // small groups to exercise many page groups
            cache: CacheConfig {
                pages_per_group: 4,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("stress_10k_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, grp INTEGER, val TEXT)",
            [],
        )
        .unwrap();

        // Insert in a single transaction for speed
        conn.execute_batch("BEGIN").unwrap();
        for i in 0..row_count {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2, ?3)",
                rusqlite::params![i, i % 100, format!("row_{}", i)],
            )
            .unwrap();
        }
        conn.execute_batch("COMMIT").unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        drop(conn);
    }

    // Cold reopen: delete cache files
    let _ = std::fs::remove_file(dir.path().join("data.cache"));
    let _ = std::fs::remove_file(dir.path().join("page_bitmap"));
    let _ = std::fs::remove_file(dir.path().join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(dir.path().join("cache_index.json"));

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                pages_per_group: 4,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("stress_10k_r_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, row_count, "all 10k rows should survive cold reopen");

        // Spot-check a few rows
        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 0", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "row_0");

        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 5000", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "row_5000");

        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 9999", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "row_9999");

        // Group query
        let grp_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t WHERE grp = 42", [], |r| r.get(0))
            .unwrap();
        assert_eq!(grp_count, 100, "each of 100 groups should have 100 rows");
    }
}

/// Many sequential overrides then compaction: verify compaction fires and data is correct.
#[test]
fn test_stress_many_overrides_compaction() {
    use std::sync::atomic::Ordering;
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);

    let dir = TempDir::new().unwrap();

    // Write phase: initial data + 10 sequential small updates with checkpoints
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            // compact after 5 overrides
            cache: CacheConfig {
                override_threshold: 100,
                compaction_threshold: 5,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("stress_ovr_cmpct_w_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();

        // Initial bulk insert
        for i in 1..=50 {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("initial_{}", i)],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        // 10 sequential small updates, each with a checkpoint (creates overrides)
        for round in 1..=10 {
            conn.execute(
                "UPDATE t SET val = ?1 WHERE id = ?2",
                rusqlite::params![format!("round_{}", round), round],
            )
            .unwrap();
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
                .unwrap();
        }

        drop(conn);
    }

    // Cold reopen: delete cache files
    let _ = std::fs::remove_file(dir.path().join("data.cache"));
    let _ = std::fs::remove_file(dir.path().join("page_bitmap"));
    let _ = std::fs::remove_file(dir.path().join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(dir.path().join("cache_index.json"));

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                override_threshold: 100,
                compaction_threshold: 5,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("stress_ovr_cmpct_r_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");

        // Rows 1-10 should have latest update values
        for i in 1..=10 {
            let val: String = conn
                .query_row(
                    "SELECT val FROM t WHERE id = ?1",
                    rusqlite::params![i],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                val,
                format!("round_{}", i),
                "row {} should have latest override value",
                i
            );
        }

        // Rows 11-50 should still have initial values
        for i in 11..=50 {
            let val: String = conn
                .query_row(
                    "SELECT val FROM t WHERE id = ?1",
                    rusqlite::params![i],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                val,
                format!("initial_{}", i),
                "row {} should retain initial value",
                i
            );
        }

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 50);
    }
}

/// Override + compression combined: both features active simultaneously.
#[test]
fn test_override_with_compression_roundtrip() {
    use std::sync::atomic::Ordering;
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);

    let dir = TempDir::new().unwrap();

    // Write phase: create base groups with compression + overrides
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            // disable compaction to keep overrides visible
            cache: CacheConfig {
                compression: true,
                compression_level: 3,
                override_threshold: 100,
                compaction_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("ovr_cmp_w_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();

        // Initial data - creates base groups
        for i in 1..=20 {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("base_{}", i)],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        // Update subset - creates overrides
        for i in 1..=5 {
            conn.execute(
                "UPDATE t SET val = ?1 WHERE id = ?2",
                rusqlite::params![format!("updated_{}", i), i],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        drop(conn);
    }

    // Cold reopen with cache deleted
    let _ = std::fs::remove_file(dir.path().join("data.cache"));
    let _ = std::fs::remove_file(dir.path().join("page_bitmap"));
    let _ = std::fs::remove_file(dir.path().join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(dir.path().join("cache_index.json"));

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                compression: true,
                compression_level: 3,
                override_threshold: 100,
                compaction_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("ovr_cmp_r_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");

        // Updated rows should have new values
        for i in 1..=5 {
            let val: String = conn
                .query_row(
                    "SELECT val FROM t WHERE id = ?1",
                    rusqlite::params![i],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(val, format!("updated_{}", i));
        }

        // Non-updated rows should have base values
        for i in 6..=20 {
            let val: String = conn
                .query_row(
                    "SELECT val FROM t WHERE id = ?1",
                    rusqlite::params![i],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(val, format!("base_{}", i));
        }

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 20);
    }
}

/// Rapid checkpoint cycles: 50 iterations of insert 1 row + checkpoint.
#[test]
fn test_stress_rapid_checkpoint_cycles() {
    let dir = TempDir::new().unwrap();
    let iterations = 50;

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                override_threshold: 100,
                compaction_threshold: 10,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("stress_rapid_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        for i in 1..=iterations {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("rapid_{}", i)],
            )
            .unwrap();
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
                .unwrap();
        }

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, iterations);

        drop(conn);
    }

    // Cold reopen
    let _ = std::fs::remove_file(dir.path().join("data.cache"));
    let _ = std::fs::remove_file(dir.path().join("page_bitmap"));
    let _ = std::fs::remove_file(dir.path().join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(dir.path().join("cache_index.json"));

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("stress_rapid_r_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            count, iterations,
            "all {} rows should survive rapid checkpoint cycles",
            iterations
        );

        // Spot check first and last
        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "rapid_1");

        let val: String = conn
            .query_row(
                "SELECT val FROM t WHERE id = ?1",
                rusqlite::params![iterations],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(val, format!("rapid_{}", iterations));
    }
}

// =========================================================================
// Edge case tests
// =========================================================================

/// Empty database: create schema, checkpoint, cold reopen, verify table exists with 0 rows.
#[test]
fn test_edge_empty_database_reopen() {
    let dir = TempDir::new().unwrap();

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("edge_empty_w_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        drop(conn);
    }

    // Cold reopen
    let _ = std::fs::remove_file(dir.path().join("data.cache"));
    let _ = std::fs::remove_file(dir.path().join("page_bitmap"));
    let _ = std::fs::remove_file(dir.path().join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(dir.path().join("cache_index.json"));

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("edge_empty_r_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");

        // Table should exist
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "empty table should have 0 rows after cold reopen");

        // Should be able to insert after cold reopen
        conn.execute("INSERT INTO t VALUES (1, 'after_reopen')", [])
            .unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }
}

/// Single row database: insert 1 row, checkpoint, cold reopen, verify.
#[test]
fn test_edge_single_row_database() {
    let dir = TempDir::new().unwrap();

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("edge_single_w_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'only_row')", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        drop(conn);
    }

    // Cold reopen
    let _ = std::fs::remove_file(dir.path().join("data.cache"));
    let _ = std::fs::remove_file(dir.path().join("page_bitmap"));
    let _ = std::fs::remove_file(dir.path().join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(dir.path().join("cache_index.json"));

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("edge_single_r_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);

        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "only_row");
    }
}

/// Group boundary: with small pages_per_group, insert enough data to land exactly
/// on a group boundary. Verify data integrity across the boundary.
#[test]
fn test_edge_group_boundary_pages() {
    let dir = TempDir::new().unwrap();
    let pages_per_group = 4u32;

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                pages_per_group,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("edge_grpbnd_w_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT, padding TEXT)",
            [],
        )
        .unwrap();

        // Insert enough rows to span multiple groups with small pages_per_group=4.
        // Each 4KB page holds a limited number of rows; ~200 rows should span many groups.
        for i in 0..200 {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2, ?3)",
                rusqlite::params![i, format!("val_{}", i), "x".repeat(100)],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        // Update last page of one group and first page of next by updating scattered rows
        conn.execute("UPDATE t SET val = 'boundary_first' WHERE id = 0", [])
            .unwrap();
        conn.execute("UPDATE t SET val = 'boundary_last' WHERE id = 199", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        drop(conn);
    }

    // Cold reopen
    let _ = std::fs::remove_file(dir.path().join("data.cache"));
    let _ = std::fs::remove_file(dir.path().join("page_bitmap"));
    let _ = std::fs::remove_file(dir.path().join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(dir.path().join("cache_index.json"));

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                pages_per_group,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("edge_grpbnd_r_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 200);

        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 0", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "boundary_first");

        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 199", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "boundary_last");

        // Middle rows should be intact
        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 100", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "val_100");
    }
}

/// Override at frame boundary: with sub_pages_per_frame=4, update pages that span
/// two frames. Verify both override frames are created and readable.
#[test]
fn test_edge_override_at_frame_boundary() {
    use std::sync::atomic::Ordering;
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);

    let dir = TempDir::new().unwrap();

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            // 8 pages per group; 2 frames per group
            cache: CacheConfig {
                pages_per_group: 8,
                sub_pages_per_frame: 4,
                override_threshold: 100,
                compaction_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("edge_ovr_frame_w_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT, padding TEXT)",
            [],
        )
        .unwrap();

        // Insert enough data to span multiple groups/frames
        for i in 0..300 {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2, ?3)",
                rusqlite::params![i, format!("base_{}", i), "p".repeat(80)],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        // Update rows scattered across different pages to hit multiple frames
        for i in (0..300).step_by(25) {
            conn.execute(
                "UPDATE t SET val = ?1 WHERE id = ?2",
                rusqlite::params![format!("frame_ovr_{}", i), i],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        drop(conn);
    }

    // Cold reopen
    let _ = std::fs::remove_file(dir.path().join("data.cache"));
    let _ = std::fs::remove_file(dir.path().join("page_bitmap"));
    let _ = std::fs::remove_file(dir.path().join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(dir.path().join("cache_index.json"));

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                pages_per_group: 8,
                sub_pages_per_frame: 4,
                override_threshold: 100,
                compaction_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("edge_ovr_frame_r_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 300);

        // Verify overridden rows
        for i in (0..300).step_by(25) {
            let val: String = conn
                .query_row(
                    "SELECT val FROM t WHERE id = ?1",
                    rusqlite::params![i],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                val,
                format!("frame_ovr_{}", i),
                "override row {} mismatch",
                i
            );
        }

        // Verify non-overridden rows
        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "base_1");
    }
}

/// Rollback with dirty pages in multiple groups: begin transaction, update pages
/// across groups, ROLLBACK, verify all data reverts.
#[test]
fn test_rollback_multi_group_dirty_pages() {
    let dir = TempDir::new().unwrap();

    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        cache: CacheConfig {
            pages_per_group: 4,
            ..Default::default()
        },
        ..Default::default()
    };
    let vfs_name = format!("edge_rollback_mg_{}", std::process::id());
    let vfs = TurboliteVfs::new_local(config).expect("local VFS");
    crate::tiered::register(&vfs_name, vfs).expect("register");

    let db_path = format!("file:test.db?vfs={}", vfs_name);
    let conn = rusqlite::Connection::open(&db_path).expect("open");
    conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT, padding TEXT)",
        [],
    )
    .unwrap();

    // Insert enough data to span multiple groups (pages_per_group=4)
    for i in 0..200 {
        conn.execute(
            "INSERT INTO t VALUES (?1, ?2, ?3)",
            rusqlite::params![i, format!("committed_{}", i), "x".repeat(80)],
        )
        .unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();

    // Begin transaction, update rows across many groups, then ROLLBACK
    conn.execute_batch("BEGIN").unwrap();
    for i in (0..200).step_by(10) {
        conn.execute(
            "UPDATE t SET val = ?1 WHERE id = ?2",
            rusqlite::params!["SHOULD_NOT_EXIST", i],
        )
        .unwrap();
    }
    conn.execute_batch("ROLLBACK").unwrap();

    // All rows should have original values
    for i in (0..200).step_by(10) {
        let val: String = conn
            .query_row(
                "SELECT val FROM t WHERE id = ?1",
                rusqlite::params![i],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            val,
            format!("committed_{}", i),
            "row {} should revert after rollback",
            i
        );
    }

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 200, "row count should be unchanged after rollback");

    // Verify we can still write after rollback
    conn.execute("INSERT INTO t VALUES (999, 'after_rollback', 'ok')", [])
        .unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 201);
}

/// Override then delete rows: insert, checkpoint (base), delete half, checkpoint (overrides),
/// cold reopen, verify only remaining rows exist.
#[test]
fn test_override_then_delete_rows() {
    use std::sync::atomic::Ordering;
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);

    let dir = TempDir::new().unwrap();

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                override_threshold: 100,
                compaction_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("edge_ovr_del_w_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();

        // Insert 100 rows, checkpoint (base groups)
        for i in 1..=100 {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("row_{}", i)],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        // Delete odd rows (50 rows), checkpoint (creates overrides for pages with deletions)
        conn.execute("DELETE FROM t WHERE id % 2 = 1", []).unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        drop(conn);
    }

    // Cold reopen
    let _ = std::fs::remove_file(dir.path().join("data.cache"));
    let _ = std::fs::remove_file(dir.path().join("page_bitmap"));
    let _ = std::fs::remove_file(dir.path().join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(dir.path().join("cache_index.json"));

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                override_threshold: 100,
                compaction_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("edge_ovr_del_r_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 50, "only even rows should remain");

        // Odd rows should not exist
        let odd_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t WHERE id % 2 = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(odd_count, 0, "all odd rows should be deleted");

        // Even rows should still exist
        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 2", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "row_2");

        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 100", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "row_100");
    }
}

/// Compaction with threshold=1: compact after every single override.
#[test]
fn test_compaction_threshold_one() {
    use std::sync::atomic::Ordering;
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);

    let dir = TempDir::new().unwrap();

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            // compact after every single override
            cache: CacheConfig {
                override_threshold: 100,
                compaction_threshold: 1,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("edge_cmpct1_w_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();

        // Initial data
        for i in 1..=10 {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("init_{}", i)],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        // Update + checkpoint (should trigger immediate compaction)
        conn.execute("UPDATE t SET val = 'compacted_1' WHERE id = 1", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        // Another update + checkpoint
        conn.execute("UPDATE t SET val = 'compacted_2' WHERE id = 2", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        drop(conn);
    }

    // Cold reopen
    let _ = std::fs::remove_file(dir.path().join("data.cache"));
    let _ = std::fs::remove_file(dir.path().join("page_bitmap"));
    let _ = std::fs::remove_file(dir.path().join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(dir.path().join("cache_index.json"));

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                override_threshold: 100,
                compaction_threshold: 1,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("edge_cmpct1_r_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");

        let val1: String = conn
            .query_row("SELECT val FROM t WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val1, "compacted_1");

        let val2: String = conn
            .query_row("SELECT val FROM t WHERE id = 2", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val2, "compacted_2");

        // Other rows untouched
        let val5: String = conn
            .query_row("SELECT val FROM t WHERE id = 5", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val5, "init_5");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 10);
    }
}

/// Cache validation with override changes between sessions:
/// Session 1 writes base, Session 2 updates with overrides, Session 3 reopens and
/// should see correct data from base + overrides.
#[test]
fn test_cache_validation_override_changes_between_sessions() {
    use std::sync::atomic::Ordering;
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);

    let dir = TempDir::new().unwrap();

    // Session 1: write base data + checkpoint
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                override_threshold: 100,
                compaction_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("cv_ovr_s1_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();
        for i in 1..=10 {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("session1_{}", i)],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        drop(conn);
    }

    // Session 2: update with overrides + checkpoint
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                override_threshold: 100,
                compaction_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("cv_ovr_s2_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open session 2");
        conn.execute("UPDATE t SET val = 'session2_updated' WHERE id <= 3", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        drop(conn);
    }

    // Session 3: reopen (cache validation should detect override changes)
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                override_threshold: 100,
                compaction_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("cv_ovr_s3_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS session 3");
        crate::tiered::register(&vfs_name, vfs).expect("register3");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open session 3");

        // Rows 1-3 should have session 2 updates
        for i in 1..=3 {
            let val: String = conn
                .query_row(
                    "SELECT val FROM t WHERE id = ?1",
                    rusqlite::params![i],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                val, "session2_updated",
                "row {} should have session 2 value",
                i
            );
        }

        // Rows 4-10 should still have session 1 values
        for i in 4..=10 {
            let val: String = conn
                .query_row(
                    "SELECT val FROM t WHERE id = ?1",
                    rusqlite::params![i],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                val,
                format!("session1_{}", i),
                "row {} should retain session 1 value",
                i
            );
        }

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 10);
    }
}

// =========================================================================
// Phase Cirrus c: per-handle `turbolite_config_set` integration tests
//
// Unit tests in `settings.rs` cover queue/stack routing mechanics. These
// exercise the full path: open a VFS-backed connection, push a setting
// via `settings::set()` (same code path as the C FFI), run a query,
// confirm the drain applies without crashing and without poisoning other
// handles on the thread.
// =========================================================================

#[test]
fn test_settings_set_round_trip() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };

    let vfs_name = format!("settings_rt_{}", std::process::id());
    let vfs = TurboliteVfs::new_local(config).expect("local VFS");
    crate::tiered::register(&vfs_name, vfs).expect("register");

    let db_path = format!("file:test.db?vfs={}", vfs_name);
    let conn = rusqlite::Connection::open(&db_path).expect("open");
    conn.execute_batch(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
         INSERT INTO t VALUES (1, 'hello'), (2, 'world');",
    )
    .unwrap();

    // Main-db handle is registered on the thread-local stack.
    crate::tiered::settings::set("prefetch_search", "0.5,0.5,0.0")
        .expect("set prefetch_search on active handle");
    crate::tiered::settings::set("prefetch_lookup", "0.0,0.0,0.0")
        .expect("set prefetch_lookup on active handle");
    crate::tiered::settings::set("plan_aware", "true").expect("set plan_aware on active handle");

    // Next read drains the queue. No observable field from outside for
    // local VFS, but the drain path must not crash and queries must
    // continue to work.
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 2);

    crate::tiered::settings::set("prefetch_reset", "").expect("reset");
    let v: String = conn
        .query_row("SELECT v FROM t WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(v, "hello");
}

#[test]
fn test_settings_set_without_handle_errors() {
    // No turbolite connection on this thread; push must fail loudly
    // rather than silently drop.
    let r = crate::tiered::settings::set("prefetch_search", "0.3,0.3,0.4");
    assert!(r.is_err(), "expected error with no active handle, got Ok");
}

#[test]
fn test_settings_set_per_connection_isolation() {
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let cfg_a = TurboliteConfig {
        cache_dir: dir_a.path().to_path_buf(),
        ..Default::default()
    };
    let cfg_b = TurboliteConfig {
        cache_dir: dir_b.path().to_path_buf(),
        ..Default::default()
    };

    let vfs_a_name = format!("settings_iso_a_{}", std::process::id());
    let vfs_b_name = format!("settings_iso_b_{}", std::process::id());
    crate::tiered::register(&vfs_a_name, TurboliteVfs::new_local(cfg_a).expect("vfs A"))
        .expect("reg A");
    crate::tiered::register(&vfs_b_name, TurboliteVfs::new_local(cfg_b).expect("vfs B"))
        .expect("reg B");

    let conn_a =
        rusqlite::Connection::open(&format!("file:a.db?vfs={}", vfs_a_name)).expect("open A");
    conn_a
        .execute_batch("CREATE TABLE a (id INTEGER); INSERT INTO a VALUES (1);")
        .unwrap();

    let conn_b =
        rusqlite::Connection::open(&format!("file:b.db?vfs={}", vfs_b_name)).expect("open B");
    conn_b
        .execute_batch("CREATE TABLE b (id INTEGER); INSERT INTO b VALUES (2);")
        .unwrap();

    // B is on top; push lands on B's queue.
    crate::tiered::settings::set("prefetch_search", "1.0").expect("set while B on top");

    // Close B → A back on top.
    drop(conn_b);

    // Push now lands on A, not the dropped B.
    crate::tiered::settings::set("prefetch_search", "0.5,0.5").expect("set while A on top");

    // A's next read drains A's queue, query still works.
    let n: i64 = conn_a
        .query_row("SELECT COUNT(*) FROM a", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 1);
}
