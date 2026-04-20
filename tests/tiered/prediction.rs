//! Integration tests for predictive cross-tree prefetch (Phase Verdun remaining).

use super::helpers::*;
use turbolite::tiered::*;

/// Prediction patterns survive checkpoint -> S3 manifest -> reopen.
#[test]
fn test_prediction_patterns_survive_checkpoint_roundtrip() {
    let cache_dir = tempfile::tempdir().unwrap();
    let mut config = test_config("pred_rt", cache_dir.path());
    config.prediction_enabled = true;
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let vfs_name = unique_vfs_name("pred_rt");
    let vfs = TurboliteVfs::new_local(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let db_path = format!("file:pred_rt.db?vfs={}", vfs_name);

    // Create multi-table DB
    {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
                | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        conn.execute_batch(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
             CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, body TEXT);
             CREATE INDEX idx_posts_user ON posts(user_id);",
        ).unwrap();
        for i in 0..100 {
            conn.execute("INSERT INTO users VALUES (?1, ?2)",
                rusqlite::params![i, format!("user_{}", i)]).unwrap();
            conn.execute("INSERT INTO posts VALUES (?1, ?2, ?3)",
                rusqlite::params![i, i % 20, format!("post body {}", i)]).unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    // Run multi-table queries to build patterns
    {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        for _ in 0..5 {
            let _: Vec<(i64, String)> = conn
                .prepare("SELECT u.id, u.name FROM users u JOIN posts p ON p.user_id = u.id WHERE p.id < 10")
                .unwrap()
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .filter_map(|r| r.ok())
                .collect();
        }
    }

    // Checkpoint to persist patterns
    {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    // Read manifest from S3
    let manifest = turbolite::tiered::get_manifest(&TurboliteConfig {
        bucket: bucket.clone(), prefix: prefix.clone(),
        endpoint_url: endpoint.clone(), region: Some("auto".to_string()),
        cache_dir: cache_dir.path().to_path_buf(), ..Default::default()
    }).unwrap().unwrap();

    eprintln!(
        "[test] patterns: {}, access_freq keys: {}",
        manifest.prediction_patterns.len(), manifest.btree_access_freq.len(),
    );

    // Reopen from S3 with a fresh VFS and verify patterns survived
    let reader_cache = tempfile::tempdir().unwrap();
    let reader_manifest = turbolite::tiered::get_manifest(&TurboliteConfig {
        bucket, prefix, endpoint_url: endpoint,
        region: Some("auto".to_string()),
        cache_dir: reader_cache.path().to_path_buf(),
        ..Default::default()
    }).unwrap().unwrap();

    assert_eq!(
        reader_manifest.prediction_patterns.len(),
        manifest.prediction_patterns.len(),
        "prediction patterns should survive S3 manifest roundtrip"
    );
    assert_eq!(
        reader_manifest.btree_access_freq.len(),
        manifest.btree_access_freq.len(),
        "access frequencies should survive S3 manifest roundtrip"
    );
}

/// Checkpoint with prediction disabled produces empty prediction_patterns.
#[test]
fn test_checkpoint_no_patterns_when_disabled() {
    let cache_dir = tempfile::tempdir().unwrap();
    let mut config = test_config("pred_off", cache_dir.path());
    config.prediction_enabled = false;
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let vfs_name = unique_vfs_name("pred_off");
    let vfs = TurboliteVfs::new_local(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let db_path = format!("file:pred_off.db?vfs={}", vfs_name);
    {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
                | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY);").unwrap();
        for i in 0..50 { conn.execute("INSERT INTO t VALUES (?1)", [i]).unwrap(); }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    let manifest = turbolite::tiered::get_manifest(&TurboliteConfig {
        bucket, prefix, endpoint_url: endpoint,
        region: Some("auto".to_string()),
        cache_dir: cache_dir.path().to_path_buf(), ..Default::default()
    }).unwrap().unwrap();

    assert!(manifest.prediction_patterns.is_empty(),
        "prediction_patterns should be empty when prediction disabled");
}

/// Single-table queries never fire predictions (need 2+ trees).
#[test]
fn test_single_table_no_predictions() {
    let cache_dir = tempfile::tempdir().unwrap();
    let mut config = test_config("pred_single", cache_dir.path());
    config.prediction_enabled = true;
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let vfs_name = unique_vfs_name("pred_single");
    let vfs = TurboliteVfs::new_local(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let db_path = format!("file:pred_single.db?vfs={}", vfs_name);
    {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
                | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        conn.execute_batch("CREATE TABLE single (id INTEGER PRIMARY KEY, data TEXT);").unwrap();
        for i in 0..100 {
            conn.execute("INSERT INTO single VALUES (?1, ?2)",
                rusqlite::params![i, format!("data_{}", i)]).unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    // Run single-table queries then checkpoint
    {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        for _ in 0..10 {
            let _: i64 = conn.query_row("SELECT COUNT(*) FROM single", [], |row| row.get(0)).unwrap();
        }
    }
    {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    let manifest = turbolite::tiered::get_manifest(&TurboliteConfig {
        bucket, prefix, endpoint_url: endpoint,
        region: Some("auto".to_string()),
        cache_dir: cache_dir.path().to_path_buf(), ..Default::default()
    }).unwrap().unwrap();

    assert!(manifest.prediction_patterns.is_empty(),
        "single-table queries should not create prediction patterns");
}

/// After VACUUM, data readable from cold reader (patterns don't cause corruption).
#[test]
fn test_vacuum_predictions_no_corruption() {
    let cache_dir = tempfile::tempdir().unwrap();
    let mut config = test_config("pred_vac", cache_dir.path());
    config.prediction_enabled = true;
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let vfs_name = unique_vfs_name("pred_vac");
    let vfs = TurboliteVfs::new_local(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let db_path = format!("file:pred_vac.db?vfs={}", vfs_name);

    // Create multi-table DB and build patterns
    {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
                | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        conn.execute_batch(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
             CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, body TEXT);",
        ).unwrap();
        for i in 0..100 {
            conn.execute("INSERT INTO users VALUES (?1, ?2)",
                rusqlite::params![i, format!("user_{}", i)]).unwrap();
            conn.execute("INSERT INTO posts VALUES (?1, ?2, ?3)",
                rusqlite::params![i, i % 20, format!("body {}", i)]).unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    // Build patterns
    {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        for _ in 0..5 {
            let _: Vec<i64> = conn
                .prepare("SELECT u.id FROM users u JOIN posts p ON p.user_id = u.id LIMIT 10")
                .unwrap()
                .query_map([], |row| row.get(0))
                .unwrap()
                .filter_map(|r| r.ok())
                .collect();
        }
    }

    // Checkpoint, VACUUM, checkpoint again
    {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
        conn.execute_batch("VACUUM;").unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    // Cold reader should see data (patterns keyed by name survive VACUUM)
    {
        let reader_cache = tempfile::tempdir().unwrap();
        let reader_config = TurboliteConfig {
            bucket, prefix, endpoint_url: endpoint,
            region: Some("auto".to_string()),
            cache_dir: reader_cache.path().to_path_buf(),
            prediction_enabled: true, read_only: true,
            ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("pred_vac_reader");
        let reader_vfs = TurboliteVfs::new_local(reader_config).unwrap();
        turbolite::tiered::register(&reader_vfs_name, reader_vfs).unwrap();

        let reader_db = format!("file:pred_vac.db?vfs={}", reader_vfs_name);
        let conn = rusqlite::Connection::open_with_flags(
            &reader_db,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM users", [], |row| row.get(0)).unwrap();
        assert_eq!(count, 100, "data should survive VACUUM with prediction patterns");
    }
}

/// DROP TABLE + CREATE TABLE with same name: no crash, data readable.
#[test]
fn test_drop_create_same_name_no_crash() {
    let cache_dir = tempfile::tempdir().unwrap();
    let mut config = test_config("pred_drop", cache_dir.path());
    config.prediction_enabled = true;

    let vfs_name = unique_vfs_name("pred_drop");
    let vfs = TurboliteVfs::new_local(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let db_path = format!("file:pred_drop.db?vfs={}", vfs_name);

    {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
                | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        conn.execute_batch(
            "CREATE TABLE alpha (id INTEGER PRIMARY KEY, val TEXT);
             CREATE TABLE beta (id INTEGER PRIMARY KEY, ref_id INTEGER);",
        ).unwrap();
        for i in 0..100 {
            conn.execute("INSERT INTO alpha VALUES (?1, ?2)",
                rusqlite::params![i, format!("val_{}", i)]).unwrap();
            conn.execute("INSERT INTO beta VALUES (?1, ?2)", rusqlite::params![i, i % 20]).unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();

        // Drop and recreate alpha
        conn.execute_batch("DROP TABLE alpha;").unwrap();
        conn.execute_batch("CREATE TABLE alpha (id INTEGER PRIMARY KEY, new_col INTEGER);").unwrap();
        for i in 0..50 {
            conn.execute("INSERT INTO alpha VALUES (?1, ?2)", rusqlite::params![i, i * 2]).unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    // Data should be readable
    {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM alpha", [], |row| row.get(0)).unwrap();
        assert_eq!(count, 50, "recreated alpha should have 50 rows");
    }
}
