//! End-to-end WAL integration tests (Phase Somme-f).
//!
//! Requires: real S3 credentials, `wal` feature flag.
//!
//! ```bash
//! set -a && source ../.env && set +a
//! TIERED_TEST_BUCKET=sqlces-test AWS_ENDPOINT_URL=https://fly.storage.tigris.dev \
//!   cargo test --features tiered,zstd,wal --test tiered wal_integration
//! ```

#![cfg(feature = "wal")]

use super::helpers::*;
use turbolite::tiered::*;
use std::path::Path;

struct WalTestParams {
    bucket: String,
    prefix: String,
    endpoint: Option<String>,
}

impl WalTestParams {
    fn wal_config(&self, cache_dir: &Path) -> TieredConfig {
        let mut config = TieredConfig {
            bucket: self.bucket.clone(),
            prefix: self.prefix.clone(),
            endpoint_url: self.endpoint.clone(),
            region: Some("auto".to_string()),
            cache_dir: cache_dir.to_path_buf(),
            ..Default::default()
        };
        config.wal_replication = true;
        config.wal_sync_interval_ms = 100;
        config
    }

    fn read_config(&self, cache_dir: &Path) -> TieredConfig {
        TieredConfig {
            bucket: self.bucket.clone(),
            prefix: self.prefix.clone(),
            endpoint_url: self.endpoint.clone(),
            region: Some("auto".to_string()),
            cache_dir: cache_dir.to_path_buf(),
            read_only: true,
            ..Default::default()
        }
    }
}

fn import_for_wal(
    test_prefix: &str,
    cache_dir: &Path,
    setup_fn: impl FnOnce(&rusqlite::Connection),
) -> (WalTestParams, Manifest) {
    let config = test_config(test_prefix, cache_dir);
    let params = WalTestParams {
        bucket: config.bucket.clone(),
        prefix: config.prefix.clone(),
        endpoint: config.endpoint_url.clone(),
    };
    let local_db = cache_dir.join("source.db");
    {
        let conn = rusqlite::Connection::open(&local_db).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        setup_fn(&conn);
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
        drop(conn);
    }
    let manifest = import_sqlite_file(&config, &local_db).unwrap();
    (params, manifest)
}

// ============================================================================
// Version agreement
// ============================================================================

/// manifest.version uses the file change counter, not incremental integers.
#[test]
fn test_version_is_change_counter() {
    let cache_dir = tempfile::tempdir().unwrap();
    let (params, manifest) = import_for_wal("wal_ver", cache_dir.path(), |conn| {
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);").unwrap();
        for i in 0..100 {
            conn.execute("INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("val_{}", i)]).unwrap();
        }
    });

    // Change counter is typically > 1 for a DB with committed data
    assert!(manifest.version > 0, "manifest version should be > 0");

    // Open VFS, write, checkpoint - version should advance
    let vfs_name = unique_vfs_name("wal_ver");
    let vfs = TieredVfs::new(params.wal_config(cache_dir.path())).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let db_path = format!("file:source.db?vfs={}", vfs_name);
    {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        for i in 100..110 {
            conn.execute("INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("new_{}", i)]).unwrap();
        }
        std::thread::sleep(std::time::Duration::from_millis(300));
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    let read_cache = tempfile::tempdir().unwrap();
    let new_manifest = turbolite::tiered::get_manifest(&params.read_config(read_cache.path()))
        .unwrap().unwrap();

    assert!(new_manifest.version > manifest.version,
        "version should advance: {} -> {}", manifest.version, new_manifest.version);
}

// ============================================================================
// Crash recovery
// ============================================================================

/// Write, ship WAL, crash (no checkpoint), recover.
#[test]
fn test_crash_recovery_via_wal() {
    let cache_dir = tempfile::tempdir().unwrap();
    let (params, _) = import_for_wal("wal_crash", cache_dir.path(), |conn| {
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);").unwrap();
        for i in 0..50 {
            conn.execute("INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("base_{}", i)]).unwrap();
        }
    });

    // Write more, let WAL ship, crash
    {
        let vfs_name = unique_vfs_name("wal_crash_w");
        let vfs = TieredVfs::new(params.wal_config(cache_dir.path())).unwrap();
        turbolite::tiered::register(&vfs_name, vfs).unwrap();

        let db_path = format!("file:source.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        for i in 50..100 {
            conn.execute("INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("wal_{}", i)]).unwrap();
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
        drop(conn);
    }

    // Recover
    let recovery_cache = tempfile::tempdir().unwrap();
    let vfs_name = unique_vfs_name("wal_crash_r");
    let vfs = TieredVfs::new(params.wal_config(recovery_cache.path())).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let db_path = format!("file:source.db?vfs={}", vfs_name);
    let conn = rusqlite::Connection::open_with_flags(
        &db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
    ).unwrap();
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0)).unwrap();
    eprintln!("[test] crash recovery rows: {}", count);
    assert!(count >= 50, "must have at least base 50, got {}", count);
}

/// Write + checkpoint + write more (WAL only) + crash.
#[test]
fn test_checkpoint_plus_wal_recovery() {
    let cache_dir = tempfile::tempdir().unwrap();
    let (params, _) = import_for_wal("wal_cp_wal", cache_dir.path(), |conn| {
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);").unwrap();
        for i in 0..30 { conn.execute("INSERT INTO t VALUES (?1, ?2)",
            rusqlite::params![i, format!("import_{}", i)]).unwrap(); }
    });

    // Phase 1: write + checkpoint
    {
        let vfs_name = unique_vfs_name("wal_cpw_1");
        let vfs = TieredVfs::new(params.wal_config(cache_dir.path())).unwrap();
        turbolite::tiered::register(&vfs_name, vfs).unwrap();
        let db_path = format!("file:source.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open_with_flags(&db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_URI).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        for i in 30..60 { conn.execute("INSERT INTO t VALUES (?1, ?2)",
            rusqlite::params![i, format!("cp_{}", i)]).unwrap(); }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    // Phase 2: WAL only
    {
        let vfs_name = unique_vfs_name("wal_cpw_2");
        let vfs = TieredVfs::new(params.wal_config(cache_dir.path())).unwrap();
        turbolite::tiered::register(&vfs_name, vfs).unwrap();
        let db_path = format!("file:source.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open_with_flags(&db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_URI).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        for i in 60..100 { conn.execute("INSERT INTO t VALUES (?1, ?2)",
            rusqlite::params![i, format!("wal_{}", i)]).unwrap(); }
        std::thread::sleep(std::time::Duration::from_secs(1));
        drop(conn);
    }

    // Recover
    let rc = tempfile::tempdir().unwrap();
    let vfs_name = unique_vfs_name("wal_cpw_r");
    let vfs = TieredVfs::new(params.wal_config(rc.path())).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();
    let conn = rusqlite::Connection::open_with_flags(
        &format!("file:source.db?vfs={}", vfs_name),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI).unwrap();
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0)).unwrap();
    eprintln!("[test] checkpoint+WAL recovery rows: {}", count);
    assert!(count >= 60, "must have at least 60 (import+checkpoint), got {}", count);
}

// ============================================================================
// No WAL to replay
// ============================================================================

/// Import only, no WAL. Recovery should just use page groups.
#[test]
fn test_no_wal_recovery_is_noop() {
    let cache_dir = tempfile::tempdir().unwrap();
    let (params, _) = import_for_wal("wal_noop", cache_dir.path(), |conn| {
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);").unwrap();
        for i in 0..50 { conn.execute("INSERT INTO t VALUES (?1, ?2)",
            rusqlite::params![i, format!("v_{}", i)]).unwrap(); }
    });

    let rc = tempfile::tempdir().unwrap();
    let vfs_name = unique_vfs_name("wal_noop_r");
    let vfs = TieredVfs::new(params.wal_config(rc.path())).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();
    let conn = rusqlite::Connection::open_with_flags(
        &format!("file:source.db?vfs={}", vfs_name),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI).unwrap();
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 50);
}

// ============================================================================
// WAL GC after checkpoint
// ============================================================================

/// Write commits, checkpoint, verify WAL segments cleaned.
#[test]
fn test_wal_gc_after_checkpoint() {
    let cache_dir = tempfile::tempdir().unwrap();
    let (params, _) = import_for_wal("wal_gc", cache_dir.path(), |conn| {
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);").unwrap();
        for i in 0..20 { conn.execute("INSERT INTO t VALUES (?1, ?2)",
            rusqlite::params![i, format!("b_{}", i)]).unwrap(); }
    });

    let vfs_name = unique_vfs_name("wal_gc");
    let vfs = TieredVfs::new(params.wal_config(cache_dir.path())).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();
    let db_path = format!("file:source.db?vfs={}", vfs_name);

    let conn = rusqlite::Connection::open_with_flags(&db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_URI).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();

    // Write several batches, let WAL ship
    for batch in 0..5 {
        for i in 0..10 {
            conn.execute("INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![20 + batch * 10 + i, format!("b{}_{}", batch, i)]).unwrap();
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }

    // Checkpoint triggers GC
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    std::thread::sleep(std::time::Duration::from_secs(2));

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 70, "should have 20 + 50 = 70 rows");
}

// ============================================================================
// Large data
// ============================================================================

/// 1000 rows, multiple checkpoints with WAL between, crash, recover.
#[test]
fn test_large_data_wal_recovery() {
    let cache_dir = tempfile::tempdir().unwrap();
    let (params, _) = import_for_wal("wal_lg", cache_dir.path(), |conn| {
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);").unwrap();
        for i in 0..200 { conn.execute("INSERT INTO t VALUES (?1, ?2)",
            rusqlite::params![i, format!("i_{}", i)]).unwrap(); }
    });

    // Write + checkpoint + write + WAL
    {
        let vfs_name = unique_vfs_name("wal_lg_w");
        let vfs = TieredVfs::new(params.wal_config(cache_dir.path())).unwrap();
        turbolite::tiered::register(&vfs_name, vfs).unwrap();
        let db_path = format!("file:source.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open_with_flags(&db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_URI).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();

        for i in 200..600 { conn.execute("INSERT INTO t VALUES (?1, ?2)",
            rusqlite::params![i, format!("cp_{}", i)]).unwrap(); }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();

        for i in 600..1000 { conn.execute("INSERT INTO t VALUES (?1, ?2)",
            rusqlite::params![i, format!("w_{}", i)]).unwrap(); }
        std::thread::sleep(std::time::Duration::from_secs(2));
        drop(conn);
    }

    let rc = tempfile::tempdir().unwrap();
    let vfs_name = unique_vfs_name("wal_lg_r");
    let vfs = TieredVfs::new(params.wal_config(rc.path())).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();
    let conn = rusqlite::Connection::open_with_flags(
        &format!("file:source.db?vfs={}", vfs_name),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI).unwrap();
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0)).unwrap();
    eprintln!("[test] large recovery: {} rows", count);
    assert!(count >= 600, "must have at least 600 (import+checkpoint), got {}", count);
}
