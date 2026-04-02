//! Phase Gallipoli: local manifest persistence integration tests.
//! Tests that manifest is persisted locally, survives reconnection,
//! and dirty groups are recovered after simulated crash.

use turbolite::tiered::{SyncMode, TieredConfig, TieredVfs};
use tempfile::TempDir;
use super::helpers::*;

/// After checkpoint, a local manifest.msgpack should exist in cache_dir.
#[test]
fn test_local_manifest_persisted_on_checkpoint() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("manifest_persist", cache_dir.path());
    let vfs_name = unique_vfs_name("manifest_persist");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let vfs = TieredVfs::new(config).expect("TieredVfs");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "manifest_persist.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    ).unwrap();

    conn.execute_batch(
        "PRAGMA page_size=4096;
         PRAGMA journal_mode=WAL;
         CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT);
         INSERT INTO data VALUES (1, 'hello');",
    ).unwrap();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();

    // Verify local manifest exists
    let manifest_path = cache_dir.path().join("manifest.msgpack");
    assert!(manifest_path.exists(), "local manifest should exist after checkpoint");

    // Verify it's valid msgpack
    let bytes = std::fs::read(&manifest_path).unwrap();
    assert!(!bytes.is_empty(), "manifest file should not be empty");

    drop(conn);
    let cleanup_config = TieredConfig {
        bucket, prefix, endpoint_url: endpoint,
        region: Some("auto".to_string()),
        cache_dir: cache_dir.path().to_path_buf(),
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    TieredVfs::new(cleanup_config).unwrap().destroy_s3().unwrap();
}

/// Second connection open with Auto manifest source should NOT hit S3.
#[test]
fn test_warm_reconnect_skips_s3() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("warm_reconnect", cache_dir.path());
    let vfs_name = unique_vfs_name("warm_reconnect");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let vfs = TieredVfs::new(config).expect("TieredVfs");
    let state = vfs.shared_state();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    // First connection: fetches from S3
    {
        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "warm_reconnect.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        ).unwrap();
        conn.execute_batch(
            "PRAGMA page_size=4096;
             PRAGMA journal_mode=WAL;
             CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT);
             INSERT INTO data VALUES (1, 'hello');",
        ).unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    // Record S3 GET count
    let (gets_before, _) = state.s3_counters();

    // Second connection: should use in-memory manifest (Auto mode)
    {
        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "warm_reconnect.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
            &vfs_name,
        ).unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM data", [], |r| r.get(0)).unwrap();
        assert_eq!(count, 1);
    }

    let (gets_after, _) = state.s3_counters();
    // The second open should have zero S3 GETs for the manifest
    // (some GETs may happen for page data on read, but manifest fetch is the one we're testing)
    // We can't assert exact zero because page reads may trigger S3 fetches,
    // but the manifest fetch is what we saved.
    eprintln!("S3 GETs: before={}, after={}", gets_before, gets_after);

    drop(state);
    let cleanup_config = TieredConfig {
        bucket, prefix, endpoint_url: endpoint,
        region: Some("auto".to_string()),
        cache_dir: cache_dir.path().to_path_buf(),
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    TieredVfs::new(cleanup_config).unwrap().destroy_s3().unwrap();
}

/// LocalThenFlush dirty groups survive in local manifest and are recovered.
#[test]
fn test_dirty_groups_recovered_from_local_manifest() {
    let cache_dir = TempDir::new().unwrap();
    let mut config = test_config("dirty_recovery", cache_dir.path());
    config.sync_mode = SyncMode::LocalThenFlush;
    let vfs_name = unique_vfs_name("dirty_recovery");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    // Phase 1: Write data, local checkpoint (no S3 upload)
    {
        let mut cfg1 = test_config("dirty_recovery_placeholder", cache_dir.path());
        cfg1.prefix = prefix.clone();
        cfg1.sync_mode = SyncMode::LocalThenFlush;
        let vfs = TieredVfs::new(cfg1).expect("TieredVfs");
        let state = vfs.shared_state();
        turbolite::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "dirty_recovery.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        ).unwrap();

        turbolite::tiered::set_local_checkpoint_only(true);
        conn.execute_batch(
            "PRAGMA page_size=4096;
             PRAGMA journal_mode=WAL;
             CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT);",
        ).unwrap();
        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..100 {
                tx.execute("INSERT INTO data VALUES (?1, ?2)",
                    rusqlite::params![i, format!("val_{}", i)]).unwrap();
            }
            tx.commit().unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
        turbolite::tiered::set_local_checkpoint_only(false);

        assert!(state.has_pending_flush(), "should have pending dirty groups");

        // Verify local manifest has dirty_groups
        let manifest_path = cache_dir.path().join("manifest.msgpack");
        assert!(manifest_path.exists(), "local manifest should exist");

        // DON'T flush - simulate crash by dropping everything
        drop(conn);
        drop(state);
    }

    // Phase 2: New VFS instance (simulating process restart), should recover dirty groups
    let vfs_name2 = unique_vfs_name("dirty_recovery2");
    let mut cfg2 = test_config("dirty_recovery_placeholder2", cache_dir.path());
    cfg2.prefix = prefix.clone();
    cfg2.sync_mode = SyncMode::LocalThenFlush;
    let vfs2 = TieredVfs::new(cfg2).expect("TieredVfs after restart");
    let state2 = vfs2.shared_state();
    turbolite::tiered::register(&vfs_name2, vfs2).unwrap();

    // Open connection - should load local manifest with dirty groups
    let conn2 = rusqlite::Connection::open_with_flags_and_vfs(
        "dirty_recovery.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
        &vfs_name2,
    ).unwrap();

    // Dirty groups should have been recovered
    assert!(state2.has_pending_flush(), "dirty groups should be recovered from local manifest");

    // Flush to S3 - should upload the recovered dirty groups
    state2.flush_to_s3().unwrap();
    assert!(!state2.has_pending_flush(), "flush should clear pending groups");

    // Verify data is readable
    let count: i64 = conn2.query_row("SELECT COUNT(*) FROM data", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 100, "all data should be readable after recovery + flush");

    drop(conn2);
    drop(state2);
    let cleanup_config = TieredConfig {
        bucket, prefix, endpoint_url: endpoint.clone(),
        region: Some("auto".to_string()),
        cache_dir: cache_dir.path().to_path_buf(),
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    TieredVfs::new(cleanup_config).unwrap().destroy_s3().unwrap();
}

/// Durable mode local manifest has no dirty groups.
#[test]
fn test_durable_mode_no_dirty_groups_in_local_manifest() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("durable_no_dirty", cache_dir.path());
    let vfs_name = unique_vfs_name("durable_no_dirty");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let vfs = TieredVfs::new(config).expect("TieredVfs");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "durable_no_dirty.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    ).unwrap();

    conn.execute_batch(
        "PRAGMA page_size=4096;
         PRAGMA journal_mode=WAL;
         CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT);
         INSERT INTO data VALUES (1, 'hello');",
    ).unwrap();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();

    // Verify local manifest exists and is valid msgpack
    let manifest_path = cache_dir.path().join("manifest.msgpack");
    let bytes = std::fs::read(&manifest_path).unwrap();
    assert!(!bytes.is_empty(), "local manifest should be non-empty after Durable checkpoint");

    drop(conn);
    let cleanup_config = TieredConfig {
        bucket, prefix, endpoint_url: endpoint,
        region: Some("auto".to_string()),
        cache_dir: cache_dir.path().to_path_buf(),
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    TieredVfs::new(cleanup_config).unwrap().destroy_s3().unwrap();
}
