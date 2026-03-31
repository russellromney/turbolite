//! Integration tests for Phase Kursk: staging log correctness.
//!
//! Tests the full write > checkpoint > flush > cold-read flow with
//! SyncMode::LocalThenFlush, including two-checkpoint-before-flush
//! and crash recovery scenarios.

use turbolite::tiered::{SyncMode, TieredConfig, TieredVfs};
use tempfile::TempDir;
use super::helpers::*;

/// Helper: open a connection on the given VFS name, create table, insert rows.
fn setup_and_insert(vfs_name: &str, db: &str, rows: i64) -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        db,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        vfs_name,
    )
    .expect("open connection");

    conn.execute_batch(
        "PRAGMA page_size=65536;
         PRAGMA journal_mode=WAL;
         CREATE TABLE IF NOT EXISTS data (id INTEGER PRIMARY KEY, value TEXT);",
    )
    .unwrap();

    let tx = conn.unchecked_transaction().unwrap();
    for i in 0..rows {
        tx.execute(
            "INSERT OR REPLACE INTO data (id, value) VALUES (?1, ?2)",
            rusqlite::params![i, format!("value_{}", i)],
        )
        .unwrap();
    }
    tx.commit().unwrap();

    conn
}

/// Two checkpoints before one flush: verify cold reader sees all data.
///
/// 1. Write 50 rows, checkpoint (staging log 1)
/// 2. Write 50 more rows (different values), checkpoint (staging log 2)
/// 3. flush_to_s3() uploads both staging logs
/// 4. Cold reader verifies all 100 rows with correct values
#[test]
fn staging_two_checkpoints_before_flush() {
    let cache_dir = TempDir::new().unwrap();
    let mut config = test_config("staging_two_cp", cache_dir.path());
    config.sync_mode = SyncMode::LocalThenFlush;
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let vfs = TieredVfs::new(config).expect("create VFS");
    let shared = vfs.shared_state();
    let vfs_name = unique_vfs_name("staging_two_cp");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = setup_and_insert(&vfs_name, "staging_two_cp.db", 50);

    // Checkpoint 1: local only (staging log created)
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    assert!(shared.has_pending_flush(), "should have pending after checkpoint 1");

    // Write 50 more rows with different values
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 50..100 {
            tx.execute(
                "INSERT INTO data (id, value) VALUES (?1, ?2)",
                rusqlite::params![i, format!("batch2_{}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    // Checkpoint 2: local only (staging log 2 created)
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    assert!(shared.has_pending_flush(), "should have pending after checkpoint 2");

    // Check staging dir has >= 2 log files (SQLite may auto-checkpoint too)
    let staging_dir = cache_dir.path().join("staging");
    let log_count = std::fs::read_dir(&staging_dir)
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .ok()
                .and_then(|e| e.path().extension().map(|ext| ext == "log"))
                .unwrap_or(false)
        })
        .count();
    assert!(log_count >= 2, "should have at least 2 staging log files, got {}", log_count);

    // Flush both to S3
    shared.flush_to_s3().expect("flush failed");
    assert!(!shared.has_pending_flush(), "should have no pending after flush");

    // Staging logs should be cleaned up
    let remaining = std::fs::read_dir(&staging_dir)
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .ok()
                .and_then(|e| e.path().extension().map(|ext| ext == "log"))
                .unwrap_or(false)
        })
        .count();
    assert_eq!(remaining, 0, "staging logs should be cleaned up after flush");

    drop(conn);

    // Cold reader: verify all 100 rows with correct values
    let cold_dir = TempDir::new().unwrap();
    let cold_config = TieredConfig {
        bucket: bucket.clone(),
        prefix: prefix.clone(),
        cache_dir: cold_dir.path().to_path_buf(),
        endpoint_url: endpoint.clone(),
        region: Some("auto".to_string()),
        read_only: true,
        ..Default::default()
    };
    let cold_vfs_name = unique_vfs_name("staging_two_cp_cold");
    let cold_vfs = TieredVfs::new(cold_config).expect("cold VFS");
    turbolite::tiered::register(&cold_vfs_name, cold_vfs).unwrap();

    let cold_conn = rusqlite::Connection::open_with_flags_and_vfs(
        "staging_two_cp.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &cold_vfs_name,
    )
    .unwrap();

    let count: i64 = cold_conn
        .query_row("SELECT COUNT(*) FROM data", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 100, "cold reader should see all 100 rows");

    // Verify batch2 values are correct (from checkpoint 2)
    let val: String = cold_conn
        .query_row(
            "SELECT value FROM data WHERE id = 75",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(val, "batch2_75", "cold reader should see batch2 values");

    // Verify batch1 values survived (from checkpoint 1)
    let val: String = cold_conn
        .query_row(
            "SELECT value FROM data WHERE id = 25",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(val, "value_25", "cold reader should see batch1 values");
}

/// Crash recovery: staging log survives process restart.
///
/// 1. Write data, checkpoint (staging log created)
/// 2. Do NOT flush — drop VFS (simulates crash)
/// 3. Create new VFS pointing at same cache_dir
/// 4. Verify staging log is recovered
/// 5. Flush, verify cold reader
#[test]
fn staging_crash_recovery() {
    let cache_dir = TempDir::new().unwrap();
    let base = test_config("staging_crash", cache_dir.path());
    let bucket = base.bucket.clone();
    let prefix = base.prefix.clone();
    let endpoint = base.endpoint_url.clone();

    // Phase 1: write + checkpoint, then "crash" (drop without flush)
    {
        let config1 = TieredConfig {
            bucket: bucket.clone(),
            prefix: prefix.clone(),
            cache_dir: cache_dir.path().to_path_buf(),
            endpoint_url: endpoint.clone(),
            region: Some("auto".to_string()),
            sync_mode: SyncMode::LocalThenFlush,
            ..Default::default()
        };
        let vfs = TieredVfs::new(config1).expect("create VFS");
        let vfs_name = unique_vfs_name("staging_crash_1");
        turbolite::tiered::register(&vfs_name, vfs).unwrap();

        let conn = setup_and_insert(&vfs_name, "staging_crash.db", 50);
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
        // Drop without flush — simulates crash
    }

    // Verify staging log file exists on disk
    let staging_dir = cache_dir.path().join("staging");
    let log_count = std::fs::read_dir(&staging_dir)
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .ok()
                .and_then(|e| e.path().extension().map(|ext| ext == "log"))
                .unwrap_or(false)
        })
        .count();
    assert!(log_count >= 1, "staging log should survive 'crash'");

    // Phase 2: reopen VFS, verify recovery, flush
    {
        let config2 = TieredConfig {
            bucket: bucket.clone(),
            prefix: prefix.clone(),
            cache_dir: cache_dir.path().to_path_buf(),
            endpoint_url: endpoint.clone(),
            region: Some("auto".to_string()),
            sync_mode: SyncMode::LocalThenFlush,
            ..Default::default()
        };
        let vfs2 = TieredVfs::new(config2).expect("reopen VFS");
        let shared = vfs2.shared_state();

        // Should have recovered the staging log
        assert!(
            shared.has_pending_flush(),
            "should recover staging logs on reopen"
        );

        // Flush recovered data to S3
        shared.flush_to_s3().expect("recovery flush failed");
        assert!(!shared.has_pending_flush(), "pending should be drained");
    }

    // Phase 3: cold reader verifies data
    let cold_dir = TempDir::new().unwrap();
    let cold_config = TieredConfig {
        bucket,
        prefix,
        cache_dir: cold_dir.path().to_path_buf(),
        endpoint_url: endpoint,
        region: Some("auto".to_string()),
        read_only: true,
        ..Default::default()
    };
    let cold_vfs_name = unique_vfs_name("staging_crash_cold");
    let cold_vfs = TieredVfs::new(cold_config).expect("cold VFS");
    turbolite::tiered::register(&cold_vfs_name, cold_vfs).unwrap();

    let cold_conn = rusqlite::Connection::open_with_flags_and_vfs(
        "staging_crash.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &cold_vfs_name,
    )
    .unwrap();

    let count: i64 = cold_conn
        .query_row("SELECT COUNT(*) FROM data", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 50, "cold reader should see 50 rows after recovery");
}

/// Durable mode creates no staging files.
///
/// Verify that SyncMode::Durable (default) does not create any staging
/// log files — the checkpoint uploads directly to S3.
#[test]
fn staging_durable_mode_no_staging_files() {
    let cache_dir = TempDir::new().unwrap();
    let mut config = test_config("staging_durable", cache_dir.path());
    config.sync_mode = SyncMode::Durable; // explicit default

    let vfs = TieredVfs::new(config).expect("create VFS");
    let vfs_name = unique_vfs_name("staging_durable");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = setup_and_insert(&vfs_name, "staging_durable.db", 50);
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();

    // No staging directory should be created
    let staging_dir = cache_dir.path().join("staging");
    if staging_dir.exists() {
        let log_count = std::fs::read_dir(&staging_dir)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .ok()
                    .and_then(|e| e.path().extension().map(|ext| ext == "log"))
                    .unwrap_or(false)
            })
            .count();
        assert_eq!(
            log_count, 0,
            "Durable mode should not create staging log files"
        );
    }
    // else: staging dir doesn't exist at all, which is also correct
}

/// Flush with no pending data is a no-op.
#[test]
fn staging_flush_noop_when_empty() {
    let cache_dir = TempDir::new().unwrap();
    let mut config = test_config("staging_noop", cache_dir.path());
    config.sync_mode = SyncMode::LocalThenFlush;

    let vfs = TieredVfs::new(config).expect("create VFS");
    let shared = vfs.shared_state();

    assert!(!shared.has_pending_flush());
    // Should succeed without error
    shared.flush_to_s3().expect("empty flush should succeed");
}

/// Overwrite test: second checkpoint modifies same rows, flush uploads correct version.
///
/// 1. Write rows 0-49 with value "v1_X", checkpoint
/// 2. UPDATE rows 0-49 to "v2_X", checkpoint
/// 3. Flush (both staging logs)
/// 4. Cold reader sees "v2_X" for all rows (later staging log wins)
#[test]
fn staging_overwrite_same_pages() {
    let cache_dir = TempDir::new().unwrap();
    let mut config = test_config("staging_overwrite", cache_dir.path());
    config.sync_mode = SyncMode::LocalThenFlush;
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let vfs = TieredVfs::new(config).expect("create VFS");
    let shared = vfs.shared_state();
    let vfs_name = unique_vfs_name("staging_overwrite");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = setup_and_insert(&vfs_name, "staging_overwrite.db", 50);

    // Checkpoint 1
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();

    // Overwrite all rows
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..50 {
            tx.execute(
                "UPDATE data SET value = ?2 WHERE id = ?1",
                rusqlite::params![i, format!("v2_{}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    // Checkpoint 2
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();

    // Flush
    shared.flush_to_s3().expect("flush failed");
    drop(conn);

    // Cold reader
    let cold_dir = TempDir::new().unwrap();
    let cold_config = TieredConfig {
        bucket,
        prefix,
        cache_dir: cold_dir.path().to_path_buf(),
        endpoint_url: endpoint,
        region: Some("auto".to_string()),
        read_only: true,
        ..Default::default()
    };
    let cold_vfs_name = unique_vfs_name("staging_overwrite_cold");
    let cold_vfs = TieredVfs::new(cold_config).expect("cold VFS");
    turbolite::tiered::register(&cold_vfs_name, cold_vfs).unwrap();

    let cold_conn = rusqlite::Connection::open_with_flags_and_vfs(
        "staging_overwrite.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &cold_vfs_name,
    )
    .unwrap();

    // All rows should have v2 values (from checkpoint 2's staging log)
    let val: String = cold_conn
        .query_row("SELECT value FROM data WHERE id = 25", [], |row| row.get(0))
        .unwrap();
    assert_eq!(val, "v2_25", "cold reader should see v2 values");

    let count: i64 = cold_conn
        .query_row("SELECT COUNT(*) FROM data", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 50);
}
