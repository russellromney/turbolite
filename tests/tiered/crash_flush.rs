//! Crash atomicity tests: verify database recovers correctly after interrupted
//! checkpoints.
//!
//! Strategy: write data, checkpoint to S3, then open a fresh VFS from the same
//! S3 prefix and verify all pre-checkpoint data is readable and passes integrity
//! check. This exercises the crash recovery path without actually killing the
//! process (which is hard to do in a test).
//!
//! The key invariant: after any successful checkpoint, a cold reader opening
//! from S3 must see exactly the data that was committed before the checkpoint.

use rusqlite::OpenFlags;
use tempfile::TempDir;
use turbolite::tiered::{TurboliteConfig, TurboliteVfs};

use super::helpers::*;

/// Helper: open a fresh read-only VFS pointing at the same S3 prefix, verify data.
fn cold_read_and_verify(
    bucket: &str,
    prefix: &str,
    endpoint: &Option<String>,
    expected_count: i64,
    label: &str,
) {
    let cold_dir = TempDir::new().expect("cold dir");
    let cold_config = TurboliteConfig {
        bucket: bucket.to_string(),
        prefix: prefix.to_string(),
        cache_dir: cold_dir.path().to_path_buf(),
        endpoint_url: endpoint.clone(),
        region: Some("auto".to_string()),
        read_only: true,
        runtime_handle: Some(shared_runtime_handle()),
        ..Default::default()
    };
    let vfs_name = unique_vfs_name("crash_cold");
    let vfs = TurboliteVfs::new(cold_config).expect("cold vfs");
    turbolite::tiered::register(&vfs_name, vfs).expect("register cold");

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "crash_cold_test.db",
        OpenFlags::SQLITE_OPEN_READ_ONLY,
        &vfs_name,
    )
    .expect("cold open");

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM data", [], |row| row.get(0))
        .expect("count query");
    assert_eq!(
        count, expected_count,
        "{}: expected {} rows, got {}",
        label, expected_count, count
    );

    // Verify specific rows
    let first: i64 = conn
        .query_row("SELECT MIN(id) FROM data", [], |row| row.get(0))
        .expect("min query");
    assert_eq!(first, 0, "{}: first row should be 0", label);

    let last: i64 = conn
        .query_row("SELECT MAX(id) FROM data", [], |row| row.get(0))
        .expect("max query");
    assert_eq!(
        last,
        expected_count - 1,
        "{}: last row should be {}",
        label,
        expected_count - 1
    );

    // Integrity check
    let integrity: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity");
    assert_eq!(
        integrity, "ok",
        "{}: integrity check failed: {}",
        label, integrity
    );
}

/// After each checkpoint, a cold reader must see exactly the committed data.
#[test]
fn crash_recovery_incremental_checkpoints() {
    let writer_dir = TempDir::new().expect("writer dir");
    let config = test_config("crash_incr", writer_dir.path());
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let vfs_name = unique_vfs_name("crash_incr");

    let vfs = TurboliteVfs::new(config).expect("vfs");
    turbolite::tiered::register(&vfs_name, vfs).expect("register");

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "crash_incr_test.db",
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .expect("open");

    conn.execute_batch(
        "PRAGMA page_size=65536;
         PRAGMA journal_mode=WAL;
         CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT);",
    )
    .expect("init");

    // Phase 1: insert 100, checkpoint
    {
        let tx = conn.unchecked_transaction().expect("tx");
        for i in 0..100 {
            tx.execute(
                "INSERT INTO data (id, value) VALUES (?1, ?2)",
                rusqlite::params![i, format!("v{}", i)],
            )
            .expect("insert");
        }
        tx.commit().expect("commit");
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("cp1");
    cold_read_and_verify(&bucket, &prefix, &endpoint, 100, "phase1");

    // Phase 2: insert 100 more, checkpoint
    {
        let tx = conn.unchecked_transaction().expect("tx");
        for i in 100..200 {
            tx.execute(
                "INSERT INTO data (id, value) VALUES (?1, ?2)",
                rusqlite::params![i, format!("v{}", i)],
            )
            .expect("insert");
        }
        tx.commit().expect("commit");
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("cp2");
    cold_read_and_verify(&bucket, &prefix, &endpoint, 200, "phase2");

    // Phase 3: update all rows, checkpoint
    conn.execute_batch("UPDATE data SET value = 'updated_' || id;")
        .expect("update");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("cp3");
    cold_read_and_verify(&bucket, &prefix, &endpoint, 200, "phase3_count");

    // Verify updated values via cold read
    {
        let cold_dir = TempDir::new().expect("cold dir");
        let cold_config = TurboliteConfig {
            bucket: bucket.clone(),
            prefix: prefix.clone(),
            cache_dir: cold_dir.path().to_path_buf(),
            endpoint_url: endpoint.clone(),
            region: Some("auto".to_string()),
            read_only: true,
            runtime_handle: Some(shared_runtime_handle()),
            ..Default::default()
        };
        let cvn = unique_vfs_name("crash_verify_update");
        let cvfs = TurboliteVfs::new(cold_config).expect("cold vfs");
        turbolite::tiered::register(&cvn, cvfs).expect("register");

        let cold = rusqlite::Connection::open_with_flags_and_vfs(
            "crash_verify_update.db",
            OpenFlags::SQLITE_OPEN_READ_ONLY,
            &cvn,
        )
        .expect("open");

        let val: String = cold
            .query_row("SELECT value FROM data WHERE id = 42", [], |row| row.get(0))
            .expect("get 42");
        assert_eq!(val, "updated_42", "update not visible in cold read");
    }

    // Phase 4: delete half, vacuum, checkpoint
    conn.execute_batch("DELETE FROM data WHERE id >= 100;")
        .expect("delete");
    conn.execute_batch("VACUUM;").expect("vacuum");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("cp4");
    cold_read_and_verify(&bucket, &prefix, &endpoint, 100, "phase4_after_vacuum");
}

/// Write uncommitted data (no checkpoint), verify cold reader sees nothing.
/// This proves the checkpoint is the durability boundary.
#[test]
fn uncommitted_data_not_visible_in_s3() {
    let writer_dir = TempDir::new().expect("writer dir");
    let config = test_config("crash_uncommit", writer_dir.path());
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let vfs_name = unique_vfs_name("crash_uncommit");

    let vfs = TurboliteVfs::new(config).expect("vfs");
    turbolite::tiered::register(&vfs_name, vfs).expect("register");

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "crash_uncommit_test.db",
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .expect("open");

    conn.execute_batch(
        "PRAGMA page_size=65536;
         PRAGMA journal_mode=WAL;
         CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT);",
    )
    .expect("init");

    // Insert 50 rows and checkpoint (the baseline)
    {
        let tx = conn.unchecked_transaction().expect("tx");
        for i in 0..50 {
            tx.execute(
                "INSERT INTO data (id, value) VALUES (?1, ?2)",
                rusqlite::params![i, format!("baseline_{}", i)],
            )
            .expect("insert");
        }
        tx.commit().expect("commit");
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("checkpoint baseline");

    // Insert 50 more rows but do NOT checkpoint
    {
        let tx = conn.unchecked_transaction().expect("tx");
        for i in 50..100 {
            tx.execute(
                "INSERT INTO data (id, value) VALUES (?1, ?2)",
                rusqlite::params![i, format!("uncheckpointed_{}", i)],
            )
            .expect("insert");
        }
        tx.commit().expect("commit");
    }
    // NO checkpoint here -- simulates crash before checkpoint

    // Cold reader should only see the 50 baseline rows
    cold_read_and_verify(
        &bucket,
        &prefix,
        &endpoint,
        50,
        "uncommitted_not_visible",
    );
}

/// Large dataset: 10,000 rows across multiple page groups, verify cold read.
#[test]
fn crash_recovery_large_dataset() {
    let writer_dir = TempDir::new().expect("writer dir");
    let config = test_config("crash_large", writer_dir.path());
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let vfs_name = unique_vfs_name("crash_large");

    let vfs = TurboliteVfs::new(config).expect("vfs");
    turbolite::tiered::register(&vfs_name, vfs).expect("register");

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "crash_large_test.db",
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .expect("open");

    conn.execute_batch(
        "PRAGMA page_size=65536;
         PRAGMA journal_mode=WAL;
         CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT, payload BLOB);",
    )
    .expect("init");

    // Insert 10,000 rows with varying payload sizes to span multiple page groups
    {
        let tx = conn.unchecked_transaction().expect("tx");
        for i in 0..10_000 {
            let payload = vec![(i % 256) as u8; (i % 500) + 100];
            tx.execute(
                "INSERT INTO data (id, value, payload) VALUES (?1, ?2, ?3)",
                rusqlite::params![i, format!("row_{}", i), payload],
            )
            .expect("insert");
        }
        tx.commit().expect("commit");
    }

    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("checkpoint");

    // Cold read verification
    let cold_dir = TempDir::new().expect("cold dir");
    let cold_config = TurboliteConfig {
        bucket: bucket.clone(),
        prefix: prefix.clone(),
        cache_dir: cold_dir.path().to_path_buf(),
        endpoint_url: endpoint.clone(),
        region: Some("auto".to_string()),
        read_only: true,
        runtime_handle: Some(shared_runtime_handle()),
        ..Default::default()
    };
    let cold_name = unique_vfs_name("crash_large_cold");
    let cold_vfs = TurboliteVfs::new(cold_config).expect("cold vfs");
    turbolite::tiered::register(&cold_name, cold_vfs).expect("register cold");

    let cold = rusqlite::Connection::open_with_flags_and_vfs(
        "crash_large_cold.db",
        OpenFlags::SQLITE_OPEN_READ_ONLY,
        &cold_name,
    )
    .expect("open cold");

    // Verify count
    let count: i64 = cold
        .query_row("SELECT COUNT(*) FROM data", [], |row| row.get(0))
        .expect("count");
    assert_eq!(count, 10_000, "expected 10000 rows in cold read");

    // Verify random samples
    for check_id in [0, 42, 999, 5000, 9999] {
        let val: String = cold
            .query_row(
                "SELECT value FROM data WHERE id = ?1",
                rusqlite::params![check_id],
                |row| row.get(0),
            )
            .expect("get row");
        assert_eq!(val, format!("row_{}", check_id));

        // Verify payload length
        let payload_len: i64 = cold
            .query_row(
                "SELECT length(payload) FROM data WHERE id = ?1",
                rusqlite::params![check_id],
                |row| row.get(0),
            )
            .expect("get payload len");
        assert_eq!(
            payload_len,
            ((check_id % 500) + 100) as i64,
            "payload length mismatch for id {}",
            check_id
        );
    }

    // Full integrity check
    let integrity: String = cold
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity");
    assert_eq!(integrity, "ok", "large dataset integrity check failed");
}
