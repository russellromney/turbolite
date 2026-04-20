//! Crash atomicity tests: verify database recovers correctly after interrupted
//! checkpoints. Runs across all TestMode variants.
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
    mode: TestMode,
    expected_count: i64,
    label: &str,
) {
    let cold_dir = TempDir::new().expect("cold dir");
    let cold_config = cold_reader_config_mode(bucket, prefix, endpoint, cold_dir.path(), mode);
    let vfs_name = unique_vfs_name("crash_cold");
    let vfs = TurboliteVfs::new_local(cold_config).expect("cold vfs");
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
    assert_eq!(count, expected_count, "{}: expected {} rows, got {}", label, expected_count, count);

    let first: i64 = conn
        .query_row("SELECT MIN(id) FROM data", [], |row| row.get(0))
        .expect("min query");
    assert_eq!(first, 0, "{}: first row should be 0", label);

    let last: i64 = conn
        .query_row("SELECT MAX(id) FROM data", [], |row| row.get(0))
        .expect("max query");
    assert_eq!(last, expected_count - 1, "{}: last row should be {}", label, expected_count - 1);

    let integrity: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity");
    assert_eq!(integrity, "ok", "{}: integrity check failed: {}", label, integrity);
}

/// Core crash recovery test: incremental checkpoints with cold-read after each.
fn run_crash_incremental(mode: TestMode) {
    let writer_dir = TempDir::new().expect("writer dir");
    let config = test_config_mode(&format!("crash_incr_{}", mode.name()), writer_dir.path(), mode);
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let vfs_name = unique_vfs_name(&format!("crash_incr_{}", mode.name()));

    let vfs = TurboliteVfs::new_local(config).expect("vfs");
    turbolite::tiered::register(&vfs_name, vfs).expect("register");

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        &format!("crash_incr_{}.db", mode.name()),
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .expect("open");

    conn.execute_batch(
        "PRAGMA page_size=65536; PRAGMA journal_mode=WAL;
         CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT);",
    )
    .expect("init");

    // Phase 1: insert 100, checkpoint, cold-verify
    {
        let tx = conn.unchecked_transaction().expect("tx");
        for i in 0..100 {
            tx.execute("INSERT INTO data VALUES (?1, ?2)", rusqlite::params![i, format!("v{}", i)])
                .expect("insert");
        }
        tx.commit().expect("commit");
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").expect("cp1");
    cold_read_and_verify(&bucket, &prefix, &endpoint, mode, 100, &format!("[{}] phase1", mode.name()));

    // Phase 2: insert 100 more, checkpoint, cold-verify
    {
        let tx = conn.unchecked_transaction().expect("tx");
        for i in 100..200 {
            tx.execute("INSERT INTO data VALUES (?1, ?2)", rusqlite::params![i, format!("v{}", i)])
                .expect("insert");
        }
        tx.commit().expect("commit");
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").expect("cp2");
    cold_read_and_verify(&bucket, &prefix, &endpoint, mode, 200, &format!("[{}] phase2", mode.name()));

    // Phase 3: update all, checkpoint, cold-verify
    conn.execute_batch("UPDATE data SET value = 'updated_' || id;").expect("update");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").expect("cp3");
    cold_read_and_verify(&bucket, &prefix, &endpoint, mode, 200, &format!("[{}] phase3", mode.name()));

    // Phase 4: delete half, vacuum, checkpoint, cold-verify
    conn.execute_batch("DELETE FROM data WHERE id >= 100;").expect("delete");
    conn.execute_batch("VACUUM;").expect("vacuum");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").expect("cp4");
    cold_read_and_verify(&bucket, &prefix, &endpoint, mode, 100, &format!("[{}] phase4", mode.name()));
}

/// Uncommitted data must not be visible in S3.
fn run_uncommitted_not_visible(mode: TestMode) {
    let writer_dir = TempDir::new().expect("writer dir");
    let config = test_config_mode(&format!("crash_uncommit_{}", mode.name()), writer_dir.path(), mode);
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let vfs_name = unique_vfs_name(&format!("crash_uncommit_{}", mode.name()));

    let vfs = TurboliteVfs::new_local(config).expect("vfs");
    turbolite::tiered::register(&vfs_name, vfs).expect("register");

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        &format!("crash_uncommit_{}.db", mode.name()),
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .expect("open");

    conn.execute_batch(
        "PRAGMA page_size=65536; PRAGMA journal_mode=WAL;
         CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT);",
    )
    .expect("init");

    // Baseline: 50 rows, checkpoint
    {
        let tx = conn.unchecked_transaction().expect("tx");
        for i in 0..50 {
            tx.execute("INSERT INTO data VALUES (?1, ?2)", rusqlite::params![i, format!("baseline_{}", i)])
                .expect("insert");
        }
        tx.commit().expect("commit");
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").expect("cp");

    // 50 more rows, NO checkpoint
    {
        let tx = conn.unchecked_transaction().expect("tx");
        for i in 50..100 {
            tx.execute("INSERT INTO data VALUES (?1, ?2)", rusqlite::params![i, format!("uncheckpointed_{}", i)])
                .expect("insert");
        }
        tx.commit().expect("commit");
    }

    // Cold reader should only see baseline
    cold_read_and_verify(&bucket, &prefix, &endpoint, mode, 50, &format!("[{}] uncommitted", mode.name()));
}

/// Large dataset spanning multiple page groups.
fn run_large_dataset(mode: TestMode) {
    let writer_dir = TempDir::new().expect("writer dir");
    let config = test_config_mode(&format!("crash_large_{}", mode.name()), writer_dir.path(), mode);
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let vfs_name = unique_vfs_name(&format!("crash_large_{}", mode.name()));

    let vfs = TurboliteVfs::new_local(config).expect("vfs");
    turbolite::tiered::register(&vfs_name, vfs).expect("register");

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        &format!("crash_large_{}.db", mode.name()),
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .expect("open");

    conn.execute_batch(
        "PRAGMA page_size=65536; PRAGMA journal_mode=WAL;
         CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT, payload BLOB);",
    )
    .expect("init");

    {
        let tx = conn.unchecked_transaction().expect("tx");
        for i in 0..10_000 {
            let payload = vec![(i % 256) as u8; (i % 500) + 100];
            tx.execute(
                "INSERT INTO data VALUES (?1, ?2, ?3)",
                rusqlite::params![i, format!("row_{}", i), payload],
            )
            .expect("insert");
        }
        tx.commit().expect("commit");
    }

    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").expect("checkpoint");

    // Cold read: verify count and spot-check rows
    let cold_dir = TempDir::new().expect("cold dir");
    let cold_config = cold_reader_config_mode(&bucket, &prefix, &endpoint, cold_dir.path(), mode);
    let cold_name = unique_vfs_name(&format!("crash_large_{}_cold", mode.name()));
    let cold_vfs = TurboliteVfs::new_local(cold_config).expect("cold vfs");
    turbolite::tiered::register(&cold_name, cold_vfs).expect("register cold");

    let cold = rusqlite::Connection::open_with_flags_and_vfs(
        &format!("crash_large_{}_cold.db", mode.name()),
        OpenFlags::SQLITE_OPEN_READ_ONLY,
        &cold_name,
    )
    .expect("open cold");

    let count: i64 = cold
        .query_row("SELECT COUNT(*) FROM data", [], |row| row.get(0))
        .expect("count");
    assert_eq!(count, 10_000, "[{}] expected 10000 rows", mode.name());

    for check_id in [0, 42, 999, 5000, 9999] {
        let val: String = cold
            .query_row("SELECT value FROM data WHERE id = ?1", rusqlite::params![check_id], |row| row.get(0))
            .expect("get row");
        assert_eq!(val, format!("row_{}", check_id), "[{}] value mismatch for id {}", mode.name(), check_id);

        let payload_len: i64 = cold
            .query_row("SELECT length(payload) FROM data WHERE id = ?1", rusqlite::params![check_id], |row| row.get(0))
            .expect("get payload len");
        assert_eq!(
            payload_len, ((check_id % 500) + 100) as i64,
            "[{}] payload length mismatch for id {}", mode.name(), check_id
        );
    }

    let integrity: String = cold
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity");
    assert_eq!(integrity, "ok", "[{}] large dataset integrity failed", mode.name());
}

// --- Parameterized tests: crash recovery x all S3 Durable mode combinations ---

#[test]
fn crash_incremental_all_modes() {
    run_across_s3_durable(run_crash_incremental);
}

#[test]
fn crash_uncommitted_all_modes() {
    run_across_s3_durable(run_uncommitted_not_visible);
}

#[test]
fn crash_large_all_modes() {
    run_across_s3_durable(run_large_dataset);
}
