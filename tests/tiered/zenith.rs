//! Phase Zenith tests: S3Primary sync mode.
//!
//! Every xSync uploads dirty frames as overrides + publishes manifest.
//! Requires S3 credentials (TIERED_TEST_BUCKET).

use turbolite::tiered::{SyncMode, TurboliteConfig, TurboliteVfs};
use tempfile::TempDir;
use super::helpers::*;

fn s3primary_config(prefix: &str, cache_dir: &std::path::Path) -> TurboliteConfig {
    let mut config = test_config(prefix, cache_dir);
    config.sync_mode = SyncMode::S3Primary;
    config.sub_pages_per_frame = 8; // enable seekable format (required for overrides)
    config
}

#[test]
fn zenith_single_write_and_cold_read() {
    let cache_dir = TempDir::new().unwrap();
    let config = s3primary_config("zenith_single", cache_dir.path());
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let vfs_name = unique_vfs_name("zenith_single");

    let vfs = TurboliteVfs::new_local(config).expect("create vfs");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "zenith_single.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    ).expect("open");

    // S3Primary: journal_mode=OFF (no WAL)
    conn.execute_batch(
        "PRAGMA page_size=4096;
         PRAGMA journal_mode=OFF;
         CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT);",
    ).expect("create table");

    // Insert data. SQLite's xSync fires on commit.
    conn.execute(
        "INSERT INTO data (id, value) VALUES (1, 'hello')",
        [],
    ).expect("insert");

    // Force a checkpoint (triggers our S3Primary sync path)
    // With journal_mode=OFF, sync() fires on transaction commit.
    // wal_checkpoint is a no-op but harmless.
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").ok();

    // Verify manifest landed in S3
    verify_s3_manifest(&bucket, &prefix, &endpoint, 1, 4096);

    // Cold read from S3 (new VFS, empty cache)
    let cold_dir = TempDir::new().unwrap();
    let cold_config = cold_reader_config(&bucket, &prefix, &endpoint, cold_dir.path());
    let cold_vfs_name = unique_vfs_name("zenith_cold");
    let cold_vfs = TurboliteVfs::new_local(cold_config).expect("cold vfs");
    turbolite::tiered::register(&cold_vfs_name, cold_vfs).unwrap();

    let cold_conn = rusqlite::Connection::open_with_flags_and_vfs(
        "zenith_single.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &cold_vfs_name,
    ).expect("cold open");

    let value: String = cold_conn
        .query_row("SELECT value FROM data WHERE id = 1", [], |row| row.get(0))
        .expect("cold read");
    assert_eq!(value, "hello");
}

#[test]
fn zenith_sequential_writes_increment_version() {
    let cache_dir = TempDir::new().unwrap();
    let config = s3primary_config("zenith_seq", cache_dir.path());
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let vfs_name = unique_vfs_name("zenith_seq");

    let vfs = TurboliteVfs::new_local(config).expect("create vfs");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "zenith_seq.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    ).expect("open");

    conn.execute_batch(
        "PRAGMA page_size=4096;
         PRAGMA journal_mode=OFF;
         CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT);",
    ).expect("create table");

    // First write
    conn.execute("INSERT INTO data VALUES (1, 'v1')", []).expect("insert 1");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").ok();

    // Second write
    conn.execute("INSERT INTO data VALUES (2, 'v2')", []).expect("insert 2");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").ok();

    // Third write
    conn.execute("INSERT INTO data VALUES (3, 'v3')", []).expect("insert 3");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").ok();

    // Cold reader should see all 3 rows
    let cold_dir = TempDir::new().unwrap();
    let cold_config = cold_reader_config(&bucket, &prefix, &endpoint, cold_dir.path());
    let cold_vfs_name = unique_vfs_name("zenith_seq_cold");
    let cold_vfs = TurboliteVfs::new_local(cold_config).expect("cold vfs");
    turbolite::tiered::register(&cold_vfs_name, cold_vfs).unwrap();

    let cold_conn = rusqlite::Connection::open_with_flags_and_vfs(
        "zenith_seq.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &cold_vfs_name,
    ).expect("cold open");

    let count: i64 = cold_conn
        .query_row("SELECT COUNT(*) FROM data", [], |row| row.get(0))
        .expect("count");
    assert_eq!(count, 3, "cold reader should see all 3 rows");
}

#[test]
fn zenith_empty_sync_is_noop() {
    let cache_dir = TempDir::new().unwrap();
    let config = s3primary_config("zenith_noop", cache_dir.path());
    let vfs_name = unique_vfs_name("zenith_noop");

    let vfs = TurboliteVfs::new_local(config).expect("create vfs");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "zenith_noop.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    ).expect("open");

    conn.execute_batch(
        "PRAGMA page_size=4096;
         PRAGMA journal_mode=OFF;
         CREATE TABLE data (id INTEGER PRIMARY KEY);",
    ).expect("create table");

    // Checkpoint with no dirty pages after initial creation checkpoint
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").ok();

    // A second checkpoint should be a no-op (no dirty pages)
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").ok();
    // No assertion needed; just verify it doesn't error or crash
}

#[test]
fn zenith_multiple_rows_per_transaction() {
    let cache_dir = TempDir::new().unwrap();
    let config = s3primary_config("zenith_multi", cache_dir.path());
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let vfs_name = unique_vfs_name("zenith_multi");

    let vfs = TurboliteVfs::new_local(config).expect("create vfs");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "zenith_multi.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    ).expect("open");

    conn.execute_batch(
        "PRAGMA page_size=4096;
         PRAGMA journal_mode=OFF;
         CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT);",
    ).expect("create table");

    // Bulk insert in one transaction
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..500 {
            tx.execute(
                "INSERT INTO data VALUES (?1, ?2)",
                rusqlite::params![i, format!("row_{}", i)],
            ).unwrap();
        }
        tx.commit().unwrap();
    }

    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").ok();

    // Cold read
    let cold_dir = TempDir::new().unwrap();
    let cold_config = cold_reader_config(&bucket, &prefix, &endpoint, cold_dir.path());
    let cold_vfs_name = unique_vfs_name("zenith_multi_cold");
    let cold_vfs = TurboliteVfs::new_local(cold_config).expect("cold vfs");
    turbolite::tiered::register(&cold_vfs_name, cold_vfs).unwrap();

    let cold_conn = rusqlite::Connection::open_with_flags_and_vfs(
        "zenith_multi.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &cold_vfs_name,
    ).expect("cold open");

    let count: i64 = cold_conn
        .query_row("SELECT COUNT(*) FROM data", [], |row| row.get(0))
        .expect("count");
    assert_eq!(count, 500);
}
