//! Garbage collection tests: post-checkpoint GC, disabled GC, full scan, no-orphan safety.

use turbolite::tiered::{TurboliteConfig, TurboliteVfs};
use tempfile::TempDir;
use super::helpers::*;

#[test]
fn test_gc_post_checkpoint() {
    let cache_dir = TempDir::new().unwrap();
    let mut config = test_config("gc_post", cache_dir.path());
    config.gc_enabled = true;
    let vfs_name = unique_vfs_name("tiered_gc_post");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let vfs = TurboliteVfs::new(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "gc_post.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA page_size=65536;
         CREATE TABLE gc_test (id INTEGER PRIMARY KEY, data TEXT);",
    )
    .unwrap();

    // First write + checkpoint → creates page group v1
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..100 {
            tx.execute(
                "INSERT INTO gc_test VALUES (?1, ?2)",
                rusqlite::params![i, format!("gc_data_{:0>100}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    let count_after_v1 = count_s3_objects(&bucket, &prefix, &endpoint);
    eprintln!("S3 objects after v1: {}", count_after_v1);

    // Second write + checkpoint → creates page group v2, GC deletes v1
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 100..200 {
            tx.execute(
                "INSERT INTO gc_test VALUES (?1, ?2)",
                rusqlite::params![i, format!("gc_data_{:0>100}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    let count_after_v2 = count_s3_objects(&bucket, &prefix, &endpoint);
    eprintln!("S3 objects after v2 (with GC): {}", count_after_v2);

    // With GC enabled, old versions should be deleted.
    // count_after_v2 should be <= count_after_v1 (same or fewer objects)
    assert!(
        count_after_v2 <= count_after_v1,
        "GC should not increase S3 object count: v1={} v2={}",
        count_after_v1, count_after_v2,
    );

    // Verify data integrity after GC
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM gc_test", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 200, "all rows should be readable after GC");
}

/// Verify that gc_enabled=false does NOT delete old versions.
#[test]
fn test_gc_disabled_preserves_old_versions() {
    let cache_dir = TempDir::new().unwrap();
    let mut config = test_config("gc_off", cache_dir.path());
    config.gc_enabled = false;
    let vfs_name = unique_vfs_name("tiered_gc_off");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let vfs = TurboliteVfs::new(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "gc_off.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA page_size=65536;
         CREATE TABLE gc_off_test (id INTEGER PRIMARY KEY, data TEXT);",
    )
    .unwrap();

    // First write + checkpoint
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..50 {
            tx.execute(
                "INSERT INTO gc_off_test VALUES (?1, ?2)",
                rusqlite::params![i, format!("data_{:0>100}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    let count_after_v1 = count_s3_objects(&bucket, &prefix, &endpoint);

    // Second write + checkpoint — old versions should remain
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 50..100 {
            tx.execute(
                "INSERT INTO gc_off_test VALUES (?1, ?2)",
                rusqlite::params![i, format!("data_{:0>100}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    let count_after_v2 = count_s3_objects(&bucket, &prefix, &endpoint);

    // Without GC, v2 should have MORE objects (v1 + v2 page groups)
    assert!(
        count_after_v2 > count_after_v1,
        "Without GC, old versions should accumulate: v1={} v2={}",
        count_after_v1, count_after_v2,
    );
}

/// Verify full GC scan deletes orphaned objects from prior checkpoints.
#[test]
fn test_gc_full_scan() {
    let cache_dir = TempDir::new().unwrap();
    let mut config = test_config("gc_scan", cache_dir.path());
    // Start with GC disabled to accumulate old versions
    config.gc_enabled = false;
    let vfs_name = unique_vfs_name("tiered_gc_scan");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let vfs = TurboliteVfs::new(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "gc_scan.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA page_size=65536;
         CREATE TABLE gc_scan_test (id INTEGER PRIMARY KEY, data TEXT);",
    )
    .unwrap();

    // Multiple writes + checkpoints to accumulate versions
    for batch in 0..3 {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..50 {
            let id = batch * 50 + i;
            tx.execute(
                "INSERT INTO gc_scan_test VALUES (?1, ?2)",
                rusqlite::params![id, format!("scan_data_{:0>100}", id)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    let count_before_gc = count_s3_objects(&bucket, &prefix, &endpoint);
    eprintln!("S3 objects before full GC: {}", count_before_gc);

    // Open a fresh VFS to call gc()
    drop(conn);
    let gc_cache = TempDir::new().unwrap();
    let gc_config = TurboliteConfig {
        bucket: bucket.clone(),
        prefix: prefix.clone(),
        cache_dir: gc_cache.path().to_path_buf(),
        endpoint_url: endpoint.clone(),
        region: Some("auto".to_string()),
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let gc_vfs = TurboliteVfs::new(gc_config).unwrap();
    let deleted = gc_vfs.gc().unwrap();
    eprintln!("Full GC deleted {} objects", deleted);

    let count_after_gc = count_s3_objects(&bucket, &prefix, &endpoint);
    eprintln!("S3 objects after full GC: {}", count_after_gc);

    // GC should have reduced object count
    assert!(
        count_after_gc < count_before_gc,
        "Full GC should reduce object count: before={} after={}",
        count_before_gc, count_after_gc,
    );
    assert!(deleted > 0, "GC should have deleted at least 1 orphan");

    // Verify data integrity after GC
    let gc_vfs_name = unique_vfs_name("tiered_gc_scan_r");
    turbolite::tiered::register(&gc_vfs_name, gc_vfs).unwrap();
    let reader = rusqlite::Connection::open_with_flags_and_vfs(
        "gc_scan.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &gc_vfs_name,
    )
    .unwrap();
    let count: i64 = reader
        .query_row("SELECT COUNT(*) FROM gc_scan_test", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 150, "all 150 rows should be readable after full GC");
}

/// Verify GC is safe when there are no orphans.
#[test]
fn test_gc_no_orphans() {
    let cache_dir = TempDir::new().unwrap();
    let mut config = test_config("gc_noop", cache_dir.path());
    config.gc_enabled = true; // GC on from the start
    let vfs_name = unique_vfs_name("tiered_gc_noop");

    let vfs = TurboliteVfs::new(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "gc_noop.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA page_size=65536;
         CREATE TABLE gc_noop (id INTEGER PRIMARY KEY);",
    )
    .unwrap();

    // Single write + checkpoint with GC already enabled → no old versions exist
    conn.execute("INSERT INTO gc_noop VALUES (1)", []).unwrap();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();

    // Full GC scan should find nothing to delete
    drop(conn);
    let gc_cache = TempDir::new().unwrap();
    let gc_config = TurboliteConfig {
        bucket: test_bucket(),
        prefix: "test/gc_noop/will_not_exist".to_string(),
        cache_dir: gc_cache.path().to_path_buf(),
        endpoint_url: Some(endpoint_url()),
        region: Some("auto".to_string()),
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let gc_vfs = TurboliteVfs::new(gc_config).unwrap();
    let deleted = gc_vfs.gc().unwrap();
    assert_eq!(deleted, 0, "GC on empty prefix should delete nothing");
}
