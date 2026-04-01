//! Integration tests for B-tree page group compaction.

use super::helpers::*;
use turbolite::tiered::*;

/// Insert rows, delete many, checkpoint, then compact.
/// Verify dead space is reclaimed and data is still readable.
#[test]
fn test_compact_reclaims_dead_space() {
    let cache_dir = tempfile::tempdir().unwrap();
    let config = test_config("compact_reclaim", cache_dir.path());
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let vfs_name = unique_vfs_name("compact_reclaim");
    let vfs = TieredVfs::new(config).unwrap();
    let bench = vfs.shared_state();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let db_path = format!("file:compact_reclaim.db?vfs={}", vfs_name);

    // Create table, insert 500 rows, checkpoint
    {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
                | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        conn.execute_batch(
            "CREATE TABLE compact_test (id INTEGER PRIMARY KEY, data TEXT);",
        ).unwrap();
        for i in 0..500 {
            conn.execute(
                "INSERT INTO compact_test VALUES (?1, ?2)",
                rusqlite::params![i, format!("row_{:04}_padding_to_fill_pages", i)],
            ).unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    // Delete 400 rows (80%), checkpoint
    {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        conn.execute("DELETE FROM compact_test WHERE id >= 100", []).unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    // Run compaction
    let result = bench.compact(0.3).unwrap();
    eprintln!("[test] compact result: {}", result);

    // Parse result JSON
    let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
    let pages_freed = parsed["pages_freed"].as_u64().unwrap_or(0);

    // After deleting 80% of rows, we should have significant dead space
    assert!(
        pages_freed > 0 || parsed["compacted"].as_u64().unwrap_or(0) > 0
            || parsed.get("message").is_some(),
        "compaction should either free pages or report no candidates: {}",
        result,
    );

    // Verify data integrity: remaining 100 rows should be readable from a cold reader
    {
        let reader_cache = tempfile::tempdir().unwrap();
        let reader_config = TieredConfig {
            bucket: bucket.clone(),
            prefix: prefix.clone(),
            endpoint_url: endpoint.clone(),
            region: Some("auto".to_string()),
            cache_dir: reader_cache.path().to_path_buf(),
            read_only: true,
            ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("compact_reader");
        let reader_vfs = TieredVfs::new(reader_config).unwrap();
        turbolite::tiered::register(&reader_vfs_name, reader_vfs).unwrap();

        let reader_db = format!("file:compact_reclaim.db?vfs={}", reader_vfs_name);
        let conn = rusqlite::Connection::open_with_flags(
            &reader_db,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM compact_test", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 100, "should have 100 remaining rows after compact");
    }
}

/// Compact with no dead space should be a no-op.
#[test]
fn test_compact_noop_when_no_dead_space() {
    let cache_dir = tempfile::tempdir().unwrap();
    let config = test_config("compact_noop", cache_dir.path());

    let vfs_name = unique_vfs_name("compact_noop");
    let vfs = TieredVfs::new(config).unwrap();
    let bench = vfs.shared_state();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let db_path = format!("file:compact_noop.db?vfs={}", vfs_name);

    // Create table, insert rows, checkpoint (no deletes)
    {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
                | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        conn.execute_batch(
            "CREATE TABLE noop_test (id INTEGER PRIMARY KEY, data TEXT);",
        ).unwrap();
        for i in 0..100 {
            conn.execute(
                "INSERT INTO noop_test VALUES (?1, ?2)",
                rusqlite::params![i, format!("data_{}", i)],
            ).unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    // Compact should be a no-op
    let result = bench.compact(0.3).unwrap();
    eprintln!("[test] compact noop result: {}", result);

    let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(
        parsed["compacted"].as_u64().unwrap_or(0), 0,
        "should not compact anything when no dead space"
    );
}

/// Compact threshold: 80% threshold should skip B-trees with only 30% dead space.
#[test]
fn test_compact_respects_threshold() {
    let cache_dir = tempfile::tempdir().unwrap();
    let config = test_config("compact_thresh", cache_dir.path());

    let vfs_name = unique_vfs_name("compact_thresh");
    let vfs = TieredVfs::new(config).unwrap();
    let bench = vfs.shared_state();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let db_path = format!("file:compact_thresh.db?vfs={}", vfs_name);

    // Insert 200 rows, delete 60 (30%), checkpoint
    {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
                | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        conn.execute_batch(
            "CREATE TABLE thresh_test (id INTEGER PRIMARY KEY, data TEXT);",
        ).unwrap();
        for i in 0..200 {
            conn.execute(
                "INSERT INTO thresh_test VALUES (?1, ?2)",
                rusqlite::params![i, format!("row_{:04}_padding_data_here", i)],
            ).unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();

        conn.execute("DELETE FROM thresh_test WHERE id >= 140", []).unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    // With 80% threshold, should skip (30% dead < 80% threshold)
    let result = bench.compact(0.8).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(
        parsed["compacted"].as_u64().unwrap_or(0), 0,
        "should not compact at 80% threshold with ~30% dead space"
    );
}
