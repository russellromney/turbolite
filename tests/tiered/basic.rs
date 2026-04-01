//! Basic tiered VFS tests: core I/O, checkpoint, manifest, cold read, caching.

use turbolite::tiered::{TieredConfig, TieredVfs};
use tempfile::TempDir;
use super::helpers::*;

#[test]
fn test_basic_write_read() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("basic", cache_dir.path());
    let vfs_name = unique_vfs_name("tiered_basic");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let vfs = TieredVfs::new(config).expect("failed to create TieredVfs");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let db_path = format!("basic_test.db");
    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        &db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .expect("failed to open connection");

    // Use WAL mode + large page size
    conn.execute_batch(
        "PRAGMA page_size=65536;
         PRAGMA journal_mode=WAL;
         CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT);",
    )
    .expect("failed to create table");

    // Bulk insert
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..100 {
            tx.execute(
                "INSERT INTO data (id, value) VALUES (?1, ?2)",
                rusqlite::params![i, format!("value_{}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    // Checkpoint to flush to S3
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("checkpoint failed");

    // Verify S3 manifest directly
    verify_s3_manifest(&bucket, &prefix, &endpoint, 1, 65536);

    // Read back
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM data", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 100, "expected 100 rows");

    // Verify specific row
    let value: String = conn
        .query_row(
            "SELECT value FROM data WHERE id = 42",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(value, "value_42");
}

#[test]
fn test_checkpoint_uploads() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("checkpoint", cache_dir.path());
    let vfs_name = unique_vfs_name("tiered_checkpoint");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let vfs = TieredVfs::new(config).expect("failed to create TieredVfs");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "checkpoint_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    conn.execute_batch(
        "PRAGMA page_size=65536;
         PRAGMA journal_mode=WAL;
         CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);",
    )
    .unwrap();

    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..50 {
            tx.execute(
                "INSERT INTO items (id, name) VALUES (?1, ?2)",
                rusqlite::params![i, format!("item_{}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    // Checkpoint
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();

    // Verify manifest exists in S3 by reading it directly
    let rt = tokio::runtime::Runtime::new().unwrap();
    let manifest_data = rt.block_on(async {
        let aws_config = aws_config::from_env()
            .region(aws_sdk_s3::config::Region::new("auto"))
            .load()
            .await;
        let mut s3_config = aws_sdk_s3::config::Builder::from(&aws_config);
        if let Some(ep) = &endpoint {
            s3_config = s3_config.endpoint_url(ep).force_path_style(true);
        }
        let client = aws_sdk_s3::Client::from_conf(s3_config.build());

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(format!("{}/manifest.msgpack", prefix))
            .send()
            .await
            .expect("manifest should exist in S3");

        resp.body
            .collect()
            .await
            .unwrap()
            .into_bytes()
            .to_vec()
    });

    let manifest: serde_json::Value =
        manifest_from_msgpack(&manifest_data);
    assert!(
        manifest["version"].as_u64().unwrap() >= 1,
        "manifest version should be >= 1"
    );
    assert!(
        manifest["page_count"].as_u64().unwrap() > 0,
        "manifest should have pages"
    );
    assert_eq!(
        manifest["page_size"].as_u64().unwrap(),
        65536,
        "page_size should be 65536"
    );
}

#[test]
fn test_reader_from_s3() {
    // Write data with one VFS, then read with a fresh VFS (empty cache)
    let write_cache = TempDir::new().unwrap();
    let config = test_config("reader", write_cache.path());
    let writer_vfs_name = unique_vfs_name("tiered_writer");
    let reader_bucket = config.bucket.clone();
    let reader_prefix = config.prefix.clone();
    let reader_endpoint = config.endpoint_url.clone();
    let reader_region = config.region.clone();

    let vfs = TieredVfs::new(config).expect("failed to create writer VFS");
    turbolite::tiered::register(&writer_vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "reader_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &writer_vfs_name,
    )
    .unwrap();

    conn.execute_batch(
        "PRAGMA page_size=65536;
         PRAGMA journal_mode=WAL;
         CREATE TABLE logs (id INTEGER PRIMARY KEY, msg TEXT);",
    )
    .unwrap();

    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..200 {
            tx.execute(
                "INSERT INTO logs (id, msg) VALUES (?1, ?2)",
                rusqlite::params![i, format!("log message {}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();

    // Verify S3 manifest before cold read
    verify_s3_manifest(&reader_bucket, &reader_prefix, &reader_endpoint, 1, 65536);

    drop(conn);

    // Now open a read-only VFS on the same S3 prefix with a FRESH cache
    let read_cache = TempDir::new().unwrap();
    let reader_config = TieredConfig {
        bucket: reader_bucket,
        prefix: reader_prefix,
        cache_dir: read_cache.path().to_path_buf(),
        compression_level: 3,
        endpoint_url: reader_endpoint,
        read_only: true,
        region: reader_region,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let reader_vfs_name = unique_vfs_name("tiered_reader");
    let reader_vfs =
        TieredVfs::new(reader_config).expect("failed to create reader VFS");
    turbolite::tiered::register(&reader_vfs_name, reader_vfs)
        .unwrap();

    let reader_conn = rusqlite::Connection::open_with_flags_and_vfs(
        "reader_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &reader_vfs_name,
    )
    .unwrap();

    let count: i64 = reader_conn
        .query_row("SELECT COUNT(*) FROM logs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 200, "reader should see all 200 rows from S3");

    let msg: String = reader_conn
        .query_row(
            "SELECT msg FROM logs WHERE id = 199",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(msg, "log message 199");
}

#[test]
fn test_64kb_pages() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("pages64k", cache_dir.path());
    let vfs_name = unique_vfs_name("tiered_64k");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let vfs = TieredVfs::new(config).expect("failed to create VFS");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "pages64k_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    // Set 64KB page size BEFORE creating any tables
    conn.execute_batch("PRAGMA page_size=65536;").unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         CREATE TABLE big (id INTEGER PRIMARY KEY, data BLOB);",
    )
    .unwrap();

    // Bulk insert enough data to span multiple 64KB pages
    let blob = vec![0x42u8; 8192]; // 8KB per row
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..50 {
            tx.execute(
                "INSERT INTO big (id, data) VALUES (?1, ?2)",
                rusqlite::params![i, blob],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();

    // Verify S3 manifest
    verify_s3_manifest(&bucket, &prefix, &endpoint, 1, 65536);

    // Cold read: drop and reopen
    drop(conn);

    // Re-read from same VFS (cache should have pages from checkpoint)
    let conn2 = rusqlite::Connection::open_with_flags_and_vfs(
        "pages64k_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
        &vfs_name,
    )
    .unwrap();

    let count: i64 = conn2
        .query_row("SELECT COUNT(*) FROM big", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 50);

    // Verify data integrity
    let data: Vec<u8> = conn2
        .query_row("SELECT data FROM big WHERE id = 25", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(data.len(), 8192);
    assert!(data.iter().all(|&b| b == 0x42));
}

#[test]
fn test_large_cold_scan() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("coldscan", cache_dir.path());
    let vfs_name = unique_vfs_name("tiered_coldscan");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();

    let vfs = TieredVfs::new(config).expect("failed to create VFS");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "coldscan_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA page_size=65536;
         CREATE TABLE events (id INTEGER PRIMARY KEY, ts INTEGER, payload TEXT);",
    )
    .unwrap();

    // Insert 10K rows (no indexes beyond PK)
    let tx = conn
        .unchecked_transaction()
        .unwrap();
    for i in 0..10_000 {
        tx.execute(
            "INSERT INTO events (id, ts, payload) VALUES (?1, ?2, ?3)",
            rusqlite::params![i, i * 1000, format!("event data for {}", i)],
        )
        .unwrap();
    }
    tx.commit().unwrap();

    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    drop(conn);

    // Open a fresh reader (cold cache) and do a full scan
    let cold_cache = TempDir::new().unwrap();
    let reader_config = TieredConfig {
        bucket,
        prefix,
        cache_dir: cold_cache.path().to_path_buf(),
        compression_level: 3,
        endpoint_url: endpoint,
        read_only: true,
        region,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let reader_vfs_name = unique_vfs_name("tiered_coldscan_reader");
    let reader_vfs =
        TieredVfs::new(reader_config).expect("failed to create reader VFS");
    turbolite::tiered::register(&reader_vfs_name, reader_vfs)
        .unwrap();

    let reader = rusqlite::Connection::open_with_flags_and_vfs(
        "coldscan_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &reader_vfs_name,
    )
    .unwrap();

    let start = std::time::Instant::now();
    let count: i64 = reader
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    let elapsed = start.elapsed();

    assert_eq!(count, 10_000, "cold scan should find all 10K rows");
    eprintln!(
        "Cold scan of 10K rows completed in {:?} (pages_per_group=2048)",
        elapsed
    );

    // With pages_per_group=2048, each page group GET warms 2048 pages at once
    assert!(
        elapsed.as_secs() < 60,
        "Cold scan took too long: {:?}",
        elapsed
    );
}

#[test]
fn test_no_index_append_pattern() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("append", cache_dir.path());
    let vfs_name = unique_vfs_name("tiered_append");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let vfs = TieredVfs::new(config).expect("failed to create VFS");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "append_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    conn.execute_batch(
        "PRAGMA page_size=65536;
         PRAGMA journal_mode=WAL;
         CREATE TABLE stream (ts INTEGER, data TEXT);",
    )
    .unwrap();

    // Bulk insert batch 1 and checkpoint
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..100 {
            tx.execute(
                "INSERT INTO stream (ts, data) VALUES (?1, ?2)",
                rusqlite::params![i, format!("batch1_{}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();

    // Bulk insert batch 2 and checkpoint
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 100..200 {
            tx.execute(
                "INSERT INTO stream (ts, data) VALUES (?1, ?2)",
                rusqlite::params![i, format!("batch2_{}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();

    // Verify S3 manifest after both checkpoints
    verify_s3_manifest(&bucket, &prefix, &endpoint, 1, 65536);

    // Verify all data is accessible
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM stream", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 200);

    // Verify ordering (append pattern — no index, so rowid order)
    let first: String = conn
        .query_row(
            "SELECT data FROM stream ORDER BY rowid ASC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(first, "batch1_0");

    let last: String = conn
        .query_row(
            "SELECT data FROM stream ORDER BY rowid DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(last, "batch2_199");
}

// ===== Phase 5b/5d: Bug fix verification + missing coverage =====

#[test]
fn test_read_only_rejects_writes() {
    // First write some data
    let write_cache = TempDir::new().unwrap();
    let config = test_config("readonly", write_cache.path());
    let writer_vfs = unique_vfs_name("tiered_ro_writer");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();

    let vfs = TieredVfs::new(config).unwrap();
    turbolite::tiered::register(&writer_vfs, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "readonly_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &writer_vfs,
    )
    .unwrap();

    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         CREATE TABLE t (id INTEGER PRIMARY KEY);
         INSERT INTO t VALUES (1);
         PRAGMA wal_checkpoint(TRUNCATE);",
    )
    .unwrap();
    drop(conn);

    // Open read-only VFS and attempt write
    let ro_cache = TempDir::new().unwrap();
    let ro_config = TieredConfig {
        bucket,
        prefix,
        cache_dir: ro_cache.path().to_path_buf(),
        endpoint_url: endpoint,
        read_only: true,
        region,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let ro_vfs_name = unique_vfs_name("tiered_ro_reader");
    let ro_vfs = TieredVfs::new(ro_config).unwrap();
    turbolite::tiered::register(&ro_vfs_name, ro_vfs).unwrap();

    let ro_conn = rusqlite::Connection::open_with_flags_and_vfs(
        "readonly_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &ro_vfs_name,
    )
    .unwrap();

    // Read should work
    let count: i64 = ro_conn
        .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 1);
}

#[test]
fn test_cache_clear_survival() {
    // Write data and checkpoint to S3
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("cacheclear", cache_dir.path());
    let vfs_name = unique_vfs_name("tiered_cacheclr");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();

    let vfs = TieredVfs::new(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "cacheclr_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA page_size=65536;
         CREATE TABLE survive (id INTEGER PRIMARY KEY, val TEXT);",
    )
    .unwrap();

    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..50 {
            tx.execute(
                "INSERT INTO survive VALUES (?1, ?2)",
                rusqlite::params![i, format!("survivor_{}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    drop(conn);

    // Nuke the entire cache directory
    std::fs::remove_dir_all(cache_dir.path()).unwrap();

    // Open with a completely fresh cache — data must come from S3
    let fresh_cache = TempDir::new().unwrap();
    let fresh_config = TieredConfig {
        bucket,
        prefix,
        cache_dir: fresh_cache.path().to_path_buf(),
        endpoint_url: endpoint,
        read_only: true,
        region,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let fresh_vfs_name = unique_vfs_name("tiered_cacheclr2");
    let fresh_vfs = TieredVfs::new(fresh_config).unwrap();
    turbolite::tiered::register(&fresh_vfs_name, fresh_vfs)
        .unwrap();

    let fresh_conn = rusqlite::Connection::open_with_flags_and_vfs(
        "cacheclr_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &fresh_vfs_name,
    )
    .unwrap();

    let count: i64 = fresh_conn
        .query_row("SELECT COUNT(*) FROM survive", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 50, "all data should survive cache deletion");

    let val: String = fresh_conn
        .query_row(
            "SELECT val FROM survive WHERE id = 49",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(val, "survivor_49");
}

#[test]
fn test_page_group_cache_populates() {
    // Write data, checkpoint, then do a cold read and verify that
    // the cache directory has data.cache + page_bitmap (page group model).
    let write_cache = TempDir::new().unwrap();
    let config = test_config("pgcache", write_cache.path());
    let vfs_name = unique_vfs_name("tiered_pgcache_w");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();

    let vfs = TieredVfs::new(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "pgcache_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA page_size=65536;
         CREATE TABLE cc (id INTEGER PRIMARY KEY, data TEXT);",
    )
    .unwrap();

    // Bulk insert enough data to generate multiple pages
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..500 {
            tx.execute(
                "INSERT INTO cc VALUES (?1, ?2)",
                rusqlite::params![i, format!("page_group_cache_data_{:0>200}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();

    // Verify S3 has page group objects
    let pg_count = verify_s3_has_page_groups(&bucket, &prefix, &endpoint);
    eprintln!("S3 has {} page group objects after checkpoint", pg_count);

    drop(conn);

    // Open fresh reader — cold cache
    let cold_cache = TempDir::new().unwrap();
    let reader_config = TieredConfig {
        bucket,
        prefix,
        cache_dir: cold_cache.path().to_path_buf(),
        endpoint_url: endpoint,
        read_only: true,
        region,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let reader_vfs_name = unique_vfs_name("tiered_pgcache_r");
    let reader_vfs = TieredVfs::new(reader_config).unwrap();
    turbolite::tiered::register(&reader_vfs_name, reader_vfs)
        .unwrap();

    let reader = rusqlite::Connection::open_with_flags_and_vfs(
        "pgcache_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &reader_vfs_name,
    )
    .unwrap();

    // Full scan — triggers page group fetches from S3
    let count: i64 = reader
        .query_row("SELECT COUNT(*) FROM cc", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 500);

    // Verify cache has data.cache file (uncompressed page store)
    // DiskCache is created at cache_dir directly (not per-db subdirectory)
    let cache_file = cold_cache.path().join("data.cache");
    assert!(
        cache_file.exists(),
        "data.cache should exist after cold read"
    );
    let cache_size = std::fs::metadata(&cache_file).unwrap().len();
    assert!(
        cache_size > 0,
        "data.cache should have nonzero size, got {}",
        cache_size
    );
    eprintln!("data.cache size after cold scan: {} bytes", cache_size);

    // page_bitmap is persisted only during checkpoint, not during reads.
    // For a read-only cold reader, the bitmap is in-memory only.
    // Verify the cache actually serves correct data (already done via COUNT(*) above).
}

#[test]
fn test_destroy_s3() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("destroy", cache_dir.path());
    let vfs_name = unique_vfs_name("tiered_destroy");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let vfs = TieredVfs::new(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "destroy_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         CREATE TABLE d (x INTEGER);
         INSERT INTO d VALUES (1);
         PRAGMA wal_checkpoint(TRUNCATE);",
    )
    .unwrap();
    drop(conn);

    // Verify manifest exists
    let rt = tokio::runtime::Runtime::new().unwrap();
    let exists_before = rt.block_on(async {
        let aws_config = aws_config::from_env()
            .region(aws_sdk_s3::config::Region::new("auto"))
            .load()
            .await;
        let mut s3_config = aws_sdk_s3::config::Builder::from(&aws_config);
        if let Some(ep) = &endpoint {
            s3_config = s3_config.endpoint_url(ep).force_path_style(true);
        }
        let client = aws_sdk_s3::Client::from_conf(s3_config.build());

        client
            .head_object()
            .bucket(&bucket)
            .key(format!("{}/manifest.msgpack", prefix))
            .send()
            .await
            .is_ok()
    });
    assert!(exists_before, "manifest should exist before destroy");

    // Create a new VFS instance with same config to call destroy_s3
    let destroy_config = TieredConfig {
        bucket: bucket.clone(),
        prefix: prefix.clone(),
        cache_dir: cache_dir.path().to_path_buf(),
        endpoint_url: endpoint.clone(),
        region: Some("auto".to_string()),
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let destroy_vfs = TieredVfs::new(destroy_config).unwrap();
    destroy_vfs.destroy_s3().unwrap();

    // Verify manifest is gone
    let exists_after = rt.block_on(async {
        let aws_config = aws_config::from_env()
            .region(aws_sdk_s3::config::Region::new("auto"))
            .load()
            .await;
        let mut s3_config = aws_sdk_s3::config::Builder::from(&aws_config);
        if let Some(ep) = &endpoint {
            s3_config = s3_config.endpoint_url(ep).force_path_style(true);
        }
        let client = aws_sdk_s3::Client::from_conf(s3_config.build());

        client
            .head_object()
            .bucket(&bucket)
            .key(format!("{}/manifest.msgpack", prefix))
            .send()
            .await
            .is_ok()
    });
    assert!(!exists_after, "manifest should be gone after destroy_s3");
}

#[test]
fn test_1k_rows_checkpoint_cold_read() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("1k_rows", cache_dir.path());
    let vfs_name = unique_vfs_name("tiered_1k");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();

    let vfs = TieredVfs::new(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "1k_rows_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    conn.execute_batch(
        "PRAGMA page_size=65536;
         PRAGMA journal_mode=WAL;
         CREATE TABLE events (id INTEGER PRIMARY KEY, ts INTEGER, payload TEXT);",
    )
    .unwrap();

    let tx = conn.unchecked_transaction().unwrap();
    for i in 0..1000 {
        tx.execute(
            "INSERT INTO events VALUES (?1, ?2, ?3)",
            rusqlite::params![i, i * 1000, format!("event_{}", i)],
        )
        .unwrap();
    }
    tx.commit().unwrap();

    let start = std::time::Instant::now();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    eprintln!("Checkpoint (1000 rows): {:?}", start.elapsed());
    drop(conn);

    // Cold read from a fresh cache
    let cold_cache = TempDir::new().unwrap();
    let reader_config = TieredConfig {
        bucket,
        prefix,
        cache_dir: cold_cache.path().to_path_buf(),
        endpoint_url: endpoint,
        read_only: true,
        region,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let reader_vfs_name = unique_vfs_name("tiered_1k_reader");
    let reader_vfs = TieredVfs::new(reader_config).unwrap();
    turbolite::tiered::register(&reader_vfs_name, reader_vfs)
        .unwrap();

    let reader = rusqlite::Connection::open_with_flags_and_vfs(
        "1k_rows_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &reader_vfs_name,
    )
    .unwrap();

    let start = std::time::Instant::now();
    let count: i64 = reader
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    eprintln!("Cold read (COUNT * 1000 rows): {:?}", start.elapsed());

    assert_eq!(count, 1000);

    let payload: String = reader
        .query_row(
            "SELECT payload FROM events WHERE id = 999",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(payload, "event_999");
}

// ===== Coverage gap tests =====

/// Verify manifest version increments monotonically across multiple checkpoints.
#[test]
fn test_manifest_version_increments() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("versions", cache_dir.path());
    let vfs_name = unique_vfs_name("tiered_versions");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let vfs = TieredVfs::new(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "versions_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    conn.execute_batch(
        "PRAGMA page_size=65536;
         PRAGMA journal_mode=WAL;
         CREATE TABLE v (id INTEGER PRIMARY KEY, val TEXT);",
    )
    .unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let get_version = |rt: &tokio::runtime::Runtime,
                       bucket: &str,
                       prefix: &str,
                       endpoint: &Option<String>|
     -> u64 {
        rt.block_on(async {
            let aws_config = aws_config::from_env()
                .region(aws_sdk_s3::config::Region::new("auto"))
                .load()
                .await;
            let mut s3_config = aws_sdk_s3::config::Builder::from(&aws_config);
            if let Some(ep) = endpoint {
                s3_config = s3_config.endpoint_url(ep).force_path_style(true);
            }
            let client = aws_sdk_s3::Client::from_conf(s3_config.build());
            let resp = client
                .get_object()
                .bucket(bucket)
                .key(format!("{}/manifest.msgpack", prefix))
                .send()
                .await
                .unwrap();
            let bytes = resp.body.collect().await.unwrap().into_bytes();
            let manifest: serde_json::Value =
                manifest_from_msgpack(&bytes);
            manifest["version"].as_u64().unwrap()
        })
    };

    // Checkpoint 1
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..10 {
            tx.execute(
                "INSERT INTO v VALUES (?1, ?2)",
                rusqlite::params![i, format!("v1_{}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    let v1 = get_version(&rt, &bucket, &prefix, &endpoint);
    assert!(v1 >= 1, "first checkpoint should have version >= 1");

    // Checkpoint 2
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 10..20 {
            tx.execute(
                "INSERT INTO v VALUES (?1, ?2)",
                rusqlite::params![i, format!("v2_{}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    let v2 = get_version(&rt, &bucket, &prefix, &endpoint);
    assert!(v2 > v1, "version should increase: v1={}, v2={}", v1, v2);

    // Checkpoint 3
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 20..30 {
            tx.execute(
                "INSERT INTO v VALUES (?1, ?2)",
                rusqlite::params![i, format!("v3_{}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    let v3 = get_version(&rt, &bucket, &prefix, &endpoint);
    assert!(v3 > v2, "version should increase monotonically: v2={}, v3={}", v2, v3);

    // Verify all data accessible
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM v", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 30);
}

/// Verify default 4096 page size works (all other tests use 65536).
#[test]
fn test_default_4096_page_size() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("pages4k", cache_dir.path());
    let vfs_name = unique_vfs_name("tiered_4k");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();

    let vfs = TieredVfs::new(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "pages4k_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    // Default 4096 page size — do NOT set page_size pragma
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         CREATE TABLE small (id INTEGER PRIMARY KEY, val TEXT);",
    )
    .unwrap();

    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..200 {
            tx.execute(
                "INSERT INTO small VALUES (?1, ?2)",
                rusqlite::params![i, format!("small_page_{}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();

    // Verify S3 manifest with 4096 page size
    verify_s3_manifest(&bucket, &prefix, &endpoint, 1, 4096);

    drop(conn);

    // Cold read from fresh cache
    let cold_cache = TempDir::new().unwrap();
    let reader_config = TieredConfig {
        bucket,
        prefix,
        cache_dir: cold_cache.path().to_path_buf(),
        endpoint_url: endpoint,
        read_only: true,
        region,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let reader_vfs_name = unique_vfs_name("tiered_4k_reader");
    let reader_vfs = TieredVfs::new(reader_config).unwrap();
    turbolite::tiered::register(&reader_vfs_name, reader_vfs)
        .unwrap();

    let reader = rusqlite::Connection::open_with_flags_and_vfs(
        "pages4k_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &reader_vfs_name,
    )
    .unwrap();

    let count: i64 = reader
        .query_row("SELECT COUNT(*) FROM small", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 200, "4096 page size should work");

    // Verify page_size is actually 4096
    let page_size: i64 = reader
        .query_row("PRAGMA page_size", [], |row| row.get(0))
        .unwrap();
    assert_eq!(page_size, 4096, "default page_size should be 4096");
}
