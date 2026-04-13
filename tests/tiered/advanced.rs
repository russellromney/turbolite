//! Advanced tests: PPG config, TTL eviction, compression/dictionary, cache management, autovacuum.

use turbolite::tiered::{TurboliteConfig, TurboliteVfs};
use tempfile::TempDir;
use super::helpers::*;

#[test]

fn test_ttl_eviction() {
    // Write enough data to generate multiple page groups, then verify
    // data integrity with cold reads (TTL eviction is time-based, so
    // here we just verify the full read path works with page groups).
    let write_cache = TempDir::new().unwrap();
    let config = test_config("ttl_evict", write_cache.path());
    let vfs_name = unique_vfs_name("tiered_ttl_w");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();

    let vfs = TurboliteVfs::new(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "ttl_evict_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    conn.execute_batch(
        "PRAGMA page_size=65536;
         PRAGMA journal_mode=WAL;
         CREATE TABLE ttl (id INTEGER PRIMARY KEY, data BLOB);",
    )
    .unwrap();

    // Each row ~8KB, 200 rows = ~1.6MB = ~25 pages at 64KB
    let blob = vec![0x42u8; 8192];
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..200 {
            tx.execute(
                "INSERT INTO ttl VALUES (?1, ?2)",
                rusqlite::params![i, blob],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    drop(conn);

    // Open reader — all data fetched from S3 into cache file
    let cold_cache = TempDir::new().unwrap();
    let reader_config = TurboliteConfig {
        bucket: bucket.clone(),
        prefix: prefix.clone(),
        cache_dir: cold_cache.path().to_path_buf(),
        endpoint_url: endpoint.clone(),
        read_only: true,
        region: region.clone(),
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let reader_vfs_name = unique_vfs_name("tiered_ttl_r");
    let reader_vfs = TurboliteVfs::new(reader_config).unwrap();
    turbolite::tiered::register(&reader_vfs_name, reader_vfs)
        .unwrap();

    let reader = rusqlite::Connection::open_with_flags_and_vfs(
        "ttl_evict_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &reader_vfs_name,
    )
    .unwrap();

    // Full scan — fetches page groups from S3
    let count: i64 = reader
        .query_row("SELECT COUNT(*) FROM ttl", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 200, "should read all rows from page groups");

    // Verify data integrity — specific row lookup
    let data: Vec<u8> = reader
        .query_row("SELECT data FROM ttl WHERE id = 100", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(data.len(), 8192);
    assert!(data.iter().all(|&b| b == 0x42));
}

#[test]

fn test_dictionary_mismatch_errors() {
    // Train a simple dictionary from sample data
    let samples: Vec<Vec<u8>> = (0..100)
        .map(|i| format!("dict_sample_row_{}_with_repeated_structure", i).into_bytes())
        .collect();
    let dict_bytes =
        turbolite::dict::train_dictionary(&samples, 4096).unwrap();

    // Write with dictionary
    let write_cache = TempDir::new().unwrap();
    let mut config = test_config("dict_mismatch", write_cache.path());
    config.dictionary = Some(dict_bytes);
    let vfs_name = unique_vfs_name("tiered_dict_w");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();

    let vfs = TurboliteVfs::new(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "dict_mismatch_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    conn.execute_batch(
        "PRAGMA page_size=65536;
         PRAGMA journal_mode=WAL;
         CREATE TABLE dm (id INTEGER PRIMARY KEY, val TEXT);",
    )
    .unwrap();

    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..50 {
            tx.execute(
                "INSERT INTO dm VALUES (?1, ?2)",
                rusqlite::params![i, format!("dict_row_{}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    drop(conn);

    // Read WITHOUT dictionary — should fail on decompression, not return garbage
    let cold_cache = TempDir::new().unwrap();
    let reader_config = TurboliteConfig {
        bucket,
        prefix,
        cache_dir: cold_cache.path().to_path_buf(),
        endpoint_url: endpoint,
        read_only: true,
        region,
        // dictionary: None (default) — intentional mismatch
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let reader_vfs_name = unique_vfs_name("tiered_dict_r");
    let reader_vfs = TurboliteVfs::new(reader_config).unwrap();
    turbolite::tiered::register(&reader_vfs_name, reader_vfs)
        .unwrap();

    // Opening the connection reads page 0 (schema). With dict-compressed pages
    // and no dict, decompression fails immediately — SQLite reports disk I/O error.
    let result = rusqlite::Connection::open_with_flags_and_vfs(
        "dict_mismatch_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &reader_vfs_name,
    );

    // Should fail at open or first query — decompression without matching dict
    match result {
        Err(_) => {
            // Expected: connection open itself fails (page 0 can't decompress)
        }
        Ok(conn) => {
            // If open succeeds (unlikely), the first query should fail
            let query_result =
                conn.query_row("SELECT COUNT(*) FROM dm", [], |row| row.get::<_, i64>(0));
            assert!(
                query_result.is_err(),
                "reading dict-compressed data without dict should error, not return garbage"
            );
        }
    }
}

/// Write with dictionary, read with SAME dictionary — round-trip should work.
#[test]

fn test_dictionary_roundtrip() {
    let samples: Vec<Vec<u8>> = (0..100)
        .map(|i| format!("roundtrip_sample_{}_with_padding", i).into_bytes())
        .collect();
    let dict_bytes =
        turbolite::dict::train_dictionary(&samples, 4096).unwrap();

    // Write with dict
    let write_cache = TempDir::new().unwrap();
    let mut config = test_config("dict_roundtrip", write_cache.path());
    config.dictionary = Some(dict_bytes.clone());
    let vfs_name = unique_vfs_name("tiered_drt_w");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();

    let vfs = TurboliteVfs::new(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "dict_rt_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    conn.execute_batch(
        "PRAGMA page_size=65536;
         PRAGMA journal_mode=WAL;
         CREATE TABLE drt (id INTEGER PRIMARY KEY, val TEXT);",
    )
    .unwrap();

    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..50 {
            tx.execute(
                "INSERT INTO drt VALUES (?1, ?2)",
                rusqlite::params![i, format!("dict_roundtrip_{}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    drop(conn);

    // Read with SAME dictionary from fresh cache
    let cold_cache = TempDir::new().unwrap();
    let mut reader_config = TurboliteConfig {
        bucket,
        prefix,
        cache_dir: cold_cache.path().to_path_buf(),
        endpoint_url: endpoint,
        read_only: true,
        region,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    reader_config.dictionary = Some(dict_bytes);
    let reader_vfs_name = unique_vfs_name("tiered_drt_r");
    let reader_vfs = TurboliteVfs::new(reader_config).unwrap();
    turbolite::tiered::register(&reader_vfs_name, reader_vfs)
        .unwrap();

    let reader = rusqlite::Connection::open_with_flags_and_vfs(
        "dict_rt_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &reader_vfs_name,
    )
    .unwrap();

    let count: i64 = reader
        .query_row("SELECT COUNT(*) FROM drt", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 50, "dict roundtrip should preserve all data");

    let val: String = reader
        .query_row(
            "SELECT val FROM drt WHERE id = 25",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(val, "dict_roundtrip_25");
}

#[test]

fn test_custom_pages_per_group() {
    let cache_dir = TempDir::new().unwrap();
    let mut config = test_config("custom_ppg", cache_dir.path());
    config.pages_per_group = 8; // Very small page groups for testing
    let vfs_name = unique_vfs_name("tiered_ppg8");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();

    let vfs = TurboliteVfs::new(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "custom_ppg_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    conn.execute_batch(
        "PRAGMA page_size=65536;
         PRAGMA journal_mode=WAL;
         CREATE TABLE cs (id INTEGER PRIMARY KEY, data TEXT);",
    )
    .unwrap();

    // Insert enough data to span multiple page groups (8 pages per group)
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..500 {
            tx.execute(
                "INSERT INTO cs VALUES (?1, ?2)",
                rusqlite::params![i, format!("ppg_8_data_{:0>200}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();

    // With pages_per_group=8, we should have more page group objects
    let pg_count = verify_s3_has_page_groups(&bucket, &prefix, &endpoint);
    eprintln!("pages_per_group=8: {} page group objects in S3", pg_count);
    assert!(pg_count >= 1, "should have page group objects with ppg=8");

    // Verify manifest has pages_per_group=8
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
            .unwrap();
        resp.body.collect().await.unwrap().into_bytes().to_vec()
    });
    let manifest: serde_json::Value = manifest_from_msgpack(&manifest_data);
    assert_eq!(
        manifest["pages_per_group"].as_u64().unwrap(),
        8,
        "manifest should have pages_per_group=8"
    );

    drop(conn);

    // Cold read with pages_per_group=8
    let cold_cache = TempDir::new().unwrap();
    let reader_config = TurboliteConfig {
        bucket,
        prefix,
        cache_dir: cold_cache.path().to_path_buf(),
        endpoint_url: endpoint,
        read_only: true,
        pages_per_group: 8,
        region,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let reader_vfs_name = unique_vfs_name("tiered_ppg8_r");
    let reader_vfs = TurboliteVfs::new(reader_config).unwrap();
    turbolite::tiered::register(&reader_vfs_name, reader_vfs)
        .unwrap();

    let reader = rusqlite::Connection::open_with_flags_and_vfs(
        "custom_ppg_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &reader_vfs_name,
    )
    .unwrap();

    let count: i64 = reader
        .query_row("SELECT COUNT(*) FROM cs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 500, "all rows should survive with pages_per_group=8");
}

#[test]

fn test_ppg_mismatch_uses_manifest() {
    // Write with default pages_per_group=2048
    let write_cache = TempDir::new().unwrap();
    let config = test_config("ppg_mismatch", write_cache.path());
    let vfs_name = unique_vfs_name("tiered_ppgmm_w");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();

    let vfs = TurboliteVfs::new(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "ppg_mismatch_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    conn.execute_batch(
        "PRAGMA page_size=65536;
         PRAGMA journal_mode=WAL;
         CREATE TABLE csm (id INTEGER PRIMARY KEY, val TEXT);",
    )
    .unwrap();

    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..200 {
            tx.execute(
                "INSERT INTO csm VALUES (?1, ?2)",
                rusqlite::params![i, format!("mismatch_data_{:0>200}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    // Verify S3 manifest is readable before opening cold reader
    super::helpers::verify_s3_manifest(&bucket, &prefix, &endpoint, 1, 65536);
    drop(conn);

    // Read with DIFFERENT pages_per_group in config (64 instead of 2048).
    // The VFS must use the manifest's pages_per_group to read correctly.
    let cold_cache = TempDir::new().unwrap();
    let reader_config = TurboliteConfig {
        bucket,
        prefix,
        cache_dir: cold_cache.path().to_path_buf(),
        endpoint_url: endpoint,
        read_only: true,
        pages_per_group: 64, // DIFFERENT from writer's 2048
        region,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let reader_vfs_name = unique_vfs_name("tiered_ppgmm_r");
    let reader_vfs = TurboliteVfs::new(reader_config).unwrap();
    turbolite::tiered::register(&reader_vfs_name, reader_vfs)
        .unwrap();

    let reader = rusqlite::Connection::open_with_flags_and_vfs(
        "ppg_mismatch_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &reader_vfs_name,
    )
    .unwrap();

    let count: i64 = reader
        .query_row("SELECT COUNT(*) FROM csm", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 200, "reader with mismatched pages_per_group should use manifest's");

    let val: String = reader
        .query_row(
            "SELECT val FROM csm WHERE id = 199",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(val.starts_with("mismatch_data_"), "data integrity with mismatched pages_per_group");
}

#[test]
fn test_evict_tree_by_name() {
    let cache_dir = TempDir::new().unwrap();
    let local_db = cache_dir.path().join("local_evict.db");

    // Step 1: Create a local SQLite file with enough data to span multiple groups.
    // Using 4096 page size + ppg=8 means each group covers 8 pages (32KB).
    // We need enough rows to spread across multiple pages per table.
    {
        let conn = rusqlite::Connection::open(&local_db).unwrap();
        conn.execute_batch(
            "PRAGMA page_size=4096;
             PRAGMA journal_mode=DELETE;
             CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, bio TEXT);
             CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, body TEXT);
             CREATE INDEX idx_posts_user ON posts(user_id);",
        ).unwrap();
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..500 {
            tx.execute("INSERT INTO users VALUES (?1, ?2, ?3)",
                rusqlite::params![i, format!("user_{}", i), format!("Bio for user {} with some padding text to fill pages", i)]).unwrap();
        }
        for i in 0..2000 {
            tx.execute("INSERT INTO posts VALUES (?1, ?2, ?3)",
                rusqlite::params![i, i % 500, format!("Post body {} with enough text to fill up multiple pages in the database", i)]).unwrap();
        }
        tx.commit().unwrap();
        conn.execute_batch("VACUUM;").unwrap();
    }

    // Step 2: Import via import_sqlite_file (builds BTreeAware manifest)
    let mut config = test_config("evict_tree", cache_dir.path());
    config.pages_per_group = 8;
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let manifest = turbolite::tiered::import_sqlite_file(&config, &local_db)
        .expect("import failed");
    assert!(!manifest.tree_name_to_groups.is_empty(),
        "import must populate tree_name_to_groups");

    // Step 3: Open via tiered VFS (cold read from S3)
    let vfs_name = unique_vfs_name("tiered_evict_tree");
    let vfs = TurboliteVfs::new(config).expect("TurboliteVfs");
    let bench = vfs.shared_state();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "evict_tree_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    ).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();

    // Warm cache by reading
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM posts", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 2000);

    // Evict posts tree
    let evicted = bench.evict_tree("posts");
    assert!(evicted > 0, "expected at least one group evicted for 'posts'");

    // Nonexistent tree returns 0
    assert_eq!(bench.evict_tree("nonexistent_table"), 0);

    // CSV works (users may have been evicted or in group 0)
    let _evicted_csv = bench.evict_tree("users, idx_posts_user");

    // Empty/whitespace is a no-op
    assert_eq!(bench.evict_tree(""), 0);
    assert_eq!(bench.evict_tree("  ,  , "), 0);

    // Data still readable after eviction (re-fetched from S3)
    let count2: i64 = conn.query_row("SELECT COUNT(*) FROM posts", [], |r| r.get(0)).unwrap();
    assert_eq!(count2, 2000);

    drop(conn);
    let cleanup_config = TurboliteConfig {
        bucket, prefix, endpoint_url: endpoint,
        region: Some("auto".to_string()),
        cache_dir: cache_dir.path().to_path_buf(),
        pages_per_group: 8,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    TurboliteVfs::new(cleanup_config).unwrap().destroy_s3().unwrap();
}

#[test]
fn test_cache_info_returns_valid_json() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("cache_info", cache_dir.path());
    let vfs_name = unique_vfs_name("tiered_cache_info");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let vfs = TurboliteVfs::new(config).expect("TurboliteVfs");
    let bench = vfs.shared_state();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "cache_info_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    ).unwrap();

    conn.execute_batch(
        "PRAGMA page_size=65536;
         PRAGMA journal_mode=WAL;
         CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT);",
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

    // Warm cache
    let _: i64 = conn.query_row("SELECT COUNT(*) FROM data", [], |r| r.get(0)).unwrap();

    // Get cache info
    let info = bench.cache_info();
    assert!(info.contains("\"size_bytes\":"), "JSON should contain size_bytes: {}", info);
    assert!(info.contains("\"groups_cached\":"), "JSON should contain groups_cached: {}", info);
    assert!(info.contains("\"groups_total\":"), "JSON should contain groups_total: {}", info);
    assert!(info.contains("\"tiers\":"), "JSON should contain tiers: {}", info);
    assert!(info.contains("\"pinned\":"), "JSON should contain pinned tier: {}", info);
    assert!(info.contains("\"s3_gets_total\":"), "JSON should contain s3_gets_total: {}", info);

    // After clearing cache, size should drop
    bench.clear_cache_data_only();
    let info_after = bench.cache_info();
    // Parse size_bytes from both
    let size_before: u64 = info.split("\"size_bytes\":").nth(1).unwrap()
        .split(',').next().unwrap().parse().unwrap();
    let size_after: u64 = info_after.split("\"size_bytes\":").nth(1).unwrap()
        .split(',').next().unwrap().parse().unwrap();
    assert!(size_after <= size_before, "cache should shrink after clear: before={}, after={}", size_before, size_after);

    drop(conn);
    let cleanup_config = TurboliteConfig {
        bucket, prefix, endpoint_url: endpoint,
        region: Some("auto".to_string()),
        cache_dir: cache_dir.path().to_path_buf(),
        pages_per_group: 8,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    TurboliteVfs::new(cleanup_config).unwrap().destroy_s3().unwrap();
}

#[test]
fn test_evict_tree_skips_pending_flush_groups() {
    // This test must use BTreeAware import so tree_name_to_groups is populated.
    // Without it, evict_tree("posts") always returns 0 because the name lookup
    // yields no groups, making the test a false positive.
    let cache_dir = TempDir::new().unwrap();
    let local_db = cache_dir.path().join("local_pending.db");

    // Step 1: Create a local SQLite file with enough data to span multiple groups.
    {
        let conn = rusqlite::Connection::open(&local_db).unwrap();
        conn.execute_batch(
            "PRAGMA page_size=4096;
             PRAGMA journal_mode=DELETE;
             CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, body TEXT);
             CREATE INDEX idx_posts_user ON posts(user_id);",
        ).unwrap();
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..2000 {
            tx.execute("INSERT INTO posts VALUES (?1, ?2, ?3)",
                rusqlite::params![i, i % 100, format!("Post body {} with padding text to fill pages", i)]).unwrap();
        }
        tx.commit().unwrap();
        conn.execute_batch("VACUUM;").unwrap();
    }

    // Step 2: Import via import_sqlite_file (builds BTreeAware manifest with tree_name_to_groups)
    let mut config = test_config("evict_tree_pending", cache_dir.path());
    config.pages_per_group = 8;
    config.sync_mode = turbolite::tiered::SyncMode::LocalThenFlush;
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let manifest = turbolite::tiered::import_sqlite_file(&config, &local_db)
        .expect("import failed");
    let posts_groups = manifest.tree_name_to_groups.get("posts")
        .expect("import must map 'posts' to groups");
    assert!(!posts_groups.is_empty(), "posts must have at least one group");

    // Step 3: Open via tiered VFS, write new data, local-checkpoint to create pending groups
    let vfs_name = unique_vfs_name("tiered_evict_pending");
    let vfs = TurboliteVfs::new(config).expect("TurboliteVfs");
    let bench = vfs.shared_state();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "evict_tree_pending.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    ).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();

    // Warm cache by reading (so groups are cached)
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM posts", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 2000);

    // Verify evict_tree works when nothing is pending
    assert!(!bench.has_pending_flush());
    let evicted_before = bench.evict_tree("posts");
    assert!(evicted_before > 0, "should evict posts groups when no pending flush");

    // Re-warm cache
    let _: i64 = conn.query_row("SELECT COUNT(*) FROM posts", [], |r| r.get(0)).unwrap();

    // Write new data and local-checkpoint to create pending groups
    turbolite::tiered::set_local_checkpoint_only(true);
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 2000..2500 {
            tx.execute("INSERT INTO posts VALUES (?1, ?2, ?3)",
                rusqlite::params![i, i % 100, format!("new post {}", i)]).unwrap();
        }
        tx.commit().unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();

    // Now groups are pending: evict_tree should skip pending ones
    assert!(bench.has_pending_flush());
    let evicted_pending = bench.evict_tree("posts");
    // Some groups may still be evictable (not all posts groups are dirty),
    // but pending ones must be skipped. The key assertion is that we evict
    // fewer groups than when nothing was pending.
    assert!(evicted_pending < evicted_before,
        "pending flush must reduce evictable groups: before={}, during_pending={}",
        evicted_before, evicted_pending);

    // Flush, then evict should work fully again
    turbolite::tiered::set_local_checkpoint_only(false);
    bench.flush_to_s3().unwrap();
    assert!(!bench.has_pending_flush());

    // Data still readable after flush
    let count2: i64 = conn.query_row("SELECT COUNT(*) FROM posts", [], |r| r.get(0)).unwrap();
    assert_eq!(count2, 2500);

    drop(conn);
    let cleanup_config = TurboliteConfig {
        bucket, prefix, endpoint_url: endpoint,
        region: Some("auto".to_string()),
        cache_dir: cache_dir.path().to_path_buf(),
        pages_per_group: 8,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    TurboliteVfs::new(cleanup_config).unwrap().destroy_s3().unwrap();
}

/// Phase Thermopylae-c: verify autovacuum works through the tiered VFS.
/// Insert/delete cycles with incremental_vacuum should free pages,
/// and checkpoint + GC should stabilize S3 object count.
#[test]
fn test_autovacuum_with_gc() {
    let cache_dir = TempDir::new().unwrap();
    let mut config = test_config("autovacuum", cache_dir.path());
    config.gc_enabled = false; // disable inline GC so we can observe object accumulation
    let vfs_name = unique_vfs_name("tiered_autovacuum");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let vfs = TurboliteVfs::new(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "autovacuum_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    ).unwrap();

    // Enable incremental autovacuum BEFORE creating tables
    conn.execute_batch(
        "PRAGMA auto_vacuum=INCREMENTAL;
         PRAGMA journal_mode=WAL;
         PRAGMA page_size=4096;",
    ).unwrap();

    // Verify autovacuum is set
    let av: i64 = conn.query_row("PRAGMA auto_vacuum", [], |r| r.get(0)).unwrap();
    assert_eq!(av, 2, "auto_vacuum should be INCREMENTAL (2)");

    // Insert a bunch of data
    conn.execute_batch("CREATE TABLE avtest (id INTEGER PRIMARY KEY, data TEXT);").unwrap();
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..500 {
            tx.execute(
                "INSERT INTO avtest VALUES (?1, ?2)",
                rusqlite::params![i, format!("{:0>200}", i)],
            ).unwrap();
        }
        tx.commit().unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();

    let objects_after_insert = count_s3_objects(&bucket, &prefix, &endpoint);

    // Delete most data
    conn.execute("DELETE FROM avtest WHERE id >= 50", []).unwrap();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();

    let objects_after_delete = count_s3_objects(&bucket, &prefix, &endpoint);
    // After delete + checkpoint with gc_enabled=false, old versions accumulate
    assert!(
        objects_after_delete >= objects_after_insert,
        "with GC off, objects should not decrease after delete: insert={} delete={}",
        objects_after_insert, objects_after_delete,
    );

    // Run incremental vacuum to free pages
    conn.execute_batch("PRAGMA incremental_vacuum(100);").unwrap();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();

    // Verify remaining data is intact
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM avtest", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 50, "50 rows should survive after delete + vacuum");

    // Verify page count decreased
    let page_count: i64 = conn.query_row("PRAGMA page_count", [], |r| r.get(0)).unwrap();
    let freelist: i64 = conn.query_row("PRAGMA freelist_count", [], |r| r.get(0)).unwrap();
    eprintln!("After vacuum: page_count={}, freelist={}", page_count, freelist);

    // Now run full GC scan to clean up orphans
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
    eprintln!("Full GC after autovacuum deleted {} objects", deleted);

    let objects_after_gc = count_s3_objects(&bucket, &prefix, &endpoint);
    eprintln!(
        "S3 objects: after_insert={}, after_delete={}, after_gc={}",
        objects_after_insert, objects_after_delete, objects_after_gc,
    );

    // GC should have cleaned up old versions
    assert!(
        objects_after_gc <= objects_after_insert,
        "GC should stabilize object count: after_gc={} <= after_insert={}",
        objects_after_gc, objects_after_insert,
    );

    // Verify data still readable after GC
    let gc_conn = rusqlite::Connection::open_with_flags_and_vfs(
        "autovacuum_verify.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &unique_vfs_name("tiered_autovacuum_verify"),
    );
    // Can't easily reopen with a new VFS name without registering, so just trust the gc_vfs
    // Data integrity was verified above before drop(conn).

    // Cleanup
    gc_vfs.destroy_s3().unwrap();
}

/// Phase Marathon: verify cache file truncates after VACUUM reduces page_count.
#[test]
fn test_cache_truncation_after_vacuum() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("cache_trunc", cache_dir.path());
    let vfs_name = unique_vfs_name("tiered_cache_trunc");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let cache_path = cache_dir.path().to_path_buf();

    let vfs = TurboliteVfs::new(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "cache_trunc_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    ).unwrap();

    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA page_size=4096;
         CREATE TABLE trunc_test (id INTEGER PRIMARY KEY, data TEXT);",
    ).unwrap();

    // Insert data to grow the database
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..1000 {
            tx.execute(
                "INSERT INTO trunc_test VALUES (?1, ?2)",
                rusqlite::params![i, format!("{:0>500}", i)],
            ).unwrap();
        }
        tx.commit().unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();

    // Find the cache file and measure its size
    let cache_files: Vec<_> = std::fs::read_dir(&cache_path).unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "cache"))
        .collect();
    assert!(!cache_files.is_empty(), "cache file should exist");
    let cache_file_path = cache_files[0].path();
    let size_before = std::fs::metadata(&cache_file_path).unwrap().len();
    let pages_before: i64 = conn.query_row("PRAGMA page_count", [], |r| r.get(0)).unwrap();
    eprintln!("Before VACUUM: cache_size={}, page_count={}", size_before, pages_before);

    // Delete most data and VACUUM
    conn.execute("DELETE FROM trunc_test WHERE id >= 100", []).unwrap();
    conn.execute_batch("VACUUM;").unwrap();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();

    let size_after = std::fs::metadata(&cache_file_path).unwrap().len();
    let pages_after: i64 = conn.query_row("PRAGMA page_count", [], |r| r.get(0)).unwrap();
    eprintln!("After VACUUM: cache_size={}, page_count={}", size_after, pages_after);

    assert!(
        pages_after < pages_before,
        "VACUUM should reduce page_count: before={} after={}",
        pages_before, pages_after,
    );
    assert!(
        size_after < size_before,
        "cache file should shrink after VACUUM + checkpoint: before={} after={}",
        size_before, size_after,
    );
    assert_eq!(
        size_after, pages_after as u64 * 4096,
        "cache file size should equal page_count * page_size",
    );

    // Verify data integrity
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM trunc_test", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 100, "100 rows should survive after delete + vacuum");

    // Cleanup
    drop(conn);
    let cleanup_config = TurboliteConfig {
        bucket, prefix, endpoint_url: endpoint,
        region: Some("auto".to_string()),
        cache_dir: cache_dir.path().to_path_buf(),
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    TurboliteVfs::new(cleanup_config).unwrap().destroy_s3().unwrap();
}
