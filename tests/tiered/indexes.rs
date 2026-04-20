//! Index bundle and index integrity tests.

use turbolite::tiered::{TurboliteConfig, TurboliteVfs};
use tempfile::TempDir;
use super::helpers::*;

#[test]
fn test_index_bundles_checkpoint_and_cold_read() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("index_bundles", cache_dir.path());
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();
    let vfs_name = unique_vfs_name("ixb");

    let vfs = TurboliteVfs::new_local(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "ixb_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    conn.execute_batch(
        "PRAGMA page_size=4096;
         PRAGMA journal_mode=WAL;
         CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, category TEXT);
         CREATE INDEX idx_name ON items(name);
         CREATE INDEX idx_category ON items(category);",
    )
    .unwrap();

    // Insert enough rows to create real index pages
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..500 {
            tx.execute(
                "INSERT INTO items VALUES (?1, ?2, ?3)",
                rusqlite::params![
                    i,
                    format!("item_{:04}", i),
                    format!("cat_{}", i % 20),
                ],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    drop(conn);

    // Verify manifest has index_chunk_keys
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
            .expect("manifest should exist");
        resp.body.collect().await.unwrap().into_bytes().to_vec()
    });
    let manifest: serde_json::Value = manifest_from_msgpack(&manifest_data);
    let index_keys = manifest.get("index_chunk_keys");
    // Index chunk keys should be present (may be empty if no index leaf pages created yet,
    // but with 500 rows and 2 indexes the B-tree should have leaves)
    assert!(
        index_keys.is_some(),
        "manifest should have index_chunk_keys field"
    );

    // Cold read: open a new connection on a fresh cache
    let cold_cache = TempDir::new().unwrap();
    let cold_config = TurboliteConfig {
        bucket: bucket.clone(),
        prefix: prefix.clone(),
        cache_dir: cold_cache.path().to_path_buf(),
        compression_level: 3,
        endpoint_url: endpoint.clone(),
        read_only: true,
        region: region.clone(),
        eager_index_load: true,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let cold_vfs_name = unique_vfs_name("ixb_cold");
    let cold_vfs = TurboliteVfs::new_local(cold_config).unwrap();
    let _bench = cold_vfs.shared_state();
    turbolite::tiered::register(&cold_vfs_name, cold_vfs).unwrap();

    let cold_conn = rusqlite::Connection::open_with_flags_and_vfs(
        "ixb_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &cold_vfs_name,
    )
    .unwrap();

    // Query using the index -- should work on cold cache
    let count: i64 = cold_conn
        .query_row("SELECT COUNT(*) FROM items WHERE name = 'item_0123'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1, "indexed query should find the row");

    let cat_count: i64 = cold_conn
        .query_row("SELECT COUNT(*) FROM items WHERE category = 'cat_5'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(cat_count, 25, "category index query should find 25 rows");

    // Cleanup
    drop(cold_conn);

    // Destroy S3 data
    let cleanup_config = TurboliteConfig {
        bucket,
        prefix,
        cache_dir: cold_cache.path().to_path_buf(),
        compression_level: 3,
        endpoint_url: endpoint,
        region,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let cleanup_vfs = TurboliteVfs::new_local(cleanup_config).unwrap();
    cleanup_vfs.destroy_s3().unwrap();
}

#[test]
fn test_index_bundles_eager_load_disabled() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("ixb_no_eager", cache_dir.path());
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();
    let vfs_name = unique_vfs_name("ixb_ne");

    let vfs = TurboliteVfs::new_local(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "ixb_ne.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    conn.execute_batch(
        "PRAGMA page_size=4096;
         PRAGMA journal_mode=WAL;
         CREATE TABLE products (id INTEGER PRIMARY KEY, sku TEXT);
         CREATE INDEX idx_sku ON products(sku);",
    )
    .unwrap();

    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..200 {
            tx.execute(
                "INSERT INTO products VALUES (?1, ?2)",
                rusqlite::params![i, format!("SKU-{:05}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    drop(conn);

    // Open cold reader with eager_index_load=false
    let cold_cache = TempDir::new().unwrap();
    let cold_config = TurboliteConfig {
        bucket: bucket.clone(),
        prefix: prefix.clone(),
        cache_dir: cold_cache.path().to_path_buf(),
        compression_level: 3,
        endpoint_url: endpoint.clone(),
        read_only: true,
        region: region.clone(),
        eager_index_load: false,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let cold_vfs_name = unique_vfs_name("ixb_ne_cold");
    let cold_vfs = TurboliteVfs::new_local(cold_config).unwrap();
    turbolite::tiered::register(&cold_vfs_name, cold_vfs).unwrap();

    let cold_conn = rusqlite::Connection::open_with_flags_and_vfs(
        "ixb_ne.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &cold_vfs_name,
    )
    .unwrap();

    // Index query should still work (fetched on demand from data page groups)
    let count: i64 = cold_conn
        .query_row("SELECT COUNT(*) FROM products WHERE sku = 'SKU-00050'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);

    drop(cold_conn);

    let cleanup_config = TurboliteConfig {
        bucket,
        prefix,
        cache_dir: cold_cache.path().to_path_buf(),
        compression_level: 3,
        endpoint_url: endpoint,
        region,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let cleanup_vfs = TurboliteVfs::new_local(cleanup_config).unwrap();
    cleanup_vfs.destroy_s3().unwrap();
}

#[test]
fn test_warm_profile_query_no_corruption_after_eager_load() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("warm_profile", cache_dir.path());
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();
    let vfs_name = unique_vfs_name("warm_prof_w");

    // -- Phase 1: Create database with benchmark schema --
    let vfs = TurboliteVfs::new_local(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "warm_profile.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    // Use 64KB pages (same as benchmark default)
    conn.execute_batch(
        "PRAGMA page_size=65536;
         PRAGMA journal_mode=WAL;
         CREATE TABLE users (
             id INTEGER PRIMARY KEY,
             first_name TEXT NOT NULL,
             last_name TEXT NOT NULL,
             school TEXT,
             city TEXT,
             bio TEXT
         );
         CREATE TABLE posts (
             id INTEGER PRIMARY KEY,
             user_id INTEGER NOT NULL,
             content TEXT NOT NULL,
             created_at TEXT NOT NULL,
             like_count INTEGER DEFAULT 0,
             FOREIGN KEY (user_id) REFERENCES users(id)
         );
         CREATE INDEX idx_posts_user ON posts(user_id);",
    )
    .unwrap();

    // Insert enough data to create real interior and index pages.
    // 200 users, 2000 posts = ~10 posts per user.
    // At 64KB pages this creates multiple page groups with scattered
    // interior and index pages.
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..200 {
            tx.execute(
                "INSERT INTO users VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    i,
                    format!("First_{}", i),
                    format!("Last_{}", i),
                    format!("School_{}", i % 20),
                    format!("City_{}", i % 50),
                    format!("Bio for user {} with some padding text to make pages bigger", i),
                ],
            )
            .unwrap();
        }
        for i in 0..2000 {
            tx.execute(
                "INSERT INTO posts VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    i,
                    i % 200, // user_id
                    format!("Post content {} with some text to take up space in the page", i),
                    format!("2024-01-{:02}T12:00:00Z", (i % 28) + 1),
                    i % 100,
                ],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    // Checkpoint to S3
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    drop(conn);

    // -- Phase 2: Fresh reader VFS (simulates benchmark reader) --
    let reader_cache = TempDir::new().unwrap();
    let reader_config = TurboliteConfig {
        bucket: bucket.clone(),
        prefix: prefix.clone(),
        cache_dir: reader_cache.path().to_path_buf(),
        compression_level: 3,
        endpoint_url: endpoint.clone(),
        read_only: true,
        region: region.clone(),
        eager_index_load: true,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let reader_vfs_name = unique_vfs_name("warm_prof_r");
    let reader_vfs = TurboliteVfs::new_local(reader_config).unwrap();
    turbolite::tiered::register(&reader_vfs_name, reader_vfs).unwrap();

    let warm_conn = rusqlite::Connection::open_with_flags_and_vfs(
        "warm_profile.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &reader_vfs_name,
    )
    .unwrap();

    // -- Phase 3: Warm up cache (same as benchmark COUNT(*)) --
    let count: i64 = warm_conn
        .query_row("SELECT COUNT(*) FROM posts", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 2000, "COUNT(*) should return 2000 posts");

    // -- Phase 4: Run the profile query (the one that was corrupted) --
    // This is the EXACT query from the benchmark that produced
    // "database disk image is malformed" before the fix.
    let profile_result: Result<Vec<(String, String, i64)>, _> = warm_conn
        .prepare(
            "SELECT u.first_name, u.last_name, p.id, p.content, p.created_at, p.like_count
             FROM users u
             JOIN posts p ON p.user_id = u.id
             WHERE u.id = 42
             ORDER BY p.created_at DESC
             LIMIT 10",
        )
        .and_then(|mut stmt| {
            stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .map(|rows| rows.filter_map(|r| r.ok()).collect())
        });

    assert!(
        profile_result.is_ok(),
        "profile query must not return 'database disk image is malformed': {:?}",
        profile_result.err()
    );
    let rows = profile_result.unwrap();
    assert_eq!(rows.len(), 10, "profile should return 10 posts for user 42");
    assert_eq!(rows[0].0, "First_42");
    assert_eq!(rows[0].1, "Last_42");

    // -- Phase 5: Run additional queries to stress test --
    // who-liked pattern (different index path)
    let user_count: i64 = warm_conn
        .query_row("SELECT COUNT(*) FROM users WHERE id < 100", [], |r| r.get(0))
        .unwrap();
    assert_eq!(user_count, 100);

    // Verify all index paths work
    let post_by_user: i64 = warm_conn
        .query_row(
            "SELECT COUNT(*) FROM posts WHERE user_id = 42",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(post_by_user, 10, "index scan on idx_posts_user should find 10 posts");

    drop(warm_conn);

    // Cleanup
    let cleanup_config = TurboliteConfig {
        bucket,
        prefix,
        cache_dir: reader_cache.path().to_path_buf(),
        compression_level: 3,
        endpoint_url: endpoint,
        region,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let cleanup_vfs = TurboliteVfs::new_local(cleanup_config).unwrap();
    cleanup_vfs.destroy_s3().unwrap();
}

#[test]
fn test_small_ppg_index_integrity() {
    let cache_dir = TempDir::new().unwrap();
    let unique_prefix = format!(
        "test/small_ppg_integrity/{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let bucket = test_bucket();
    let endpoint = Some(endpoint_url());
    let region = Some("auto".to_string());

    // Use ppg=8 to force many small page groups
    let config = TurboliteConfig {
        bucket: bucket.clone(),
        prefix: unique_prefix.clone(),
        cache_dir: cache_dir.path().to_path_buf(),
        compression_level: 3,
        endpoint_url: endpoint.clone(),
        region: region.clone(),
        pages_per_group: 8,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };

    let vfs_name = unique_vfs_name("small_ppg_write");
    let vfs = TurboliteVfs::new_local(config).expect("failed to create VFS");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "small_ppg_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    conn.execute_batch(
        "PRAGMA page_size=4096;
         PRAGMA journal_mode=WAL;
         PRAGMA wal_autocheckpoint=0;
         CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL, counter INTEGER NOT NULL);
         CREATE INDEX idx_items_counter ON items(counter);
         CREATE INDEX idx_items_value ON items(value);",
    )
    .unwrap();

    // Insert enough rows to span multiple page groups (ppg=8 = 32KB per group)
    let mut tx = conn.unchecked_transaction().unwrap();
    for i in 0..2000 {
        tx.execute(
            "INSERT INTO items (id, value, counter) VALUES (?1, ?2, ?3)",
            rusqlite::params![i, format!("item_{:08}", i), i % 100],
        )
        .unwrap();
    }
    tx.commit().unwrap();

    // Checkpoint to S3
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    drop(conn);

    // Cold reader: fresh cache, read from S3
    let read_cache = TempDir::new().unwrap();
    let reader_config = TurboliteConfig {
        bucket: bucket.clone(),
        prefix: unique_prefix.clone(),
        cache_dir: read_cache.path().to_path_buf(),
        compression_level: 3,
        endpoint_url: endpoint.clone(),
        read_only: true,
        region: region.clone(),
        pages_per_group: 8,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };

    let reader_vfs_name = unique_vfs_name("small_ppg_reader");
    let reader_vfs = TurboliteVfs::new_local(reader_config).expect("failed to create reader VFS");
    turbolite::tiered::register(&reader_vfs_name, reader_vfs).unwrap();

    let reader_conn = rusqlite::Connection::open_with_flags_and_vfs(
        "small_ppg_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &reader_vfs_name,
    )
    .unwrap();

    // integrity_check must pass (this was the regression: index pages returned as zeros)
    let results: Vec<String> = reader_conn
        .prepare("PRAGMA integrity_check")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .filter_map(|r| r.ok())
        .collect();
    assert_eq!(
        results,
        vec!["ok".to_string()],
        "integrity_check failed with ppg=8: {:?}",
        &results[..std::cmp::min(results.len(), 5)]
    );

    // Verify row count
    let count: i64 = reader_conn
        .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 2000, "expected 2000 rows, got {}", count);

    // Verify index is usable (query that requires the index)
    let sum: i64 = reader_conn
        .query_row(
            "SELECT SUM(counter) FROM items WHERE counter BETWEEN 10 AND 20",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(sum > 0, "index query returned 0, index may be corrupt");

    drop(reader_conn);

    // Cleanup S3
    {
        let cleanup_cache = TempDir::new().unwrap();
        let cleanup_config = TurboliteConfig {
            bucket,
            prefix: unique_prefix,
            cache_dir: cleanup_cache.path().to_path_buf(),
            compression_level: 3,
            endpoint_url: endpoint,
            region,
            pages_per_group: 8,
            runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
        };
        let cleanup_vfs = TurboliteVfs::new_local(cleanup_config).unwrap();
        cleanup_vfs.destroy_s3().unwrap();
    }
}
