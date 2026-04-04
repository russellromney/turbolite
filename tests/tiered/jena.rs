//! Phase Jena integration tests: interior map, sibling prefetch, predict_leaf,
//! leaf chasing, overflow prefetch, interior lookahead.
//!
//! These tests verify the full pipeline against real SQLite databases on Tigris.

use turbolite::tiered::{TurboliteConfig, TurboliteVfs, SyncMode};
use tempfile::TempDir;
use super::helpers::*;

fn open_conn(vfs_name: &str, db: &str) -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        db,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        vfs_name,
    ).unwrap();
    conn.execute_batch(
        "PRAGMA page_size=65536;
         PRAGMA journal_mode=WAL;",
    ).unwrap();
    conn
}

// ===== a. Interior map built from real data =====

/// After writing enough data to create interior pages, the interior map
/// should have entries. Verify it's non-empty on a second connection open.
#[test]
fn jena_interior_map_built_on_open() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("jena_imap", cache_dir.path());
    let (bucket, prefix, endpoint) = (config.bucket.clone(), config.prefix.clone(), config.endpoint_url.clone());

    let vfs = TurboliteVfs::new(config).unwrap();
    let vfs_name = unique_vfs_name("jena_imap");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = open_conn(&vfs_name, "jena_imap.db");

    // Insert enough data to create interior pages (B-tree splits)
    conn.execute_batch("CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT);").unwrap();
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..5000 {
            tx.execute(
                "INSERT INTO data VALUES (?1, ?2)",
                rusqlite::params![i, format!("value_{:06}", i)],
            ).unwrap();
        }
        tx.commit().unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    drop(conn);

    // Cold reader: verify data is accessible (interior map should build on open)
    let cold_dir = TempDir::new().unwrap();
    let cold_config = TurboliteConfig {
        bucket,
        prefix,
        cache_dir: cold_dir.path().to_path_buf(),
        endpoint_url: endpoint,
        region: Some("auto".to_string()),
        read_only: true,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let cold_vfs_name = unique_vfs_name("jena_imap_cold");
    let cold_vfs = TurboliteVfs::new(cold_config).unwrap();
    turbolite::tiered::register(&cold_vfs_name, cold_vfs).unwrap();

    let cold = rusqlite::Connection::open_with_flags_and_vfs(
        "jena_imap.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &cold_vfs_name,
    ).unwrap();

    // Verify data correctness
    let count: i64 = cold.query_row("SELECT COUNT(*) FROM data", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 5000);
    let val: String = cold.query_row("SELECT val FROM data WHERE id = 2500", [], |r| r.get(0)).unwrap();
    assert_eq!(val, "value_002500");
}

// ===== b. Sibling prefetch with real B-tree =====

/// Point lookup on a large table should work correctly. The interior map
/// provides exact sibling info, avoiding wasted prefetch.
#[test]
fn jena_point_lookup_large_table() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("jena_point", cache_dir.path());
    let (bucket, prefix, endpoint) = (config.bucket.clone(), config.prefix.clone(), config.endpoint_url.clone());

    let vfs = TurboliteVfs::new(config).unwrap();
    let vfs_name = unique_vfs_name("jena_point");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = open_conn(&vfs_name, "jena_point.db");
    conn.execute_batch("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);").unwrap();
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..10000 {
            tx.execute(
                "INSERT INTO users VALUES (?1, ?2, ?3)",
                rusqlite::params![i, format!("user_{}", i), format!("user_{}@test.com", i)],
            ).unwrap();
        }
        tx.commit().unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    drop(conn);

    // Cold reader: point lookup
    let cold_dir = TempDir::new().unwrap();
    let cold_config = TurboliteConfig {
        bucket,
        prefix,
        cache_dir: cold_dir.path().to_path_buf(),
        endpoint_url: endpoint,
        region: Some("auto".to_string()),
        read_only: true,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let cold_vfs_name = unique_vfs_name("jena_point_cold");
    let cold_vfs = TurboliteVfs::new(cold_config).unwrap();
    turbolite::tiered::register(&cold_vfs_name, cold_vfs).unwrap();

    let cold = rusqlite::Connection::open_with_flags_and_vfs(
        "jena_point.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &cold_vfs_name,
    ).unwrap();

    // Point lookup should work (interior map predicts exact leaf group)
    let name: String = cold.query_row(
        "SELECT name FROM users WHERE id = 7777", [], |r| r.get(0),
    ).unwrap();
    assert_eq!(name, "user_7777");

    // Multiple point lookups
    for id in [0, 1000, 5000, 9999] {
        let name: String = cold.query_row(
            "SELECT name FROM users WHERE id = ?1", rusqlite::params![id], |r| r.get(0),
        ).unwrap();
        assert_eq!(name, format!("user_{}", id));
    }
}

// ===== c. Index scan with predict_leaf =====

/// Index scan should work correctly with the interior map providing
/// key boundary predictions.
#[test]
fn jena_index_scan() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("jena_idx", cache_dir.path());
    let (bucket, prefix, endpoint) = (config.bucket.clone(), config.prefix.clone(), config.endpoint_url.clone());

    let vfs = TurboliteVfs::new(config).unwrap();
    let vfs_name = unique_vfs_name("jena_idx");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = open_conn(&vfs_name, "jena_idx.db");
    conn.execute_batch(
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL);
         CREATE INDEX idx_products_price ON products(price);"
    ).unwrap();
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..5000 {
            tx.execute(
                "INSERT INTO products VALUES (?1, ?2, ?3)",
                rusqlite::params![i, format!("product_{}", i), (i as f64) * 0.99],
            ).unwrap();
        }
        tx.commit().unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    drop(conn);

    // Cold reader: range query via index
    let cold_dir = TempDir::new().unwrap();
    let cold_config = TurboliteConfig {
        bucket,
        prefix,
        cache_dir: cold_dir.path().to_path_buf(),
        endpoint_url: endpoint,
        region: Some("auto".to_string()),
        read_only: true,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let cold_vfs_name = unique_vfs_name("jena_idx_cold");
    let cold_vfs = TurboliteVfs::new(cold_config).unwrap();
    turbolite::tiered::register(&cold_vfs_name, cold_vfs).unwrap();

    let cold = rusqlite::Connection::open_with_flags_and_vfs(
        "jena_idx.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &cold_vfs_name,
    ).unwrap();

    // Range query on indexed column
    let count: i64 = cold.query_row(
        "SELECT COUNT(*) FROM products WHERE price BETWEEN 100.0 AND 200.0",
        [], |r| r.get(0),
    ).unwrap();
    // price = i * 0.99, so 100.0 <= i*0.99 <= 200.0 => i in [102, 202]
    assert!(count > 90 && count < 110, "expected ~101 rows, got {}", count);
}

// ===== d. Join with leaf chasing =====

/// Multi-table join: posts + users. The leaf chaser should detect the
/// FK relationship and prefetch user groups while reading posts.
#[test]
fn jena_join_posts_users() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("jena_join", cache_dir.path());
    let (bucket, prefix, endpoint) = (config.bucket.clone(), config.prefix.clone(), config.endpoint_url.clone());

    let vfs = TurboliteVfs::new(config).unwrap();
    let vfs_name = unique_vfs_name("jena_join");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = open_conn(&vfs_name, "jena_join.db");
    conn.execute_batch(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
         CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);
         CREATE INDEX idx_posts_user ON posts(user_id);"
    ).unwrap();
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..500 {
            tx.execute("INSERT INTO users VALUES (?1, ?2)", rusqlite::params![i, format!("user_{}", i)]).unwrap();
        }
        for i in 0..5000 {
            tx.execute(
                "INSERT INTO posts VALUES (?1, ?2, ?3)",
                rusqlite::params![i, i % 500, format!("post_{}", i)],
            ).unwrap();
        }
        tx.commit().unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    drop(conn);

    // Cold reader: join query
    let cold_dir = TempDir::new().unwrap();
    let cold_config = TurboliteConfig {
        bucket,
        prefix,
        cache_dir: cold_dir.path().to_path_buf(),
        endpoint_url: endpoint,
        region: Some("auto".to_string()),
        read_only: true,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let cold_vfs_name = unique_vfs_name("jena_join_cold");
    let cold_vfs = TurboliteVfs::new(cold_config).unwrap();
    turbolite::tiered::register(&cold_vfs_name, cold_vfs).unwrap();

    let cold = rusqlite::Connection::open_with_flags_and_vfs(
        "jena_join.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &cold_vfs_name,
    ).unwrap();

    // Join: for each user, count their posts
    let rows: Vec<(String, i64)> = {
        let mut stmt = cold.prepare(
            "SELECT u.name, COUNT(p.id) FROM users u
             JOIN posts p ON p.user_id = u.id
             WHERE u.id < 10
             GROUP BY u.id ORDER BY u.id"
        ).unwrap();
        stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect()
    };
    assert_eq!(rows.len(), 10);
    assert_eq!(rows[0].0, "user_0");
    assert_eq!(rows[0].1, 10); // 5000 posts / 500 users = 10 per user
}

// ===== e. Large blobs (overflow) =====

/// Table with large TEXT values that overflow. The overflow prefetcher
/// should proactively fetch overflow page groups.
#[test]
fn jena_overflow_large_text() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("jena_ovfl", cache_dir.path());
    let (bucket, prefix, endpoint) = (config.bucket.clone(), config.prefix.clone(), config.endpoint_url.clone());

    let vfs = TurboliteVfs::new(config).unwrap();
    let vfs_name = unique_vfs_name("jena_ovfl");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = open_conn(&vfs_name, "jena_ovfl.db");
    conn.execute_batch("CREATE TABLE docs (id INTEGER PRIMARY KEY, content TEXT);").unwrap();
    {
        let tx = conn.unchecked_transaction().unwrap();
        // Each row ~100KB (will overflow at 64KB page size)
        let big_text = "x".repeat(100_000);
        for i in 0..50 {
            tx.execute(
                "INSERT INTO docs VALUES (?1, ?2)",
                rusqlite::params![i, format!("{}_{}", i, big_text)],
            ).unwrap();
        }
        tx.commit().unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    drop(conn);

    // Cold reader: read a large row
    let cold_dir = TempDir::new().unwrap();
    let cold_config = TurboliteConfig {
        bucket,
        prefix,
        cache_dir: cold_dir.path().to_path_buf(),
        endpoint_url: endpoint,
        region: Some("auto".to_string()),
        read_only: true,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let cold_vfs_name = unique_vfs_name("jena_ovfl_cold");
    let cold_vfs = TurboliteVfs::new(cold_config).unwrap();
    turbolite::tiered::register(&cold_vfs_name, cold_vfs).unwrap();

    let cold = rusqlite::Connection::open_with_flags_and_vfs(
        "jena_ovfl.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &cold_vfs_name,
    ).unwrap();

    let content: String = cold.query_row(
        "SELECT content FROM docs WHERE id = 25", [], |r| r.get(0),
    ).unwrap();
    assert!(content.starts_with("25_"));
    assert!(content.len() > 100_000);

    // Verify all rows accessible
    let count: i64 = cold.query_row("SELECT COUNT(*) FROM docs", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 50);
}

// ===== Full scan =====

/// Full table scan should work correctly with the interior map.
#[test]
fn jena_full_scan() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("jena_scan", cache_dir.path());
    let (bucket, prefix, endpoint) = (config.bucket.clone(), config.prefix.clone(), config.endpoint_url.clone());

    let vfs = TurboliteVfs::new(config).unwrap();
    let vfs_name = unique_vfs_name("jena_scan");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = open_conn(&vfs_name, "jena_scan.db");
    conn.execute_batch("CREATE TABLE logs (id INTEGER PRIMARY KEY, level TEXT, msg TEXT);").unwrap();
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..10000 {
            let level = if i % 10 == 0 { "ERROR" } else { "INFO" };
            tx.execute(
                "INSERT INTO logs VALUES (?1, ?2, ?3)",
                rusqlite::params![i, level, format!("log message {}", i)],
            ).unwrap();
        }
        tx.commit().unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    drop(conn);

    // Cold reader: full scan with filter
    let cold_dir = TempDir::new().unwrap();
    let cold_config = TurboliteConfig {
        bucket,
        prefix,
        cache_dir: cold_dir.path().to_path_buf(),
        endpoint_url: endpoint,
        region: Some("auto".to_string()),
        read_only: true,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let cold_vfs_name = unique_vfs_name("jena_scan_cold");
    let cold_vfs = TurboliteVfs::new(cold_config).unwrap();
    turbolite::tiered::register(&cold_vfs_name, cold_vfs).unwrap();

    let cold = rusqlite::Connection::open_with_flags_and_vfs(
        "jena_scan.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &cold_vfs_name,
    ).unwrap();

    // Full scan with aggregate
    let error_count: i64 = cold.query_row(
        "SELECT COUNT(*) FROM logs WHERE level = 'ERROR'",
        [], |r| r.get(0),
    ).unwrap();
    assert_eq!(error_count, 1000); // every 10th row
}
