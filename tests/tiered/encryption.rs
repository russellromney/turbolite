//! Encryption and key rotation tests.

use turbolite::tiered::{TurboliteConfig, TurboliteVfs};
use tempfile::TempDir;
use super::helpers::*;

fn test_encryption_key() -> [u8; 32] {
    let mut key = [0u8; 32];
    for (i, b) in key.iter_mut().enumerate() { *b = (i as u8).wrapping_mul(7); }
    key
}

/// Create a TurboliteConfig with encryption key set.
fn test_config_encrypted(prefix: &str, cache_dir: &std::path::Path) -> TurboliteConfig {
    let mut config = test_config(prefix, cache_dir);
    config.encryption_key = Some(test_encryption_key());
    config
}

fn test_encryption_key_2() -> [u8; 32] {
    let mut key = [0u8; 32];
    for (i, b) in key.iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(13).wrapping_add(0x42);
    }
    key
}

/// Write encrypted data to S3, then read it back from a fresh cold cache.
/// Verifies the full encrypt/decrypt pipeline: encode -> S3 -> cold read -> decode.
#[test]
fn test_encrypted_write_cold_read() {
    let writer_cache = TempDir::new().unwrap();
    let config = test_config_encrypted("encrypted_basic", writer_cache.path());
    let vfs_name = unique_vfs_name("tiered_enc");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();

    // Write phase
    {
        let vfs = TurboliteVfs::new_local(config).expect("failed to create encrypted TurboliteVfs");
        turbolite::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "encrypted_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        ).expect("failed to open connection");

        conn.execute_batch(
            "PRAGMA page_size=65536;
             PRAGMA journal_mode=WAL;
             CREATE TABLE secrets (id INTEGER PRIMARY KEY, data TEXT);",
        ).expect("failed to create table");

        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..200 {
                tx.execute(
                    "INSERT INTO secrets (id, data) VALUES (?1, ?2)",
                    rusqlite::params![i, format!("secret_{}", i)],
                ).unwrap();
            }
            tx.commit().unwrap();
        }

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .expect("checkpoint failed");

        // Verify manifest exists in S3
        verify_s3_manifest(&bucket, &prefix, &endpoint, 1, 65536);
    }

    // Cold read phase: fresh cache, same encryption key
    {
        let reader_cache = TempDir::new().unwrap();
        let reader_config = TurboliteConfig {
            bucket: bucket.clone(),
            prefix: prefix.clone(),
            cache_dir: reader_cache.path().to_path_buf(),
            compression_level: 3,
            endpoint_url: endpoint.clone(),
            region: region.clone(),
            encryption_key: Some(test_encryption_key()),
            runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("tiered_enc_read");
        let vfs = TurboliteVfs::new_local(reader_config).expect("failed to create reader VFS");
        turbolite::tiered::register(&reader_vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "encrypted_test_reader.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &reader_vfs_name,
        ).expect("failed to open reader connection");

        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM secrets", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 200, "expected 200 rows from cold read");

        let value: String = conn
            .query_row(
                "SELECT data FROM secrets WHERE id = 99",
                [],
                |row| row.get(0),
            ).unwrap();
        assert_eq!(value, "secret_99", "row content mismatch on cold read");
    }

    // Cleanup S3
    {
        let cleanup_cache = TempDir::new().unwrap();
        let cleanup_config = TurboliteConfig {
            bucket,
            prefix,
            cache_dir: cleanup_cache.path().to_path_buf(),
            compression_level: 3,
            endpoint_url: endpoint,
            region,
            encryption_key: Some(test_encryption_key()),
            runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
        };
        let cleanup_vfs = TurboliteVfs::new_local(cleanup_config).unwrap();
        cleanup_vfs.destroy_s3().unwrap();
    }
}

/// Verify that reading encrypted S3 data with the wrong key fails gracefully.
#[test]
fn test_encrypted_wrong_key_cold_read_fails() {
    let writer_cache = TempDir::new().unwrap();
    let config = test_config_encrypted("encrypted_wrong_key", writer_cache.path());
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();

    // Write with correct key
    {
        let vfs_name = unique_vfs_name("tiered_enc_wr");
        let vfs = TurboliteVfs::new_local(config).expect("failed to create encrypted TurboliteVfs");
        turbolite::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "enc_wrong_key.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        ).expect("failed to open connection");

        conn.execute_batch(
            "PRAGMA page_size=65536;
             PRAGMA journal_mode=WAL;
             CREATE TABLE t (id INTEGER PRIMARY KEY);
             INSERT INTO t VALUES (1);
             PRAGMA wal_checkpoint(TRUNCATE);",
        ).unwrap();
    }

    // Cold read with WRONG key — must fail (at open, pragma, or query level)
    {
        let reader_cache = TempDir::new().unwrap();
        let wrong_key = [0xFFu8; 32];
        let reader_config = TurboliteConfig {
            bucket: bucket.clone(),
            prefix: prefix.clone(),
            cache_dir: reader_cache.path().to_path_buf(),
            compression_level: 3,
            endpoint_url: endpoint.clone(),
            region: region.clone(),
            encryption_key: Some(wrong_key),
            runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("tiered_enc_wrong");
        let vfs = TurboliteVfs::new_local(reader_config).expect("failed to create reader VFS");
        turbolite::tiered::register(&reader_vfs_name, vfs).unwrap();

        // Wrong key may fail at connection open (SQLite reads page 0 header
        // during open, triggering decryption with wrong key), or later on query.
        let conn_result = rusqlite::Connection::open_with_flags_and_vfs(
            "enc_wrong_key_reader.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &reader_vfs_name,
        );
        let failed = if let Ok(conn) = conn_result {
            let pragma_result = conn.execute_batch("PRAGMA journal_mode=WAL;");
            let query_result = conn.query_row("SELECT COUNT(*) FROM t", [], |row| row.get::<_, i64>(0));
            pragma_result.is_err() || query_result.is_err()
        } else {
            true // open itself failed with wrong key
        };
        assert!(failed, "wrong encryption key must cause failure, not return garbage");
    }

    // Cleanup
    {
        let cleanup_cache = TempDir::new().unwrap();
        let cleanup_config = TurboliteConfig {
            bucket,
            prefix,
            cache_dir: cleanup_cache.path().to_path_buf(),
            compression_level: 3,
            endpoint_url: endpoint,
            region,
            encryption_key: Some(test_encryption_key()),
            runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
        };
        let cleanup_vfs = TurboliteVfs::new_local(cleanup_config).unwrap();
        cleanup_vfs.destroy_s3().unwrap();
    }
}

/// Arctic start with encrypted DB: completely fresh cache, verify all page types
/// (interior, index leaf, data) decrypt correctly from S3.
#[test]
fn test_encrypted_arctic_start_all_page_types() {
    let writer_cache = TempDir::new().unwrap();
    let config = test_config_encrypted("encrypted_arctic", writer_cache.path());
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();

    // Write: create a table with an index (exercises interior, index leaf, and data pages)
    {
        let vfs_name = unique_vfs_name("tiered_enc_arctic_w");
        let vfs = TurboliteVfs::new_local(config).expect("failed to create encrypted TurboliteVfs");
        turbolite::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "enc_arctic.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        ).expect("failed to open connection");

        conn.execute_batch(
            "PRAGMA page_size=65536;
             PRAGMA journal_mode=WAL;
             CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, value REAL);
             CREATE INDEX idx_name ON items(name);",
        ).unwrap();

        // Insert enough rows to exercise multiple page types
        conn.execute_batch("BEGIN;").unwrap();
        for i in 0..500 {
            conn.execute(
                "INSERT INTO items VALUES (?1, ?2, ?3)",
                rusqlite::params![i, format!("item_{:05}", i), i as f64 * 1.5],
            ).unwrap();
        }
        conn.execute_batch("COMMIT;").unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    // Arctic read: completely fresh cache, no data cached at all
    {
        let reader_cache = TempDir::new().unwrap();
        let reader_config = TurboliteConfig {
            bucket: bucket.clone(),
            prefix: prefix.clone(),
            cache_dir: reader_cache.path().to_path_buf(),
            compression_level: 3,
            endpoint_url: endpoint.clone(),
            region: region.clone(),
            encryption_key: Some(test_encryption_key()),
            runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("tiered_enc_arctic_r");
        let vfs = TurboliteVfs::new_local(reader_config).expect("failed to create reader VFS");
        turbolite::tiered::register(&reader_vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "enc_arctic_reader.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &reader_vfs_name,
        ).expect("failed to open reader connection");

        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();

        // Verify data pages: count and content
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM items", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 500, "expected 500 rows from arctic encrypted read");

        // Verify index pages: use the index to look up by name
        let value: f64 = conn
            .query_row(
                "SELECT value FROM items WHERE name = 'item_00250'",
                [],
                |row| row.get(0),
            ).unwrap();
        assert!((value - 375.0).abs() < 0.001, "indexed lookup value mismatch: {}", value);

        // Verify interior pages: point lookup by PK (traverses interior B-tree)
        let name: String = conn
            .query_row(
                "SELECT name FROM items WHERE id = 499",
                [],
                |row| row.get(0),
            ).unwrap();
        assert_eq!(name, "item_00499", "PK lookup mismatch on arctic encrypted read");
    }

    // Cleanup S3
    {
        let cleanup_cache = TempDir::new().unwrap();
        let cleanup_config = TurboliteConfig {
            bucket,
            prefix,
            cache_dir: cleanup_cache.path().to_path_buf(),
            compression_level: 3,
            endpoint_url: endpoint,
            region,
            encryption_key: Some(test_encryption_key()),
            runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
        };
        let cleanup_vfs = TurboliteVfs::new_local(cleanup_config).unwrap();
        cleanup_vfs.destroy_s3().unwrap();
    }
}

// ===== Key rotation integration tests =====

/// Write encrypted data with key A, rotate to key B, cold read with key B succeeds.
#[test]
fn test_rotate_key_cold_read_succeeds() {
    use turbolite::tiered::rotate_encryption_key;

    let writer_cache = TempDir::new().unwrap();
    let config = test_config_encrypted("rotate_basic", writer_cache.path());
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();
    let key_a = test_encryption_key();
    let key_b = test_encryption_key_2();

    // Write phase with key A
    {
        let vfs_name = unique_vfs_name("tiered_rot_wr");
        let vfs = TurboliteVfs::new_local(config).expect("failed to create encrypted TurboliteVfs");
        turbolite::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "rotate_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch(
            "PRAGMA page_size=65536;
             PRAGMA journal_mode=WAL;
             CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT);
             CREATE INDEX idx_value ON data(value);",
        )
        .unwrap();

        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..500 {
                tx.execute(
                    "INSERT INTO data (id, value) VALUES (?1, ?2)",
                    rusqlite::params![i, format!("row_{:05}", i)],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
    }

    // Rotate key A to key B
    {
        let rotate_cache = TempDir::new().unwrap();
        let rotate_config = TurboliteConfig {
            bucket: bucket.clone(),
            prefix: prefix.clone(),
            cache_dir: rotate_cache.path().to_path_buf(),
            compression_level: 3,
            endpoint_url: endpoint.clone(),
            region: region.clone(),
            encryption_key: Some(key_a),
            runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
        };

        rotate_encryption_key(&rotate_config, Some(key_b))
            .expect("key rotation failed");
    }

    // Cold read with key B (fresh cache)
    {
        let reader_cache = TempDir::new().unwrap();
        let reader_config = TurboliteConfig {
            bucket: bucket.clone(),
            prefix: prefix.clone(),
            cache_dir: reader_cache.path().to_path_buf(),
            compression_level: 3,
            endpoint_url: endpoint.clone(),
            region: region.clone(),
            encryption_key: Some(key_b),
            runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("tiered_rot_rd");
        let vfs = TurboliteVfs::new_local(reader_config).unwrap();
        turbolite::tiered::register(&reader_vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "rotate_reader.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &reader_vfs_name,
        )
        .unwrap();

        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();

        // Data pages: COUNT
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM data", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 500, "row count mismatch after rotation");

        // Index pages: WHERE value = (uses idx_value)
        let val: String = conn
            .query_row(
                "SELECT value FROM data WHERE value = 'row_00250'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(val, "row_00250", "index lookup failed after rotation");

        // Interior pages: WHERE id = (PK B-tree traversal)
        let val: String = conn
            .query_row("SELECT value FROM data WHERE id = 499", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(val, "row_00499", "PK lookup failed after rotation");
    }

    // Cleanup
    {
        let cleanup_cache = TempDir::new().unwrap();
        let cleanup_config = TurboliteConfig {
            bucket,
            prefix,
            cache_dir: cleanup_cache.path().to_path_buf(),
            compression_level: 3,
            endpoint_url: endpoint,
            region,
            encryption_key: Some(key_b),
            runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
        };
        let cleanup_vfs = TurboliteVfs::new_local(cleanup_config).unwrap();
        cleanup_vfs.destroy_s3().unwrap();
    }
}

/// After rotation, reading with the OLD key must fail.
#[test]
fn test_rotate_key_old_key_fails() {
    use turbolite::tiered::rotate_encryption_key;

    let writer_cache = TempDir::new().unwrap();
    let config = test_config_encrypted("rotate_old_key", writer_cache.path());
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();
    let key_a = test_encryption_key();
    let key_b = test_encryption_key_2();

    // Write with key A
    {
        let vfs_name = unique_vfs_name("tiered_rot_old_wr");
        let vfs = TurboliteVfs::new_local(config).unwrap();
        turbolite::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "rotate_old_key.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch(
            "PRAGMA page_size=65536;
             PRAGMA journal_mode=WAL;
             CREATE TABLE t (id INTEGER PRIMARY KEY);
             INSERT INTO t VALUES (1);
             PRAGMA wal_checkpoint(TRUNCATE);",
        )
        .unwrap();
    }

    // Rotate to key B
    {
        let rotate_cache = TempDir::new().unwrap();
        let rotate_config = TurboliteConfig {
            bucket: bucket.clone(),
            prefix: prefix.clone(),
            cache_dir: rotate_cache.path().to_path_buf(),
            compression_level: 3,
            endpoint_url: endpoint.clone(),
            region: region.clone(),
            encryption_key: Some(key_a),
            runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
        };
        rotate_encryption_key(&rotate_config, Some(key_b)).unwrap();
    }

    // Cold read with OLD key A must fail (at open, pragma, or query level)
    {
        let reader_cache = TempDir::new().unwrap();
        let reader_config = TurboliteConfig {
            bucket: bucket.clone(),
            prefix: prefix.clone(),
            cache_dir: reader_cache.path().to_path_buf(),
            compression_level: 3,
            endpoint_url: endpoint.clone(),
            region: region.clone(),
            encryption_key: Some(key_a),
            runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("tiered_rot_old_rd");
        let vfs = TurboliteVfs::new_local(reader_config).unwrap();
        turbolite::tiered::register(&reader_vfs_name, vfs).unwrap();

        // Wrong key may fail at connection open (SQLite reads page 0 header
        // during open, which triggers decryption), or at the subsequent query.
        let conn_result = rusqlite::Connection::open_with_flags_and_vfs(
            "rotate_old_key_reader.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &reader_vfs_name,
        );
        let failed = if let Ok(conn) = conn_result {
            let pragma_result = conn.execute_batch("PRAGMA journal_mode=WAL;");
            let query_result =
                conn.query_row("SELECT COUNT(*) FROM t", [], |row| row.get::<_, i64>(0));
            pragma_result.is_err() || query_result.is_err()
        } else {
            true // open itself failed, which is a valid failure mode
        };
        assert!(failed, "old key must fail after rotation");
    }

    // Cleanup with new key B
    {
        let cleanup_cache = TempDir::new().unwrap();
        let cleanup_config = TurboliteConfig {
            bucket,
            prefix,
            cache_dir: cleanup_cache.path().to_path_buf(),
            compression_level: 3,
            endpoint_url: endpoint,
            region,
            encryption_key: Some(key_b),
            runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
        };
        let cleanup_vfs = TurboliteVfs::new_local(cleanup_config).unwrap();
        cleanup_vfs.destroy_s3().unwrap();
    }
}

/// After rotation, old S3 objects should be cleaned up by GC.
#[test]
fn test_rotate_key_gc_cleans_old_objects() {
    use turbolite::tiered::rotate_encryption_key;

    let writer_cache = TempDir::new().unwrap();
    let config = test_config_encrypted("rotate_gc", writer_cache.path());
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();
    let key_a = test_encryption_key();
    let key_b = test_encryption_key_2();

    // Write with key A
    {
        let vfs_name = unique_vfs_name("tiered_rot_gc_wr");
        let vfs = TurboliteVfs::new_local(config).unwrap();
        turbolite::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "rotate_gc.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch(
            "PRAGMA page_size=65536;
             PRAGMA journal_mode=WAL;
             CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);",
        )
        .unwrap();

        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..100 {
                tx.execute(
                    "INSERT INTO t (id, v) VALUES (?1, ?2)",
                    rusqlite::params![i, format!("v{}", i)],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
    }

    // List S3 objects before rotation
    let keys_before = {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
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
                .list_objects_v2()
                .bucket(&bucket)
                .prefix(&prefix)
                .send()
                .await
                .unwrap();
            resp.contents()
                .iter()
                .filter_map(|o| o.key().map(String::from))
                .collect::<Vec<_>>()
        })
    };

    // Rotate to key B (includes GC of old objects)
    {
        let rotate_cache = TempDir::new().unwrap();
        let rotate_config = TurboliteConfig {
            bucket: bucket.clone(),
            prefix: prefix.clone(),
            cache_dir: rotate_cache.path().to_path_buf(),
            compression_level: 3,
            endpoint_url: endpoint.clone(),
            region: region.clone(),
            encryption_key: Some(key_a),
            runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
        };
        rotate_encryption_key(&rotate_config, Some(key_b)).unwrap();
    }

    // List S3 objects after rotation
    let keys_after = {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
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
                .list_objects_v2()
                .bucket(&bucket)
                .prefix(&prefix)
                .send()
                .await
                .unwrap();
            resp.contents()
                .iter()
                .filter_map(|o| o.key().map(String::from))
                .collect::<Vec<_>>()
        })
    };

    // After rotation, there should be fewer or equal keys (GC deletes replaced versions).
    // The exact set depends on how many checkpoint versions were created, but the new
    // manifest should reference only new-version keys.
    assert!(
        keys_after.len() <= keys_before.len() + 1,
        "rotation should not create unbounded key growth: before={}, after={}",
        keys_before.len(), keys_after.len(),
    );

    // Manifest still exists (it's the same key, not versioned)
    let manifest_key = format!("{}/manifest.msgpack", prefix);
    assert!(
        keys_after.contains(&manifest_key),
        "manifest must still exist after rotation"
    );

    // New versioned objects exist (version > any pre-rotation version)
    // After rotation, manifest.version is one higher than the checkpoint version.
    // Check that keys_after has objects NOT present in keys_before (rotation created new versions).
    let new_keys: Vec<&String> = keys_after.iter()
        .filter(|k| !keys_before.contains(k) && k.contains("/p/d/"))
        .collect();
    assert!(!new_keys.is_empty(), "rotation must create new versioned page group objects, got keys_after={:?}", keys_after);

    // Cleanup with new key
    {
        let cleanup_cache = TempDir::new().unwrap();
        let cleanup_config = TurboliteConfig {
            bucket,
            prefix,
            cache_dir: cleanup_cache.path().to_path_buf(),
            compression_level: 3,
            endpoint_url: endpoint,
            region,
            encryption_key: Some(key_b),
            runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
        };
        let cleanup_vfs = TurboliteVfs::new_local(cleanup_config).unwrap();
        cleanup_vfs.destroy_s3().unwrap();
    }
}

/// Data integrity: row-level verification that rotation preserves all data.
#[test]
fn test_rotate_key_data_integrity() {
    use turbolite::tiered::rotate_encryption_key;
    use std::collections::HashMap;

    let writer_cache = TempDir::new().unwrap();
    let config = test_config_encrypted("rotate_integrity", writer_cache.path());
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();
    let key_a = test_encryption_key();
    let key_b = test_encryption_key_2();

    // Write data and collect checksums
    let mut expected_rows: HashMap<i64, String> = HashMap::new();
    {
        let vfs_name = unique_vfs_name("tiered_rot_int_wr");
        let vfs = TurboliteVfs::new_local(config).unwrap();
        turbolite::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "rotate_integrity.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch(
            "PRAGMA page_size=65536;
             PRAGMA journal_mode=WAL;
             CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
             CREATE INDEX idx_data_value ON data(value);",
        )
        .unwrap();

        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..500 {
                let value = format!("integrity_check_{:05}_{}", i, "x".repeat((i % 50) as usize));
                tx.execute(
                    "INSERT INTO data (id, value) VALUES (?1, ?2)",
                    rusqlite::params![i, value],
                )
                .unwrap();
                expected_rows.insert(i, value);
            }
            tx.commit().unwrap();
        }

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
    }

    // Rotate
    {
        let rotate_cache = TempDir::new().unwrap();
        let rotate_config = TurboliteConfig {
            bucket: bucket.clone(),
            prefix: prefix.clone(),
            cache_dir: rotate_cache.path().to_path_buf(),
            compression_level: 3,
            endpoint_url: endpoint.clone(),
            region: region.clone(),
            encryption_key: Some(key_a),
            runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
        };
        rotate_encryption_key(&rotate_config, Some(key_b)).unwrap();
    }

    // Verify every row matches
    {
        let reader_cache = TempDir::new().unwrap();
        let reader_config = TurboliteConfig {
            bucket: bucket.clone(),
            prefix: prefix.clone(),
            cache_dir: reader_cache.path().to_path_buf(),
            compression_level: 3,
            endpoint_url: endpoint.clone(),
            region: region.clone(),
            encryption_key: Some(key_b),
            runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("tiered_rot_int_rd");
        let vfs = TurboliteVfs::new_local(reader_config).unwrap();
        turbolite::tiered::register(&reader_vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "rotate_integrity_reader.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &reader_vfs_name,
        )
        .unwrap();

        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();

        let mut stmt = conn.prepare("SELECT id, value FROM data ORDER BY id").unwrap();
        let rows: Vec<(i64, String)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(rows.len(), expected_rows.len(), "row count mismatch");
        for (id, value) in &rows {
            let expected = expected_rows.get(id).expect("unexpected row id");
            assert_eq!(
                value, expected,
                "data corruption: row {} has '{}', expected '{}'",
                id, value, expected,
            );
        }
    }

    // Cleanup
    {
        let cleanup_cache = TempDir::new().unwrap();
        let cleanup_config = TurboliteConfig {
            bucket,
            prefix,
            cache_dir: cleanup_cache.path().to_path_buf(),
            compression_level: 3,
            endpoint_url: endpoint,
            region,
            encryption_key: Some(key_b),
            runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
        };
        let cleanup_vfs = TurboliteVfs::new_local(cleanup_config).unwrap();
        cleanup_vfs.destroy_s3().unwrap();
    }
}

/// Write encrypted data, remove encryption (rotate to None), cold read without key.
#[test]
fn test_remove_encryption_cold_read() {
    use turbolite::tiered::rotate_encryption_key;

    let writer_cache = TempDir::new().unwrap();
    let config = test_config_encrypted("remove_enc", writer_cache.path());
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();
    let key_a = test_encryption_key();

    // Write phase with encryption
    {
        let vfs_name = unique_vfs_name("tiered_rmenc_wr");
        let vfs = TurboliteVfs::new_local(config).expect("failed to create encrypted TurboliteVfs");
        turbolite::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "rmenc_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch(
            "PRAGMA page_size=65536;
             PRAGMA journal_mode=WAL;
             CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT);",
        )
        .unwrap();

        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..100 {
                tx.execute(
                    "INSERT INTO data (id, value) VALUES (?1, ?2)",
                    rusqlite::params![i, format!("val_{:04}", i)],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
    }

    // Remove encryption (rotate to None)
    {
        let rotate_cache = TempDir::new().unwrap();
        let rotate_config = TurboliteConfig {
            bucket: bucket.clone(),
            prefix: prefix.clone(),
            cache_dir: rotate_cache.path().to_path_buf(),
            compression_level: 3,
            endpoint_url: endpoint.clone(),
            region: region.clone(),
            encryption_key: Some(key_a),
            runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
        };

        rotate_encryption_key(&rotate_config, None)
            .expect("remove encryption failed");
    }

    // Cold read WITHOUT encryption key
    {
        let reader_cache = TempDir::new().unwrap();
        let reader_config = TurboliteConfig {
            bucket: bucket.clone(),
            prefix: prefix.clone(),
            cache_dir: reader_cache.path().to_path_buf(),
            compression_level: 3,
            endpoint_url: endpoint.clone(),
            region: region.clone(),
            encryption_key: None, // no key needed
            runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("tiered_rmenc_rd");
        let vfs = TurboliteVfs::new_local(reader_config).unwrap();
        turbolite::tiered::register(&reader_vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "rmenc_reader.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &reader_vfs_name,
        )
        .unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM data", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 100, "expected 100 rows after removing encryption");

        let val: String = conn
            .query_row("SELECT value FROM data WHERE id = 42", [], |row| row.get(0))
            .unwrap();
        assert_eq!(val, "val_0042");
    }

    // Cleanup
    {
        let cleanup_cache = TempDir::new().unwrap();
        let cleanup_config = TurboliteConfig {
            bucket,
            prefix,
            cache_dir: cleanup_cache.path().to_path_buf(),
            compression_level: 3,
            endpoint_url: endpoint,
            region,
            encryption_key: None,
            runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
        };
        let cleanup_vfs = TurboliteVfs::new_local(cleanup_config).unwrap();
        cleanup_vfs.destroy_s3().unwrap();
    }
}

/// Write unencrypted data, add encryption (rotate from None to Some), cold read with key.
#[test]
fn test_add_encryption_cold_read() {
    use turbolite::tiered::rotate_encryption_key;

    let writer_cache = TempDir::new().unwrap();
    let config = test_config("add_enc", writer_cache.path());
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();
    let key_b = test_encryption_key();

    // Write phase WITHOUT encryption
    {
        let vfs_name = unique_vfs_name("tiered_addenc_wr");
        let vfs = TurboliteVfs::new_local(config).expect("failed to create TurboliteVfs");
        turbolite::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "addenc_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch(
            "PRAGMA page_size=65536;
             PRAGMA journal_mode=WAL;
             CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT);",
        )
        .unwrap();

        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..100 {
                tx.execute(
                    "INSERT INTO data (id, value) VALUES (?1, ?2)",
                    rusqlite::params![i, format!("val_{:04}", i)],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
    }

    // Add encryption (rotate from None to Some(key_b))
    {
        let rotate_cache = TempDir::new().unwrap();
        let rotate_config = TurboliteConfig {
            bucket: bucket.clone(),
            prefix: prefix.clone(),
            cache_dir: rotate_cache.path().to_path_buf(),
            compression_level: 3,
            endpoint_url: endpoint.clone(),
            region: region.clone(),
            encryption_key: None, // no old key
            runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
        };

        rotate_encryption_key(&rotate_config, Some(key_b))
            .expect("add encryption failed");
    }

    // Cold read WITH encryption key
    {
        let reader_cache = TempDir::new().unwrap();
        let reader_config = TurboliteConfig {
            bucket: bucket.clone(),
            prefix: prefix.clone(),
            cache_dir: reader_cache.path().to_path_buf(),
            compression_level: 3,
            endpoint_url: endpoint.clone(),
            region: region.clone(),
            encryption_key: Some(key_b),
            runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("tiered_addenc_rd");
        let vfs = TurboliteVfs::new_local(reader_config).unwrap();
        turbolite::tiered::register(&reader_vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "addenc_reader.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &reader_vfs_name,
        )
        .unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM data", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 100, "expected 100 rows after adding encryption");

        let val: String = conn
            .query_row("SELECT value FROM data WHERE id = 42", [], |row| row.get(0))
            .unwrap();
        assert_eq!(val, "val_0042");
    }

    // Verify old unencrypted read fails
    {
        let fail_cache = TempDir::new().unwrap();
        let fail_config = TurboliteConfig {
            bucket: bucket.clone(),
            prefix: prefix.clone(),
            cache_dir: fail_cache.path().to_path_buf(),
            compression_level: 3,
            endpoint_url: endpoint.clone(),
            region: region.clone(),
            encryption_key: None, // no key, but data is now encrypted
            runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
        };
        let fail_vfs_name = unique_vfs_name("tiered_addenc_fail");
        let vfs = TurboliteVfs::new_local(fail_config).unwrap();
        turbolite::tiered::register(&fail_vfs_name, vfs).unwrap();

        let result = rusqlite::Connection::open_with_flags_and_vfs(
            "addenc_fail.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &fail_vfs_name,
        );
        // Opening or reading should fail (encrypted data can't be decompressed)
        if let Ok(conn) = result {
            let query_result: Result<i64, _> =
                conn.query_row("SELECT COUNT(*) FROM data", [], |row| row.get(0));
            assert!(
                query_result.is_err(),
                "unencrypted read of encrypted data must fail"
            );
        }
    }

    // Cleanup
    {
        let cleanup_cache = TempDir::new().unwrap();
        let cleanup_config = TurboliteConfig {
            bucket,
            prefix,
            cache_dir: cleanup_cache.path().to_path_buf(),
            compression_level: 3,
            endpoint_url: endpoint,
            region,
            encryption_key: Some(key_b),
            runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
        };
        let cleanup_vfs = TurboliteVfs::new_local(cleanup_config).unwrap();
        cleanup_vfs.destroy_s3().unwrap();
    }
}
