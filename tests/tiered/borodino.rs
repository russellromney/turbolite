//! Phase Borodino: version counter + cross-cutting correctness tests.
//!
//! Each test targets a specific bug or untested interaction discovered during
//! Kursk stress testing. Tests are written to FAIL against the current code,
//! then fixed one by one.

use turbolite::tiered::{SyncMode, TurboliteConfig, TurboliteVfs};
use tempfile::TempDir;
use super::helpers::*;

// ===== Helpers (reused from staging.rs pattern) =====

fn open_conn(vfs_name: &str, db: &str) -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        db,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        vfs_name,
    )
    .expect("open connection");
    conn.execute_batch(
        "PRAGMA page_size=65536;
         PRAGMA journal_mode=WAL;
         CREATE TABLE IF NOT EXISTS data (id INTEGER PRIMARY KEY, value TEXT);",
    )
    .unwrap();
    conn
}

fn insert_rows(conn: &rusqlite::Connection, start: i64, end: i64, prefix: &str) {
    let tx = conn.unchecked_transaction().unwrap();
    for i in start..end {
        tx.execute(
            "INSERT OR REPLACE INTO data (id, value) VALUES (?1, ?2)",
            rusqlite::params![i, format!("{}_{}", prefix, i)],
        )
        .unwrap();
    }
    tx.commit().unwrap();
}

fn ltf_config(test_name: &str, cache_dir: &std::path::Path) -> TurboliteConfig {
    let mut c = test_config(test_name, cache_dir);
    c.sync_mode = SyncMode::LocalThenFlush;
    c
}

fn cold_reader(
    bucket: &str,
    prefix: &str,
    endpoint: &Option<String>,
    db: &str,
) -> rusqlite::Connection {
    let cold_dir = TempDir::new().unwrap();
    let cold_config = TurboliteConfig {
        bucket: bucket.to_string(),
        prefix: prefix.to_string(),
        cache_dir: cold_dir.into_path(),
        endpoint_url: endpoint.clone(),
        region: Some("auto".to_string()),
        read_only: true,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let cold_vfs_name = unique_vfs_name("cold");
    let cold_vfs = TurboliteVfs::new_local(cold_config).expect("cold VFS");
    turbolite::tiered::register(&cold_vfs_name, cold_vfs).unwrap();
    rusqlite::Connection::open_with_flags_and_vfs(
        db,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &cold_vfs_name,
    )
    .unwrap()
}

// ===== a. Version counter: duplicate versions in WAL mode =====

/// Two INSERT batches + checkpoints must produce different manifest versions.
/// Currently fails: both checkpoints produce v=2 (file change counter doesn't
/// increment per checkpoint in WAL mode).
#[test]
fn borodino_version_increments_per_checkpoint() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("ver_incr", cache_dir.path());
    let (bucket, prefix, endpoint) = (config.bucket.clone(), config.prefix.clone(), config.endpoint_url.clone());

    let vfs = TurboliteVfs::new_local(config).unwrap();
    let vfs_name = unique_vfs_name("ver_incr");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = open_conn(&vfs_name, "ver_incr.db");
    insert_rows(&conn, 0, 10, "a");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let v1 = get_manifest_version(&rt, &bucket, &prefix, &endpoint);

    insert_rows(&conn, 10, 20, "b");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();

    let v2 = get_manifest_version(&rt, &bucket, &prefix, &endpoint);
    assert!(v2 > v1, "version must increase per checkpoint: v1={}, v2={}", v1, v2);
}

/// GC after checkpoint must not delete the current page group version.
/// With duplicate versions, GC deletes the "old" key which is the same as the new one.
#[test]
fn borodino_gc_does_not_delete_current_version() {
    let cache_dir = TempDir::new().unwrap();
    let mut config = test_config("gc_cur", cache_dir.path());
    config.gc_enabled = true;
    let (bucket, prefix, endpoint) = (config.bucket.clone(), config.prefix.clone(), config.endpoint_url.clone());

    let vfs = TurboliteVfs::new_local(config).unwrap();
    let vfs_name = unique_vfs_name("gc_cur");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = open_conn(&vfs_name, "gc_cur.db");
    insert_rows(&conn, 0, 50, "a");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();

    insert_rows(&conn, 50, 100, "b");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    drop(conn);

    // Cold reader must see all 100 rows. If GC deleted the current version,
    // the cold reader will fail with "file is not a database" or missing data.
    let cold = cold_reader(&bucket, &prefix, &endpoint, "gc_cur.db");
    let count: i64 = cold.query_row("SELECT COUNT(*) FROM data", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 100, "GC must not delete current page group version");
}

// ===== c. Encryption + staging log interaction =====

/// Staging log with encryption: flush produces correct S3 data, cold reader decrypts.
#[cfg(feature = "encryption")]
#[test]
fn borodino_encryption_staging_roundtrip() {
    let cache_dir = TempDir::new().unwrap();
    let mut config = ltf_config("enc_staging", cache_dir.path());
    config.encryption_key = Some([0xAB; 32]);
    let (bucket, prefix, endpoint) = (config.bucket.clone(), config.prefix.clone(), config.endpoint_url.clone());

    let vfs = TurboliteVfs::new_local(config).unwrap();
    let shared = vfs.shared_state();
    let vfs_name = unique_vfs_name("enc_staging");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = open_conn(&vfs_name, "enc_staging.db");
    insert_rows(&conn, 0, 50, "secret");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    shared.flush_to_s3().unwrap();
    drop(conn);

    // Cold reader with correct key
    let cold_dir = TempDir::new().unwrap();
    let cold_config = TurboliteConfig {
        bucket: bucket.clone(),
        prefix: prefix.clone(),
        cache_dir: cold_dir.into_path(),
        endpoint_url: endpoint.clone(),
        region: Some("auto".to_string()),
        read_only: true,
        encryption_key: Some([0xAB; 32]),
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let cold_vfs_name = unique_vfs_name("enc_staging_cold");
    let cold_vfs = TurboliteVfs::new_local(cold_config).unwrap();
    turbolite::tiered::register(&cold_vfs_name, cold_vfs).unwrap();
    let cold = rusqlite::Connection::open_with_flags_and_vfs(
        "enc_staging.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &cold_vfs_name,
    ).unwrap();
    let val: String = cold.query_row("SELECT value FROM data WHERE id = 25", [], |r| r.get(0)).unwrap();
    assert_eq!(val, "secret_25", "encrypted staging roundtrip must produce correct data");
}

/// Wrong key on recovery VFS must fail, not silently corrupt.
#[cfg(feature = "encryption")]
#[test]
fn borodino_encryption_staging_wrong_key_fails() {
    let cache_dir = TempDir::new().unwrap();
    let mut config = ltf_config("enc_wrong", cache_dir.path());
    config.encryption_key = Some([0xAB; 32]);
    let (bucket, prefix, endpoint) = (config.bucket.clone(), config.prefix.clone(), config.endpoint_url.clone());

    let vfs = TurboliteVfs::new_local(config).unwrap();
    let shared = vfs.shared_state();
    let vfs_name = unique_vfs_name("enc_wrong");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = open_conn(&vfs_name, "enc_wrong.db");
    insert_rows(&conn, 0, 10, "secret");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    shared.flush_to_s3().unwrap();
    drop(conn);

    // Cold reader with WRONG key should fail
    let cold_dir = TempDir::new().unwrap();
    let cold_config = TurboliteConfig {
        bucket,
        prefix,
        cache_dir: cold_dir.into_path(),
        endpoint_url: endpoint,
        region: Some("auto".to_string()),
        read_only: true,
        encryption_key: Some([0xCD; 32]), // wrong key
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let cold_vfs_name = unique_vfs_name("enc_wrong_cold");
    let cold_vfs = TurboliteVfs::new_local(cold_config).unwrap();
    turbolite::tiered::register(&cold_vfs_name, cold_vfs).unwrap();
    let result = rusqlite::Connection::open_with_flags_and_vfs(
        "enc_wrong.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &cold_vfs_name,
    );
    // Should fail to open or fail on first query (not silently return wrong data)
    if let Ok(cold) = result {
        let query_result: Result<String, _> = cold.query_row(
            "SELECT value FROM data WHERE id = 0", [], |r| r.get(0),
        );
        assert!(query_result.is_err(), "wrong encryption key must not silently return data");
    }
}

// ===== d. VACUUM + LocalThenFlush =====

/// VACUUM between checkpoints in LocalThenFlush mode: cold reader must see correct data.
#[test]
fn borodino_vacuum_local_then_flush() {
    let cache_dir = TempDir::new().unwrap();
    let config = ltf_config("vac_ltf", cache_dir.path());
    let (bucket, prefix, endpoint) = (config.bucket.clone(), config.prefix.clone(), config.endpoint_url.clone());

    let vfs = TurboliteVfs::new_local(config).unwrap();
    let shared = vfs.shared_state();
    let vfs_name = unique_vfs_name("vac_ltf");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = open_conn(&vfs_name, "vac_ltf.db");
    insert_rows(&conn, 0, 200, "pre");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();

    // Delete half the data then VACUUM
    conn.execute_batch("DELETE FROM data WHERE id >= 100;").unwrap();
    conn.execute_batch("VACUUM;").unwrap();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();

    shared.flush_to_s3().unwrap();
    drop(conn);

    let cold = cold_reader(&bucket, &prefix, &endpoint, "vac_ltf.db");
    let count: i64 = cold.query_row("SELECT COUNT(*) FROM data", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 100, "after VACUUM + flush, cold reader must see 100 rows");
    let max_id: i64 = cold.query_row("SELECT MAX(id) FROM data", [], |r| r.get(0)).unwrap();
    assert_eq!(max_id, 99);
}

// ===== e. Compaction between checkpoint and flush =====

/// Compact between LocalThenFlush checkpoint and flush.
/// Flush must use staging log data (pre-compaction state).
#[test]
fn borodino_compact_between_checkpoint_and_flush() {
    let cache_dir = TempDir::new().unwrap();
    let config = ltf_config("compact_btw", cache_dir.path());
    let (bucket, prefix, endpoint) = (config.bucket.clone(), config.prefix.clone(), config.endpoint_url.clone());

    let vfs = TurboliteVfs::new_local(config).unwrap();
    let shared = vfs.shared_state();
    let vfs_name = unique_vfs_name("compact_btw");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = open_conn(&vfs_name, "compact_btw.db");
    insert_rows(&conn, 0, 100, "v1");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();

    // Try to compact (may be a no-op if no dead space, that's fine)
    let _ = conn.execute_batch("SELECT turbolite_compact();");

    shared.flush_to_s3().unwrap();
    drop(conn);

    let cold = cold_reader(&bucket, &prefix, &endpoint, "compact_btw.db");
    let count: i64 = cold.query_row("SELECT COUNT(*) FROM data", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 100, "data must survive compact + flush");
}

// ===== f. Cache eviction under memory pressure with pending staging =====

/// Low cache budget with pending staging: eviction must not discard pending pages.
#[test]
fn borodino_eviction_protects_pending_staging() {
    let cache_dir = TempDir::new().unwrap();
    let mut config = ltf_config("evict_pend", cache_dir.path());
    config.max_cache_bytes = Some(256 * 1024); // 256KB, very small
    let (bucket, prefix, endpoint) = (config.bucket.clone(), config.prefix.clone(), config.endpoint_url.clone());

    let vfs = TurboliteVfs::new_local(config).unwrap();
    let shared = vfs.shared_state();
    let vfs_name = unique_vfs_name("evict_pend");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = open_conn(&vfs_name, "evict_pend.db");
    // Write enough to exceed cache budget
    insert_rows(&conn, 0, 500, "big");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();

    assert!(shared.has_pending_flush(), "should have pending staging");

    // Read different data to trigger eviction pressure
    let _count: i64 = conn.query_row("SELECT COUNT(*) FROM data WHERE id > 400", [], |r| r.get(0)).unwrap();

    // Flush must still succeed (pending pages not evicted)
    shared.flush_to_s3().unwrap();
    drop(conn);

    let cold = cold_reader(&bucket, &prefix, &endpoint, "evict_pend.db");
    let count: i64 = cold.query_row("SELECT COUNT(*) FROM data", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 500, "all rows must survive eviction + flush");
}

// ===== g. Multiple databases on same VFS =====

/// Multiple databases require separate VFS instances (separate S3 prefixes).
/// This test verifies that two VFS instances with LocalThenFlush work correctly
/// when each has its own prefix.
#[test]
fn borodino_multi_db_separate_vfs() {
    // DB 1: own VFS + prefix
    let cache_dir1 = TempDir::new().unwrap();
    let config1 = ltf_config("multi_db1", cache_dir1.path());
    let (bucket1, prefix1, endpoint1) = (config1.bucket.clone(), config1.prefix.clone(), config1.endpoint_url.clone());

    let vfs1 = TurboliteVfs::new_local(config1).unwrap();
    let shared1 = vfs1.shared_state();
    let vfs_name1 = unique_vfs_name("multi_db1");
    turbolite::tiered::register(&vfs_name1, vfs1).unwrap();

    let conn1 = open_conn(&vfs_name1, "multi_db1.db");
    insert_rows(&conn1, 0, 50, "db1");
    conn1.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    shared1.flush_to_s3().unwrap();
    drop(conn1);

    // DB 2: own VFS + prefix
    let cache_dir2 = TempDir::new().unwrap();
    let config2 = ltf_config("multi_db2", cache_dir2.path());
    let (bucket2, prefix2, endpoint2) = (config2.bucket.clone(), config2.prefix.clone(), config2.endpoint_url.clone());

    let vfs2 = TurboliteVfs::new_local(config2).unwrap();
    let shared2 = vfs2.shared_state();
    let vfs_name2 = unique_vfs_name("multi_db2");
    turbolite::tiered::register(&vfs_name2, vfs2).unwrap();

    let conn2 = open_conn(&vfs_name2, "multi_db2.db");
    insert_rows(&conn2, 0, 50, "db2");
    conn2.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    shared2.flush_to_s3().unwrap();
    drop(conn2);

    // Verify each independently
    let cold1 = cold_reader(&bucket1, &prefix1, &endpoint1, "multi_db1.db");
    let v1: String = cold1.query_row("SELECT value FROM data WHERE id = 25", [], |r| r.get(0)).unwrap();
    assert_eq!(v1, "db1_25");

    let cold2 = cold_reader(&bucket2, &prefix2, &endpoint2, "multi_db2.db");
    let v2: String = cold2.query_row("SELECT value FROM data WHERE id = 25", [], |r| r.get(0)).unwrap();
    assert_eq!(v2, "db2_25");
}

// ===== Helper: read manifest version from S3 =====

fn get_manifest_version(
    rt: &tokio::runtime::Runtime,
    bucket: &str,
    prefix: &str,
    endpoint: &Option<String>,
) -> u64 {
    let bucket = bucket.to_string();
    let prefix = prefix.to_string();
    let endpoint = endpoint.clone();
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
            .get_object()
            .bucket(&bucket)
            .key(format!("{}/manifest.msgpack", prefix))
            .send()
            .await
            .expect("manifest should exist");
        let bytes = resp.body.collect().await.unwrap().into_bytes();
        let manifest: serde_json::Value = {
            let m: turbolite::tiered::Manifest = rmp_serde::from_slice(&bytes).unwrap();
            serde_json::to_value(&m).unwrap()
        };
        manifest["version"].as_u64().unwrap()
    })
}
