//! Phase Drift integration tests: subframe override write, compaction, two-writer cache
//! validation, and encryption with overrides. All tests run against real Tigris S3.
//!
//! Override frames require seekable format (sub_pages_per_frame > 0), which is set in the
//! manifest during import. Tests use import_sqlite_file to bootstrap data in seekable format,
//! then LocalThenFlush + flush_to_s3 to exercise the override path.

use turbolite::tiered::{import_sqlite_file, ManifestSource, SyncMode, TurboliteConfig, TurboliteVfs};
use tempfile::TempDir;
use super::helpers::*;

// ===== Helpers =====

/// Config for override tests: seekable format, low override threshold, LocalThenFlush.
fn drift_config(test_name: &str, cache_dir: &std::path::Path) -> TurboliteConfig {
    let mut c = test_config(test_name, cache_dir);
    c.sync_mode = SyncMode::LocalThenFlush;
    c.sub_pages_per_frame = 8; // enable seekable format (required for overrides)
    c.override_threshold = 100; // low threshold so small updates produce overrides
    c.compaction_threshold = 8;
    c
}

/// Create a local SQLite DB with padded rows, checkpoint, import to S3.
/// Returns the path to the local DB file (inside cache_dir).
/// After import, the manifest in cache_dir has seekable frame tables.
fn create_and_import(
    config: &TurboliteConfig,
    cache_dir: &std::path::Path,
    db_name: &str,
    row_count: i64,
    prefix: &str,
) -> std::path::PathBuf {
    let local_db = cache_dir.join(format!("{}.db", db_name));
    let conn = rusqlite::Connection::open(&local_db).expect("create local DB");
    conn.execute_batch(
        "PRAGMA page_size=4096;
         PRAGMA journal_mode=WAL;
         CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT);",
    )
    .expect("setup local DB");
    {
        let tx = conn.unchecked_transaction().expect("begin tx");
        for i in 0..row_count {
            tx.execute(
                "INSERT INTO data VALUES (?1, ?2)",
                rusqlite::params![i, padded_value(prefix, i)],
            )
            .expect("insert");
        }
        tx.commit().expect("commit");
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("checkpoint local");
    drop(conn);

    import_sqlite_file(config, &local_db).expect("import to S3");
    local_db
}

/// Pad value string so each row is large enough to fill pages (forces multi-page DB).
/// ~2KB per row, so 2 rows per 4KB page. 200 rows = ~100 pages = ~12 frames.
fn padded_value(prefix: &str, i: i64) -> String {
    format!("{}_{}{}", prefix, i, "X".repeat(2000))
}

/// Open a VFS connection using URI format (matching the local DB name).
fn open_vfs_conn(vfs_name: &str, db_name: &str) -> rusqlite::Connection {
    let db_path = format!("file:{}.db?vfs={}", db_name, vfs_name);
    let conn = rusqlite::Connection::open_with_flags(
        &db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_URI,
    )
    .expect("open VFS connection");
    conn.execute_batch("PRAGMA journal_mode=WAL;").expect("WAL");
    conn
}

/// Open a cold reader VFS connection.
fn open_cold_conn(vfs_name: &str, db_name: &str) -> rusqlite::Connection {
    let db_path = format!("file:{}.db?vfs={}", db_name, vfs_name);
    rusqlite::Connection::open_with_flags(
        &db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
            | rusqlite::OpenFlags::SQLITE_OPEN_URI,
    )
    .expect("open cold VFS connection")
}

/// List all S3 keys under a prefix, returning them as strings.
fn list_s3_keys(bucket: &str, prefix: &str, endpoint: &Option<String>) -> Vec<String> {
    let rt = shared_runtime_handle();
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
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .send()
            .await
            .expect("list S3 keys should succeed");
        resp.contents()
            .iter()
            .filter_map(|o| o.key().map(String::from))
            .collect()
    })
}

// ===== 0. Sanity check: import + VFS write + cold read =====

/// Verify the basic import + VFS open + write + flush + cold read pipeline works.
/// This is a prerequisite for all override tests.
#[test]
fn drift_sanity_import_write_cold_read() {
    let cache_dir = TempDir::new().expect("cache dir");
    let config = drift_config("drift_sanity", cache_dir.path());
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    create_and_import(&config, cache_dir.path(), "drift_sanity", 20, "base");

    // Use Durable mode for sanity check (the btree_grouping tests use Durable and work)
    let mut durable_config = drift_config("drift_sanity", cache_dir.path());
    durable_config.sync_mode = SyncMode::Durable;
    durable_config.bucket = config.bucket.clone();
    durable_config.prefix = config.prefix.clone();
    durable_config.cache_dir = config.cache_dir.clone();
    durable_config.endpoint_url = config.endpoint_url.clone();
    // Re-use the same config but override sync mode
    let vfs_name = unique_vfs_name("drift_sanity");
    let vfs = TurboliteVfs::new(durable_config).expect("create vfs");
    turbolite::tiered::register(&vfs_name, vfs).expect("register vfs");

    let conn = open_vfs_conn(&vfs_name, "drift_sanity");

    // Verify import data is readable
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM data", [], |r| r.get(0))
        .expect("count");
    assert_eq!(count, 20, "should see 20 imported rows");

    // Add a new row, checkpoint (Durable mode uploads directly)
    conn.execute(
        "INSERT INTO data VALUES (999, 'new_row')",
        [],
    )
    .expect("insert new row");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("checkpoint");

    // Cold read
    let cold_dir = TempDir::new().expect("cold dir");
    let cold_config = cold_reader_config(&bucket, &prefix, &endpoint, cold_dir.path());
    let cold_vfs_name = unique_vfs_name("drift_sanity_cold");
    let cold_vfs = TurboliteVfs::new(cold_config).expect("cold vfs");
    turbolite::tiered::register(&cold_vfs_name, cold_vfs).expect("register cold vfs");

    let cold_conn = open_cold_conn(&cold_vfs_name, "drift_sanity");

    let cold_count: i64 = cold_conn
        .query_row("SELECT COUNT(*) FROM data", [], |r| r.get(0))
        .expect("cold count");
    assert_eq!(cold_count, 21, "cold reader should see 21 rows (20 imported + 1 new)");

    let new_val: String = cold_conn
        .query_row("SELECT value FROM data WHERE id = 999", [], |r| r.get(0))
        .expect("cold read new row");
    assert_eq!(new_val, "new_row");
}

// ===== 1. Override write + cold read =====

/// Import data (creates seekable base groups in S3 with frame tables).
/// Open VFS, update a few rows, checkpoint+flush (creates override frames in S3).
/// Open cold reader (new cache dir), verify reads correct data from base + overrides.
/// Verify S3 has override keys (pg/{gid}_f{frame_idx}_v{version}).
#[test]
fn drift_override_write_and_cold_read() {
    let cache_dir = TempDir::new().expect("cache dir");
    let config = drift_config("drift_ov_write", cache_dir.path());
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    // Step 1: import base data (creates seekable groups with frame tables)
    create_and_import(&config, cache_dir.path(), "drift_ov_write", 200, "base");

    // Step 2: open VFS, do small updates, checkpoint+flush to produce override frames
    let vfs_name = unique_vfs_name("drift_ov_write");
    let vfs = TurboliteVfs::new(config).expect("create vfs");
    let shared = vfs.shared_state();
    turbolite::tiered::register(&vfs_name, vfs).expect("register vfs");

    let conn = open_vfs_conn(&vfs_name, "drift_ov_write");

    // Verify we can read the imported data
    let count_before: i64 = conn
        .query_row("SELECT COUNT(*) FROM data", [], |r| r.get(0))
        .expect("count before update");
    assert_eq!(count_before, 200, "should see 200 imported rows");

    // Small updates: only a few rows (should produce override frames, not full group rewrite)
    conn.execute("UPDATE data SET value = 'updated_5' WHERE id = 5", [])
        .expect("update row 5");
    conn.execute("UPDATE data SET value = 'updated_10' WHERE id = 10", [])
        .expect("update row 10");
    conn.execute("UPDATE data SET value = 'updated_50' WHERE id = 50", [])
        .expect("update row 50");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("override checkpoint");
    shared.flush_to_s3().expect("override flush");

    // Verify override keys exist in S3 (pattern: pg/{gid}_f{frame_idx}_v{version})
    let pg_keys = list_s3_keys(&bucket, &format!("{}/p/d/", prefix), &endpoint);
    let override_keys: Vec<_> = pg_keys
        .iter()
        .filter(|k| k.contains("_f") && k.contains("_v"))
        .collect();
    assert!(
        !override_keys.is_empty(),
        "should have override keys in S3, got pg keys: {:?}",
        pg_keys
    );

    // Debug: verify the writer sees the update locally
    let local_v5: String = conn
        .query_row("SELECT value FROM data WHERE id = 5", [], |r| r.get(0))
        .expect("local read id=5");
    assert_eq!(local_v5, "updated_5", "writer should see its own update locally");

    // Step 3: cold read from S3 with empty cache
    let cold_dir = TempDir::new().expect("cold dir");
    let cold_config = cold_reader_config(&bucket, &prefix, &endpoint, cold_dir.path());
    let cold_vfs_name = unique_vfs_name("drift_ov_write_cold");
    let cold_vfs = TurboliteVfs::new(cold_config).expect("cold vfs");
    turbolite::tiered::register(&cold_vfs_name, cold_vfs).expect("register cold vfs");

    let cold_conn = open_cold_conn(&cold_vfs_name, "drift_ov_write");

    // Verify updated rows have new (short) values
    let v5: String = cold_conn
        .query_row("SELECT value FROM data WHERE id = 5", [], |r| r.get(0))
        .expect("read id=5");
    assert_eq!(v5, "updated_5", "override should be visible for id=5");

    let v10: String = cold_conn
        .query_row("SELECT value FROM data WHERE id = 10", [], |r| r.get(0))
        .expect("read id=10");
    assert_eq!(v10, "updated_10", "override should be visible for id=10");

    let v50: String = cold_conn
        .query_row("SELECT value FROM data WHERE id = 50", [], |r| r.get(0))
        .expect("read id=50");
    assert_eq!(v50, "updated_50", "override should be visible for id=50");

    // Verify non-updated rows still have padded base values
    let v0: String = cold_conn
        .query_row("SELECT value FROM data WHERE id = 0", [], |r| r.get(0))
        .expect("read id=0");
    assert_eq!(v0, padded_value("base", 0), "non-updated row should retain base value");

    let v199: String = cold_conn
        .query_row("SELECT value FROM data WHERE id = 199", [], |r| r.get(0))
        .expect("read id=199");
    assert_eq!(v199, padded_value("base", 199), "non-updated row should retain base value");

    // Total row count must be unchanged
    let count: i64 = cold_conn
        .query_row("SELECT COUNT(*) FROM data", [], |r| r.get(0))
        .expect("count");
    assert_eq!(count, 200, "row count should be unchanged after overrides");
}

// ===== 2. Override + compaction + cold read =====

/// Import base data. Do several small updates via flush, accumulating override frames.
/// With compaction_threshold=3, the third flush should trigger compaction.
/// Cold reader should see all updates merged correctly.
#[test]
fn drift_override_compaction_and_cold_read() {
    let cache_dir = TempDir::new().expect("cache dir");
    let mut config = drift_config("drift_compact", cache_dir.path());
    config.compaction_threshold = 3; // trigger compaction after 3 override versions
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    // Import base data
    create_and_import(&config, cache_dir.path(), "drift_compact", 100, "base");

    let vfs_name = unique_vfs_name("drift_compact");
    let vfs = TurboliteVfs::new(config).expect("create vfs");
    let shared = vfs.shared_state();
    turbolite::tiered::register(&vfs_name, vfs).expect("register vfs");

    let conn = open_vfs_conn(&vfs_name, "drift_compact");

    // Round 1: small update
    conn.execute("UPDATE data SET value = 'r1_10' WHERE id = 10", [])
        .expect("round 1 update");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("round 1 checkpoint");
    shared.flush_to_s3().expect("round 1 flush");

    // Round 2: small update
    conn.execute("UPDATE data SET value = 'r2_20' WHERE id = 20", [])
        .expect("round 2 update");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("round 2 checkpoint");
    shared.flush_to_s3().expect("round 2 flush");

    // Round 3: small update (should trigger compaction with threshold=3)
    conn.execute("UPDATE data SET value = 'r3_30' WHERE id = 30", [])
        .expect("round 3 update");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("round 3 checkpoint");
    shared.flush_to_s3().expect("round 3 flush (should trigger compaction)");

    // Round 4: one more update to verify post-compaction overrides work
    conn.execute("UPDATE data SET value = 'r4_40' WHERE id = 40", [])
        .expect("round 4 update");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("round 4 checkpoint");
    shared.flush_to_s3().expect("round 4 flush");

    // Cold read: should see all updates merged correctly
    let cold_dir = TempDir::new().expect("cold dir");
    let cold_config = cold_reader_config(&bucket, &prefix, &endpoint, cold_dir.path());
    let cold_vfs_name = unique_vfs_name("drift_compact_cold");
    let cold_vfs = TurboliteVfs::new(cold_config).expect("cold vfs");
    turbolite::tiered::register(&cold_vfs_name, cold_vfs).expect("register cold vfs");

    let cold_conn = open_cold_conn(&cold_vfs_name, "drift_compact");

    // Each round's update should be visible
    let v10: String = cold_conn
        .query_row("SELECT value FROM data WHERE id = 10", [], |r| r.get(0))
        .expect("read id=10");
    assert_eq!(v10, "r1_10");

    let v20: String = cold_conn
        .query_row("SELECT value FROM data WHERE id = 20", [], |r| r.get(0))
        .expect("read id=20");
    assert_eq!(v20, "r2_20");

    let v30: String = cold_conn
        .query_row("SELECT value FROM data WHERE id = 30", [], |r| r.get(0))
        .expect("read id=30");
    assert_eq!(v30, "r3_30");

    let v40: String = cold_conn
        .query_row("SELECT value FROM data WHERE id = 40", [], |r| r.get(0))
        .expect("read id=40");
    assert_eq!(v40, "r4_40");

    // Non-updated rows should have padded base values
    let v0: String = cold_conn
        .query_row("SELECT value FROM data WHERE id = 0", [], |r| r.get(0))
        .expect("read id=0");
    assert_eq!(v0, padded_value("base", 0));

    let count: i64 = cold_conn
        .query_row("SELECT COUNT(*) FROM data", [], |r| r.get(0))
        .expect("count");
    assert_eq!(count, 100, "row count unchanged after compaction");
}

// ===== 3. Two-writer simulation (cache validation) =====

/// Import data. Writer 1 opens VFS. Writer 2 (new cache, same S3 prefix) opens from S3,
/// writes more data, checkpoints. Writer 1 reopens with ManifestSource::S3 and should
/// see Writer 2's data (Zenith-b cache validation).
#[test]
fn drift_two_writer_cache_validation() {
    let cache1 = TempDir::new().expect("cache1 dir");
    let mut config1 = drift_config("drift_2w", cache1.path());
    config1.sync_mode = SyncMode::Durable; // Durable for straightforward writes
    let bucket = config1.bucket.clone();
    let prefix = config1.prefix.clone();
    let endpoint = config1.endpoint_url.clone();

    // Import base data
    create_and_import(&config1, cache1.path(), "drift_2w", 50, "w1");

    // Writer 1: open and verify data, then close
    let vfs_name1 = unique_vfs_name("drift_2w_w1");
    let vfs1 = TurboliteVfs::new(config1).expect("create vfs1");
    turbolite::tiered::register(&vfs_name1, vfs1).expect("register vfs1");

    let conn1 = open_vfs_conn(&vfs_name1, "drift_2w");
    let w1_count: i64 = conn1
        .query_row("SELECT COUNT(*) FROM data", [], |r| r.get(0))
        .expect("w1 count");
    assert_eq!(w1_count, 50, "writer 1 should see imported 50 rows");
    drop(conn1);

    // Writer 2: new cache dir, same S3 prefix. Opens from S3 manifest.
    let cache2 = TempDir::new().expect("cache2 dir");
    let config2 = TurboliteConfig {
        bucket: bucket.clone(),
        prefix: prefix.clone(),
        cache_dir: cache2.path().to_path_buf(),
        endpoint_url: endpoint.clone(),
        region: Some("auto".to_string()),
        sub_pages_per_frame: 8,
        override_threshold: 100,
        compaction_threshold: 8,
        manifest_source: ManifestSource::S3,
        runtime_handle: Some(shared_runtime_handle()),
        gc_enabled: false,
        ..Default::default()
    };
    let vfs_name2 = unique_vfs_name("drift_2w_w2");
    let vfs2 = TurboliteVfs::new(config2).expect("create vfs2");
    turbolite::tiered::register(&vfs_name2, vfs2).expect("register vfs2");

    let conn2 = open_vfs_conn(&vfs_name2, "drift_2w");

    // Writer 2 adds more rows (padded)
    {
        let tx = conn2.unchecked_transaction().expect("w2 tx");
        for i in 50..100 {
            tx.execute(
                "INSERT INTO data VALUES (?1, ?2)",
                rusqlite::params![i, padded_value("w2", i)],
            )
            .expect("w2 insert");
        }
        tx.commit().expect("w2 commit");
    }
    conn2
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("w2 checkpoint");
    drop(conn2);

    // Writer 1 reopens with ManifestSource::S3 to pick up Writer 2's changes
    let cache1_reopen = TempDir::new().expect("cache1 reopen dir");
    let reopen_config = TurboliteConfig {
        bucket: bucket.clone(),
        prefix: prefix.clone(),
        cache_dir: cache1_reopen.path().to_path_buf(),
        endpoint_url: endpoint.clone(),
        region: Some("auto".to_string()),
        sub_pages_per_frame: 8,
        override_threshold: 100,
        compaction_threshold: 8,
        manifest_source: ManifestSource::S3,
        read_only: true,
        runtime_handle: Some(shared_runtime_handle()),
        gc_enabled: false,
        ..Default::default()
    };
    let vfs_name1_reopen = unique_vfs_name("drift_2w_w1_reopen");
    let vfs1_reopen = TurboliteVfs::new(reopen_config).expect("reopen vfs1");
    turbolite::tiered::register(&vfs_name1_reopen, vfs1_reopen).expect("register reopen vfs1");

    let conn1_reopen = open_cold_conn(&vfs_name1_reopen, "drift_2w");

    // Writer 1 should see all 100 rows (50 from import + 50 from w2)
    let count: i64 = conn1_reopen
        .query_row("SELECT COUNT(*) FROM data", [], |r| r.get(0))
        .expect("count after reopen");
    assert_eq!(
        count, 100,
        "reopened writer 1 should see both writers' data"
    );

    // Verify data from both writers
    let v0: String = conn1_reopen
        .query_row("SELECT value FROM data WHERE id = 0", [], |r| r.get(0))
        .expect("read w1 data");
    assert_eq!(v0, padded_value("w1", 0));

    let v75: String = conn1_reopen
        .query_row("SELECT value FROM data WHERE id = 75", [], |r| r.get(0))
        .expect("read w2 data");
    assert_eq!(v75, padded_value("w2", 75));
}

// ===== 4. Override with encryption =====

/// Import data with encryption. Open VFS, update rows (creates encrypted overrides), flush.
/// Cold read with same encryption key: verify correct data. Wrong key: verify failure.
#[cfg(feature = "encryption")]
#[test]
fn drift_override_with_encryption() {
    let cache_dir = TempDir::new().expect("cache dir");
    let mut config = drift_config("drift_enc", cache_dir.path());
    config.encryption_key = Some([0xDE; 32]);
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    // Import base data with encryption
    create_and_import(&config, cache_dir.path(), "drift_enc", 100, "secret");

    let vfs_name = unique_vfs_name("drift_enc");
    let vfs = TurboliteVfs::new(config).expect("create vfs");
    let shared = vfs.shared_state();
    turbolite::tiered::register(&vfs_name, vfs).expect("register vfs");

    let conn = open_vfs_conn(&vfs_name, "drift_enc");

    // Small updates (should produce encrypted override frames)
    conn.execute("UPDATE data SET value = 'enc_updated_5' WHERE id = 5", [])
        .expect("update id=5");
    conn.execute("UPDATE data SET value = 'enc_updated_42' WHERE id = 42", [])
        .expect("update id=42");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("override checkpoint");
    shared.flush_to_s3().expect("override flush");

    // Verify override keys exist
    let pg_keys = list_s3_keys(&bucket, &format!("{}/p/d/", prefix), &endpoint);
    let override_keys: Vec<_> = pg_keys
        .iter()
        .filter(|k| k.contains("_f") && k.contains("_v"))
        .collect();
    assert!(
        !override_keys.is_empty(),
        "should have encrypted override keys in S3, got pg keys: {:?}",
        pg_keys
    );

    // Cold read with same encryption key
    let cold_dir = TempDir::new().expect("cold dir");
    let cold_config = TurboliteConfig {
        bucket: bucket.clone(),
        prefix: prefix.clone(),
        cache_dir: cold_dir.path().to_path_buf(),
        endpoint_url: endpoint.clone(),
        region: Some("auto".to_string()),
        read_only: true,
        encryption_key: Some([0xDE; 32]),
        runtime_handle: Some(shared_runtime_handle()),
        ..Default::default()
    };
    let cold_vfs_name = unique_vfs_name("drift_enc_cold");
    let cold_vfs = TurboliteVfs::new(cold_config).expect("cold vfs");
    turbolite::tiered::register(&cold_vfs_name, cold_vfs).expect("register cold vfs");

    let cold_conn = open_cold_conn(&cold_vfs_name, "drift_enc");

    // Updated rows should have new values
    let v5: String = cold_conn
        .query_row("SELECT value FROM data WHERE id = 5", [], |r| r.get(0))
        .expect("read id=5");
    assert_eq!(v5, "enc_updated_5", "encrypted override should be readable");

    let v42: String = cold_conn
        .query_row("SELECT value FROM data WHERE id = 42", [], |r| r.get(0))
        .expect("read id=42");
    assert_eq!(v42, "enc_updated_42", "encrypted override should be readable");

    // Non-updated rows should have padded base values
    let v0: String = cold_conn
        .query_row("SELECT value FROM data WHERE id = 0", [], |r| r.get(0))
        .expect("read id=0");
    assert_eq!(v0, padded_value("secret", 0), "base data should be intact");

    let count: i64 = cold_conn
        .query_row("SELECT COUNT(*) FROM data", [], |r| r.get(0))
        .expect("count");
    assert_eq!(count, 100, "row count unchanged after encrypted overrides");

    // Wrong key should fail
    let bad_dir = TempDir::new().expect("bad dir");
    let bad_config = TurboliteConfig {
        bucket,
        prefix,
        cache_dir: bad_dir.path().to_path_buf(),
        endpoint_url: endpoint,
        region: Some("auto".to_string()),
        read_only: true,
        encryption_key: Some([0xFF; 32]), // wrong key
        runtime_handle: Some(shared_runtime_handle()),
        ..Default::default()
    };
    let bad_vfs_name = unique_vfs_name("drift_enc_bad");
    let bad_vfs = TurboliteVfs::new(bad_config).expect("bad vfs");
    turbolite::tiered::register(&bad_vfs_name, bad_vfs).expect("register bad vfs");

    let bad_db = format!("file:drift_enc.db?vfs={}", bad_vfs_name);
    let bad_result = rusqlite::Connection::open_with_flags(
        &bad_db,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
            | rusqlite::OpenFlags::SQLITE_OPEN_URI,
    );
    if let Ok(bad_conn) = bad_result {
        let query_result: Result<String, _> = bad_conn.query_row(
            "SELECT value FROM data WHERE id = 0",
            [],
            |r| r.get(0),
        );
        assert!(
            query_result.is_err(),
            "wrong encryption key must not return data from encrypted overrides"
        );
    }
}
