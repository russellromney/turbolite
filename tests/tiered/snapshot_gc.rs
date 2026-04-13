//! Phase Recall: GC snapshot awareness tests.
//!
//! Tests that gc() respects snapshot manifests (manifest-snap-*.msgpack):
//! - Page groups referenced by snapshot manifests survive gc
//! - Page groups cleaned up after snapshot manifest deleted
//! - Corrupt snapshot manifests are skipped gracefully
//! - Multiple snapshots protect overlapping page groups

use turbolite::tiered::{SharedTurboliteVfs, TurboliteConfig, TurboliteVfs};
use tempfile::TempDir;
use super::helpers::*;

/// Write data, checkpoint, snapshot the manifest, write more data, checkpoint again.
/// gc() should NOT delete the page groups referenced by the snapshot manifest.
#[test]
fn test_gc_preserves_snapshot_page_groups() {
    let cache_dir = TempDir::new().expect("temp dir");
    let mut config = test_config("gc_snap_preserve", cache_dir.path());
    config.gc_enabled = false; // manual gc only
    let vfs_name = unique_vfs_name("gc_snap_preserve");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let vfs = SharedTurboliteVfs::new(TurboliteVfs::new(config).expect("vfs"));
    let vfs_ref = vfs.clone();
    turbolite::tiered::register_shared(&vfs_name, vfs).expect("register");

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "gc_snap_preserve.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .expect("open");

    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA page_size=65536;
         CREATE TABLE snap_test (id INTEGER PRIMARY KEY, data TEXT);",
    )
    .expect("schema");

    // Write batch 1, checkpoint (creates page group v1)
    {
        let tx = conn.unchecked_transaction().expect("tx");
        for i in 0..100 {
            tx.execute(
                "INSERT INTO snap_test VALUES (?1, ?2)",
                rusqlite::params![i, format!("snap_data_{:0>100}", i)],
            )
            .expect("insert");
        }
        tx.commit().expect("commit");
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").expect("checkpoint v1");

    let count_after_v1 = count_s3_objects(&bucket, &prefix, &endpoint);
    eprintln!("S3 objects after v1: {}", count_after_v1);

    // Snapshot: copy current manifest to manifest-snap-test123.msgpack
    let snap_key = vfs_ref.copy_manifest_to_snapshot("test123").expect("snapshot copy");
    eprintln!("Snapshot manifest key: {}", snap_key);

    // Write batch 2, checkpoint (creates page group v2, old v1 groups are now orphans
    // from the current manifest's perspective, but snapshot should protect them)
    {
        let tx = conn.unchecked_transaction().expect("tx");
        for i in 100..200 {
            tx.execute(
                "INSERT INTO snap_test VALUES (?1, ?2)",
                rusqlite::params![i, format!("snap_data_{:0>100}", i)],
            )
            .expect("insert");
        }
        tx.commit().expect("commit");
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").expect("checkpoint v2");

    let count_after_v2 = count_s3_objects(&bucket, &prefix, &endpoint);
    eprintln!("S3 objects after v2 (no GC yet): {}", count_after_v2);
    // v2 should have more objects (v1 + v2 page groups + snapshot manifest)
    assert!(count_after_v2 > count_after_v1, "v2 should accumulate: v1={} v2={}", count_after_v1, count_after_v2);

    // Run gc(). With snapshot manifest present, v1 page groups should survive.
    drop(conn);
    let gc_cache = TempDir::new().expect("gc cache");
    let gc_config = TurboliteConfig {
        bucket: bucket.clone(),
        prefix: prefix.clone(),
        cache_dir: gc_cache.path().to_path_buf(),
        endpoint_url: endpoint.clone(),
        region: Some("auto".to_string()),
        runtime_handle: Some(shared_runtime_handle()),
        ..Default::default()
    };
    let gc_vfs = TurboliteVfs::new(gc_config).expect("gc vfs");
    let deleted = gc_vfs.gc().expect("gc");
    eprintln!("GC deleted {} objects (with snapshot protecting v1)", deleted);

    let count_after_gc = count_s3_objects(&bucket, &prefix, &endpoint);
    eprintln!("S3 objects after gc with snapshot: {}", count_after_gc);

    // Snapshot protects v1 page groups: count should stay >= count_after_v1
    assert!(
        count_after_gc >= count_after_v1,
        "snapshot should protect v1 page groups from gc: after_v1={} after_gc={}",
        count_after_v1, count_after_gc,
    );

    // Verify data integrity: open at current manifest, all 200 rows visible
    let read_vfs_name = unique_vfs_name("gc_snap_preserve_read");
    turbolite::tiered::register(&read_vfs_name, gc_vfs).expect("register reader");
    let reader = rusqlite::Connection::open_with_flags_and_vfs(
        "gc_snap_preserve.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &read_vfs_name,
    )
    .expect("open reader");
    let count: i64 = reader
        .query_row("SELECT COUNT(*) FROM snap_test", [], |row| row.get(0))
        .expect("count");
    assert_eq!(count, 200, "all 200 rows should be readable after gc with snapshot");
}

/// After deleting the snapshot manifest, gc() should clean up the old page groups.
#[test]
fn test_gc_cleans_up_after_snapshot_deleted() {
    let cache_dir = TempDir::new().expect("temp dir");
    let mut config = test_config("gc_snap_delete", cache_dir.path());
    config.gc_enabled = false;
    let vfs_name = unique_vfs_name("gc_snap_delete");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let vfs = SharedTurboliteVfs::new(TurboliteVfs::new(config).expect("vfs"));
    let vfs_ref = vfs.clone();
    turbolite::tiered::register_shared(&vfs_name, vfs).expect("register");

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "gc_snap_delete.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .expect("open");

    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA page_size=65536;
         CREATE TABLE snap_del (id INTEGER PRIMARY KEY, data TEXT);",
    )
    .expect("schema");

    // Write + checkpoint v1
    {
        let tx = conn.unchecked_transaction().expect("tx");
        for i in 0..100 {
            tx.execute(
                "INSERT INTO snap_del VALUES (?1, ?2)",
                rusqlite::params![i, format!("del_data_{:0>100}", i)],
            )
            .expect("insert");
        }
        tx.commit().expect("commit");
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").expect("checkpoint v1");

    // Snapshot
    vfs_ref.copy_manifest_to_snapshot("del456").expect("snapshot");

    // Write + checkpoint v2
    {
        let tx = conn.unchecked_transaction().expect("tx");
        for i in 100..200 {
            tx.execute(
                "INSERT INTO snap_del VALUES (?1, ?2)",
                rusqlite::params![i, format!("del_data_{:0>100}", i)],
            )
            .expect("insert");
        }
        tx.commit().expect("commit");
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").expect("checkpoint v2");

    let count_before_delete = count_s3_objects(&bucket, &prefix, &endpoint);

    // Delete the snapshot manifest
    vfs_ref.delete_snapshot_manifest("del456").expect("delete snapshot");

    // Now gc should clean up v1 page groups
    drop(conn);
    let gc_cache = TempDir::new().expect("gc cache");
    let gc_config = TurboliteConfig {
        bucket: bucket.clone(),
        prefix: prefix.clone(),
        cache_dir: gc_cache.path().to_path_buf(),
        endpoint_url: endpoint.clone(),
        region: Some("auto".to_string()),
        runtime_handle: Some(shared_runtime_handle()),
        ..Default::default()
    };
    let gc_vfs = TurboliteVfs::new(gc_config).expect("gc vfs");
    let deleted = gc_vfs.gc().expect("gc");
    eprintln!("GC deleted {} objects after snapshot deleted", deleted);

    let count_after_gc = count_s3_objects(&bucket, &prefix, &endpoint);
    eprintln!("S3 objects: before_delete={} after_gc={}", count_before_delete, count_after_gc);

    // With snapshot deleted, gc should have cleaned up v1 orphans
    assert!(
        count_after_gc < count_before_delete,
        "gc should reduce object count after snapshot deleted: before={} after={}",
        count_before_delete, count_after_gc,
    );
    assert!(deleted > 0, "gc should have deleted at least 1 orphan after snapshot deleted");

    // Data integrity: current data (200 rows) still readable
    let read_vfs_name = unique_vfs_name("gc_snap_delete_read");
    turbolite::tiered::register(&read_vfs_name, gc_vfs).expect("register reader");
    let reader = rusqlite::Connection::open_with_flags_and_vfs(
        "gc_snap_delete.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &read_vfs_name,
    )
    .expect("open reader");
    let count: i64 = reader
        .query_row("SELECT COUNT(*) FROM snap_del", [], |row| row.get(0))
        .expect("count");
    assert_eq!(count, 200, "current data should be intact after gc");
}

/// Corrupt snapshot manifest: gc() should skip it gracefully and continue.
#[test]
fn test_gc_skips_corrupt_snapshot_manifest() {
    let cache_dir = TempDir::new().expect("temp dir");
    let mut config = test_config("gc_snap_corrupt", cache_dir.path());
    config.gc_enabled = false;
    let vfs_name = unique_vfs_name("gc_snap_corrupt");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let vfs = TurboliteVfs::new(config).expect("vfs");
    turbolite::tiered::register(&vfs_name, vfs).expect("register");

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "gc_snap_corrupt.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .expect("open");

    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA page_size=65536;
         CREATE TABLE snap_corrupt (id INTEGER PRIMARY KEY, data TEXT);",
    )
    .expect("schema");

    // Write + checkpoint
    {
        let tx = conn.unchecked_transaction().expect("tx");
        for i in 0..50 {
            tx.execute(
                "INSERT INTO snap_corrupt VALUES (?1, ?2)",
                rusqlite::params![i, format!("corrupt_data_{:0>50}", i)],
            )
            .expect("insert");
        }
        tx.commit().expect("commit");
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").expect("checkpoint");

    // Write a corrupt snapshot manifest directly to S3
    let rt = shared_runtime_handle();
    let corrupt_key = format!("{}/manifest-snap-corrupt789.msgpack", prefix);
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
        client
            .put_object()
            .bucket(&bucket)
            .key(&corrupt_key)
            .body(aws_sdk_s3::primitives::ByteStream::from(b"this is not valid msgpack".to_vec()))
            .send()
            .await
            .expect("put corrupt manifest");
    });

    // gc() should not crash despite the corrupt snapshot manifest
    drop(conn);
    let gc_cache = TempDir::new().expect("gc cache");
    let gc_config = TurboliteConfig {
        bucket: bucket.clone(),
        prefix: prefix.clone(),
        cache_dir: gc_cache.path().to_path_buf(),
        endpoint_url: endpoint.clone(),
        region: Some("auto".to_string()),
        runtime_handle: Some(shared_runtime_handle()),
        ..Default::default()
    };
    let gc_vfs = TurboliteVfs::new(gc_config).expect("gc vfs");
    let result = gc_vfs.gc();
    assert!(result.is_ok(), "gc should not crash on corrupt snapshot manifest: {:?}", result.err());

    // The corrupt manifest key itself should still exist (it's kept alive as a live key)
    let all_keys_after = count_s3_objects(&bucket, &prefix, &endpoint);
    assert!(all_keys_after > 0, "S3 should still have objects");

    // Current data should be intact
    let read_vfs_name = unique_vfs_name("gc_snap_corrupt_read");
    turbolite::tiered::register(&read_vfs_name, gc_vfs).expect("register reader");
    let reader = rusqlite::Connection::open_with_flags_and_vfs(
        "gc_snap_corrupt.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &read_vfs_name,
    )
    .expect("open reader");
    let count: i64 = reader
        .query_row("SELECT COUNT(*) FROM snap_corrupt", [], |row| row.get(0))
        .expect("count");
    assert_eq!(count, 50, "data should be intact after gc with corrupt snapshot");
}

/// Multiple snapshots: gc() should protect page groups from ALL snapshot manifests.
#[test]
fn test_gc_multiple_snapshots() {
    let cache_dir = TempDir::new().expect("temp dir");
    let mut config = test_config("gc_snap_multi", cache_dir.path());
    config.gc_enabled = false;
    let vfs_name = unique_vfs_name("gc_snap_multi");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let vfs = SharedTurboliteVfs::new(TurboliteVfs::new(config).expect("vfs"));
    let vfs_ref = vfs.clone();
    turbolite::tiered::register_shared(&vfs_name, vfs).expect("register");

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "gc_snap_multi.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .expect("open");

    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA page_size=65536;
         CREATE TABLE snap_multi (id INTEGER PRIMARY KEY, data TEXT);",
    )
    .expect("schema");

    // v1: write, checkpoint, snapshot "snap-a"
    {
        let tx = conn.unchecked_transaction().expect("tx");
        for i in 0..50 {
            tx.execute(
                "INSERT INTO snap_multi VALUES (?1, ?2)",
                rusqlite::params![i, format!("multi_a_{:0>100}", i)],
            )
            .expect("insert");
        }
        tx.commit().expect("commit");
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").expect("checkpoint v1");
    vfs_ref.copy_manifest_to_snapshot("snap-a").expect("snapshot a");

    // v2: write, checkpoint, snapshot "snap-b"
    {
        let tx = conn.unchecked_transaction().expect("tx");
        for i in 50..100 {
            tx.execute(
                "INSERT INTO snap_multi VALUES (?1, ?2)",
                rusqlite::params![i, format!("multi_b_{:0>100}", i)],
            )
            .expect("insert");
        }
        tx.commit().expect("commit");
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").expect("checkpoint v2");
    vfs_ref.copy_manifest_to_snapshot("snap-b").expect("snapshot b");

    // v3: write, checkpoint (current state)
    {
        let tx = conn.unchecked_transaction().expect("tx");
        for i in 100..150 {
            tx.execute(
                "INSERT INTO snap_multi VALUES (?1, ?2)",
                rusqlite::params![i, format!("multi_c_{:0>100}", i)],
            )
            .expect("insert");
        }
        tx.commit().expect("commit");
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").expect("checkpoint v3");

    let count_before_gc = count_s3_objects(&bucket, &prefix, &endpoint);
    eprintln!("S3 objects before gc (3 versions, 2 snapshots): {}", count_before_gc);

    // gc with both snapshots alive: should protect v1 and v2 page groups
    drop(conn);
    let gc_cache = TempDir::new().expect("gc cache");
    let gc_config = TurboliteConfig {
        bucket: bucket.clone(),
        prefix: prefix.clone(),
        cache_dir: gc_cache.path().to_path_buf(),
        endpoint_url: endpoint.clone(),
        region: Some("auto".to_string()),
        runtime_handle: Some(shared_runtime_handle()),
        ..Default::default()
    };
    let gc_vfs = TurboliteVfs::new(gc_config).expect("gc vfs");
    let deleted = gc_vfs.gc().expect("gc");
    eprintln!("GC with 2 snapshots deleted {} objects", deleted);

    let count_after_gc = count_s3_objects(&bucket, &prefix, &endpoint);
    // v1, v2, v3 page groups + 2 snapshot manifests + current manifest = all protected
    // Only truly orphaned objects (like manifest.json) should be deleted
    assert!(
        count_after_gc >= count_before_gc - 1, // at most manifest.json deleted
        "both snapshots should protect their page groups: before={} after={}",
        count_before_gc, count_after_gc,
    );

    // Delete snap-a, gc again: v1-specific page groups should now be cleanable
    gc_vfs.delete_snapshot_manifest("snap-a").expect("delete snap-a");
    let deleted2 = gc_vfs.gc().expect("gc after snap-a deleted");
    eprintln!("GC after snap-a deleted: deleted {} objects", deleted2);

    // snap-b still protects v2 page groups, but v1-only groups are now orphans
    // (v1 groups that aren't shared with v2 or v3 get cleaned up)

    // Delete snap-b, gc again: now all old page groups should be cleanable
    gc_vfs.delete_snapshot_manifest("snap-b").expect("delete snap-b");
    let deleted3 = gc_vfs.gc().expect("gc after snap-b deleted");
    eprintln!("GC after snap-b deleted: deleted {} objects", deleted3);

    let count_final = count_s3_objects(&bucket, &prefix, &endpoint);
    eprintln!("Final S3 object count: {}", count_final);

    // Verify current data is intact
    let read_vfs_name = unique_vfs_name("gc_snap_multi_read");
    turbolite::tiered::register(&read_vfs_name, gc_vfs).expect("register reader");
    let reader = rusqlite::Connection::open_with_flags_and_vfs(
        "gc_snap_multi.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &read_vfs_name,
    )
    .expect("open reader");
    let count: i64 = reader
        .query_row("SELECT COUNT(*) FROM snap_multi", [], |row| row.get(0))
        .expect("count");
    assert_eq!(count, 150, "all 150 rows should be readable after all gc passes");
}

/// Phase Recall Step 2: fork from a snapshot manifest into a new prefix.
/// Write data, snapshot, write more data, fork to new prefix, verify the fork
/// sees the old data (from snapshot) but NOT the new data (written after snapshot).
#[test]
fn test_fork_from_snapshot_manifest() {
    let cache_dir = TempDir::new().expect("temp dir");
    let mut config = test_config("fork_src", cache_dir.path());
    config.gc_enabled = false;
    let vfs_name = unique_vfs_name("fork_src");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let vfs = SharedTurboliteVfs::new(TurboliteVfs::new(config).expect("vfs"));
    let vfs_ref = vfs.clone();
    turbolite::tiered::register_shared(&vfs_name, vfs).expect("register");

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "fork_src.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .expect("open");

    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA page_size=65536;
         CREATE TABLE fork_test (id INTEGER PRIMARY KEY, data TEXT);",
    )
    .expect("schema");

    // Write batch 1 (ids 0-49), checkpoint
    {
        let tx = conn.unchecked_transaction().expect("tx");
        for i in 0..50 {
            tx.execute(
                "INSERT INTO fork_test VALUES (?1, ?2)",
                rusqlite::params![i, format!("before_snap_{}", i)],
            )
            .expect("insert");
        }
        tx.commit().expect("commit");
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").expect("checkpoint v1");

    // Snapshot
    vfs_ref.copy_manifest_to_snapshot("fork-snap").expect("snapshot");
    let snap_manifest = vfs_ref.get_snapshot_manifest("fork-snap")
        .expect("get snapshot")
        .expect("should exist");
    eprintln!("Snapshot manifest v{} with {} page groups", snap_manifest.version, snap_manifest.page_group_keys.len());

    // Write batch 2 (ids 50-99), checkpoint -- this is AFTER the snapshot
    {
        let tx = conn.unchecked_transaction().expect("tx");
        for i in 50..100 {
            tx.execute(
                "INSERT INTO fork_test VALUES (?1, ?2)",
                rusqlite::params![i, format!("after_snap_{}", i)],
            )
            .expect("insert");
        }
        tx.commit().expect("commit");
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").expect("checkpoint v2");

    // Verify source has all 100 rows
    let src_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM fork_test", [], |row| row.get(0))
        .expect("count");
    assert_eq!(src_count, 100, "source should have 100 rows");

    // Fork: create new VFS at a new prefix, seed with snapshot manifest
    let fork_cache = TempDir::new().expect("fork cache");
    let fork_prefix = format!("{}_fork", prefix);
    let fork_config = TurboliteConfig {
        bucket: bucket.clone(),
        prefix: fork_prefix.clone(),
        cache_dir: fork_cache.path().to_path_buf(),
        endpoint_url: endpoint.clone(),
        region: Some("auto".to_string()),
        runtime_handle: Some(shared_runtime_handle()),
        gc_enabled: false,
        ..Default::default()
    };
    let fork_vfs = TurboliteVfs::new(fork_config).expect("fork vfs");

    // Seed the fork with the snapshot manifest
    fork_vfs.seed_manifest(&snap_manifest).expect("seed manifest");

    // Open a connection through the forked VFS
    let fork_vfs_name = unique_vfs_name("fork_dst");
    turbolite::tiered::register(&fork_vfs_name, fork_vfs).expect("register fork");
    let fork_conn = rusqlite::Connection::open_with_flags_and_vfs(
        "fork_dst.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &fork_vfs_name,
    )
    .expect("open fork");

    // Fork should see only the pre-snapshot data (50 rows, not 100)
    let fork_count: i64 = fork_conn
        .query_row("SELECT COUNT(*) FROM fork_test", [], |row| row.get(0))
        .expect("count");
    assert_eq!(fork_count, 50, "fork should see only 50 rows (pre-snapshot), got {}", fork_count);

    // Verify the data content is from before the snapshot
    let fork_data: String = fork_conn
        .query_row("SELECT data FROM fork_test WHERE id = 0", [], |row| row.get(0))
        .expect("query");
    assert_eq!(fork_data, "before_snap_0", "fork data should be pre-snapshot");

    // Verify post-snapshot data is NOT visible
    let missing: Result<i64, _> = fork_conn
        .query_row("SELECT id FROM fork_test WHERE id = 50", [], |row| row.get(0));
    assert!(missing.is_err(), "post-snapshot row 50 should NOT be visible in fork");
}

/// Fork from snapshot, write to the fork, verify the original is untouched (COW).
#[test]
fn test_fork_cow_isolation() {
    let cache_dir = TempDir::new().expect("temp dir");
    let mut config = test_config("cow_src", cache_dir.path());
    config.gc_enabled = false;
    let vfs_name = unique_vfs_name("cow_src");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let vfs = SharedTurboliteVfs::new(TurboliteVfs::new(config).expect("vfs"));
    let vfs_ref = vfs.clone();
    turbolite::tiered::register_shared(&vfs_name, vfs).expect("register");

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "cow_src.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .expect("open");

    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA page_size=65536;
         CREATE TABLE cow_test (id INTEGER PRIMARY KEY, data TEXT);",
    )
    .expect("schema");

    // Write source data
    {
        let tx = conn.unchecked_transaction().expect("tx");
        for i in 0..30 {
            tx.execute(
                "INSERT INTO cow_test VALUES (?1, ?2)",
                rusqlite::params![i, format!("original_{}", i)],
            )
            .expect("insert");
        }
        tx.commit().expect("commit");
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").expect("checkpoint");

    // Snapshot + read it
    vfs_ref.copy_manifest_to_snapshot("cow-snap").expect("snapshot");
    let snap_manifest = vfs_ref.get_snapshot_manifest("cow-snap")
        .expect("get")
        .expect("exists");

    // Create fork VFS + seed
    let fork_cache = TempDir::new().expect("fork cache");
    let fork_config = TurboliteConfig {
        bucket: bucket.clone(),
        prefix: format!("{}_cow_fork", prefix),
        cache_dir: fork_cache.path().to_path_buf(),
        endpoint_url: endpoint.clone(),
        region: Some("auto".to_string()),
        runtime_handle: Some(shared_runtime_handle()),
        gc_enabled: false,
        ..Default::default()
    };
    let fork_vfs = SharedTurboliteVfs::new(TurboliteVfs::new(fork_config).expect("fork vfs"));
    let fork_vfs_ref = fork_vfs.clone();
    fork_vfs_ref.seed_manifest(&snap_manifest).expect("seed");

    let fork_vfs_name = unique_vfs_name("cow_fork");
    turbolite::tiered::register_shared(&fork_vfs_name, fork_vfs).expect("register fork");

    // Open fork as read-write and insert new data
    let fork_conn = rusqlite::Connection::open_with_flags_and_vfs(
        "cow_fork.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &fork_vfs_name,
    )
    .expect("open fork rw");

    fork_conn.execute_batch("PRAGMA journal_mode=WAL;").expect("wal");

    // Write to the fork
    {
        let tx = fork_conn.unchecked_transaction().expect("tx");
        for i in 30..60 {
            tx.execute(
                "INSERT INTO cow_test VALUES (?1, ?2)",
                rusqlite::params![i, format!("fork_data_{}", i)],
            )
            .expect("insert fork");
        }
        tx.commit().expect("commit fork");
    }
    fork_conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").expect("checkpoint fork");

    // Fork should have 60 rows
    let fork_count: i64 = fork_conn
        .query_row("SELECT COUNT(*) FROM cow_test", [], |row| row.get(0))
        .expect("count fork");
    assert_eq!(fork_count, 60, "fork should have 60 rows after write");

    // Original should still have 30 rows (untouched by fork write)
    let src_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM cow_test", [], |row| row.get(0))
        .expect("count src");
    assert_eq!(src_count, 30, "original should still have 30 rows (COW isolation)");

    // Verify no fork data leaked into original
    let leak_check: Result<i64, _> = conn
        .query_row("SELECT id FROM cow_test WHERE id = 30", [], |row| row.get(0));
    assert!(leak_check.is_err(), "fork data should NOT appear in original");
}

/// Verify that copy_manifest_to_snapshot + get_snapshot_manifest round-trips correctly.
#[test]
fn test_snapshot_manifest_roundtrip() {
    let cache_dir = TempDir::new().expect("temp dir");
    let mut config = test_config("snap_roundtrip", cache_dir.path());
    config.gc_enabled = false;
    let vfs_name = unique_vfs_name("snap_roundtrip");

    let vfs = SharedTurboliteVfs::new(TurboliteVfs::new(config).expect("vfs"));
    let vfs_ref = vfs.clone();
    turbolite::tiered::register_shared(&vfs_name, vfs).expect("register");

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "snap_roundtrip.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .expect("open");

    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA page_size=65536;
         CREATE TABLE rt_test (id INTEGER PRIMARY KEY, data TEXT);",
    )
    .expect("schema");

    // Write data + checkpoint
    {
        let tx = conn.unchecked_transaction().expect("tx");
        for i in 0..20 {
            tx.execute(
                "INSERT INTO rt_test VALUES (?1, ?2)",
                rusqlite::params![i, format!("rt_data_{}", i)],
            )
            .expect("insert");
        }
        tx.commit().expect("commit");
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").expect("checkpoint");

    // Copy manifest to snapshot
    let snap_key = vfs_ref.copy_manifest_to_snapshot("rt-test").expect("snapshot");
    assert!(snap_key.contains("manifest-snap-rt-test.msgpack"), "snap key should contain expected name: {}", snap_key);

    // Read it back
    let snap_manifest = vfs_ref.get_snapshot_manifest("rt-test").expect("get snapshot").expect("should exist");
    assert!(snap_manifest.version >= 1, "snapshot manifest version should be >= 1");
    assert!(snap_manifest.page_count > 0, "snapshot manifest should have pages");
    assert!(!snap_manifest.page_group_keys.is_empty(), "snapshot manifest should have page groups");

    // Non-existent snapshot returns None
    let missing = vfs_ref.get_snapshot_manifest("nonexistent").expect("get missing");
    assert!(missing.is_none(), "nonexistent snapshot should return None");

    // Delete and verify gone
    vfs_ref.delete_snapshot_manifest("rt-test").expect("delete snapshot");
    let after_delete = vfs_ref.get_snapshot_manifest("rt-test").expect("get after delete");
    assert!(after_delete.is_none(), "deleted snapshot should return None");
}
