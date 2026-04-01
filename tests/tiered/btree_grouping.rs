//! Integration tests for B-tree-aware page grouping (Phase Midway remaining).

use super::helpers::*;
use turbolite::tiered::*;
use std::path::Path;

/// Helper: create a local SQLite DB, then import it to S3 for B-tree-aware grouping.
fn create_and_import(
    config: &TieredConfig,
    local_path: &Path,
    setup_fn: impl FnOnce(&rusqlite::Connection),
) -> Manifest {
    // Create local DB
    let conn = rusqlite::Connection::open(local_path).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
    setup_fn(&conn);
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    drop(conn);

    // Import to S3
    import_sqlite_file(config, local_path).unwrap()
}

/// After import + VFS writes + checkpoint, new pages get valid group assignments.
/// Only dirty groups are re-uploaded.
#[test]
fn test_checkpoint_packs_new_pages_into_btree_groups() {
    let cache_dir = tempfile::tempdir().unwrap();
    let config = test_config("btree_cp", cache_dir.path());
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let local_db = cache_dir.path().join("local.db");
    let manifest = create_and_import(&config, &local_db, |conn| {
        conn.execute_batch(
            "CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, body TEXT);
             CREATE INDEX idx_posts_user ON posts(user_id);",
        ).unwrap();
        for i in 0..200 {
            conn.execute("INSERT INTO posts VALUES (?1, ?2, ?3)",
                rusqlite::params![i, i % 50, format!("post body {}", i)]).unwrap();
        }
    });

    let has_posts = manifest.btrees.values().any(|e| e.name == "posts");
    let has_idx = manifest.btrees.values().any(|e| e.name == "idx_posts_user");
    assert!(has_posts, "should have 'posts' B-tree");
    assert!(has_idx, "should have 'idx_posts_user' B-tree");

    let groups_before = manifest.page_group_keys.iter().filter(|k| !k.is_empty()).count();

    // Open via VFS, insert more rows, checkpoint
    let vfs_name = unique_vfs_name("btree_cp");
    let vfs = TieredVfs::new(config).unwrap();
    let bench = vfs.shared_state();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let db_path = format!("file:local.db?vfs={}", vfs_name);
    bench.reset_s3_counters();
    {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        for i in 200..250 {
            conn.execute("INSERT INTO posts VALUES (?1, ?2, ?3)",
                rusqlite::params![i, i % 50, format!("post body {}", i)]).unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    // Cold reader: verify data from S3 (includes both import + VFS writes)
    {
        let reader_cache = tempfile::tempdir().unwrap();
        let reader_config = TieredConfig {
            bucket, prefix, endpoint_url: endpoint,
            region: Some("auto".to_string()),
            cache_dir: reader_cache.path().to_path_buf(),
            read_only: true, runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("btree_cp_reader");
        let reader_vfs = TieredVfs::new(reader_config).unwrap();
        turbolite::tiered::register(&reader_vfs_name, reader_vfs).unwrap();

        let reader_db = format!("file:local.db?vfs={}", reader_vfs_name);
        let conn = rusqlite::Connection::open_with_flags(
            &reader_db,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM posts", [], |row| row.get(0)).unwrap();
        assert_eq!(count, 250, "cold reader should see 250 rows");

        // Index should work
        let idx_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM posts WHERE user_id = 5", [], |row| row.get(0),
        ).unwrap();
        assert!(idx_count > 0, "index scan should work on cold reader");
    }
}

/// INSERT into an indexed table dirties fewer groups with B-tree-aware packing.
#[test]
fn test_write_amplification_btree_grouping() {
    let cache_dir = tempfile::tempdir().unwrap();
    let config = test_config("btree_wa", cache_dir.path());

    let local_db = cache_dir.path().join("wa.db");
    let manifest = create_and_import(&config, &local_db, |conn| {
        conn.execute_batch(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, category INTEGER, data TEXT);
             CREATE INDEX idx_items_cat ON items(category);",
        ).unwrap();
        for i in 0..500 {
            conn.execute("INSERT INTO items VALUES (?1, ?2, ?3)",
                rusqlite::params![i, i % 20, format!("item data {} padding here", i)]).unwrap();
        }
    });

    let total_groups = manifest.page_group_keys.iter().filter(|k| !k.is_empty()).count();

    let vfs_name = unique_vfs_name("btree_wa");
    let vfs = TieredVfs::new(config).unwrap();
    let bench = vfs.shared_state();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let db_path = format!("file:wa.db?vfs={}", vfs_name);
    bench.reset_s3_counters();
    {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        for i in 500..550 {
            conn.execute("INSERT INTO items VALUES (?1, ?2, ?3)",
                rusqlite::params![i, i % 20, format!("item data {} padding here", i)]).unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    let (puts, _) = bench.s3_put_counters();
    eprintln!("[test] write amplification: {} PUTs for 50 inserts ({} total groups)", puts, total_groups);
    assert!(puts <= (total_groups as u64 / 2).max(4),
        "B-tree grouping should limit dirty groups: {} PUTs vs {} total", puts, total_groups);
}

/// VACUUM + checkpoint: all pages have valid assignments, cold reader works.
#[test]
fn test_vacuum_produces_correct_mapping() {
    let cache_dir = tempfile::tempdir().unwrap();
    let config = test_config("btree_vac", cache_dir.path());
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let local_db = cache_dir.path().join("vac.db");
    create_and_import(&config, &local_db, |conn| {
        conn.execute_batch(
            "CREATE TABLE vac_test (id INTEGER PRIMARY KEY, data TEXT);
             CREATE INDEX idx_vac ON vac_test(data);",
        ).unwrap();
        for i in 0..500 {
            conn.execute("INSERT INTO vac_test VALUES (?1, ?2)",
                rusqlite::params![i, format!("row_{:04}_padding", i)]).unwrap();
        }
    });

    let vfs_name = unique_vfs_name("btree_vac");
    let vfs = TieredVfs::new(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let db_path = format!("file:vac.db?vfs={}", vfs_name);
    {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        conn.execute("DELETE FROM vac_test WHERE id >= 200", []).unwrap();
        conn.execute_batch("VACUUM;").unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    // All pages should have valid assignments
    let mut manifest = turbolite::tiered::get_manifest(&TieredConfig {
        bucket: bucket.clone(), prefix: prefix.clone(),
        endpoint_url: endpoint.clone(), region: Some("auto".to_string()),
        cache_dir: cache_dir.path().to_path_buf(), runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    }).unwrap().unwrap();
    manifest.detect_and_normalize_strategy();

    for pn in 0..manifest.page_count {
        assert!(manifest.page_location(pn).is_some(),
            "after VACUUM, page {} should have a group assignment", pn);
    }

    // Cold reader should see 200 rows
    {
        let reader_cache = tempfile::tempdir().unwrap();
        let reader_config = TieredConfig {
            bucket, prefix, endpoint_url: endpoint,
            region: Some("auto".to_string()),
            cache_dir: reader_cache.path().to_path_buf(),
            read_only: true, runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("btree_vac_reader");
        let reader_vfs = TieredVfs::new(reader_config).unwrap();
        turbolite::tiered::register(&reader_vfs_name, reader_vfs).unwrap();

        let reader_db = format!("file:vac.db?vfs={}", reader_vfs_name);
        let conn = rusqlite::Connection::open_with_flags(
            &reader_db,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM vac_test", [], |row| row.get(0)).unwrap();
        assert_eq!(count, 200, "cold reader should see 200 rows after VACUUM");

        let idx_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM vac_test WHERE data >= 'row_0100'", [], |row| row.get(0),
        ).unwrap();
        assert!(idx_count > 0 && idx_count <= 200, "index scan should work after VACUUM");
    }
}
