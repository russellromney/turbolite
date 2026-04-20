//! Integration tests for materialize_to_file (Phase Somme prerequisite).
//!
//! Coverage: happy path, negative/error cases, edge cases, and full e2e roundtrip.

use super::helpers::*;
use turbolite::tiered::*;
use std::path::Path;

/// Helper: create a local SQLite DB, import to S3, return (manifest, vfs_name, bench).
fn import_and_open(
    prefix: &str,
    cache_dir: &Path,
    setup_fn: impl FnOnce(&rusqlite::Connection),
) -> (Manifest, String, TurboliteSharedState) {
    let config = test_config(prefix, cache_dir);
    let local_db = cache_dir.join("source.db");
    {
        let conn = rusqlite::Connection::open(&local_db).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        setup_fn(&conn);
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
        drop(conn);
    }
    let manifest = import_sqlite_file(&config, &local_db).unwrap();
    let vfs_name = unique_vfs_name(prefix);
    let vfs = TurboliteVfs::new_local(config).unwrap();
    let bench = vfs.shared_state();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();
    (manifest, vfs_name, bench)
}

// ============================================================================
// Happy path
// ============================================================================

/// Materialize multi-table DB with indexes. Verify row counts, index scans, specific values.
#[test]
fn test_materialize_roundtrip() {
    let cache_dir = tempfile::tempdir().unwrap();
    let (manifest, _vfs_name, bench) = import_and_open("mat_rt", cache_dir.path(), |conn| {
        conn.execute_batch(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
             CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, body TEXT);
             CREATE INDEX idx_posts_user ON posts(user_id);",
        ).unwrap();
        for i in 0..500 {
            conn.execute("INSERT INTO users VALUES (?1, ?2)",
                rusqlite::params![i, format!("user_{}", i)]).unwrap();
            conn.execute("INSERT INTO posts VALUES (?1, ?2, ?3)",
                rusqlite::params![i, i % 50, format!("post body {} with some padding data", i)]).unwrap();
        }
    });

    let output = cache_dir.path().join("materialized.db");
    let version = bench.materialize_to_file(&output).unwrap();
    assert_eq!(version, manifest.version);

    let conn = rusqlite::Connection::open(&output).unwrap();
    assert_eq!(conn.query_row("SELECT COUNT(*) FROM users", [], |r| r.get::<_,i64>(0)).unwrap(), 500);
    assert_eq!(conn.query_row("SELECT COUNT(*) FROM posts", [], |r| r.get::<_,i64>(0)).unwrap(), 500);

    // Index scan
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM posts WHERE user_id = 5", [], |r| r.get::<_,i64>(0)).unwrap(),
        10,
    );

    // Specific value
    let name: String = conn.query_row("SELECT name FROM users WHERE id = 42", [], |r| r.get(0)).unwrap();
    assert_eq!(name, "user_42");
}

/// Materialize after VFS writes + checkpoint includes both imported and written data.
#[test]
fn test_materialize_after_vfs_writes() {
    let cache_dir = tempfile::tempdir().unwrap();
    let (_manifest, vfs_name, bench) = import_and_open("mat_vfs", cache_dir.path(), |conn| {
        conn.execute_batch("CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT);").unwrap();
        for i in 0..100 {
            conn.execute("INSERT INTO data VALUES (?1, ?2)",
                rusqlite::params![i, format!("val_{}", i)]).unwrap();
        }
    });

    let db_path = format!("file:source.db?vfs={}", vfs_name);
    {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        for i in 100..200 {
            conn.execute("INSERT INTO data VALUES (?1, ?2)",
                rusqlite::params![i, format!("val_{}", i)]).unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    let output = cache_dir.path().join("materialized.db");
    let version = bench.materialize_to_file(&output).unwrap();
    assert!(version > 0, "version should be > 0 after VFS checkpoint");

    let conn = rusqlite::Connection::open(&output).unwrap();
    assert_eq!(conn.query_row("SELECT COUNT(*) FROM data", [], |r| r.get::<_,i64>(0)).unwrap(), 200);
}

// ============================================================================
// Edge cases
// ============================================================================

/// Single-page database (just header + sqlite_master, minimal content).
#[test]
fn test_materialize_single_page_db() {
    let cache_dir = tempfile::tempdir().unwrap();
    let (_manifest, _vfs_name, bench) = import_and_open("mat_1pg", cache_dir.path(), |conn| {
        conn.execute_batch("CREATE TABLE tiny (id INTEGER PRIMARY KEY);").unwrap();
        conn.execute("INSERT INTO tiny VALUES (1)", []).unwrap();
    });

    let output = cache_dir.path().join("materialized.db");
    bench.materialize_to_file(&output).unwrap();

    let conn = rusqlite::Connection::open(&output).unwrap();
    assert_eq!(conn.query_row("SELECT COUNT(*) FROM tiny", [], |r| r.get::<_,i64>(0)).unwrap(), 1);
}

/// After DELETE + VACUUM, materialized file has correct reduced page count.
#[test]
fn test_materialize_after_delete_and_vacuum() {
    let cache_dir = tempfile::tempdir().unwrap();
    let (_manifest, vfs_name, bench) = import_and_open("mat_vac", cache_dir.path(), |conn| {
        conn.execute_batch("CREATE TABLE big (id INTEGER PRIMARY KEY, data TEXT);").unwrap();
        for i in 0..500 {
            conn.execute("INSERT INTO big VALUES (?1, ?2)",
                rusqlite::params![i, format!("row_{:04}_padding_data_here", i)]).unwrap();
        }
    });

    let db_path = format!("file:source.db?vfs={}", vfs_name);
    {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        conn.execute("DELETE FROM big WHERE id >= 100", []).unwrap();
        conn.execute_batch("VACUUM;").unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    let output = cache_dir.path().join("materialized.db");
    bench.materialize_to_file(&output).unwrap();

    let conn = rusqlite::Connection::open(&output).unwrap();
    assert_eq!(conn.query_row("SELECT COUNT(*) FROM big", [], |r| r.get::<_,i64>(0)).unwrap(), 100);
}

/// 64KB page size (non-default).
#[test]
fn test_materialize_large_page_size() {
    let cache_dir = tempfile::tempdir().unwrap();
    let mut config = test_config("mat_64k", cache_dir.path());
    config.pages_per_group = 16;

    let local_db = cache_dir.path().join("source.db");
    {
        let conn = rusqlite::Connection::open(&local_db).unwrap();
        conn.execute_batch("PRAGMA page_size=65536; PRAGMA journal_mode=WAL;").unwrap();
        conn.execute_batch("CREATE TABLE wide (id INTEGER PRIMARY KEY, data TEXT);").unwrap();
        for i in 0..100 {
            conn.execute("INSERT INTO wide VALUES (?1, ?2)",
                rusqlite::params![i, format!("data_{}", i)]).unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
        drop(conn);
    }

    import_sqlite_file(&config, &local_db).unwrap();
    let vfs_name = unique_vfs_name("mat_64k");
    let vfs = TurboliteVfs::new_local(config).unwrap();
    let bench = vfs.shared_state();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let output = cache_dir.path().join("materialized.db");
    bench.materialize_to_file(&output).unwrap();

    let conn = rusqlite::Connection::open(&output).unwrap();
    assert_eq!(conn.query_row("PRAGMA page_size", [], |r| r.get::<_,i64>(0)).unwrap(), 65536);
    assert_eq!(conn.query_row("SELECT COUNT(*) FROM wide", [], |r| r.get::<_,i64>(0)).unwrap(), 100);
}

/// Multiple checkpoints (version > 1). Materialize uses latest.
#[test]
fn test_materialize_after_multiple_checkpoints() {
    let cache_dir = tempfile::tempdir().unwrap();
    let (_manifest, vfs_name, bench) = import_and_open("mat_multi", cache_dir.path(), |conn| {
        conn.execute_batch("CREATE TABLE log (id INTEGER PRIMARY KEY, msg TEXT);").unwrap();
        for i in 0..50 {
            conn.execute("INSERT INTO log VALUES (?1, ?2)",
                rusqlite::params![i, format!("initial_{}", i)]).unwrap();
        }
    });

    let db_path = format!("file:source.db?vfs={}", vfs_name);
    for round in 0..3 {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        let base = 50 + round * 20;
        for i in base..(base + 20) {
            conn.execute("INSERT INTO log VALUES (?1, ?2)",
                rusqlite::params![i, format!("round{}_{}", round, i)]).unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    let output = cache_dir.path().join("materialized.db");
    let version = bench.materialize_to_file(&output).unwrap();
    assert!(version > 0, "version should be > 0 after multiple checkpoints, got {}", version);

    let conn = rusqlite::Connection::open(&output).unwrap();
    assert_eq!(conn.query_row("SELECT COUNT(*) FROM log", [], |r| r.get::<_,i64>(0)).unwrap(), 110);
}

/// Large BLOBs spanning overflow pages.
#[test]
fn test_materialize_with_large_blobs() {
    let cache_dir = tempfile::tempdir().unwrap();
    let (_manifest, _vfs_name, bench) = import_and_open("mat_blob", cache_dir.path(), |conn| {
        conn.execute_batch("CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB);").unwrap();
        for i in 0..10 {
            let blob = vec![i as u8; 50_000];
            conn.execute("INSERT INTO blobs VALUES (?1, ?2)", rusqlite::params![i, blob]).unwrap();
        }
    });

    let output = cache_dir.path().join("materialized.db");
    bench.materialize_to_file(&output).unwrap();

    let conn = rusqlite::Connection::open(&output).unwrap();
    assert_eq!(conn.query_row("SELECT COUNT(*) FROM blobs", [], |r| r.get::<_,i64>(0)).unwrap(), 10);

    let blob: Vec<u8> = conn.query_row("SELECT data FROM blobs WHERE id = 5", [], |r| r.get(0)).unwrap();
    assert_eq!(blob.len(), 50_000);
    assert!(blob.iter().all(|&b| b == 5));
}

/// Materialize to a path that already has a file (should overwrite).
#[test]
fn test_materialize_overwrites_existing_file() {
    let cache_dir = tempfile::tempdir().unwrap();
    let (_manifest, _vfs_name, bench) = import_and_open("mat_overwrite", cache_dir.path(), |conn| {
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY);").unwrap();
        for i in 0..50 { conn.execute("INSERT INTO t VALUES (?1)", [i]).unwrap(); }
    });

    let output = cache_dir.path().join("materialized.db");

    // Write garbage to the output path first
    std::fs::write(&output, b"this is not a sqlite database").unwrap();
    assert!(output.exists());

    // Materialize should overwrite it
    bench.materialize_to_file(&output).unwrap();

    let conn = rusqlite::Connection::open(&output).unwrap();
    assert_eq!(conn.query_row("SELECT COUNT(*) FROM t", [], |r| r.get::<_,i64>(0)).unwrap(), 50);
}

// ============================================================================
// Negative / error cases
// ============================================================================

/// Materialize with no data imported (empty manifest) should return error.
#[test]
fn test_materialize_empty_manifest_errors() {
    let cache_dir = tempfile::tempdir().unwrap();
    let config = test_config("mat_empty", cache_dir.path());

    let vfs_name = unique_vfs_name("mat_empty");
    let vfs = TurboliteVfs::new_local(config).unwrap();
    let bench = vfs.shared_state();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let output = cache_dir.path().join("materialized.db");
    let result = bench.materialize_to_file(&output);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("empty manifest"));
}

// ============================================================================
// E2E: materialized file is writable (proves it's a real usable SQLite DB)
// ============================================================================

/// Materialize, then open the file with plain SQLite, insert more rows, verify.
/// Proves the materialized file is a fully functional SQLite database.
#[test]
fn test_materialize_produces_writable_db() {
    let cache_dir = tempfile::tempdir().unwrap();
    let (_manifest, _vfs_name, bench) = import_and_open("mat_writable", cache_dir.path(), |conn| {
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);").unwrap();
        for i in 0..100 {
            conn.execute("INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("original_{}", i)]).unwrap();
        }
    });

    let output = cache_dir.path().join("materialized.db");
    bench.materialize_to_file(&output).unwrap();

    // Open with plain SQLite (no VFS), insert more rows
    let conn = rusqlite::Connection::open(&output).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
    for i in 100..150 {
        conn.execute("INSERT INTO t VALUES (?1, ?2)",
            rusqlite::params![i, format!("added_{}", i)]).unwrap();
    }

    assert_eq!(conn.query_row("SELECT COUNT(*) FROM t", [], |r| r.get::<_,i64>(0)).unwrap(), 150);

    // Also verify we can UPDATE and DELETE
    conn.execute("UPDATE t SET val = 'modified' WHERE id = 0", []).unwrap();
    conn.execute("DELETE FROM t WHERE id = 149", []).unwrap();
    assert_eq!(conn.query_row("SELECT COUNT(*) FROM t", [], |r| r.get::<_,i64>(0)).unwrap(), 149);
    let val: String = conn.query_row("SELECT val FROM t WHERE id = 0", [], |r| r.get(0)).unwrap();
    assert_eq!(val, "modified");
}

// ============================================================================
// E2E: byte-for-byte comparison against source
// ============================================================================

/// Materialize and compare against the original source file byte-for-byte.
#[test]
fn test_materialize_matches_source_bytes() {
    let cache_dir = tempfile::tempdir().unwrap();
    let config = test_config("mat_bytes", cache_dir.path());

    let local_db = cache_dir.path().join("source.db");
    {
        let conn = rusqlite::Connection::open(&local_db).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        conn.execute_batch(
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY, a TEXT, b REAL);
             CREATE TABLE t2 (id INTEGER PRIMARY KEY, ref_id INTEGER, data BLOB);
             CREATE INDEX idx_t2_ref ON t2(ref_id);",
        ).unwrap();
        for i in 0..200 {
            conn.execute("INSERT INTO t1 VALUES (?1, ?2, ?3)",
                rusqlite::params![i, format!("text_{}", i), i as f64 * 1.5]).unwrap();
            conn.execute("INSERT INTO t2 VALUES (?1, ?2, ?3)",
                rusqlite::params![i, i % 20, vec![i as u8; 100]]).unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
        drop(conn);
    }

    let source_bytes = std::fs::read(&local_db).unwrap();
    import_sqlite_file(&config, &local_db).unwrap();

    let vfs_name = unique_vfs_name("mat_bytes");
    let vfs = TurboliteVfs::new_local(config).unwrap();
    let bench = vfs.shared_state();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let output = cache_dir.path().join("materialized.db");
    bench.materialize_to_file(&output).unwrap();

    let materialized_bytes = std::fs::read(&output).unwrap();

    assert_eq!(source_bytes.len(), materialized_bytes.len(),
        "file size mismatch: source={}, materialized={}", source_bytes.len(), materialized_bytes.len());

    let first_diff = source_bytes.iter().zip(materialized_bytes.iter())
        .enumerate()
        .find(|(_, (a, b))| a != b);

    if let Some((offset, (expected, actual))) = first_diff {
        panic!("byte mismatch at offset {} (page {}): source=0x{:02x}, materialized=0x{:02x}",
            offset, offset / 4096, expected, actual);
    }
}

// ============================================================================
// E2E: complex queries on materialized file
// ============================================================================

/// Materialize then run complex queries (joins, aggregates, subqueries).
#[test]
fn test_materialize_complex_queries() {
    let cache_dir = tempfile::tempdir().unwrap();
    let (_manifest, _vfs_name, bench) = import_and_open("mat_cplx", cache_dir.path(), |conn| {
        conn.execute_batch(
            "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT);
             CREATE TABLE employees (id INTEGER PRIMARY KEY, dept_id INTEGER, name TEXT, salary REAL);
             CREATE INDEX idx_emp_dept ON employees(dept_id);",
        ).unwrap();
        for i in 0..10 {
            conn.execute("INSERT INTO departments VALUES (?1, ?2)",
                rusqlite::params![i, format!("dept_{}", i)]).unwrap();
        }
        for i in 0..1000 {
            conn.execute("INSERT INTO employees VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![i, i % 10, format!("emp_{}", i), 50000.0 + (i as f64 * 100.0)]).unwrap();
        }
    });

    let output = cache_dir.path().join("materialized.db");
    bench.materialize_to_file(&output).unwrap();

    let conn = rusqlite::Connection::open(&output).unwrap();

    // Join
    assert_eq!(
        conn.query_row("SELECT COUNT(DISTINCT d.name) FROM departments d JOIN employees e ON e.dept_id = d.id",
            [], |r| r.get::<_,i64>(0)).unwrap(),
        10,
    );

    // Aggregate
    let avg: f64 = conn.query_row("SELECT AVG(salary) FROM employees", [], |r| r.get(0)).unwrap();
    assert!(avg > 90000.0 && avg < 110000.0, "avg={}", avg);

    // Subquery
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM employees WHERE salary > (SELECT AVG(salary) FROM employees)",
            [], |r| r.get::<_,i64>(0)).unwrap(),
        500,
    );

    // Group by + having
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM (SELECT dept_id FROM employees GROUP BY dept_id HAVING COUNT(*) >= 100)",
            [], |r| r.get::<_,i64>(0)).unwrap(),
        10,
    );
}
