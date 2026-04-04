//! Multi-process concurrent access tests for TurboliteVfs.
//!
//! Each test creates a database, spawns child processes (by re-invoking
//! the test binary with env vars), and verifies data integrity.

use rusqlite::{Connection, OpenFlags};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};
use turbolite::tiered::{StorageBackend, TurboliteConfig, TurboliteVfs};

/// If this env var is set, we are a child process; run the child workload.
const CHILD_MODE_ENV: &str = "TURBOLITE_TEST_CHILD_MODE";
const CHILD_DIR_ENV: &str = "TURBOLITE_TEST_DIR";

fn register_local_vfs(name: &str, cache_dir: &Path) {
    let config = TurboliteConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: cache_dir.into(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new(config).expect("create VFS");
    turbolite::tiered::register(name, vfs).expect("register VFS");
}

fn open_db(db_path: &Path, vfs_name: &str) -> Connection {
    Connection::open_with_flags_and_vfs(
        db_path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        vfs_name,
    )
    .expect("open db")
}

fn open_db_readonly(db_path: &Path, vfs_name: &str) -> Connection {
    Connection::open_with_flags_and_vfs(
        db_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY,
        vfs_name,
    )
    .expect("open db readonly")
}

/// Spawn a child process that runs the same test binary with child mode env vars.
fn spawn_child(test_dir: &Path, mode: &str) -> std::process::Child {
    let exe = std::env::current_exe().expect("current exe");
    Command::new(exe)
        .env(CHILD_MODE_ENV, mode)
        .env(CHILD_DIR_ENV, test_dir.to_str().expect("dir to str"))
        .arg("--ignored")
        .arg("child_entry_point")
        .arg("--exact")
        .arg("--nocapture")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn child")
}

/// Wait for a child and return (exit status, stdout, stderr).
fn wait_child(child: std::process::Child) -> (std::process::ExitStatus, String, String) {
    let output = child.wait_with_output().expect("wait_with_output");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    (output.status, stdout, stderr)
}

fn child_vfs_name() -> String {
    format!("mp_child_{}", std::process::id())
}

// ---------------------------------------------------------------------------
// Child workloads
// ---------------------------------------------------------------------------

fn child_reader(dir: &Path) {
    let vfs_name = child_vfs_name();
    register_local_vfs(&vfs_name, dir);

    let db_path = dir.join("test.db");
    let conn = open_db_readonly(&db_path, &vfs_name);

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut reads = 0u64;
    while Instant::now() < deadline {
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM test", [], |row| row.get(0))
            .expect("reader: SELECT COUNT(*)");
        assert!(count >= 100, "reader: expected >= 100 rows, got {}", count);
        reads += 1;
    }
    eprintln!("child_reader: completed {} reads", reads);
}

fn child_writer(dir: &Path) {
    let vfs_name = child_vfs_name();
    register_local_vfs(&vfs_name, dir);

    let db_path = dir.join("test.db");
    let conn = open_db(&db_path, &vfs_name);
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000;")
        .expect("writer pragmas");

    for i in 0..50 {
        conn.execute(
            "INSERT INTO test (id, data) VALUES (?, ?)",
            rusqlite::params![100 + i + 1, format!("child_writer_{}", i)],
        )
        .unwrap_or_else(|e| panic!("writer: INSERT {} failed: {}", i, e));
        std::thread::sleep(Duration::from_millis(5));
    }
    eprintln!("child_writer: inserted 50 rows");
}

fn child_point_reader(dir: &Path) {
    let vfs_name = child_vfs_name();
    register_local_vfs(&vfs_name, dir);

    let db_path = dir.join("test.db");
    let conn = open_db_readonly(&db_path, &vfs_name);

    for i in 0..100 {
        let rowid = (i % 1000) + 1;
        let data: String = conn
            .query_row(
                "SELECT data FROM test WHERE id = ?",
                [rowid],
                |row| row.get(0),
            )
            .unwrap_or_else(|e| panic!("point_reader: row {} failed: {}", rowid, e));
        assert!(
            data.starts_with("row_"),
            "unexpected data for id {}: {}",
            rowid,
            data
        );
    }
    eprintln!("child_point_reader: 100 point reads OK");
}

fn child_verify_reader(dir: &Path) {
    let vfs_name = child_vfs_name();
    register_local_vfs(&vfs_name, dir);

    let db_path = dir.join("test.db");
    let conn = open_db_readonly(&db_path, &vfs_name);

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM test", [], |row| row.get(0))
        .expect("verify_reader: COUNT(*)");
    assert_eq!(count, 200, "verify_reader: expected 200 rows, got {}", count);

    // Spot-check a few rows.
    for id in [1, 50, 100, 150, 200] {
        let data: String = conn
            .query_row(
                "SELECT data FROM test WHERE id = ?",
                [id],
                |row| row.get(0),
            )
            .unwrap_or_else(|e| panic!("verify_reader: id {} failed: {}", id, e));
        assert!(!data.is_empty(), "verify_reader: empty data for id {}", id);
    }
    eprintln!("child_verify_reader: all 200 rows verified");
}

// ---------------------------------------------------------------------------
// Child entry point (ignored test, invoked by spawn_child)
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn child_entry_point() {
    let mode = match std::env::var(CHILD_MODE_ENV) {
        Ok(m) => m,
        Err(_) => return,
    };
    let dir = PathBuf::from(std::env::var(CHILD_DIR_ENV).expect("CHILD_DIR"));

    match mode.as_str() {
        "reader" => child_reader(&dir),
        "writer" => child_writer(&dir),
        "point_reader" => child_point_reader(&dir),
        "verify_reader" => child_verify_reader(&dir),
        other => panic!("unknown child mode: {}", other),
    }
}

// ---------------------------------------------------------------------------
// Main tests
// ---------------------------------------------------------------------------

/// Main process creates DB in WAL mode, inserts 100 rows, then spawns a
/// reader child and a writer child concurrently. After both exit, verifies
/// the final row count is 150.
#[test]
fn test_multiprocess_reader_writer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let vfs_name = format!("mp_rw_{}", std::process::id());
    register_local_vfs(&vfs_name, dir.path());

    let db_path = dir.path().join("test.db");

    // Create DB and insert 100 rows.
    {
        let conn = open_db(&db_path, &vfs_name);
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;",
        )
        .expect("pragmas");
        conn.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT NOT NULL)",
            [],
        )
        .expect("CREATE TABLE");
        for i in 1..=100 {
            conn.execute(
                "INSERT INTO test (id, data) VALUES (?, ?)",
                rusqlite::params![i, format!("row_{}", i)],
            )
            .expect("INSERT");
        }
    }

    // Spawn reader + writer children.
    let reader_child = spawn_child(dir.path(), "reader");
    let writer_child = spawn_child(dir.path(), "writer");

    let (r_status, r_out, r_err) = wait_child(reader_child);
    let (w_status, w_out, w_err) = wait_child(writer_child);

    assert!(
        r_status.success(),
        "reader child failed (exit {:?}):\nstdout: {}\nstderr: {}",
        r_status.code(),
        r_out,
        r_err,
    );
    assert!(
        w_status.success(),
        "writer child failed (exit {:?}):\nstdout: {}\nstderr: {}",
        w_status.code(),
        w_out,
        w_err,
    );

    // Verify final count: 100 original + 50 written = 150.
    {
        let conn = open_db_readonly(&db_path, &vfs_name);
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM test", [], |row| row.get(0))
            .expect("final count");
        assert_eq!(count, 150, "expected 150 rows, got {}", count);
    }
}

/// Main creates DB with 1000 rows. Spawns 2 reader children that each do
/// 100 random point lookups. Both should exit 0.
#[test]
fn test_multiprocess_two_readers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let vfs_name = format!("mp_2r_{}", std::process::id());
    register_local_vfs(&vfs_name, dir.path());

    let db_path = dir.path().join("test.db");

    {
        let conn = open_db(&db_path, &vfs_name);
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;",
        )
        .expect("pragmas");
        conn.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT NOT NULL)",
            [],
        )
        .expect("CREATE TABLE");
        for i in 1..=1000 {
            conn.execute(
                "INSERT INTO test (id, data) VALUES (?, ?)",
                rusqlite::params![i, format!("row_{}", i)],
            )
            .expect("INSERT");
        }
    }

    let child_a = spawn_child(dir.path(), "point_reader");
    let child_b = spawn_child(dir.path(), "point_reader");

    let (a_status, a_out, a_err) = wait_child(child_a);
    let (b_status, b_out, b_err) = wait_child(child_b);

    assert!(
        a_status.success(),
        "reader A failed (exit {:?}):\nstdout: {}\nstderr: {}",
        a_status.code(),
        a_out,
        a_err,
    );
    assert!(
        b_status.success(),
        "reader B failed (exit {:?}):\nstdout: {}\nstderr: {}",
        b_status.code(),
        b_out,
        b_err,
    );
}

/// Main creates DB, inserts data, closes connection. Spawns a child that
/// opens DB read-only and verifies all data. Child exits 0 on success.
#[test]
fn test_multiprocess_write_then_read() {
    let dir = tempfile::tempdir().expect("tempdir");
    let vfs_name = format!("mp_wr_{}", std::process::id());
    register_local_vfs(&vfs_name, dir.path());

    let db_path = dir.path().join("test.db");

    {
        let conn = open_db(&db_path, &vfs_name);
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;",
        )
        .expect("pragmas");
        conn.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT NOT NULL)",
            [],
        )
        .expect("CREATE TABLE");
        for i in 1..=200 {
            conn.execute(
                "INSERT INTO test (id, data) VALUES (?, ?)",
                rusqlite::params![i, format!("row_{}", i)],
            )
            .expect("INSERT");
        }
        // Checkpoint so child can read without WAL.
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .expect("checkpoint");
    }

    let child = spawn_child(dir.path(), "verify_reader");
    let (status, out, err) = wait_child(child);

    assert!(
        status.success(),
        "verify_reader child failed (exit {:?}):\nstdout: {}\nstderr: {}",
        status.code(),
        out,
        err,
    );
}
