//! Differential oracle with S3 backend: write via turbolite, checkpoint to S3,
//! cold-read from S3 only, compare against vanilla SQLite.
//!
//! This catches manifest/page-group ordering bugs, compression round-trip errors,
//! and any case where the S3 flush path loses or corrupts data.

use rusqlite::{Connection, OpenFlags};
use tempfile::TempDir;
use turbolite::tiered::{TurboliteConfig, TurboliteVfs};

use super::helpers::*;

/// Snapshot all rows from a table as sorted strings for comparison.
fn snapshot_table(conn: &Connection, table: &str, cols: &str) -> Vec<String> {
    let sql = format!("SELECT {} FROM {} ORDER BY rowid", cols, table);
    let mut stmt = conn.prepare(&sql).expect("prepare snapshot");
    stmt.query_map([], |row| {
        let mut parts = Vec::new();
        for i in 0.. {
            match row.get_ref(i) {
                Ok(val) => {
                    let s = match val {
                        rusqlite::types::ValueRef::Null => "NULL".to_string(),
                        rusqlite::types::ValueRef::Integer(i) => i.to_string(),
                        rusqlite::types::ValueRef::Real(f) => format!("{:.10}", f),
                        rusqlite::types::ValueRef::Text(t) => {
                            format!("'{}'", String::from_utf8_lossy(t))
                        }
                        rusqlite::types::ValueRef::Blob(b) => {
                            let hex: String =
                                b.iter().map(|byte| format!("{:02x}", byte)).collect();
                            format!("x'{}'", hex)
                        }
                    };
                    parts.push(s);
                }
                Err(_) => break,
            }
        }
        Ok(parts.join("|"))
    })
    .expect("query")
    .collect::<Result<Vec<_>, _>>()
    .expect("collect")
}

fn open_vanilla(path: &std::path::Path) -> Connection {
    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
    )
    .expect("vanilla open")
}

fn open_turbolite(path: &str, vfs_name: &str) -> Connection {
    Connection::open_with_flags_and_vfs(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        vfs_name,
    )
    .expect("turbolite open")
}

fn open_turbolite_readonly(path: &str, vfs_name: &str) -> Connection {
    Connection::open_with_flags_and_vfs(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY,
        vfs_name,
    )
    .expect("turbolite readonly open")
}

/// Core S3 oracle: write data, checkpoint to S3, cold-read from fresh VFS, compare.
#[test]
fn oracle_s3_write_checkpoint_cold_read() {
    let vanilla_dir = TempDir::new().expect("tempdir");
    let writer_dir = TempDir::new().expect("tempdir");
    let reader_dir = TempDir::new().expect("tempdir");

    // Set up S3-backed turbolite
    let config = test_config("oracle_s3", writer_dir.path());
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let vfs_name = unique_vfs_name("oracle_s3_writer");

    let vfs = TurboliteVfs::new(config).expect("vfs");
    turbolite::tiered::register(&vfs_name, vfs).expect("register");

    // Open both connections
    let v = open_vanilla(&vanilla_dir.path().join("vanilla.db"));
    let t = open_turbolite("oracle_s3_test.db", &vfs_name);

    // WAL mode on both
    v.execute_batch("PRAGMA page_size=65536; PRAGMA journal_mode=WAL;")
        .expect("pragma vanilla");
    t.execute_batch("PRAGMA page_size=65536; PRAGMA journal_mode=WAL;")
        .expect("pragma turbo");

    // Create schema on both
    let schema = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, score REAL);
                  CREATE TABLE logs (id INTEGER PRIMARY KEY, data BLOB);";
    v.execute_batch(schema).expect("schema vanilla");
    t.execute_batch(schema).expect("schema turbo");

    // Insert data on both
    for i in 0..500 {
        let name = format!("user_{}", i);
        let score = i as f64 * 1.23;
        v.execute(
            "INSERT INTO users (id, name, score) VALUES (?1, ?2, ?3)",
            rusqlite::params![i, name, score],
        )
        .expect("insert vanilla");
        t.execute(
            "INSERT INTO users (id, name, score) VALUES (?1, ?2, ?3)",
            rusqlite::params![i, name, score],
        )
        .expect("insert turbo");
    }

    // Insert some blobs
    for i in 0..50 {
        let blob = vec![i as u8; (i + 1) * 100];
        v.execute(
            "INSERT INTO logs (id, data) VALUES (?1, ?2)",
            rusqlite::params![i, blob],
        )
        .expect("blob vanilla");
        t.execute(
            "INSERT INTO logs (id, data) VALUES (?1, ?2)",
            rusqlite::params![i, blob],
        )
        .expect("blob turbo");
    }

    // Checkpoint turbolite to flush to S3
    t.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("checkpoint");

    // Verify S3 manifest exists
    verify_s3_manifest(&bucket, &prefix, &endpoint, 1, 65536);

    // Compare hot read (from writer's cache)
    let v_users = snapshot_table(&v, "users", "id, name, score");
    let t_users = snapshot_table(&t, "users", "id, name, score");
    assert_eq!(v_users, t_users, "users diverged (hot read)");

    let v_logs = snapshot_table(&v, "logs", "id, hex(data)");
    let t_logs = snapshot_table(&t, "logs", "id, hex(data)");
    assert_eq!(v_logs, t_logs, "logs diverged (hot read)");

    // Drop writer connection
    drop(t);

    // Cold read: open a FRESH VFS with empty cache, same S3 prefix
    let cold_config = TurboliteConfig {
        bucket: bucket.clone(),
        prefix: prefix.clone(),
        cache_dir: reader_dir.path().to_path_buf(),
        endpoint_url: endpoint.clone(),
        region: Some("auto".to_string()),
        read_only: true,
        runtime_handle: Some(shared_runtime_handle()),
        ..Default::default()
    };
    let cold_vfs_name = unique_vfs_name("oracle_s3_cold");
    let cold_vfs = TurboliteVfs::new(cold_config).expect("cold vfs");
    turbolite::tiered::register(&cold_vfs_name, cold_vfs).expect("register cold");

    let cold = open_turbolite_readonly("oracle_s3_cold.db", &cold_vfs_name);

    // Compare cold read against vanilla
    let cold_users = snapshot_table(&cold, "users", "id, name, score");
    assert_eq!(
        v_users, cold_users,
        "users diverged after cold read from S3"
    );

    let cold_logs = snapshot_table(&cold, "logs", "id, hex(data)");
    assert_eq!(v_logs, cold_logs, "logs diverged after cold read from S3");

    // Integrity check on cold reader
    let integrity: String = cold
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity");
    assert_eq!(integrity, "ok", "cold read integrity check failed");
}

/// Oracle with multiple checkpoint cycles: write, checkpoint, write more, checkpoint again.
/// Verifies incremental S3 state is correct.
#[test]
fn oracle_s3_incremental_checkpoints() {
    let vanilla_dir = TempDir::new().expect("tempdir");
    let writer_dir = TempDir::new().expect("tempdir");

    let config = test_config("oracle_incr", writer_dir.path());
    let vfs_name = unique_vfs_name("oracle_incr");

    let vfs = TurboliteVfs::new(config).expect("vfs");
    turbolite::tiered::register(&vfs_name, vfs).expect("register");

    let v = open_vanilla(&vanilla_dir.path().join("vanilla.db"));
    let t = open_turbolite("oracle_incr_test.db", &vfs_name);

    v.execute_batch("PRAGMA page_size=65536; PRAGMA journal_mode=WAL;")
        .expect("pragma");
    t.execute_batch("PRAGMA page_size=65536; PRAGMA journal_mode=WAL;")
        .expect("pragma");

    let schema = "CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT);";
    v.execute_batch(schema).expect("schema v");
    t.execute_batch(schema).expect("schema t");

    // Batch 1: insert 100 rows, checkpoint
    for i in 0..100 {
        let k = format!("key_{}", i);
        let val = format!("val_{}", i);
        v.execute(
            "INSERT INTO kv (key, value) VALUES (?1, ?2)",
            rusqlite::params![k, val],
        )
        .expect("insert v");
        t.execute(
            "INSERT INTO kv (key, value) VALUES (?1, ?2)",
            rusqlite::params![k, val],
        )
        .expect("insert t");
    }
    t.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("cp1");

    let v_snap1 = snapshot_table(&v, "kv", "key, value");
    let t_snap1 = snapshot_table(&t, "kv", "key, value");
    assert_eq!(v_snap1, t_snap1, "diverged after checkpoint 1");

    // Batch 2: update 50 rows, delete 25, insert 50 new, checkpoint
    for i in 0..50 {
        let k = format!("key_{}", i);
        let val = format!("updated_{}", i);
        v.execute(
            "UPDATE kv SET value = ?2 WHERE key = ?1",
            rusqlite::params![k, val],
        )
        .expect("update v");
        t.execute(
            "UPDATE kv SET value = ?2 WHERE key = ?1",
            rusqlite::params![k, val],
        )
        .expect("update t");
    }
    for i in 50..75 {
        let k = format!("key_{}", i);
        v.execute("DELETE FROM kv WHERE key = ?1", rusqlite::params![k])
            .expect("del v");
        t.execute("DELETE FROM kv WHERE key = ?1", rusqlite::params![k])
            .expect("del t");
    }
    for i in 100..150 {
        let k = format!("key_{}", i);
        let val = format!("new_{}", i);
        v.execute(
            "INSERT INTO kv (key, value) VALUES (?1, ?2)",
            rusqlite::params![k, val],
        )
        .expect("insert v");
        t.execute(
            "INSERT INTO kv (key, value) VALUES (?1, ?2)",
            rusqlite::params![k, val],
        )
        .expect("insert t");
    }
    t.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("cp2");

    let v_snap2 = snapshot_table(&v, "kv", "key, value");
    let t_snap2 = snapshot_table(&t, "kv", "key, value");
    assert_eq!(v_snap2, t_snap2, "diverged after checkpoint 2");

    // Batch 3: VACUUM then more writes
    v.execute_batch("VACUUM").expect("vacuum v");
    t.execute_batch("VACUUM").expect("vacuum t");
    t.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("cp3");

    for i in 150..200 {
        let k = format!("key_{}", i);
        let val = format!("post_vacuum_{}", i);
        v.execute(
            "INSERT INTO kv (key, value) VALUES (?1, ?2)",
            rusqlite::params![k, val],
        )
        .expect("insert v");
        t.execute(
            "INSERT INTO kv (key, value) VALUES (?1, ?2)",
            rusqlite::params![k, val],
        )
        .expect("insert t");
    }
    t.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("cp4");

    let v_final = snapshot_table(&v, "kv", "key, value");
    let t_final = snapshot_table(&t, "kv", "key, value");
    assert_eq!(v_final, t_final, "diverged after vacuum + more writes");

    // Integrity
    let integrity: String = t
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity");
    assert_eq!(integrity, "ok");
}
