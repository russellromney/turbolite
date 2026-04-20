//! Differential oracle with S3 backend: write via turbolite, checkpoint to S3,
//! cold-read from S3 only, compare against vanilla SQLite.
//!
//! Runs across all TestMode variants: compressed, compressed+encrypted, plain,
//! and LocalThenFlush. Any divergence from vanilla SQLite is a turbolite bug.

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

/// Run the full oracle for a given mode: write, checkpoint, cold-read, compare.
fn run_oracle_write_checkpoint_cold_read(mode: TestMode) {
    let vanilla_dir = TempDir::new().expect("tempdir");
    let writer_dir = TempDir::new().expect("tempdir");
    let reader_dir = TempDir::new().expect("tempdir");

    let config = test_config_mode(
        &format!("oracle_{}", mode.name()),
        writer_dir.path(),
        mode,
    );
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let vfs_name = unique_vfs_name(&format!("oracle_{}_w", mode.name()));

    let vfs = TurboliteVfs::new_local(config).expect("vfs");
    turbolite::tiered::register(&vfs_name, vfs).expect("register");

    let v = open_vanilla(&vanilla_dir.path().join("vanilla.db"));
    let t = open_turbolite(
        &format!("oracle_{}_test.db", mode.name()),
        &vfs_name,
    );

    v.execute_batch("PRAGMA page_size=65536; PRAGMA journal_mode=WAL;")
        .expect("pragma vanilla");
    t.execute_batch("PRAGMA page_size=65536; PRAGMA journal_mode=WAL;")
        .expect("pragma turbo");

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

    // Checkpoint
    t.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("checkpoint");

    // For LocalThenFlush mode, we need to get the VFS and call flush
    // (checkpoint only writes locally; flush uploads to S3)
    // But we can't access the VFS after registration easily.
    // The checkpoint in WAL mode with Durable sync already uploads.
    // For LTF, the data won't be in S3 until flush -- skip cold read for LTF.
    let skip_cold_read = !mode.supports_cold_read();

    // Verify S3 manifest exists (except LTF which doesn't upload on checkpoint)
    if !skip_cold_read {
        verify_s3_manifest(&bucket, &prefix, &endpoint, 1, 65536);
    }

    // Compare hot read
    let v_users = snapshot_table(&v, "users", "id, name, score");
    let t_users = snapshot_table(&t, "users", "id, name, score");
    assert_eq!(v_users, t_users, "[{}] users diverged (hot read)", mode.name());

    let v_logs = snapshot_table(&v, "logs", "id, hex(data)");
    let t_logs = snapshot_table(&t, "logs", "id, hex(data)");
    assert_eq!(v_logs, t_logs, "[{}] logs diverged (hot read)", mode.name());

    // Cold read from S3
    if !skip_cold_read {
        drop(t);

        let cold_config = cold_reader_config_mode(
            &bucket, &prefix, &endpoint, reader_dir.path(), mode,
        );
        let cold_vfs_name = unique_vfs_name(&format!("oracle_{}_cold", mode.name()));
        let cold_vfs = TurboliteVfs::new_local(cold_config).expect("cold vfs");
        turbolite::tiered::register(&cold_vfs_name, cold_vfs).expect("register cold");

        let cold = Connection::open_with_flags_and_vfs(
            &format!("oracle_{}_cold.db", mode.name()),
            OpenFlags::SQLITE_OPEN_READ_ONLY,
            &cold_vfs_name,
        )
        .expect("cold open");

        let cold_users = snapshot_table(&cold, "users", "id, name, score");
        assert_eq!(
            v_users, cold_users,
            "[{}] users diverged after cold read",
            mode.name()
        );

        let cold_logs = snapshot_table(&cold, "logs", "id, hex(data)");
        assert_eq!(
            v_logs, cold_logs,
            "[{}] logs diverged after cold read",
            mode.name()
        );

        let integrity: String = cold
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .expect("integrity");
        assert_eq!(
            integrity, "ok",
            "[{}] cold read integrity check failed",
            mode.name()
        );
    }
}

/// Run incremental checkpoint oracle for a given mode.
fn run_oracle_incremental(mode: TestMode) {
    let vanilla_dir = TempDir::new().expect("tempdir");
    let writer_dir = TempDir::new().expect("tempdir");

    let config = test_config_mode(
        &format!("oracle_incr_{}", mode.name()),
        writer_dir.path(),
        mode,
    );
    let vfs_name = unique_vfs_name(&format!("oracle_incr_{}", mode.name()));

    let vfs = TurboliteVfs::new_local(config).expect("vfs");
    turbolite::tiered::register(&vfs_name, vfs).expect("register");

    let v = open_vanilla(&vanilla_dir.path().join("vanilla.db"));
    let t = open_turbolite(
        &format!("oracle_incr_{}.db", mode.name()),
        &vfs_name,
    );

    v.execute_batch("PRAGMA page_size=65536; PRAGMA journal_mode=WAL;")
        .expect("pragma");
    t.execute_batch("PRAGMA page_size=65536; PRAGMA journal_mode=WAL;")
        .expect("pragma");

    let schema = "CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT);";
    v.execute_batch(schema).expect("schema v");
    t.execute_batch(schema).expect("schema t");

    // Batch 1: insert
    for i in 0..100 {
        let k = format!("key_{}", i);
        let val = format!("val_{}", i);
        v.execute("INSERT INTO kv VALUES (?1, ?2)", rusqlite::params![k, val])
            .expect("insert v");
        t.execute("INSERT INTO kv VALUES (?1, ?2)", rusqlite::params![k, val])
            .expect("insert t");
    }
    t.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").expect("cp1");

    let v1 = snapshot_table(&v, "kv", "key, value");
    let t1 = snapshot_table(&t, "kv", "key, value");
    assert_eq!(v1, t1, "[{}] diverged after cp1", mode.name());

    // Batch 2: update + delete + insert
    for i in 0..50 {
        let k = format!("key_{}", i);
        let val = format!("updated_{}", i);
        v.execute("UPDATE kv SET value = ?2 WHERE key = ?1", rusqlite::params![k, val]).expect("upd v");
        t.execute("UPDATE kv SET value = ?2 WHERE key = ?1", rusqlite::params![k, val]).expect("upd t");
    }
    for i in 50..75 {
        let k = format!("key_{}", i);
        v.execute("DELETE FROM kv WHERE key = ?1", rusqlite::params![k]).expect("del v");
        t.execute("DELETE FROM kv WHERE key = ?1", rusqlite::params![k]).expect("del t");
    }
    for i in 100..150 {
        let k = format!("key_{}", i);
        let val = format!("new_{}", i);
        v.execute("INSERT INTO kv VALUES (?1, ?2)", rusqlite::params![k, val]).expect("ins v");
        t.execute("INSERT INTO kv VALUES (?1, ?2)", rusqlite::params![k, val]).expect("ins t");
    }
    t.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").expect("cp2");

    let v2 = snapshot_table(&v, "kv", "key, value");
    let t2 = snapshot_table(&t, "kv", "key, value");
    assert_eq!(v2, t2, "[{}] diverged after cp2", mode.name());

    // Batch 3: vacuum + more writes
    v.execute_batch("VACUUM").expect("vacuum v");
    t.execute_batch("VACUUM").expect("vacuum t");
    t.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").expect("cp3");

    for i in 150..200 {
        let k = format!("key_{}", i);
        let val = format!("post_vacuum_{}", i);
        v.execute("INSERT INTO kv VALUES (?1, ?2)", rusqlite::params![k, val]).expect("ins v");
        t.execute("INSERT INTO kv VALUES (?1, ?2)", rusqlite::params![k, val]).expect("ins t");
    }
    t.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").expect("cp4");

    let vf = snapshot_table(&v, "kv", "key, value");
    let tf = snapshot_table(&t, "kv", "key, value");
    assert_eq!(vf, tf, "[{}] diverged after vacuum+writes", mode.name());

    let integrity: String = t
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity");
    assert_eq!(integrity, "ok", "[{}] integrity failed", mode.name());
}

// --- Parameterized tests: oracle x all mode combinations ---

#[test]
fn oracle_write_checkpoint_cold_read_all_modes() {
    run_across_all_s3(run_oracle_write_checkpoint_cold_read);
}

#[test]
fn oracle_incremental_all_modes() {
    run_across_all_s3(run_oracle_incremental);
}
