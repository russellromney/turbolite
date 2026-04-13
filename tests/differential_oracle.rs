//! Differential oracle: compare vanilla SQLite against turbolite VFS.
//!
//! The single most valuable correctness test. Runs identical workloads on both
//! and asserts byte-identical results. Any divergence is a turbolite bug.

use proptest::prelude::*;
use rusqlite::{Connection, OpenFlags};
use std::sync::atomic::{AtomicU64, Ordering};
use turbolite::tiered::{StorageBackend, TurboliteConfig, TurboliteVfs};

static VFS_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Register a fresh turbolite VFS with a unique name. Returns the name.
fn register_fresh_vfs(dir: &std::path::Path) -> String {
    let id = VFS_COUNTER.fetch_add(1, Ordering::Relaxed);
    let name = format!("oracle_{}", id);
    let config = TurboliteConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.into(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new(config).expect("failed to create VFS");
    turbolite::tiered::register(&name, vfs).expect("failed to register VFS");
    name
}

/// Open a vanilla SQLite connection (default VFS).
fn open_vanilla(path: &std::path::Path) -> Connection {
    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
    )
    .expect("failed to open vanilla SQLite")
}

/// Open a turbolite-backed connection.
fn open_turbolite(path: &std::path::Path, vfs_name: &str) -> Connection {
    Connection::open_with_flags_and_vfs(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        vfs_name,
    )
    .expect("failed to open turbolite SQLite")
}

// ---------------------------------------------------------------------------
// SQL operation types for the workload generator
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum SqlOp {
    Insert { id: i64, text: String, real_val: f64 },
    Update { id: i64, text: String },
    Delete { id: i64 },
    InsertBlob { id: i64, data: Vec<u8> },
}

/// Generate a random SQL operation.
fn sql_op_strategy() -> impl Strategy<Value = SqlOp> {
    prop_oneof![
        // INSERT with text
        (1..10_000i64, "[a-zA-Z0-9 ]{0,200}", any::<f64>()).prop_map(
            |(id, text, real_val)| SqlOp::Insert {
                id: id.abs(),
                text,
                real_val,
            }
        ),
        // UPDATE
        (1..10_000i64, "[a-zA-Z0-9 ]{0,200}")
            .prop_map(|(id, text)| SqlOp::Update {
                id: id.abs(),
                text,
            }),
        // DELETE
        (1..10_000i64).prop_map(|id| SqlOp::Delete { id: id.abs() }),
        // INSERT with blob
        (1..10_000i64, proptest::collection::vec(any::<u8>(), 0..1024))
            .prop_map(|(id, data)| SqlOp::InsertBlob {
                id: id.abs(),
                data,
            }),
    ]
}

/// Execute a SqlOp on a connection. Returns Ok if the op succeeded, or the
/// error string if it failed. Both connections must produce the same result.
fn execute_op(conn: &Connection, op: &SqlOp) -> Result<usize, String> {
    match op {
        SqlOp::Insert { id, text, real_val } => conn
            .execute(
                "INSERT OR REPLACE INTO test_data (id, text_col, real_col) VALUES (?1, ?2, ?3)",
                rusqlite::params![id, text, real_val],
            )
            .map_err(|e| e.to_string()),
        SqlOp::Update { id, text } => conn
            .execute(
                "UPDATE test_data SET text_col = ?2 WHERE id = ?1",
                rusqlite::params![id, text],
            )
            .map_err(|e| e.to_string()),
        SqlOp::Delete { id } => conn
            .execute(
                "DELETE FROM test_data WHERE id = ?1",
                rusqlite::params![id],
            )
            .map_err(|e| e.to_string()),
        SqlOp::InsertBlob { id, data } => conn
            .execute(
                "INSERT OR REPLACE INTO blob_data (id, blob_col) VALUES (?1, ?2)",
                rusqlite::params![id, data],
            )
            .map_err(|e| e.to_string()),
    }
}

/// Initialize the schema on a connection.
fn init_schema(conn: &Connection) {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS test_data (
            id INTEGER PRIMARY KEY,
            text_col TEXT,
            real_col REAL
        );
        CREATE TABLE IF NOT EXISTS blob_data (
            id INTEGER PRIMARY KEY,
            blob_col BLOB
        );",
    )
    .expect("failed to create schema");
}

/// Snapshot all data from both tables, returned as sorted Vec of debug strings.
/// This is the comparison point: if these differ, the VFS changed semantics.
fn snapshot(conn: &Connection) -> (Vec<String>, Vec<String>) {
    let text_rows = {
        let mut stmt = conn
            .prepare("SELECT id, text_col, real_col FROM test_data ORDER BY id")
            .expect("failed to prepare snapshot query");
        stmt.query_map([], |row| {
            let id: i64 = row.get(0)?;
            let text: Option<String> = row.get(1)?;
            let real_val: Option<f64> = row.get(2)?;
            Ok(format!("{}|{:?}|{:?}", id, text, real_val))
        })
        .expect("failed to query")
        .collect::<Result<Vec<_>, _>>()
        .expect("failed to collect rows")
    };

    let blob_rows = {
        let mut stmt = conn
            .prepare("SELECT id, length(blob_col), hex(blob_col) FROM blob_data ORDER BY id")
            .expect("failed to prepare blob snapshot");
        stmt.query_map([], |row| {
            let id: i64 = row.get(0)?;
            let len: Option<i64> = row.get(1)?;
            let hex: Option<String> = row.get(2)?;
            Ok(format!("{}|{:?}|{:?}", id, len, hex))
        })
        .expect("failed to query blobs")
        .collect::<Result<Vec<_>, _>>()
        .expect("failed to collect blob rows")
    };

    (text_rows, blob_rows)
}

/// Run integrity check on a connection. Panics if it fails.
fn assert_integrity(conn: &Connection, label: &str) {
    let result: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("failed to run integrity_check");
    assert_eq!(
        result, "ok",
        "{} integrity_check failed: {}",
        label, result
    );
}

/// Compare schemas between two connections.
fn assert_schema_match(vanilla: &Connection, turbo: &Connection) {
    let get_schema = |conn: &Connection| -> Vec<String> {
        let mut stmt = conn
            .prepare("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name")
            .expect("failed to query schema");
        stmt.query_map([], |row| row.get(0))
            .expect("failed to query")
            .collect::<Result<Vec<String>, _>>()
            .expect("failed to collect")
    };
    let v_schema = get_schema(vanilla);
    let t_schema = get_schema(turbo);
    assert_eq!(v_schema, t_schema, "schemas diverged");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    /// Core differential oracle: random operations, compare after every batch.
    #[test]
    fn oracle_random_ops(ops in proptest::collection::vec(sql_op_strategy(), 10..200)) {
        let vanilla_dir = tempfile::tempdir().expect("tempdir");
        let turbo_dir = tempfile::tempdir().expect("tempdir");

        let vfs_name = register_fresh_vfs(turbo_dir.path());
        let v_conn = open_vanilla(&vanilla_dir.path().join("vanilla.db"));
        let t_conn = open_turbolite(&turbo_dir.path().join("turbo.db"), &vfs_name);

        init_schema(&v_conn);
        init_schema(&t_conn);

        // Execute each operation on both connections
        for op in &ops {
            let v_result = execute_op(&v_conn, op);
            let t_result = execute_op(&t_conn, op);

            // Both must succeed or both must fail
            match (&v_result, &t_result) {
                (Ok(v_rows), Ok(t_rows)) => {
                    prop_assert_eq!(v_rows, t_rows, "row count diverged on {:?}", op);
                }
                (Err(_), Err(_)) => {
                    // Both failed, that's fine
                }
                _ => {
                    prop_assert!(
                        false,
                        "result diverged: vanilla={:?}, turbo={:?}, op={:?}",
                        v_result,
                        t_result,
                        op
                    );
                }
            }
        }

        // Compare final state
        let (v_text, v_blob) = snapshot(&v_conn);
        let (t_text, t_blob) = snapshot(&t_conn);
        prop_assert_eq!(&v_text, &t_text, "text_data rows diverged after {} ops", ops.len());
        prop_assert_eq!(&v_blob, &t_blob, "blob_data rows diverged after {} ops", ops.len());

        // Both must pass integrity check
        assert_integrity(&v_conn, "vanilla");
        assert_integrity(&t_conn, "turbolite");

        // Schemas must match
        assert_schema_match(&v_conn, &t_conn);
    }

    /// Oracle with WAL checkpoint between batches.
    #[test]
    fn oracle_with_checkpoint(
        batches in proptest::collection::vec(
            proptest::collection::vec(sql_op_strategy(), 5..50),
            2..10,
        )
    ) {
        let vanilla_dir = tempfile::tempdir().expect("tempdir");
        let turbo_dir = tempfile::tempdir().expect("tempdir");

        let vfs_name = register_fresh_vfs(turbo_dir.path());
        let v_conn = open_vanilla(&vanilla_dir.path().join("vanilla.db"));
        let t_conn = open_turbolite(&turbo_dir.path().join("turbo.db"), &vfs_name);

        // Use WAL mode on both
        v_conn.execute_batch("PRAGMA journal_mode=WAL").expect("wal vanilla");
        t_conn.execute_batch("PRAGMA journal_mode=WAL").expect("wal turbo");

        init_schema(&v_conn);
        init_schema(&t_conn);

        for (batch_idx, batch) in batches.iter().enumerate() {
            for op in batch {
                let v_result = execute_op(&v_conn, op);
                let t_result = execute_op(&t_conn, op);
                match (&v_result, &t_result) {
                    (Ok(v), Ok(t)) => prop_assert_eq!(v, t),
                    (Err(_), Err(_)) => {}
                    _ => prop_assert!(false, "diverged on {:?}", op),
                }
            }

            // Checkpoint after each batch
            v_conn
                .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
                .expect("checkpoint vanilla");
            t_conn
                .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
                .expect("checkpoint turbo");

            // Compare after each checkpoint
            let (v_text, v_blob) = snapshot(&v_conn);
            let (t_text, t_blob) = snapshot(&t_conn);
            prop_assert_eq!(
                &v_text, &t_text,
                "text rows diverged after batch {}",
                batch_idx
            );
            prop_assert_eq!(
                &v_blob, &t_blob,
                "blob rows diverged after batch {}",
                batch_idx
            );
        }

        assert_integrity(&v_conn, "vanilla");
        assert_integrity(&t_conn, "turbolite");
    }
}

/// Deterministic oracle with DDL: CREATE INDEX, ALTER TABLE, DROP TABLE.
#[test]
fn oracle_ddl_operations() {
    let vanilla_dir = tempfile::tempdir().expect("tempdir");
    let turbo_dir = tempfile::tempdir().expect("tempdir");

    let vfs_name = register_fresh_vfs(turbo_dir.path());
    let v = open_vanilla(&vanilla_dir.path().join("vanilla.db"));
    let t = open_turbolite(&turbo_dir.path().join("turbo.db"), &vfs_name);

    let ddl_ops = [
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
        "INSERT INTO users VALUES (1, 'Alice', 30)",
        "INSERT INTO users VALUES (2, 'Bob', 25)",
        "INSERT INTO users VALUES (3, 'Charlie', 35)",
        "CREATE INDEX idx_age ON users(age)",
        "INSERT INTO users VALUES (4, 'Diana', 28)",
        "ALTER TABLE users ADD COLUMN email TEXT",
        "UPDATE users SET email = 'alice@test.com' WHERE id = 1",
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)",
        "INSERT INTO orders VALUES (1, 1, 99.99)",
        "INSERT INTO orders VALUES (2, 2, 49.50)",
        "INSERT INTO orders VALUES (3, 1, 25.00)",
        "CREATE INDEX idx_user_orders ON orders(user_id)",
        "DELETE FROM users WHERE id = 3",
        "DROP TABLE orders",
    ];

    for sql in &ddl_ops {
        let v_result = v.execute_batch(sql);
        let t_result = t.execute_batch(sql);
        match (&v_result, &t_result) {
            (Ok(_), Ok(_)) => {}
            (Err(ve), Err(te)) => {
                assert_eq!(
                    ve.to_string(),
                    te.to_string(),
                    "error messages differ for: {}",
                    sql
                );
            }
            _ => panic!(
                "diverged on '{}': vanilla={:?}, turbo={:?}",
                sql, v_result, t_result
            ),
        }
    }

    // Compare final state
    let v_rows: Vec<String> = {
        let mut stmt = v
            .prepare("SELECT id, name, age, email FROM users ORDER BY id")
            .expect("prepare");
        stmt.query_map([], |row| {
            Ok(format!(
                "{}|{:?}|{:?}|{:?}",
                row.get::<_, i64>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, Option<String>>(3)?
            ))
        })
        .expect("query")
        .collect::<Result<_, _>>()
        .expect("collect")
    };

    let t_rows: Vec<String> = {
        let mut stmt = t
            .prepare("SELECT id, name, age, email FROM users ORDER BY id")
            .expect("prepare");
        stmt.query_map([], |row| {
            Ok(format!(
                "{}|{:?}|{:?}|{:?}",
                row.get::<_, i64>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, Option<String>>(3)?
            ))
        })
        .expect("query")
        .collect::<Result<_, _>>()
        .expect("collect")
    };

    assert_eq!(v_rows, t_rows, "user rows diverged after DDL ops");
    assert_schema_match(&v, &t);
    assert_integrity(&v, "vanilla");
    assert_integrity(&t, "turbolite");
}

/// Oracle with transactions and rollbacks.
#[test]
fn oracle_transactions() {
    let vanilla_dir = tempfile::tempdir().expect("tempdir");
    let turbo_dir = tempfile::tempdir().expect("tempdir");

    let vfs_name = register_fresh_vfs(turbo_dir.path());
    let v = open_vanilla(&vanilla_dir.path().join("vanilla.db"));
    let t = open_turbolite(&turbo_dir.path().join("turbo.db"), &vfs_name);

    init_schema(&v);
    init_schema(&t);

    // Committed transaction
    for conn in [&v, &t] {
        conn.execute_batch("BEGIN").expect("begin");
        conn.execute(
            "INSERT INTO test_data (id, text_col, real_col) VALUES (1, 'committed', 1.0)",
            [],
        )
        .expect("insert");
        conn.execute(
            "INSERT INTO test_data (id, text_col, real_col) VALUES (2, 'also committed', 2.0)",
            [],
        )
        .expect("insert");
        conn.execute_batch("COMMIT").expect("commit");
    }

    // Rolled-back transaction
    for conn in [&v, &t] {
        conn.execute_batch("BEGIN").expect("begin");
        conn.execute(
            "INSERT INTO test_data (id, text_col, real_col) VALUES (3, 'rolled back', 3.0)",
            [],
        )
        .expect("insert");
        conn.execute_batch("ROLLBACK").expect("rollback");
    }

    // Another committed transaction
    for conn in [&v, &t] {
        conn.execute_batch("BEGIN").expect("begin");
        conn.execute(
            "INSERT INTO test_data (id, text_col, real_col) VALUES (4, 'after rollback', 4.0)",
            [],
        )
        .expect("insert");
        conn.execute_batch("COMMIT").expect("commit");
    }

    let (v_text, _) = snapshot(&v);
    let (t_text, _) = snapshot(&t);
    assert_eq!(v_text, t_text, "transaction semantics diverged");

    // Should have rows 1, 2, 4 (not 3)
    assert_eq!(v_text.len(), 3);
    assert!(v_text[0].starts_with("1|"));
    assert!(v_text[1].starts_with("2|"));
    assert!(v_text[2].starts_with("4|"));
}

/// Oracle with VACUUM (reshuffles all pages).
#[test]
fn oracle_vacuum() {
    let vanilla_dir = tempfile::tempdir().expect("tempdir");
    let turbo_dir = tempfile::tempdir().expect("tempdir");

    let vfs_name = register_fresh_vfs(turbo_dir.path());
    let v = open_vanilla(&vanilla_dir.path().join("vanilla.db"));
    let t = open_turbolite(&turbo_dir.path().join("turbo.db"), &vfs_name);

    init_schema(&v);
    init_schema(&t);

    // Insert a bunch of data
    for i in 0..500 {
        let sql = format!(
            "INSERT INTO test_data (id, text_col, real_col) VALUES ({}, 'row_{}', {})",
            i,
            i,
            i as f64 * 1.1
        );
        v.execute_batch(&sql).expect("insert vanilla");
        t.execute_batch(&sql).expect("insert turbo");
    }

    // Delete half
    v.execute_batch("DELETE FROM test_data WHERE id % 2 = 0")
        .expect("delete vanilla");
    t.execute_batch("DELETE FROM test_data WHERE id % 2 = 0")
        .expect("delete turbo");

    // VACUUM reshuffles all pages
    v.execute_batch("VACUUM").expect("vacuum vanilla");
    t.execute_batch("VACUUM").expect("vacuum turbo");

    // Insert more after vacuum
    for i in 500..600 {
        let sql = format!(
            "INSERT INTO test_data (id, text_col, real_col) VALUES ({}, 'post_vacuum_{}', {})",
            i,
            i,
            i as f64 * 2.2
        );
        v.execute_batch(&sql).expect("insert vanilla");
        t.execute_batch(&sql).expect("insert turbo");
    }

    let (v_text, _) = snapshot(&v);
    let (t_text, _) = snapshot(&t);
    assert_eq!(v_text, t_text, "rows diverged after VACUUM");
    assert_integrity(&v, "vanilla");
    assert_integrity(&t, "turbolite");
}
