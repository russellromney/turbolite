//! Integration tests using the VFS with SQLite

use rusqlite::Connection;
use turbolite::tiered::{TurboliteVfs, TurboliteConfig, StorageBackend};

#[test]
fn test_basic_operations() {
    let dir = tempfile::tempdir().unwrap();
    let config = TurboliteConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.path().into(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new(config).expect("Failed to create VFS");
    turbolite::tiered::register("compressed", vfs).expect("Failed to register VFS");

    // Open connection with our VFS
    let conn = Connection::open_with_flags_and_vfs(
        dir.path().join("test.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        "compressed",
    )
    .expect("Failed to open database");

    // Create table
    conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)",
        [],
    )
    .expect("Failed to create table");

    // Insert data
    conn.execute("INSERT INTO test (id, value) VALUES (1, 'hello')", [])
        .expect("Failed to insert");
    conn.execute("INSERT INTO test (id, value) VALUES (2, 'world')", [])
        .expect("Failed to insert");

    // Query data
    let mut stmt = conn.prepare("SELECT id, value FROM test ORDER BY id").unwrap();
    let rows: Vec<(i64, String)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1, "hello".to_string()));
    assert_eq!(rows[1], (2, "world".to_string()));
}

#[test]
fn test_large_data() {
    let dir = tempfile::tempdir().unwrap();
    let config = TurboliteConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.path().into(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new(config).expect("Failed to create VFS");
    turbolite::tiered::register("compressed2", vfs).expect("Failed to register VFS");

    let conn = Connection::open_with_flags_and_vfs(
        dir.path().join("large.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        "compressed2",
    )
    .expect("Failed to open database");

    conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, blob BLOB)", [])
        .expect("Failed to create table");

    // Insert larger blobs
    let large_data = vec![0xABu8; 100_000];
    for i in 0..10 {
        conn.execute(
            "INSERT INTO data (id, blob) VALUES (?1, ?2)",
            rusqlite::params![i, &large_data],
        )
        .expect("Failed to insert");
    }

    // Verify
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM data", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 10);

    // Check blob integrity
    let retrieved: Vec<u8> = conn
        .query_row("SELECT blob FROM data WHERE id = 5", [], |r| r.get(0))
        .unwrap();
    assert_eq!(retrieved, large_data);
}

#[test]
fn test_wal_mode() {
    let dir = tempfile::tempdir().unwrap();
    let config = TurboliteConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.path().into(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new(config).expect("Failed to create VFS");
    turbolite::tiered::register("compressed_wal", vfs).expect("Failed to register VFS");

    let conn = Connection::open_with_flags_and_vfs(
        dir.path().join("wal_test.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        "compressed_wal",
    )
    .expect("Failed to open database");

    // Enable WAL mode
    conn.pragma_update(None, "journal_mode", "WAL")
        .expect("Failed to set WAL mode");

    // Verify WAL mode is set
    let mode: String = conn
        .pragma_query_value(None, "journal_mode", |r| r.get(0))
        .unwrap();
    println!("Journal mode: {}", mode);

    // Create table and insert data
    conn.execute("CREATE TABLE wal_test (id INTEGER PRIMARY KEY, data TEXT)", [])
        .expect("Failed to create table");

    for i in 0..100 {
        conn.execute(
            "INSERT INTO wal_test (id, data) VALUES (?1, ?2)",
            rusqlite::params![i, format!("row_{}", i)],
        )
        .expect("Failed to insert");
    }

    // Verify data
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM wal_test", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 100);

    // Checkpoint to sync WAL to main database
    let _: i64 = conn
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| r.get(0))
        .expect("Checkpoint failed");
}

#[test]
fn test_persistence_with_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let config = TurboliteConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.path().into(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new(config).expect("Failed to create VFS");
    turbolite::tiered::register("compressed_persist", vfs).expect("Failed to register VFS");

    let db_path = dir.path().join("persist.db");

    // Write data
    {
        let conn = Connection::open_with_flags_and_vfs(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            "compressed_persist",
        )
        .unwrap();

        conn.execute("CREATE TABLE persist (value TEXT)", []).unwrap();
        conn.execute("INSERT INTO persist VALUES ('test_value')", []).unwrap();
    }

    // Reopen and verify
    {
        let conn = Connection::open_with_flags_and_vfs(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
            "compressed_persist",
        )
        .unwrap();

        let value: String = conn
            .query_row("SELECT value FROM persist", [], |r| r.get(0))
            .unwrap();
        assert_eq!(value, "test_value");
    }
}

#[test]
fn test_passthrough_mode() {
    let dir = tempfile::tempdir().unwrap();
    let config = TurboliteConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.path().into(),
        compression_level: 0,
        ..Default::default()
    };
    let vfs = TurboliteVfs::new(config).expect("Failed to create VFS");
    turbolite::tiered::register("passthrough_test", vfs).expect("Failed to register VFS");

    let conn = Connection::open_with_flags_and_vfs(
        dir.path().join("passthrough.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        "passthrough_test",
    )
    .unwrap();

    conn.execute("CREATE TABLE test (id INTEGER, data TEXT)", []).unwrap();

    for i in 0..50 {
        conn.execute(
            "INSERT INTO test VALUES (?1, ?2)",
            rusqlite::params![i, format!("data_{}", i)],
        ).unwrap();
    }

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM test", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 50);
}

#[test]
fn test_delete_and_update() {
    let dir = tempfile::tempdir().unwrap();
    let config = TurboliteConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.path().into(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new(config).expect("Failed to create VFS");
    turbolite::tiered::register("integ_delete_update", vfs).expect("Failed to register VFS");

    let conn = Connection::open_with_flags_and_vfs(
        dir.path().join("test.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        "integ_delete_update",
    )
    .expect("Failed to open database");

    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)", []).unwrap();
    for i in 1..=10 {
        conn.execute(
            "INSERT INTO items (id, name, qty) VALUES (?1, ?2, ?3)",
            rusqlite::params![i, format!("item_{}", i), i * 10],
        )
        .unwrap();
    }

    // Update rows 3 and 7
    conn.execute("UPDATE items SET qty = 999 WHERE id IN (3, 7)", []).unwrap();

    // Delete rows 1, 5, 10
    conn.execute("DELETE FROM items WHERE id IN (1, 5, 10)", []).unwrap();

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 7);

    // Verify updated rows
    let qty3: i64 = conn.query_row("SELECT qty FROM items WHERE id = 3", [], |r| r.get(0)).unwrap();
    assert_eq!(qty3, 999);
    let qty7: i64 = conn.query_row("SELECT qty FROM items WHERE id = 7", [], |r| r.get(0)).unwrap();
    assert_eq!(qty7, 999);

    // Verify deleted rows are gone
    let deleted: Vec<i64> = conn
        .prepare("SELECT id FROM items ORDER BY id")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(deleted, vec![2, 3, 4, 6, 7, 8, 9]);

    // Verify an unmodified row
    let qty4: i64 = conn.query_row("SELECT qty FROM items WHERE id = 4", [], |r| r.get(0)).unwrap();
    assert_eq!(qty4, 40);
}

#[test]
fn test_rollback_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let config = TurboliteConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.path().into(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new(config).expect("Failed to create VFS");
    turbolite::tiered::register("integ_rollback", vfs).expect("Failed to register VFS");

    let conn = Connection::open_with_flags_and_vfs(
        dir.path().join("test.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        "integ_rollback",
    )
    .expect("Failed to open database");

    conn.execute("CREATE TABLE rb (id INTEGER PRIMARY KEY, val TEXT)", []).unwrap();

    // BEGIN, INSERT, ROLLBACK
    conn.execute_batch("BEGIN; INSERT INTO rb VALUES (1, 'a'); INSERT INTO rb VALUES (2, 'b'); ROLLBACK;").unwrap();

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM rb", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 0, "ROLLBACK should leave no rows");

    // BEGIN, INSERT, COMMIT
    conn.execute_batch("BEGIN; INSERT INTO rb VALUES (1, 'a'); INSERT INTO rb VALUES (2, 'b'); COMMIT;").unwrap();

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM rb", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 2, "COMMIT should persist rows");

    let val: String = conn.query_row("SELECT val FROM rb WHERE id = 2", [], |r| r.get(0)).unwrap();
    assert_eq!(val, "b");
}

#[test]
fn test_create_index_and_query() {
    let dir = tempfile::tempdir().unwrap();
    let config = TurboliteConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.path().into(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new(config).expect("Failed to create VFS");
    turbolite::tiered::register("integ_index", vfs).expect("Failed to register VFS");

    let conn = Connection::open_with_flags_and_vfs(
        dir.path().join("test.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        "integ_index",
    )
    .expect("Failed to open database");

    conn.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, category TEXT, price REAL)", []).unwrap();
    conn.execute("CREATE INDEX idx_category ON products(category)", []).unwrap();

    let categories = ["electronics", "books", "clothing"];
    for i in 1..=90 {
        conn.execute(
            "INSERT INTO products (id, category, price) VALUES (?1, ?2, ?3)",
            rusqlite::params![i, categories[(i as usize - 1) % 3], i as f64 * 1.5],
        )
        .unwrap();
    }

    // Query using the indexed column
    let elec_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM products WHERE category = 'electronics'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(elec_count, 30);

    let book_prices: Vec<f64> = conn
        .prepare("SELECT price FROM products WHERE category = 'books' ORDER BY price LIMIT 3")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(book_prices, vec![3.0, 7.5, 12.0]);
}

#[test]
fn test_multiple_tables_and_joins() {
    let dir = tempfile::tempdir().unwrap();
    let config = TurboliteConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.path().into(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new(config).expect("Failed to create VFS");
    turbolite::tiered::register("integ_joins", vfs).expect("Failed to register VFS");

    let conn = Connection::open_with_flags_and_vfs(
        dir.path().join("test.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        "integ_joins",
    )
    .expect("Failed to open database");

    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", []).unwrap();
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)", []).unwrap();

    conn.execute("INSERT INTO users VALUES (1, 'Alice')", []).unwrap();
    conn.execute("INSERT INTO users VALUES (2, 'Bob')", []).unwrap();
    conn.execute("INSERT INTO users VALUES (3, 'Charlie')", []).unwrap();

    conn.execute("INSERT INTO orders VALUES (1, 1, 100.0)", []).unwrap();
    conn.execute("INSERT INTO orders VALUES (2, 1, 200.0)", []).unwrap();
    conn.execute("INSERT INTO orders VALUES (3, 2, 50.0)", []).unwrap();

    // JOIN query: total per user
    let results: Vec<(String, f64)> = conn
        .prepare("SELECT u.name, SUM(o.amount) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.id ORDER BY u.name")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0], ("Alice".to_string(), 300.0));
    assert_eq!(results[1], ("Bob".to_string(), 50.0));

    // LEFT JOIN to include Charlie with no orders
    let all_users: Vec<(String, Option<f64>)> = conn
        .prepare("SELECT u.name, SUM(o.amount) FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id ORDER BY u.name")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(all_users.len(), 3);
    assert_eq!(all_users[2], ("Charlie".to_string(), None));
}

#[test]
fn test_alter_table_add_column() {
    let dir = tempfile::tempdir().unwrap();
    let config = TurboliteConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.path().into(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new(config).expect("Failed to create VFS");
    turbolite::tiered::register("integ_alter", vfs).expect("Failed to register VFS");

    let conn = Connection::open_with_flags_and_vfs(
        dir.path().join("test.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        "integ_alter",
    )
    .expect("Failed to open database");

    conn.execute("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)", []).unwrap();
    conn.execute("INSERT INTO people VALUES (1, 'Alice')", []).unwrap();
    conn.execute("INSERT INTO people VALUES (2, 'Bob')", []).unwrap();

    conn.execute("ALTER TABLE people ADD COLUMN age INTEGER", []).unwrap();

    conn.execute("INSERT INTO people VALUES (3, 'Charlie', 30)", []).unwrap();

    // Old rows should have NULL for the new column
    let alice_age: Option<i64> = conn
        .query_row("SELECT age FROM people WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(alice_age, None);

    // New row should have the value
    let charlie_age: Option<i64> = conn
        .query_row("SELECT age FROM people WHERE id = 3", [], |r| r.get(0))
        .unwrap();
    assert_eq!(charlie_age, Some(30));

    // Verify all rows still present
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM people", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 3);
}

#[test]
fn test_page_size_64k() {
    let dir = tempfile::tempdir().unwrap();
    let config = TurboliteConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.path().into(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new(config).expect("Failed to create VFS");
    turbolite::tiered::register("integ_page64k", vfs).expect("Failed to register VFS");

    let conn = Connection::open_with_flags_and_vfs(
        dir.path().join("test.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        "integ_page64k",
    )
    .expect("Failed to open database");

    conn.execute_batch("PRAGMA page_size = 65536;").unwrap();
    conn.execute("CREATE TABLE big (id INTEGER PRIMARY KEY, data BLOB)", []).unwrap();

    let page_size: i64 = conn
        .pragma_query_value(None, "page_size", |r| r.get(0))
        .unwrap();
    assert_eq!(page_size, 65536);

    // Insert enough data for multiple pages (each row ~1KB, 200 rows = ~200KB > 3 pages)
    let blob = vec![0xCDu8; 1024];
    for i in 0..200 {
        conn.execute(
            "INSERT INTO big (id, data) VALUES (?1, ?2)",
            rusqlite::params![i, &blob],
        )
        .unwrap();
    }

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM big", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 200);

    let retrieved: Vec<u8> = conn
        .query_row("SELECT data FROM big WHERE id = 150", [], |r| r.get(0))
        .unwrap();
    assert_eq!(retrieved, blob);
}

#[test]
fn test_page_size_4k() {
    let dir = tempfile::tempdir().unwrap();
    let config = TurboliteConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.path().into(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new(config).expect("Failed to create VFS");
    turbolite::tiered::register("integ_page4k", vfs).expect("Failed to register VFS");

    let conn = Connection::open_with_flags_and_vfs(
        dir.path().join("test.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        "integ_page4k",
    )
    .expect("Failed to open database");

    conn.execute_batch("PRAGMA page_size = 4096;").unwrap();
    conn.execute("CREATE TABLE small (id INTEGER PRIMARY KEY, data TEXT)", []).unwrap();

    let page_size: i64 = conn
        .pragma_query_value(None, "page_size", |r| r.get(0))
        .unwrap();
    assert_eq!(page_size, 4096);

    // Insert enough data to span many 4K pages
    for i in 0..500 {
        conn.execute(
            "INSERT INTO small (id, data) VALUES (?1, ?2)",
            rusqlite::params![i, format!("row_data_{:0>100}", i)],
        )
        .unwrap();
    }

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM small", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 500);

    let val: String = conn
        .query_row("SELECT data FROM small WHERE id = 499", [], |r| r.get(0))
        .unwrap();
    assert_eq!(val, format!("row_data_{:0>100}", 499));
}

#[test]
fn test_vacuum() {
    let dir = tempfile::tempdir().unwrap();
    let config = TurboliteConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.path().into(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new(config).expect("Failed to create VFS");
    turbolite::tiered::register("integ_vacuum", vfs).expect("Failed to register VFS");

    let db_path = dir.path().join("test.db");

    let conn = Connection::open_with_flags_and_vfs(
        &db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        "integ_vacuum",
    )
    .expect("Failed to open database");

    conn.execute("CREATE TABLE vac (id INTEGER PRIMARY KEY, data TEXT)", []).unwrap();
    for i in 0..1000 {
        conn.execute(
            "INSERT INTO vac VALUES (?1, ?2)",
            rusqlite::params![i, format!("padding_{:0>200}", i)],
        )
        .unwrap();
    }

    // Delete half
    conn.execute("DELETE FROM vac WHERE id >= 500", []).unwrap();

    // VACUUM
    conn.execute_batch("VACUUM;").unwrap();

    // Verify remaining data
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM vac", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 500);

    let val: String = conn
        .query_row("SELECT data FROM vac WHERE id = 0", [], |r| r.get(0))
        .unwrap();
    assert_eq!(val, format!("padding_{:0>200}", 0));

    // Close and reopen to verify not corrupted
    drop(conn);

    let conn2 = Connection::open_with_flags_and_vfs(
        &db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
        "integ_vacuum",
    )
    .unwrap();

    let count2: i64 = conn2.query_row("SELECT COUNT(*) FROM vac", [], |r| r.get(0)).unwrap();
    assert_eq!(count2, 500);

    // Integrity check
    let integrity: String = conn2
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    assert_eq!(integrity, "ok");
}

#[test]
fn test_savepoint_release_and_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let config = TurboliteConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.path().into(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new(config).expect("Failed to create VFS");
    turbolite::tiered::register("integ_savepoint", vfs).expect("Failed to register VFS");

    let conn = Connection::open_with_flags_and_vfs(
        dir.path().join("test.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        "integ_savepoint",
    )
    .expect("Failed to open database");

    conn.execute("CREATE TABLE sp (id INTEGER PRIMARY KEY, val TEXT)", []).unwrap();

    conn.execute_batch("SAVEPOINT sp1;").unwrap();
    conn.execute("INSERT INTO sp VALUES (1, 'first')", []).unwrap();

    conn.execute_batch("SAVEPOINT sp2;").unwrap();
    conn.execute("INSERT INTO sp VALUES (2, 'second')", []).unwrap();

    conn.execute_batch("ROLLBACK TO sp2;").unwrap();
    conn.execute_batch("RELEASE sp1;").unwrap();

    // Only the first INSERT should exist
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM sp", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 1);

    let val: String = conn
        .query_row("SELECT val FROM sp WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(val, "first");

    // Verify second row is gone
    let missing = conn.query_row("SELECT val FROM sp WHERE id = 2", [], |r| r.get::<_, String>(0));
    assert!(missing.is_err(), "Row 2 should not exist after ROLLBACK TO sp2");
}

#[test]
fn test_large_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let config = TurboliteConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.path().into(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new(config).expect("Failed to create VFS");
    turbolite::tiered::register("integ_large_tx", vfs).expect("Failed to register VFS");

    let conn = Connection::open_with_flags_and_vfs(
        dir.path().join("test.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        "integ_large_tx",
    )
    .expect("Failed to open database");

    conn.execute("CREATE TABLE bulk (id INTEGER PRIMARY KEY, payload TEXT)", []).unwrap();

    conn.execute_batch("BEGIN;").unwrap();
    for i in 0..10_000 {
        conn.execute(
            "INSERT INTO bulk VALUES (?1, ?2)",
            rusqlite::params![i, format!("payload_{}", i)],
        )
        .unwrap();
    }
    conn.execute_batch("COMMIT;").unwrap();

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM bulk", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 10_000);

    // Check random rows for integrity
    for check_id in [0, 999, 4567, 9999] {
        let payload: String = conn
            .query_row(
                "SELECT payload FROM bulk WHERE id = ?1",
                rusqlite::params![check_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(payload, format!("payload_{}", check_id));
    }
}

#[test]
fn test_checkpoint_truncate() {
    let dir = tempfile::tempdir().unwrap();
    let config = TurboliteConfig {
        storage_backend: StorageBackend::Local,
        cache_dir: dir.path().into(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new(config).expect("Failed to create VFS");
    turbolite::tiered::register("integ_checkpoint", vfs).expect("Failed to register VFS");

    let db_path = dir.path().join("test.db");

    {
        let conn = Connection::open_with_flags_and_vfs(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            "integ_checkpoint",
        )
        .expect("Failed to open database");

        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        let mode: String = conn
            .pragma_query_value(None, "journal_mode", |r| r.get(0))
            .unwrap();
        assert!(
            mode.eq_ignore_ascii_case("wal"),
            "Expected WAL mode, got {}",
            mode
        );

        conn.execute("CREATE TABLE ckpt (id INTEGER PRIMARY KEY, data TEXT)", []).unwrap();
        for i in 0..100 {
            conn.execute(
                "INSERT INTO ckpt VALUES (?1, ?2)",
                rusqlite::params![i, format!("ckpt_{}", i)],
            )
            .unwrap();
        }

        // Checkpoint with TRUNCATE
        let _: i64 = conn
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| r.get(0))
            .expect("Checkpoint failed");

        // Verify data after checkpoint
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM ckpt", [], |r| r.get(0)).unwrap();
        assert_eq!(count, 100);
    }

    // Reopen and verify persistence
    {
        let conn = Connection::open_with_flags_and_vfs(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
            "integ_checkpoint",
        )
        .unwrap();

        let count: i64 = conn.query_row("SELECT COUNT(*) FROM ckpt", [], |r| r.get(0)).unwrap();
        assert_eq!(count, 100);

        let val: String = conn
            .query_row("SELECT data FROM ckpt WHERE id = 50", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "ckpt_50");
    }
}
