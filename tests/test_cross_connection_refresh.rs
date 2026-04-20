use rusqlite::{Connection, OpenFlags};
use turbolite::tiered::{TurboliteVfs, TurboliteConfig};

/// Test refresh when one connection writes and another reads the new data
#[test]
fn test_cross_connection_refresh() {
    let dir = tempfile::tempdir().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().into(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new_local(config).expect("Failed to create VFS");
    turbolite::tiered::register("test_cross", vfs).expect("Failed to register VFS");

    let db_path = dir.path().join("test.db");

    // Create initial database with data
    {
        let conn = Connection::open_with_flags_and_vfs(
            &db_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
            "test_cross",
        ).unwrap();

        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)", []).unwrap();

        for i in 0..500 {
            conn.execute("INSERT INTO test (data) VALUES (?)", (format!("initial_{}", i),)).unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        println!("Initial data created: 500 rows");
    }

    // Open reader connection - it will scan the file and have initial index
    let reader_conn = Connection::open_with_flags_and_vfs(
        &db_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY,
        "test_cross",
    ).unwrap();

    let count: i64 = reader_conn.query_row("SELECT COUNT(*) FROM test", [], |row| row.get(0)).unwrap();
    println!("Reader initial count: {}", count);

    // Now write NEW data with a different connection
    {
        let writer_conn = Connection::open_with_flags_and_vfs(
            &db_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE,
            "test_cross",
        ).unwrap();

        for i in 500..1500 {
            writer_conn.execute("INSERT INTO test (data) VALUES (?)", (format!("new_{}", i),)).unwrap();
        }

        // Checkpoint to move data from WAL to main DB - this is key!
        writer_conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        println!("Writer added 1000 new rows and checkpointed");
    }

    // Force reader to start a new transaction to see checkpointed data
    // In WAL mode, connections maintain transaction snapshots
    println!("\nClosing and reopening reader to start fresh transaction...");
    drop(reader_conn);

    let reader_conn = Connection::open_with_flags_and_vfs(
        &db_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY,
        "test_cross",
    ).unwrap();

    // Now reader should see new data after refresh
    println!("Reader attempting to read new data after checkpoint...");
    let new_count: i64 = reader_conn.query_row("SELECT COUNT(*) FROM test", [], |row| row.get(0)).unwrap();
    println!("Reader new count: {}", new_count);

    // Try reading specific new rows
    for rowid in [501, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400] {
        match reader_conn.query_row(
            "SELECT data FROM test WHERE id = ?",
            [rowid],
            |row| row.get::<_, String>(0),
        ) {
            Ok(data) => println!("  Read row {}: {}", rowid, &data[..20.min(data.len())]),
            Err(e) => println!("  ERROR reading row {}: {}", rowid, e),
        }
    }

    println!("\nTest complete - check for PROFILE summary on connection drop");
}
