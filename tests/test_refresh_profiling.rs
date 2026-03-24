use rusqlite::{Connection, OpenFlags};
use turbolite::{register, CompressedVfs};
use std::path::PathBuf;

/// Simple test to verify refresh profiling works
/// Creates data, then opens new connections that must scan the file
#[test]
fn test_refresh_profiling_simple() {
    let dir = tempfile::tempdir().unwrap();
    let vfs = CompressedVfs::new(dir.path(), 3);
    register("test_refresh_prof", vfs).expect("Failed to register VFS");

    let db_path = dir.path().join("test.db");

    // Create initial database with data
    {
        let conn = Connection::open_with_flags_and_vfs(
            &db_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
            "test_refresh_prof",
        ).unwrap();

        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)", []).unwrap();

        for i in 0..1000 {
            conn.execute("INSERT INTO test (data) VALUES (?)", (format!("data_{}", i),)).unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        println!("Initial data created: 1000 rows");
    }

    // Open 5 new connections - each will scan the file and refresh
    for conn_id in 0..5 {
        let conn = Connection::open_with_flags_and_vfs(
            &db_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY,
            "test_refresh_prof",
        ).unwrap();

        // Read a page to trigger refresh
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM test", [], |row| row.get(0)).unwrap();
        println!("Connection {} read count: {}", conn_id, count);

        // Connection drops here, should print profiling summary
    }

    println!("Test complete");
}
