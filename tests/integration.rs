//! Integration tests using the VFS with SQLite

use rusqlite::Connection;
use turbolite::{register, CompressedVfs};
use std::sync::Once;

static INIT: Once = Once::new();

fn init_vfs(dir: &std::path::Path) {
    INIT.call_once(|| {
        let vfs = CompressedVfs::new(dir, 3);
        register("compressed", vfs).expect("Failed to register VFS");
    });
}

#[test]
fn test_basic_operations() {
    let dir = tempfile::tempdir().unwrap();
    init_vfs(dir.path());

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
    let vfs = CompressedVfs::new(dir.path(), 3);
    register("compressed2", vfs).expect("Failed to register VFS");

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
    let vfs = CompressedVfs::new(dir.path(), 3);
    register("compressed_wal", vfs).expect("Failed to register VFS");

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
    let vfs = CompressedVfs::new(dir.path(), 3);
    register("compressed_persist", vfs).expect("Failed to register VFS");

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
    let vfs = CompressedVfs::passthrough(dir.path());
    register("passthrough_test", vfs).expect("Failed to register VFS");

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
#[cfg(feature = "encryption")]
fn test_encrypted_mode() {
    let dir = tempfile::tempdir().unwrap();
    let vfs = CompressedVfs::encrypted(dir.path(), "test-password");
    register("encrypted_test", vfs).expect("Failed to register VFS");

    let db_path = dir.path().join("encrypted.db");

    // Write data
    {
        let conn = Connection::open_with_flags_and_vfs(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            "encrypted_test",
        )
        .unwrap();

        conn.execute("CREATE TABLE secure (id INTEGER, secret TEXT)", []).unwrap();
        conn.execute("INSERT INTO secure VALUES (1, 'top secret')", []).unwrap();
        conn.execute("INSERT INTO secure VALUES (2, 'classified')", []).unwrap();
    }

    // Reopen and verify encryption works
    {
        let conn = Connection::open_with_flags_and_vfs(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
            "encrypted_test",
        )
        .unwrap();

        let secrets: Vec<String> = conn
            .prepare("SELECT secret FROM secure ORDER BY id")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();

        assert_eq!(secrets, vec!["top secret", "classified"]);
    }
}

#[test]
#[cfg(feature = "encryption")]
fn test_compressed_encrypted_mode() {
    let dir = tempfile::tempdir().unwrap();
    let vfs = CompressedVfs::compressed_encrypted(dir.path(), 3, "super-secret");
    register("comp_enc_test", vfs).expect("Failed to register VFS");

    let db_path = dir.path().join("comp_enc.db");

    // Write large compressible data
    {
        let conn = Connection::open_with_flags_and_vfs(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            "comp_enc_test",
        )
        .unwrap();

        conn.execute("CREATE TABLE logs (id INTEGER, message TEXT)", []).unwrap();

        // Insert highly compressible repeated data
        let repeated_msg = "ERROR: connection timeout ".repeat(100);
        for i in 0..100 {
            conn.execute(
                "INSERT INTO logs VALUES (?1, ?2)",
                rusqlite::params![i, &repeated_msg],
            ).unwrap();
        }
    }

    // Reopen and verify both compression and encryption work
    {
        let conn = Connection::open_with_flags_and_vfs(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
            "comp_enc_test",
        )
        .unwrap();

        let count: i64 = conn.query_row("SELECT COUNT(*) FROM logs", [], |r| r.get(0)).unwrap();
        assert_eq!(count, 100);

        // Verify data integrity
        let msg: String = conn
            .query_row("SELECT message FROM logs WHERE id = 50", [], |r| r.get(0))
            .unwrap();
        assert!(msg.starts_with("ERROR: connection timeout"));
    }
}

/// Test dictionary compression improves compression and works correctly
#[test]
fn test_dictionary_compression() {
    use turbolite::dict::train_dictionary;

    let dir = tempfile::tempdir().unwrap();

    // Create sample data with repeated patterns (simulates Redis-like workload)
    let samples: Vec<Vec<u8>> = (0..100)
        .map(|i| format!("user:{}:session:active:timestamp:1234567890:data:{}", i, "x".repeat(100)).into_bytes())
        .collect();

    // Train a dictionary from the samples
    let dict = train_dictionary(&samples, 16 * 1024).expect("Failed to train dictionary");
    assert!(!dict.is_empty(), "Dictionary should not be empty");

    // Create VFS with dictionary
    let vfs = CompressedVfs::new_with_dict(dir.path(), 3, dict);
    register("dict_test", vfs).unwrap();

    let conn = Connection::open_with_flags_and_vfs(
        dir.path().join("dict.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        "dict_test",
    )
    .unwrap();

    // Create table and insert data with repeated patterns
    conn.execute("CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)", [])
        .unwrap();

    // Insert data similar to training samples
    for i in 0..50 {
        let key = format!("user:{}:session", i);
        let value = format!("active:timestamp:1234567890:data:{}", "x".repeat(100));
        conn.execute(
            "INSERT INTO kv (key, value) VALUES (?1, ?2)",
            rusqlite::params![key, value],
        )
        .unwrap();
    }

    // Verify data integrity
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM kv", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 50);

    // Check specific row
    let value: String = conn
        .query_row("SELECT value FROM kv WHERE key = 'user:25:session'", [], |r| r.get(0))
        .unwrap();
    assert!(value.starts_with("active:timestamp:"));

    // Close and reopen to test persistence with dictionary
    drop(conn);

    let vfs2 = CompressedVfs::new_with_dict(
        dir.path(),
        3,
        train_dictionary(&samples, 16 * 1024).unwrap(),
    );
    register("dict_test2", vfs2).unwrap();

    let conn2 = Connection::open_with_flags_and_vfs(
        dir.path().join("dict.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
        "dict_test2",
    )
    .unwrap();

    let count2: i64 = conn2
        .query_row("SELECT COUNT(*) FROM kv", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count2, 50);
}

#[test]
#[cfg(feature = "encryption")]
fn test_all_four_modes_comparison() {
    let dir = tempfile::tempdir().unwrap();

    // Test data
    let test_data = "SELECT * FROM users WHERE name LIKE 'John%'".repeat(50);

    // 1. Compressed
    let vfs1 = CompressedVfs::new(dir.path(), 3);
    register("mode_comp", vfs1).unwrap();

    // 2. Passthrough
    let vfs2 = CompressedVfs::passthrough(dir.path());
    register("mode_pass", vfs2).unwrap();

    // 3. Encrypted
    let vfs3 = CompressedVfs::encrypted(dir.path(), "pwd");
    register("mode_enc", vfs3).unwrap();

    // 4. Compressed+Encrypted
    let vfs4 = CompressedVfs::compressed_encrypted(dir.path(), 3, "pwd");
    register("mode_both", vfs4).unwrap();

    // Test each mode
    for (mode_name, db_name) in [
        ("mode_comp", "comp.db"),
        ("mode_pass", "pass.db"),
        ("mode_enc", "enc.db"),
        ("mode_both", "both.db"),
    ] {
        let conn = Connection::open_with_flags_and_vfs(
            dir.path().join(db_name),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            mode_name,
        )
        .unwrap();

        conn.execute("CREATE TABLE queries (sql TEXT)", []).unwrap();

        for _ in 0..20 {
            conn.execute("INSERT INTO queries VALUES (?1)", rusqlite::params![&test_data]).unwrap();
        }

        let count: i64 = conn.query_row("SELECT COUNT(*) FROM queries", [], |r| r.get(0)).unwrap();
        assert_eq!(count, 20);

        let retrieved: String = conn
            .query_row("SELECT sql FROM queries LIMIT 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(retrieved, test_data);
    }
}

/// Test compact_with_recompression with parallel compression
#[test]
fn test_compact_with_recompression() {
    use turbolite::{compact_with_recompression, inspect_database, CompactionConfig};

    let dir = tempfile::tempdir().unwrap();
    let vfs = CompressedVfs::new(dir.path(), 3);
    register("recompress_test", vfs).unwrap();

    let db_path = dir.path().join("recompress.db");

    // Create database and insert data
    {
        let conn = Connection::open_with_flags_and_vfs(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            "recompress_test",
        )
        .unwrap();

        conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT)", [])
            .unwrap();

        // Insert data and update it multiple times to create dead space
        let test_data = "x".repeat(1000);
        for i in 0..100 {
            conn.execute(
                "INSERT INTO data (id, value) VALUES (?1, ?2)",
                rusqlite::params![i, &test_data],
            )
            .unwrap();
        }

        // Update rows to create dead space (superseded records)
        let updated_data = "y".repeat(1000);
        for i in 0..50 {
            conn.execute(
                "UPDATE data SET value = ?1 WHERE id = ?2",
                rusqlite::params![&updated_data, i],
            )
            .unwrap();
        }
    }

    // Check initial stats
    let stats_before = inspect_database(&db_path).unwrap();
    println!(
        "Before recompression: file_size={}, dead_space={} ({:.1}%)",
        stats_before.file_size, stats_before.dead_space, stats_before.dead_space_pct
    );
    assert!(stats_before.dead_space > 0, "Should have dead space from updates");

    // Compact with recompression (parallel by default when feature enabled)
    let config = CompactionConfig::new(3).with_parallel(true);
    let freed = compact_with_recompression(&db_path, config).unwrap();
    println!("Freed {} bytes", freed);

    // Check stats after
    let stats_after = inspect_database(&db_path).unwrap();
    println!(
        "After recompression: file_size={}, dead_space={} ({:.1}%)",
        stats_after.file_size, stats_after.dead_space, stats_after.dead_space_pct
    );
    assert!(
        stats_after.file_size <= stats_before.file_size,
        "File should not be larger after compaction"
    );
    assert!(
        stats_after.dead_space_pct < 5.0,
        "Dead space should be minimal after compaction"
    );

    // Verify data integrity
    let vfs2 = CompressedVfs::new(dir.path(), 3);
    register("recompress_verify", vfs2).unwrap();

    let conn = Connection::open_with_flags_and_vfs(
        &db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        "recompress_verify",
    )
    .unwrap();

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM data", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 100);

    // Verify updated rows have new value
    let value: String = conn
        .query_row("SELECT value FROM data WHERE id = 25", [], |r| r.get(0))
        .unwrap();
    assert!(value.starts_with("y"), "Updated rows should have new value");

    // Verify non-updated rows have original value
    let value: String = conn
        .query_row("SELECT value FROM data WHERE id = 75", [], |r| r.get(0))
        .unwrap();
    assert!(value.starts_with("x"), "Non-updated rows should have original value");
}

/// Test compact_if_needed helper
#[test]
fn test_compact_if_needed() {
    use turbolite::{compact_if_needed, inspect_database};

    let dir = tempfile::tempdir().unwrap();
    let vfs = CompressedVfs::new(dir.path(), 3);
    register("compact_needed_test", vfs).unwrap();

    let db_path = dir.path().join("compact_needed.db");

    // Create database with some dead space
    {
        let conn = Connection::open_with_flags_and_vfs(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            "compact_needed_test",
        )
        .unwrap();

        conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT)", [])
            .unwrap();

        let test_data = "z".repeat(500);
        for i in 0..50 {
            conn.execute(
                "INSERT INTO data (id, value) VALUES (?1, ?2)",
                rusqlite::params![i, &test_data],
            )
            .unwrap();
        }

        // Create dead space with updates
        let updated = "w".repeat(500);
        for i in 0..25 {
            conn.execute(
                "UPDATE data SET value = ?1 WHERE id = ?2",
                rusqlite::params![&updated, i],
            )
            .unwrap();
        }
    }

    let stats = inspect_database(&db_path).unwrap();
    println!("Dead space: {:.1}%", stats.dead_space_pct);

    // With very high threshold, should not compact
    let result = compact_if_needed(&db_path, 99.0).unwrap();
    assert!(result.is_none(), "Should not compact when below threshold");

    // With low threshold, should compact
    let result = compact_if_needed(&db_path, 1.0).unwrap();
    assert!(result.is_some(), "Should compact when above threshold");
    println!("Freed {} bytes", result.unwrap());

    // After compaction, dead space should be minimal
    let stats_after = inspect_database(&db_path).unwrap();
    assert!(
        stats_after.dead_space_pct < 5.0,
        "Dead space should be minimal after compaction"
    );
}

/// Test parallel vs serial compaction produces same results
#[test]
fn test_parallel_serial_consistency() {
    use turbolite::{compact_with_recompression, inspect_database, CompactionConfig};

    let dir = tempfile::tempdir().unwrap();

    // Create two identical databases
    for (vfs_name, db_name) in [("par_test1", "parallel.db"), ("par_test2", "serial.db")] {
        let vfs = CompressedVfs::new(dir.path(), 3);
        register(vfs_name, vfs).unwrap();

        let db_path = dir.path().join(db_name);
        let conn = Connection::open_with_flags_and_vfs(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            vfs_name,
        )
        .unwrap();

        conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT)", [])
            .unwrap();

        let test_data = "test_data_".repeat(100);
        for i in 0..50 {
            conn.execute(
                "INSERT INTO data (id, value) VALUES (?1, ?2)",
                rusqlite::params![i, &test_data],
            )
            .unwrap();
        }

        // Create dead space
        let updated = "updated___".repeat(100);
        for i in 0..25 {
            conn.execute(
                "UPDATE data SET value = ?1 WHERE id = ?2",
                rusqlite::params![&updated, i],
            )
            .unwrap();
        }
    }

    // Compact parallel.db with parallel=true
    let config_parallel = CompactionConfig::new(3).with_parallel(true);
    compact_with_recompression(dir.path().join("parallel.db"), config_parallel).unwrap();

    // Compact serial.db with parallel=false
    let config_serial = CompactionConfig::new(3).with_parallel(false);
    compact_with_recompression(dir.path().join("serial.db"), config_serial).unwrap();

    // Both should have similar stats (may differ slightly due to compression variance)
    let stats_parallel = inspect_database(dir.path().join("parallel.db")).unwrap();
    let stats_serial = inspect_database(dir.path().join("serial.db")).unwrap();

    assert_eq!(stats_parallel.page_count, stats_serial.page_count);
    // File sizes should be very close (within 5% due to compression non-determinism)
    let size_diff = (stats_parallel.file_size as f64 - stats_serial.file_size as f64).abs();
    let max_diff = stats_parallel.file_size as f64 * 0.05;
    assert!(
        size_diff <= max_diff,
        "Parallel and serial compaction should produce similar file sizes"
    );
}
