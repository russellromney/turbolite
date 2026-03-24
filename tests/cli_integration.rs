//! Integration tests for sqlces CLI library functions
//!
//! These tests verify the inspect_database and compact library functions
//! that power the CLI tool.

use rusqlite::Connection;
use turbolite::{compact, inspect_database, register, CompressedVfs};
use std::fs;
use tempfile::TempDir;

fn create_test_vfs(temp_dir: &TempDir, name: &str) -> String {
    let vfs_name = format!("test_{}_{}", name, std::process::id());
    let vfs = CompressedVfs::new(temp_dir.path(), 3);
    register(&vfs_name, vfs).expect("Failed to register VFS");
    vfs_name
}

fn create_compressed_db(temp_dir: &TempDir, name: &str, rows: usize) -> std::path::PathBuf {
    let vfs_name = create_test_vfs(temp_dir, name);
    let db_path = temp_dir.path().join(format!("{}.db", name));

    let conn = Connection::open_with_flags_and_vfs(
        &db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .expect("Failed to open connection");

    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")
        .unwrap();
    conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)",
        [],
    )
    .unwrap();

    for i in 0..rows {
        conn.execute(
            "INSERT INTO test (data) VALUES (?)",
            [format!("test_data_{}", i)],
        )
        .unwrap();
    }

    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    drop(conn);

    db_path
}

#[test]
fn test_inspect_database_valid() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = create_compressed_db(&temp_dir, "inspect_valid", 100);

    let stats = inspect_database(&db_path).expect("inspect_database should succeed");

    assert_eq!(stats.page_size, 4096, "Default page size should be 4096");
    assert!(stats.page_count > 0, "Should have pages");
    assert!(stats.file_size > 0, "File should have size");
    assert!(stats.logical_size > 0, "Logical size should be positive");
    assert!(stats.compression_ratio > 1.0, "Should have compression");
}

#[test]
fn test_inspect_database_invalid_file() {
    let temp_dir = TempDir::new().unwrap();
    let invalid_path = temp_dir.path().join("invalid.db");
    fs::write(&invalid_path, b"not a database").unwrap();

    let result = inspect_database(&invalid_path);
    assert!(result.is_err(), "Should fail for invalid file");
}

#[test]
fn test_inspect_database_nonexistent() {
    let result = inspect_database("/nonexistent/path/db.db");
    assert!(result.is_err(), "Should fail for nonexistent file");
}

#[test]
fn test_compact_removes_dead_space() {
    let temp_dir = TempDir::new().unwrap();
    let vfs_name = create_test_vfs(&temp_dir, "compact");
    let db_path = temp_dir.path().join("compact.db");

    // Create database and do updates to generate dead space
    {
        let conn = Connection::open_with_flags_and_vfs(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")
            .unwrap();
        conn.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)",
            [],
        )
        .unwrap();

        // Insert data
        for i in 0..50 {
            conn.execute(
                "INSERT INTO test (data) VALUES (?)",
                [format!("initial_data_{}", i)],
            )
            .unwrap();
        }

        // Update data multiple times to create dead space
        for _ in 0..5 {
            for i in 1..=25 {
                conn.execute(
                    "UPDATE test SET data = ? WHERE id = ?",
                    [format!("updated_data"), i.to_string()],
                )
                .unwrap();
            }
        }

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
    }

    // Check stats before compact
    let stats_before = inspect_database(&db_path).unwrap();
    assert!(
        stats_before.dead_space > 0 || stats_before.total_records > stats_before.page_count,
        "Should have dead space or superseded records"
    );

    // Compact
    let freed = compact(&db_path).expect("compact should succeed");

    // Check stats after compact
    let stats_after = inspect_database(&db_path).unwrap();
    assert_eq!(
        stats_after.dead_space, 0,
        "Should have no dead space after compact"
    );
    assert_eq!(
        stats_after.total_records, stats_after.page_count,
        "Records should equal pages after compact"
    );

    // Verify data is still accessible
    let vfs_name2 = format!("test_compact_verify_{}", std::process::id());
    let vfs2 = CompressedVfs::new(temp_dir.path(), 3);
    register(&vfs_name2, vfs2).unwrap();

    let conn = Connection::open_with_flags_and_vfs(
        &db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &vfs_name2,
    )
    .unwrap();

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM test", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 50, "Should still have 50 rows");

    println!(
        "Compact freed {} bytes, dead space: {}% -> {}%",
        freed, stats_before.dead_space_pct, stats_after.dead_space_pct
    );
}

#[test]
fn test_compact_invalid_file() {
    let temp_dir = TempDir::new().unwrap();
    let invalid_path = temp_dir.path().join("invalid.db");
    fs::write(&invalid_path, b"not a database").unwrap();

    let result = compact(&invalid_path);
    assert!(result.is_err(), "Should fail for invalid file");
}

#[test]
fn test_compact_preserves_data_integrity() {
    let temp_dir = TempDir::new().unwrap();
    let vfs_name = create_test_vfs(&temp_dir, "integrity");
    let db_path = temp_dir.path().join("integrity.db");

    // Create database with specific data
    {
        let conn = Connection::open_with_flags_and_vfs(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        conn.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, value REAL)",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO items (name, value) VALUES ('alpha', 1.5)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO items (name, value) VALUES ('beta', 2.5)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO items (name, value) VALUES ('gamma', 3.5)",
            [],
        )
        .unwrap();

        // Update to create dead space
        conn.execute("UPDATE items SET value = 10.0 WHERE id = 1", [])
            .unwrap();
        conn.execute("UPDATE items SET value = 20.0 WHERE id = 2", [])
            .unwrap();

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
    }

    // Compact
    compact(&db_path).unwrap();

    // Verify data integrity
    let vfs_name2 = format!("test_integrity_verify_{}", std::process::id());
    let vfs2 = CompressedVfs::new(temp_dir.path(), 3);
    register(&vfs_name2, vfs2).unwrap();

    let conn = Connection::open_with_flags_and_vfs(
        &db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &vfs_name2,
    )
    .unwrap();

    // Check specific values
    let alpha_value: f64 = conn
        .query_row("SELECT value FROM items WHERE name = 'alpha'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(alpha_value, 10.0, "Alpha should be updated to 10.0");

    let beta_value: f64 = conn
        .query_row("SELECT value FROM items WHERE name = 'beta'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(beta_value, 20.0, "Beta should be updated to 20.0");

    let gamma_value: f64 = conn
        .query_row("SELECT value FROM items WHERE name = 'gamma'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(gamma_value, 3.5, "Gamma should still be 3.5");

    // Run integrity check
    let integrity: String = conn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    assert_eq!(integrity, "ok", "Database should pass integrity check");
}

#[test]
fn test_stats_compression_ratio() {
    let temp_dir = TempDir::new().unwrap();

    // Create a database with highly compressible data
    let vfs_name = create_test_vfs(&temp_dir, "compression");
    let db_path = temp_dir.path().join("compression.db");

    {
        let conn = Connection::open_with_flags_and_vfs(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, text TEXT)", [])
            .unwrap();

        // Insert repetitive, highly compressible data
        for i in 0..100 {
            conn.execute(
                "INSERT INTO data (text) VALUES (?)",
                [format!(
                    "This is repetitive text number {} with padding aaaaaaaaaaaaaaaaaaa",
                    i
                )],
            )
            .unwrap();
        }

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
    }

    compact(&db_path).unwrap();

    let stats = inspect_database(&db_path).unwrap();

    println!(
        "Compression ratio: {:.2}x (logical: {} bytes, compressed: {} bytes)",
        stats.compression_ratio, stats.logical_size, stats.live_data_size
    );

    assert!(
        stats.compression_ratio > 2.0,
        "Should achieve at least 2x compression on repetitive data"
    );
}

#[test]
fn test_stats_fields_consistency() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = create_compressed_db(&temp_dir, "consistency", 50);
    compact(&db_path).unwrap();

    let stats = inspect_database(&db_path).unwrap();

    // After compact, records should equal pages
    assert_eq!(
        stats.total_records, stats.page_count,
        "After compact: total_records should equal page_count"
    );

    // Dead space should be 0 after compact
    assert_eq!(stats.dead_space, 0, "After compact: dead_space should be 0");
    assert!(
        stats.dead_space_pct < 0.01,
        "After compact: dead_space_pct should be ~0"
    );

    // Logical size should match page calculations
    // Allow for max_page being different from page_count
    assert!(
        stats.logical_size >= stats.page_count * stats.page_size as u64,
        "Logical size should be at least page_count * page_size"
    );

    // File size should be reasonable
    assert!(
        stats.file_size < stats.logical_size,
        "Compressed file should be smaller than logical size"
    );
}

#[cfg(feature = "encryption")]
mod encryption_tests {
    use super::*;

    fn create_encrypted_vfs(temp_dir: &TempDir, name: &str, password: &str) -> String {
        let vfs_name = format!("test_enc_{}_{}", name, std::process::id());
        let vfs = CompressedVfs::compressed_encrypted(temp_dir.path(), 3, password);
        register(&vfs_name, vfs).expect("Failed to register VFS");
        vfs_name
    }

    #[test]
    fn test_inspect_encrypted_database() {
        let temp_dir = TempDir::new().unwrap();
        let vfs_name = create_encrypted_vfs(&temp_dir, "enc_inspect", "secret");
        let db_path = temp_dir.path().join("encrypted.db");

        {
            let conn = Connection::open_with_flags_and_vfs(
                &db_path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
                &vfs_name,
            )
            .unwrap();

            conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            conn.execute("CREATE TABLE secret_data (id INTEGER PRIMARY KEY, data TEXT)", [])
                .unwrap();
            conn.execute(
                "INSERT INTO secret_data (data) VALUES ('confidential')",
                [],
            )
            .unwrap();
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
                .unwrap();
        }

        // inspect_database works on the raw file format
        let stats = inspect_database(&db_path).expect("Should be able to inspect encrypted db");
        assert!(stats.page_count > 0);
    }

    #[test]
    fn test_compact_encrypted_database() {
        let temp_dir = TempDir::new().unwrap();
        let vfs_name = create_encrypted_vfs(&temp_dir, "enc_compact", "password123");
        let db_path = temp_dir.path().join("enc_compact.db");

        {
            let conn = Connection::open_with_flags_and_vfs(
                &db_path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
                &vfs_name,
            )
            .unwrap();

            conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT)", [])
                .unwrap();

            for i in 0..20 {
                conn.execute("INSERT INTO data (value) VALUES (?)", [format!("val_{}", i)])
                    .unwrap();
            }

            // Create dead space
            for i in 1..=10 {
                conn.execute(
                    "UPDATE data SET value = 'updated' WHERE id = ?",
                    [i],
                )
                .unwrap();
            }

            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
                .unwrap();
        }

        // Compact preserves encrypted data
        compact(&db_path).expect("Should compact encrypted db");

        // Verify data is still readable
        let vfs_name2 = format!("test_enc_verify_{}", std::process::id());
        let vfs2 = CompressedVfs::compressed_encrypted(temp_dir.path(), 3, "password123");
        register(&vfs_name2, vfs2).unwrap();

        let conn = Connection::open_with_flags_and_vfs(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &vfs_name2,
        )
        .unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM data", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 20);
    }
}
