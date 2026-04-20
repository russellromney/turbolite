//! Data operation tests: UPDATE/DELETE, VACUUM, rollback, multiple tables, BLOBs, journal modes.

use turbolite::tiered::{TurboliteConfig, TurboliteVfs};
use tempfile::TempDir;
use super::helpers::*;

#[test]
fn test_rollback_discards_dirty_pages() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("rollback", cache_dir.path());
    let vfs_name = unique_vfs_name("tiered_rollback");

    let vfs = TurboliteVfs::new_local(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "rollback_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    conn.execute_batch(
        "PRAGMA page_size=65536;
         PRAGMA journal_mode=WAL;
         CREATE TABLE rb (id INTEGER PRIMARY KEY, val TEXT);",
    )
    .unwrap();

    // Bulk insert committed data and checkpoint
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..10 {
            tx.execute(
                "INSERT INTO rb VALUES (?1, ?2)",
                rusqlite::params![i, format!("committed_{}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();

    // Start a transaction, bulk insert more rows, then ROLLBACK
    conn.execute_batch("BEGIN;").unwrap();
    for i in 10..110 {
        conn.execute(
            "INSERT INTO rb VALUES (?1, ?2)",
            rusqlite::params![i, format!("rolled_back_{}", i)],
        )
        .unwrap();
    }
    conn.execute_batch("ROLLBACK;").unwrap();

    // Only the committed 10 rows should exist
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM rb", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 10, "rollback should discard uncommitted rows");

    // Checkpoint again — should still be consistent
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();

    let count2: i64 = conn
        .query_row("SELECT COUNT(*) FROM rb", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count2, 10, "checkpoint after rollback should preserve only committed data");

    // Bulk insert more after rollback — should work fine
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 10..20 {
            tx.execute(
                "INSERT INTO rb VALUES (?1, ?2)",
                rusqlite::params![i, format!("after_rollback_{}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();

    let count3: i64 = conn
        .query_row("SELECT COUNT(*) FROM rb", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count3, 20, "should have 20 rows after recovery");
}

#[test]
fn test_large_representative_db() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("large_db", cache_dir.path());
    let vfs_name = unique_vfs_name("tiered_large");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();

    let vfs = TurboliteVfs::new_local(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "large_db_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    conn.execute_batch(
        "PRAGMA page_size=65536;
         PRAGMA journal_mode=WAL;
         PRAGMA wal_autocheckpoint=0;
         CREATE TABLE sensor_readings (
             id INTEGER PRIMARY KEY,
             device_id TEXT NOT NULL,
             ts_ms INTEGER NOT NULL,
             temperature REAL,
             humidity REAL,
             pressure REAL,
             battery_pct INTEGER,
             status TEXT,
             payload BLOB
         );",
    )
    .unwrap();

    let total_rows: i64 = 100_000;
    let batch_size = 10_000;
    let devices = [
        "sensor-A1", "sensor-A2", "sensor-B1", "sensor-B2",
        "sensor-C1", "sensor-C2", "sensor-D1", "sensor-D2",
    ];
    let statuses = ["ok", "warning", "critical", "offline", "maintenance"];
    // ~128 byte payload to simulate realistic sensor data blobs
    let payload = vec![0xABu8; 128];

    let insert_start = std::time::Instant::now();

    // Bulk insert in batches of 10K
    for batch in 0..(total_rows / batch_size) {
        let tx = conn.unchecked_transaction().unwrap();
        let start = batch * batch_size;
        let end = start + batch_size;
        for i in start..end {
            let device = devices[(i % devices.len() as i64) as usize];
            let status = statuses[(i % statuses.len() as i64) as usize];
            let ts = 1700000000000_i64 + i * 1000; // 1s intervals
            let temp = 20.0 + (i % 100) as f64 * 0.1;
            let humidity = 40.0 + (i % 60) as f64 * 0.5;
            let pressure = 1013.0 + (i % 50) as f64 * 0.2;
            let battery = 100 - (i % 100) as i32;
            tx.execute(
                "INSERT INTO sensor_readings VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                rusqlite::params![i, device, ts, temp, humidity, pressure, battery, status, payload],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    let insert_elapsed = insert_start.elapsed();
    eprintln!(
        "Inserted {} rows in {:?} ({:.0} rows/sec)",
        total_rows,
        insert_elapsed,
        total_rows as f64 / insert_elapsed.as_secs_f64()
    );

    // Checkpoint to S3
    let ckpt_start = std::time::Instant::now();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    let ckpt_elapsed = ckpt_start.elapsed();
    eprintln!("Checkpoint {} rows to S3: {:?}", total_rows, ckpt_elapsed);

    // Verify data was written locally
    let local_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM sensor_readings", [], |row| row.get(0))
        .unwrap();
    assert_eq!(local_count, total_rows, "local count mismatch");

    drop(conn);

    // Verify S3 manifest directly — proves data landed in Tigris
    verify_s3_manifest(&bucket, &prefix, &endpoint, 2, 65536);

    // Cold read from a completely fresh cache
    let cold_cache = TempDir::new().unwrap();
    let reader_config = TurboliteConfig {
        bucket,
        prefix,
        cache_dir: cold_cache.path().to_path_buf(),
        endpoint_url: endpoint,
        read_only: true,
        region,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let reader_vfs_name = unique_vfs_name("tiered_large_reader");
    let reader_vfs = TurboliteVfs::new_local(reader_config).unwrap();
    turbolite::tiered::register(&reader_vfs_name, reader_vfs)
        .unwrap();

    let reader = rusqlite::Connection::open_with_flags_and_vfs(
        "large_db_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &reader_vfs_name,
    )
    .unwrap();

    // Full count — forces complete scan from S3
    let cold_start = std::time::Instant::now();
    let cold_count: i64 = reader
        .query_row("SELECT COUNT(*) FROM sensor_readings", [], |row| row.get(0))
        .unwrap();
    let cold_elapsed = cold_start.elapsed();
    assert_eq!(cold_count, total_rows, "cold scan count mismatch");
    eprintln!(
        "Cold scan COUNT(*) of {} rows: {:?}",
        total_rows, cold_elapsed
    );

    // Aggregation query — realistic analytics workload
    let avg_temp: f64 = reader
        .query_row(
            "SELECT AVG(temperature) FROM sensor_readings WHERE device_id = 'sensor-A1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(avg_temp > 0.0, "avg temperature should be positive");

    // Per-device counts — validates grouping works
    let device_count: i64 = reader
        .query_row(
            "SELECT COUNT(*) FROM sensor_readings WHERE device_id = 'sensor-B2'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        device_count,
        total_rows / devices.len() as i64,
        "each device should have equal row count"
    );

    // Verify specific row integrity (first, middle, last)
    let first_ts: i64 = reader
        .query_row(
            "SELECT ts_ms FROM sensor_readings WHERE id = 0",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(first_ts, 1700000000000);

    let mid_id = total_rows / 2;
    let mid_ts: i64 = reader
        .query_row(
            "SELECT ts_ms FROM sensor_readings WHERE id = ?1",
            rusqlite::params![mid_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(mid_ts, 1700000000000 + mid_id * 1000);

    let last_ts: i64 = reader
        .query_row(
            "SELECT ts_ms FROM sensor_readings WHERE id = ?1",
            rusqlite::params![total_rows - 1],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(last_ts, 1700000000000 + (total_rows - 1) * 1000);

    // Verify BLOB integrity
    let blob_data: Vec<u8> = reader
        .query_row(
            "SELECT payload FROM sensor_readings WHERE id = ?1",
            rusqlite::params![total_rows / 3],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(blob_data.len(), 128);
    assert!(blob_data.iter().all(|&b| b == 0xAB));

    eprintln!("Large representative DB test passed: {} rows, bulk insert + S3 checkpoint + cold read", total_rows);
}

#[test]
fn test_oltp_with_indexes() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("oltp_idx", cache_dir.path());
    let vfs_name = unique_vfs_name("tiered_oltp");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();

    let vfs = TurboliteVfs::new_local(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "oltp_idx_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    conn.execute_batch(
        "PRAGMA page_size=65536;
         PRAGMA journal_mode=WAL;
         CREATE TABLE orders (
             id INTEGER PRIMARY KEY,
             customer TEXT NOT NULL,
             amount REAL NOT NULL,
             status TEXT NOT NULL,
             created_at INTEGER NOT NULL
         );
         CREATE INDEX idx_customer ON orders(customer);
         CREATE INDEX idx_status ON orders(status);
         CREATE INDEX idx_amount ON orders(amount);",
    )
    .unwrap();

    // INSERT 1000 rows
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..1000 {
            tx.execute(
                "INSERT INTO orders VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    i,
                    format!("cust_{}", i % 50),
                    (i as f64) * 1.5,
                    if i % 3 == 0 { "shipped" } else { "pending" },
                    1700000000 + i * 60
                ],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();

    // UPDATE 200 rows in the range that won't be deleted (id < 900)
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..200 {
            tx.execute(
                "UPDATE orders SET status = 'delivered', amount = amount + 10.0 WHERE id = ?1",
                rusqlite::params![i],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();

    // DELETE 100 rows
    {
        let tx = conn.unchecked_transaction().unwrap();
        tx.execute("DELETE FROM orders WHERE id >= 900", [])
            .unwrap();
        tx.commit().unwrap();
    }

    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();

    // Verify locally
    let local_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM orders", [], |row| row.get(0))
        .unwrap();
    assert_eq!(local_count, 900);

    let delivered: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM orders WHERE status = 'delivered'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(delivered, 200);

    drop(conn);

    // Cold read from S3
    let cold_cache = TempDir::new().unwrap();
    let reader_config = TurboliteConfig {
        bucket,
        prefix,
        cache_dir: cold_cache.path().to_path_buf(),
        endpoint_url: endpoint,
        read_only: true,
        region,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let reader_vfs_name = unique_vfs_name("tiered_oltp_r");
    let reader_vfs = TurboliteVfs::new_local(reader_config).unwrap();
    turbolite::tiered::register(&reader_vfs_name, reader_vfs)
        .unwrap();

    let reader = rusqlite::Connection::open_with_flags_and_vfs(
        "oltp_idx_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &reader_vfs_name,
    )
    .unwrap();

    let cold_count: i64 = reader
        .query_row("SELECT COUNT(*) FROM orders", [], |row| row.get(0))
        .unwrap();
    assert_eq!(cold_count, 900, "cold read should see 900 rows after deletes");

    // Index-assisted queries should work
    let cust_count: i64 = reader
        .query_row(
            "SELECT COUNT(*) FROM orders WHERE customer = 'cust_0'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(cust_count > 0, "index lookup by customer should find rows");

    let cold_delivered: i64 = reader
        .query_row(
            "SELECT COUNT(*) FROM orders WHERE status = 'delivered'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(cold_delivered, 200, "cold read should see updated statuses");

    // Range query using amount index
    let high_value: i64 = reader
        .query_row(
            "SELECT COUNT(*) FROM orders WHERE amount > 1000.0",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(high_value > 0, "range query on indexed column should work");

    eprintln!("OLTP with indexes test passed: 3 indexes, insert/update/delete, cold read");
}

/// UPDATE and DELETE operations exercise read-modify-write of existing page groups.
/// Verifies data integrity after modifying and deleting rows across multiple
/// checkpoints.
#[test]
fn test_update_delete_operations() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("upd_del", cache_dir.path());
    let vfs_name = unique_vfs_name("tiered_upddel");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();

    let vfs = TurboliteVfs::new_local(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "upd_del_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    conn.execute_batch(
        "PRAGMA page_size=65536;
         PRAGMA journal_mode=WAL;
         CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT NOT NULL);",
    )
    .unwrap();

    // Initial insert
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..500 {
            tx.execute(
                "INSERT INTO kv VALUES (?1, ?2)",
                rusqlite::params![format!("key_{:04}", i), format!("original_{}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();

    // Update half the rows (changes value in-place on existing pages)
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..250 {
            tx.execute(
                "UPDATE kv SET value = ?1 WHERE key = ?2",
                rusqlite::params![
                    format!("updated_{}_with_longer_value_to_change_page_layout", i),
                    format!("key_{:04}", i * 2)
                ],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();

    // Delete a range
    {
        let tx = conn.unchecked_transaction().unwrap();
        tx.execute("DELETE FROM kv WHERE key >= 'key_0400'", [])
            .unwrap();
        tx.commit().unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();

    drop(conn);

    // Cold read
    let cold_cache = TempDir::new().unwrap();
    let reader_config = TurboliteConfig {
        bucket,
        prefix,
        cache_dir: cold_cache.path().to_path_buf(),
        endpoint_url: endpoint,
        read_only: true,
        region,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let reader_vfs_name = unique_vfs_name("tiered_upddel_r");
    let reader_vfs = TurboliteVfs::new_local(reader_config).unwrap();
    turbolite::tiered::register(&reader_vfs_name, reader_vfs)
        .unwrap();

    let reader = rusqlite::Connection::open_with_flags_and_vfs(
        "upd_del_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &reader_vfs_name,
    )
    .unwrap();

    let count: i64 = reader
        .query_row("SELECT COUNT(*) FROM kv", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 400, "should have 400 rows after deleting 100");

    // Verify updated values
    let val: String = reader
        .query_row(
            "SELECT value FROM kv WHERE key = 'key_0000'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(val.starts_with("updated_"), "key_0000 should be updated");

    // Verify non-updated values
    let val2: String = reader
        .query_row(
            "SELECT value FROM kv WHERE key = 'key_0001'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(val2.starts_with("original_"), "key_0001 should be original");

    // Verify deleted range is gone
    let deleted_count: i64 = reader
        .query_row(
            "SELECT COUNT(*) FROM kv WHERE key >= 'key_0400'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(deleted_count, 0, "deleted range should be gone");
}

/// Multiple tables in the same database. Verifies that internal B-tree pages
/// and schema table pages are correctly stored in page groups.
#[test]
fn test_multiple_tables() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("multi_tbl", cache_dir.path());
    let vfs_name = unique_vfs_name("tiered_multitbl");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();

    let vfs = TurboliteVfs::new_local(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "multi_tbl_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    conn.execute_batch(
        "PRAGMA page_size=65536;
         PRAGMA journal_mode=WAL;
         CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
         CREATE TABLE products (id INTEGER PRIMARY KEY, title TEXT, price REAL);
         CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, product_id INTEGER, qty INTEGER);
         CREATE TABLE audit_log (id INTEGER PRIMARY KEY, action TEXT, ts INTEGER);",
    )
    .unwrap();

    // Populate all 4 tables
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..100 {
            tx.execute(
                "INSERT INTO users VALUES (?1, ?2)",
                rusqlite::params![i, format!("user_{}", i)],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO products VALUES (?1, ?2, ?3)",
                rusqlite::params![i, format!("product_{}", i), i as f64 * 9.99],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO orders VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![i, i % 50, i % 100, (i % 10) + 1],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO audit_log VALUES (?1, ?2, ?3)",
                rusqlite::params![i, format!("create_order_{}", i), 1700000000 + i],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    drop(conn);

    // Cold read
    let cold_cache = TempDir::new().unwrap();
    let reader_config = TurboliteConfig {
        bucket,
        prefix,
        cache_dir: cold_cache.path().to_path_buf(),
        endpoint_url: endpoint,
        read_only: true,
        region,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let reader_vfs_name = unique_vfs_name("tiered_multitbl_r");
    let reader_vfs = TurboliteVfs::new_local(reader_config).unwrap();
    turbolite::tiered::register(&reader_vfs_name, reader_vfs)
        .unwrap();

    let reader = rusqlite::Connection::open_with_flags_and_vfs(
        "multi_tbl_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &reader_vfs_name,
    )
    .unwrap();

    // Verify all 4 tables
    for (table, expected) in [("users", 100), ("products", 100), ("orders", 100), ("audit_log", 100)] {
        let count: i64 = reader
            .query_row(&format!("SELECT COUNT(*) FROM {}", table), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, expected, "{} should have {} rows", table, expected);
    }

    // JOIN query across tables
    let join_count: i64 = reader
        .query_row(
            "SELECT COUNT(*) FROM orders o
             JOIN users u ON u.id = o.user_id
             JOIN products p ON p.id = o.product_id",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(join_count, 100, "join should return all orders");
}

#[test]
fn test_large_overflow_blobs() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("overflow", cache_dir.path());
    let vfs_name = unique_vfs_name("tiered_overflow");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();

    let vfs = TurboliteVfs::new_local(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "overflow_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    // Use 4096 page size to make overflow more likely with smaller blobs
    conn.execute_batch(
        "PRAGMA page_size=4096;
         PRAGMA journal_mode=WAL;
         CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB);",
    )
    .unwrap();

    // Insert blobs of varying sizes — some larger than page size
    let sizes = [100, 1000, 5000, 10000, 50000, 100000];
    {
        let tx = conn.unchecked_transaction().unwrap();
        for (i, &size) in sizes.iter().enumerate() {
            let blob: Vec<u8> = (0..size).map(|b| (b % 256) as u8).collect();
            tx.execute(
                "INSERT INTO blobs VALUES (?1, ?2)",
                rusqlite::params![i as i64, blob],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    drop(conn);

    // Cold read
    let cold_cache = TempDir::new().unwrap();
    let reader_config = TurboliteConfig {
        bucket,
        prefix,
        cache_dir: cold_cache.path().to_path_buf(),
        endpoint_url: endpoint,
        read_only: true,
        region,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let reader_vfs_name = unique_vfs_name("tiered_overflow_r");
    let reader_vfs = TurboliteVfs::new_local(reader_config).unwrap();
    turbolite::tiered::register(&reader_vfs_name, reader_vfs)
        .unwrap();

    let reader = rusqlite::Connection::open_with_flags_and_vfs(
        "overflow_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &reader_vfs_name,
    )
    .unwrap();

    // Verify each blob's size and content
    for (i, &expected_size) in sizes.iter().enumerate() {
        let data: Vec<u8> = reader
            .query_row(
                "SELECT data FROM blobs WHERE id = ?1",
                rusqlite::params![i as i64],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            data.len(),
            expected_size,
            "blob {} should be {} bytes",
            i,
            expected_size
        );
        // Verify content pattern
        for (j, &byte) in data.iter().enumerate() {
            assert_eq!(
                byte,
                (j % 256) as u8,
                "blob {} byte {} mismatch",
                i,
                j
            );
        }
    }

    eprintln!(
        "Overflow blob test passed: sizes {:?}",
        sizes
    );
}

/// VACUUM after deletes reorganizes pages. Verifies that the VFS
/// handles page count changes and page reuse correctly.
#[test]
fn test_vacuum_reorganizes() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("vacuum", cache_dir.path());
    let vfs_name = unique_vfs_name("tiered_vacuum");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();

    let vfs = TurboliteVfs::new_local(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "vacuum_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    conn.execute_batch(
        "PRAGMA page_size=65536;
         PRAGMA journal_mode=WAL;
         CREATE TABLE bulk (id INTEGER PRIMARY KEY, payload TEXT);",
    )
    .unwrap();

    // Insert enough data to create many pages
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..2000 {
            tx.execute(
                "INSERT INTO bulk VALUES (?1, ?2)",
                rusqlite::params![i, format!("payload_{:0>500}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();

    let pages_before: i64 = conn
        .query_row("PRAGMA page_count", [], |row| row.get(0))
        .unwrap();
    eprintln!("Pages before delete: {}", pages_before);

    // Delete most rows
    conn.execute("DELETE FROM bulk WHERE id >= 200", [])
        .unwrap();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();

    // VACUUM reclaims space (switches from WAL to rollback journal temporarily)
    // Need to re-enable WAL after VACUUM
    conn.execute_batch("VACUUM;").unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();

    let pages_after: i64 = conn
        .query_row("PRAGMA page_count", [], |row| row.get(0))
        .unwrap();
    eprintln!("Pages after VACUUM: {}", pages_after);
    assert!(
        pages_after < pages_before,
        "VACUUM should reduce page count: {} -> {}",
        pages_before,
        pages_after
    );

    // Verify remaining data
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM bulk", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 200);

    drop(conn);

    // Cold read after VACUUM
    let cold_cache = TempDir::new().unwrap();
    let reader_config = TurboliteConfig {
        bucket,
        prefix,
        cache_dir: cold_cache.path().to_path_buf(),
        endpoint_url: endpoint,
        read_only: true,
        region,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let reader_vfs_name = unique_vfs_name("tiered_vacuum_r");
    let reader_vfs = TurboliteVfs::new_local(reader_config).unwrap();
    turbolite::tiered::register(&reader_vfs_name, reader_vfs)
        .unwrap();

    let reader = rusqlite::Connection::open_with_flags_and_vfs(
        "vacuum_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &reader_vfs_name,
    )
    .unwrap();

    let cold_count: i64 = reader
        .query_row("SELECT COUNT(*) FROM bulk", [], |row| row.get(0))
        .unwrap();
    assert_eq!(cold_count, 200, "cold read after VACUUM should see 200 rows");

    let val: String = reader
        .query_row(
            "SELECT payload FROM bulk WHERE id = 199",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(val.starts_with("payload_"));
}

/// VFS delete() must NOT destroy S3 data. Regression test for the bug where
/// delete() called destroy_s3() unconditionally. When a connection is closed,
/// SQLite may call delete on WAL/journal files — this must only affect local files.
#[test]
fn test_delete_preserves_s3() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("del_s3", cache_dir.path());
    let vfs_name = unique_vfs_name("tiered_del_s3");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();

    let vfs = TurboliteVfs::new_local(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    // Write data and checkpoint to S3
    {
        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "del_s3_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch(
            "PRAGMA page_size=65536;
             PRAGMA journal_mode=WAL;
             CREATE TABLE ds (id INTEGER PRIMARY KEY, val TEXT);",
        )
        .unwrap();

        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..100 {
                tx.execute(
                    "INSERT INTO ds VALUES (?1, ?2)",
                    rusqlite::params![i, format!("del_s3_{}", i)],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();

        // Verify S3 has data
        verify_s3_manifest(&bucket, &prefix, &endpoint, 1, 65536);
    }
    // Connection dropped — SQLite may call xDelete on WAL/SHM files

    // S3 data MUST still exist after connection close + WAL cleanup
    verify_s3_manifest(&bucket, &prefix, &endpoint, 1, 65536);
    let pg_after = verify_s3_has_page_groups(&bucket, &prefix, &endpoint);
    assert!(
        pg_after >= 1,
        "S3 page groups must survive connection close"
    );

    // Cold read from fresh cache to prove S3 data survived
    let cold_cache = TempDir::new().unwrap();
    let reader_config = TurboliteConfig {
        bucket,
        prefix,
        cache_dir: cold_cache.path().to_path_buf(),
        endpoint_url: endpoint,
        read_only: true,
        region,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let reader_vfs_name = unique_vfs_name("tiered_del_s3_r");
    let reader_vfs = TurboliteVfs::new_local(reader_config).unwrap();
    turbolite::tiered::register(&reader_vfs_name, reader_vfs)
        .unwrap();

    let reader = rusqlite::Connection::open_with_flags_and_vfs(
        "del_s3_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &reader_vfs_name,
    )
    .unwrap();

    let cold_count: i64 = reader
        .query_row("SELECT COUNT(*) FROM ds", [], |row| row.get(0))
        .unwrap();
    assert_eq!(cold_count, 100, "cold read after close must work");
}

/// Verify the VFS works in DELETE journal mode (non-WAL).
/// Each transaction commit triggers write_all_at → sync → S3 upload.
#[test]
fn test_delete_journal_mode() {
    let cache_dir = TempDir::new().unwrap();
    let config = test_config("jdel", cache_dir.path());
    let vfs_name = unique_vfs_name("tiered_jdel");
    let bucket = config.bucket.clone();
    let prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();
    let region = config.region.clone();

    let vfs = TurboliteVfs::new_local(config).unwrap();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "jdel_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();

    // Use DELETE mode (non-WAL) from the start
    conn.execute_batch(
        "PRAGMA page_size=65536;
         PRAGMA journal_mode=DELETE;
         CREATE TABLE jd (id INTEGER PRIMARY KEY, val TEXT);",
    )
    .unwrap();

    // Insert data — each commit goes through write_all_at → sync
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..200 {
            tx.execute(
                "INSERT INTO jd VALUES (?1, ?2)",
                rusqlite::params![i, format!("delete_mode_{}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    // Verify data is readable
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM jd", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 200, "DELETE mode should store data correctly");

    drop(conn);

    // Verify S3 has data (sync was called during commit)
    verify_s3_manifest(&bucket, &prefix, &endpoint, 1, 65536);

    // Cold read from fresh cache
    let cold_cache = TempDir::new().unwrap();
    let reader_config = TurboliteConfig {
        bucket,
        prefix,
        cache_dir: cold_cache.path().to_path_buf(),
        endpoint_url: endpoint,
        read_only: true,
        region,
        runtime_handle: Some(super::helpers::shared_runtime_handle()), ..Default::default()
    };
    let reader_vfs_name = unique_vfs_name("tiered_jdel_r");
    let reader_vfs = TurboliteVfs::new_local(reader_config).unwrap();
    turbolite::tiered::register(&reader_vfs_name, reader_vfs)
        .unwrap();

    let reader = rusqlite::Connection::open_with_flags_and_vfs(
        "jdel_test.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &reader_vfs_name,
    )
    .unwrap();

    let cold_count: i64 = reader
        .query_row("SELECT COUNT(*) FROM jd", [], |row| row.get(0))
        .unwrap();
    assert_eq!(cold_count, 200, "cold read in DELETE mode must work");
}
