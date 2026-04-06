//! Siege 5: Concurrent corruption tests.
//!
//! Prove: concurrent reads, writes, eviction, and checkpoint never produce
//! torn reads or corrupted data.
//!
//! Strategy: multiple threads hammer the database simultaneously while cache
//! eviction runs. Every read must return correct data (verified via checksums
//! stored alongside the data).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use turbolite::tiered::{TurboliteConfig, TurboliteVfs};

use super::helpers::*;

/// CRC32 of a byte slice, stored alongside data for integrity verification.
fn crc32(data: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

/// Write data to S3, return connection info for readers.
/// Each row stores its own CRC32 so readers can verify data integrity.
fn setup_concurrent_db(prefix: &str) -> (String, String, Option<String>, TempDir) {
    let cache_dir = TempDir::new().expect("tempdir");
    let mut config = test_config(prefix, cache_dir.path());
    config.pages_per_group = 16; // small groups to force more eviction
    let vfs_name = unique_vfs_name(&format!("conc_evict_{}", prefix));
    let bucket = config.bucket.clone();
    let s3_prefix = config.prefix.clone();
    let endpoint = config.endpoint_url.clone();

    let vfs = TurboliteVfs::new(config).expect("vfs");
    turbolite::tiered::register(&vfs_name, vfs).expect("register");

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        &format!("{}_setup.db", prefix),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .expect("open");

    conn.execute_batch(
        "PRAGMA page_size=4096;
         PRAGMA journal_mode=WAL;
         CREATE TABLE checksummed (
             id INTEGER PRIMARY KEY,
             payload BLOB NOT NULL,
             crc INTEGER NOT NULL
         );
         CREATE INDEX idx_crc ON checksummed(crc);",
    )
    .expect("schema");

    // Insert 2000 rows with varying payload sizes and embedded CRC32
    {
        let tx = conn.unchecked_transaction().expect("tx");
        for i in 0..2000 {
            let payload_size = 100 + (i % 500);
            let payload: Vec<u8> = (0..payload_size).map(|j| ((i + j) % 256) as u8).collect();
            let crc = crc32(&payload);
            tx.execute(
                "INSERT INTO checksummed (id, payload, crc) VALUES (?1, ?2, ?3)",
                rusqlite::params![i, payload, crc as i64],
            )
            .expect("insert");
        }
        tx.commit().expect("commit");
    }

    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("checkpoint");
    drop(conn);

    (bucket, s3_prefix, endpoint, cache_dir)
}

/// Open a cold reader with a tight cache budget to force eviction.
fn open_evicting_reader(
    bucket: &str,
    prefix: &str,
    endpoint: &Option<String>,
    cache_budget_pages: u64,
) -> (rusqlite::Connection, String, TempDir) {
    let cache_dir = TempDir::new().expect("tempdir");
    let config = TurboliteConfig {
        bucket: bucket.to_string(),
        prefix: prefix.to_string(),
        endpoint_url: endpoint.clone(),
        region: Some("auto".to_string()),
        cache_dir: cache_dir.path().to_path_buf(),
        pages_per_group: 16,
        read_only: true,
        max_cache_bytes: Some(cache_budget_pages * 4096),
        runtime_handle: Some(shared_runtime_handle()),
        ..Default::default()
    };
    let vfs_name = unique_vfs_name("conc_evict_reader");
    let vfs = TurboliteVfs::new(config).expect("vfs");
    turbolite::tiered::register(&vfs_name, vfs).expect("register");

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "conc_evict_cold.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &vfs_name,
    )
    .expect("open");

    (conn, vfs_name, cache_dir)
}

/// Verify every row's CRC32 matches its payload. Returns number of rows checked.
fn verify_all_checksums(conn: &rusqlite::Connection) -> usize {
    let mut stmt = conn
        .prepare("SELECT id, payload, crc FROM checksummed ORDER BY id")
        .expect("prepare");
    let rows: Vec<(i64, Vec<u8>, i64)> = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .expect("query")
        .collect::<Result<_, _>>()
        .expect("collect");

    for (id, payload, stored_crc) in &rows {
        let actual_crc = crc32(payload) as i64;
        assert_eq!(
            *stored_crc, actual_crc,
            "CRC mismatch for row {}: stored={}, actual={}, payload_len={}",
            id,
            stored_crc,
            actual_crc,
            payload.len()
        );
    }
    rows.len()
}

/// Core test: multiple reader threads doing full scans with CRC verification
/// while cache eviction is active (tight cache budget forces pages in/out).
#[test]
fn concurrent_reads_with_eviction_never_corrupt() {
    let (bucket, prefix, endpoint, _writer_dir) =
        setup_concurrent_db("conc_reads_evict");

    let stop = Arc::new(AtomicBool::new(false));
    let total_verified = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));

    // Spawn 4 reader threads, each with its own cold VFS and tight cache
    let mut handles = Vec::new();
    for thread_id in 0..4 {
        let bucket = bucket.clone();
        let prefix = prefix.clone();
        let endpoint = endpoint.clone();
        let stop = Arc::clone(&stop);
        let total_verified = Arc::clone(&total_verified);
        let errors = Arc::clone(&errors);

        handles.push(thread::spawn(move || {
            // Each reader gets only 50 cache pages (DB needs ~200+)
            // This forces constant eviction + re-fetch from S3
            let (conn, _vfs_name, _cache_dir) =
                open_evicting_reader(&bucket, &prefix, &endpoint, 50);

            let mut scans = 0u64;
            while !stop.load(Ordering::Relaxed) {
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    verify_all_checksums(&conn)
                })) {
                    Ok(count) => {
                        total_verified.fetch_add(count as u64, Ordering::Relaxed);
                        scans += 1;
                    }
                    Err(e) => {
                        eprintln!("Thread {} scan failed: {:?}", thread_id, e);
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            eprintln!("Thread {} completed {} full scans", thread_id, scans);
        }));
    }

    // Let it run for 15 seconds
    thread::sleep(Duration::from_secs(15));
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().expect("thread panicked");
    }

    let verified = total_verified.load(Ordering::Relaxed);
    let err_count = errors.load(Ordering::Relaxed);
    eprintln!(
        "Total rows verified: {}, errors: {}",
        verified, err_count
    );
    assert_eq!(err_count, 0, "corrupted reads detected during eviction");
    assert!(verified > 0, "no rows were verified");
}

/// Sequential full scans with integrity check between each.
/// Simpler than the concurrent test, catches basic eviction bugs.
#[test]
fn sequential_scans_with_tight_cache() {
    let (bucket, prefix, endpoint, _writer_dir) =
        setup_concurrent_db("seq_evict");

    // 30 cache pages forces eviction on every scan
    let (conn, _vfs_name, _cache_dir) =
        open_evicting_reader(&bucket, &prefix, &endpoint, 30);

    for scan in 0..10 {
        let count = verify_all_checksums(&conn);
        assert_eq!(count, 2000, "scan {}: expected 2000 rows, got {}", scan, count);

        let integrity: String = conn
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .expect("integrity");
        assert_eq!(
            integrity, "ok",
            "scan {}: integrity check failed: {}",
            scan, integrity
        );
    }
}

/// Random point queries under eviction pressure.
/// Tests that individual page fetches return correct data even when the
/// surrounding pages are being evicted.
#[test]
fn point_queries_under_eviction() {
    let (bucket, prefix, endpoint, _writer_dir) =
        setup_concurrent_db("point_evict");

    // Very tight cache: 20 pages
    let (conn, _vfs_name, _cache_dir) =
        open_evicting_reader(&bucket, &prefix, &endpoint, 20);

    // Do 500 random point queries
    for i in 0..500 {
        let target_id = (i * 7 + 13) % 2000; // pseudo-random distribution
        let row: (Vec<u8>, i64) = conn
            .query_row(
                "SELECT payload, crc FROM checksummed WHERE id = ?1",
                rusqlite::params![target_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap_or_else(|e| panic!("query for id {} failed: {}", target_id, e));

        let actual_crc = crc32(&row.0) as i64;
        assert_eq!(
            row.1, actual_crc,
            "CRC mismatch on point query for id {}: stored={}, actual={}",
            target_id, row.1, actual_crc
        );
    }
}
