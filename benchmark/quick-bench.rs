//! Quick benchmark for TurboliteVfs local mode.
//!
//! Run with: cargo run --example quick-bench --release
//!
//! Measures basic read/write throughput with TurboliteVfs local storage.

use rusqlite::Connection;
use turbolite::tiered::{TurboliteVfs, TurboliteConfig};
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn measure<F: FnMut()>(name: &str, iterations: usize, mut f: F) -> Duration {
    let start = Instant::now();
    for _ in 0..iterations {
        f();
    }
    let elapsed = start.elapsed();
    let per_iter = elapsed / iterations as u32;
    println!("  {}: {:?} total, {:?}/iter ({} iters)", name, elapsed, per_iter, iterations);
    elapsed
}

fn main() {
    println!("turbolite quick-bench (TurboliteVfs local mode)\n");

    let dir = TempDir::new().expect("create temp dir");

    // Register TurboliteVfs local
    let config = TurboliteConfig {
        cache_dir: dir.path().into(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new_local(config).expect("create VFS");
    turbolite::tiered::register("bench", vfs).expect("register VFS");

    let db_path = dir.path().join("bench.db");
    let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE;

    // Write benchmark
    println!("Write benchmark:");
    {
        let conn = Connection::open_with_flags_and_vfs(&db_path, flags, "bench").expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap();
        conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT)", []).unwrap();

        let data = "x".repeat(1000);
        measure("insert 10k rows", 1, || {
            conn.execute("BEGIN", []).unwrap();
            for i in 0..10_000 {
                conn.execute("INSERT INTO bench (data) VALUES (?1)", [&data]).unwrap();
            }
            conn.execute("COMMIT", []).unwrap();
        });

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    // Read benchmark
    println!("\nRead benchmark:");
    {
        let conn = Connection::open_with_flags_and_vfs(&db_path, flags, "bench").expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();

        measure("point lookup x1000", 1, || {
            for i in 1..=1000 {
                let _: String = conn.query_row(
                    "SELECT data FROM bench WHERE id = ?1", [i],
                    |row| row.get(0),
                ).unwrap();
            }
        });

        measure("full scan", 3, || {
            let _: i64 = conn.query_row("SELECT COUNT(*) FROM bench", [], |r| r.get(0)).unwrap();
        });
    }

    println!("\nDone.");
}
