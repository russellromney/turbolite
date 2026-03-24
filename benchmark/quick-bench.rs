//! Quick benchmark comparing SQLite VFS configurations.
//!
//! Run with: cargo run --example quick-bench --features encryption --release
//!
//! Uses realistic data from benchmark/corpora/ (sessions, logs, API responses, Redis users)

use rusqlite::Connection;
use turbolite::{register, CompressedVfs};
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn measure<F: FnMut()>(name: &str, iterations: usize, mut f: F) -> Duration {
    let start = Instant::now();
    for _ in 0..iterations {
        f();
    }
    let elapsed = start.elapsed();
    let per_op = elapsed / iterations as u32;
    let ops_per_sec = (iterations as f64) / elapsed.as_secs_f64();
    println!("  {}: {:?} total, {:?}/op, {:.0} ops/sec", name, elapsed, per_op, ops_per_sec);
    elapsed
}

/// Load samples from corpora directory
fn load_corpus(dir: &Path, prefix: &str, ext: &str, count: usize) -> Vec<Vec<u8>> {
    let mut samples = Vec::with_capacity(count);
    for i in 0..count {
        let filename = format!("{}_{:05}.{}", prefix, i, ext);
        let path = dir.join(&filename);
        if let Ok(data) = fs::read(&path) {
            samples.push(data);
        }
    }
    samples
}

fn main() {
    println!("SQLCEs Performance Comparison");
    println!("==============================\n");

    // Try to load realistic data from corpora
    let corpora_dir = Path::new("examples/corpora");

    let sessions: Vec<Vec<u8>> = if corpora_dir.exists() {
        load_corpus(&corpora_dir.join("sessions"), "session", "json", 2000)
    } else {
        Vec::new()
    };

    let logs: Vec<Vec<u8>> = if corpora_dir.exists() {
        load_corpus(&corpora_dir.join("logs"), "log", "txt", 3000)
    } else {
        Vec::new()
    };

    let api_responses: Vec<Vec<u8>> = if corpora_dir.exists() {
        load_corpus(&corpora_dir.join("json-api"), "api_error", "json", 500)
    } else {
        Vec::new()
    };

    let redis_users: Vec<Vec<u8>> = if corpora_dir.exists() {
        load_corpus(&corpora_dir.join("redis-users"), "user", "bin", 5997)
    } else {
        Vec::new()
    };

    // Build mixed samples from real data
    let has_corpora = !sessions.is_empty() && !logs.is_empty();

    if has_corpora {
        println!("Using realistic data from examples/corpora/");
        println!("  Sessions: {} samples (~{} bytes avg)",
            sessions.len(),
            sessions.iter().map(|s| s.len()).sum::<usize>() / sessions.len().max(1));
        println!("  Logs: {} samples (~{} bytes avg)",
            logs.len(),
            logs.iter().map(|s| s.len()).sum::<usize>() / logs.len().max(1));
        println!("  API responses: {} samples (~{} bytes avg)",
            api_responses.len(),
            api_responses.iter().map(|s| s.len()).sum::<usize>() / api_responses.len().max(1));
        println!("  Redis users: {} samples (~{} bytes avg)\n",
            redis_users.len(),
            redis_users.iter().map(|s| s.len()).sum::<usize>() / redis_users.len().max(1));
    } else {
        println!("WARNING: No corpora found. Run scripts/download-corpora.sh first.");
        println!("Falling back to generated data.\n");
    }

    let write_count = 1000;
    let read_count = 1000;

    // Build samples: mix of real data types
    let json_samples: Vec<String> = (0..write_count).map(|i| {
        if has_corpora {
            // Rotate through different data types
            match i % 4 {
                0 => String::from_utf8_lossy(&sessions[i % sessions.len()]).to_string(),
                1 => String::from_utf8_lossy(&logs[i % logs.len()]).to_string(),
                2 => String::from_utf8_lossy(&api_responses[i % api_responses.len()]).to_string(),
                _ => String::from_utf8_lossy(&redis_users[i % redis_users.len()]).to_string(),
            }
        } else {
            // Fallback: generate varied JSON
            format!(
                r#"{{"user_id": {}, "name": "User {}", "email": "user{}@example.com", "score": {}, "active": {}, "tags": ["tag{}", "tag{}"], "created": "2024-01-{:02}T10:30:00Z"}}"#,
                i, i, i, i * 17 % 1000, i % 2 == 0, i % 10, i % 5, (i % 28) + 1
            )
        }
    }).collect();

    // For blobs: use pseudo-random bytes (represents encrypted/binary data that doesn't compress)
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    fn make_blob(i: usize, size: usize) -> Vec<u8> {
        let mut hasher = DefaultHasher::new();
        let mut result = Vec::with_capacity(size);
        for j in 0..size {
            (i, j).hash(&mut hasher);
            result.push((hasher.finish() & 0xFF) as u8);
        }
        result
    }

    let blob_samples: Vec<Vec<u8>> = (0..write_count).map(|i| make_blob(i, 200)).collect();

    let avg_json_size = json_samples.iter().map(|s| s.len()).sum::<usize>() / write_count;
    let avg_blob_size = blob_samples.iter().map(|s| s.len()).sum::<usize>() / write_count;
    println!("Data: {} rows × (~{} bytes JSON + {} bytes blob) = ~{} bytes/row\n",
        write_count, avg_json_size, avg_blob_size, avg_json_size + avg_blob_size);


    // ===== AUTOCOMMIT WRITES (worst case) =====
    println!("AUTOCOMMIT WRITES ({} rows, worst case):", write_count);
    println!("─────────────────────────────────────────");

    // Plain SQLite
    let dir = TempDir::new().unwrap();
    let conn = Connection::open(dir.path().join("plain.db")).unwrap();
    conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, blob BLOB)", []).unwrap();
    let plain_autocommit = measure("Plain SQLite", 1, || {
        for i in 0..write_count {
            conn.execute(
                "INSERT INTO bench (id, data, blob) VALUES (?1, ?2, ?3)",
                rusqlite::params![i, &json_samples[i], &blob_samples[i]],
            ).unwrap();
        }
    });
    drop(conn);

    // Compressed (Eager)
    let dir2 = TempDir::new().unwrap();
    let vfs = CompressedVfs::new(dir2.path(), 3);
    register("bench_auto_eager", vfs).unwrap();
    let conn = Connection::open_with_flags_and_vfs(
        dir2.path().join("compressed.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        "bench_auto_eager",
    ).unwrap();
    conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, blob BLOB)", []).unwrap();
    let eager_autocommit = measure("Compressed", 1, || {
        for i in 0..write_count {
            conn.execute(
                "INSERT INTO bench (id, data, blob) VALUES (?1, ?2, ?3)",
                rusqlite::params![i, &json_samples[i], &blob_samples[i]],
            ).unwrap();
        }
    });
    drop(conn);

    // ===== WAL MODE WRITES (recommended) =====
    println!("\nWAL MODE WRITES ({} rows in single tx):", write_count);
    println!("─────────────────────────────────────────────");

    // Plain SQLite with WAL mode
    let dir = TempDir::new().unwrap();
    let conn = Connection::open(dir.path().join("plain.db")).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap();
    conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, blob BLOB)", []).unwrap();
    let plain_tx = measure("Plain SQLite", 1, || {
        conn.execute("BEGIN", []).unwrap();
        for i in 0..write_count {
            conn.execute(
                "INSERT INTO bench (id, data, blob) VALUES (?1, ?2, ?3)",
                rusqlite::params![i, &json_samples[i], &blob_samples[i]],
            ).unwrap();
        }
        conn.execute("COMMIT", []).unwrap();
    });
    drop(conn);

    // Compressed (Eager) with WAL mode
    let dir2 = TempDir::new().unwrap();
    let vfs = CompressedVfs::new(dir2.path(), 3);
    register("bench_tx_eager", vfs).unwrap();
    let conn = Connection::open_with_flags_and_vfs(
        dir2.path().join("compressed.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        "bench_tx_eager",
    ).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap();
    conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, blob BLOB)", []).unwrap();
    let eager_tx = measure("Compressed (Eager)", 1, || {
        conn.execute("BEGIN", []).unwrap();
        for i in 0..write_count {
            conn.execute(
                "INSERT INTO bench (id, data, blob) VALUES (?1, ?2, ?3)",
                rusqlite::params![i, &json_samples[i], &blob_samples[i]],
            ).unwrap();
        }
        conn.execute("COMMIT", []).unwrap();
    });
    drop(conn);

    // Passthrough (no compression) with WAL mode
    let dir3 = TempDir::new().unwrap();
    let vfs = CompressedVfs::passthrough(dir3.path());
    register("bench_tx_passthrough", vfs).unwrap();
    let conn = Connection::open_with_flags_and_vfs(
        dir3.path().join("passthrough.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        "bench_tx_passthrough",
    ).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap();
    conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, blob BLOB)", []).unwrap();
    let passthrough_tx = measure("Passthrough (no compression)", 1, || {
        conn.execute("BEGIN", []).unwrap();
        for i in 0..write_count {
            conn.execute(
                "INSERT INTO bench (id, data, blob) VALUES (?1, ?2, ?3)",
                rusqlite::params![i, &json_samples[i], &blob_samples[i]],
            ).unwrap();
        }
        conn.execute("COMMIT", []).unwrap();
    });
    drop(conn);

    // Compressed + Encrypted with WAL mode
    #[cfg(feature = "encryption")]
    let encrypted_tx = {
        let dir4 = TempDir::new().unwrap();
        let vfs = CompressedVfs::compressed_encrypted(dir4.path(), 3, "test-password");
        register("bench_tx_encrypted", vfs).unwrap();
        let conn = Connection::open_with_flags_and_vfs(
            dir4.path().join("encrypted.db"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            "bench_tx_encrypted",
        ).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap();
        conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, blob BLOB)", []).unwrap();
        let elapsed = measure("Compressed + Encrypted", 1, || {
            conn.execute("BEGIN", []).unwrap();
            for i in 0..write_count {
                conn.execute(
                    "INSERT INTO bench (id, data, blob) VALUES (?1, ?2, ?3)",
                    rusqlite::params![i, &json_samples[i], &blob_samples[i]],
                ).unwrap();
            }
            conn.execute("COMMIT", []).unwrap();
        });
        drop(conn);
        elapsed
    };

    // ===== READ BENCHMARK =====
    println!("\nREADS ({} rows):", read_count);
    println!("─────────────────────────────────────");

    // Plain SQLite
    let dir = TempDir::new().unwrap();
    let conn = Connection::open(dir.path().join("plain.db")).unwrap();
    conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, blob BLOB)", []).unwrap();
    conn.execute("BEGIN", []).unwrap();
    for i in 0..read_count {
        conn.execute("INSERT INTO bench (id, data, blob) VALUES (?1, ?2, ?3)", rusqlite::params![i, &json_samples[i], &blob_samples[i]]).unwrap();
    }
    conn.execute("COMMIT", []).unwrap();
    let plain_read = measure("Plain SQLite", 1, || {
        let mut stmt = conn.prepare("SELECT id, data, blob FROM bench").unwrap();
        let _rows: Vec<(i64, String, Vec<u8>)> = stmt
            .query_map([], |row| Ok((row.get(0).unwrap(), row.get(1).unwrap(), row.get(2).unwrap())))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
    });
    drop(conn);

    // Compressed (Eager)
    let dir2 = TempDir::new().unwrap();
    let vfs = CompressedVfs::new(dir2.path(), 3);
    register("bench_read_eager", vfs).unwrap();
    let conn = Connection::open_with_flags_and_vfs(
        dir2.path().join("compressed.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        "bench_read_eager",
    ).unwrap();
    conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, blob BLOB)", []).unwrap();
    conn.execute("BEGIN", []).unwrap();
    for i in 0..read_count {
        conn.execute("INSERT INTO bench (id, data, blob) VALUES (?1, ?2, ?3)", rusqlite::params![i, &json_samples[i], &blob_samples[i]]).unwrap();
    }
    conn.execute("COMMIT", []).unwrap();
    let eager_read = measure("Compressed (Eager)", 1, || {
        let mut stmt = conn.prepare("SELECT id, data, blob FROM bench").unwrap();
        let _rows: Vec<(i64, String, Vec<u8>)> = stmt
            .query_map([], |row| Ok((row.get(0).unwrap(), row.get(1).unwrap(), row.get(2).unwrap())))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
    });
    drop(conn);

    // Compressed + Encrypted read benchmark
    #[cfg(feature = "encryption")]
    let encrypted_read = {
        let dir4 = TempDir::new().unwrap();
        let vfs = CompressedVfs::compressed_encrypted(dir4.path(), 3, "test-password");
        register("bench_read_encrypted", vfs).unwrap();
        let conn = Connection::open_with_flags_and_vfs(
            dir4.path().join("encrypted.db"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            "bench_read_encrypted",
        ).unwrap();
        conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, blob BLOB)", []).unwrap();
        conn.execute("BEGIN", []).unwrap();
        for i in 0..read_count {
            conn.execute("INSERT INTO bench (id, data, blob) VALUES (?1, ?2, ?3)", rusqlite::params![i, &json_samples[i], &blob_samples[i]]).unwrap();
        }
        conn.execute("COMMIT", []).unwrap();
        let elapsed = measure("Compressed + Encrypted", 1, || {
            let mut stmt = conn.prepare("SELECT id, data, blob FROM bench").unwrap();
            let _rows: Vec<(i64, String, Vec<u8>)> = stmt
                .query_map([], |row| Ok((row.get(0).unwrap(), row.get(1).unwrap(), row.get(2).unwrap())))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
        });
        drop(conn);
        elapsed
    };

    // ===== FILE SIZE COMPARISON =====
    println!("\nFILE SIZE (1000 rows, ~650 bytes each):");
    println!("─────────────────────────────────────────");

    let dir = TempDir::new().unwrap();
    let conn = Connection::open(dir.path().join("plain.db")).unwrap();
    conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, blob BLOB)", []).unwrap();
    conn.execute("BEGIN", []).unwrap();
    for i in 0..1000 {
        conn.execute("INSERT INTO bench (id, data, blob) VALUES (?1, ?2, ?3)", rusqlite::params![i, &json_samples[i], &blob_samples[i]]).unwrap();
    }
    conn.execute("COMMIT", []).unwrap();
    drop(conn);
    let plain_size = std::fs::metadata(dir.path().join("plain.db")).unwrap().len();
    println!("  Plain SQLite:      {:>10} bytes", plain_size);

    let dir2 = TempDir::new().unwrap();
    let vfs = CompressedVfs::new(dir2.path(), 3);
    register("bench_size", vfs).unwrap();
    let conn = Connection::open_with_flags_and_vfs(
        dir2.path().join("compressed.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        "bench_size",
    ).unwrap();
    conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, blob BLOB)", []).unwrap();
    conn.execute("BEGIN", []).unwrap();
    for i in 0..1000 {
        conn.execute("INSERT INTO bench (id, data, blob) VALUES (?1, ?2, ?3)", rusqlite::params![i, &json_samples[i], &blob_samples[i]]).unwrap();
    }
    conn.execute("COMMIT", []).unwrap();
    drop(conn);
    let compressed_size = std::fs::metadata(dir2.path().join("compressed.db")).unwrap().len();
    let ratio = plain_size as f64 / compressed_size as f64;
    println!("  Compressed:        {:>10} bytes ({:.1}x smaller)", compressed_size, ratio);

    #[cfg(feature = "encryption")]
    {
        let dir3 = TempDir::new().unwrap();
        let vfs = CompressedVfs::compressed_encrypted(dir3.path(), 3, "test-password");
        register("bench_size_enc", vfs).unwrap();
        let conn = Connection::open_with_flags_and_vfs(
            dir3.path().join("encrypted.db"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            "bench_size_enc",
        ).unwrap();
        conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, blob BLOB)", []).unwrap();
        conn.execute("BEGIN", []).unwrap();
        for i in 0..1000 {
            conn.execute("INSERT INTO bench (id, data, blob) VALUES (?1, ?2, ?3)", rusqlite::params![i, &json_samples[i], &blob_samples[i]]).unwrap();
        }
        conn.execute("COMMIT", []).unwrap();
        drop(conn);
        let encrypted_size = std::fs::metadata(dir3.path().join("encrypted.db")).unwrap().len();
        let enc_ratio = plain_size as f64 / encrypted_size as f64;
        println!("  Compressed+Encrypted: {:>10} bytes ({:.1}x smaller)", encrypted_size, enc_ratio);
    }

    // ===== SUMMARY =====
    println!("\n==============================");
    println!("SUMMARY (vs Plain SQLite):");
    println!("==============================");

    println!("\nAutocommit mode (per-row fsync):");
    println!("  Compressed: {:.1}x slower writes", eager_autocommit.as_secs_f64() / plain_autocommit.as_secs_f64());

    println!("\nWAL mode (recommended):");
    println!("  Compressed Eager: {:.1}x slower writes, {:.1}x slower reads",
        eager_tx.as_secs_f64() / plain_tx.as_secs_f64(),
        eager_read.as_secs_f64() / plain_read.as_secs_f64());
    println!("  Passthrough:      {:.1}x slower writes",
        passthrough_tx.as_secs_f64() / plain_tx.as_secs_f64());
    #[cfg(feature = "encryption")]
    println!("  Compressed+Encrypted: {:.1}x slower writes, {:.1}x slower reads",
        encrypted_tx.as_secs_f64() / plain_tx.as_secs_f64(),
        encrypted_read.as_secs_f64() / plain_read.as_secs_f64());

    println!("\nStorage:");
    println!("  Compression ratio: {:.1}x smaller", ratio);
}
