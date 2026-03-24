use rusqlite::{Connection, OpenFlags};
use turbolite::{register, CompressedVfs};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

/// Test that triggers frequent checkpoints to force index refreshes
/// This helps profile refresh performance between encrypted and compressed modes
#[test]
fn test_checkpoint_refresh_performance() {
    let dir = tempfile::tempdir().unwrap();
    let vfs = CompressedVfs::new(dir.path(), 3);
    register("test_checkpoint", vfs).expect("Failed to register VFS");

    let db_path = dir.path().join("test.db");
    let db_path = Arc::new(db_path);

    // Create database with larger text documents
    let large_text = "x".repeat(5000); // ~5KB per row
    let num_docs = 1000;

    {
        let conn = Connection::open_with_flags_and_vfs(
            &*db_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
            "test_checkpoint",
        ).unwrap();

        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap();
        conn.execute("CREATE TABLE articles (author TEXT, title TEXT, body TEXT)", []).unwrap();

        conn.execute("BEGIN", []).unwrap();
        for i in 0..num_docs {
            conn.execute(
                "INSERT INTO articles (author, title, body) VALUES (?, ?, ?)",
                (format!("Author_{}", i), format!("Title_{}", i), &large_text),
            ).unwrap();
        }
        conn.execute("COMMIT", []).unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        println!("Initial data inserted: {} documents", num_docs);
    }

    // Collect valid rowids
    let valid_ids: Vec<i64> = {
        let conn = Connection::open_with_flags_and_vfs(
            &*db_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY,
            "test_checkpoint",
        ).unwrap();

        let mut stmt = conn.prepare("SELECT rowid FROM articles").unwrap();
        let rows = stmt.query_map([], |row| row.get(0)).unwrap();
        rows.filter_map(|r| r.ok()).collect()
    };
    let valid_ids = Arc::new(valid_ids);

    let stop_flag = Arc::new(AtomicBool::new(false));
    let read_count = Arc::new(AtomicUsize::new(0));
    let write_count = Arc::new(AtomicUsize::new(0));

    let duration_secs = 5;
    let num_readers = 4;
    let num_writers = 2;

    let mut handles = Vec::new();

    // Spawn readers
    for reader_id in 0..num_readers {
        let db_path = Arc::clone(&db_path);
        let valid_ids = Arc::clone(&valid_ids);
        let stop_flag = Arc::clone(&stop_flag);
        let read_count = Arc::clone(&read_count);

        handles.push(thread::spawn(move || {
            let conn = Connection::open_with_flags_and_vfs(
                &*db_path,
                OpenFlags::SQLITE_OPEN_READ_ONLY,
                "test_checkpoint",
            ).unwrap();

            let mut i = 0usize;
            while !stop_flag.load(Ordering::Relaxed) {
                let idx = (i + reader_id * 1000) % valid_ids.len();
                let rowid = valid_ids[idx];

                match conn.query_row(
                    "SELECT author, title, body FROM articles WHERE rowid = ?",
                    [rowid],
                    |row| {
                        let _: String = row.get(0)?;
                        let _: String = row.get(1)?;
                        let _: String = row.get(2)?;
                        Ok(())
                    },
                ) {
                    Ok(()) => {
                        read_count.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        eprintln!("Reader {} ERROR: {}", reader_id, e);
                        break;
                    }
                }
                i += 1;
            }
        }));
    }

    // Spawn writers
    for writer_id in 0..num_writers {
        let db_path = Arc::clone(&db_path);
        let stop_flag = Arc::clone(&stop_flag);
        let write_count = Arc::clone(&write_count);

        handles.push(thread::spawn(move || {
            let conn = Connection::open_with_flags_and_vfs(
                &*db_path,
                OpenFlags::SQLITE_OPEN_READ_WRITE,
                "test_checkpoint",
            ).unwrap();

            conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap();

            let body = "y".repeat(1000);
            let mut i = 0usize;

            while !stop_flag.load(Ordering::Relaxed) {
                match conn.execute(
                    "INSERT INTO articles (author, title, body) VALUES (?, ?, ?)",
                    (format!("NewAuthor_{}_{}", writer_id, i), format!("NewTitle_{}_{}", writer_id, i), &body),
                ) {
                    Ok(_) => {
                        write_count.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        eprintln!("Writer {} ERROR: {}", writer_id, e);
                        break;
                    }
                }
                i += 1;
            }
        }));
    }

    // Checkpoint thread - triggers checkpoints every 100ms to force refreshes
    let db_path_checkpoint = Arc::clone(&db_path);
    let stop_flag_checkpoint = Arc::clone(&stop_flag);
    let checkpoint_handle = thread::spawn(move || {
        let conn = Connection::open_with_flags_and_vfs(
            &*db_path_checkpoint,
            OpenFlags::SQLITE_OPEN_READ_WRITE,
            "test_checkpoint",
        ).unwrap();

        let mut checkpoint_count = 0;
        while !stop_flag_checkpoint.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(100));
            if let Err(e) = conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE)") {
                eprintln!("Checkpoint ERROR: {}", e);
                break;
            }
            checkpoint_count += 1;
        }
        println!("Checkpoints triggered: {}", checkpoint_count);
    });

    // Run for specified duration
    thread::sleep(Duration::from_secs(duration_secs));
    stop_flag.store(true, Ordering::Relaxed);

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
    checkpoint_handle.join().unwrap();

    let total_reads = read_count.load(Ordering::Relaxed);
    let total_writes = write_count.load(Ordering::Relaxed);

    println!("\nResults:");
    println!("  Reads: {} ({}/sec)", total_reads, total_reads / duration_secs as usize);
    println!("  Writes: {} ({}/sec)", total_writes, total_writes / duration_secs as usize);
}
