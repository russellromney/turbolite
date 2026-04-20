use rusqlite::{Connection, OpenFlags};
use turbolite::tiered::{TurboliteVfs, TurboliteConfig};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn test_concurrent_read_write() {
    let dir = tempfile::tempdir().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().into(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new_local(config).expect("Failed to create VFS");
    turbolite::tiered::register("test_concurrent", vfs).expect("Failed to register VFS");

    let db_path = dir.path().join("test.db");
    let db_path = Arc::new(db_path);

    // Create database and insert initial data
    {
        let conn = Connection::open_with_flags_and_vfs(
            &*db_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
            "test_concurrent",
        ).unwrap();

        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap();
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)", []).unwrap();

        for i in 0..1000 {
            conn.execute("INSERT INTO test (id, data) VALUES (?, ?)", (i, format!("initial_{}", i))).unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        println!("Initial data inserted");
    }

    let db_path_reader = Arc::clone(&db_path);
    let db_path_writer = Arc::clone(&db_path);

    // Reader thread
    let reader = thread::spawn(move || {
        let conn = Connection::open_with_flags_and_vfs(
            &*db_path_reader,
            OpenFlags::SQLITE_OPEN_READ_ONLY,
            "test_concurrent",
        ).unwrap();

        for i in 0..100 {
            let rowid = (i % 1000) + 1;
            match conn.query_row(
                "SELECT data FROM test WHERE id = ?",
                [rowid],
                |row| row.get::<_, String>(0),
            ) {
                Ok(data) => {
                    if i % 20 == 0 {
                        println!("Reader: row {} = {}", rowid, &data[..20.min(data.len())]);
                    }
                }
                Err(e) => {
                    eprintln!("Reader ERROR at row {}: {}", rowid, e);
                    return Err(e.to_string());
                }
            }
            thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    });

    // Writer thread
    let writer = thread::spawn(move || {
        let conn = Connection::open_with_flags_and_vfs(
            &*db_path_writer,
            OpenFlags::SQLITE_OPEN_READ_WRITE,
            "test_concurrent",
        ).unwrap();

        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap();

        for i in 0..50 {
            match conn.execute(
                "INSERT INTO test (data) VALUES (?)",
                (format!("new_row_{}", i),),
            ) {
                Ok(_) => {
                    if i % 10 == 0 {
                        println!("Writer: inserted row {}", i);
                    }
                }
                Err(e) => {
                    eprintln!("Writer ERROR: {}", e);
                    return Err(e.to_string());
                }
            }
            thread::sleep(Duration::from_millis(20));
        }
        Ok(())
    });

    let reader_result = reader.join().unwrap();
    let writer_result = writer.join().unwrap();

    assert!(reader_result.is_ok(), "Reader failed: {:?}", reader_result);
    assert!(writer_result.is_ok(), "Writer failed: {:?}", writer_result);

    // NOTE: do NOT call clear_all_caches() here. It wipes the global
    // IN_PROCESS_LOCKS map, which breaks WAL index lock coordination for
    // any concurrent test that shares the same process.
}

/// Test without WAL mode to isolate the issue
#[test]
fn test_concurrent_no_wal() {
    let dir = tempfile::tempdir().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().into(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new_local(config).expect("Failed to create VFS");
    turbolite::tiered::register("test_no_wal", vfs).expect("Failed to register VFS");

    let db_path = dir.path().join("test.db");
    let db_path = Arc::new(db_path);

    // Create database with DELETE journal mode (not WAL)
    let large_text = "x".repeat(5000);
    let num_docs = 2000;

    {
        let conn = Connection::open_with_flags_and_vfs(
            &*db_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
            "test_no_wal",
        ).unwrap();

        conn.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA synchronous=NORMAL;").unwrap();
        conn.execute("CREATE TABLE articles (author TEXT, title TEXT, body TEXT)", []).unwrap();

        conn.execute("BEGIN", []).unwrap();
        for i in 0..num_docs {
            conn.execute(
                "INSERT INTO articles (author, title, body) VALUES (?, ?, ?)",
                (format!("Author_{}", i), format!("Title_{}", i), &large_text),
            ).unwrap();
        }
        conn.execute("COMMIT", []).unwrap();
        println!("Initial data inserted: {} documents", num_docs);
    }

    let valid_ids: Vec<i64> = {
        let conn = Connection::open_with_flags_and_vfs(
            &*db_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY,
            "test_no_wal",
        ).unwrap();

        let mut stmt = conn.prepare("SELECT rowid FROM articles").unwrap();
        let rows = stmt.query_map([], |row| row.get(0)).unwrap();
        rows.filter_map(|r| r.ok()).collect()
    };
    let valid_ids = Arc::new(valid_ids);

    let stop_flag = Arc::new(AtomicBool::new(false));
    let read_count = Arc::new(AtomicUsize::new(0));
    let read_errors = Arc::new(AtomicUsize::new(0));

    let duration_secs = 3;
    let num_readers = 4;

    let mut handles = Vec::new();

    // Spawn readers ONLY (no concurrent writes in DELETE journal mode)
    for reader_id in 0..num_readers {
        let db_path = Arc::clone(&db_path);
        let valid_ids = Arc::clone(&valid_ids);
        let stop_flag = Arc::clone(&stop_flag);
        let read_count = Arc::clone(&read_count);
        let read_errors = Arc::clone(&read_errors);

        handles.push(thread::spawn(move || {
            let conn = Connection::open_with_flags_and_vfs(
                &*db_path,
                OpenFlags::SQLITE_OPEN_READ_ONLY,
                "test_no_wal",
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
                        eprintln!("Reader {} ERROR at row {}: {} (code: {:?})", reader_id, rowid, e, e.sqlite_error_code());
                        read_errors.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                }
                i += 1;
            }
        }));
    }

    // Run for specified duration
    thread::sleep(Duration::from_secs(duration_secs));
    stop_flag.store(true, Ordering::Relaxed);

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }

    let reads = read_count.load(Ordering::Relaxed);
    let r_errors = read_errors.load(Ordering::Relaxed);

    println!("\nResults:");
    println!("  Reads: {} ({}/sec)", reads, reads / duration_secs as usize);
    println!("  Read errors: {}", r_errors);

    assert_eq!(r_errors, 0, "Had {} read errors", r_errors);

    // NOTE: do NOT call clear_all_caches() here. It wipes the global
    // IN_PROCESS_LOCKS map, which breaks WAL index lock coordination for
    // any concurrent test that shares the same process.
}

/// More aggressive test matching benchmark conditions
#[test]
fn test_concurrent_high_throughput() {
    let dir = tempfile::tempdir().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().into(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new_local(config).expect("Failed to create VFS");
    turbolite::tiered::register("test_high_throughput", vfs).expect("Failed to register VFS");

    let db_path = dir.path().join("test.db");
    let db_path = Arc::new(db_path);

    // Create database with larger text documents (like Gutenberg corpus)
    let large_text = "x".repeat(5000); // ~5KB per row
    let num_docs = 2000;

    {
        let conn = Connection::open_with_flags_and_vfs(
            &*db_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
            "test_high_throughput",
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
            "test_high_throughput",
        ).unwrap();

        let mut stmt = conn.prepare("SELECT rowid FROM articles").unwrap();
        let rows = stmt.query_map([], |row| row.get(0)).unwrap();
        rows.filter_map(|r| r.ok()).collect()
    };
    let valid_ids = Arc::new(valid_ids);

    let stop_flag = Arc::new(AtomicBool::new(false));
    let read_count = Arc::new(AtomicUsize::new(0));
    let write_count = Arc::new(AtomicUsize::new(0));
    let read_errors = Arc::new(AtomicUsize::new(0));
    let write_errors = Arc::new(AtomicUsize::new(0));

    let duration_secs = 3;
    let num_readers = 4;
    let num_writers = 2;

    let mut handles = Vec::new();

    // Spawn readers
    for reader_id in 0..num_readers {
        let db_path = Arc::clone(&db_path);
        let valid_ids = Arc::clone(&valid_ids);
        let stop_flag = Arc::clone(&stop_flag);
        let read_count = Arc::clone(&read_count);
        let read_errors = Arc::clone(&read_errors);

        handles.push(thread::spawn(move || {
            let conn = Connection::open_with_flags_and_vfs(
                &*db_path,
                OpenFlags::SQLITE_OPEN_READ_ONLY,
                "test_high_throughput",
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
                        eprintln!("Reader {} ERROR at row {}: {} (code: {:?})", reader_id, rowid, e, e.sqlite_error_code());
                        read_errors.fetch_add(1, Ordering::Relaxed);
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
        let write_errors = Arc::clone(&write_errors);

        handles.push(thread::spawn(move || {
            let conn = Connection::open_with_flags_and_vfs(
                &*db_path,
                OpenFlags::SQLITE_OPEN_READ_WRITE,
                "test_high_throughput",
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
                        eprintln!("Writer {} ERROR: {} (code: {:?})", writer_id, e, e.sqlite_error_code());
                        write_errors.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                }
                i += 1;
            }
        }));
    }

    // Run for specified duration
    thread::sleep(Duration::from_secs(duration_secs));
    stop_flag.store(true, Ordering::Relaxed);

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }

    let reads = read_count.load(Ordering::Relaxed);
    let writes = write_count.load(Ordering::Relaxed);
    let r_errors = read_errors.load(Ordering::Relaxed);
    let w_errors = write_errors.load(Ordering::Relaxed);

    println!("\nResults:");
    println!("  Reads: {} ({}/sec)", reads, reads / duration_secs as usize);
    println!("  Writes: {} ({}/sec)", writes, writes / duration_secs as usize);
    println!("  Read errors: {}", r_errors);
    println!("  Write errors: {}", w_errors);

    assert_eq!(r_errors, 0, "Had {} read errors", r_errors);
    assert_eq!(w_errors, 0, "Had {} write errors", w_errors);

    // NOTE: do NOT call clear_all_caches() here. It wipes the global
    // IN_PROCESS_LOCKS map, which breaks WAL index lock coordination for
    // any concurrent test that shares the same process.
}
