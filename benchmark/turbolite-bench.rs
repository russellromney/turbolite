//! turbolite-bench - turbolite Benchmark CLI
//!
//! Benchmarks VFS modes using real corpus data
//!
//! Subcommands:
//!   bench-db        Benchmark VFS modes (read/write throughput)
//!   bench-compact   Benchmark parallel vs serial compaction
//!
//! Presets:
//!   --preset 100mb    100MB Gutenberg corpus, 10MB cache, 20MB mmap
//!   --preset 50mb     50MB Gutenberg corpus, 5MB cache, 10MB mmap
//!   --preset 10mb     10MB quick test, 1MB cache, 2MB mmap

use clap::{Parser, Subcommand};
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use turbolite::{clear_all_caches, register, CompressedVfs};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

/// Preset benchmark configurations
struct PresetConfig {
    db_path: &'static str,
    cache_kb: i64,
    mmap_kb: i64,
    duration_secs: u64,
}

fn get_preset(name: &str) -> Option<PresetConfig> {
    match name {
        "100mb" => Some(PresetConfig {
            db_path: "scripts/real_100mb.db",
            cache_kb: 10 * 1024,  // 10MB
            mmap_kb: 20 * 1024,   // 20MB
            duration_secs: 10,
        }),
        "50mb" => Some(PresetConfig {
            db_path: "scripts/real_50mb.db",
            cache_kb: 5 * 1024,   // 5MB
            mmap_kb: 10 * 1024,   // 10MB
            duration_secs: 10,
        }),
        "10mb" => Some(PresetConfig {
            db_path: "scripts/real_10mb.db",
            cache_kb: 1024,       // 1MB
            mmap_kb: 2 * 1024,    // 2MB
            duration_secs: 5,
        }),
        "500mb" => Some(PresetConfig {
            db_path: "scripts/real_500mb.db",
            cache_kb: 50 * 1024,  // 50MB
            mmap_kb: 100 * 1024,  // 100MB
            duration_secs: 15,
        }),
        _ => None,
    }
}

#[derive(Parser)]
#[command(name = "turbolite-bench")]
#[command(about = "SQLite Compression+Encryption Benchmark CLI\n\nSubcommands: bench-db, bench-compact\nPresets: 10mb, 50mb, 100mb, 500mb")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Use a preset configuration (10mb, 50mb, 100mb, 500mb)
    #[arg(short, long)]
    preset: Option<String>,

    /// Path to the source database file (required if no preset)
    #[arg(short, long)]
    database: Option<PathBuf>,

    /// Duration in seconds to run benchmark (readers and writers run concurrently)
    #[arg(long, default_value = "10")]
    duration_secs: u64,

    /// Number of concurrent reader threads
    #[arg(long, default_value = "4")]
    reader_threads: usize,

    /// Number of concurrent writer threads
    #[arg(long, default_value = "1")]
    writer_threads: usize,

    /// Page cache size in KB (overrides preset/percentage)
    #[arg(long)]
    cache_kb: Option<i64>,

    /// Memory-mapped I/O size in KB (overrides preset)
    #[arg(long)]
    mmap_kb: Option<i64>,

    /// Page cache size as percentage of logical DB size (0.0-1.0)
    #[arg(long, default_value = "0.20")]
    cache_percent: f64,

    /// Memory-mapped I/O size as percentage of logical DB size (0.0-1.0)
    #[arg(long, default_value = "0.50")]
    mmap_percent: f64,

    /// WAL autocheckpoint threshold in pages (0 = disable)
    #[arg(long, default_value = "5000")]
    wal_autocheckpoint: i64,

    /// Encryption key (for encrypted modes)
    #[arg(long, default_value = "bench-key")]
    encryption_key: String,

    /// Output format: console, json, or markdown
    #[arg(long, default_value = "console")]
    output_format: String,

    /// Output file path
    #[arg(long)]
    output_file: Option<PathBuf>,

    /// Use Project Gutenberg texts to generate test data (no --database or --preset needed)
    #[arg(long)]
    gutenberg: bool,

    /// Corpus size in MB when using --gutenberg (default: 10)
    #[arg(long, default_value = "10")]
    corpus_size_mb: usize,

    /// VFS modes to test (comma-separated: passthrough,compressed,encrypted,both)
    #[arg(long, default_value = "passthrough,compressed,encrypted,both")]
    modes: String,

    /// Force fresh generation (ignore cached corpus and databases)
    #[arg(long)]
    fresh: bool,
}

/// Get the cache directory for benchmark data
fn get_cache_dir() -> PathBuf {
    // Use ~/.cache/turbolite-bench/ on Unix, or temp dir on other platforms
    if let Some(home) = dirs_home() {
        home.join(".cache").join("turbolite-bench")
    } else {
        std::env::temp_dir().join("turbolite-bench")
    }
}

fn dirs_home() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

#[derive(Subcommand)]
enum Commands {
    /// Benchmark VFS modes (read/write throughput)
    BenchDb,
    /// Benchmark parallel vs serial compaction
    BenchCompact {
        /// Number of rows to insert (more rows = longer compaction, ignored with --gutenberg)
        #[arg(long, default_value = "1000")]
        rows: usize,

        /// Number of iterations for averaging
        #[arg(long, default_value = "3")]
        iterations: usize,

        /// Compression level (1-22)
        #[arg(long, default_value = "3")]
        compression_level: i32,

        /// Use Project Gutenberg texts for realistic compression benchmarks
        #[arg(long)]
        gutenberg: bool,

        /// Corpus size in MB when using --gutenberg (default: 10)
        #[arg(long, default_value = "10")]
        corpus_size_mb: usize,
    },
}

#[derive(Serialize, Deserialize, Debug)]
struct BenchmarkResult {
    mode: String,
    document_count: usize,
    read_ops_per_sec: f64,
    read_p50_us: f64,
    read_p99_us: f64,
    write_ops_per_sec: f64,
    write_p50_us: f64,
    write_p99_us: f64,
    file_size_mb: f64,
    setup_duration_secs: f64,
    total_duration_secs: f64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Handle subcommands
    match &cli.command {
        Some(Commands::BenchCompact { rows, iterations, compression_level, gutenberg, corpus_size_mb }) => {
            return bench_compact_command(*rows, *iterations, *compression_level, *gutenberg, *corpus_size_mb);
        }
        Some(Commands::BenchDb) | None => {
            // Default behavior: bench-db
        }
    }

    // Parse modes
    let modes: Vec<&str> = cli.modes.split(',').map(|s| s.trim()).collect();

    // Handle --gutenberg mode (generate test data inline)
    if cli.gutenberg {
        return bench_gutenberg_db(
            cli.corpus_size_mb,
            cli.duration_secs,
            cli.reader_threads,
            cli.writer_threads,
            cli.cache_kb,
            cli.mmap_kb,
            cli.cache_percent,
            cli.mmap_percent,
            cli.wal_autocheckpoint,
            &cli.encryption_key,
            &cli.output_format,
            cli.output_file.as_deref(),
            &modes,
            cli.fresh,
        );
    }

    // Resolve database path and settings from preset or CLI args
    let (database, duration_secs, cache_kb, mmap_kb) = if let Some(preset_name) = &cli.preset {
        let preset = get_preset(preset_name).ok_or_else(|| {
            format!("Unknown preset '{}'. Available: 10mb, 50mb, 100mb, 500mb", preset_name)
        })?;
        let db_path = PathBuf::from(preset.db_path);
        (
            db_path,
            cli.duration_secs.max(preset.duration_secs),
            cli.cache_kb.unwrap_or(preset.cache_kb),
            cli.mmap_kb.unwrap_or(preset.mmap_kb),
        )
    } else if let Some(db_path) = &cli.database {
        (db_path.clone(), cli.duration_secs, cli.cache_kb.unwrap_or(0), cli.mmap_kb.unwrap_or(0))
    } else {
        eprintln!("Error: Either --preset, --database, or --gutenberg is required");
        eprintln!("  Example: turbolite-bench --preset 50mb");
        eprintln!("  Example: turbolite-bench --database path/to/db.db");
        eprintln!("  Example: turbolite-bench --gutenberg --corpus-size-mb 20");
        std::process::exit(1);
    };

    if !database.exists() {
        eprintln!("Error: Database file not found: {}", database.display());
        std::process::exit(1);
    }

    println!("=== SQLCEs Database Benchmark ===");
    println!("Database: {}", database.display());
    println!("Modes: {:?}", modes);
    println!("Duration: {}s with {}x readers + {}x writers running concurrently",
             duration_secs, cli.reader_threads, cli.writer_threads);
    if cache_kb > 0 {
        println!("Cache: {} KB, mmap: {} KB", cache_kb, mmap_kb);
    } else {
        println!("Cache: {:.0}% of logical size, mmap: {:.0}% of logical size", cli.cache_percent * 100.0, cli.mmap_percent * 100.0);
    }
    println!();

    let mut results = Vec::new();

    for mode in &modes {
        println!("Testing {} mode...", mode);
        let enc_key = if mode.contains("encrypted") || mode == &"both" {
            Some(cli.encryption_key.as_str())
        } else {
            None
        };

        match bench_existing_db(&database, mode, duration_secs,
                               cli.reader_threads, cli.writer_threads,
                               cache_kb, mmap_kb, cli.cache_percent, cli.mmap_percent,
                               cli.wal_autocheckpoint, enc_key) {
            Ok(result) => {
                println!("  ✓ Complete - {:.0} read ops/sec, {:.0} write ops/sec, {:.2} MB",
                         result.read_ops_per_sec, result.write_ops_per_sec, result.file_size_mb);
                results.push(result);
            }
            Err(e) => {
                println!("  ✗ Failed: {}", e);
            }
        }
        println!();
    }

    match cli.output_format.to_lowercase().as_str() {
        "json" => {
            let json = serde_json::to_string_pretty(&results)?;
            if let Some(path) = cli.output_file {
                std::fs::write(path, json)?;
            } else {
                println!("{}", json);
            }
        }
        "markdown" => {
            let markdown = format_markdown_report(&results);
            if let Some(path) = cli.output_file {
                std::fs::write(path, markdown)?;
            } else {
                println!("{}", markdown);
            }
        }
        _ => {
            print_comparison_table(&results);
            if let Some(path) = cli.output_file {
                let text = format_comparison_table(&results);
                std::fs::write(path, text)?;
            }
        }
    }

    Ok(())
}

/// Benchmark VFS modes using Gutenberg-generated data
fn bench_gutenberg_db(
    corpus_size_mb: usize,
    duration_secs: u64,
    reader_threads: usize,
    writer_threads: usize,
    cache_kb_override: Option<i64>,
    mmap_kb_override: Option<i64>,
    cache_percent: f64,
    mmap_percent: f64,
    wal_autocheckpoint: i64,
    encryption_key: &str,
    output_format: &str,
    output_file: Option<&std::path::Path>,
    modes: &[&str],
    fresh: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::AtomicU32;
    static VFS_COUNTER: AtomicU32 = AtomicU32::new(0);

    // Clear VFS caches when running fresh to avoid stale state
    if fresh {
        clear_all_caches();
    }

    // Get or generate corpus (uses cache by default)
    let corpus = get_or_generate_corpus(corpus_size_mb, fresh)?;

    println!();
    println!("=== SQLCEs Database Benchmark (Gutenberg Data) ===");
    println!("Corpus: {} documents, {} MB", corpus.len(), corpus_size_mb);
    println!("Modes: {:?}", modes);
    println!("Duration: {}s with {}x readers + {}x writers running concurrently",
             duration_secs, reader_threads, writer_threads);
    if cache_kb_override.unwrap_or(0) > 0 {
        println!("Cache: {} KB, mmap: {} KB", cache_kb_override.unwrap(), mmap_kb_override.unwrap_or(0));
    } else {
        println!("Cache: {:.0}% of logical size, mmap: {:.0}% of logical size", cache_percent * 100.0, mmap_percent * 100.0);
    }
    println!();

    let mut results = Vec::new();

    for mode in modes {
        println!("Testing {} mode...", mode);

        let result = bench_with_corpus(
            &corpus,
            corpus_size_mb,
            *mode,
            duration_secs,
            reader_threads,
            writer_threads,
            cache_kb_override.unwrap_or(0),
            mmap_kb_override.unwrap_or(0),
            cache_percent,
            mmap_percent,
            wal_autocheckpoint,
            encryption_key,
            fresh,
            &VFS_COUNTER,
        );

        match result {
            Ok(res) => {
                println!("  ✓ Complete - {:.0} read ops/sec, {:.0} write ops/sec, {:.2} MB",
                         res.read_ops_per_sec, res.write_ops_per_sec, res.file_size_mb);
                results.push(res);
            }
            Err(e) => {
                println!("  ✗ Failed: {}", e);
            }
        }
        println!();
    }

    // Output results
    match output_format.to_lowercase().as_str() {
        "json" => {
            let json = serde_json::to_string_pretty(&results)?;
            if let Some(path) = output_file {
                std::fs::write(path, json)?;
            } else {
                println!("{}", json);
            }
        }
        "markdown" => {
            let markdown = format_markdown_report(&results);
            if let Some(path) = output_file {
                std::fs::write(path, markdown)?;
            } else {
                println!("{}", markdown);
            }
        }
        _ => {
            print_comparison_table(&results);
            if let Some(path) = output_file {
                let text = format_comparison_table(&results);
                std::fs::write(path, text)?;
            }
        }
    }

    Ok(())
}

/// Benchmark a specific VFS mode with provided corpus data
fn bench_with_corpus(
    corpus: &[(String, String, String)],
    corpus_size_mb: usize,
    mode: &str,
    duration_secs: u64,
    reader_threads: usize,
    writer_threads: usize,
    cache_kb_override: i64,
    mmap_kb_override: i64,
    cache_percent: f64,
    mmap_percent: f64,
    wal_autocheckpoint: i64,
    encryption_key: &str,
    fresh: bool,
    vfs_counter: &std::sync::atomic::AtomicU32,
) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
    let start_total = Instant::now();
    let cache_dir = get_cache_dir();
    std::fs::create_dir_all(&cache_dir)?;

    // Database cache paths
    // For passthrough mode, we use the source directly (plain SQLite)
    // For other modes, we have a separate converted database
    let source_db_dir = cache_dir.join(format!("db_{}mb_source", corpus_size_mb));
    let source_db_path = source_db_dir.join("bench.db");
    let target_db_dir = cache_dir.join(format!("db_{}mb_{}", corpus_size_mb, mode));
    let target_db_path = target_db_dir.join("bench.db");

    // Setup timing
    let setup_start = Instant::now();

    // Step 1: Ensure plain SQLite source database exists (no VFS - regular SQLite format)
    let source_needs_creation = fresh || !source_db_path.exists();
    if source_needs_creation {
        println!("  Creating source database (plain SQLite)...");
        std::fs::create_dir_all(&source_db_dir)?;

        // Remove old files if fresh
        if source_db_path.exists() {
            std::fs::remove_file(&source_db_path)?;
        }
        let wal_path = source_db_dir.join("bench.db-wal");
        let shm_path = source_db_dir.join("bench.db-shm");
        if wal_path.exists() { std::fs::remove_file(&wal_path)?; }
        if shm_path.exists() { std::fs::remove_file(&shm_path)?; }

        // Create plain SQLite database (NO VFS) for source
        let conn = Connection::open(&source_db_path)?;

        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
        conn.execute("CREATE TABLE articles (author TEXT, title TEXT, body TEXT)", [])?;

        // Bulk insert corpus
        conn.execute("BEGIN", [])?;
        for (author, title, body) in corpus {
            conn.execute(
                "INSERT INTO articles (author, title, body) VALUES (?, ?, ?)",
                (author, title, body),
            )?;
        }
        conn.execute("COMMIT", [])?;
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
        drop(conn);

        println!("  Source database created: {} documents", corpus.len());
    } else {
        println!("  Using cached source database");
    }

    // Step 2: Create/reuse target database for the specific mode
    let target_needs_creation = fresh || !target_db_path.exists();

    // Create VFS for target
    std::fs::create_dir_all(&target_db_dir)?;
    let vfs_id = vfs_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let vfs_name = format!("bench_{}_{}", mode, vfs_id);

    let vfs = match mode {
        "passthrough" => CompressedVfs::passthrough(&target_db_dir),
        "compressed" => CompressedVfs::new(&target_db_dir, 3),
        "encrypted" => {
            #[cfg(feature = "encryption")]
            {
                CompressedVfs::encrypted(&target_db_dir, encryption_key)
            }
            #[cfg(not(feature = "encryption"))]
            {
                return Err("Encryption not enabled".into());
            }
        }
        "both" => {
            #[cfg(feature = "encryption")]
            {
                CompressedVfs::compressed_encrypted(&target_db_dir, 3, encryption_key)
            }
            #[cfg(not(feature = "encryption"))]
            {
                return Err("Encryption not enabled".into());
            }
        }
        _ => return Err(format!("Unknown mode: {}", mode).into()),
    };

    register(&vfs_name, vfs)?;

    if target_needs_creation {
        println!("  Converting to {} mode...", mode);

        // Remove old files if fresh
        if target_db_path.exists() {
            std::fs::remove_file(&target_db_path)?;
        }
        let wal_path = target_db_dir.join("bench.db-wal");
        let shm_path = target_db_dir.join("bench.db-shm");
        if wal_path.exists() { std::fs::remove_file(&wal_path)?; }
        if shm_path.exists() { std::fs::remove_file(&shm_path)?; }

        // Copy data from source (plain SQLite) to target (through VFS)
        // This applies to ALL modes including passthrough
        let source_conn = Connection::open_with_flags(
            &source_db_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;

        let target_conn = Connection::open_with_flags_and_vfs(
            &target_db_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )?;

        target_conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
        target_conn.execute("CREATE TABLE articles (author TEXT, title TEXT, body TEXT)", [])?;
        target_conn.execute("BEGIN", [])?;

        // Copy all data from source to target
        {
            let mut stmt = source_conn.prepare("SELECT author, title, body FROM articles")?;
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                let author: String = row.get(0)?;
                let title: String = row.get(1)?;
                let body: String = row.get(2)?;
                target_conn.execute(
                    "INSERT INTO articles (author, title, body) VALUES (?, ?, ?)",
                    (&author, &title, &body),
                )?;
            }
        }

        target_conn.execute("COMMIT", [])?;
        target_conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;

        println!("  Conversion complete");
    } else {
        println!("  Using cached {} database", mode);
    }

    // Open the target database for benchmarking
    let db_path = target_db_path;
    let conn = Connection::open_with_flags_and_vfs(
        &db_path,
        OpenFlags::SQLITE_OPEN_READ_WRITE,
        &vfs_name,
    )?;

    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;

    let doc_count: usize = conn.query_row("SELECT COUNT(*) FROM articles", [], |r| r.get(0))?;
    let setup_duration = setup_start.elapsed().as_secs_f64();

    // Configure cache and mmap
    let page_count: i64 = conn.query_row("PRAGMA page_count", [], |row| row.get(0))?;
    let page_size: i64 = conn.query_row("PRAGMA page_size", [], |row| row.get(0))?;
    let logical_size_kb = (page_count * page_size) / 1024;

    let cache_size_kb = if cache_kb_override > 0 {
        cache_kb_override
    } else {
        (logical_size_kb as f64 * cache_percent) as i64
    };
    let mmap_size_kb = if mmap_kb_override > 0 {
        mmap_kb_override
    } else {
        (logical_size_kb as f64 * mmap_percent) as i64
    };

    let file_size_mb = std::fs::metadata(&db_path)?.len() as f64 / (1024.0 * 1024.0);

    conn.execute(&format!("PRAGMA cache_size = -{}", cache_size_kb), [])?;
    conn.execute(&format!("PRAGMA mmap_size = {}", mmap_size_kb * 1024), [])?;

    // Collect rowids for random access
    let mut valid_ids: Vec<i64> = Vec::new();
    let mut stmt = conn.prepare("SELECT rowid FROM articles")?;
    let rows = stmt.query_map([], |row| row.get(0))?;
    for row_result in rows {
        valid_ids.push(row_result?);
    }
    drop(stmt);
    drop(conn);

    // Share state across threads
    let valid_ids = Arc::new(valid_ids);
    let db_path = Arc::new(db_path);
    let vfs_name = Arc::new(vfs_name);

    // Run readers and writers concurrently
    use std::thread;

    let db_path_readers = Arc::clone(&db_path);
    let vfs_name_readers = Arc::clone(&vfs_name);
    let valid_ids_readers = Arc::clone(&valid_ids);

    let db_path_writers = Arc::clone(&db_path);
    let vfs_name_writers = Arc::clone(&vfs_name);

    let reader_handle = thread::spawn(move || -> Result<(usize, Vec<f64>), String> {
        bench_reads_concurrent(
            &db_path_readers, &vfs_name_readers, duration_secs, reader_threads,
            cache_size_kb, mmap_size_kb, &valid_ids_readers
        ).map_err(|e| e.to_string())
    });

    let writer_handle = thread::spawn(move || -> Result<(usize, Vec<f64>), String> {
        bench_writes_concurrent(&db_path_writers, &vfs_name_writers, duration_secs, writer_threads, wal_autocheckpoint)
            .map_err(|e| e.to_string())
    });

    let (read_count, read_latencies) = reader_handle.join()
        .map_err(|_| "Reader thread panicked".to_string())??;
    let (write_count, write_latencies) = writer_handle.join()
        .map_err(|_| "Writer thread panicked".to_string())??;

    let read_ops_per_sec = read_count as f64 / read_latencies.iter().sum::<f64>() * 1_000_000.0;
    let write_ops_per_sec = write_count as f64 / write_latencies.iter().sum::<f64>() * 1_000_000.0;

    let read_p50 = percentile(&read_latencies, 0.5);
    let read_p99 = percentile(&read_latencies, 0.99);
    let write_p50 = percentile(&write_latencies, 0.5);
    let write_p99 = percentile(&write_latencies, 0.99);

    Ok(BenchmarkResult {
        mode: mode.to_string(),
        document_count: doc_count,
        read_ops_per_sec,
        read_p50_us: read_p50,
        read_p99_us: read_p99,
        write_ops_per_sec,
        write_p50_us: write_p50,
        write_p99_us: write_p99,
        file_size_mb,
        setup_duration_secs: setup_duration,
        total_duration_secs: start_total.elapsed().as_secs_f64(),
    })
}

fn bench_existing_db(
    source_db: &std::path::Path,
    mode: &str,
    duration_secs: u64,
    reader_threads: usize,
    writer_threads: usize,
    cache_kb_override: i64,
    mmap_kb_override: i64,
    cache_percent: f64,
    mmap_percent: f64,
    wal_autocheckpoint: i64,
    encryption_key: Option<&str>,
) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
    let start_total = Instant::now();

    // Create temp dir and VFS
    let temp_dir = tempfile::TempDir::new()?;
    let vfs_name = format!("bench_{}", mode);

    let vfs = match mode {
        "passthrough" => CompressedVfs::passthrough(temp_dir.path()),
        "compressed" => CompressedVfs::new(temp_dir.path(), 3),
        "encrypted" => {
            #[cfg(feature = "encryption")]
            {
                let key = encryption_key.ok_or("Encryption key required")?;
                CompressedVfs::encrypted(temp_dir.path(), key)
            }
            #[cfg(not(feature = "encryption"))]
            {
                return Err("Encryption not enabled".into());
            }
        }
        "both" => {
            #[cfg(feature = "encryption")]
            {
                let key = encryption_key.ok_or("Encryption key required")?;
                CompressedVfs::compressed_encrypted(temp_dir.path(), 3, key)
            }
            #[cfg(not(feature = "encryption"))]
            {
                return Err("Encryption not enabled".into());
            }
        }
        _ => return Err(format!("Unknown mode: {}", mode).into()),
    };

    register(&vfs_name, vfs)?;

    // Create NEW database with VFS and copy data from source
    let setup_start = Instant::now();
    let db_path = temp_dir.path().join("bench.db");
    let conn = Connection::open_with_flags_and_vfs(
        &db_path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )?;

    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;

    // Read from source database and insert into VFS database
    let source_conn = Connection::open(source_db)?;
    conn.execute("CREATE TABLE articles (author TEXT, title TEXT, body TEXT)", [])?;

    conn.execute("BEGIN", [])?;
    let mut stmt = source_conn.prepare("SELECT author, title, body FROM articles")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;

    for row_result in rows {
        let (author, title, body) = row_result?;
        conn.execute(
            "INSERT INTO articles (author, title, body) VALUES (?, ?, ?)",
            (&author, &title, &body),
        )?;
    }
    conn.execute("COMMIT", [])?;

    // CRITICAL: Checkpoint WAL to flush data to main database file
    // Without this, all data stays in uncompressed WAL and we never test compression!
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;

    // Get document count
    let doc_count: usize = conn.query_row("SELECT COUNT(*) FROM articles", [], |row| row.get(0))?;
    let setup_duration = setup_start.elapsed().as_secs_f64();

    // Set page cache size and mmap_size for ALL modes
    // Cache stores decompressed pages, so size based on logical (uncompressed) size
    let page_count: i64 = conn.query_row("PRAGMA page_count", [], |row| row.get(0))?;
    let page_size: i64 = conn.query_row("PRAGMA page_size", [], |row| row.get(0))?;
    let logical_size_kb = (page_count * page_size) / 1024;

    // Use override if provided, otherwise calculate from percentage
    let cache_size_kb = if cache_kb_override > 0 {
        cache_kb_override
    } else {
        (logical_size_kb as f64 * cache_percent) as i64
    };
    let mmap_size_kb = if mmap_kb_override > 0 {
        mmap_kb_override
    } else {
        (logical_size_kb as f64 * mmap_percent) as i64
    };

    let file_size_mb = std::fs::metadata(&db_path)?.len() as f64 / (1024.0 * 1024.0);

    conn.execute(&format!("PRAGMA cache_size = -{}", cache_size_kb), [])?;
    conn.execute(&format!("PRAGMA mmap_size = {}", mmap_size_kb * 1024), [])?;

    // Collect all existing rowids for random access
    let mut valid_ids: Vec<i64> = Vec::new();
    let mut stmt = conn.prepare("SELECT rowid FROM articles")?;
    let rows = stmt.query_map([], |row| row.get(0))?;
    for row_result in rows {
        valid_ids.push(row_result?);
    }
    drop(stmt);
    drop(conn);

    // Share valid_ids across threads
    let valid_ids = Arc::new(valid_ids);
    let db_path = Arc::new(db_path);
    let vfs_name = Arc::new(vfs_name);

    // Run readers and writers CONCURRENTLY for the same duration
    use std::thread;

    let db_path_readers = Arc::clone(&db_path);
    let vfs_name_readers = Arc::clone(&vfs_name);
    let valid_ids_readers = Arc::clone(&valid_ids);

    let db_path_writers = Arc::clone(&db_path);
    let vfs_name_writers = Arc::clone(&vfs_name);

    // Spawn reader threads
    let reader_handle = thread::spawn(move || -> Result<(usize, Vec<f64>), String> {
        bench_reads_concurrent(
            &db_path_readers, &vfs_name_readers, duration_secs, reader_threads,
            cache_size_kb, mmap_size_kb, &valid_ids_readers
        ).map_err(|e| e.to_string())
    });

    // Spawn writer threads
    let writer_handle = thread::spawn(move || -> Result<(usize, Vec<f64>), String> {
        bench_writes_concurrent(&db_path_writers, &vfs_name_writers, duration_secs, writer_threads, wal_autocheckpoint)
            .map_err(|e| e.to_string())
    });

    // Wait for both to complete
    let (read_count, read_latencies) = reader_handle.join()
        .map_err(|_| "Reader thread panicked".to_string())??;
    let (write_count, write_latencies) = writer_handle.join()
        .map_err(|_| "Writer thread panicked".to_string())??;

    let read_ops_per_sec = read_count as f64 / read_latencies.iter().sum::<f64>() * 1_000_000.0;
    let write_ops_per_sec = write_count as f64 / write_latencies.iter().sum::<f64>() * 1_000_000.0;

    let read_p50 = percentile(&read_latencies, 0.5);
    let read_p99 = percentile(&read_latencies, 0.99);
    let write_p50 = percentile(&write_latencies, 0.5);
    let write_p99 = percentile(&write_latencies, 0.99);

    Ok(BenchmarkResult {
        mode: mode.to_string(),
        document_count: doc_count,
        read_ops_per_sec,
        read_p50_us: read_p50,
        read_p99_us: read_p99,
        write_ops_per_sec,
        write_p50_us: write_p50,
        write_p99_us: write_p99,
        file_size_mb,
        setup_duration_secs: setup_duration,
        total_duration_secs: start_total.elapsed().as_secs_f64(),
    })
}

fn bench_reads_concurrent(
    db_path: &Arc<PathBuf>,
    vfs_name: &Arc<String>,
    duration_secs: u64,
    num_threads: usize,
    cache_size_kb: i64,
    mmap_size_kb: i64,
    valid_ids: &Arc<Vec<i64>>,
) -> Result<(usize, Vec<f64>), Box<dyn std::error::Error>> {
    use std::thread;

    let mut handles = Vec::new();

    for thread_id in 0..num_threads {
        let db_path = Arc::clone(db_path);
        let vfs_name = Arc::clone(vfs_name);
        let valid_ids = Arc::clone(valid_ids);

        let handle = thread::spawn(move || -> Result<(usize, Vec<f64>), String> {
            // Each reader opens its own connection (WAL mode allows concurrent reads)
            let conn = Connection::open_with_flags_and_vfs(
                &*db_path,
                OpenFlags::SQLITE_OPEN_READ_ONLY,
                &*vfs_name,
            ).map_err(|e| e.to_string())?;

            // Configure cache for this connection
            conn.execute(&format!("PRAGMA cache_size = -{}", cache_size_kb), [])
                .map_err(|e| e.to_string())?;
            conn.execute(&format!("PRAGMA mmap_size = {}", mmap_size_kb * 1024), [])
                .map_err(|e| e.to_string())?;

            let mut latencies = Vec::new();
            let mut i = 0usize;
            let deadline = Instant::now() + std::time::Duration::from_secs(duration_secs);

            // Use fast random number generator for random access pattern
            use std::collections::hash_map::RandomState;
            use std::hash::{BuildHasher, Hasher};
            let hasher = RandomState::new();

            while Instant::now() < deadline {
                // Generate random index - include thread_id for different sequences per thread
                let mut h = hasher.build_hasher();
                h.write_usize(i);
                h.write_usize(thread_id);
                let random_idx = (h.finish() as usize) % valid_ids.len();
                let rowid = valid_ids[random_idx];

                let start = Instant::now();

                conn.query_row(
                    "SELECT author, title, body FROM articles WHERE rowid = ?",
                    [rowid],
                    |row| {
                        let _author: String = row.get(0)?;
                        let _title: String = row.get(1)?;
                        let _body: String = row.get(2)?;
                        Ok(())
                    },
                ).map_err(|e| format!("Thread {}: SELECT rowid {} failed: {}", thread_id, rowid, e))?;

                latencies.push(start.elapsed().as_micros() as f64);
                i += 1;
            }

            Ok((i, latencies))
        });

        handles.push(handle);
    }

    // Collect results from all threads
    let mut total_count = 0;
    let mut all_latencies = Vec::new();

    for handle in handles {
        let (count, mut latencies) = handle.join()
            .map_err(|_| "Thread panicked")?
            .map_err(|e| e.to_string())?;
        total_count += count;
        all_latencies.append(&mut latencies);
    }

    Ok((total_count, all_latencies))
}

fn bench_writes_concurrent(
    db_path: &Arc<PathBuf>,
    vfs_name: &Arc<String>,
    duration_secs: u64,
    num_threads: usize,
    wal_autocheckpoint: i64,
) -> Result<(usize, Vec<f64>), Box<dyn std::error::Error>> {
    use std::thread;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let counter = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for thread_id in 0..num_threads {
        let db_path = Arc::clone(db_path);
        let vfs_name = Arc::clone(vfs_name);
        let counter = Arc::clone(&counter);

        let handle = thread::spawn(move || -> Result<(usize, Vec<f64>), String> {
            // Each writer opens its own connection (WAL mode handles serialization)
            let conn = Connection::open_with_flags_and_vfs(
                &*db_path,
                OpenFlags::SQLITE_OPEN_READ_WRITE,
                &*vfs_name,
            ).map_err(|e| e.to_string())?;

            // CRITICAL: Enable WAL mode and configure for performance
            conn.execute_batch(&format!(
                "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA wal_autocheckpoint={};",
                wal_autocheckpoint
            )).map_err(|e| e.to_string())?;

            let mut latencies = Vec::new();
            let body = "x".repeat(1000);
            let mut local_count = 0usize;
            let deadline = Instant::now() + std::time::Duration::from_secs(duration_secs);

            while Instant::now() < deadline {
                let i = counter.fetch_add(1, Ordering::Relaxed);
                let start = Instant::now();

                conn.execute(
                    "INSERT INTO articles (author, title, body) VALUES (?, ?, ?)",
                    (
                        format!("Write Author {} T{}", i, thread_id),
                        format!("Write Title {} T{}", i, thread_id),
                        &body,
                    ),
                ).map_err(|e| e.to_string())?;

                latencies.push(start.elapsed().as_micros() as f64);
                local_count += 1;
            }

            Ok((local_count, latencies))
        });

        handles.push(handle);
    }

    // Collect results from all threads
    let mut total_count = 0;
    let mut all_latencies = Vec::new();

    for handle in handles {
        let (count, mut latencies) = handle.join()
            .map_err(|_| "Thread panicked")?
            .map_err(|e| e.to_string())?;
        total_count += count;
        all_latencies.append(&mut latencies);
    }

    // Final checkpoint after all writers are done
    let conn = Connection::open_with_flags_and_vfs(
        &**db_path,
        OpenFlags::SQLITE_OPEN_READ_WRITE,
        &**vfs_name,
    )?;
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;

    Ok((total_count, all_latencies))
}

fn percentile(latencies: &[f64], p: f64) -> f64 {
    if latencies.is_empty() {
        return 0.0;
    }
    let mut sorted = latencies.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((p * sorted.len() as f64) as usize).min(sorted.len() - 1);
    sorted[idx]
}

fn print_comparison_table(results: &[BenchmarkResult]) {
    println!("=== VFS Mode Comparison ===\n");
    println!("{:<15} {:>12} {:>12} {:>10} {:>10}",
             "Mode", "Read ops/s", "Write ops/s", "Read P99", "File MB");
    println!("{:-<15} {:->12} {:->12} {:->10} {:->10}",
             "", "", "", "", "");

    for result in results {
        println!("{:<15} {:>12.0} {:>12.0} {:>8.1}µs {:>10.2}",
                 result.mode,
                 result.read_ops_per_sec,
                 result.write_ops_per_sec,
                 result.read_p99_us,
                 result.file_size_mb);
    }

    println!("\nSetup times:");
    for result in results {
        println!("  {}: {:.2}s", result.mode, result.setup_duration_secs);
    }
}

fn format_comparison_table(results: &[BenchmarkResult]) -> String {
    let mut output = String::from("=== VFS Mode Comparison ===\n\n");
    output.push_str(&format!("{:<15} {:>12} {:>12} {:>10} {:>10}\n",
                             "Mode", "Read ops/s", "Write ops/s", "Read P99", "File MB"));
    output.push_str(&format!("{:-<15} {:->12} {:->12} {:->10} {:->10}\n",
                             "", "", "", "", ""));

    for result in results {
        output.push_str(&format!("{:<15} {:>12.0} {:>12.0} {:>8.1}µs {:>10.2}\n",
                                 result.mode,
                                 result.read_ops_per_sec,
                                 result.write_ops_per_sec,
                                 result.read_p99_us,
                                 result.file_size_mb));
    }
    output
}

fn format_markdown_report(results: &[BenchmarkResult]) -> String {
    let mut output = String::from("# SQLCEs Benchmark Report\n\n");

    output.push_str("## Performance Comparison\n\n");
    output.push_str("| Mode | Read (ops/s) | Write (ops/s) | Read P99 (µs) | File Size (MB) |\n");
    output.push_str("|------|-------------:|--------------:|--------------:|---------------:|\n");

    for result in results {
        output.push_str(&format!("| {} | {:.0} | {:.0} | {:.1} | {:.2} |\n",
                                 result.mode,
                                 result.read_ops_per_sec,
                                 result.write_ops_per_sec,
                                 result.read_p99_us,
                                 result.file_size_mb));
    }

    output.push_str("\n## Detailed Results\n\n");
    for result in results {
        output.push_str(&format!("### {} Mode\n\n", result.mode));
        output.push_str(&format!("- **Documents**: {}\n", result.document_count));
        output.push_str(&format!("- **Read throughput**: {:.0} ops/sec\n", result.read_ops_per_sec));
        output.push_str(&format!("- **Read P50/P99**: {:.2}µs / {:.2}µs\n", result.read_p50_us, result.read_p99_us));
        output.push_str(&format!("- **Write throughput**: {:.0} ops/sec\n", result.write_ops_per_sec));
        output.push_str(&format!("- **Write P50/P99**: {:.2}µs / {:.2}µs\n", result.write_p50_us, result.write_p99_us));
        output.push_str(&format!("- **File size**: {:.2} MB\n", result.file_size_mb));
        output.push_str(&format!("- **Setup time**: {:.2}s\n\n", result.setup_duration_secs));
    }

    output
}

/// Project Gutenberg texts to download (eBook ID, Author, Title)
const GUTENBERG_TEXTS: &[(u32, &str, &str)] = &[
    (100, "William Shakespeare", "Complete Works of Shakespeare"),
    (2701, "Herman Melville", "Moby Dick"),
    (1342, "Jane Austen", "Pride and Prejudice"),
    (84, "Mary Shelley", "Frankenstein"),
    (1661, "Arthur Conan Doyle", "Sherlock Holmes"),
    (11, "Lewis Carroll", "Alice in Wonderland"),
    (1400, "Homer", "The Iliad"),
    (1727, "Homer", "The Odyssey"),
    (2600, "Leo Tolstoy", "War and Peace"),
    (1497, "The Bible", "King James Bible"),
    (4300, "Charles Darwin", "Origin of Species"),
    (98, "Charles Dickens", "A Tale of Two Cities"),
    (1080, "Charles Dickens", "David Copperfield"),
    (1404, "Fyodor Dostoevsky", "Brothers Karamazov"),
    (2554, "Fyodor Dostoevsky", "Crime and Punishment"),
];

/// Download and cache a Gutenberg text
fn download_gutenberg_text(ebook_id: u32, cache_dir: &std::path::Path) -> Result<String, Box<dyn std::error::Error>> {
    let cache_file = cache_dir.join(format!("{}.txt", ebook_id));

    if cache_file.exists() {
        return Ok(std::fs::read_to_string(&cache_file)?);
    }

    // Try UTF-8 version first, then ASCII
    let urls = [
        format!("https://www.gutenberg.org/files/{}/{}-0.txt", ebook_id, ebook_id),
        format!("https://www.gutenberg.org/files/{}/{}.txt", ebook_id, ebook_id),
    ];

    for url in &urls {
        match ureq::get(url).call() {
            Ok(response) => {
                let text = response.into_string()?;
                // Clean Gutenberg headers/footers
                let cleaned = clean_gutenberg_text(&text);
                std::fs::write(&cache_file, &cleaned)?;
                return Ok(cleaned);
            }
            Err(_) => continue,
        }
    }

    Err(format!("Failed to download ebook {}", ebook_id).into())
}

/// Remove Project Gutenberg headers and footers
fn clean_gutenberg_text(text: &str) -> String {
    let start_markers = [
        "*** START OF THIS PROJECT GUTENBERG",
        "***START OF THE PROJECT GUTENBERG",
        "*** START OF THE PROJECT GUTENBERG",
    ];
    let end_markers = [
        "*** END OF THIS PROJECT GUTENBERG",
        "***END OF THE PROJECT GUTENBERG",
        "*** END OF THE PROJECT GUTENBERG",
    ];

    let mut result = text.to_string();

    for marker in start_markers {
        if let Some(pos) = result.find(marker) {
            if let Some(newline) = result[pos..].find('\n') {
                result = result[pos + newline + 1..].to_string();
            }
            break;
        }
    }

    for marker in end_markers {
        if let Some(pos) = result.find(marker) {
            result = result[..pos].to_string();
            break;
        }
    }

    result.trim().to_string()
}

/// Get or generate corpus, using cache if available
fn get_or_generate_corpus(target_size_mb: usize, fresh: bool) -> Result<Vec<(String, String, String)>, Box<dyn std::error::Error>> {
    let cache_dir = get_cache_dir();
    std::fs::create_dir_all(&cache_dir)?;

    let corpus_cache_file = cache_dir.join(format!("corpus_{}mb.json", target_size_mb));

    // Check for cached corpus
    if !fresh && corpus_cache_file.exists() {
        println!("Loading cached corpus from {:?}...", corpus_cache_file);
        let cached_data = std::fs::read_to_string(&corpus_cache_file)?;
        let corpus: Vec<(String, String, String)> = serde_json::from_str(&cached_data)?;
        println!("Loaded {} documents from cache", corpus.len());
        return Ok(corpus);
    }

    // Generate fresh corpus
    let corpus = generate_gutenberg_corpus(target_size_mb)?;

    // Save to cache
    println!("Caching corpus to {:?}...", corpus_cache_file);
    let json = serde_json::to_string(&corpus)?;
    std::fs::write(&corpus_cache_file, json)?;

    Ok(corpus)
}

/// Generate corpus from Gutenberg texts
fn generate_gutenberg_corpus(target_size_mb: usize) -> Result<Vec<(String, String, String)>, Box<dyn std::error::Error>> {
    let gutenberg_cache_dir = get_cache_dir().join("gutenberg_texts");
    std::fs::create_dir_all(&gutenberg_cache_dir)?;

    let target_bytes = target_size_mb * 1024 * 1024;
    let mut corpus = Vec::new();
    let mut current_size = 0usize;

    println!("Downloading Gutenberg texts (cached in {:?})...", gutenberg_cache_dir);

    for (ebook_id, author, title) in GUTENBERG_TEXTS {
        if current_size >= target_bytes {
            break;
        }

        print!("  {} - {}... ", author, title);
        std::io::Write::flush(&mut std::io::stdout())?;

        match download_gutenberg_text(*ebook_id, &gutenberg_cache_dir) {
            Ok(text) => {
                // Split into sections of ~2000 chars each
                let sections = split_into_sections(&text, author, title);
                let section_size: usize = sections.iter().map(|(a, t, txt)| a.len() + t.len() + txt.len()).sum();
                current_size += section_size;

                println!("{} sections, {:.2} MB", sections.len(), section_size as f64 / (1024.0 * 1024.0));
                corpus.extend(sections);
            }
            Err(e) => {
                println!("FAILED: {}", e);
            }
        }
    }

    // If we need more, repeat texts
    let mut cycle = 1;
    while current_size < target_bytes {
        cycle += 1;
        for (ebook_id, author, title) in GUTENBERG_TEXTS {
            if current_size >= target_bytes {
                break;
            }

            let cache_file = gutenberg_cache_dir.join(format!("{}.txt", ebook_id));
            if !cache_file.exists() {
                continue;
            }

            let text = std::fs::read_to_string(&cache_file)?;
            let cycled_author = format!("{} (Cycle {})", author, cycle);
            let sections = split_into_sections(&text, &cycled_author, title);
            let section_size: usize = sections.iter().map(|(a, t, txt)| a.len() + t.len() + txt.len()).sum();
            current_size += section_size;
            corpus.extend(sections);
        }
    }

    println!("Corpus: {} documents, {:.2} MB", corpus.len(), current_size as f64 / (1024.0 * 1024.0));
    Ok(corpus)
}

/// Split text into sections for more realistic document sizes
fn split_into_sections(text: &str, author: &str, title: &str) -> Vec<(String, String, String)> {
    let section_size = 2000;
    let words: Vec<&str> = text.split_whitespace().collect();
    let mut sections = Vec::new();
    let words_per_section = section_size / 6; // ~6 chars per word average

    for (i, chunk) in words.chunks(words_per_section).enumerate() {
        let section_text = chunk.join(" ");
        if section_text.len() > 500 {
            sections.push((
                author.to_string(),
                format!("{} - Part {}", title, i + 1),
                section_text,
            ));
        }
    }

    sections
}

/// Benchmark parallel vs serial compaction
fn bench_compact_command(rows: usize, iterations: usize, compression_level: i32, gutenberg: bool, corpus_size_mb: usize) -> Result<(), Box<dyn std::error::Error>> {
    use turbolite::{compact_with_recompression, inspect_database, CompactionConfig};
    use std::sync::atomic::{AtomicU32, Ordering};

    static VFS_COUNTER: AtomicU32 = AtomicU32::new(0);

    // Load corpus if using Gutenberg
    let corpus = if gutenberg {
        Some(generate_gutenberg_corpus(corpus_size_mb)?)
    } else {
        None
    };

    let doc_count = corpus.as_ref().map(|c| c.len()).unwrap_or(rows);

    println!();
    println!("=== SQLCEs Parallel Compaction Benchmark ===");
    if gutenberg {
        println!("Mode: Gutenberg corpus ({} MB, {} documents)", corpus_size_mb, doc_count);
    } else {
        println!("Mode: Synthetic data ({} rows)", rows);
    }
    println!("Iterations: {}, Compression level: {}", iterations, compression_level);
    println!();

    let mut parallel_times = Vec::new();
    let mut serial_times = Vec::new();

    for iter in 0..iterations {
        println!("Iteration {}/{}...", iter + 1, iterations);

        // Create test databases for this iteration
        for (mode, times) in [("parallel", &mut parallel_times), ("serial", &mut serial_times)] {
            let dir = tempfile::tempdir()?;
            let vfs_id = VFS_COUNTER.fetch_add(1, Ordering::SeqCst);
            let vfs_name = format!("compact_bench_{}_{}", mode, vfs_id);

            let vfs = CompressedVfs::new(dir.path(), compression_level);
            register(&vfs_name, vfs)?;

            let db_path = dir.path().join("bench.db");

            // Create database and insert data
            {
                let conn = Connection::open_with_flags_and_vfs(
                    &db_path,
                    OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
                    &vfs_name,
                )?;

                conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;

                if let Some(ref corpus) = corpus {
                    // Use Gutenberg corpus
                    conn.execute("CREATE TABLE articles (id INTEGER PRIMARY KEY, author TEXT, title TEXT, body TEXT)", [])?;

                    conn.execute("BEGIN", [])?;
                    for (i, (author, title, body)) in corpus.iter().enumerate() {
                        conn.execute(
                            "INSERT INTO articles (id, author, title, body) VALUES (?1, ?2, ?3, ?4)",
                            rusqlite::params![i as i64, author, title, body],
                        )?;
                    }
                    conn.execute("COMMIT", [])?;

                    // Create dead space with updates (50% of documents)
                    conn.execute("BEGIN", [])?;
                    for i in 0..(corpus.len() / 2) {
                        let updated_body = format!("UPDATED: {}", &corpus[i].2);
                        conn.execute(
                            "UPDATE articles SET body = ?1 WHERE id = ?2",
                            rusqlite::params![&updated_body, i as i64],
                        )?;
                    }
                    conn.execute("COMMIT", [])?;
                } else {
                    // Use synthetic data
                    conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT)", [])?;

                    let test_data = "benchmark_data_for_compaction_".repeat(50);
                    conn.execute("BEGIN", [])?;
                    for i in 0..rows {
                        conn.execute(
                            "INSERT INTO data (id, value) VALUES (?1, ?2)",
                            rusqlite::params![i as i64, &test_data],
                        )?;
                    }
                    conn.execute("COMMIT", [])?;

                    // Create dead space with updates (50% of rows)
                    let updated = "updated_data_for_compaction_test_".repeat(50);
                    conn.execute("BEGIN", [])?;
                    for i in 0..(rows / 2) {
                        conn.execute(
                            "UPDATE data SET value = ?1 WHERE id = ?2",
                            rusqlite::params![&updated, i as i64],
                        )?;
                    }
                    conn.execute("COMMIT", [])?;
                }

                // Checkpoint WAL to main file
                conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
            }

            let stats_before = inspect_database(&db_path)?;

            // Run compaction
            let start = Instant::now();
            let config = CompactionConfig::new(compression_level)
                .with_parallel(mode == "parallel");
            let _freed = compact_with_recompression(&db_path, config)?;
            let elapsed = start.elapsed();

            times.push(elapsed.as_secs_f64());

            if iter == 0 {
                let stats_after = inspect_database(&db_path)?;
                println!("  {} mode: {:.3}s (before: {:.2} MB, after: {:.2} MB, dead: {:.1}% -> {:.1}%)",
                    mode,
                    elapsed.as_secs_f64(),
                    stats_before.file_size as f64 / (1024.0 * 1024.0),
                    stats_after.file_size as f64 / (1024.0 * 1024.0),
                    stats_before.dead_space_pct,
                    stats_after.dead_space_pct);
            }
        }
    }

    println!();
    println!("=== Results ===");
    println!();

    let parallel_avg = parallel_times.iter().sum::<f64>() / parallel_times.len() as f64;
    let serial_avg = serial_times.iter().sum::<f64>() / serial_times.len() as f64;
    let speedup = serial_avg / parallel_avg;

    println!("{:<12} {:>12} {:>12}", "Mode", "Avg Time", "Speedup");
    println!("{:-<12} {:->12} {:->12}", "", "", "");
    println!("{:<12} {:>10.3}s {:>12}", "parallel", parallel_avg, format!("{:.2}x", speedup));
    println!("{:<12} {:>10.3}s {:>12}", "serial", serial_avg, "baseline");

    println!();
    if speedup > 1.5 {
        println!("Parallel compaction is {:.1}x faster!", speedup);
    } else if speedup > 1.0 {
        println!("Parallel compaction provides {:.0}% speedup.", (speedup - 1.0) * 100.0);
    } else {
        println!("Serial compaction is faster for this workload (try more rows).");
    }

    Ok(())
}
