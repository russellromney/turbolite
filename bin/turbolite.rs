//! turbolite CLI
//!
//! Management commands for turbolite databases: inspect manifests, run GC,
//! import/export, interactive shell, cache warming, and more.

use std::io::{self, BufRead, Write as _};
use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "turbolite", version, about = "turbolite CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Print version
    Version,

    /// Print manifest summary for a turbolite database
    Info {
        /// Path to the database file
        #[arg(long)]
        db: PathBuf,

        /// S3 bucket (or set TURBOLITE_BUCKET)
        #[arg(long, env = "TURBOLITE_BUCKET")]
        bucket: Option<String>,

        /// S3 key prefix (or set TURBOLITE_PREFIX)
        #[arg(long, env = "TURBOLITE_PREFIX")]
        prefix: Option<String>,

        /// S3 endpoint URL (or set AWS_ENDPOINT_URL)
        #[arg(long, env = "AWS_ENDPOINT_URL")]
        endpoint: Option<String>,

        /// AWS region (or set AWS_REGION)
        #[arg(long, env = "AWS_REGION")]
        region: Option<String>,

        /// Local cache directory
        #[arg(long, default_value = "/tmp/turbolite-cache")]
        cache_dir: PathBuf,
    },

    /// Open an interactive SQLite shell on a turbolite database
    Shell {
        /// Path to the database file
        #[arg(long)]
        db: PathBuf,

        /// S3 bucket (or set TURBOLITE_BUCKET)
        #[arg(long, env = "TURBOLITE_BUCKET")]
        bucket: Option<String>,

        /// S3 key prefix (or set TURBOLITE_PREFIX)
        #[arg(long, env = "TURBOLITE_PREFIX")]
        prefix: Option<String>,

        /// S3 endpoint URL (or set AWS_ENDPOINT_URL)
        #[arg(long, env = "AWS_ENDPOINT_URL")]
        endpoint: Option<String>,

        /// AWS region (or set AWS_REGION)
        #[arg(long, env = "AWS_REGION")]
        region: Option<String>,

        /// Local cache directory
        #[arg(long, default_value = "/tmp/turbolite-cache")]
        cache_dir: PathBuf,

        /// Open in read-only mode
        #[arg(long)]
        read_only: bool,
    },

    /// Run garbage collection on old S3 page group versions
    Gc {
        /// Path to the database file
        #[arg(long)]
        db: PathBuf,

        /// S3 bucket (required, or set TURBOLITE_BUCKET)
        #[arg(long, env = "TURBOLITE_BUCKET")]
        bucket: String,

        /// S3 key prefix (or set TURBOLITE_PREFIX)
        #[arg(long, env = "TURBOLITE_PREFIX")]
        prefix: Option<String>,

        /// S3 endpoint URL (or set AWS_ENDPOINT_URL)
        #[arg(long, env = "AWS_ENDPOINT_URL")]
        endpoint: Option<String>,

        /// AWS region (or set AWS_REGION)
        #[arg(long, env = "AWS_REGION")]
        region: Option<String>,

        /// Local cache directory
        #[arg(long, default_value = "/tmp/turbolite-cache")]
        cache_dir: PathBuf,
    },

    /// Force a checkpoint and S3 upload
    Checkpoint {
        /// Path to the database file
        #[arg(long)]
        db: PathBuf,

        /// S3 bucket (required, or set TURBOLITE_BUCKET)
        #[arg(long, env = "TURBOLITE_BUCKET")]
        bucket: String,

        /// S3 key prefix (or set TURBOLITE_PREFIX)
        #[arg(long, env = "TURBOLITE_PREFIX")]
        prefix: Option<String>,

        /// S3 endpoint URL (or set AWS_ENDPOINT_URL)
        #[arg(long, env = "AWS_ENDPOINT_URL")]
        endpoint: Option<String>,

        /// AWS region (or set AWS_REGION)
        #[arg(long, env = "AWS_REGION")]
        region: Option<String>,

        /// Local cache directory
        #[arg(long, default_value = "/tmp/turbolite-cache")]
        cache_dir: PathBuf,
    },

    /// Download the entire database from S3 into local cache
    Download {
        /// Path to the database file
        #[arg(long)]
        db: PathBuf,

        /// S3 bucket (required, or set TURBOLITE_BUCKET)
        #[arg(long, env = "TURBOLITE_BUCKET")]
        bucket: String,

        /// S3 key prefix (or set TURBOLITE_PREFIX)
        #[arg(long, env = "TURBOLITE_PREFIX")]
        prefix: Option<String>,

        /// S3 endpoint URL (or set AWS_ENDPOINT_URL)
        #[arg(long, env = "AWS_ENDPOINT_URL")]
        endpoint: Option<String>,

        /// AWS region (or set AWS_REGION)
        #[arg(long, env = "AWS_REGION")]
        region: Option<String>,

        /// Local cache directory
        #[arg(long, default_value = "/tmp/turbolite-cache")]
        cache_dir: PathBuf,

        /// Number of prefetch threads
        #[arg(long, default_value = "8")]
        threads: u32,
    },

    /// Export a turbolite database to a plain SQLite file
    Export {
        /// Path to the turbolite database
        #[arg(long)]
        db: PathBuf,

        /// Output path for the plain SQLite file
        #[arg(long)]
        output: PathBuf,

        /// S3 bucket (or set TURBOLITE_BUCKET)
        #[arg(long, env = "TURBOLITE_BUCKET")]
        bucket: Option<String>,

        /// S3 key prefix (or set TURBOLITE_PREFIX)
        #[arg(long, env = "TURBOLITE_PREFIX")]
        prefix: Option<String>,

        /// S3 endpoint URL (or set AWS_ENDPOINT_URL)
        #[arg(long, env = "AWS_ENDPOINT_URL")]
        endpoint: Option<String>,

        /// AWS region (or set AWS_REGION)
        #[arg(long, env = "AWS_REGION")]
        region: Option<String>,

        /// Local cache directory
        #[arg(long, default_value = "/tmp/turbolite-cache")]
        cache_dir: PathBuf,
    },

    /// Import a plain SQLite file into turbolite S3 format
    Import {
        /// Path to the plain SQLite file to import
        #[arg(long)]
        input: PathBuf,

        /// S3 bucket (required, or set TURBOLITE_BUCKET)
        #[arg(long, env = "TURBOLITE_BUCKET")]
        bucket: String,

        /// S3 key prefix (or set TURBOLITE_PREFIX)
        #[arg(long, env = "TURBOLITE_PREFIX")]
        prefix: Option<String>,

        /// S3 endpoint URL (or set AWS_ENDPOINT_URL)
        #[arg(long, env = "AWS_ENDPOINT_URL")]
        endpoint: Option<String>,

        /// AWS region (or set AWS_REGION)
        #[arg(long, env = "AWS_REGION")]
        region: Option<String>,

        /// Pages per group (default 256)
        #[arg(long, default_value = "256")]
        pages_per_group: u32,

        /// Zstd compression level (1-22, default 3)
        #[arg(long, default_value = "3")]
        compression_level: i32,
    },
}

/// Build a TurboliteConfig from common CLI args.
fn build_config(
    cache_dir: PathBuf,
    bucket: Option<String>,
    prefix: Option<String>,
    endpoint: Option<String>,
    region: Option<String>,
    read_only: bool,
) -> turbolite::tiered::TurboliteConfig {
    use turbolite::tiered::{StorageBackend, TurboliteConfig};

    let storage_backend = match bucket {
        Some(ref b) => StorageBackend::S3 {
            bucket: b.clone(),
            prefix: prefix.clone().unwrap_or_default(),
            endpoint_url: endpoint.clone(),
            region: region.clone(),
        },
        None => StorageBackend::Local,
    };

    TurboliteConfig {
        storage_backend,
        bucket: bucket.unwrap_or_default(),
        prefix: prefix.unwrap_or_default(),
        endpoint_url: endpoint,
        region,
        cache_dir,
        read_only,
        ..Default::default()
    }
}

/// Register a VFS and open a rusqlite connection.
fn open_connection(
    db: &std::path::Path,
    config: turbolite::tiered::TurboliteConfig,
    vfs_name: &str,
) -> Result<rusqlite::Connection> {
    let vfs = turbolite::tiered::TurboliteVfs::new(config)
        .context("failed to create VFS")?;
    turbolite::tiered::register(vfs_name, vfs)
        .context("failed to register VFS")?;

    let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE;
    let conn = rusqlite::Connection::open_with_flags_and_vfs(db, flags, vfs_name)
        .context("failed to open database")?;
    Ok(conn)
}

/// Register a shared VFS and return (SharedTurboliteVfs, Connection).
fn open_shared(
    db: &std::path::Path,
    config: turbolite::tiered::TurboliteConfig,
    vfs_name: &str,
) -> Result<(turbolite::tiered::SharedTurboliteVfs, rusqlite::Connection)> {
    let vfs = turbolite::tiered::TurboliteVfs::new(config)
        .context("failed to create VFS")?;
    let shared = turbolite::tiered::SharedTurboliteVfs::new(vfs);
    turbolite::tiered::register_shared(vfs_name, shared.clone())
        .context("failed to register shared VFS")?;

    let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE;
    let conn = rusqlite::Connection::open_with_flags_and_vfs(db, flags, vfs_name)
        .context("failed to open database")?;
    Ok((shared, conn))
}

fn cmd_info(
    db: PathBuf,
    bucket: Option<String>,
    prefix: Option<String>,
    endpoint: Option<String>,
    region: Option<String>,
    cache_dir: PathBuf,
) -> Result<()> {
    let config = build_config(cache_dir, bucket, prefix, endpoint, region, true);
    let vfs_name = format!("turbolite-info-{}", std::process::id());
    let vfs = turbolite::tiered::TurboliteVfs::new(config)
        .context("failed to create VFS")?;
    let shared = turbolite::tiered::SharedTurboliteVfs::new(vfs);
    turbolite::tiered::register_shared(&vfs_name, shared.clone())
        .context("failed to register VFS")?;

    // Open connection to initialize VFS state
    let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE;
    let _conn = rusqlite::Connection::open_with_flags_and_vfs(&db, flags, &vfs_name)
        .context("failed to open database")?;

    let manifest = shared.manifest();

    let total_groups = manifest.page_group_keys.len();
    let interior_chunks = manifest.interior_chunk_keys.len();
    let index_chunks = manifest.index_chunk_keys.len();
    let page_count = manifest.page_count;
    let page_size = manifest.page_size;
    let uncompressed_bytes = page_count * page_size as u64;
    let btree_count = manifest.btrees.len();

    println!("turbolite database: {}", db.display());
    println!();
    println!("  manifest version:  {}", manifest.version);
    println!("  change counter:    {}", manifest.change_counter);
    println!("  storage:           {}", if shared.has_remote_storage() { "S3" } else { "local" });
    println!("  strategy:          {:?}", manifest.strategy);
    println!();
    println!("  pages:             {}", page_count);
    println!("  page size:         {} bytes", page_size);
    println!("  uncompressed size: {:.1} MB", uncompressed_bytes as f64 / (1024.0 * 1024.0));
    println!();
    println!("  page groups:       {}", total_groups);
    println!("  pages per group:   {}", manifest.pages_per_group);
    println!("  interior chunks:   {}", interior_chunks);
    println!("  index chunks:      {}", index_chunks);
    println!("  B-trees:           {}", btree_count);

    if manifest.sub_pages_per_frame > 0 {
        println!("  seekable frames:   yes (sub_ppf={})", manifest.sub_pages_per_frame);
    } else {
        println!("  seekable frames:   no");
    }

    if !manifest.btrees.is_empty() {
        println!();
        println!("  B-tree summary:");
        let mut entries: Vec<_> = manifest.btrees.iter().collect();
        entries.sort_by_key(|(root, _)| *root);
        for (root, entry) in entries {
            println!(
                "    root={:<6} type={:<8} name={:<30} groups={}",
                root,
                entry.obj_type,
                entry.name,
                entry.group_ids.len(),
            );
        }
    }

    Ok(())
}

fn cmd_shell(
    db: PathBuf,
    bucket: Option<String>,
    prefix: Option<String>,
    endpoint: Option<String>,
    region: Option<String>,
    cache_dir: PathBuf,
    read_only: bool,
) -> Result<()> {
    let config = build_config(cache_dir, bucket, prefix, endpoint, region, read_only);
    let vfs_name = format!("turbolite-shell-{}", std::process::id());
    let conn = open_connection(&db, config, &vfs_name)?;

    let mode_str = if read_only { " (read-only)" } else { "" };
    println!("turbolite shell{} -- {}", mode_str, db.display());
    println!("Type .quit to exit, .tables to list tables, .schema to show schema.");
    println!();

    let stdin = io::stdin();
    let stdout = io::stdout();

    loop {
        print!("turbolite> ");
        stdout.lock().flush()?;

        let mut line = String::new();
        if stdin.lock().read_line(&mut line)? == 0 {
            // EOF
            println!();
            break;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        match trimmed {
            ".quit" | ".exit" => break,
            ".tables" => {
                let sql = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name";
                match conn.prepare(sql) {
                    Ok(mut stmt) => {
                        let rows = stmt.query_map([], |row| row.get::<_, String>(0));
                        match rows {
                            Ok(rows) => {
                                for row in rows {
                                    match row {
                                        Ok(name) => println!("{}", name),
                                        Err(e) => eprintln!("Error: {}", e),
                                    }
                                }
                            }
                            Err(e) => eprintln!("Error: {}", e),
                        }
                    }
                    Err(e) => eprintln!("Error: {}", e),
                }
            }
            ".schema" => {
                let sql = "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type, name";
                match conn.prepare(sql) {
                    Ok(mut stmt) => {
                        let rows = stmt.query_map([], |row| row.get::<_, String>(0));
                        match rows {
                            Ok(rows) => {
                                for row in rows {
                                    match row {
                                        Ok(sql) => println!("{};", sql),
                                        Err(e) => eprintln!("Error: {}", e),
                                    }
                                }
                            }
                            Err(e) => eprintln!("Error: {}", e),
                        }
                    }
                    Err(e) => eprintln!("Error: {}", e),
                }
            }
            _ => {
                // Try as a query first (SELECT, EXPLAIN, PRAGMA, etc.)
                let is_query = {
                    let upper = trimmed.to_uppercase();
                    upper.starts_with("SELECT")
                        || upper.starts_with("EXPLAIN")
                        || upper.starts_with("PRAGMA")
                        || upper.starts_with("WITH")
                };

                if is_query {
                    match conn.prepare(trimmed) {
                        Ok(mut stmt) => {
                            let col_count = stmt.column_count();
                            let col_names: Vec<String> = (0..col_count)
                                .map(|i| stmt.column_name(i).expect("column name").to_string())
                                .collect();

                            // Print header
                            println!("{}", col_names.join("|"));

                            match stmt.query_map([], |row| {
                                let vals: Vec<String> = (0..col_count)
                                    .map(|i| {
                                        row.get::<_, rusqlite::types::Value>(i)
                                            .map(|v| format_value(&v))
                                            .unwrap_or_else(|_| "ERROR".to_string())
                                    })
                                    .collect();
                                Ok(vals.join("|"))
                            }) {
                                Ok(rows) => {
                                    for row in rows {
                                        match row {
                                            Ok(line) => println!("{}", line),
                                            Err(e) => eprintln!("Error: {}", e),
                                        }
                                    }
                                }
                                Err(e) => eprintln!("Error: {}", e),
                            }
                        }
                        Err(e) => eprintln!("Error: {}", e),
                    }
                } else {
                    match conn.execute_batch(trimmed) {
                        Ok(()) => {}
                        Err(e) => eprintln!("Error: {}", e),
                    }
                }
            }
        }
    }

    Ok(())
}

fn format_value(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_string(),
        rusqlite::types::Value::Integer(i) => i.to_string(),
        rusqlite::types::Value::Real(f) => f.to_string(),
        rusqlite::types::Value::Text(s) => s.clone(),
        rusqlite::types::Value::Blob(b) => format!("x'{}'", hex_encode(b)),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn cmd_gc(
    db: PathBuf,
    bucket: String,
    prefix: Option<String>,
    endpoint: Option<String>,
    region: Option<String>,
    cache_dir: PathBuf,
) -> Result<()> {
    let config = build_config(cache_dir, Some(bucket), prefix, endpoint, region, false);
    let vfs_name = format!("turbolite-gc-{}", std::process::id());
    let (shared, _conn) = open_shared(&db, config, &vfs_name)?;

    let deleted = shared.gc()
        .map_err(|e| anyhow!("gc failed: {}", e))?;

    if deleted > 0 {
        println!("gc: deleted {} orphaned S3 objects", deleted);
    } else {
        println!("gc: no orphaned objects found");
    }

    Ok(())
}

fn cmd_checkpoint(
    db: PathBuf,
    bucket: String,
    prefix: Option<String>,
    endpoint: Option<String>,
    region: Option<String>,
    cache_dir: PathBuf,
) -> Result<()> {
    let config = build_config(cache_dir, Some(bucket), prefix, endpoint, region, false);
    let vfs_name = format!("turbolite-ckpt-{}", std::process::id());
    let (_shared, conn) = open_shared(&db, config, &vfs_name)?;

    // Force a WAL checkpoint (TRUNCATE mode flushes all WAL frames and resets the WAL)
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .context("checkpoint failed")?;

    println!("checkpoint: complete");
    Ok(())
}

fn cmd_download(
    db: PathBuf,
    bucket: String,
    prefix: Option<String>,
    endpoint: Option<String>,
    region: Option<String>,
    cache_dir: PathBuf,
    threads: u32,
) -> Result<()> {
    let mut config = build_config(cache_dir, Some(bucket), prefix, endpoint, region, true);
    config.prefetch_threads = threads;

    let vfs_name = format!("turbolite-download-{}", std::process::id());
    let (shared, conn) = open_shared(&db, config, &vfs_name)?;

    let manifest = shared.manifest();
    let total_groups = manifest.page_group_keys.len();

    // Connection open fetches interior + index pages eagerly.
    // Full table scans pull all remaining data pages into local cache.
    let tables: Vec<String> = {
        let mut stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .context("failed to list tables")?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .context("failed to query tables")?;
        rows.filter_map(|r| r.ok()).collect()
    };

    for table in &tables {
        let sql = format!("SELECT COUNT(*) FROM \"{}\"", table.replace('"', "\"\""));
        let _: i64 = conn
            .query_row(&sql, [], |row| row.get(0))
            .unwrap_or(0);
    }

    let (fetches, bytes) = shared.s3_counters();
    println!(
        "download: {} tables, {} groups, {} S3 GETs, {:.1} MB",
        tables.len(),
        total_groups,
        fetches,
        bytes as f64 / (1024.0 * 1024.0),
    );

    Ok(())
}

fn cmd_export(
    db: PathBuf,
    output: PathBuf,
    bucket: Option<String>,
    prefix: Option<String>,
    endpoint: Option<String>,
    region: Option<String>,
    cache_dir: PathBuf,
) -> Result<()> {
    let config = build_config(cache_dir, bucket, prefix, endpoint, region, true);
    let vfs_name = format!("turbolite-export-{}", std::process::id());
    let src_conn = open_connection(&db, config, &vfs_name)?;

    // Open a plain SQLite destination (no VFS) and use the backup API
    // to copy all pages. This produces a standard SQLite file.
    let mut dst_conn = rusqlite::Connection::open(&output)
        .context("failed to create output file")?;

    let backup = rusqlite::backup::Backup::new(&src_conn, &mut dst_conn)
        .context("failed to initialize backup")?;

    backup
        .run_to_completion(100, std::time::Duration::from_millis(0), None)
        .context("backup failed")?;

    drop(backup);

    // Verify the output is a valid SQLite file
    let integrity: String = dst_conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .context("integrity check failed")?;

    if integrity != "ok" {
        return Err(anyhow!(
            "exported file failed integrity check: {}",
            integrity
        ));
    }

    let file_size = std::fs::metadata(&output)?.len();
    println!(
        "export: {} -> {} ({:.1} MB, integrity ok)",
        db.display(),
        output.display(),
        file_size as f64 / (1024.0 * 1024.0),
    );

    Ok(())
}

#[cfg(feature = "cloud")]
fn cmd_import(
    input: PathBuf,
    bucket: String,
    prefix: Option<String>,
    endpoint: Option<String>,
    region: Option<String>,
    pages_per_group: u32,
    compression_level: i32,
) -> Result<()> {
    use turbolite::tiered::{StorageBackend, TurboliteConfig};

    let prefix_str = prefix.unwrap_or_default();
    let config = TurboliteConfig {
        storage_backend: StorageBackend::S3 {
            bucket: bucket.clone(),
            prefix: prefix_str.clone(),
            endpoint_url: endpoint.clone(),
            region: region.clone(),
        },
        bucket: bucket.clone(),
        prefix: prefix_str,
        endpoint_url: endpoint,
        region,
        pages_per_group,
        compression_level,
        ..Default::default()
    };

    let manifest = turbolite::tiered::import_sqlite_file(&config, &input)
        .map_err(|e| anyhow!("import failed: {}", e))?;

    println!(
        "import: {} -> s3://{}/{} ({} pages, {} groups, manifest v{})",
        input.display(),
        config.bucket,
        config.prefix,
        manifest.page_count,
        manifest.page_group_keys.len(),
        manifest.version,
    );

    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Version => {
            println!("turbolite {}", env!("CARGO_PKG_VERSION"));
        }
        Commands::Info { db, bucket, prefix, endpoint, region, cache_dir } => {
            cmd_info(db, bucket, prefix, endpoint, region, cache_dir)?;
        }
        Commands::Shell { db, bucket, prefix, endpoint, region, cache_dir, read_only } => {
            cmd_shell(db, bucket, prefix, endpoint, region, cache_dir, read_only)?;
        }
        Commands::Gc { db, bucket, prefix, endpoint, region, cache_dir } => {
            cmd_gc(db, bucket, prefix, endpoint, region, cache_dir)?;
        }
        Commands::Checkpoint { db, bucket, prefix, endpoint, region, cache_dir } => {
            cmd_checkpoint(db, bucket, prefix, endpoint, region, cache_dir)?;
        }
        Commands::Download { db, bucket, prefix, endpoint, region, cache_dir, threads } => {
            cmd_download(db, bucket, prefix, endpoint, region, cache_dir, threads)?;
        }
        Commands::Export { db, output, bucket, prefix, endpoint, region, cache_dir } => {
            cmd_export(db, output, bucket, prefix, endpoint, region, cache_dir)?;
        }
        Commands::Import {
            input, bucket, prefix, endpoint, region,
            pages_per_group, compression_level,
        } => {
            #[cfg(feature = "cloud")]
            cmd_import(input, bucket, prefix, endpoint, region, pages_per_group, compression_level)?;

            #[cfg(not(feature = "cloud"))]
            return Err(anyhow!("import requires the 'cloud' feature. Rebuild with: cargo build --features cloud,zstd"));
        }
    }

    Ok(())
}
