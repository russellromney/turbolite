//! sqlces CLI - Management tool for SQLite Compress Encrypt VFS databases
//!
//! Commands:
//! - info: Show database statistics (pages, dead space, compression ratio)
//! - compact: Remove dead space from database
//! - train: Train compression dictionary from database
//! - convert: Convert between plain SQLite and compressed formats
//! - encrypt: Add encryption to existing database
//! - decrypt: Remove encryption from database

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand};
use rusqlite::{Connection, OpenFlags};
use turbolite::{compact, inspect_database, register, CompressedVfs};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(name = "sqlces")]
#[command(about = "SQLite Compress Encrypt VFS management tool", long_about = None)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Show database statistics
    Info {
        /// Path to database file
        db: PathBuf,
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Remove dead space from database
    Compact {
        /// Path to database file
        db: PathBuf,
        /// Show detailed progress
        #[arg(short, long)]
        verbose: bool,
    },

    /// Train compression dictionary from database
    Train {
        /// Path to database file
        db: PathBuf,
        /// Dictionary size in KB (default: 100)
        #[arg(long, default_value = "100")]
        dict_size: usize,
        /// Output dictionary file (optional, defaults to <db>.dict)
        #[arg(long)]
        dict_out: Option<PathBuf>,
        /// Compression level (1-22, default: 3)
        #[arg(long, default_value = "3")]
        level: i32,
    },

    /// Convert between plain SQLite and compressed formats
    Convert {
        /// Source database file
        src: PathBuf,
        /// Destination database file
        dst: PathBuf,
        /// Compression level for output (1-22, default: 3, 0 for plain SQLite)
        #[arg(short, long, default_value = "3")]
        level: i32,
        /// Encryption password for output (requires --encrypt flag)
        #[arg(long)]
        password: Option<String>,
    },

    /// Add encryption to existing database
    #[cfg(feature = "encryption")]
    Encrypt {
        /// Path to database file
        db: PathBuf,
        /// Encryption password
        #[arg(long)]
        password: String,
        /// Keep compression (if already compressed)
        #[arg(long)]
        keep_compression: bool,
    },

    /// Remove encryption from database
    #[cfg(feature = "encryption")]
    Decrypt {
        /// Path to encrypted database file
        db: PathBuf,
        /// Encryption password
        #[arg(long)]
        password: String,
        /// Keep compression (output compressed but unencrypted)
        #[arg(long)]
        keep_compression: bool,
    },

    /// Embed a compression dictionary into a database file
    EmbedDict {
        /// Path to database file
        db: PathBuf,
        /// Path to dictionary file
        dict: PathBuf,
        /// Create backup before modifying
        #[arg(long, default_value = "true")]
        backup: bool,
    },

    /// Extract embedded dictionary from a database file
    ExtractDict {
        /// Path to database file
        db: PathBuf,
        /// Output dictionary file (optional, defaults to <db>.dict)
        #[arg(long)]
        dict_out: Option<PathBuf>,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Info { db, json } => cmd_info(&db, json),
        Commands::Compact { db, verbose } => cmd_compact(&db, verbose),
        Commands::Train {
            db,
            dict_size,
            dict_out,
            level,
        } => cmd_train(&db, dict_size, dict_out, level),
        Commands::Convert {
            src,
            dst,
            level,
            password,
        } => cmd_convert(&src, &dst, level, password),
        #[cfg(feature = "encryption")]
        Commands::Encrypt {
            db,
            password,
            keep_compression,
        } => cmd_encrypt(&db, &password, keep_compression),
        #[cfg(feature = "encryption")]
        Commands::Decrypt {
            db,
            password,
            keep_compression,
        } => cmd_decrypt(&db, &password, keep_compression),
        Commands::EmbedDict { db, dict, backup } => cmd_embed_dict(&db, &dict, backup),
        Commands::ExtractDict { db, dict_out } => cmd_extract_dict(&db, dict_out),
    }
}

/// Detect if a file is a SQLCEs compressed database
fn is_sqlces_file(path: &Path) -> Result<bool> {
    let mut file = File::open(path).context("Failed to open file")?;
    let mut magic = [0u8; 8];
    if file.read_exact(&mut magic).is_err() {
        return Ok(false);
    }
    Ok(&magic == b"SQLCEvfS")
}

/// Detect if a file is a plain SQLite database
fn is_sqlite_file(path: &Path) -> Result<bool> {
    let mut file = File::open(path).context("Failed to open file")?;
    let mut magic = [0u8; 16];
    if file.read_exact(&mut magic).is_err() {
        return Ok(false);
    }
    Ok(&magic[0..16] == b"SQLite format 3\0")
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

fn cmd_info(db: &Path, json: bool) -> Result<()> {
    if !db.exists() {
        bail!("File not found: {}", db.display());
    }

    // Check file type
    if is_sqlces_file(db)? {
        let stats = inspect_database(db).context("Failed to inspect database")?;

        if json {
            let output = serde_json::json!({
                "format": "sqlces",
                "page_size": stats.page_size,
                "page_count": stats.page_count,
                "total_records": stats.total_records,
                "file_size": stats.file_size,
                "logical_size": stats.logical_size,
                "live_data_size": stats.live_data_size,
                "dead_space": stats.dead_space,
                "dead_space_pct": stats.dead_space_pct,
                "compression_ratio": stats.compression_ratio,
            });
            println!("{}", serde_json::to_string_pretty(&output)?);
        } else {
            println!("SQLCEs Database: {}", db.display());
            println!("  Page size:         {} bytes", stats.page_size);
            println!("  Pages:             {}", stats.page_count);
            println!("  Total records:     {} (includes superseded)", stats.total_records);
            println!("  File size:         {}", format_bytes(stats.file_size));
            println!("  Logical size:      {}", format_bytes(stats.logical_size));
            println!("  Live data:         {}", format_bytes(stats.live_data_size));
            println!(
                "  Dead space:        {} ({:.1}%)",
                format_bytes(stats.dead_space),
                stats.dead_space_pct
            );
            println!("  Compression ratio: {:.2}x", stats.compression_ratio);
        }
    } else if is_sqlite_file(db)? {
        let file_size = fs::metadata(db)?.len();

        // Open with rusqlite to get page info
        let conn = Connection::open_with_flags(db, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        let page_size: u32 = conn.query_row("PRAGMA page_size", [], |r| r.get(0))?;
        let page_count: u64 = conn.query_row("PRAGMA page_count", [], |r| r.get(0))?;
        let freelist_count: u64 = conn.query_row("PRAGMA freelist_count", [], |r| r.get(0))?;

        if json {
            let output = serde_json::json!({
                "format": "sqlite",
                "page_size": page_size,
                "page_count": page_count,
                "freelist_count": freelist_count,
                "file_size": file_size,
            });
            println!("{}", serde_json::to_string_pretty(&output)?);
        } else {
            println!("SQLite Database: {}", db.display());
            println!("  Page size:       {} bytes", page_size);
            println!("  Pages:           {}", page_count);
            println!("  Freelist pages:  {}", freelist_count);
            println!("  File size:       {}", format_bytes(file_size));
        }
    } else {
        bail!(
            "Unknown file format: {}. Expected SQLite or SQLCEs database.",
            db.display()
        );
    }

    Ok(())
}

fn cmd_compact(db: &Path, verbose: bool) -> Result<()> {
    if !db.exists() {
        bail!("File not found: {}", db.display());
    }

    if !is_sqlces_file(db)? {
        bail!("Not a SQLCEs database: {}", db.display());
    }

    if verbose {
        let stats_before = inspect_database(db)?;
        println!("Before: {} ({:.1}% dead space)",
            format_bytes(stats_before.file_size),
            stats_before.dead_space_pct);
    }

    let freed = compact(db).context("Compaction failed")?;

    if verbose {
        let stats_after = inspect_database(db)?;
        println!("After:  {} ({:.1}% dead space)",
            format_bytes(stats_after.file_size),
            stats_after.dead_space_pct);
    }

    println!("Freed {} of dead space", format_bytes(freed));
    Ok(())
}

fn cmd_train(db: &Path, dict_size_kb: usize, dict_out: Option<PathBuf>, level: i32) -> Result<()> {
    use turbolite::dict;

    if !db.exists() {
        bail!("File not found: {}", db.display());
    }

    let dict_size = dict_size_kb * 1024;
    let dict_path = dict_out.unwrap_or_else(|| {
        let mut p = db.to_path_buf();
        p.set_extension("dict");
        p
    });

    // Determine source type and get samples
    let samples = if is_sqlces_file(db)? {
        // For SQLCEs files, we need to decompress pages first
        // This requires opening via VFS and reading through SQLite
        extract_samples_from_sqlces(db)?
    } else if is_sqlite_file(db)? {
        // For plain SQLite, read pages directly
        extract_samples_from_sqlite(db)?
    } else {
        bail!("Unknown file format: {}", db.display());
    };

    if samples.is_empty() {
        bail!("No data found in database");
    }

    // Calculate total sample size
    let total_sample_size: usize = samples.iter().map(|s| s.len()).sum();

    // zstd dictionary training requires:
    // 1. At least ~100 samples for good results
    // 2. Total sample size should be at least 100x the dictionary size
    // 3. Minimum of about 100KB of sample data for reasonable dictionaries
    const MIN_SAMPLES: usize = 10;
    const MIN_SAMPLE_DATA: usize = 40 * 1024; // 40KB minimum (10 pages)

    if samples.len() < MIN_SAMPLES || total_sample_size < MIN_SAMPLE_DATA {
        bail!(
            "Database too small for dictionary training.\n\
             Found {} samples ({}).\n\
             Need at least {} samples with {} of data.\n\
             Try training on a larger database.",
            samples.len(),
            format_bytes(total_sample_size as u64),
            MIN_SAMPLES,
            format_bytes(MIN_SAMPLE_DATA as u64)
        );
    }

    // zstd requires dict_size to be much smaller than total sample data
    // Typically at least 100x the dictionary size in samples is recommended
    let max_dict_size = total_sample_size / 100; // At most 1% of sample data
    let actual_dict_size = if dict_size > max_dict_size {
        println!(
            "Note: Reducing dict size from {}KB to {}KB (need more samples for larger dict)",
            dict_size / 1024,
            max_dict_size.max(1024) / 1024
        );
        max_dict_size.max(1024) // At least 1KB
    } else {
        dict_size
    };

    if samples.len() < 100 {
        println!("Note: {} samples available. For best results, use 100+ pages.", samples.len());
    }

    println!("Training dictionary from {} samples ({} total)...", samples.len(), format_bytes(total_sample_size as u64));
    let dictionary = dict::train_dictionary(&samples, actual_dict_size)
        .context("Dictionary training failed")?;

    // Save dictionary
    let mut dict_file = File::create(&dict_path)?;
    dict_file.write_all(&dictionary)?;
    println!("Dictionary saved to: {} ({} bytes)", dict_path.display(), dictionary.len());

    // Now recompress the database with the new dictionary
    println!("Recompressing with dictionary (level {})...", level);
    recompress_with_dict(db, &dictionary, level)?;

    println!("Done!");
    Ok(())
}

fn extract_samples_from_sqlite(db: &Path) -> Result<Vec<Vec<u8>>> {
    let mut file = File::open(db)?;
    let file_size = file.metadata()?.len();
    let mut samples = Vec::new();

    // Read SQLite header to get page size
    let mut header = [0u8; 100];
    file.read_exact(&mut header)?;
    let page_size = u16::from_be_bytes([header[16], header[17]]) as usize;
    let page_size = if page_size == 1 { 65536 } else { page_size };

    // Read all pages as samples
    file.seek(SeekFrom::Start(0))?;
    let page_count = (file_size as usize + page_size - 1) / page_size;

    for _ in 0..page_count {
        let mut page = vec![0u8; page_size];
        match file.read_exact(&mut page) {
            Ok(()) => samples.push(page),
            Err(_) => break,
        }
    }

    Ok(samples)
}

fn extract_samples_from_sqlces(db: &Path) -> Result<Vec<Vec<u8>>> {
    // For SQLCEs files, we first convert to plain SQLite, then extract samples
    // This ensures we get the actual uncompressed page data
    let stats = inspect_database(db)?;
    if stats.page_count == 0 {
        return Ok(Vec::new());
    }

    // Create temp directory
    let temp_dir = tempfile::tempdir()?;
    let vfs_name = format!("sqlces_train_{}", std::process::id());

    // Copy the file to temp location for VFS access
    let temp_db = temp_dir.path().join("src.db");
    fs::copy(db, &temp_db)?;

    // Register VFS to read the compressed file
    let vfs = CompressedVfs::new(temp_dir.path(), 3);
    register(&vfs_name, vfs).map_err(|e| anyhow!("VFS registration failed: {}", e))?;

    // Open via VFS
    let src_conn = Connection::open_with_flags_and_vfs(
        "src.db",
        OpenFlags::SQLITE_OPEN_READ_ONLY,
        &vfs_name,
    )?;

    // Create a plain SQLite copy to extract samples from
    let plain_path = temp_dir.path().join("plain.db");
    let dst_conn = Connection::open(&plain_path)?;

    // Copy data
    copy_database(&src_conn, &dst_conn)?;
    drop(src_conn);
    drop(dst_conn);

    // Now extract samples from the plain SQLite file
    extract_samples_from_sqlite(&plain_path)
}

fn recompress_with_dict(db: &Path, _dictionary: &[u8], _level: i32) -> Result<()> {
    // TODO: Implement dictionary-based recompression
    // This would require:
    // 1. Read all pages from current database
    // 2. Decompress with current settings
    // 3. Recompress with dictionary
    // 4. Write new file
    // 5. Atomic replace
    //
    // For now, just run compaction which at least removes dead space
    println!("Note: Dictionary recompression not yet implemented. Running compaction only.");
    compact(db)?;
    Ok(())
}

fn cmd_convert(src: &Path, dst: &Path, level: i32, password: Option<String>) -> Result<()> {
    if !src.exists() {
        bail!("Source file not found: {}", src.display());
    }

    if dst.exists() {
        bail!("Destination already exists: {}", dst.display());
    }

    let src_is_sqlces = is_sqlces_file(src)?;
    let src_is_sqlite = is_sqlite_file(src)?;

    if !src_is_sqlces && !src_is_sqlite {
        bail!("Source is neither SQLite nor SQLCEs format: {}", src.display());
    }

    // Determine conversion direction
    let to_plain = level == 0 && password.is_none();

    if to_plain {
        // Converting to plain SQLite
        if src_is_sqlite {
            // Already plain, just copy
            fs::copy(src, dst)?;
            println!("Copied plain SQLite database");
        } else {
            // SQLCEs -> SQLite
            convert_sqlces_to_sqlite(src, dst)?;
        }
    } else {
        // Converting to compressed (with optional encryption)
        if src_is_sqlces && password.is_none() {
            // Already compressed, just copy and optionally recompress at new level
            convert_sqlces_to_sqlces(src, dst, level)?;
        } else {
            // SQLite -> SQLCEs or SQLCEs -> SQLCEs with encryption
            convert_to_sqlces(src, dst, level, password)?;
        }
    }

    Ok(())
}

fn convert_sqlces_to_sqlite(src: &Path, dst: &Path) -> Result<()> {
    println!("Converting SQLCEs to plain SQLite...");

    // Create temp directory for VFS
    let temp_dir = tempfile::tempdir()?;
    let vfs_name = format!("sqlces_convert_{}", std::process::id());

    // Copy source to temp
    let temp_src = temp_dir.path().join("src.db");
    fs::copy(src, &temp_src)?;

    // Register VFS
    let vfs = CompressedVfs::new(temp_dir.path(), 3);
    register(&vfs_name, vfs).map_err(|e| anyhow!("VFS registration failed: {}", e))?;

    // Open source via VFS
    let src_conn = Connection::open_with_flags_and_vfs(
        "src.db",
        OpenFlags::SQLITE_OPEN_READ_ONLY,
        &vfs_name,
    )?;

    // Create destination as plain SQLite
    let dst_conn = Connection::open(dst)?;

    // Copy schema and data
    copy_database(&src_conn, &dst_conn)?;

    println!("Converted to: {}", dst.display());
    Ok(())
}

fn convert_sqlces_to_sqlces(src: &Path, dst: &Path, level: i32) -> Result<()> {
    println!("Recompressing at level {}...", level);

    // For now, copy and compact
    // Full recompression would require reading all pages and rewriting
    fs::copy(src, dst)?;
    compact(dst)?;

    println!("Copied to: {}", dst.display());
    Ok(())
}

fn convert_to_sqlces(src: &Path, dst: &Path, level: i32, password: Option<String>) -> Result<()> {
    let src_is_sqlces = is_sqlces_file(src)?;

    println!(
        "Converting to SQLCEs (level={}{})...",
        level,
        if password.is_some() { ", encrypted" } else { "" }
    );

    // Create temp directory for VFS
    let temp_dir = tempfile::tempdir()?;
    let src_vfs_name = format!("sqlces_src_{}", std::process::id());
    let dst_vfs_name = format!("sqlces_dst_{}", std::process::id());

    // Open source
    let src_conn = if src_is_sqlces {
        let temp_src = temp_dir.path().join("src.db");
        fs::copy(src, &temp_src)?;
        let vfs = CompressedVfs::new(temp_dir.path(), 3);
        register(&src_vfs_name, vfs).map_err(|e| anyhow!("VFS registration failed: {}", e))?;
        Connection::open_with_flags_and_vfs(
            "src.db",
            OpenFlags::SQLITE_OPEN_READ_ONLY,
            &src_vfs_name,
        )?
    } else {
        Connection::open_with_flags(src, OpenFlags::SQLITE_OPEN_READ_ONLY)?
    };

    // Create destination directory
    let dst_dir = dst.parent().unwrap_or(Path::new("."));
    fs::create_dir_all(dst_dir)?;

    // Register destination VFS
    #[cfg(feature = "encryption")]
    let dst_vfs = if let Some(ref pwd) = password {
        CompressedVfs::compressed_encrypted(dst_dir, level, pwd)
    } else {
        CompressedVfs::new(dst_dir, level)
    };

    #[cfg(not(feature = "encryption"))]
    let dst_vfs = {
        if password.is_some() {
            bail!("Encryption requires the 'encryption' feature");
        }
        CompressedVfs::new(dst_dir, level)
    };

    register(&dst_vfs_name, dst_vfs).map_err(|e| anyhow!("VFS registration failed: {}", e))?;

    // Open destination
    let dst_filename = dst.file_name().unwrap().to_string_lossy();
    let dst_conn = Connection::open_with_flags_and_vfs(
        dst_filename.as_ref(),
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        &dst_vfs_name,
    )?;

    // Copy data
    copy_database(&src_conn, &dst_conn)?;

    // Checkpoint WAL
    dst_conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;

    println!("Converted to: {}", dst.display());
    Ok(())
}

fn copy_database(src: &Connection, dst: &Connection) -> Result<()> {
    // Get list of tables
    let mut stmt = src.prepare(
        "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
    )?;
    let tables: Vec<(String, String)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<_, _>>()?;

    // Copy each table
    for (table_name, create_sql) in &tables {
        // Create table in destination
        dst.execute(create_sql, [])?;

        // Get column count
        let col_count: usize = {
            let stmt = src.prepare(&format!("SELECT * FROM \"{}\" LIMIT 0", table_name))?;
            stmt.column_count()
        };

        // Copy data in batches
        let placeholders: Vec<_> = (0..col_count).map(|_| "?").collect();
        let insert_sql = format!(
            "INSERT INTO \"{}\" VALUES ({})",
            table_name,
            placeholders.join(", ")
        );

        let mut select_stmt = src.prepare(&format!("SELECT * FROM \"{}\"", table_name))?;
        let mut insert_stmt = dst.prepare(&insert_sql)?;

        dst.execute("BEGIN", [])?;
        let mut count = 0;

        let mut rows = select_stmt.query([])?;
        while let Some(row) = rows.next()? {
            let values: Vec<rusqlite::types::Value> = (0..col_count)
                .map(|i| row.get(i).unwrap_or(rusqlite::types::Value::Null))
                .collect();

            let params: Vec<&dyn rusqlite::ToSql> =
                values.iter().map(|v| v as &dyn rusqlite::ToSql).collect();
            insert_stmt.execute(params.as_slice())?;

            count += 1;
            if count % 10000 == 0 {
                dst.execute("COMMIT", [])?;
                dst.execute("BEGIN", [])?;
            }
        }
        dst.execute("COMMIT", [])?;

        println!("  Copied table '{}': {} rows", table_name, count);
    }

    // Copy indexes
    let mut stmt = src.prepare(
        "SELECT sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL",
    )?;
    let indexes: Vec<String> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?;

    for index_sql in indexes {
        dst.execute(&index_sql, [])?;
    }

    Ok(())
}

#[cfg(feature = "encryption")]
fn cmd_encrypt(db: &Path, password: &str, keep_compression: bool) -> Result<()> {
    if !db.exists() {
        bail!("File not found: {}", db.display());
    }

    let is_sqlces = is_sqlces_file(db)?;
    let level = if keep_compression && is_sqlces { 3 } else { 0 };

    // Create backup
    let backup_path = db.with_extension("bak");
    fs::copy(db, &backup_path)?;

    // Convert with encryption
    let temp_path = db.with_extension("enc.tmp");

    match convert_to_sqlces(&backup_path, &temp_path, level, Some(password.to_string())) {
        Ok(()) => {
            // Replace original
            fs::rename(&temp_path, db)?;
            fs::remove_file(&backup_path)?;
            println!("Database encrypted successfully");
            Ok(())
        }
        Err(e) => {
            // Cleanup on failure
            let _ = fs::remove_file(&temp_path);
            fs::rename(&backup_path, db)?;
            Err(e)
        }
    }
}

#[cfg(feature = "encryption")]
fn cmd_decrypt(db: &Path, password: &str, keep_compression: bool) -> Result<()> {
    if !db.exists() {
        bail!("File not found: {}", db.display());
    }

    if !is_sqlces_file(db)? {
        bail!("Not a SQLCEs database: {}", db.display());
    }

    // Create temp directory for VFS
    let temp_dir = tempfile::tempdir()?;
    let vfs_name = format!("sqlces_decrypt_{}", std::process::id());

    // Copy source to temp
    let temp_src = temp_dir.path().join("src.db");
    fs::copy(db, &temp_src)?;

    // Register VFS with password
    let vfs = CompressedVfs::compressed_encrypted(temp_dir.path(), 3, password);
    register(&vfs_name, vfs).map_err(|e| anyhow!("VFS registration failed: {}", e))?;

    // Try to open (will fail if password is wrong)
    let src_conn = Connection::open_with_flags_and_vfs(
        "src.db",
        OpenFlags::SQLITE_OPEN_READ_ONLY,
        &vfs_name,
    ).context("Failed to open database - wrong password?")?;

    // Verify we can read
    src_conn.query_row("PRAGMA schema_version", [], |_| Ok(()))
        .context("Failed to read database - wrong password?")?;

    // Create output
    let backup_path = db.with_extension("bak");
    fs::copy(db, &backup_path)?;

    let temp_out = db.with_extension("dec.tmp");

    let result = if keep_compression {
        // Output compressed but not encrypted
        let out_dir = temp_out.parent().unwrap_or(Path::new("."));
        let out_vfs_name = format!("sqlces_out_{}", std::process::id());
        let out_vfs = CompressedVfs::new(out_dir, 3);
        register(&out_vfs_name, out_vfs).map_err(|e| anyhow!("VFS registration failed: {}", e))?;

        let out_filename = temp_out.file_name().unwrap().to_string_lossy();
        let dst_conn = Connection::open_with_flags_and_vfs(
            out_filename.as_ref(),
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
            &out_vfs_name,
        )?;

        copy_database(&src_conn, &dst_conn)?;
        dst_conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
        Ok(())
    } else {
        // Output as plain SQLite
        let dst_conn = Connection::open(&temp_out)?;
        copy_database(&src_conn, &dst_conn)
    };

    match result {
        Ok(()) => {
            fs::rename(&temp_out, db)?;
            fs::remove_file(&backup_path)?;
            println!("Database decrypted successfully");
            Ok(())
        }
        Err(e) => {
            let _ = fs::remove_file(&temp_out);
            fs::rename(&backup_path, db)?;
            Err(e)
        }
    }
}

/// Header size for SQLCEs format
const HEADER_SIZE: u64 = 64;

/// Embed a compression dictionary into a SQLCEs database file.
///
/// This modifies the file in-place by:
/// 1. Reading the existing header and page data
/// 2. Updating the header with new dict_size and data_start
/// 3. Writing: header + dictionary + page data
fn cmd_embed_dict(db: &Path, dict_path: &Path, backup: bool) -> Result<()> {
    if !db.exists() {
        bail!("Database file not found: {}", db.display());
    }
    if !dict_path.exists() {
        bail!("Dictionary file not found: {}", dict_path.display());
    }
    if !is_sqlces_file(db)? {
        bail!("Not a SQLCEs database (expected SQLCEvfS magic bytes)");
    }

    // Read dictionary
    let dict_bytes = fs::read(dict_path).context("Failed to read dictionary file")?;
    let dict_size = dict_bytes.len() as u32;

    if dict_size == 0 {
        bail!("Dictionary file is empty");
    }

    println!("Embedding dictionary ({} bytes) into {}", dict_size, db.display());

    // Create backup if requested
    if backup {
        let backup_path = db.with_extension("db.backup");
        fs::copy(db, &backup_path).context("Failed to create backup")?;
        println!("Backup created: {}", backup_path.display());
    }

    // Read existing file
    let mut file = File::open(db).context("Failed to open database")?;

    // Read header
    let mut header = [0u8; 64];
    file.read_exact(&mut header).context("Failed to read header")?;

    // Parse header fields
    let old_data_start = u64::from_le_bytes(header[12..20].try_into().unwrap());
    let old_dict_size = u32::from_le_bytes(header[20..24].try_into().unwrap());

    // Read existing page data (everything after header + old dictionary)
    file.seek(SeekFrom::Start(old_data_start))?;
    let mut page_data = Vec::new();
    file.read_to_end(&mut page_data)?;
    drop(file);

    // Calculate new data_start (header + new dictionary)
    let new_data_start = HEADER_SIZE + dict_size as u64;

    // Update header with new values
    header[12..20].copy_from_slice(&new_data_start.to_le_bytes());
    header[20..24].copy_from_slice(&dict_size.to_le_bytes());

    // Write new file
    let mut file = File::create(db).context("Failed to write database")?;
    file.write_all(&header)?;
    file.write_all(&dict_bytes)?;
    file.write_all(&page_data)?;
    file.sync_all()?;

    println!("Dictionary embedded successfully");
    if old_dict_size > 0 {
        println!("  Replaced existing dictionary ({} bytes -> {} bytes)", old_dict_size, dict_size);
    } else {
        println!("  Added new dictionary ({} bytes)", dict_size);
    }
    println!("  Data start: {} -> {}", old_data_start, new_data_start);

    Ok(())
}

/// Extract embedded dictionary from a SQLCEs database file.
fn cmd_extract_dict(db: &Path, dict_out: Option<PathBuf>) -> Result<()> {
    if !db.exists() {
        bail!("Database file not found: {}", db.display());
    }
    if !is_sqlces_file(db)? {
        bail!("Not a SQLCEs database (expected SQLCEvfS magic bytes)");
    }

    // Read header
    let mut file = File::open(db).context("Failed to open database")?;
    let mut header = [0u8; 64];
    file.read_exact(&mut header).context("Failed to read header")?;

    // Parse dict_size
    let dict_size = u32::from_le_bytes(header[20..24].try_into().unwrap());

    if dict_size == 0 {
        bail!("Database does not contain an embedded dictionary");
    }

    // Read dictionary bytes (immediately after header)
    let mut dict_bytes = vec![0u8; dict_size as usize];
    file.read_exact(&mut dict_bytes).context("Failed to read dictionary")?;

    // Determine output path
    let out_path = dict_out.unwrap_or_else(|| {
        let mut p = db.to_path_buf();
        p.set_extension("dict");
        p
    });

    // Write dictionary
    fs::write(&out_path, &dict_bytes).context("Failed to write dictionary file")?;

    println!("Dictionary extracted successfully");
    println!("  Size: {} bytes", dict_size);
    println!("  Output: {}", out_path.display());

    Ok(())
}
