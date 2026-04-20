//! turbolite CLI
//!
//! Management commands for turbolite databases: inspect manifests,
//! import/export, interactive shell, and cache download.
//!
//! The CLI is the embedder of turbolite here; it picks the concrete
//! `StorageBackend` (local filesystem or S3 via hadb_storage_s3) and
//! injects it via `TurboliteVfs::with_backend`. turbolite itself
//! has no S3 deps.

use std::io::{self, BufRead};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use clap::{CommandFactory, Parser, Subcommand};

#[derive(Parser)]
#[command(name = "turbolite", version, about = "turbolite CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Show setup instructions (shell completions, etc.)
    Setup,

    /// Generate shell completions (bash, zsh, fish)
    #[command(hide = true)]
    Completions {
        /// Shell to generate completions for
        #[arg(value_enum)]
        shell: clap_complete::Shell,
    },

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

    /// Validate database integrity against S3
    Validate {
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

/// Owned tokio runtime for commands that talk to S3. Kept alive for the
/// lifetime of the VFS so background prefetch workers can drive async
/// calls. Local-mode commands return `None`.
type RuntimeGuard = Option<tokio::runtime::Runtime>;

/// Build a TurboliteConfig + concrete backend + runtime guard from CLI args.
///
/// When `bucket` is `None`, returns a local-only VFS (no runtime needed).
/// When `bucket` is `Some`, builds an `S3Storage` rooted at that bucket
/// and returns an owned tokio runtime that outlives the returned VFS.
fn build_vfs(
    cache_dir: PathBuf,
    bucket: Option<String>,
    prefix: Option<String>,
    endpoint: Option<String>,
    _region: Option<String>,
    read_only: bool,
) -> Result<(turbolite::tiered::TurboliteVfs, RuntimeGuard)> {
    use turbolite::tiered::{TurboliteConfig, TurboliteVfs};

    let config = TurboliteConfig {
        cache_dir,
        read_only,
        ..Default::default()
    };
    match bucket {
        None => Ok((
            TurboliteVfs::new_local(config).context("failed to create local VFS")?,
            None,
        )),
        Some(b) => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .context("tokio runtime")?;
            let handle = runtime.handle().clone();
            let prefix_for_s3 = prefix.unwrap_or_default();
            let endpoint_clone = endpoint.clone();
            let s3 = runtime
                .block_on(async move {
                    hadb_storage_s3::S3Storage::from_env(b, endpoint_clone.as_deref()).await
                })
                .map(|s| s.with_prefix(prefix_for_s3))
                .context("build S3Storage")?;
            let backend: Arc<dyn hadb_storage::StorageBackend> = Arc::new(s3);
            let vfs = TurboliteVfs::with_backend(config, backend, handle)
                .context("failed to create S3-backed VFS")?;
            Ok((vfs, Some(runtime)))
        }
    }
}

/// Register a VFS and open a rusqlite connection.
fn open_connection(
    db: &std::path::Path,
    vfs: turbolite::tiered::TurboliteVfs,
    vfs_name: &str,
) -> Result<rusqlite::Connection> {
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
    vfs: turbolite::tiered::TurboliteVfs,
    vfs_name: &str,
) -> Result<(turbolite::tiered::SharedTurboliteVfs, rusqlite::Connection)> {
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
    let (vfs, _runtime) = build_vfs(cache_dir, bucket, prefix, endpoint, region, true)?;
    let vfs_name = format!("turbolite-info-{}", std::process::id());
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

// ── Shell tab completion ────────────────────────────────────────────

/// Load table and column names from the database for tab completion.
fn load_schema_completions(conn: &rusqlite::Connection) -> (Vec<String>, Vec<String>) {
    let tables: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        .and_then(|mut s| {
            s.query_map([], |r| r.get(0))
                .map(|rows| rows.filter_map(|r| r.ok()).collect())
        })
        .unwrap_or_default();

    let mut columns: Vec<String> = Vec::new();
    for table in &tables {
        if let Ok(mut stmt) = conn.prepare(&format!("PRAGMA table_info(\"{}\")", table.replace('"', "\"\""))) {
            if let Ok(rows) = stmt.query_map([], |r| r.get::<_, String>(1)) {
                for col in rows.flatten() {
                    if !columns.contains(&col) {
                        columns.push(col);
                    }
                }
            }
        }
    }

    (tables, columns)
}

struct ShellCompleter {
    tables: Vec<String>,
    columns: Vec<String>,
    dot_commands: Vec<String>,
}

impl ShellCompleter {
    fn new(tables: Vec<String>, columns: Vec<String>) -> Self {
        Self {
            tables,
            columns,
            dot_commands: vec![
                ".quit".into(), ".exit".into(), ".tables".into(), ".schema".into(),
            ],
        }
    }
}

impl rustyline::completion::Completer for ShellCompleter {
    type Candidate = String;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &rustyline::Context<'_>,
    ) -> rustyline::Result<(usize, Vec<String>)> {
        let before = &line[..pos];

        // Dot commands
        if before.starts_with('.') {
            let matches: Vec<String> = self.dot_commands.iter()
                .filter(|c| c.starts_with(before))
                .cloned()
                .collect();
            return Ok((0, matches));
        }

        // Find the word being typed
        let word_start = before.rfind(|c: char| c.is_whitespace() || c == '(' || c == ',')
            .map(|i| i + 1)
            .unwrap_or(0);
        let partial = &before[word_start..];
        if partial.is_empty() {
            return Ok((pos, Vec::new()));
        }

        let partial_upper = partial.to_uppercase();

        // After FROM, JOIN, INTO, UPDATE, TABLE -- complete table names
        let upper = before.to_uppercase();
        let context_is_table = upper.contains("FROM ")
            || upper.contains("JOIN ")
            || upper.contains("INTO ")
            || upper.contains("UPDATE ")
            || upper.contains("TABLE ");

        let mut matches: Vec<String> = Vec::new();

        if context_is_table {
            for t in &self.tables {
                if t.to_uppercase().starts_with(&partial_upper) {
                    matches.push(t.clone());
                }
            }
        }

        // Always try column names too (covers SELECT, WHERE, ORDER BY, etc.)
        for c in &self.columns {
            if c.to_uppercase().starts_with(&partial_upper) && !matches.contains(c) {
                matches.push(c.clone());
            }
        }

        // SQL keywords as fallback
        let keywords = ["SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE",
            "CREATE", "DROP", "ALTER", "INDEX", "TABLE", "INTO", "VALUES",
            "SET", "AND", "OR", "NOT", "NULL", "ORDER", "BY", "GROUP",
            "HAVING", "LIMIT", "OFFSET", "JOIN", "LEFT", "INNER", "ON",
            "AS", "DISTINCT", "COUNT", "SUM", "AVG", "MIN", "MAX",
            "BEGIN", "COMMIT", "ROLLBACK", "PRAGMA", "EXPLAIN"];
        for kw in &keywords {
            if kw.starts_with(&partial_upper) && !matches.iter().any(|m| m.to_uppercase() == *kw) {
                matches.push(kw.to_string());
            }
        }

        Ok((word_start, matches))
    }
}

impl rustyline::hint::Hinter for ShellCompleter {
    type Hint = String;
}
impl rustyline::highlight::Highlighter for ShellCompleter {}
impl rustyline::validate::Validator for ShellCompleter {}
impl rustyline::Helper for ShellCompleter {}

// ── Shell command ──────────────────────────────────────────────────

fn cmd_shell(
    db: PathBuf,
    bucket: Option<String>,
    prefix: Option<String>,
    endpoint: Option<String>,
    region: Option<String>,
    cache_dir: PathBuf,
    read_only: bool,
) -> Result<()> {
    let (vfs, _runtime) = build_vfs(cache_dir, bucket, prefix, endpoint, region, read_only)?;
    let vfs_name = format!("turbolite-shell-{}", std::process::id());
    let conn = open_connection(&db, vfs, &vfs_name)?;

    let mode_str = if read_only { " (read-only)" } else { "" };
    println!("turbolite shell{} -- {}", mode_str, db.display());
    println!("Type .quit to exit, .tables to list tables, .schema to show schema.");
    println!();

    // Load schema for tab completion
    let (tables, columns) = load_schema_completions(&conn);
    let completer = ShellCompleter::new(tables, columns);

    // Use rustyline for interactive TTY, fall back to stdin for piped input
    let is_tty = atty::is(atty::Stream::Stdin);

    if is_tty {
        let config = rustyline::Config::builder()
            .auto_add_history(true)
            .build();
        let mut rl = rustyline::Editor::with_config(config)
            .context("failed to create readline editor")?;
        rl.set_helper(Some(completer));

        loop {
            match rl.readline("turbolite> ") {
                Ok(line) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() { continue; }
                    if trimmed == ".quit" || trimmed == ".exit" { break; }
                    execute_shell_line(&conn, trimmed);
                }
                Err(rustyline::error::ReadlineError::Eof) => {
                    println!();
                    break;
                }
                Err(rustyline::error::ReadlineError::Interrupted) => {
                    break;
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    break;
                }
            }
        }
    } else {
        // Piped input (tests, scripts) -- no readline, no completion
        let stdin = io::stdin();
        loop {
            let mut line = String::new();
            if stdin.lock().read_line(&mut line)? == 0 {
                println!();
                break;
            }
            let trimmed = line.trim();
            if trimmed.is_empty() { continue; }
            if trimmed == ".quit" || trimmed == ".exit" { break; }
            execute_shell_line(&conn, trimmed);
        }
    }

    Ok(())
}

fn execute_shell_line(conn: &rusqlite::Connection, trimmed: &str) {
    match trimmed {
        ".tables" => {
            let sql = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name";
            match conn.prepare(sql) {
                Ok(mut stmt) => {
                    if let Ok(rows) = stmt.query_map([], |row| row.get::<_, String>(0)) {
                        for row in rows.flatten() {
                            println!("{}", row);
                        }
                    }
                }
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        ".schema" => {
            let sql = "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type, name";
            match conn.prepare(sql) {
                Ok(mut stmt) => {
                    if let Ok(rows) = stmt.query_map([], |row| row.get::<_, String>(0)) {
                        for row in rows.flatten() {
                            println!("{};", row);
                        }
                    }
                }
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        _ => {
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
                        println!("{}", col_names.join("|"));

                        if let Ok(rows) = stmt.query_map([], |row| {
                            let vals: Vec<String> = (0..col_count)
                                .map(|i| {
                                    row.get::<_, rusqlite::types::Value>(i)
                                        .map(|v| format_value(&v))
                                        .unwrap_or_else(|_| "ERROR".to_string())
                                })
                                .collect();
                            Ok(vals.join("|"))
                        }) {
                            for row in rows.flatten() {
                                println!("{}", row);
                            }
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

const MAX_COLUMN_WIDTH: usize = 80;

fn format_value(v: &rusqlite::types::Value) -> String {
    let s = match v {
        rusqlite::types::Value::Null => return "NULL".to_string(),
        rusqlite::types::Value::Integer(i) => return i.to_string(),
        rusqlite::types::Value::Real(f) => return f.to_string(),
        rusqlite::types::Value::Text(s) => s.as_str(),
        rusqlite::types::Value::Blob(b) => return format!("x'{}'", hex_encode(b)),
    };
    if s.len() > MAX_COLUMN_WIDTH {
        format!("{}...", &s[..MAX_COLUMN_WIDTH])
    } else {
        s.to_string()
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
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
    let (vfs, _runtime) = build_with_prefetch_threads(
        cache_dir, Some(bucket), prefix, endpoint, region, true, threads,
    )?;
    let vfs_name = format!("turbolite-download-{}", std::process::id());
    let (shared, conn) = open_shared(&db, vfs, &vfs_name)?;

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

    println!("download: {} tables, {} groups", tables.len(), total_groups);

    Ok(())
}

fn build_with_prefetch_threads(
    cache_dir: PathBuf,
    bucket: Option<String>,
    prefix: Option<String>,
    endpoint: Option<String>,
    _region: Option<String>,
    read_only: bool,
    prefetch_threads: u32,
) -> Result<(turbolite::tiered::TurboliteVfs, RuntimeGuard)> {
    use turbolite::tiered::{PrefetchConfig, TurboliteConfig, TurboliteVfs};
    let config = TurboliteConfig {
        cache_dir,
        read_only,
        prefetch: PrefetchConfig { threads: prefetch_threads, ..Default::default() },
        ..Default::default()
    };
    match bucket {
        None => Ok((
            TurboliteVfs::new_local(config).context("failed to create local VFS")?,
            None,
        )),
        Some(b) => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .context("tokio runtime")?;
            let handle = runtime.handle().clone();
            let prefix_for_s3 = prefix.unwrap_or_default();
            let endpoint_clone = endpoint.clone();
            let s3 = runtime
                .block_on(async move {
                    hadb_storage_s3::S3Storage::from_env(b, endpoint_clone.as_deref()).await
                })
                .map(|s| s.with_prefix(prefix_for_s3))
                .context("build S3Storage")?;
            let backend: Arc<dyn hadb_storage::StorageBackend> = Arc::new(s3);
            let vfs = TurboliteVfs::with_backend(config, backend, handle)
                .context("failed to create S3-backed VFS")?;
            Ok((vfs, Some(runtime)))
        }
    }
}

fn cmd_validate(
    db: PathBuf,
    bucket: String,
    prefix: Option<String>,
    endpoint: Option<String>,
    region: Option<String>,
    cache_dir: PathBuf,
) -> Result<()> {
    let (vfs, _runtime) = build_vfs(cache_dir, Some(bucket), prefix, endpoint, region, true)?;
    let vfs_name = format!("turbolite-validate-{}", std::process::id());
    let (shared, conn) = open_shared(&db, vfs, &vfs_name)?;

    println!("validating manifest...");
    let result = shared.validate()
        .map_err(|e| anyhow!("validate failed: {}", e))?;

    println!(
        "  page groups:       {}/{} present",
        result.page_groups_present, result.page_groups_total,
    );
    for key in &result.page_groups_missing {
        println!("    MISSING: {}", key);
    }

    println!(
        "  interior chunks:   {}/{} present",
        result.interior_chunks_present, result.interior_chunks_total,
    );
    for key in &result.interior_chunks_missing {
        println!("    MISSING: {}", key);
    }

    println!(
        "  index chunks:      {}/{} present",
        result.index_chunks_present, result.index_chunks_total,
    );
    for key in &result.index_chunks_missing {
        println!("    MISSING: {}", key);
    }

    println!("  orphaned objects:  {}", result.orphaned_keys.len());
    for key in &result.orphaned_keys {
        println!("    ORPHAN: {}", key);
    }

    if !result.decode_errors.is_empty() {
        println!("\nvalidating data...");
        for (key, err) in &result.decode_errors {
            println!("  DECODE ERROR: {} -- {}", key, err);
        }
    } else {
        println!("\nvalidating data...");
        println!("  page groups:       {}/{} decode ok", result.page_groups_present, result.page_groups_total);
    }

    println!("\nvalidating database...");
    let integrity: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .context("integrity_check failed")?;
    println!("  integrity_check:   {}", integrity);

    println!();
    if result.s3_ok() && integrity == "ok" {
        println!("validate: passed");
    } else {
        println!("validate: FAILED");
        std::process::exit(1);
    }

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
    let (vfs, _runtime) = build_vfs(cache_dir, bucket, prefix, endpoint, region, true)?;
    let vfs_name = format!("turbolite-export-{}", std::process::id());
    let src_conn = open_connection(&db, vfs, &vfs_name)?;

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

fn cmd_import(
    input: PathBuf,
    bucket: String,
    prefix: Option<String>,
    endpoint: Option<String>,
    _region: Option<String>,
    pages_per_group: u32,
    compression_level: i32,
) -> Result<()> {
    use turbolite::tiered::{CacheConfig, CompressionConfig, TurboliteConfig};

    let prefix_str = prefix.unwrap_or_default();
    let config = TurboliteConfig {
        cache: CacheConfig { pages_per_group, ..Default::default() },
        compression: CompressionConfig { level: compression_level, ..Default::default() },
        ..Default::default()
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .context("tokio runtime")?;
    let handle = runtime.handle().clone();
    let prefix_for_s3 = prefix_str.clone();
    let endpoint_clone = endpoint.clone();
    let bucket_clone = bucket.clone();
    let s3 = runtime
        .block_on(async move {
            hadb_storage_s3::S3Storage::from_env(bucket_clone, endpoint_clone.as_deref()).await
        })
        .map(|s| s.with_prefix(prefix_for_s3))
        .context("build S3Storage")?;
    let backend: Arc<dyn hadb_storage::StorageBackend> = Arc::new(s3);

    let manifest = turbolite::tiered::import_sqlite_file(&config, backend, handle, &input)
        .map_err(|e| anyhow!("import failed: {}", e))?;

    println!(
        "import: {} -> s3://{}/{} ({} pages, {} groups, manifest v{})",
        input.display(),
        bucket,
        prefix_str,
        manifest.page_count,
        manifest.page_group_keys.len(),
        manifest.version,
    );

    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Setup => {
            println!("turbolite setup");
            println!();
            println!("  Shell completions:");
            println!("    bash:  eval \"$(turbolite completions bash)\"");
            println!("    zsh:   eval \"$(turbolite completions zsh)\"");
            println!("    fish:  turbolite completions fish > ~/.config/fish/completions/turbolite.fish");
            println!();
            println!("  Add the line for your shell to your profile (~/.bashrc, ~/.zshrc, etc.)");
            println!("  to enable tab completion for turbolite commands and flags.");
        }
        Commands::Completions { shell } => {
            clap_complete::generate(
                shell,
                &mut Cli::command(),
                "turbolite",
                &mut std::io::stdout(),
            );
        }
        Commands::Info { db, bucket, prefix, endpoint, region, cache_dir } => {
            cmd_info(db, bucket, prefix, endpoint, region, cache_dir)?;
        }
        Commands::Shell { db, bucket, prefix, endpoint, region, cache_dir, read_only } => {
            cmd_shell(db, bucket, prefix, endpoint, region, cache_dir, read_only)?;
        }
        Commands::Download { db, bucket, prefix, endpoint, region, cache_dir, threads } => {
            cmd_download(db, bucket, prefix, endpoint, region, cache_dir, threads)?;
        }
        Commands::Validate { db, bucket, prefix, endpoint, region, cache_dir } => {
            cmd_validate(db, bucket, prefix, endpoint, region, cache_dir)?;
        }
        Commands::Export { db, output, bucket, prefix, endpoint, region, cache_dir } => {
            cmd_export(db, output, bucket, prefix, endpoint, region, cache_dir)?;
        }
        Commands::Import {
            input, bucket, prefix, endpoint, region,
            pages_per_group, compression_level,
        } => {
            cmd_import(input, bucket, prefix, endpoint, region, pages_per_group, compression_level)?;
        }
    }

    Ok(())
}
