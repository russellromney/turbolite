//! tiered-tune - Prefetch schedule tuner for existing turbolite databases.
//!
//! Connects to an existing S3-backed database and sweeps prefetch schedule
//! combinations against user-provided queries. Outputs a comparison table
//! per query and recommends optimal schedules.
//!
//! ```bash
//! TIERED_TEST_BUCKET=sqlces-test \
//!   AWS_ENDPOINT_URL=https://t3.storage.dev \
//!   cargo run --release --features tiered,zstd --bin tiered-tune -- \
//!   --prefix "bench/social_100000/20260325_123456" \
//!   --query "SELECT * FROM users WHERE id = 42" \
//!   --iterations 10
//! ```

use clap::Parser;
use rusqlite::{Connection, OpenFlags};
use turbolite::tiered::{
    TurboliteSharedState, TurboliteConfig, TurboliteVfs,
    parse_eqp_output, push_planned_accesses, push_setting,
};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;

static VFS_COUNTER: AtomicU32 = AtomicU32::new(0);

// Post-Phase-Anvil-g: build an S3 backend + wrap the VFS's SharedState with
// baseline-subtraction counters (see tiered-bench for the same pattern).
fn build_s3_backend(
    runtime: &tokio::runtime::Handle,
    prefix: &str,
) -> Arc<hadb_storage_s3::S3Storage> {
    let bucket = test_bucket();
    let endpoint = endpoint_url();
    let storage = runtime.block_on(async {
        hadb_storage_s3::S3Storage::from_env(bucket, endpoint.as_deref())
            .await
            .expect("build S3Storage")
    });
    Arc::new(storage.with_prefix(prefix.to_string()))
}

struct BenchCtx<'a> {
    state: &'a TurboliteSharedState,
    s3: Arc<hadb_storage_s3::S3Storage>,
    fetch_count_base: AtomicU64,
    bytes_fetched_base: AtomicU64,
}

impl<'a> BenchCtx<'a> {
    fn new(state: &'a TurboliteSharedState, s3: Arc<hadb_storage_s3::S3Storage>) -> Self {
        Self {
            fetch_count_base: AtomicU64::new(s3.fetch_count()),
            bytes_fetched_base: AtomicU64::new(s3.bytes_fetched()),
            state,
            s3,
        }
    }
    fn clear_cache_data_only(&self) { self.state.clear_cache_data_only(); }
    fn clear_cache_all(&self) { self.state.clear_cache_all(); }
    fn reset_s3_counters(&self) {
        self.fetch_count_base.store(self.s3.fetch_count(), Ordering::Relaxed);
        self.bytes_fetched_base.store(self.s3.bytes_fetched(), Ordering::Relaxed);
    }
    fn s3_counters(&self) -> (u64, u64) {
        let fc = self.s3.fetch_count().saturating_sub(self.fetch_count_base.load(Ordering::Relaxed));
        let fb = self.s3.bytes_fetched().saturating_sub(self.bytes_fetched_base.load(Ordering::Relaxed));
        (fc, fb)
    }
}

#[derive(Parser)]
#[command(name = "tiered-tune")]
#[command(about = "Prefetch schedule tuner for existing turbolite databases")]
struct Cli {
    /// S3 prefix of the existing database (e.g. "bench/social_100000/20260325_123456")
    #[arg(long, env = "TUNE_PREFIX")]
    prefix: String,

    /// SQL queries to tune (repeatable). Each query gets its own comparison table.
    /// Use ?1, ?2 etc. for parameters; provide values via --param.
    #[arg(long = "query", env = "TUNE_QUERIES")]
    queries: Vec<String>,

    /// Query parameter values (repeatable, applied to all queries in order).
    /// Integers, floats, and strings are auto-detected.
    #[arg(long = "param", env = "TUNE_PARAMS")]
    params: Vec<String>,

    /// Number of measured iterations per query per schedule pair.
    #[arg(long, default_value = "10", env = "TUNE_ITERATIONS")]
    iterations: usize,

    /// Warmup iterations (not measured).
    #[arg(long, default_value = "2", env = "TUNE_WARMUP")]
    warmup: usize,

    /// Pages per page group (must match the imported database).
    #[arg(long, default_value = "256", env = "TUNE_PPG")]
    ppg: u32,

    /// Page size (must match the imported database).
    #[arg(long, default_value = "65536", env = "TUNE_PAGE_SIZE")]
    page_size: u32,

    /// Number of prefetch worker threads.
    #[arg(long, default_value = "8", env = "TUNE_PREFETCH_THREADS")]
    prefetch_threads: u32,

    /// Search schedules to test (semicolon-separated, each is comma-separated fractions).
    /// Default covers conservative to aggressive.
    #[arg(long, default_value = "0.3,0.3,0.4;0.5,0.5;1.0;0.2,0.3,0.5;0.4,0.3,0.3", env = "TUNE_SEARCH_SCHEDULES")]
    search_schedules: String,

    /// Lookup schedules to test (semicolon-separated).
    /// Default covers zero-heavy to moderate.
    #[arg(long, default_value = "0;0,0,0.1;0,0.1,0.2;0,0,0,0.1,0.2;0.1,0.1,0.2", env = "TUNE_LOOKUP_SCHEDULES")]
    lookup_schedules: String,

    /// Also include "off/off" baseline (no prefetch at all).
    #[arg(long, default_value = "true", env = "TUNE_BASELINE")]
    baseline: bool,

    /// Cache level to test at: "none" (full cold) or "index" (index cached, data from S3).
    #[arg(long, default_value = "none", env = "TUNE_CACHE_LEVEL")]
    cache_level: String,

    /// Enable plan-aware prefetch (Phase Marne).
    #[arg(long, env = "TUNE_PLAN_AWARE")]
    plan_aware: bool,
}

// =========================================================================
// Schedule pair (same as tiered-bench)
// =========================================================================

#[derive(Clone, Debug)]
struct SchedulePair {
    search: Option<Vec<f32>>,
    lookup: Option<Vec<f32>>,
}

impl SchedulePair {
    fn pair(search: Option<Vec<f32>>, lookup: Option<Vec<f32>>) -> Self {
        Self { search, lookup }
    }

    fn off() -> Self {
        Self { search: None, lookup: None }
    }

    fn push(&self) {
        let zeros = "0,0,0,0,0,0,0,0,0,0".to_string();
        let search_str = self.search.as_ref()
            .map(|s| s.iter().map(|f| f.to_string()).collect::<Vec<_>>().join(","))
            .unwrap_or_else(|| zeros.clone());
        let lookup_str = self.lookup.as_ref()
            .map(|s| s.iter().map(|f| f.to_string()).collect::<Vec<_>>().join(","))
            .unwrap_or(zeros);
        push_setting("prefetch_search".to_string(), search_str);
        push_setting("prefetch_lookup".to_string(), lookup_str);
    }

    fn label(&self) -> String {
        let fmt = |s: &Option<Vec<f32>>| -> String {
            match s {
                Some(v) => v.iter().map(|f| format!("{:.2}", f)).collect::<Vec<_>>().join(","),
                None => "off".to_string(),
            }
        };
        format!("{} / {}", fmt(&self.search), fmt(&self.lookup))
    }
}

// =========================================================================
// Helpers
// =========================================================================

fn unique_vfs_name() -> String {
    let n = VFS_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("tune_{}", n)
}

fn test_bucket() -> String {
    std::env::var("TIERED_TEST_BUCKET")
        .or_else(|_| std::env::var("BUCKET_NAME"))
        .expect("TIERED_TEST_BUCKET or BUCKET_NAME env var required")
}

fn endpoint_url() -> Option<String> {
    std::env::var("AWS_ENDPOINT_URL")
        .or_else(|_| std::env::var("AWS_ENDPOINT_URL_S3"))
        .ok()
}

fn percentile(latencies: &[f64], p: f64) -> f64 {
    if latencies.is_empty() { return 0.0; }
    let mut sorted = latencies.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((p * sorted.len() as f64) as usize).min(sorted.len() - 1);
    sorted[idx]
}

fn format_ms(us: f64) -> String {
    if us >= 1_000_000.0 {
        format!("{:.1}s", us / 1_000_000.0)
    } else if us >= 1000.0 {
        format!("{:.1}ms", us / 1000.0)
    } else {
        format!("{:.0}us", us)
    }
}

fn format_kb(kb: f64) -> String {
    if kb >= 1024.0 {
        format!("{:.1}MB", kb / 1024.0)
    } else {
        format!("{:.0}KB", kb)
    }
}

fn parse_schedule(s: &str) -> Vec<f32> {
    s.split(',')
        .filter_map(|v| v.trim().parse::<f32>().ok())
        .collect()
}

fn parse_param(s: &str) -> rusqlite::types::Value {
    if let Ok(i) = s.parse::<i64>() {
        return rusqlite::types::Value::Integer(i);
    }
    if let Ok(f) = s.parse::<f64>() {
        return rusqlite::types::Value::Real(f);
    }
    rusqlite::types::Value::Text(s.to_string())
}

struct BenchResult {
    label: String,
    latencies_us: Vec<f64>,
    s3_fetches: Vec<u64>,
    s3_bytes: Vec<u64>,
}

impl BenchResult {
    fn p50(&self) -> f64 { percentile(&self.latencies_us, 0.5) }
    fn p90(&self) -> f64 { percentile(&self.latencies_us, 0.9) }
    fn avg_fetches(&self) -> f64 {
        if self.s3_fetches.is_empty() { return 0.0; }
        self.s3_fetches.iter().sum::<u64>() as f64 / self.s3_fetches.len() as f64
    }
    fn avg_bytes_kb(&self) -> f64 {
        if self.s3_bytes.is_empty() { return 0.0; }
        (self.s3_bytes.iter().sum::<u64>() as f64 / self.s3_bytes.len() as f64) / 1024.0
    }
}

// =========================================================================
// Query plan push (same as tiered-bench)
// =========================================================================

fn push_query_plan(conn: &Connection, sql: &str, params: &[rusqlite::types::Value]) {
    let eqp_sql = format!("EXPLAIN QUERY PLAN {}", sql);
    let mut stmt = match conn.prepare(&eqp_sql) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("  [push-plan] prepare FAILED: {}", e);
            return;
        }
    };
    let mut output = String::new();
    let mut rows = match stmt.query(rusqlite::params_from_iter(params)) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("  [push-plan] query FAILED: {}", e);
            return;
        }
    };
    while let Ok(Some(row)) = rows.next() {
        if let Ok(detail) = row.get::<_, String>(3) {
            output.push_str(&detail);
            output.push('\n');
        }
    }
    let accesses = parse_eqp_output(&output);
    push_planned_accesses(accesses);
}

fn run_query_pair(
    conn: &Connection,
    sql: &str,
    params: &[rusqlite::types::Value],
    plan_aware: bool,
    pair: &SchedulePair,
) -> Result<usize, rusqlite::Error> {
    pair.push();
    if plan_aware {
        push_query_plan(conn, sql, params);
    }
    let mut stmt = conn.prepare_cached(sql)?;
    let rows: Vec<Vec<rusqlite::types::Value>> = stmt
        .query_map(rusqlite::params_from_iter(params), |row| {
            let n = row.as_ref().column_count();
            let mut vals = Vec::with_capacity(n);
            for i in 0..n {
                vals.push(row.get::<_, rusqlite::types::Value>(i)?);
            }
            Ok(vals)
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows.len())
}

// =========================================================================
// Cold benchmark (cache level: none)
// =========================================================================

fn bench_cold(
    vfs_name: &str,
    db_name: &str,
    handle: &BenchCtx,
    sql: &str,
    params: &[rusqlite::types::Value],
    warmup: usize,
    iterations: usize,
    plan_aware: bool,
    pair: &SchedulePair,
) -> BenchResult {
    // Warmup (not measured)
    for _ in 0..warmup {
        handle.clear_cache_all();
        handle.reset_s3_counters();
        let conn = Connection::open_with_flags_and_vfs(
            db_name, OpenFlags::SQLITE_OPEN_READ_ONLY, vfs_name,
        ).expect("tune connection");
        let _ = run_query_pair(&conn, sql, params, plan_aware, pair);
        drop(conn);
    }

    let mut latencies = Vec::with_capacity(iterations);
    let mut s3_fetches = Vec::with_capacity(iterations);
    let mut s3_bytes = Vec::with_capacity(iterations);

    for i in 0..iterations {
        handle.clear_cache_all();
        handle.reset_s3_counters();

        let start = Instant::now();
        let conn = Connection::open_with_flags_and_vfs(
            db_name, OpenFlags::SQLITE_OPEN_READ_ONLY, vfs_name,
        ).expect("tune connection");
        match run_query_pair(&conn, sql, params, plan_aware, pair) {
            Ok(_) => {
                latencies.push(start.elapsed().as_micros() as f64);
                let (fc, fb) = handle.s3_counters();
                s3_fetches.push(fc);
                s3_bytes.push(fb);
            }
            Err(e) => eprintln!("  [tune] iter {} error: {}", i, e),
        }
        drop(conn);
    }

    BenchResult {
        label: pair.label(),
        latencies_us: latencies,
        s3_fetches,
        s3_bytes,
    }
}

// =========================================================================
// Index-level benchmark (interior + index cached, data from S3)
// =========================================================================

fn bench_index_level(
    vfs_name: &str,
    db_name: &str,
    handle: &BenchCtx,
    sql: &str,
    params: &[rusqlite::types::Value],
    warmup: usize,
    iterations: usize,
    plan_aware: bool,
    pair: &SchedulePair,
) -> BenchResult {
    for _ in 0..warmup {
        handle.clear_cache_data_only();
        handle.reset_s3_counters();
        let conn = Connection::open_with_flags_and_vfs(
            db_name, OpenFlags::SQLITE_OPEN_READ_ONLY, vfs_name,
        ).expect("tune connection");
        let _ = run_query_pair(&conn, sql, params, plan_aware, pair);
        drop(conn);
    }

    let mut latencies = Vec::with_capacity(iterations);
    let mut s3_fetches = Vec::with_capacity(iterations);
    let mut s3_bytes = Vec::with_capacity(iterations);

    for i in 0..iterations {
        handle.clear_cache_data_only();
        handle.reset_s3_counters();

        let start = Instant::now();
        let conn = Connection::open_with_flags_and_vfs(
            db_name, OpenFlags::SQLITE_OPEN_READ_ONLY, vfs_name,
        ).expect("tune connection");
        match run_query_pair(&conn, sql, params, plan_aware, pair) {
            Ok(_) => {
                latencies.push(start.elapsed().as_micros() as f64);
                let (fc, fb) = handle.s3_counters();
                s3_fetches.push(fc);
                s3_bytes.push(fb);
            }
            Err(e) => eprintln!("  [tune] iter {} error: {}", i, e),
        }
        drop(conn);
    }

    BenchResult {
        label: pair.label(),
        latencies_us: latencies,
        s3_fetches,
        s3_bytes,
    }
}

// =========================================================================
// Main
// =========================================================================

fn main() {
    let cli = Cli::parse();

    if cli.queries.is_empty() {
        eprintln!("Error: at least one --query is required");
        std::process::exit(1);
    }

    // Parse parameters
    let params: Vec<rusqlite::types::Value> = cli.params.iter().map(|s| parse_param(s)).collect();

    // Build schedule grid
    let search_scheds: Vec<Vec<f32>> = cli.search_schedules
        .split(';')
        .map(|s| parse_schedule(s.trim()))
        .filter(|v| !v.is_empty())
        .collect();
    let lookup_scheds: Vec<Vec<f32>> = cli.lookup_schedules
        .split(';')
        .map(|s| parse_schedule(s.trim()))
        .filter(|v| !v.is_empty())
        .collect();

    let mut pairs: Vec<SchedulePair> = Vec::new();
    if cli.baseline {
        pairs.push(SchedulePair::off());
    }
    for search in &search_scheds {
        for lookup in &lookup_scheds {
            pairs.push(SchedulePair::pair(Some(search.clone()), Some(lookup.clone())));
        }
    }

    let n_pairs = pairs.len();
    let n_queries = cli.queries.len();
    let total_runs = n_pairs * n_queries * cli.iterations;

    // Print header
    println!("=== tiered-tune: Prefetch Schedule Tuner ===");
    println!("Bucket:       {}", test_bucket());
    println!("Endpoint:     {}", endpoint_url().as_deref().unwrap_or("(default S3)"));
    println!("Prefix:       {}", cli.prefix);
    println!("Page size:    {} bytes", cli.page_size);
    println!("Pages/group:  {}", cli.ppg);
    println!("Cache level:  {}", cli.cache_level);
    println!("Plan-aware:   {}", if cli.plan_aware { "ENABLED" } else { "disabled" });
    println!("Queries:      {}", n_queries);
    println!("Schedule pairs: {} ({} search x {} lookup{})",
        n_pairs,
        search_scheds.len(),
        lookup_scheds.len(),
        if cli.baseline { " + baseline" } else { "" },
    );
    println!("Iterations:   {} measured + {} warmup", cli.iterations, cli.warmup);
    println!("Total runs:   {}", total_runs);
    println!();

    // Register VFS
    let cache_dir = TempDir::new().expect("failed to create temp dir");
    let vfs_name = unique_vfs_name();
    let config = TurboliteConfig {
        cache_dir: cache_dir.path().to_path_buf(),
        compression_level: 1,
        read_only: true,
        pages_per_group: cli.ppg,
        prefetch_threads: cli.prefetch_threads,
        query_plan_prefetch: cli.plan_aware,
        ..Default::default()
    };

    let owned_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("build bench runtime");
    let rt_handle = owned_runtime.handle().clone();
    let s3 = build_s3_backend(&rt_handle, &cli.prefix);
    let vfs = TurboliteVfs::new_with_storage(
        config,
        s3.clone() as Arc<dyn hadb_storage::StorageBackend>,
        rt_handle.clone(),
    ).expect("failed to create VFS");
    let shared_state = vfs.shared_state();
    let handle = BenchCtx::new(&shared_state, s3.clone());
    turbolite::tiered::register(&vfs_name, vfs).expect("failed to register VFS");

    let db_name = "tune.db";

    // Verify connection works
    {
        let conn = Connection::open_with_flags_and_vfs(
            db_name, OpenFlags::SQLITE_OPEN_READ_ONLY, &vfs_name,
        ).expect("failed to open database");
        let page_count: i64 = conn
            .query_row("PRAGMA page_count", [], |row| row.get(0))
            .expect("PRAGMA page_count failed");
        let page_size: i64 = conn
            .query_row("PRAGMA page_size", [], |row| row.get(0))
            .expect("PRAGMA page_size failed");
        println!("Database: {} pages x {} bytes = {:.1} MB",
            page_count, page_size,
            page_count as f64 * page_size as f64 / (1024.0 * 1024.0),
        );

        // Show tables
        let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").unwrap();
        let tables: Vec<String> = stmt.query_map([], |row| row.get(0)).unwrap()
            .filter_map(|r| r.ok())
            .collect();
        println!("Tables:   {}", tables.join(", "));
        println!();
    }

    let use_cold = cli.cache_level == "none";

    // Run each query
    for (qi, sql) in cli.queries.iter().enumerate() {
        let short_sql = if sql.len() > 60 { &sql[..60] } else { sql };
        println!("  --- Query {}: {} ---", qi + 1, short_sql);
        println!(
            "  {:<36} {:>10} {:>10} {:>10} {:>12}",
            "search / lookup", "p50", "p90", "GETs", "bytes"
        );
        println!(
            "  {:-<36} {:->10} {:->10} {:->10} {:->12}",
            "", "", "", "", ""
        );

        let mut best_p50 = f64::MAX;
        let mut best_label = String::new();
        let mut best_pair_idx = 0;

        for (pi, pair) in pairs.iter().enumerate() {
            let r = if use_cold {
                bench_cold(&vfs_name, db_name, &handle, sql, &params, cli.warmup, cli.iterations, cli.plan_aware, pair)
            } else {
                bench_index_level(&vfs_name, db_name, &handle, sql, &params, cli.warmup, cli.iterations, cli.plan_aware, pair)
            };

            let p50 = r.p50();
            println!(
                "  {:<36} {:>10} {:>10} {:>10} {:>12}",
                r.label,
                format_ms(p50),
                format_ms(r.p90()),
                format!("{:.1}", r.avg_fetches()),
                format_kb(r.avg_bytes_kb()),
            );

            if p50 < best_p50 {
                best_p50 = p50;
                best_label = r.label.clone();
                best_pair_idx = pi;
            }
        }

        println!();
        println!("  Best: {} (p50 = {})", best_label, format_ms(best_p50));

        // Print recommended TurboliteConfig values (set at construction time).
        let best = &pairs[best_pair_idx];
        if let Some(ref search) = best.search {
            let s = search.iter().map(|f| f.to_string()).collect::<Vec<_>>().join(", ");
            println!("  TurboliteConfig.prefetch_search = vec![{}];", s);
        }
        if let Some(ref lookup) = best.lookup {
            let s = lookup.iter().map(|f| f.to_string()).collect::<Vec<_>>().join(", ");
            println!("  TurboliteConfig.prefetch_lookup = vec![{}];", s);
        }
        if best.search.is_none() && best.lookup.is_none() {
            println!("  // No prefetch recommended (off/off was fastest)");
        }
        println!();
    }

    println!("Done.");
}
