//! tiered-bench - Benchmark for tiered S3-backed VFS
//!
//! Early-Facebook-style dataset:
//!   - users, posts, friendships, likes (4 tables, indexed)
//!   - Point lookups, JOINs, indexed filters, scans
//!
//! Four cache levels:
//!   none     = nothing cached (full cold start from S3)
//!   interior = interior B-tree pages cached, index + data from S3
//!   index    = interior + index pages cached, data from S3
//!   data     = everything cached (warm, measures pread latency)
//!
//! ```bash
//! TIERED_TEST_BUCKET=turbolite-test \
//!   AWS_ENDPOINT_URL=https://t3.storage.dev \
//!   cargo run --release --features tiered,zstd --bin tiered-bench
//! ```

use clap::Parser;
use rusqlite::{Connection, OpenFlags};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;
use turbolite::tiered::{
    parse_eqp_output, push_planned_accesses, set_local_checkpoint_only, CacheConfig,
    CompressionConfig, PrefetchConfig, TurboliteConfig, TurboliteSharedState, TurboliteVfs,
};

static VFS_COUNTER: AtomicU32 = AtomicU32::new(0);

// =========================================================================
// Post-Phase-Anvil-g storage wiring
//
// The VFS no longer owns an S3Client and no longer exposes byte counters.
// Benchmarks that want counters hold an Arc<S3Storage> themselves and read
// the backend's own counters. `BenchCtx` pairs the SharedState (cache
// control) with the S3Storage (counters) and exposes the same
// `clear_cache_*`, `reset_s3_counters`, `s3_counters` method names the
// pre-Anvil-g bench code called on `TurboliteSharedState`. Baseline
// subtraction is used for "reset" because S3Storage is cumulative.
// =========================================================================

/// Build an S3 backend from env. Shared by every site that used to build a
/// TurboliteConfig with `bucket`/`prefix`/`endpoint_url`/`region`.
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

/// Wrapper that pairs shared-state cache control with an S3Storage counter
/// reader. Baseline subtraction gives the illusion of `reset_s3_counters`.
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

    fn clear_cache_data_only(&self) {
        self.state.clear_cache_data_only();
    }
    fn clear_cache_interior_only(&self) {
        self.state.clear_cache_interior_only();
    }
    fn clear_cache_all(&self) {
        self.state.clear_cache_all();
    }

    /// Snapshot the current S3 counters as a new baseline. Subsequent
    /// `s3_counters()` calls report deltas from this moment.
    fn reset_s3_counters(&self) {
        self.fetch_count_base
            .store(self.s3.fetch_count(), Ordering::Relaxed);
        self.bytes_fetched_base
            .store(self.s3.bytes_fetched(), Ordering::Relaxed);
    }

    fn s3_counters(&self) -> (u64, u64) {
        let fc = self
            .s3
            .fetch_count()
            .saturating_sub(self.fetch_count_base.load(Ordering::Relaxed));
        let fb = self
            .s3
            .bytes_fetched()
            .saturating_sub(self.bytes_fetched_base.load(Ordering::Relaxed));
        (fc, fb)
    }
}

// =========================================================================
// Data constants
// =========================================================================

const FIRST_NAMES: &[&str] = &[
    "Mark",
    "Eduardo",
    "Dustin",
    "Chris",
    "Sean",
    "Priscilla",
    "Sheryl",
    "Andrew",
    "Adam",
    "Mike",
    "Sarah",
    "Jessica",
    "Emily",
    "David",
    "Alex",
    "Randi",
    "Naomi",
    "Kevin",
    "Amy",
    "Dan",
    "Lisa",
    "Tom",
    "Rachel",
    "Brian",
    "Caitlin",
    "Nicole",
    "Matt",
    "Laura",
    "Jake",
    "Megan",
];

const LAST_NAMES: &[&str] = &[
    "Zuckerberg",
    "Saverin",
    "Moskovitz",
    "Hughes",
    "Parker",
    "Chan",
    "Sandberg",
    "McCollum",
    "D'Angelo",
    "Schroepfer",
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Rodriguez",
    "Martinez",
    "Anderson",
    "Taylor",
    "Thomas",
    "Hernandez",
    "Moore",
    "Martin",
    "Jackson",
    "Thompson",
    "White",
    "Lopez",
];

const SCHOOLS: &[&str] = &[
    "Harvard",
    "Stanford",
    "MIT",
    "Yale",
    "Princeton",
    "Columbia",
    "Penn",
    "Brown",
    "Cornell",
    "Dartmouth",
    "Duke",
    "Georgetown",
    "UCLA",
    "Berkeley",
    "Michigan",
    "NYU",
    "Boston University",
    "Northeastern",
    "USC",
    "Emory",
];

const CITIES: &[&str] = &[
    "Palo Alto, CA",
    "San Francisco, CA",
    "New York, NY",
    "Boston, MA",
    "Cambridge, MA",
    "Seattle, WA",
    "Austin, TX",
    "Chicago, IL",
    "Los Angeles, CA",
    "Miami, FL",
    "Denver, CO",
    "Portland, OR",
    "Philadelphia, PA",
    "Washington, DC",
    "Atlanta, GA",
];

const POST_TEMPLATES: &[&str] = &[
    "Just moved into my new dorm room! {} is going to be amazing this year.",
    "Can't believe we won the game last night. Go {}!",
    "Anyone else studying for the {} midterm? This is brutal.",
    "Looking for people to join our {} intramural team. DM me!",
    "Had the best {} at that new place downtown. Highly recommend!",
    "Working on a new project with {}. Can't say much yet but stay tuned...",
    "Missing home but {} makes it worth it. Great people here.",
    "Just finished reading {}. Changed my perspective on everything.",
    "Road trip to {} this weekend! Who's in?",
    "Three exams in one week. {} life is no joke.",
    "Happy birthday to my roommate {}! Best {} ever.",
    "Throwback to that {} concert last summer. Need to see them again.",
    "Anyone want to grab {} at the dining hall? Meeting at 6pm.",
    "Finally submitted my {} paper. Time to celebrate!",
    "The weather in {} is unreal today. Perfect for frisbee on the quad.",
];

const FILL_WORDS: &[&str] = &[
    "college",
    "freshman year",
    "organic chemistry",
    "basketball",
    "pizza",
    "the team",
    "campus",
    "Malcolm Gladwell",
    "New York",
    "Harvard",
    "Alex",
    "friend",
    "Radiohead",
    "dinner",
    "thesis",
    "San Francisco",
];

#[derive(Parser)]
#[command(name = "tiered-bench")]
#[command(about = "Warm/cold benchmark for tiered S3-backed VFS")]
struct Cli {
    /// Row counts (total posts) to benchmark (comma-separated)
    #[arg(long, default_value = "10000", env = "BENCH_SIZES")]
    sizes: String,

    /// Number of measured iterations per query per mode (warm + cold)
    #[arg(long, default_value = "10", env = "BENCH_ITERATIONS")]
    iterations: usize,

    /// Number of warmup iterations before measuring (cold only)
    #[arg(long, default_value = "2", env = "BENCH_WARMUP")]
    warmup: usize,

    /// Delete S3 data after benchmarks (default: keep for reuse)
    #[arg(long, env = "BENCH_CLEANUP")]
    cleanup: bool,

    /// Force regeneration even if S3 data exists at the prefix
    #[arg(long, env = "BENCH_FORCE")]
    force: bool,

    /// Import a local SQLite DB file to S3 (skip VFS-based data gen).
    /// Use --import auto to generate locally then import, or --import <path> for existing file.
    #[arg(long, env = "BENCH_IMPORT")]
    import: Option<String>,

    /// Page size (bytes). Default 65536 (64KB).
    #[arg(long, default_value = "65536", env = "BENCH_PAGE_SIZE")]
    page_size: u32,

    /// Pages per page group. Default 256 (16MB uncompressed at 64KB pages).
    #[arg(long, default_value = "256", env = "BENCH_PPG")]
    ppg: u32,

    /// Rows per transaction commit during data generation. Default 10000.
    #[arg(long, default_value = "10000", env = "BENCH_BATCH_SIZE")]
    batch_size: usize,

    /// Number of worker threads for parallel S3 fetches. Default 8.
    #[arg(long, default_value = "8", env = "BENCH_PREFETCH_THREADS")]
    prefetch_threads: u32,

    /// Prefetch schedule for SEARCH queries (aggressive warmup).
    /// Comma-separated fractions. Default "0.3,0.3,0.4".
    /// SCAN queries use plan-aware bulk prefetch (bypasses schedule entirely).
    #[arg(long, default_value = "0.3,0.3,0.4", env = "BENCH_PREFETCH_SEARCH")]
    prefetch_search: String,

    /// Prefetch schedule for index lookups / point queries (conservative).
    /// Comma-separated fractions. Default "0,0,0" (three free hops before any prefetch).
    #[arg(long, default_value = "0,0,0", env = "BENCH_PREFETCH_LOOKUP")]
    prefetch_lookup: String,

    /// Which queries to run (comma-separated). Default: all.
    /// Options: post, profile, who-liked, mutual
    #[arg(long, env = "BENCH_QUERIES")]
    queries: Option<String>,

    /// Which modes to run (comma-separated). Default: all.
    /// Cache levels: none, interior, index, data
    ///   none     = nothing cached (full cold start from S3)
    ///   interior = interior B-tree pages cached, index + data from S3
    ///   index    = interior + index pages cached, data from S3
    ///   data     = everything cached (warm)
    #[arg(long, env = "BENCH_MODES")]
    modes: Option<String>,

    /// Skip COUNT(*) verification (avoids full table scan on tiny machines)
    #[arg(long, env = "BENCH_SKIP_VERIFY")]
    skip_verify: bool,

    /// Phase Marne: query-plan-aware prefetch. Before each query, runs
    /// EXPLAIN QUERY PLAN and pushes planned B-tree accesses to the global
    /// queue. The VFS drains the queue on first read and prefetches all
    /// planned groups immediately instead of waiting for the hop schedule.
    #[arg(long, env = "BENCH_PLAN_AWARE")]
    plan_aware: bool,

    /// Naive mode: disable ALL prefetch (no sibling prefetch, no plan-aware).
    /// Simulates dumb "fetch page from S3 on demand" like other S3-backed SQLite.
    /// Useful as a baseline to measure turbolite's prefetch advantage.
    #[arg(long, env = "BENCH_NAIVE")]
    naive: bool,

    /// Per-query SEARCH prefetch schedule for "post+user" (point lookup).
    /// Default: "off" (point lookup, 1-2 pages per tree, prefetch is wasted).
    #[arg(long, default_value = "off", env = "BENCH_POST_PREFETCH")]
    post_prefetch: String,

    /// Per-query LOOKUP prefetch schedule for "post+user".
    /// Default: "off" (point lookup needs zero lookup prefetch).
    #[arg(long, default_value = "off", env = "BENCH_POST_LOOKUP")]
    post_lookup: String,

    /// Per-query SEARCH prefetch schedule for "profile" (multi-tree join).
    /// Default: "0.1,0.2,0.3" (moderate, hits a few pages across trees).
    #[arg(long, default_value = "0.1,0.2,0.3", env = "BENCH_PROFILE_PREFETCH")]
    profile_prefetch: String,

    /// Per-query LOOKUP prefetch schedule for "profile".
    /// Default: "0,0,0,0" (multi-join, conservative: 4 free hops before any lookup prefetch).
    #[arg(long, default_value = "0,0,0,0", env = "BENCH_PROFILE_LOOKUP")]
    profile_lookup: String,

    /// Per-query SEARCH prefetch schedule for "who-liked" (SEARCH on likes index).
    /// Default: "0.3,0.3,0.4" (aggressive, scans unknown portion of index).
    #[arg(long, default_value = "0.3,0.3,0.4", env = "BENCH_WHO_LIKED_PREFETCH")]
    who_liked_prefetch: String,

    /// Per-query LOOKUP prefetch schedule for "who-liked".
    /// Default: "0,0,0" (index SEARCH, no lookup prefetch needed).
    #[arg(long, default_value = "0,0,0", env = "BENCH_WHO_LIKED_LOOKUP")]
    who_liked_lookup: String,

    /// Per-query SEARCH prefetch schedule for "mutual" (multiple SEARCH scans).
    /// Default: "0.4,0.3,0.3" (very aggressive, many index pages).
    #[arg(long, default_value = "0.4,0.3,0.3", env = "BENCH_MUTUAL_PREFETCH")]
    mutual_prefetch: String,

    /// Per-query LOOKUP prefetch schedule for "mutual".
    /// Default: "0,0,0" (multiple SEARCH, no lookup prefetch needed).
    #[arg(long, default_value = "0,0,0", env = "BENCH_MUTUAL_LOOKUP")]
    mutual_lookup: String,

    /// Per-query SEARCH prefetch schedule for "idx-filter" (index range scan).
    /// Default: "0.2,0.3,0.5" (moderate-aggressive, range of index pages).
    #[arg(long, default_value = "0.2,0.3,0.5", env = "BENCH_IDX_FILTER_PREFETCH")]
    idx_filter_prefetch: String,

    /// Per-query LOOKUP prefetch schedule for "idx-filter".
    /// Default: "0,0,0" (covered index scan, no lookup prefetch).
    #[arg(long, default_value = "0,0,0", env = "BENCH_IDX_FILTER_LOOKUP")]
    idx_filter_lookup: String,

    /// Per-query SEARCH prefetch schedule for "scan-filter" (full table scan).
    /// Default: "off" (plan-aware bulk prefetch handles this, schedule irrelevant).
    #[arg(long, default_value = "off", env = "BENCH_SCAN_FILTER_PREFETCH")]
    scan_filter_prefetch: String,

    /// Per-query LOOKUP prefetch schedule for "scan-filter".
    /// Default: "off" (plan-aware handles this).
    #[arg(long, default_value = "off", env = "BENCH_SCAN_FILTER_LOOKUP")]
    scan_filter_lookup: String,

    /// Matrix mode: test each query at "none" level with every schedule in --matrix-schedules.
    /// Outputs a comparison table per query: schedule vs latency/GETs.
    #[arg(long, env = "BENCH_MATRIX")]
    matrix: bool,

    /// Schedule pairs to test in matrix mode (semicolon-separated).
    /// Format: "search/lookup" per entry. "off" = both disabled.
    /// No slash = same schedule for both search and lookup.
    /// 10 pairs covering off -> aggressive, symmetric and asymmetric.
    #[arg(
        long,
        default_value = "off;0.33,0.33,0.34;0.3,0.3,0.4/0,0.1,0.2;0.3,0.3,0.4/0.3,0.3,0.4;0.5,0.5/0,0,0.1;0.5,0.5/0.1,0.2,0.3;0.5,0.3,0.2/0.1,0.1,0.2;1.0/0;0.2,0.3,0.5/0,0,0.2;0.4,0.3,0.3/0.1,0.2,0.3",
        env = "BENCH_MATRIX_SCHEDULES"
    )]
    matrix_schedules: String,
}

// =========================================================================
// Helpers
// =========================================================================

fn unique_vfs_name(prefix: &str) -> String {
    let n = VFS_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("bench_{}_{}", prefix, n)
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

/// Deterministic pseudo-random hash
fn phash(seed: u64) -> u64 {
    let mut x = seed;
    x = x
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x
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

struct BenchResult {
    label: String,
    latencies_us: Vec<f64>,
    s3_fetches: Vec<u64>,
    s3_bytes: Vec<u64>,
}

impl BenchResult {
    fn p50(&self) -> f64 {
        percentile(&self.latencies_us, 0.5)
    }
    fn p90(&self) -> f64 {
        percentile(&self.latencies_us, 0.9)
    }
    fn p99(&self) -> f64 {
        percentile(&self.latencies_us, 0.99)
    }
    fn avg_fetches(&self) -> f64 {
        if self.s3_fetches.is_empty() {
            return 0.0;
        }
        self.s3_fetches.iter().sum::<u64>() as f64 / self.s3_fetches.len() as f64
    }
    fn avg_bytes_kb(&self) -> f64 {
        if self.s3_bytes.is_empty() {
            return 0.0;
        }
        (self.s3_bytes.iter().sum::<u64>() as f64 / self.s3_bytes.len() as f64) / 1024.0
    }
}

fn format_number(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(ch);
    }
    result.chars().rev().collect()
}

/// Parse a per-query prefetch schedule. "off" = None (disabled), otherwise comma-separated floats.
fn parse_query_prefetch(s: &str) -> Option<Vec<f32>> {
    match s.trim().to_lowercase().as_str() {
        "off" | "none" | "disabled" | "" => None,
        _ => Some(parse_prefetch_hops(s)),
    }
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

fn parse_prefetch_hops(s: &str) -> Vec<f32> {
    s.split(',')
        .filter_map(|v| v.trim().parse::<f32>().ok())
        .collect()
}

fn make_config(
    _prefix: &str,
    cache_dir: &std::path::Path,
    ppg: u32,
    prefetch_threads: u32,
    prefetch_search: Vec<f32>,
    prefetch_lookup: Vec<f32>,
) -> TurboliteConfig {
    TurboliteConfig {
        cache_dir: cache_dir.to_path_buf(),
        compression: CompressionConfig {
            level: 1,
            ..Default::default()
        },
        cache: CacheConfig {
            pages_per_group: ppg,
            ..Default::default()
        },
        prefetch: PrefetchConfig {
            threads: prefetch_threads,
            search: prefetch_search,
            lookup: prefetch_lookup,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn make_reader_config(
    _prefix: &str,
    cache_dir: &std::path::Path,
    ppg: u32,
    prefetch_threads: u32,
    prefetch_search: Vec<f32>,
    prefetch_lookup: Vec<f32>,
) -> TurboliteConfig {
    TurboliteConfig {
        cache_dir: cache_dir.to_path_buf(),
        compression: CompressionConfig {
            level: 1,
            ..Default::default()
        },
        read_only: true,
        cache: CacheConfig {
            pages_per_group: ppg,
            ..Default::default()
        },
        prefetch: PrefetchConfig {
            threads: prefetch_threads,
            search: prefetch_search,
            lookup: prefetch_lookup,
            ..Default::default()
        },
        ..Default::default()
    }
}

// =========================================================================
// Schema
// =========================================================================

const SCHEMA: &str = "
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT NOT NULL,
    school TEXT NOT NULL,
    city TEXT NOT NULL,
    bio TEXT NOT NULL,
    joined_at INTEGER NOT NULL
);

CREATE TABLE posts (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    content TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    like_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE friendships (
    user_a INTEGER NOT NULL,
    user_b INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (user_a, user_b)
);

CREATE TABLE likes (
    user_id INTEGER NOT NULL,
    post_id INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (user_id, post_id)
);

CREATE INDEX idx_posts_user ON posts(user_id);
CREATE INDEX idx_posts_created ON posts(created_at);
CREATE INDEX idx_friendships_b ON friendships(user_b, user_a);
CREATE INDEX idx_likes_post ON likes(post_id);
CREATE INDEX idx_likes_user ON likes(user_id, created_at);
CREATE INDEX idx_users_school ON users(school);
";

// =========================================================================
// Data generation
// =========================================================================

fn generate_post_content(id: i64) -> String {
    let h = phash(id as u64 + 9_000_000);
    let template = POST_TEMPLATES[(h as usize) % POST_TEMPLATES.len()];
    let fill1 = FILL_WORDS[((h >> 16) as usize) % FILL_WORDS.len()];
    let fill2 = FILL_WORDS[((h >> 24) as usize) % FILL_WORDS.len()];
    let mut content = template.replacen("{}", fill1, 1);
    content = content.replacen("{}", fill2, 1);
    let target_len = 200 + ((h >> 32) % 1800) as usize;
    if content.len() < target_len {
        let padding = [
            " Can't wait to see what happens next.",
            " This semester is flying by.",
            " Anyone else feel the same way?",
            " Comment below if you're interested!",
            " Life is good right now.",
            " Really grateful for this experience.",
            " Shoutout to everyone who made this happen.",
            " More updates coming soon!",
        ];
        while content.len() < target_len {
            let pidx = phash(content.len() as u64 + id as u64) as usize % padding.len();
            content.push_str(padding[pidx]);
        }
    }
    content
}

fn generate_bio(id: i64) -> String {
    let h = phash(id as u64 + 7_000_000);
    let interests = [
        "music",
        "startups",
        "hiking",
        "photography",
        "cooking",
        "travel",
        "reading",
        "sports",
        "gaming",
        "art",
    ];
    let i1 = interests[((h >> 8) as usize) % interests.len()];
    let i2 = interests[((h >> 16) as usize) % interests.len()];
    let i3 = interests[((h >> 24) as usize) % interests.len()];
    let year = 2004 + (h % 4);
    format!(
        "Class of {}. Into {}, {}, and {}. Looking to connect!",
        year, i1, i2, i3
    )
}

fn generate_data(conn: &Connection, n_posts: usize, batch_size: usize) {
    let n_users = (n_posts / 10).max(100);
    let friends_per_user = 50usize;
    let n_friendships = n_users * friends_per_user / 2;
    let n_likes = n_posts * 3;

    eprintln!(
        "  Generating: {} users, {} posts, {} friendships, {} likes",
        format_number(n_users),
        format_number(n_posts),
        format_number(n_friendships),
        format_number(n_likes),
    );

    // Users (batch commit every batch_size rows)
    {
        let mut batch = 0usize;
        let mut tx = conn.unchecked_transaction().unwrap();
        for i in 0..n_users as i64 {
            let h = phash(i as u64);
            tx.execute(
                "INSERT INTO users VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
                rusqlite::params![
                    i,
                    FIRST_NAMES[(h as usize) % FIRST_NAMES.len()],
                    LAST_NAMES[((h >> 16) as usize) % LAST_NAMES.len()],
                    format!(
                        "{}.{}{}@{}.edu",
                        FIRST_NAMES[(h as usize) % FIRST_NAMES.len()].to_lowercase(),
                        LAST_NAMES[((h >> 16) as usize) % LAST_NAMES.len()].to_lowercase(),
                        i,
                        SCHOOLS[((h >> 24) as usize) % SCHOOLS.len()]
                            .to_lowercase()
                            .replace(' ', "")
                    ),
                    SCHOOLS[((h >> 24) as usize) % SCHOOLS.len()],
                    CITIES[((h >> 32) as usize) % CITIES.len()],
                    generate_bio(i),
                    1075000000i64 + (h % 100_000_000) as i64,
                ],
            )
            .unwrap();
            batch += 1;
            if batch >= batch_size {
                tx.commit().unwrap();
                tx = conn.unchecked_transaction().unwrap();
                batch = 0;
            }
        }
        tx.commit().unwrap();
        eprintln!("    users inserted: {}", format_number(n_users));
    }

    // Posts
    {
        let mut batch = 0usize;
        let mut tx = conn.unchecked_transaction().unwrap();
        for i in 0..n_posts as i64 {
            let h = phash(i as u64 + 1_000_000);
            tx.execute(
                "INSERT INTO posts (id, user_id, content, created_at, like_count) VALUES (?1,?2,?3,?4,?5)",
                rusqlite::params![
                    i,
                    (h % n_users as u64) as i64,
                    generate_post_content(i),
                    1075000000i64 + (h >> 16) as i64 % 94_000_000,
                    (phash(i as u64 + 2_000_000) % 200) as i64,
                ],
            ).unwrap();
            batch += 1;
            if batch >= batch_size {
                tx.commit().unwrap();
                tx = conn.unchecked_transaction().unwrap();
                batch = 0;
                if (i as usize) % (batch_size * 10) == 0 {
                    eprintln!(
                        "    posts: {}/{}",
                        format_number(i as usize),
                        format_number(n_posts)
                    );
                }
            }
        }
        tx.commit().unwrap();
        eprintln!("    posts inserted: {}", format_number(n_posts));
    }

    // Friendships
    {
        let mut batch = 0usize;
        let mut count = 0usize;
        let mut tx = conn.unchecked_transaction().unwrap();
        for i in 0..n_users as u64 {
            let n_friends = friends_per_user.min(n_users - 1);
            for j in 0..n_friends {
                let h = phash(i * 100 + j as u64 + 3_000_000);
                let friend = (h % n_users as u64) as i64;
                if friend != i as i64 {
                    let (a, b) = if (i as i64) < friend {
                        (i as i64, friend)
                    } else {
                        (friend, i as i64)
                    };
                    tx.execute(
                        "INSERT OR IGNORE INTO friendships VALUES (?1,?2,?3)",
                        rusqlite::params![a, b, 1075000000i64 + (h >> 16) as i64 % 94_000_000],
                    )
                    .unwrap();
                    count += 1;
                    batch += 1;
                    if batch >= batch_size {
                        tx.commit().unwrap();
                        tx = conn.unchecked_transaction().unwrap();
                        batch = 0;
                    }
                }
                if count >= n_friendships {
                    break;
                }
            }
            if count >= n_friendships {
                break;
            }
        }
        tx.commit().unwrap();
        eprintln!("    friendships inserted: {}", format_number(count));
    }

    // Likes
    {
        let mut batch = 0usize;
        let mut tx = conn.unchecked_transaction().unwrap();
        for i in 0..n_likes as u64 {
            let h = phash(i + 4_000_000);
            tx.execute(
                "INSERT OR IGNORE INTO likes VALUES (?1,?2,?3)",
                rusqlite::params![
                    (h % n_users as u64) as i64,
                    ((h >> 16) % n_posts as u64) as i64,
                    1075000000i64 + (h >> 32) as i64 % 94_000_000,
                ],
            )
            .unwrap();
            batch += 1;
            if batch >= batch_size {
                tx.commit().unwrap();
                tx = conn.unchecked_transaction().unwrap();
                batch = 0;
                if (i as usize) % (batch_size * 10) == 0 && i > 0 {
                    eprintln!(
                        "    likes: {}/{}",
                        format_number(i as usize),
                        format_number(n_likes)
                    );
                }
            }
        }
        tx.commit().unwrap();
        eprintln!("    likes inserted: {}", format_number(n_likes));
    }
}

/// Generate a plain SQLite DB file locally (no VFS, max speed).
/// Uses journal_mode=OFF and synchronous=OFF for fastest possible writes.
fn generate_local_db(path: &std::path::Path, n_posts: usize, batch_size: usize, page_size: u32) {
    let conn = Connection::open(path).expect("open local DB");
    conn.execute_batch(&format!(
        "PRAGMA page_size={}; PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA cache_size=-262144;",
        page_size,
    )).expect("pragma setup");
    conn.execute_batch(SCHEMA).expect("create tables");
    generate_data(&conn, n_posts, batch_size);
    // Verify
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM posts", [], |r| r.get(0))
        .unwrap();
    let page_count: i64 = conn
        .query_row("PRAGMA page_count", [], |r| r.get(0))
        .unwrap();
    let ps: i64 = conn
        .query_row("PRAGMA page_size", [], |r| r.get(0))
        .unwrap();
    eprintln!(
        "[local-gen] {} posts, {} pages x {} bytes = {:.1} MB",
        count,
        page_count,
        ps,
        (page_count * ps) as f64 / (1024.0 * 1024.0),
    );
}

// =========================================================================
// Benchmark queries
// =========================================================================

const Q_POST_DETAIL: &str = "\
SELECT posts.id, posts.content, posts.created_at, posts.like_count,
       users.first_name, users.last_name, users.school, users.city
FROM posts
JOIN users ON users.id = posts.user_id
WHERE posts.id = ?1";

const Q_PROFILE: &str = "\
SELECT users.first_name, users.last_name, users.school, users.city, users.bio,
       posts.id, posts.content, posts.created_at, posts.like_count
FROM users
JOIN posts ON posts.user_id = users.id
WHERE users.id = ?1
ORDER BY posts.created_at DESC
LIMIT 10";

const Q_WHO_LIKED: &str = "\
SELECT users.first_name, users.last_name, users.school, likes.created_at
FROM likes
JOIN users ON users.id = likes.user_id
WHERE likes.post_id = ?1
ORDER BY likes.created_at DESC
LIMIT 50";

const Q_MUTUAL: &str = "\
SELECT users.id, users.first_name, users.last_name, users.school
FROM friendships
JOIN friendships AS friendships_b ON friendships.user_b = friendships_b.user_b
JOIN users ON users.id = friendships.user_b
WHERE friendships.user_a = ?1 AND friendships_b.user_a = ?2
LIMIT 20";

/// Indexed filter: uses idx_posts_user. On cold, only the matching index leaf
/// + data page need fetching. Shows index benefit for selective queries.
const Q_IDX_FILTER: &str = "\
SELECT COUNT(*) FROM posts WHERE user_id = ?1";

/// Full scan filter: no index on like_count. Must scan all data pages.
/// On cold, this forces multiple group fetches. Shows cost of no index.
const Q_SCAN_FILTER: &str = "\
SELECT COUNT(*) FROM posts WHERE like_count > ?1";

// =========================================================================
// Benchmark runners
// =========================================================================

/// Phase Marne: run EXPLAIN QUERY PLAN via rusqlite and push planned
/// accesses to the global queue. The VFS drains the queue on first read.
fn push_query_plan(conn: &Connection, sql: &str, params: &[rusqlite::types::Value]) {
    let eqp_sql = format!("EXPLAIN QUERY PLAN {}", sql);
    let mut stmt = match conn.prepare(&eqp_sql) {
        Ok(s) => s,
        Err(e) => {
            if std::env::var("BENCH_VERBOSE").is_ok() {
                eprintln!("  [push-plan] prepare FAILED: {}", e);
            }
            return;
        }
    };
    let mut output = String::new();
    let mut rows = match stmt.query(rusqlite::params_from_iter(params)) {
        Ok(r) => r,
        Err(e) => {
            if std::env::var("BENCH_VERBOSE").is_ok() {
                eprintln!("  [push-plan] query FAILED: {}", e);
            }
            return;
        }
    };
    while let Ok(Some(row)) = rows.next() {
        if let Ok(detail) = row.get::<_, String>(3) {
            output.push_str(&detail);
            output.push('\n');
        }
    }
    if std::env::var("BENCH_VERBOSE").is_ok() {
        eprintln!(
            "  [push-plan] sql={} eqp_output={:?}",
            &sql[..sql.len().min(60)],
            &output
        );
    }
    let accesses = parse_eqp_output(&output);
    if std::env::var("BENCH_VERBOSE").is_ok() {
        let names: Vec<&str> = accesses.iter().map(|a| a.tree_name.as_str()).collect();
        eprintln!("  [push-plan] parsed={:?}", names);
    }
    push_planned_accesses(accesses);
}

/// A pair of search/lookup schedules to push before a query.
/// When `unified` is Some, it sets both via "prefetch".
/// When `search`/`lookup` are set, they push independently.
#[derive(Clone, Debug)]
struct SchedulePair {
    search: Option<Vec<f32>>,
    lookup: Option<Vec<f32>>,
}

impl SchedulePair {
    /// Unified schedule: sets both search and lookup to the same values.
    fn unified(sched: Option<Vec<f32>>) -> Self {
        Self {
            search: sched.clone(),
            lookup: sched,
        }
    }

    /// Independent search/lookup pair.
    fn pair(search: Option<Vec<f32>>, lookup: Option<Vec<f32>>) -> Self {
        Self { search, lookup }
    }

    /// Push this pair's prefetch schedules onto the current connection's
    /// turbolite handle via the per-handle settings queue restored in
    /// Phase Cirrus c. The next slow-path read on the handle drains and
    /// applies the update, so each pair in matrix mode is actually
    /// measured with its own schedule.
    fn push(&self) {
        let search_val = match &self.search {
            Some(v) => v
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<_>>()
                .join(","),
            None => "0,0,0".to_string(),
        };
        let lookup_val = match &self.lookup {
            Some(v) => v
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<_>>()
                .join(","),
            None => "0,0,0".to_string(),
        };
        // `set` returns Err only if no active handle or the value fails
        // validation; both are programmer errors in this bench, so panic.
        turbolite::tiered::settings::set("prefetch_search", &search_val)
            .expect("settings::set prefetch_search");
        turbolite::tiered::settings::set("prefetch_lookup", &lookup_val)
            .expect("settings::set prefetch_lookup");
    }

    fn label(&self) -> String {
        let fmt = |s: &Option<Vec<f32>>| -> String {
            match s {
                Some(v) => v
                    .iter()
                    .map(|f| format!("{:.2}", f))
                    .collect::<Vec<_>>()
                    .join(","),
                None => "off".to_string(),
            }
        };
        format!("{} / {}", fmt(&self.search), fmt(&self.lookup))
    }
}

/// Push independent search/lookup schedules, then execute the query.
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

/// WARM benchmark: cache populated, reuse connection.
/// First call primes the cache, subsequent calls measure cache-hit latency.
/// Cache level: data — everything cached, reuse connection. Measures pread latency.
fn bench_data(
    conn: &Connection,
    handle: &BenchCtx,
    label: &str,
    sql: &str,
    param_fn: &dyn Fn(usize) -> Vec<rusqlite::types::Value>,
    iterations: usize,
    plan_aware: bool,
    schedule: &SchedulePair,
) -> BenchResult {
    // Prime: run query once to populate cache, then reuse same params
    // to measure true cache-hit latency (no S3 fetches)
    let params = param_fn(0);
    let _ = run_query_pair(conn, sql, &params, false, schedule);
    handle.reset_s3_counters();

    let mut latencies = Vec::with_capacity(iterations);
    let mut s3_fetches = Vec::with_capacity(iterations);
    let mut s3_bytes = Vec::with_capacity(iterations);

    for i in 0..iterations {
        handle.reset_s3_counters();
        let start = Instant::now();
        match run_query_pair(conn, sql, &params, plan_aware, schedule) {
            Ok(_) => {
                latencies.push(start.elapsed().as_micros() as f64);
                let (fc, fb) = handle.s3_counters();
                s3_fetches.push(fc);
                s3_bytes.push(fb);
            }
            Err(e) => eprintln!("    [data] {} iter {} error: {}", label, i, e),
        }
    }

    BenchResult {
        label: format!("[data] {}", label),
        latencies_us: latencies,
        s3_fetches,
        s3_bytes,
    }
}

/// Cache level: index — interior + index pages cached, data pages from S3.
fn bench_index(
    vfs_name: &str,
    db_name: &str,
    handle: &BenchCtx,
    label: &str,
    sql: &str,
    param_fn: &dyn Fn(usize) -> Vec<rusqlite::types::Value>,
    warmup: usize,
    iterations: usize,
    plan_aware: bool,
    schedule: &SchedulePair,
) -> BenchResult {
    for i in 0..warmup {
        handle.clear_cache_data_only();
        handle.reset_s3_counters();
        let params = param_fn(i);
        let conn = Connection::open_with_flags_and_vfs(
            db_name,
            OpenFlags::SQLITE_OPEN_READ_ONLY,
            vfs_name,
        )
        .expect("index-level connection");
        if let Err(e) = run_query_pair(&conn, sql, &params, plan_aware, schedule) {
            eprintln!("    [index] {} warmup {} error: {}", label, i, e);
        }
        drop(conn);
    }

    let mut latencies = Vec::with_capacity(iterations);
    let mut s3_fetches = Vec::with_capacity(iterations);
    let mut s3_bytes = Vec::with_capacity(iterations);

    for i in 0..iterations {
        handle.clear_cache_data_only();
        handle.reset_s3_counters();

        let params = param_fn(warmup + i);
        let start = Instant::now();
        let conn = Connection::open_with_flags_and_vfs(
            db_name,
            OpenFlags::SQLITE_OPEN_READ_ONLY,
            vfs_name,
        )
        .expect("index-level connection");
        match run_query_pair(&conn, sql, &params, plan_aware, schedule) {
            Ok(_) => {
                latencies.push(start.elapsed().as_micros() as f64);
                let (fc, fb) = handle.s3_counters();
                s3_fetches.push(fc);
                s3_bytes.push(fb);
            }
            Err(e) => eprintln!("    [index] {} iter {} error: {}", label, i, e),
        }
        drop(conn);
    }

    BenchResult {
        label: format!("[index] {}", label),
        latencies_us: latencies,
        s3_fetches,
        s3_bytes,
    }
}

/// Cache level: interior — interior B-tree pages cached, index + data from S3.
/// This is the realistic "first query after connection open" state.
fn bench_interior(
    vfs_name: &str,
    db_name: &str,
    handle: &BenchCtx,
    label: &str,
    sql: &str,
    param_fn: &dyn Fn(usize) -> Vec<rusqlite::types::Value>,
    warmup: usize,
    iterations: usize,
    plan_aware: bool,
    schedule: &SchedulePair,
) -> BenchResult {
    for i in 0..warmup {
        handle.clear_cache_interior_only();
        handle.reset_s3_counters();
        let params = param_fn(i);
        let conn = Connection::open_with_flags_and_vfs(
            db_name,
            OpenFlags::SQLITE_OPEN_READ_ONLY,
            vfs_name,
        )
        .expect("interior-level connection");
        if let Err(e) = run_query_pair(&conn, sql, &params, plan_aware, schedule) {
            eprintln!("    [interior] {} warmup {} error: {}", label, i, e);
        }
        drop(conn);
    }

    let mut latencies = Vec::with_capacity(iterations);
    let mut s3_fetches = Vec::with_capacity(iterations);
    let mut s3_bytes = Vec::with_capacity(iterations);

    for i in 0..iterations {
        handle.clear_cache_interior_only();
        handle.reset_s3_counters();

        let params = param_fn(warmup + i);
        let start = Instant::now();
        let conn = Connection::open_with_flags_and_vfs(
            db_name,
            OpenFlags::SQLITE_OPEN_READ_ONLY,
            vfs_name,
        )
        .expect("interior-level connection");
        match run_query_pair(&conn, sql, &params, plan_aware, schedule) {
            Ok(_) => {
                latencies.push(start.elapsed().as_micros() as f64);
                let (fc, fb) = handle.s3_counters();
                s3_fetches.push(fc);
                s3_bytes.push(fb);
            }
            Err(e) => eprintln!("    [interior] {} iter {} error: {}", label, i, e),
        }
        drop(conn);
    }

    BenchResult {
        label: format!("[interior] {}", label),
        latencies_us: latencies,
        s3_fetches,
        s3_bytes,
    }
}

/// Cache level: none — everything evicted, including interior pages.
/// Interior chunks must be re-fetched from S3 on each connection open.
fn bench_none(
    vfs_name: &str,
    db_name: &str,
    handle: &BenchCtx,
    label: &str,
    sql: &str,
    param_fn: &dyn Fn(usize) -> Vec<rusqlite::types::Value>,
    warmup: usize,
    iterations: usize,
    plan_aware: bool,
    schedule: &SchedulePair,
) -> BenchResult {
    for i in 0..warmup {
        handle.clear_cache_all();
        handle.reset_s3_counters();
        let params = param_fn(i);
        let conn = Connection::open_with_flags_and_vfs(
            db_name,
            OpenFlags::SQLITE_OPEN_READ_ONLY,
            vfs_name,
        )
        .expect("none-level connection");
        if let Err(e) = run_query_pair(&conn, sql, &params, plan_aware, schedule) {
            eprintln!("    [none] {} warmup {} error: {}", label, i, e);
        }
        drop(conn);
    }

    let mut latencies = Vec::with_capacity(iterations);
    let mut s3_fetches = Vec::with_capacity(iterations);
    let mut s3_bytes = Vec::with_capacity(iterations);

    for i in 0..iterations {
        handle.clear_cache_all();
        handle.reset_s3_counters();

        let params = param_fn(warmup + i);
        let start = Instant::now();
        let conn = Connection::open_with_flags_and_vfs(
            db_name,
            OpenFlags::SQLITE_OPEN_READ_ONLY,
            vfs_name,
        )
        .expect("none-level connection");
        match run_query_pair(&conn, sql, &params, plan_aware, schedule) {
            Ok(_) => {
                latencies.push(start.elapsed().as_micros() as f64);
                let (fc, fb) = handle.s3_counters();
                s3_fetches.push(fc);
                s3_bytes.push(fb);
            }
            Err(e) => eprintln!("    [none] {} iter {} error: {}", label, i, e),
        }
        drop(conn);
    }

    BenchResult {
        label: format!("[none] {}", label),
        latencies_us: latencies,
        s3_fetches,
        s3_bytes,
    }
}

/// Like bench_none but uses a SchedulePair for independent search/lookup schedules.
fn bench_none_pair(
    vfs_name: &str,
    db_name: &str,
    handle: &BenchCtx,
    label: &str,
    sql: &str,
    param_fn: &dyn Fn(usize) -> Vec<rusqlite::types::Value>,
    warmup: usize,
    iterations: usize,
    plan_aware: bool,
    pair: &SchedulePair,
) -> BenchResult {
    for i in 0..warmup {
        handle.clear_cache_all();
        handle.reset_s3_counters();
        let params = param_fn(i);
        let conn = Connection::open_with_flags_and_vfs(
            db_name,
            OpenFlags::SQLITE_OPEN_READ_ONLY,
            vfs_name,
        )
        .expect("none-level connection");
        if let Err(e) = run_query_pair(&conn, sql, &params, plan_aware, pair) {
            eprintln!("    [matrix] {} warmup {} error: {}", label, i, e);
        }
        drop(conn);
    }

    let mut latencies = Vec::with_capacity(iterations);
    let mut s3_fetches = Vec::with_capacity(iterations);
    let mut s3_bytes = Vec::with_capacity(iterations);

    for i in 0..iterations {
        handle.clear_cache_all();
        handle.reset_s3_counters();

        let params = param_fn(warmup + i);
        let start = Instant::now();
        let conn = Connection::open_with_flags_and_vfs(
            db_name,
            OpenFlags::SQLITE_OPEN_READ_ONLY,
            vfs_name,
        )
        .expect("none-level connection");
        match run_query_pair(&conn, sql, &params, plan_aware, pair) {
            Ok(_) => {
                latencies.push(start.elapsed().as_micros() as f64);
                let (fc, fb) = handle.s3_counters();
                s3_fetches.push(fc);
                s3_bytes.push(fb);
            }
            Err(e) => eprintln!("    [matrix] {} iter {} error: {}", label, i, e),
        }
        drop(conn);
    }

    BenchResult {
        label: format!("[matrix] {}", label),
        latencies_us: latencies,
        s3_fetches,
        s3_bytes,
    }
}

// =========================================================================
// Output
// =========================================================================

fn print_header() {
    println!(
        "  {:<24} {:>10} {:>10} {:>10} {:>10} {:>12}",
        "", "p50", "p90", "p99", "s3 GETs", "s3 bytes"
    );
    println!(
        "  {:-<24} {:->10} {:->10} {:->10} {:->10} {:->12}",
        "", "", "", "", "", ""
    );
}

fn print_result(r: &BenchResult) {
    println!(
        "  {:<24} {:>10} {:>10} {:>10} {:>10} {:>12}",
        r.label,
        format_ms(r.p50()),
        format_ms(r.p90()),
        format_ms(r.p99()),
        format!("{:.1}", r.avg_fetches()),
        format_kb(r.avg_bytes_kb()),
    );
}

// =========================================================================
// Main benchmark runner
// =========================================================================

fn run_benchmark(n_posts: usize, cli: &Cli) {
    let n_users = (n_posts / 10).max(100);
    let est_bytes = n_posts as f64 * 700.0
        + n_users as f64 * 200.0
        + (n_users as f64 * 25.0) * 24.0
        + (n_posts as f64 * 3.0) * 24.0;
    let db_size_mb = est_bytes / (1024.0 * 1024.0);
    let ppg = cli.ppg as u64;
    let page_size = cli.page_size as u64;
    let est_pages = (est_bytes as u64) / page_size + 1;
    let est_page_groups = est_pages / ppg + 1;
    let db_name = format!("social_{}.db", n_posts);

    println!();
    let prefetch_search = parse_prefetch_hops(&cli.prefetch_search);
    let prefetch_lookup = parse_prefetch_hops(&cli.prefetch_lookup);
    println!(
        "--- {} posts, {} users (~{:.1} MB, ~{} pages, ~{} page groups) ---",
        format_number(n_posts),
        format_number(n_users),
        db_size_mb,
        est_pages,
        est_page_groups,
    );
    println!("    prefetch_search: {:?}", prefetch_search);
    println!("    prefetch_lookup: {:?}", prefetch_lookup);

    let cache_dir = TempDir::new().expect("failed to create temp dir");

    eprintln!("[bench] run_benchmark({}) starting", n_posts);
    // Data source priority:
    // 1. --import: generate locally + upload to S3 (fast path)
    // 2. --force: generate through VFS (legacy path)
    // 3. Default: reuse existing S3 data at social_{n_posts}
    let s3_prefix = format!("social_{}_btree", n_posts);

    // Each benchmark run owns its own multi-threaded tokio runtime; every
    // S3Storage built below shares this handle so block_on calls from sync
    // code work.
    let bench_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("build bench runtime");
    let rt_handle = bench_runtime.handle().clone();

    if cli.import.is_some() {
        // Fast path: generate plain SQLite DB locally, then import to S3.
        // When --import auto and data already exists on S3, skip generation and import.
        let check_backend = build_s3_backend(&rt_handle, &s3_prefix);
        let existing_manifest = turbolite::tiered::get_manifest(check_backend.as_ref(), &rt_handle)
            .expect("failed to check S3 manifest");

        let is_auto = cli.import.as_deref() == Some("auto");
        if is_auto && existing_manifest.is_some() {
            let m = existing_manifest.unwrap();
            eprintln!(
                "[bench] S3 data already exists at prefix '{}' ({} pages, {} groups), skipping generation",
                s3_prefix, m.page_count, m.page_group_keys.len(),
            );
        } else {
            let _gen_tmp = TempDir::new().expect("temp dir for local gen");
            let local_path = if let Some(ref path) = cli.import {
                if path == "auto" {
                    let local_db = _gen_tmp.path().join(&db_name);
                    eprintln!("[bench] generating local DB: {}", local_db.display());
                    let gen_start = Instant::now();
                    generate_local_db(&local_db, n_posts, cli.batch_size, cli.page_size);
                    println!("  Local gen:   {:.2}s", gen_start.elapsed().as_secs_f64());
                    local_db.to_string_lossy().to_string()
                } else {
                    path.clone()
                }
            } else {
                unreachable!()
            };

            let import_start = Instant::now();
            let config = turbolite::tiered::TurboliteConfig {
                cache: CacheConfig {
                    pages_per_group: cli.ppg,
                    ..Default::default()
                },
                compression: CompressionConfig {
                    level: 1,
                    ..Default::default()
                },
                ..Default::default()
            };
            let import_backend = build_s3_backend(&rt_handle, &s3_prefix);
            let manifest = turbolite::tiered::import_sqlite_file(
                &config,
                import_backend as Arc<dyn hadb_storage::StorageBackend>,
                rt_handle.clone(),
                std::path::Path::new(&local_path),
            )
            .expect("import failed");
            println!(
                "  S3 import:   {:.2}s ({} pages, {} groups, {} interior chunks)",
                import_start.elapsed().as_secs_f64(),
                manifest.page_count,
                manifest.page_group_keys.len(),
                manifest.interior_chunk_keys.len(),
            );
        }
    } else if cli.force {
        // Legacy VFS generation path
        let config = make_config(
            &s3_prefix,
            cache_dir.path(),
            cli.ppg,
            cli.prefetch_threads,
            prefetch_search.clone(),
            prefetch_lookup.clone(),
        );
        let vfs_name = unique_vfs_name("write");
        let writer_s3 = build_s3_backend(&rt_handle, &s3_prefix);
        let vfs = TurboliteVfs::with_backend(
            config,
            writer_s3 as Arc<dyn hadb_storage::StorageBackend>,
            rt_handle.clone(),
        )
        .expect("failed to create TurboliteVfs");
        turbolite::tiered::register(&vfs_name, vfs).unwrap();

        let conn = Connection::open_with_flags_and_vfs(
            &db_name,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .expect("failed to open connection");
        conn.execute_batch(&format!(
            "PRAGMA page_size={}; PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;",
            cli.page_size
        ))
        .expect("pragma setup failed");
        conn.execute_batch(
            "
            DROP TABLE IF EXISTS likes;
            DROP TABLE IF EXISTS friendships;
            DROP TABLE IF EXISTS posts;
            DROP TABLE IF EXISTS users;
        ",
        )
        .expect("drop tables failed");
        conn.execute_batch(SCHEMA).expect("create tables failed");

        set_local_checkpoint_only(true);
        conn.execute_batch("PRAGMA wal_autocheckpoint=100000;")
            .expect("autocheckpoint failed");

        let gen_start = Instant::now();
        generate_data(&conn, n_posts, cli.batch_size);
        println!("  Data gen:    {:.2}s", gen_start.elapsed().as_secs_f64());

        set_local_checkpoint_only(false);
        conn.execute_batch("PRAGMA wal_autocheckpoint=0;")
            .expect("autocheckpoint reset failed");
        conn.execute("PRAGMA user_version = 1", [])
            .expect("force dirty page failed");
        let cp_start = Instant::now();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .expect("checkpoint failed");
        println!("  Checkpoint:  {:.2}s", cp_start.elapsed().as_secs_f64());

        let page_count: i64 = conn
            .query_row("PRAGMA page_count", [], |r| r.get(0))
            .unwrap_or(0);
        let page_sz: i64 = conn
            .query_row("PRAGMA page_size", [], |r| r.get(0))
            .unwrap_or(0);
        let actual_mb = (page_count * page_sz) as f64 / (1024.0 * 1024.0);
        let actual_groups = (page_count as u64 + ppg - 1) / ppg;
        println!(
            "  Actual DB:   {} pages x {} bytes = {:.1} MB ({} page groups)",
            page_count, page_sz, actual_mb, actual_groups
        );
        drop(conn);
    } else {
        // Default: reuse existing S3 data
        println!("  Reusing S3 prefix: {}", s3_prefix);
    }

    // Create reader VFS + bench handle
    eprintln!("[bench] creating reader VFS...");
    let reader_cache = TempDir::new().expect("reader temp dir");
    let reader_config = make_reader_config(
        &s3_prefix,
        reader_cache.path(),
        cli.ppg,
        cli.prefetch_threads,
        prefetch_search.clone(),
        prefetch_lookup.clone(),
    );
    // Phase Cirrus d removed eager interior/index prefetch entirely; those
    // pages now come in on first-query miss. BENCH_NO_EAGER_INDEX is a no-op.
    eprintln!("[bench] calling TurboliteVfs::with_backend()...");
    let reader_s3 = build_s3_backend(&rt_handle, &s3_prefix);
    let reader_vfs = TurboliteVfs::with_backend(
        reader_config,
        reader_s3.clone() as Arc<dyn hadb_storage::StorageBackend>,
        rt_handle.clone(),
    )
    .expect("reader VFS");
    eprintln!("[bench] TurboliteVfs::with_backend() returned OK");
    let shared_state = reader_vfs.shared_state();
    let ctx = BenchCtx::new(&shared_state, reader_s3.clone());
    let reader_vfs_name = unique_vfs_name("reader");
    eprintln!("[bench] registering VFS '{}'", reader_vfs_name);
    turbolite::tiered::register(&reader_vfs_name, reader_vfs).unwrap();
    eprintln!("[bench] VFS registered, opening connection...");

    // Open persistent connection for warm benchmarks
    let warm_conn = Connection::open_with_flags_and_vfs(
        &db_name,
        OpenFlags::SQLITE_OPEN_READ_ONLY,
        &reader_vfs_name,
    )
    .expect("warm connection");
    if cli.skip_verify {
        eprintln!("[bench] connection opened, skipping COUNT(*) verification");
    } else {
        // Run integrity_check first to get detailed corruption info
        eprintln!("[bench] connection opened, running integrity_check...");
        let mut stmt = warm_conn
            .prepare("PRAGMA integrity_check(100)")
            .expect("prepare integrity_check");
        let results: Vec<String> = stmt
            .query_map([], |row| row.get(0))
            .expect("integrity_check query")
            .filter_map(|r| r.ok())
            .collect();
        if results.len() == 1 && results[0] == "ok" {
            eprintln!("[bench] integrity_check: ok");
        } else {
            for msg in &results {
                eprintln!("[bench] INTEGRITY ERROR: {}", msg);
            }
            panic!("[bench] integrity_check found {} errors", results.len());
        }

        eprintln!("[bench] running COUNT(*)...");
        let row_count: i64 = warm_conn
            .query_row("SELECT COUNT(*) FROM posts", [], |r| r.get(0))
            .expect("count query failed");
        eprintln!("[bench] COUNT(*) returned {}", row_count);
        println!(
            "  Verified: {} posts accessible",
            format_number(row_count as usize)
        );
    }

    // =====================================================================
    // Run benchmarks
    // =====================================================================

    struct QueryDef {
        label: &'static str,
        sql: &'static str,
        param_fn: Box<dyn Fn(usize) -> Vec<rusqlite::types::Value>>,
        /// Per-query search/lookup schedule pair. Pushed independently to VFS.
        schedule: SchedulePair,
    }

    let query_filter: Option<Vec<String>> = cli
        .queries
        .as_ref()
        .map(|q| q.split(',').map(|s| s.trim().to_lowercase()).collect());
    let mode_filter: Option<Vec<String>> = cli
        .modes
        .as_ref()
        .map(|m| m.split(',').map(|s| s.trim().to_lowercase()).collect());
    let should_run_query = |label: &str| -> bool {
        query_filter
            .as_ref()
            .map_or(true, |f| f.iter().any(|q| label.contains(q.as_str())))
    };
    let should_run_mode = |mode: &str| -> bool {
        mode_filter
            .as_ref()
            .map_or(true, |f| f.contains(&mode.to_string()))
    };

    // --naive overrides: disable all prefetch and plan-aware
    let plan_aware = if cli.naive { false } else { cli.plan_aware };
    let naive = cli.naive;

    // Per-query search/lookup schedule pairs (--naive forces all to None)
    let mk_pair = |search: &str, lookup: &str| -> SchedulePair {
        if naive {
            SchedulePair::pair(None, None)
        } else {
            SchedulePair::pair(parse_query_prefetch(search), parse_query_prefetch(lookup))
        }
    };
    let post_pair = mk_pair(&cli.post_prefetch, &cli.post_lookup);
    let profile_pair = mk_pair(&cli.profile_prefetch, &cli.profile_lookup);
    let who_liked_pair = mk_pair(&cli.who_liked_prefetch, &cli.who_liked_lookup);
    let mutual_pair = mk_pair(&cli.mutual_prefetch, &cli.mutual_lookup);
    let idx_filter_pair = mk_pair(&cli.idx_filter_prefetch, &cli.idx_filter_lookup);
    let scan_filter_pair = mk_pair(&cli.scan_filter_prefetch, &cli.scan_filter_lookup);

    let all_queries = vec![
        QueryDef {
            label: "post+user",
            sql: Q_POST_DETAIL,
            param_fn: Box::new(move |i| {
                let pid = phash(i as u64 + 500) % n_posts as u64;
                vec![rusqlite::types::Value::Integer(pid as i64)]
            }),
            schedule: post_pair,
        },
        QueryDef {
            label: "profile",
            sql: Q_PROFILE,
            param_fn: Box::new(move |i| {
                let uid = phash(i as u64 + 100) % n_users as u64;
                vec![rusqlite::types::Value::Integer(uid as i64)]
            }),
            schedule: profile_pair,
        },
        QueryDef {
            label: "who-liked",
            sql: Q_WHO_LIKED,
            param_fn: Box::new(move |i| {
                let pid = phash(i as u64 + 200) % n_posts as u64;
                vec![rusqlite::types::Value::Integer(pid as i64)]
            }),
            schedule: who_liked_pair,
        },
        QueryDef {
            label: "mutual",
            sql: Q_MUTUAL,
            param_fn: Box::new(move |i| {
                let a = phash(i as u64 + 300) % n_users as u64;
                let b = phash(i as u64 + 400) % n_users as u64;
                vec![
                    rusqlite::types::Value::Integer(a as i64),
                    rusqlite::types::Value::Integer(b as i64),
                ]
            }),
            schedule: mutual_pair,
        },
        QueryDef {
            label: "idx-filter",
            sql: Q_IDX_FILTER,
            param_fn: Box::new(move |i| {
                let uid = phash(i as u64 + 600) % n_users as u64;
                vec![rusqlite::types::Value::Integer(uid as i64)]
            }),
            schedule: idx_filter_pair,
        },
        QueryDef {
            label: "scan-filter",
            sql: Q_SCAN_FILTER,
            param_fn: Box::new(move |i| {
                // like_count > threshold — selects ~half the posts
                let threshold = (phash(i as u64 + 700) % 50) as i64;
                vec![rusqlite::types::Value::Integer(threshold)]
            }),
            schedule: scan_filter_pair,
        },
    ];
    let queries: Vec<QueryDef> = all_queries
        .into_iter()
        .filter(|q| should_run_query(q.label))
        .collect();

    if naive {
        println!("  Mode:        NAIVE (no prefetch, no plan-aware, fetch-on-demand only)");
    } else if plan_aware {
        println!("  Plan-aware:  ENABLED (Phase Marne query-plan prefetch)");
    }
    // Print per-query schedules (search / lookup)
    for q in &queries {
        println!("  {:12} schedule: {}", q.label, q.schedule.label());
    }

    if should_run_mode("data") {
        println!();
        println!("=== CACHE LEVEL: DATA (everything cached, reuse connection) ===");
        print_header();
        for q in &queries {
            print_result(&bench_data(
                &warm_conn,
                &ctx,
                q.label,
                q.sql,
                &q.param_fn,
                cli.iterations,
                plan_aware,
                &q.schedule,
            ));
        }
    }

    drop(warm_conn); // close warm connection before S3-fetching benchmarks

    if should_run_mode("index") {
        println!();
        println!("=== CACHE LEVEL: INDEX (interior + index cached, data from S3) ===");
        print_header();
        for q in &queries {
            print_result(&bench_index(
                &reader_vfs_name,
                &db_name,
                &ctx,
                q.label,
                q.sql,
                &q.param_fn,
                cli.warmup,
                cli.iterations,
                plan_aware,
                &q.schedule,
            ));
        }
    }

    if should_run_mode("interior") {
        println!();
        println!("=== CACHE LEVEL: INTERIOR (interior cached, index + data from S3) ===");
        print_header();
        for q in &queries {
            print_result(&bench_interior(
                &reader_vfs_name,
                &db_name,
                &ctx,
                q.label,
                q.sql,
                &q.param_fn,
                cli.warmup,
                cli.iterations,
                plan_aware,
                &q.schedule,
            ));
        }
    }

    if should_run_mode("none") {
        println!();
        println!("=== CACHE LEVEL: NONE (everything from S3) ===");
        print_header();
        for q in &queries {
            print_result(&bench_none(
                &reader_vfs_name,
                &db_name,
                &ctx,
                q.label,
                q.sql,
                &q.param_fn,
                cli.warmup,
                cli.iterations,
                plan_aware,
                &q.schedule,
            ));
        }
    }

    // =====================================================================
    // Matrix mode: test each query with search/lookup schedule pairs
    // =====================================================================
    if cli.matrix {
        // Parse matrix schedules: "search/lookup;search/lookup;..."
        // Each entry is "search_schedule/lookup_schedule" or "off" for both off.
        let pairs: Vec<SchedulePair> = cli
            .matrix_schedules
            .split(';')
            .map(|entry| {
                let entry = entry.trim();
                if entry.eq_ignore_ascii_case("off") || entry.eq_ignore_ascii_case("none") {
                    return SchedulePair::pair(None, None);
                }
                if let Some((search_str, lookup_str)) = entry.split_once('/') {
                    let search = parse_query_prefetch(search_str.trim());
                    let lookup = parse_query_prefetch(lookup_str.trim());
                    SchedulePair::pair(search, lookup)
                } else {
                    // No slash = same schedule for both
                    let sched = parse_query_prefetch(entry);
                    SchedulePair::unified(sched)
                }
            })
            .collect();

        println!();
        println!("=== MATRIX MODE: search/lookup schedule pairs at NONE level ===");
        println!(
            "  {} pairs x {} queries x {} iterations",
            pairs.len(),
            queries.len(),
            cli.iterations
        );
        println!();

        for q in &queries {
            println!("  --- {} ---", q.label);
            println!(
                "  {:<36} {:>10} {:>10} {:>10} {:>12}",
                "search / lookup", "p50", "p90", "GETs", "bytes"
            );
            println!(
                "  {:-<36} {:->10} {:->10} {:->10} {:->12}",
                "", "", "", "", ""
            );

            for pair in &pairs {
                let r = bench_none_pair(
                    &reader_vfs_name,
                    &db_name,
                    &ctx,
                    q.label,
                    q.sql,
                    &q.param_fn,
                    cli.warmup,
                    cli.iterations,
                    plan_aware,
                    pair,
                );
                println!(
                    "  {:<36} {:>10} {:>10} {:>10} {:>12}",
                    pair.label(),
                    format_ms(r.p50()),
                    format_ms(r.p90()),
                    format!("{:.1}", r.avg_fetches()),
                    format_kb(r.avg_bytes_kb()),
                );
            }
            println!();
        }
    }

    println!();

    // --- Cleanup S3 ---
    if cli.cleanup {
        eprint!("  Cleaning up S3... ");
        let cleanup_cache = TempDir::new().expect("cleanup temp dir");
        let cleanup_config = TurboliteConfig {
            cache_dir: cleanup_cache.path().to_path_buf(),
            compression: CompressionConfig {
                level: 1,
                ..Default::default()
            },
            ..Default::default()
        };
        let cleanup_s3 = build_s3_backend(&rt_handle, &s3_prefix);
        let cleanup_vfs = TurboliteVfs::with_backend(
            cleanup_config,
            cleanup_s3 as Arc<dyn hadb_storage::StorageBackend>,
            rt_handle.clone(),
        )
        .expect("cleanup VFS");
        cleanup_vfs.destroy_remote().expect("remote cleanup failed");
        eprintln!("done");
    }
}

fn main() {
    eprintln!("[bench] main() starting");
    let cli = Cli::parse();
    eprintln!("[bench] CLI parsed");
    let sizes: Vec<usize> = cli
        .sizes
        .split(',')
        .map(|s| s.trim().parse().expect("invalid size"))
        .collect();

    println!("=== Tiered VFS Benchmark (Page Group Model) ===");
    println!("Bucket:       {}", test_bucket());
    println!(
        "Endpoint:     {}",
        endpoint_url().as_deref().unwrap_or("(default S3)")
    );
    println!("Page size:    {} bytes", cli.page_size);
    println!(
        "Pages/group:  {} ({:.1} MB uncompressed per group)",
        cli.ppg,
        cli.ppg as f64 * cli.page_size as f64 / (1024.0 * 1024.0)
    );
    println!(
        "Iterations:   {} measured + {} warmup per cold query, {} per warm query",
        cli.iterations, cli.warmup, cli.iterations
    );
    println!("Queries:      post detail, profile, who-liked, mutual friends");
    if cli.plan_aware {
        println!("Plan-aware:   ENABLED (Phase Marne query-plan prefetch)");
    }

    for size in sizes {
        run_benchmark(size, &cli);
    }

    println!();
    println!("Reference: Neon cold start ~500ms (us-east-2, compute pool)");
    println!("Done.");
}
