//! tiered-bench - Warm/cold benchmark for tiered S3-backed VFS
//!
//! Early-Facebook-style dataset:
//!   - users, posts, friendships, likes (4 tables, indexed)
//!   - Point lookups and small JOINs only
//!
//! Two modes per query:
//!   WARM: cache populated, reuse connection → measures cache-hit path (pread)
//!   COLD: clear_cache() before each iteration → measures S3 fetch + decode
//!
//! ```bash
//! TIERED_TEST_BUCKET=turbolite-test \
//!   AWS_ENDPOINT_URL=https://t3.storage.dev \
//!   cargo run --release --features tiered,zstd --bin tiered-bench
//! ```

use clap::Parser;
use rusqlite::{Connection, OpenFlags};
use sqlite_compress_encrypt_vfs::tiered::{TieredBenchHandle, TieredConfig, TieredVfs, set_local_checkpoint_only};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;
use tempfile::TempDir;

static VFS_COUNTER: AtomicU32 = AtomicU32::new(0);

// =========================================================================
// Data constants
// =========================================================================

const FIRST_NAMES: &[&str] = &[
    "Mark", "Eduardo", "Dustin", "Chris", "Sean", "Priscilla", "Sheryl",
    "Andrew", "Adam", "Mike", "Sarah", "Jessica", "Emily", "David", "Alex",
    "Randi", "Naomi", "Kevin", "Amy", "Dan", "Lisa", "Tom", "Rachel",
    "Brian", "Caitlin", "Nicole", "Matt", "Laura", "Jake", "Megan",
];

const LAST_NAMES: &[&str] = &[
    "Zuckerberg", "Saverin", "Moskovitz", "Hughes", "Parker", "Chan",
    "Sandberg", "McCollum", "D'Angelo", "Schroepfer", "Smith", "Johnson",
    "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez",
    "Martinez", "Anderson", "Taylor", "Thomas", "Hernandez", "Moore",
    "Martin", "Jackson", "Thompson", "White", "Lopez",
];

const SCHOOLS: &[&str] = &[
    "Harvard", "Stanford", "MIT", "Yale", "Princeton", "Columbia",
    "Penn", "Brown", "Cornell", "Dartmouth", "Duke", "Georgetown",
    "UCLA", "Berkeley", "Michigan", "NYU", "Boston University",
    "Northeastern", "USC", "Emory",
];

const CITIES: &[&str] = &[
    "Palo Alto, CA", "San Francisco, CA", "New York, NY", "Boston, MA",
    "Cambridge, MA", "Seattle, WA", "Austin, TX", "Chicago, IL",
    "Los Angeles, CA", "Miami, FL", "Denver, CO", "Portland, OR",
    "Philadelphia, PA", "Washington, DC", "Atlanta, GA",
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
    "college", "freshman year", "organic chemistry", "basketball",
    "pizza", "the team", "campus", "Malcolm Gladwell", "New York",
    "Harvard", "Alex", "friend", "Radiohead", "dinner", "thesis",
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

    /// Prefetch hop schedule as comma-separated fractions. Default "0.33,0.33"
    /// means 3 hops: 33% on 1st miss, 33% on 2nd, remaining on 3rd+.
    #[arg(long, default_value = "0.33,0.33", env = "BENCH_PREFETCH_HOPS")]
    prefetch_hops: String,

    /// Which queries to run (comma-separated). Default: all.
    /// Options: post, profile, who-liked, mutual
    #[arg(long, env = "BENCH_QUERIES")]
    queries: Option<String>,

    /// Which modes to run (comma-separated). Default: all.
    /// Options: warm, cold, arctic
    #[arg(long, env = "BENCH_MODES")]
    modes: Option<String>,

    /// Skip COUNT(*) verification (avoids full table scan on tiny machines)
    #[arg(long, env = "BENCH_SKIP_VERIFY")]
    skip_verify: bool,
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
    x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
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
    fn p50(&self) -> f64 { percentile(&self.latencies_us, 0.5) }
    fn p90(&self) -> f64 { percentile(&self.latencies_us, 0.9) }
    fn p99(&self) -> f64 { percentile(&self.latencies_us, 0.99) }
    fn avg_fetches(&self) -> f64 {
        if self.s3_fetches.is_empty() { return 0.0; }
        self.s3_fetches.iter().sum::<u64>() as f64 / self.s3_fetches.len() as f64
    }
    fn avg_bytes_kb(&self) -> f64 {
        if self.s3_bytes.is_empty() { return 0.0; }
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
    prefix: &str,
    cache_dir: &std::path::Path,
    ppg: u32,
    prefetch_threads: u32,
    prefetch_hops: Vec<f32>,
) -> TieredConfig {
    TieredConfig {
        bucket: test_bucket(),
        prefix: prefix.to_string(),
        cache_dir: cache_dir.to_path_buf(),
        compression_level: 1,
        endpoint_url: endpoint_url(),
        region: std::env::var("AWS_REGION").ok(),
        pages_per_group: ppg,
        prefetch_threads,
        prefetch_hops,
        ..Default::default()
    }
}

fn make_reader_config(
    prefix: &str,
    cache_dir: &std::path::Path,
    ppg: u32,
    prefetch_threads: u32,
    prefetch_hops: Vec<f32>,
) -> TieredConfig {
    TieredConfig {
        bucket: test_bucket(),
        prefix: prefix.to_string(),
        cache_dir: cache_dir.to_path_buf(),
        compression_level: 1,
        endpoint_url: endpoint_url(),
        read_only: true,
        region: std::env::var("AWS_REGION").ok(),
        pages_per_group: ppg,
        prefetch_threads,
        prefetch_hops,
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
    let interests = ["music", "startups", "hiking", "photography", "cooking",
                     "travel", "reading", "sports", "gaming", "art"];
    let i1 = interests[((h >> 8) as usize) % interests.len()];
    let i2 = interests[((h >> 16) as usize) % interests.len()];
    let i3 = interests[((h >> 24) as usize) % interests.len()];
    let year = 2004 + (h % 4);
    format!("Class of {}. Into {}, {}, and {}. Looking to connect!", year, i1, i2, i3)
}

fn generate_data(conn: &Connection, n_posts: usize, batch_size: usize) {
    let n_users = (n_posts / 10).max(100);
    let friends_per_user = 50usize;
    let n_friendships = n_users * friends_per_user / 2;
    let n_likes = n_posts * 3;

    eprintln!(
        "  Generating: {} users, {} posts, {} friendships, {} likes",
        format_number(n_users), format_number(n_posts),
        format_number(n_friendships), format_number(n_likes),
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
                    format!("{}.{}{}@{}.edu",
                        FIRST_NAMES[(h as usize) % FIRST_NAMES.len()].to_lowercase(),
                        LAST_NAMES[((h >> 16) as usize) % LAST_NAMES.len()].to_lowercase(),
                        i, SCHOOLS[((h >> 24) as usize) % SCHOOLS.len()].to_lowercase().replace(' ', "")),
                    SCHOOLS[((h >> 24) as usize) % SCHOOLS.len()],
                    CITIES[((h >> 32) as usize) % CITIES.len()],
                    generate_bio(i),
                    1075000000i64 + (h % 100_000_000) as i64,
                ],
            ).unwrap();
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
                    eprintln!("    posts: {}/{}", format_number(i as usize), format_number(n_posts));
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
                    let (a, b) = if (i as i64) < friend { (i as i64, friend) } else { (friend, i as i64) };
                    tx.execute(
                        "INSERT OR IGNORE INTO friendships VALUES (?1,?2,?3)",
                        rusqlite::params![a, b, 1075000000i64 + (h >> 16) as i64 % 94_000_000],
                    ).unwrap();
                    count += 1;
                    batch += 1;
                    if batch >= batch_size {
                        tx.commit().unwrap();
                        tx = conn.unchecked_transaction().unwrap();
                        batch = 0;
                    }
                }
                if count >= n_friendships { break; }
            }
            if count >= n_friendships { break; }
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
            ).unwrap();
            batch += 1;
            if batch >= batch_size {
                tx.commit().unwrap();
                tx = conn.unchecked_transaction().unwrap();
                batch = 0;
                if (i as usize) % (batch_size * 10) == 0 && i > 0 {
                    eprintln!("    likes: {}/{}", format_number(i as usize), format_number(n_likes));
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
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM posts", [], |r| r.get(0)).unwrap();
    let page_count: i64 = conn.query_row("PRAGMA page_count", [], |r| r.get(0)).unwrap();
    let ps: i64 = conn.query_row("PRAGMA page_size", [], |r| r.get(0)).unwrap();
    eprintln!(
        "[local-gen] {} posts, {} pages x {} bytes = {:.1} MB",
        count, page_count, ps, (page_count * ps) as f64 / (1024.0 * 1024.0),
    );
}

// =========================================================================
// Benchmark queries
// =========================================================================

const Q_POST_DETAIL: &str = "\
SELECT p.id, p.content, p.created_at, p.like_count,
       u.first_name, u.last_name, u.school, u.city
FROM posts p
JOIN users u ON u.id = p.user_id
WHERE p.id = ?1";

const Q_PROFILE: &str = "\
SELECT u.first_name, u.last_name, u.school, u.city, u.bio,
       p.id, p.content, p.created_at, p.like_count
FROM users u
JOIN posts p ON p.user_id = u.id
WHERE u.id = ?1
ORDER BY p.created_at DESC
LIMIT 10";

const Q_WHO_LIKED: &str = "\
SELECT u.first_name, u.last_name, u.school, l.created_at
FROM likes l
JOIN users u ON u.id = l.user_id
WHERE l.post_id = ?1
ORDER BY l.created_at DESC
LIMIT 50";

const Q_MUTUAL: &str = "\
SELECT u.id, u.first_name, u.last_name, u.school
FROM friendships f1
JOIN friendships f2 ON f1.user_b = f2.user_b
JOIN users u ON u.id = f1.user_b
WHERE f1.user_a = ?1 AND f2.user_a = ?2
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

fn run_query(
    conn: &Connection,
    sql: &str,
    params: &[rusqlite::types::Value],
) -> Result<usize, rusqlite::Error> {
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
fn bench_warm(
    conn: &Connection,
    handle: &TieredBenchHandle,
    label: &str,
    sql: &str,
    param_fn: &dyn Fn(usize) -> Vec<rusqlite::types::Value>,
    iterations: usize,
) -> BenchResult {
    // Prime: run query once to populate cache, then reuse same params
    // to measure true cache-hit latency (no S3 fetches)
    let params = param_fn(0);
    let _ = run_query(conn, sql, &params);
    handle.reset_s3_counters();

    let mut latencies = Vec::with_capacity(iterations);
    let mut s3_fetches = Vec::with_capacity(iterations);
    let mut s3_bytes = Vec::with_capacity(iterations);

    for i in 0..iterations {
        handle.reset_s3_counters();
        let start = Instant::now();
        match run_query(conn, sql, &params) {
            Ok(_) => {
                latencies.push(start.elapsed().as_micros() as f64);
                let (fc, fb) = handle.s3_counters();
                s3_fetches.push(fc);
                s3_bytes.push(fb);
            }
            Err(e) => eprintln!("    [warm] {} iter {} error: {}", label, i, e),
        }
    }

    BenchResult { label: format!("[warm] {}", label), latencies_us: latencies, s3_fetches, s3_bytes }
}

/// COLD benchmark: clear data pages before each iteration, interior pages stay cached.
fn bench_cold(
    vfs_name: &str,
    db_name: &str,
    handle: &TieredBenchHandle,
    label: &str,
    sql: &str,
    param_fn: &dyn Fn(usize) -> Vec<rusqlite::types::Value>,
    warmup: usize,
    iterations: usize,
) -> BenchResult {
    for i in 0..warmup {
        handle.clear_cache_data_only();
        handle.reset_s3_counters();
        let params = param_fn(i);
        let conn = Connection::open_with_flags_and_vfs(
            db_name, OpenFlags::SQLITE_OPEN_READ_ONLY, vfs_name,
        ).expect("cold connection");
        if let Err(e) = run_query(&conn, sql, &params) {
            eprintln!("    [cold] {} warmup {} error: {}", label, i, e);
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
            db_name, OpenFlags::SQLITE_OPEN_READ_ONLY, vfs_name,
        ).expect("cold connection");
        match run_query(&conn, sql, &params) {
            Ok(_) => {
                latencies.push(start.elapsed().as_micros() as f64);
                let (fc, fb) = handle.s3_counters();
                s3_fetches.push(fc);
                s3_bytes.push(fb);
            }
            Err(e) => eprintln!("    [cold] {} iter {} error: {}", label, i, e),
        }
        drop(conn);
    }

    BenchResult { label: format!("[cold] {}", label), latencies_us: latencies, s3_fetches, s3_bytes }
}

/// ARCTIC benchmark: clear EVERYTHING before each iteration, including interior pages.
/// Interior chunks must be re-fetched from S3 on each connection open.
fn bench_arctic(
    vfs_name: &str,
    db_name: &str,
    handle: &TieredBenchHandle,
    label: &str,
    sql: &str,
    param_fn: &dyn Fn(usize) -> Vec<rusqlite::types::Value>,
    warmup: usize,
    iterations: usize,
) -> BenchResult {
    for i in 0..warmup {
        handle.clear_cache_all();
        handle.reset_s3_counters();
        let params = param_fn(i);
        let conn = Connection::open_with_flags_and_vfs(
            db_name, OpenFlags::SQLITE_OPEN_READ_ONLY, vfs_name,
        ).expect("arctic connection");
        if let Err(e) = run_query(&conn, sql, &params) {
            eprintln!("    [arctic] {} warmup {} error: {}", label, i, e);
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
            db_name, OpenFlags::SQLITE_OPEN_READ_ONLY, vfs_name,
        ).expect("arctic connection");
        match run_query(&conn, sql, &params) {
            Ok(_) => {
                latencies.push(start.elapsed().as_micros() as f64);
                let (fc, fb) = handle.s3_counters();
                s3_fetches.push(fc);
                s3_bytes.push(fb);
            }
            Err(e) => eprintln!("    [arctic] {} iter {} error: {}", label, i, e),
        }
        drop(conn);
    }

    BenchResult { label: format!("[arctic] {}", label), latencies_us: latencies, s3_fetches, s3_bytes }
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
    println!(
        "--- {} posts, {} users (~{:.1} MB, ~{} pages, ~{} page groups) ---",
        format_number(n_posts), format_number(n_users), db_size_mb, est_pages, est_page_groups,
    );

    let cache_dir = TempDir::new().expect("failed to create temp dir");

    eprintln!("[bench] run_benchmark({}) starting", n_posts);
    // Data source priority:
    // 1. --import: generate locally + upload to S3 (fast path)
    // 2. --force: generate through VFS (legacy path)
    // 3. Default: reuse existing S3 data at social_{n_posts}
    let s3_prefix = format!("social_{}", n_posts);
    if cli.import.is_some() {
        // Fast path: generate plain SQLite DB locally, then import to S3
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
        let config = sqlite_compress_encrypt_vfs::tiered::TieredConfig {
            bucket: test_bucket(),
            prefix: s3_prefix.clone(),
            endpoint_url: endpoint_url(),
            region: std::env::var("AWS_REGION").ok(),
            pages_per_group: cli.ppg,
            compression_level: 1,
            ..Default::default()
        };
        let manifest = sqlite_compress_encrypt_vfs::tiered::import_sqlite_file(
            &config,
            std::path::Path::new(&local_path),
        ).expect("import failed");
        println!("  S3 import:   {:.2}s ({} pages, {} groups, {} interior chunks)",
            import_start.elapsed().as_secs_f64(),
            manifest.page_count,
            manifest.page_group_keys.len(),
            manifest.interior_chunk_keys.len(),
        );
    } else if cli.force {
        // Legacy VFS generation path
        let config = make_config(&s3_prefix, cache_dir.path(), cli.ppg, cli.prefetch_threads, parse_prefetch_hops(&cli.prefetch_hops));
        let vfs_name = unique_vfs_name("write");
        let vfs = TieredVfs::new(config).expect("failed to create TieredVfs");
        sqlite_compress_encrypt_vfs::tiered::register(&vfs_name, vfs).unwrap();

        let conn = Connection::open_with_flags_and_vfs(
            &db_name,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        ).expect("failed to open connection");
        conn.execute_batch(&format!(
            "PRAGMA page_size={}; PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;",
            cli.page_size
        )).expect("pragma setup failed");
        conn.execute_batch("
            DROP TABLE IF EXISTS likes;
            DROP TABLE IF EXISTS friendships;
            DROP TABLE IF EXISTS posts;
            DROP TABLE IF EXISTS users;
        ").expect("drop tables failed");
        conn.execute_batch(SCHEMA).expect("create tables failed");

        set_local_checkpoint_only(true);
        conn.execute_batch("PRAGMA wal_autocheckpoint=100000;").expect("autocheckpoint failed");

        let gen_start = Instant::now();
        generate_data(&conn, n_posts, cli.batch_size);
        println!("  Data gen:    {:.2}s", gen_start.elapsed().as_secs_f64());

        set_local_checkpoint_only(false);
        conn.execute_batch("PRAGMA wal_autocheckpoint=0;").expect("autocheckpoint reset failed");
        conn.execute("PRAGMA user_version = 1", []).expect("force dirty page failed");
        let cp_start = Instant::now();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").expect("checkpoint failed");
        println!("  Checkpoint:  {:.2}s", cp_start.elapsed().as_secs_f64());

        let page_count: i64 = conn.query_row("PRAGMA page_count", [], |r| r.get(0)).unwrap_or(0);
        let page_sz: i64 = conn.query_row("PRAGMA page_size", [], |r| r.get(0)).unwrap_or(0);
        let actual_mb = (page_count * page_sz) as f64 / (1024.0 * 1024.0);
        let actual_groups = (page_count as u64 + ppg - 1) / ppg;
        println!("  Actual DB:   {} pages x {} bytes = {:.1} MB ({} page groups)",
            page_count, page_sz, actual_mb, actual_groups);
        drop(conn);
    } else {
        // Default: reuse existing S3 data
        println!("  Reusing S3 prefix: {}", s3_prefix);
    }

    // Create reader VFS + bench handle
    eprintln!("[bench] creating reader VFS...");
    let reader_cache = TempDir::new().expect("reader temp dir");
    let mut reader_config = make_reader_config(&s3_prefix, reader_cache.path(), cli.ppg, cli.prefetch_threads, parse_prefetch_hops(&cli.prefetch_hops));
    if std::env::var("BENCH_NO_EAGER_INDEX").is_ok() {
        reader_config.eager_index_load = false;
        eprintln!("[bench] eager index loading DISABLED (BENCH_NO_EAGER_INDEX set)");
    }
    eprintln!("[bench] calling TieredVfs::new()...");
    let reader_vfs = TieredVfs::new(reader_config).expect("reader VFS");
    eprintln!("[bench] TieredVfs::new() returned OK");
    let bench_handle = reader_vfs.bench_handle();
    let reader_vfs_name = unique_vfs_name("reader");
    eprintln!("[bench] registering VFS '{}'", reader_vfs_name);
    sqlite_compress_encrypt_vfs::tiered::register(&reader_vfs_name, reader_vfs).unwrap();
    eprintln!("[bench] VFS registered, opening connection...");

    // Open persistent connection for warm benchmarks
    let warm_conn = Connection::open_with_flags_and_vfs(
        &db_name, OpenFlags::SQLITE_OPEN_READ_ONLY, &reader_vfs_name,
    ).expect("warm connection");
    if cli.skip_verify {
        eprintln!("[bench] connection opened, skipping COUNT(*) verification");
    } else {
        eprintln!("[bench] connection opened, running COUNT(*)...");
        let row_count: i64 = warm_conn
            .query_row("SELECT COUNT(*) FROM posts", [], |r| r.get(0))
            .expect("count query failed");
        eprintln!("[bench] COUNT(*) returned {}", row_count);
        println!("  Verified: {} posts accessible", format_number(row_count as usize));
    }

    // =====================================================================
    // Run benchmarks
    // =====================================================================

    struct QueryDef {
        label: &'static str,
        sql: &'static str,
        param_fn: Box<dyn Fn(usize) -> Vec<rusqlite::types::Value>>,
    }

    let query_filter: Option<Vec<String>> = cli.queries.as_ref().map(|q| {
        q.split(',').map(|s| s.trim().to_lowercase()).collect()
    });
    let mode_filter: Option<Vec<String>> = cli.modes.as_ref().map(|m| {
        m.split(',').map(|s| s.trim().to_lowercase()).collect()
    });
    let should_run_query = |label: &str| -> bool {
        query_filter.as_ref().map_or(true, |f| f.iter().any(|q| label.contains(q.as_str())))
    };
    let should_run_mode = |mode: &str| -> bool {
        mode_filter.as_ref().map_or(true, |f| f.contains(&mode.to_string()))
    };

    let all_queries = vec![
        QueryDef {
            label: "post+user",
            sql: Q_POST_DETAIL,
            param_fn: Box::new(move |i| {
                let pid = phash(i as u64 + 500) % n_posts as u64;
                vec![rusqlite::types::Value::Integer(pid as i64)]
            }),
        },
        QueryDef {
            label: "profile",
            sql: Q_PROFILE,
            param_fn: Box::new(move |i| {
                let uid = phash(i as u64 + 100) % n_users as u64;
                vec![rusqlite::types::Value::Integer(uid as i64)]
            }),
        },
        QueryDef {
            label: "who-liked",
            sql: Q_WHO_LIKED,
            param_fn: Box::new(move |i| {
                let pid = phash(i as u64 + 200) % n_posts as u64;
                vec![rusqlite::types::Value::Integer(pid as i64)]
            }),
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
        },
        QueryDef {
            label: "idx-filter",
            sql: Q_IDX_FILTER,
            param_fn: Box::new(move |i| {
                let uid = phash(i as u64 + 600) % n_users as u64;
                vec![rusqlite::types::Value::Integer(uid as i64)]
            }),
        },
        QueryDef {
            label: "scan-filter",
            sql: Q_SCAN_FILTER,
            param_fn: Box::new(move |i| {
                // like_count > threshold — selects ~half the posts
                let threshold = (phash(i as u64 + 700) % 50) as i64;
                vec![rusqlite::types::Value::Integer(threshold)]
            }),
        },
    ];
    let queries: Vec<QueryDef> = all_queries.into_iter().filter(|q| should_run_query(q.label)).collect();

    if should_run_mode("warm") {
        println!();
        println!("=== WARM (cache populated, reuse connection) ===");
        print_header();
        for q in &queries {
            print_result(&bench_warm(&warm_conn, &bench_handle, q.label, q.sql, &q.param_fn, cli.iterations));
        }
    }

    drop(warm_conn); // close warm connection before cold benchmarks

    if should_run_mode("cold") {
        println!();
        println!("=== COLD (data evicted, interior pages + group 0 cached) ===");
        print_header();
        for q in &queries {
            print_result(&bench_cold(&reader_vfs_name, &db_name, &bench_handle, q.label, q.sql, &q.param_fn, cli.warmup, cli.iterations));
        }
    }

    if should_run_mode("arctic") {
        println!();
        println!("=== ARCTIC (everything evicted, interior chunks re-fetched from S3) ===");
        print_header();
        for q in &queries {
            print_result(&bench_arctic(&reader_vfs_name, &db_name, &bench_handle, q.label, q.sql, &q.param_fn, cli.warmup, cli.iterations));
        }
    }

    println!();

    // --- Cleanup S3 ---
    if cli.cleanup {
        eprint!("  Cleaning up S3... ");
        let cleanup_cache = TempDir::new().expect("cleanup temp dir");
        let cleanup_config = TieredConfig {
            bucket: test_bucket(),
            prefix: s3_prefix,
            cache_dir: cleanup_cache.path().to_path_buf(),
            compression_level: 1,
            endpoint_url: endpoint_url(),
            region: std::env::var("AWS_REGION").ok(),
            ..Default::default()
        };
        let cleanup_vfs = TieredVfs::new(cleanup_config).expect("cleanup VFS");
        cleanup_vfs.destroy_s3().expect("S3 cleanup failed");
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
    println!("Endpoint:     {}", endpoint_url().as_deref().unwrap_or("(default S3)"));
    println!("Page size:    {} bytes", cli.page_size);
    println!("Pages/group:  {} ({:.1} MB uncompressed per group)", cli.ppg, cli.ppg as f64 * cli.page_size as f64 / (1024.0 * 1024.0));
    println!("Iterations:   {} measured + {} warmup per cold query, {} per warm query", cli.iterations, cli.warmup, cli.iterations);
    println!("Queries:      post detail, profile, who-liked, mutual friends");

    for size in sizes {
        run_benchmark(size, &cli);
    }

    println!();
    println!("Reference: Neon cold start ~500ms (us-east-2, compute pool)");
    println!("Done.");
}
