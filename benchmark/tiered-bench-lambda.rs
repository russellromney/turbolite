//! Lambda handler that runs tiered VFS benchmark directly (no subprocess).
//! All config via env vars: BENCH_SIZES, BENCH_NO_CLEANUP, BENCH_REUSE, BUCKET_NAME.
//!
//! Uses a dedicated tokio runtime for the VFS S3 operations to avoid deadlocking
//! with the Lambda runtime's worker threads.

use lambda_runtime::{service_fn, Error, LambdaEvent};
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use turbolite::tiered::{TieredConfig, TieredVfs};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

static VFS_COUNTER: AtomicU32 = AtomicU32::new(0);

#[derive(Deserialize, Default)]
struct Request {
    #[serde(default)]
    reuse: Option<String>,
}

#[derive(Serialize)]
struct Response {
    output: String,
}

fn unique_vfs_name(label: &str) -> String {
    let id = VFS_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("bench_{}_{}", label, id)
}

fn bucket() -> String {
    std::env::var("BUCKET_NAME").expect("BUCKET_NAME required")
}

fn endpoint_url() -> Option<String> {
    std::env::var("AWS_ENDPOINT_URL")
        .or_else(|_| std::env::var("AWS_ENDPOINT_URL_S3"))
        .ok()
}

async fn handler(event: LambdaEvent<Request>) -> Result<Response, Error> {
    let (req, _ctx) = event.into_parts();
    let mut output = String::new();

    let reuse = req
        .reuse
        .or_else(|| std::env::var("BENCH_REUSE").ok())
        .expect("BENCH_REUSE or reuse payload field required");

    let bucket = bucket();
    let endpoint = endpoint_url();
    let region = std::env::var("AWS_REGION").ok();

    let msg = format!(
        "=== Lambda Tiered VFS Benchmark ===\nBucket: {}\nEndpoint: {}\nRegion: {:?}\nPrefix: {}\n",
        bucket,
        endpoint.as_deref().unwrap_or("(default S3)"),
        region,
        reuse,
    );
    eprintln!("{}", msg);
    output.push_str(&msg);

    // Run on spawn_blocking so the Lambda handler doesn't block a tokio worker.
    // The VFS creates its own internal tokio runtime for S3 operations —
    // it must NOT share the Lambda runtime (would deadlock on block_on).
    let result = tokio::task::spawn_blocking(move || {
        run_benchmark(&bucket, &reuse, endpoint.as_deref(), region.as_deref())
    })
    .await??;

    output.push_str(&result);
    Ok(Response { output })
}

fn run_benchmark(
    bucket: &str,
    prefix: &str,
    endpoint: Option<&str>,
    region: Option<&str>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let mut output = String::new();
    let cache_dir = tempfile::TempDir::new()?;

    let config = TieredConfig {
        bucket: bucket.to_string(),
        prefix: prefix.to_string(),
        cache_dir: cache_dir.path().to_path_buf(),
        compression_level: 3,
        endpoint_url: endpoint.map(|s| s.to_string()),
        read_only: true,
        region: region.map(|s| s.to_string()),
        pages_per_group: 4096,
        ..Default::default()
    };

    eprintln!("[lambda] creating TieredVfs...");
    let vfs = TieredVfs::new(config)?;
    eprintln!("[lambda] VFS created");

    let bench_handle = vfs.bench_handle();
    let vfs_name = unique_vfs_name("lambda");
    eprintln!("[lambda] registering VFS '{}'...", vfs_name);
    turbolite::tiered::register(&vfs_name, vfs)?;
    eprintln!("[lambda] VFS registered, opening connection...");

    let conn = Connection::open_with_flags_and_vfs(
        "social_100000.db",
        OpenFlags::SQLITE_OPEN_READ_ONLY,
        &vfs_name,
    )?;
    eprintln!("[lambda] connection opened, running COUNT(*)...");

    let row_count: i64 = conn.query_row("SELECT COUNT(*) FROM posts", [], |r| r.get(0))?;
    let msg = format!("Verified: {} posts accessible\n", row_count);
    eprintln!("{}", msg);
    output.push_str(&msg);

    // Benchmark queries
    let queries: Vec<(&str, &str)> = vec![
        (
            "post+user",
            "SELECT p.id, p.content, u.first_name, u.last_name FROM posts p JOIN users u ON p.user_id = u.id WHERE p.id = ?",
        ),
        (
            "mutual",
            "SELECT u.first_name, u.last_name FROM friendships f1 JOIN friendships f2 ON f1.friend_id = f2.friend_id JOIN users u ON u.id = f1.friend_id WHERE f1.user_id = ? AND f2.user_id = ? AND f1.friend_id != ? AND f1.friend_id != ?",
        ),
    ];

    let iterations = 20;

    for (label, sql) in &queries {
        let mut cold_times = Vec::with_capacity(iterations);

        for i in 0..iterations {
            bench_handle.clear_cache_all();

            let start = Instant::now();
            let _result: Result<Option<String>, _> = if *label == "mutual" {
                let uid1 = (i as i64 * 7 + 1) % row_count.max(1);
                let uid2 = (i as i64 * 13 + 2) % row_count.max(1);
                conn.query_row(sql, rusqlite::params![uid1, uid2, uid1, uid2], |r| {
                    r.get(0)
                })
                .optional()
            } else {
                let pid = (i as i64 * 7 + 1) % row_count.max(1);
                conn.query_row(sql, [pid], |r| r.get(0)).optional()
            };
            cold_times.push(start.elapsed().as_micros() as f64);
        }

        cold_times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50 = cold_times[iterations / 2];
        let p99 = cold_times[(iterations as f64 * 0.99) as usize];
        let (fetches, bytes) = bench_handle.reset_s3_counters();

        let msg = format!(
            "{}: cold p50={:.1}ms  p99={:.1}ms  ({} fetches, {:.1}KB/query)\n",
            label,
            p50 / 1000.0,
            p99 / 1000.0,
            fetches / iterations as u64,
            bytes as f64 / 1024.0 / iterations as f64,
        );
        eprintln!("{}", msg);
        output.push_str(&msg);
    }

    // Keep cache_dir alive until benchmark completes
    drop(conn);
    drop(cache_dir);

    Ok(output)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    lambda_runtime::run(service_fn(handler)).await
}

trait Optional<T> {
    fn optional(self) -> Result<Option<T>, rusqlite::Error>;
}

impl<T> Optional<T> for Result<T, rusqlite::Error> {
    fn optional(self) -> Result<Option<T>, rusqlite::Error> {
        match self {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }
}
