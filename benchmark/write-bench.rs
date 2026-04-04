//! write-bench - Benchmark for write throughput, checkpoint latency, and
//! incremental correctness of the tiered S3-backed VFS.
//!
//! Scenarios:
//!   sustained          = INSERT with periodic auto-checkpoints firing to S3 inline
//!   checkpoint-latency = Seed DB, dirty N pages, checkpoint, measure. Vary N.
//!   incremental        = Write-checkpoint-write-checkpoint, cold verify each time
//!   update             = UPDATE existing rows, checkpoint, verify
//!   delete             = DELETE rows, checkpoint, verify
//!   realistic          = Randomized bursty mix of INSERT/UPDATE/DELETE/UPSERT/SELECT
//!   cold-write         = Open cold VFS against existing S3 data, write immediately
//!
//! Default page_size=4096, ppg=8 (32KB per group) to force multi-group behavior
//! at modest row counts.
//!
//! ```bash
//! TIERED_TEST_BUCKET=turbolite-test \
//!   AWS_ENDPOINT_URL=https://t3.storage.dev \
//!   cargo run --release --features tiered,zstd --bin write-bench -- --scenario sustained
//! ```

use clap::Parser;
use rusqlite::{Connection, OpenFlags};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;
use tempfile::TempDir;
use turbolite::tiered::{TurboliteSharedState, TurboliteConfig, TurboliteVfs};

static VFS_COUNTER: AtomicU32 = AtomicU32::new(0);

// =========================================================================
// CLI
// =========================================================================

#[derive(Parser)]
#[command(name = "write-bench")]
#[command(about = "Write throughput and checkpoint latency benchmarks for tiered VFS")]
struct Cli {
    /// Scenario to run
    #[arg(long, default_value = "sustained")]
    scenario: String,

    /// Total rows to insert (sustained, incremental, update, delete, realistic)
    #[arg(long, default_value = "5000")]
    rows: usize,

    /// Rows per transaction batch
    #[arg(long, default_value = "500")]
    batch_size: usize,

    /// WAL autocheckpoint threshold in pages (sustained scenario).
    /// Set low to force checkpoints during inserts. Default 20 pages = ~80KB at 4KB pages.
    #[arg(long, default_value = "20")]
    autocheckpoint: i64,

    /// Page size in bytes. Default 4096 to force multi-group at modest row counts.
    #[arg(long, default_value = "4096")]
    page_size: u32,

    /// Pages per page group. Default 8 (32KB per group at 4KB pages).
    #[arg(long, default_value = "8")]
    ppg: u32,

    /// Number of prefetch threads
    #[arg(long, default_value = "4")]
    prefetch_threads: u32,

    /// Comma-separated dirty page counts for checkpoint-latency scenario
    #[arg(long, default_value = "1,5,10,50,100,500")]
    dirty_pages: String,

    /// Number of write-checkpoint cycles for incremental scenario
    #[arg(long, default_value = "5")]
    cycles: usize,

    /// Rows per cycle for incremental scenario
    #[arg(long, default_value = "1000")]
    rows_per_cycle: usize,

    /// Number of rounds for realistic scenario
    #[arg(long, default_value = "20")]
    rounds: usize,

    /// S3 prefix for existing data (merge-write, arctic-write scenarios).
    /// E.g. "social_100000_btree" for the 100K dataset.
    #[arg(long)]
    prefix: Option<String>,

    /// DB filename for existing prefix scenarios
    #[arg(long, default_value = "test.db")]
    db_name: String,
}

// =========================================================================
// Helpers
// =========================================================================

fn unique_vfs_name(prefix: &str) -> String {
    let n = VFS_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("wbench_{}_{}", prefix, n)
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

fn make_config(prefix: &str, cache_dir: &std::path::Path, cli: &Cli) -> TurboliteConfig {
    TurboliteConfig {
        bucket: test_bucket(),
        prefix: prefix.to_string(),
        cache_dir: cache_dir.to_path_buf(),
        compression_level: 1,
        endpoint_url: endpoint_url(),
        region: std::env::var("AWS_REGION").ok(),
        pages_per_group: cli.ppg,
        prefetch_threads: cli.prefetch_threads,
        gc_enabled: true,
        ..Default::default()
    }
}

fn make_reader_config(prefix: &str, cache_dir: &std::path::Path, cli: &Cli) -> TurboliteConfig {
    TurboliteConfig {
        bucket: test_bucket(),
        prefix: prefix.to_string(),
        cache_dir: cache_dir.to_path_buf(),
        compression_level: 1,
        endpoint_url: endpoint_url(),
        read_only: true,
        region: std::env::var("AWS_REGION").ok(),
        pages_per_group: cli.ppg,
        prefetch_threads: cli.prefetch_threads,
        ..Default::default()
    }
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

const SCHEMA: &str = "
CREATE TABLE items (
    id INTEGER PRIMARY KEY,
    value TEXT NOT NULL,
    counter INTEGER NOT NULL DEFAULT 0,
    payload BLOB
);
CREATE INDEX idx_items_counter ON items(counter);
CREATE INDEX idx_items_value ON items(value);
";

fn generate_value(id: i64) -> String {
    let h = phash(id as u64);
    format!("item_{:08}_{}", id, &format!("{:016x}", h)[..8])
}

fn generate_payload(id: i64) -> Vec<u8> {
    let h = phash(id as u64 + 5_000_000);
    let len = 100 + (h % 400) as usize;
    (0..len).map(|i| phash(id as u64 + i as u64) as u8).collect()
}

// =========================================================================
// S3 I/O tracking
// =========================================================================

struct S3Snapshot {
    puts: u64,
    put_bytes: u64,
    gets: u64,
    get_bytes: u64,
}

fn s3_snapshot(bench: &TurboliteSharedState) -> S3Snapshot {
    let (puts, put_bytes) = bench.s3_put_counters();
    let (gets, get_bytes) = bench.s3_counters();
    S3Snapshot { puts, put_bytes, gets, get_bytes }
}

fn s3_put_delta(bench: &TurboliteSharedState, before: &S3Snapshot) -> (u64, u64) {
    let (puts, bytes) = bench.s3_put_counters();
    (puts - before.puts, bytes - before.put_bytes)
}

fn s3_get_delta(bench: &TurboliteSharedState, before: &S3Snapshot) -> (u64, u64) {
    let (gets, bytes) = bench.s3_counters();
    (gets - before.gets, bytes - before.get_bytes)
}

fn format_bytes(b: u64) -> String {
    if b >= 1024 * 1024 {
        format!("{:.1}MB", b as f64 / (1024.0 * 1024.0))
    } else if b >= 1024 {
        format!("{:.1}KB", b as f64 / 1024.0)
    } else {
        format!("{}B", b)
    }
}

// =========================================================================
// Checkpoint timing helper
// =========================================================================

struct CheckpointStats {
    wall_ms: f64,
    s3_puts: u64,
    s3_bytes: u64,
}

fn timed_checkpoint(conn: &Connection, bench: &TurboliteSharedState) -> CheckpointStats {
    let before = s3_snapshot(bench);
    let start = Instant::now();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("checkpoint failed");
    let wall_ms = start.elapsed().as_secs_f64() * 1000.0;
    let (puts, bytes) = s3_put_delta(bench, &before);
    CheckpointStats { wall_ms, s3_puts: puts, s3_bytes: bytes }
}

/// Two-phase non-blocking checkpoint stats.
struct TwoPhaseCheckpointStats {
    /// Time holding SQLite EXCLUSIVE lock (local WAL compaction only)
    lock_ms: f64,
    /// Time for async S3 upload (no lock held)
    flush_ms: f64,
    /// Total wall clock
    total_ms: f64,
    s3_puts: u64,
    s3_bytes: u64,
}

/// Two-phase checkpoint: fast local phase (holds lock briefly) + async S3 upload (no lock).
/// Reads and writes can continue during the S3 upload phase.
fn timed_checkpoint_two_phase(conn: &Connection, bench: &TurboliteSharedState) -> TwoPhaseCheckpointStats {
    let before = s3_snapshot(bench);
    let total_start = Instant::now();

    // Phase 1: local checkpoint (fast, holds EXCLUSIVE lock)
    turbolite::tiered::set_local_checkpoint_only(true);
    let lock_start = Instant::now();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("local checkpoint failed");
    let lock_ms = lock_start.elapsed().as_secs_f64() * 1000.0;
    turbolite::tiered::set_local_checkpoint_only(false);

    // Phase 2: S3 upload (no SQLite lock held)
    let flush_start = Instant::now();
    bench.flush_to_s3().expect("flush_to_s3 failed");
    let flush_ms = flush_start.elapsed().as_secs_f64() * 1000.0;

    let total_ms = total_start.elapsed().as_secs_f64() * 1000.0;
    let (puts, bytes) = s3_put_delta(bench, &before);
    TwoPhaseCheckpointStats { lock_ms, flush_ms, total_ms, s3_puts: puts, s3_bytes: bytes }
}

// =========================================================================
// Cold reader verification
// =========================================================================

fn verify_cold_reader(prefix: &str, db_name: &str, cli: &Cli, expected_rows: usize) {
    let reader_cache = TempDir::new().expect("reader temp dir");
    let reader_config = make_reader_config(prefix, reader_cache.path(), cli);
    let vfs_name = unique_vfs_name("verify");
    let vfs = TurboliteVfs::new(reader_config).expect("reader VFS");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = Connection::open_with_flags_and_vfs(
        db_name,
        OpenFlags::SQLITE_OPEN_READ_ONLY,
        &vfs_name,
    )
    .expect("cold reader connection");

    // integrity_check
    let mut stmt = conn
        .prepare("PRAGMA integrity_check(100)")
        .expect("prepare integrity_check");
    let results: Vec<String> = stmt
        .query_map([], |row| row.get(0))
        .expect("integrity_check")
        .filter_map(|r| r.ok())
        .collect();
    if results.len() != 1 || results[0] != "ok" {
        for msg in &results {
            eprintln!("  INTEGRITY ERROR: {}", msg);
        }
        panic!(
            "integrity_check failed with {} errors after {} expected rows",
            results.len(), expected_rows
        );
    }

    // Row count
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
        .expect("count query");
    assert_eq!(
        count as usize, expected_rows,
        "expected {} rows, got {}", expected_rows, count
    );
    eprintln!(
        "  Cold reader verified: {} rows, integrity_check OK",
        format_number(expected_rows)
    );
}

fn db_stats(conn: &Connection) -> (i64, i64, f64) {
    let page_count: i64 = conn.query_row("PRAGMA page_count", [], |r| r.get(0)).unwrap_or(0);
    let page_sz: i64 = conn.query_row("PRAGMA page_size", [], |r| r.get(0)).unwrap_or(0);
    let db_mb = (page_count * page_sz) as f64 / (1024.0 * 1024.0);
    (page_count, page_sz, db_mb)
}

fn groups_for(page_count: i64, ppg: u32) -> i64 {
    (page_count + ppg as i64 - 1) / ppg as i64
}

// =========================================================================
// VFS + connection setup helper
// =========================================================================

struct VfsSetup {
    conn: Connection,
    bench: TurboliteSharedState,
    prefix: String,
    db_name: String,
    _cache_dir: TempDir,
}

fn setup_writer_vfs(name_prefix: &str, cli: &Cli) -> VfsSetup {
    let cache_dir = TempDir::new().expect("cache dir");
    let prefix = format!("write-bench/{}/{}", name_prefix, std::process::id());
    let config = make_config(&prefix, cache_dir.path(), cli);
    let db_name = format!("{}_test.db", name_prefix);

    let vfs_name = unique_vfs_name(name_prefix);
    let vfs = TurboliteVfs::new(config).expect("TurboliteVfs");
    let bench = vfs.shared_state();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = Connection::open_with_flags_and_vfs(
        &db_name,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .expect("open connection");

    conn.execute_batch(&format!(
        "PRAGMA page_size={};
         PRAGMA journal_mode=WAL;
         PRAGMA wal_autocheckpoint=0;",
        cli.page_size
    ))
    .expect("pragma setup");
    conn.execute_batch(SCHEMA).expect("create tables");

    VfsSetup { conn, bench, prefix, db_name, _cache_dir: cache_dir }
}

/// Bulk insert rows with batched transactions. Returns insert wall time in ms.
fn bulk_insert(conn: &Connection, start_id: usize, count: usize, batch_size: usize) -> f64 {
    let t = Instant::now();
    let mut batch = 0usize;
    let mut tx = conn.unchecked_transaction().unwrap();
    for i in 0..count {
        let id = (start_id + i) as i64;
        tx.execute(
            "INSERT INTO items (id, value, counter, payload) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![id, generate_value(id), phash(id as u64) % 1000, generate_payload(id)],
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
    t.elapsed().as_secs_f64() * 1000.0
}

// =========================================================================
// Scenario: sustained (with inline autocheckpoints to S3)
// =========================================================================

fn scenario_sustained(cli: &Cli) {
    println!("=== Scenario: sustained write throughput (with S3 checkpoints inline) ===");
    println!(
        "  rows={}, batch={}, autocheckpoint={} pages, page_size={}, ppg={}",
        format_number(cli.rows), format_number(cli.batch_size),
        cli.autocheckpoint, cli.page_size, cli.ppg
    );

    let s = setup_writer_vfs("sustained", cli);

    // Enable autocheckpoint so S3 uploads happen during inserts
    s.conn.execute_batch(&format!(
        "PRAGMA wal_autocheckpoint={};", cli.autocheckpoint
    )).expect("autocheckpoint");

    let before = s3_snapshot(&s.bench);
    let overall_start = Instant::now();
    bulk_insert(&s.conn, 0, cli.rows, cli.batch_size);
    let overall_ms = overall_start.elapsed().as_secs_f64() * 1000.0;
    let (auto_puts, auto_bytes) = s3_put_delta(&s.bench, &before);

    // Final manual checkpoint for any remaining dirty pages
    let cp = timed_checkpoint(&s.conn, &s.bench);

    let (pages, psz, mb) = db_stats(&s.conn);
    let groups = groups_for(pages, cli.ppg);

    println!("\n--- Results ---");
    println!("  Insert time:         {:.1}ms ({:.0} rows/sec including auto-checkpoints)",
        overall_ms, cli.rows as f64 / (overall_ms / 1000.0));
    println!("  Auto-checkpoint S3:  {} PUTs, {}", auto_puts, format_bytes(auto_bytes));
    println!("  Final checkpoint:    {:.1}ms, {} PUTs, {}",
        cp.wall_ms, cp.s3_puts, format_bytes(cp.s3_bytes));
    println!("  DB: {} pages x {} = {:.1}MB ({} groups)",
        pages, psz, mb, groups);

    println!("\n--- Cold reader verification ---");
    drop(s.conn);
    verify_cold_reader(&s.prefix, &s.db_name, cli, cli.rows);
}

// =========================================================================
// Scenario: checkpoint-latency
// =========================================================================

fn scenario_checkpoint_latency(cli: &Cli) {
    println!("=== Scenario: checkpoint latency vs dirty page count ===");
    println!("  page_size={}, ppg={}", cli.page_size, cli.ppg);

    let dirty_targets: Vec<usize> = cli.dirty_pages
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    println!(
        "\n  {:>12} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "dirty_pages", "cp_ms", "s3_puts", "s3_bytes", "db_pages", "groups"
    );
    println!("  {}", "-".repeat(65));

    for &target_pages in &dirty_targets {
        let s = setup_writer_vfs(&format!("cplat_{}", target_pages), cli);

        // Phase 1: Seed the DB with enough data to span multiple groups, then checkpoint.
        // We need a baseline DB so that subsequent writes dirty existing groups.
        let seed_rows = 2000;
        bulk_insert(&s.conn, 0, seed_rows, cli.batch_size);
        let _ = timed_checkpoint(&s.conn, &s.bench);

        let (pages_after_seed, _, _) = db_stats(&s.conn);

        // Phase 2: Insert just enough to dirty ~target_pages new pages.
        // At 4KB pages, each row is ~200-600 bytes, so ~8-20 rows per page.
        // Be generous: insert target_pages * 10 rows to ensure we actually dirty that many pages.
        let extra_rows = (target_pages * 10).max(10);
        bulk_insert(&s.conn, seed_rows, extra_rows, extra_rows);

        let (pages_now, _, _) = db_stats(&s.conn);
        let actual_dirty = pages_now - pages_after_seed;

        // Phase 3: Timed checkpoint of just the dirty pages
        let cp = timed_checkpoint(&s.conn, &s.bench);
        let groups = groups_for(pages_now, cli.ppg);

        println!(
            "  {:>12} {:>8.1}ms {:>10} {:>10} {:>10} {:>10}",
            actual_dirty, cp.wall_ms, cp.s3_puts,
            format_bytes(cp.s3_bytes), pages_now, groups
        );

        drop(s.conn);
    }
}

// =========================================================================
// Scenario: incremental
// =========================================================================

fn scenario_incremental(cli: &Cli) {
    println!("=== Scenario: incremental checkpoint correctness ===");
    println!(
        "  cycles={}, rows_per_cycle={}, page_size={}, ppg={}",
        cli.cycles, format_number(cli.rows_per_cycle), cli.page_size, cli.ppg
    );

    let s = setup_writer_vfs("incremental", cli);
    let mut total_rows = 0usize;

    for cycle in 0..cli.cycles {
        let insert_ms = bulk_insert(&s.conn, total_rows, cli.rows_per_cycle, cli.batch_size);
        total_rows += cli.rows_per_cycle;

        let cp = timed_checkpoint(&s.conn, &s.bench);
        let (pages, _, _) = db_stats(&s.conn);
        let groups = groups_for(pages, cli.ppg);

        println!(
            "  cycle {}/{}: {} rows, insert {:.1}ms, cp {:.1}ms ({} PUTs, {}), {} pages ({} groups)",
            cycle + 1, cli.cycles, format_number(total_rows),
            insert_ms, cp.wall_ms, cp.s3_puts, format_bytes(cp.s3_bytes),
            pages, groups
        );

        verify_cold_reader(&s.prefix, &s.db_name, cli, total_rows);
    }

    println!("\n  All {} cycles passed.", cli.cycles);
    drop(s.conn);
}

// =========================================================================
// Scenario: update
// =========================================================================

fn scenario_update(cli: &Cli) {
    println!("=== Scenario: UPDATE existing rows ===");

    let s = setup_writer_vfs("update", cli);

    // Phase 1: Insert + checkpoint
    println!("  Phase 1: INSERT {} rows", format_number(cli.rows));
    let insert_ms = bulk_insert(&s.conn, 0, cli.rows, cli.batch_size);
    let cp1 = timed_checkpoint(&s.conn, &s.bench);
    let (p1, _, _) = db_stats(&s.conn);
    println!("    insert: {:.1}ms, cp: {:.1}ms ({} PUTs, {}), {} pages ({} groups)",
        insert_ms, cp1.wall_ms, cp1.s3_puts, format_bytes(cp1.s3_bytes),
        p1, groups_for(p1, cli.ppg));
    verify_cold_reader(&s.prefix, &s.db_name, cli, cli.rows);

    // Phase 2: UPDATE 10% of rows (scattered to dirty multiple groups)
    let update_count = cli.rows / 10;
    println!("\n  Phase 2: UPDATE {} rows (10%, scattered)", format_number(update_count));
    let t = Instant::now();
    {
        let tx = s.conn.unchecked_transaction().unwrap();
        for i in 0..update_count as i64 {
            let target_id = phash(i as u64 + 99_000) % cli.rows as u64;
            tx.execute(
                "UPDATE items SET value = ?1, counter = counter + 1 WHERE id = ?2",
                rusqlite::params![format!("updated_{:08}", i), target_id as i64],
            ).unwrap();
        }
        tx.commit().unwrap();
    }
    let update_ms = t.elapsed().as_secs_f64() * 1000.0;
    let cp2 = timed_checkpoint(&s.conn, &s.bench);
    println!("    update: {:.1}ms, cp: {:.1}ms ({} PUTs, {})",
        update_ms, cp2.wall_ms, cp2.s3_puts, format_bytes(cp2.s3_bytes));

    // Verify cold reader sees updates
    {
        let reader_cache = TempDir::new().expect("reader temp dir");
        let reader_config = make_reader_config(&s.prefix, reader_cache.path(), cli);
        let vfs_name = unique_vfs_name("verify_upd");
        let vfs = TurboliteVfs::new(reader_config).expect("reader VFS");
        turbolite::tiered::register(&vfs_name, vfs).unwrap();
        let reader = Connection::open_with_flags_and_vfs(
            &s.db_name, OpenFlags::SQLITE_OPEN_READ_ONLY, &vfs_name,
        ).expect("cold reader");

        let count: i64 = reader.query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0)).unwrap();
        assert_eq!(count as usize, cli.rows, "row count changed after UPDATE");
        let updated: i64 = reader.query_row(
            "SELECT COUNT(*) FROM items WHERE counter > 0", [], |r| r.get(0),
        ).unwrap();
        assert!(updated > 0, "no updated rows visible in cold reader");

        let mut stmt = reader.prepare("PRAGMA integrity_check(100)").unwrap();
        let results: Vec<String> = stmt.query_map([], |row| row.get(0)).unwrap().filter_map(|r| r.ok()).collect();
        assert!(results.len() == 1 && results[0] == "ok", "integrity_check failed after UPDATE");
        println!("    cold reader: {} total, {} updated, integrity OK", count, updated);
    }
    drop(s.conn);
}

// =========================================================================
// Scenario: delete
// =========================================================================

fn scenario_delete(cli: &Cli) {
    println!("=== Scenario: DELETE rows ===");

    let s = setup_writer_vfs("delete", cli);

    println!("  Phase 1: INSERT {} rows", format_number(cli.rows));
    let insert_ms = bulk_insert(&s.conn, 0, cli.rows, cli.batch_size);
    let cp1 = timed_checkpoint(&s.conn, &s.bench);
    let (p1, _, _) = db_stats(&s.conn);
    println!("    insert: {:.1}ms, cp: {:.1}ms ({} PUTs, {}), {} pages ({} groups)",
        insert_ms, cp1.wall_ms, cp1.s3_puts, format_bytes(cp1.s3_bytes),
        p1, groups_for(p1, cli.ppg));
    verify_cold_reader(&s.prefix, &s.db_name, cli, cli.rows);

    let delete_count = cli.rows / 2;
    println!("\n  Phase 2: DELETE {} rows (50%)", format_number(delete_count));
    let t = Instant::now();
    {
        let tx = s.conn.unchecked_transaction().unwrap();
        tx.execute("DELETE FROM items WHERE id % 2 = 0", []).unwrap();
        tx.commit().unwrap();
    }
    let delete_ms = t.elapsed().as_secs_f64() * 1000.0;
    let cp2 = timed_checkpoint(&s.conn, &s.bench);
    println!("    delete: {:.1}ms, cp: {:.1}ms ({} PUTs, {})",
        delete_ms, cp2.wall_ms, cp2.s3_puts, format_bytes(cp2.s3_bytes));

    let remaining = cli.rows - delete_count;
    verify_cold_reader(&s.prefix, &s.db_name, cli, remaining);
    drop(s.conn);
}

// =========================================================================
// Scenario: realistic (randomized bursty workload)
// =========================================================================

fn scenario_realistic(cli: &Cli) {
    println!("=== Scenario: realistic randomized workload ===");
    println!(
        "  seed={} rows, rounds={}, page_size={}, ppg={}",
        format_number(cli.rows), cli.rounds, cli.page_size, cli.ppg
    );

    let s = setup_writer_vfs("realistic", cli);

    // Seed
    let seed_ms = bulk_insert(&s.conn, 0, cli.rows, cli.batch_size);
    let cp_seed = timed_checkpoint(&s.conn, &s.bench);
    let (p_seed, _, _) = db_stats(&s.conn);
    println!("  Seed: {} rows in {:.1}ms, cp {:.1}ms ({} PUTs), {} pages ({} groups)",
        format_number(cli.rows), seed_ms, cp_seed.wall_ms, cp_seed.s3_puts,
        p_seed, groups_for(p_seed, cli.ppg));
    verify_cold_reader(&s.prefix, &s.db_name, cli, cli.rows);

    let mut next_id = cli.rows;
    let mut expected_rows = cli.rows as i64;
    let checkpoint_every = 3; // checkpoint every N rounds

    println!(
        "\n  {:>5} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>10} {:>8} {:>10} {:>8}",
        "round", "inserts", "updates", "deletes", "upserts", "selects", "round_ms",
        "cp_ms", "s3_puts", "s3_bytes", "rows"
    );
    println!("  {}", "-".repeat(105));

    for round in 0..cli.rounds {
        let h = phash(round as u64 + 42);

        // Bursty: vary batch size randomly (1 to batch_size)
        let burst = 1 + (h % cli.batch_size as u64) as usize;

        let round_start = Instant::now();
        let mut n_insert = 0usize;
        let mut n_update = 0usize;
        let mut n_delete = 0usize;
        let mut n_upsert = 0usize;
        let mut n_select = 0usize;

        let tx = s.conn.unchecked_transaction().unwrap();

        for op_i in 0..burst {
            let op_hash = phash(round as u64 * 10000 + op_i as u64);
            let op_type = op_hash % 100;

            if expected_rows <= 1 {
                // If almost empty, only insert
                let id = next_id as i64;
                tx.execute(
                    "INSERT INTO items (id, value, counter, payload) VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![id, generate_value(id), 0i64, generate_payload(id)],
                ).unwrap();
                next_id += 1;
                expected_rows += 1;
                n_insert += 1;
                continue;
            }

            match op_type {
                // 30% INSERT
                0..=29 => {
                    let id = next_id as i64;
                    tx.execute(
                        "INSERT INTO items (id, value, counter, payload) VALUES (?1, ?2, ?3, ?4)",
                        rusqlite::params![id, generate_value(id), 0i64, generate_payload(id)],
                    ).unwrap();
                    next_id += 1;
                    expected_rows += 1;
                    n_insert += 1;
                }
                // 25% UPDATE (random existing row)
                30..=54 => {
                    let target = phash(op_hash + 1) % next_id as u64;
                    tx.execute(
                        "UPDATE items SET value = ?1, counter = counter + 1 WHERE id = ?2",
                        rusqlite::params![
                            format!("upd_r{}_o{}", round, op_i),
                            target as i64
                        ],
                    ).unwrap();
                    n_update += 1;
                }
                // 10% DELETE (random existing row)
                55..=64 => {
                    let target = phash(op_hash + 2) % next_id as u64;
                    let deleted = tx.execute(
                        "DELETE FROM items WHERE id = ?1",
                        rusqlite::params![target as i64],
                    ).unwrap();
                    expected_rows -= deleted as i64;
                    n_delete += 1;
                }
                // 15% UPSERT (INSERT OR REPLACE)
                65..=79 => {
                    let target = phash(op_hash + 3) % (next_id as u64 + 10);
                    let is_new = target >= next_id as u64;
                    tx.execute(
                        "INSERT OR REPLACE INTO items (id, value, counter, payload) VALUES (?1, ?2, ?3, ?4)",
                        rusqlite::params![
                            target as i64,
                            format!("upsert_r{}_o{}", round, op_i),
                            (phash(op_hash + 4) % 500) as i64,
                            generate_payload(target as i64)
                        ],
                    ).unwrap();
                    if is_new {
                        if target >= next_id as u64 {
                            next_id = target as usize + 1;
                        }
                        expected_rows += 1;
                    }
                    n_upsert += 1;
                }
                // 20% SELECT (point lookup + indexed scan)
                _ => {
                    let target = phash(op_hash + 5) % next_id as u64;
                    let _: Option<String> = tx.query_row(
                        "SELECT value FROM items WHERE id = ?1",
                        rusqlite::params![target as i64],
                        |r| r.get(0),
                    ).ok();
                    // Also do an indexed range scan
                    let lo = phash(op_hash + 6) % 1000;
                    let hi = lo + 50;
                    let mut stmt = tx.prepare_cached(
                        "SELECT COUNT(*) FROM items WHERE counter BETWEEN ?1 AND ?2"
                    ).unwrap();
                    let _: i64 = stmt.query_row(
                        rusqlite::params![lo as i64, hi as i64], |r| r.get(0)
                    ).unwrap_or(0);
                    n_select += 1;
                }
            }
        }
        tx.commit().unwrap();
        let round_ms = round_start.elapsed().as_secs_f64() * 1000.0;

        // Checkpoint periodically
        if (round + 1) % checkpoint_every == 0 || round == cli.rounds - 1 {
            let cp = timed_checkpoint(&s.conn, &s.bench);

            // Get actual row count for verification
            let actual: i64 = s.conn.query_row(
                "SELECT COUNT(*) FROM items", [], |r| r.get(0)
            ).unwrap();
            expected_rows = actual; // reconcile (DELETE of already-deleted row = no-op)

            println!(
                "  {:>5} {:>8} {:>8} {:>8} {:>8} {:>8} {:>6.1}ms {:>8.1}ms {:>8} {:>10} {:>8}",
                round + 1, n_insert, n_update, n_delete, n_upsert, n_select,
                round_ms, cp.wall_ms, cp.s3_puts, format_bytes(cp.s3_bytes),
                format_number(actual as usize)
            );

            verify_cold_reader(&s.prefix, &s.db_name, cli, actual as usize);
        } else {
            println!(
                "  {:>5} {:>8} {:>8} {:>8} {:>8} {:>8} {:>6.1}ms {:>8} {:>8} {:>10} {:>8}",
                round + 1, n_insert, n_update, n_delete, n_upsert, n_select,
                round_ms, "-", "-", "-",
                format_number(expected_rows as usize)
            );
        }
    }

    let (pages_final, _, mb_final) = db_stats(&s.conn);
    println!("\n  Final: {} pages ({} groups), {:.1}MB", pages_final, groups_for(pages_final, cli.ppg), mb_final);
    drop(s.conn);
}

// =========================================================================
// Scenario: cold-write
// =========================================================================

fn scenario_cold_write(cli: &Cli) {
    println!("=== Scenario: cold-start write latency ===");

    // Phase 1: Create a DB and checkpoint it to S3
    let cache_dir = TempDir::new().expect("cache dir");
    let prefix = format!("write-bench/cold-write/{}", std::process::id());
    let config = make_config(&prefix, cache_dir.path(), cli);
    let db_name = "cold_write_test.db";

    let vfs_name = unique_vfs_name("cold_seed");
    let vfs = TurboliteVfs::new(config).expect("TurboliteVfs");
    let bench_seed = vfs.shared_state();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = Connection::open_with_flags_and_vfs(
        db_name,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    ).expect("open");
    conn.execute_batch(&format!(
        "PRAGMA page_size={}; PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;",
        cli.page_size
    )).expect("pragma");
    conn.execute_batch(SCHEMA).expect("schema");

    let seed_rows = 2000;
    bulk_insert(&conn, 0, seed_rows, cli.batch_size);
    let _ = timed_checkpoint(&conn, &bench_seed);
    let (pages, _, _) = db_stats(&conn);
    println!("  Seeded {} rows ({} pages, {} groups) to S3", seed_rows, pages, groups_for(pages, cli.ppg));
    drop(conn);

    // Phase 2: Open a COLD VFS against the same S3 prefix (empty cache) and write
    let cold_cache = TempDir::new().expect("cold cache dir");
    let cold_config = make_config(&prefix, cold_cache.path(), cli);
    let cold_vfs_name = unique_vfs_name("cold_write");
    let cold_vfs = TurboliteVfs::new(cold_config).expect("cold TurboliteVfs");
    let cold_bench = cold_vfs.shared_state();
    turbolite::tiered::register(&cold_vfs_name, cold_vfs).unwrap();

    let cold_conn = Connection::open_with_flags_and_vfs(
        db_name,
        OpenFlags::SQLITE_OPEN_READ_WRITE,
        &cold_vfs_name,
    ).expect("cold open");
    cold_conn.execute_batch(&format!(
        "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;"
    )).expect("pragma");

    // Measure first write on cold DB
    let before = s3_snapshot(&cold_bench);
    let t = Instant::now();
    cold_conn.execute(
        "INSERT INTO items (id, value, counter, payload) VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![seed_rows as i64, generate_value(seed_rows as i64), 0i64, generate_payload(seed_rows as i64)],
    ).expect("cold write");
    let first_write_ms = t.elapsed().as_secs_f64() * 1000.0;
    let (first_gets, first_bytes) = s3_get_delta(&cold_bench, &before);

    // Measure second write (should be warm now)
    let before2 = s3_snapshot(&cold_bench);
    let t2 = Instant::now();
    cold_conn.execute(
        "INSERT INTO items (id, value, counter, payload) VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![(seed_rows + 1) as i64, generate_value((seed_rows + 1) as i64), 0i64, generate_payload((seed_rows + 1) as i64)],
    ).expect("warm write");
    let second_write_ms = t2.elapsed().as_secs_f64() * 1000.0;
    let (second_gets, second_bytes) = s3_get_delta(&cold_bench, &before2);

    // Checkpoint and verify
    let cp = timed_checkpoint(&cold_conn, &cold_bench);

    println!("\n  First write (cold):  {:.1}ms ({} S3 GETs, {})", first_write_ms, first_gets, format_bytes(first_bytes));
    println!("  Second write (warm): {:.1}ms ({} S3 GETs, {})", second_write_ms, second_gets, format_bytes(second_bytes));
    println!("  Checkpoint:          {:.1}ms ({} PUTs, {})", cp.wall_ms, cp.s3_puts, format_bytes(cp.s3_bytes));

    drop(cold_conn);
    verify_cold_reader(&prefix, db_name, cli, seed_rows + 2);
}

// =========================================================================
// Scenario: merge-write (write against existing 100K data, full merge path)
// =========================================================================

/// Open a cold VFS against existing S3 data, write, checkpoint.
/// Exercises the download-edit-reupload merge path because the VFS must
/// fetch existing page groups from S3 to merge with dirty pages.
fn scenario_merge_write(cli: &Cli) {
    let prefix = cli.prefix.as_ref().expect("--prefix required for merge-write scenario");
    println!("=== Scenario: merge-write (cold VFS against existing data) ===");
    println!("  prefix={}, db={}", prefix, cli.db_name);

    // Open cold VFS (empty cache) against existing S3 prefix
    let cache_dir = TempDir::new().expect("cache dir");
    // Use default config but override ppg/page_size from manifest (manifest takes precedence)
    let config = TurboliteConfig {
        bucket: test_bucket(),
        prefix: prefix.to_string(),
        cache_dir: cache_dir.path().to_path_buf(),
        compression_level: 1,
        endpoint_url: endpoint_url(),
        region: std::env::var("AWS_REGION").ok(),
        pages_per_group: cli.ppg,
        prefetch_threads: cli.prefetch_threads,
        gc_enabled: true,
        ..Default::default()
    };
    let vfs_name = unique_vfs_name("merge_write");
    let vfs = TurboliteVfs::new(config).expect("TurboliteVfs");
    let bench = vfs.shared_state();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = Connection::open_with_flags_and_vfs(
        &cli.db_name,
        OpenFlags::SQLITE_OPEN_READ_WRITE,
        &vfs_name,
    ).expect("open connection");
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;").unwrap();
    let jm: String = conn.query_row("PRAGMA journal_mode", [], |r| r.get(0)).unwrap();
    println!("  journal_mode={}", jm);

    // Get baseline DB stats
    let (pages_before, psz, mb_before) = db_stats(&conn);
    let existing_rows: i64 = conn.query_row(
        "SELECT COUNT(*) FROM posts", [], |r| r.get(0)
    ).unwrap_or(0);
    println!("  Existing DB: {} pages x {} = {:.1}MB, {} posts",
        pages_before, psz, mb_before, format_number(existing_rows as usize));

    // Phase 1: Single cold write (measures S3 GETs for B-tree traversal)
    println!("\n--- Phase 1: Single INSERT (cold, triggers S3 merge on checkpoint) ---");

    let before = s3_snapshot(&bench);
    let t = Instant::now();
    conn.execute(
        "INSERT INTO posts (user_id, content, created_at, like_count) VALUES (?1, ?2, datetime('now'), 0)",
        rusqlite::params![1i64, "merge-write benchmark post"],
    ).expect("cold write");

    let write_ms = t.elapsed().as_secs_f64() * 1000.0;
    let (write_gets, write_get_bytes) = s3_get_delta(&bench, &before);
    println!("  Write: {:.1}ms ({} S3 GETs, {})", write_ms, write_gets, format_bytes(write_get_bytes));

    // Checkpoint: this is the interesting part - forces merge

    let before_cp = s3_snapshot(&bench);
    let cp = timed_checkpoint(&conn, &bench);
    let (cp_gets, cp_get_bytes) = s3_get_delta(&bench, &before_cp);

    println!("  Checkpoint: {:.1}ms", cp.wall_ms);
    println!("    S3 GETs (merge): {} requests, {}", cp_gets, format_bytes(cp_get_bytes));
    println!("    S3 PUTs (upload): {} requests, {}", cp.s3_puts, format_bytes(cp.s3_bytes));

    // Phase 2: Batch write (100 posts) - some pages already warm from phase 1
    println!("\n--- Phase 2: Batch INSERT 100 posts (partially warm) ---");
    let before2 = s3_snapshot(&bench);
    let t2 = Instant::now();
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..100 {
            tx.execute(
                "INSERT INTO posts (user_id, content, created_at, like_count) VALUES (?1, ?2, datetime('now'), 0)",
                rusqlite::params![(i % 1000 + 1) as i64, format!("merge-write batch post {}", i)],
            ).unwrap();
        }
        tx.commit().unwrap();
    }
    let batch_ms = t2.elapsed().as_secs_f64() * 1000.0;
    let (batch_gets, batch_get_bytes) = s3_get_delta(&bench, &before2);
    println!("  Batch write: {:.1}ms ({} S3 GETs, {})", batch_ms, batch_gets, format_bytes(batch_get_bytes));

    let before_cp2 = s3_snapshot(&bench);
    let cp2 = timed_checkpoint(&conn, &bench);
    let (cp2_gets, cp2_get_bytes) = s3_get_delta(&bench, &before_cp2);
    println!("  Checkpoint: {:.1}ms", cp2.wall_ms);
    println!("    S3 GETs (merge): {} requests, {}", cp2_gets, format_bytes(cp2_get_bytes));
    println!("    S3 PUTs (upload): {} requests, {}", cp2.s3_puts, format_bytes(cp2.s3_bytes));

    // Phase 3: Larger batch (1000 posts) - measures sustained merge cost
    println!("\n--- Phase 3: Batch INSERT 1000 posts (measures merge scaling) ---");
    let before3 = s3_snapshot(&bench);
    let t3 = Instant::now();
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..1000 {
            tx.execute(
                "INSERT INTO posts (user_id, content, created_at, like_count) VALUES (?1, ?2, datetime('now'), 0)",
                rusqlite::params![(i % 1000 + 1) as i64, format!("merge-write large batch post {}", i)],
            ).unwrap();
        }
        tx.commit().unwrap();
    }
    let large_ms = t3.elapsed().as_secs_f64() * 1000.0;
    let (large_gets, large_get_bytes) = s3_get_delta(&bench, &before3);
    println!("  Batch write: {:.1}ms ({} S3 GETs, {})", large_ms, large_gets, format_bytes(large_get_bytes));

    let before_cp3 = s3_snapshot(&bench);
    let cp3 = timed_checkpoint(&conn, &bench);
    let (cp3_gets, cp3_get_bytes) = s3_get_delta(&bench, &before_cp3);
    println!("  Checkpoint: {:.1}ms", cp3.wall_ms);
    println!("    S3 GETs (merge): {} requests, {}", cp3_gets, format_bytes(cp3_get_bytes));
    println!("    S3 PUTs (upload): {} requests, {}", cp3.s3_puts, format_bytes(cp3.s3_bytes));

    let (pages_after, _, mb_after) = db_stats(&conn);
    let total_rows: i64 = conn.query_row(
        "SELECT COUNT(*) FROM posts", [], |r| r.get(0)
    ).unwrap_or(0);
    println!("\n  Final: {} pages ({:.1}MB), {} posts", pages_after, mb_after, format_number(total_rows as usize));

    drop(conn);
}

// =========================================================================
// Scenario: arctic-write (no cache at all, even interior pages must be fetched)
// =========================================================================

/// Like merge-write but clears ALL cache (including interior pages) before writing.
/// This simulates the absolute worst case: a VFS that has never seen this DB.
fn scenario_arctic_write(cli: &Cli) {
    let prefix = cli.prefix.as_ref().expect("--prefix required for arctic-write scenario");
    println!("=== Scenario: arctic-write (no cache, full S3 dependency) ===");
    println!("  prefix={}, db={}", prefix, cli.db_name);

    let cache_dir = TempDir::new().expect("cache dir");
    let config = TurboliteConfig {
        bucket: test_bucket(),
        prefix: prefix.to_string(),
        cache_dir: cache_dir.path().to_path_buf(),
        compression_level: 1,
        endpoint_url: endpoint_url(),
        region: std::env::var("AWS_REGION").ok(),
        pages_per_group: cli.ppg,
        prefetch_threads: cli.prefetch_threads,
        gc_enabled: true,
        ..Default::default()
    };
    let vfs_name = unique_vfs_name("arctic_write");
    let vfs = TurboliteVfs::new(config).expect("TurboliteVfs");
    let bench = vfs.shared_state();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    // Open connection (this eagerly fetches interior bundles)
    let conn = Connection::open_with_flags_and_vfs(
        &cli.db_name,
        OpenFlags::SQLITE_OPEN_READ_WRITE,
        &vfs_name,
    ).expect("open connection");
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")
        .expect("pragma setup");

    let (pages, psz, mb) = db_stats(&conn);
    let existing_rows: i64 = conn.query_row(
        "SELECT COUNT(*) FROM posts", [], |r| r.get(0)
    ).unwrap_or(0);
    println!("  DB: {} pages x {} = {:.1}MB, {} posts", pages, psz, mb, format_number(existing_rows as usize));

    // Now evict ALL cache (interior pages, index pages, everything)
    println!("\n  Evicting ALL cache (arctic cold)...");
    bench.clear_cache_all();

    // Measure write on truly arctic DB
    println!("\n--- Arctic write: single INSERT ---");
    let before = s3_snapshot(&bench);
    bench.reset_s3_counters();
    let t = Instant::now();
    conn.execute(
        "INSERT INTO posts (user_id, content, created_at, like_count) VALUES (?1, ?2, datetime('now'), 0)",
        rusqlite::params![1i64, "arctic-write benchmark post"],
    ).expect("arctic write");
    let write_ms = t.elapsed().as_secs_f64() * 1000.0;
    let (gets, get_bytes) = bench.s3_counters();
    let (puts, put_bytes) = bench.s3_put_counters();
    println!("  Write: {:.1}ms", write_ms);
    println!("    S3 GETs: {} requests, {}", gets, format_bytes(get_bytes));
    println!("    S3 PUTs: {} requests, {}", puts, format_bytes(put_bytes));

    // Measure second write (some pages now warm from first write's B-tree traversal)
    bench.reset_s3_counters();
    let t2 = Instant::now();
    conn.execute(
        "INSERT INTO posts (user_id, content, created_at, like_count) VALUES (?1, ?2, datetime('now'), 0)",
        rusqlite::params![2i64, "arctic-write second post"],
    ).expect("second write");
    let write2_ms = t2.elapsed().as_secs_f64() * 1000.0;
    let (gets2, get_bytes2) = bench.s3_counters();
    println!("\n--- Second write (partially warm) ---");
    println!("  Write: {:.1}ms ({} S3 GETs, {})", write2_ms, gets2, format_bytes(get_bytes2));

    // Batch write
    bench.reset_s3_counters();
    let t3 = Instant::now();
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..100 {
            tx.execute(
                "INSERT INTO posts (user_id, content, created_at, like_count) VALUES (?1, ?2, datetime('now'), 0)",
                rusqlite::params![(i % 1000 + 1) as i64, format!("arctic batch post {}", i)],
            ).unwrap();
        }
        tx.commit().unwrap();
    }
    let batch_ms = t3.elapsed().as_secs_f64() * 1000.0;
    let (gets3, get_bytes3) = bench.s3_counters();
    println!("\n--- Batch INSERT 100 posts ---");
    println!("  Write: {:.1}ms ({} S3 GETs, {})", batch_ms, gets3, format_bytes(get_bytes3));

    // Checkpoint: full merge required for all dirty groups
    println!("\n--- Checkpoint (full merge from S3) ---");
    bench.reset_s3_counters();
    let cp_start = Instant::now();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("checkpoint failed");
    let cp_ms = cp_start.elapsed().as_secs_f64() * 1000.0;
    let (cp_gets, cp_get_bytes) = bench.s3_counters();
    let (cp_puts, cp_put_bytes) = bench.s3_put_counters();
    println!("  Checkpoint: {:.1}ms", cp_ms);
    println!("    S3 GETs (merge): {} requests, {}", cp_gets, format_bytes(cp_get_bytes));
    println!("    S3 PUTs (upload): {} requests, {}", cp_puts, format_bytes(cp_put_bytes));

    drop(conn);
}

// =========================================================================
// Scenario: two-phase (compares blocking vs non-blocking checkpoint)
// =========================================================================

/// Side-by-side comparison of blocking checkpoint vs two-phase non-blocking checkpoint.
/// Uses the same write workload to show the difference in lock hold time.
fn scenario_two_phase(cli: &Cli) {
    let prefix = cli.prefix.as_ref().expect("--prefix required for two-phase scenario");
    println!("=== Scenario: two-phase checkpoint (blocking vs non-blocking) ===");
    println!("  prefix={}, db={}", prefix, cli.db_name);

    let cache_dir = TempDir::new().expect("cache dir");
    let config = TurboliteConfig {
        bucket: test_bucket(),
        prefix: prefix.to_string(),
        cache_dir: cache_dir.path().to_path_buf(),
        compression_level: 1,
        endpoint_url: endpoint_url(),
        region: std::env::var("AWS_REGION").ok(),
        pages_per_group: cli.ppg,
        prefetch_threads: cli.prefetch_threads,
        gc_enabled: true,
        ..Default::default()
    };
    let vfs_name = unique_vfs_name("two_phase");
    let vfs = TurboliteVfs::new(config).expect("TurboliteVfs");
    let bench = vfs.shared_state();
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let conn = Connection::open_with_flags_and_vfs(
        &cli.db_name,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    ).expect("open connection");
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;").unwrap();

    // Create schema if fresh prefix
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
         CREATE TABLE IF NOT EXISTS posts (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, content TEXT NOT NULL, created_at TEXT NOT NULL, like_count INTEGER NOT NULL DEFAULT 0);
         CREATE INDEX IF NOT EXISTS idx_posts_user ON posts(user_id);"
    ).unwrap();

    // Seed some users if needed
    let user_count: i64 = conn.query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0)).unwrap();
    if user_count == 0 {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 1..=1000 {
            tx.execute("INSERT INTO users (id, name) VALUES (?1, ?2)", rusqlite::params![i, format!("user_{}", i)]).unwrap();
        }
        tx.commit().unwrap();
        // Checkpoint to S3 so schema + seed data are durable
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    let existing_rows: i64 = conn.query_row(
        "SELECT COUNT(*) FROM posts", [], |r| r.get(0)
    ).unwrap_or(0);
    let (pages, psz, mb) = db_stats(&conn);
    println!("  DB: {} pages x {} = {:.1}MB, {} posts", pages, psz, mb, format_number(existing_rows as usize));

    // Test 1: Blocking checkpoint (100 posts)
    println!("\n--- Test 1: 100 posts + BLOCKING checkpoint ---");
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..100 {
            tx.execute(
                "INSERT INTO posts (user_id, content, created_at, like_count) VALUES (?1, ?2, datetime('now'), 0)",
                rusqlite::params![(i % 1000 + 1) as i64, format!("blocking-test post {}", i)],
            ).unwrap();
        }
        tx.commit().unwrap();
    }
    bench.reset_s3_counters();
    let cp1 = timed_checkpoint(&conn, &bench);
    println!("  Lock held: {:.1}ms (entire checkpoint)", cp1.wall_ms);
    println!("  S3 PUTs: {} requests, {}", cp1.s3_puts, format_bytes(cp1.s3_bytes));

    // Test 2: Two-phase checkpoint (100 posts)
    println!("\n--- Test 2: 100 posts + TWO-PHASE checkpoint ---");
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..100 {
            tx.execute(
                "INSERT INTO posts (user_id, content, created_at, like_count) VALUES (?1, ?2, datetime('now'), 0)",
                rusqlite::params![(i % 1000 + 1) as i64, format!("two-phase-test post {}", i)],
            ).unwrap();
        }
        tx.commit().unwrap();
    }
    bench.reset_s3_counters();
    let cp2 = timed_checkpoint_two_phase(&conn, &bench);
    println!("  Lock held: {:.1}ms (local WAL compaction only)", cp2.lock_ms);
    println!("  S3 flush:  {:.1}ms (no lock, reads/writes can continue)", cp2.flush_ms);
    println!("  Total:     {:.1}ms", cp2.total_ms);
    println!("  S3 PUTs: {} requests, {}", cp2.s3_puts, format_bytes(cp2.s3_bytes));

    // Test 3: Two-phase with larger batch (1000 posts)
    println!("\n--- Test 3: 1000 posts + TWO-PHASE checkpoint ---");
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..1000 {
            tx.execute(
                "INSERT INTO posts (user_id, content, created_at, like_count) VALUES (?1, ?2, datetime('now'), 0)",
                rusqlite::params![(i % 1000 + 1) as i64, format!("two-phase-large post {}", i)],
            ).unwrap();
        }
        tx.commit().unwrap();
    }
    bench.reset_s3_counters();
    let cp3 = timed_checkpoint_two_phase(&conn, &bench);
    println!("  Lock held: {:.1}ms (local WAL compaction only)", cp3.lock_ms);
    println!("  S3 flush:  {:.1}ms (no lock, reads/writes can continue)", cp3.flush_ms);
    println!("  Total:     {:.1}ms", cp3.total_ms);
    println!("  S3 PUTs: {} requests, {}", cp3.s3_puts, format_bytes(cp3.s3_bytes));

    // Summary comparison
    println!("\n--- Summary ---");
    println!("  Blocking:   lock={:.1}ms (readers and writers blocked entire time)", cp1.wall_ms);
    println!("  Two-phase:  lock={:.1}ms, flush={:.1}ms (readers/writers only blocked {:.1}ms)",
        cp2.lock_ms, cp2.flush_ms, cp2.lock_ms);
    println!("  Lock reduction: {:.0}x faster lock release",
        if cp2.lock_ms > 0.0 { cp1.wall_ms / cp2.lock_ms } else { f64::INFINITY });

    drop(conn);
}

// =========================================================================
// Main
// =========================================================================

fn main() {
    let cli = Cli::parse();

    println!("write-bench (turbolite)");
    println!(
        "  page_size={}, ppg={}, prefetch_threads={}",
        cli.page_size, cli.ppg, cli.prefetch_threads
    );
    println!();

    match cli.scenario.as_str() {
        "sustained" => scenario_sustained(&cli),
        "checkpoint-latency" => scenario_checkpoint_latency(&cli),
        "incremental" => scenario_incremental(&cli),
        "update" => scenario_update(&cli),
        "delete" => scenario_delete(&cli),
        "realistic" => scenario_realistic(&cli),
        "cold-write" => scenario_cold_write(&cli),
        "merge-write" => scenario_merge_write(&cli),
        "arctic-write" => scenario_arctic_write(&cli),
        "two-phase" => scenario_two_phase(&cli),
        "all" => {
            scenario_sustained(&cli);
            println!("\n{}\n", "=".repeat(70));
            scenario_checkpoint_latency(&cli);
            println!("\n{}\n", "=".repeat(70));
            scenario_incremental(&cli);
            println!("\n{}\n", "=".repeat(70));
            scenario_update(&cli);
            println!("\n{}\n", "=".repeat(70));
            scenario_delete(&cli);
            println!("\n{}\n", "=".repeat(70));
            scenario_realistic(&cli);
            println!("\n{}\n", "=".repeat(70));
            scenario_cold_write(&cli);
        }
        other => {
            eprintln!(
                "Unknown scenario: '{}'. Options: sustained, checkpoint-latency, incremental, update, delete, realistic, cold-write, merge-write, arctic-write, two-phase, all",
                other
            );
            std::process::exit(1);
        }
    }

    println!("\nDone.");
}
