//! TPC-H benchmark for tiered S3-backed VFS.
//!
//! Generates TPC-H schema (8 tables, foreign keys, indexes), loads data through
//! the VFS, then benchmarks standard TPC-H queries at hot/warm/cold tiers.
//!
//! Exercises: full scans, multi-table joins (3-way, 6-way), filtered scans,
//! point lookups, range queries — the random chunk jumps that real workloads cause.
//!
//! ```bash
//! TIERED_TEST_BUCKET=turbolite-test AWS_ENDPOINT_URL=https://t3.storage.dev \
//!   cargo run --release --features tiered,zstd --bin tpch-bench -- --scale 0.01
//! ```

use clap::Parser;
use rusqlite::{Connection, OpenFlags};
use turbolite::tiered::{TurboliteConfig, TurboliteVfs};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;
use tempfile::TempDir;

static VFS_COUNTER: AtomicU32 = AtomicU32::new(0);

#[derive(Parser)]
#[command(name = "tpch-bench")]
#[command(about = "TPC-H benchmark for tiered S3-backed VFS")]
struct Cli {
    /// TPC-H scale factor (0.01=~10MB, 0.1=~100MB, 1.0=~1GB, 5.0=~5GB)
    #[arg(long, default_value = "0.01")]
    scale: f64,
    /// Iterations for hot/warm benchmarks
    #[arg(long, default_value = "100")]
    iterations: usize,
    /// Iterations for cold benchmarks
    #[arg(long, default_value = "5")]
    cold_iterations: usize,
    /// Skip S3 cleanup after benchmarks
    #[arg(long)]
    no_cleanup: bool,
}

// =========================================================================
// Helpers (shared with tiered-bench)
// =========================================================================

fn unique_vfs_name(prefix: &str) -> String {
    let n = VFS_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("tpch_{}_{}", prefix, n)
}

fn test_bucket() -> String {
    std::env::var("TIERED_TEST_BUCKET").expect("TIERED_TEST_BUCKET env var required")
}

fn endpoint_url() -> String {
    std::env::var("AWS_ENDPOINT_URL").unwrap_or_else(|_| "https://t3.storage.dev".to_string())
}

fn percentile(latencies: &[f64], p: f64) -> f64 {
    if latencies.is_empty() { return 0.0; }
    let mut sorted = latencies.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((p * sorted.len() as f64) as usize).min(sorted.len() - 1);
    sorted[idx]
}

fn format_number(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 { result.push(','); }
        result.push(ch);
    }
    result.chars().rev().collect()
}

struct BenchResult {
    label: String,
    ops: usize,
    latencies_us: Vec<f64>,
}

impl BenchResult {
    fn ops_per_sec(&self) -> f64 {
        let total_secs: f64 = self.latencies_us.iter().sum::<f64>() / 1_000_000.0;
        if total_secs == 0.0 { return 0.0; }
        self.ops as f64 / total_secs
    }
    fn p50(&self) -> f64 { percentile(&self.latencies_us, 0.5) }
    fn p90(&self) -> f64 { percentile(&self.latencies_us, 0.9) }
    fn p99(&self) -> f64 { percentile(&self.latencies_us, 0.99) }
}

fn open_reader(db_name: &str, vfs_name: &str, cache_pages: i64) -> Connection {
    let conn = Connection::open_with_flags_and_vfs(
        db_name, OpenFlags::SQLITE_OPEN_READ_ONLY, vfs_name,
    ).expect("failed to open reader");
    if cache_pages > 0 {
        conn.execute(&format!("PRAGMA cache_size = {}", cache_pages), []).unwrap();
    }
    conn
}

fn make_config(prefix: &str, cache_dir: &std::path::Path) -> TurboliteConfig {
    let unique_prefix = format!("tpch/{}/{}", prefix,
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());
    TurboliteConfig {
        bucket: test_bucket(),
        prefix: unique_prefix,
        cache_dir: cache_dir.to_path_buf(),
        compression_level: 3,
        endpoint_url: Some(endpoint_url()),
        region: Some("auto".to_string()),
        ..Default::default()
    }
}

fn make_reader_config(prefix: &str, cache_dir: &std::path::Path) -> TurboliteConfig {
    TurboliteConfig {
        bucket: test_bucket(),
        prefix: prefix.to_string(),
        cache_dir: cache_dir.to_path_buf(),
        compression_level: 3,
        endpoint_url: Some(endpoint_url()),
        read_only: true,
        region: Some("auto".to_string()),
        ..Default::default()
    }
}

// =========================================================================
// TPC-H constants
// =========================================================================

const REGIONS: [&str; 5] = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"];
const NATIONS: [(i64, &str, i64); 25] = [
    (0,"ALGERIA",0),(1,"ARGENTINA",1),(2,"BRAZIL",1),(3,"CANADA",1),
    (4,"EGYPT",4),(5,"ETHIOPIA",0),(6,"FRANCE",3),(7,"GERMANY",3),
    (8,"INDIA",2),(9,"INDONESIA",2),(10,"IRAN",4),(11,"IRAQ",4),
    (12,"JAPAN",2),(13,"JORDAN",4),(14,"KENYA",0),(15,"MOROCCO",0),
    (16,"MOZAMBIQUE",0),(17,"PERU",1),(18,"CHINA",2),(19,"ROMANIA",3),
    (20,"SAUDI ARABIA",4),(21,"VIETNAM",2),(22,"RUSSIA",3),(23,"UNITED KINGDOM",3),
    (24,"UNITED STATES",1),
];
const SEGMENTS: [&str; 5] = ["AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"];
const PRIORITIES: [&str; 5] = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"];
const SHIP_MODES: [&str; 7] = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"];
const RETURN_FLAGS: [&str; 3] = ["R", "A", "N"];
const LINE_STATUSES: [&str; 2] = ["O", "F"];

/// Deterministic pseudo-random: hash(seed) → u64
fn phash(seed: u64) -> u64 {
    let mut x = seed;
    x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x
}

/// Date as "YYYY-MM-DD" from days offset (base: 1992-01-01)
fn date_from_days(days: i64) -> String {
    let base = 8036; // days from epoch to 1992-01-01
    let total = base + days;
    let (y, m, d) = days_to_ymd(total);
    format!("{:04}-{:02}-{:02}", y, m, d)
}

fn days_to_ymd(days: i64) -> (i64, i64, i64) {
    // Simplified calendar conversion
    let mut y = 1970 + days / 365;
    let mut remaining = days - (y - 1970) * 365 - leap_years_since_1970(y);
    if remaining < 0 { y -= 1; remaining = days - (y - 1970) * 365 - leap_years_since_1970(y); }
    let leap = if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) { 1 } else { 0 };
    let month_days = [31, 28 + leap, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let mut m = 0;
    while m < 12 && remaining >= month_days[m] {
        remaining -= month_days[m];
        m += 1;
    }
    (y, m as i64 + 1, remaining + 1)
}

fn leap_years_since_1970(y: i64) -> i64 {
    let count = |yr: i64| yr / 4 - yr / 100 + yr / 400;
    count(y - 1) - count(1969)
}

// =========================================================================
// Schema + data generation
// =========================================================================

const SCHEMA: &str = "
CREATE TABLE region (r_regionkey INTEGER PRIMARY KEY, r_name TEXT NOT NULL, r_comment TEXT);
CREATE TABLE nation (n_nationkey INTEGER PRIMARY KEY, n_name TEXT NOT NULL, n_regionkey INTEGER NOT NULL, n_comment TEXT);
CREATE TABLE supplier (s_suppkey INTEGER PRIMARY KEY, s_name TEXT NOT NULL, s_address TEXT NOT NULL, s_nationkey INTEGER NOT NULL, s_phone TEXT NOT NULL, s_acctbal REAL NOT NULL, s_comment TEXT);
CREATE TABLE customer (c_custkey INTEGER PRIMARY KEY, c_name TEXT NOT NULL, c_address TEXT NOT NULL, c_nationkey INTEGER NOT NULL, c_phone TEXT NOT NULL, c_acctbal REAL NOT NULL, c_mktsegment TEXT NOT NULL, c_comment TEXT);
CREATE TABLE part (p_partkey INTEGER PRIMARY KEY, p_name TEXT NOT NULL, p_mfgr TEXT NOT NULL, p_brand TEXT NOT NULL, p_type TEXT NOT NULL, p_size INTEGER NOT NULL, p_container TEXT NOT NULL, p_retailprice REAL NOT NULL, p_comment TEXT);
CREATE TABLE partsupp (ps_partkey INTEGER NOT NULL, ps_suppkey INTEGER NOT NULL, ps_availqty INTEGER NOT NULL, ps_supplycost REAL NOT NULL, ps_comment TEXT, PRIMARY KEY (ps_partkey, ps_suppkey));
CREATE TABLE orders (o_orderkey INTEGER PRIMARY KEY, o_custkey INTEGER NOT NULL, o_orderstatus TEXT NOT NULL, o_totalprice REAL NOT NULL, o_orderdate TEXT NOT NULL, o_orderpriority TEXT NOT NULL, o_clerk TEXT NOT NULL, o_shippriority INTEGER NOT NULL, o_comment TEXT);
CREATE TABLE lineitem (l_orderkey INTEGER NOT NULL, l_partkey INTEGER NOT NULL, l_suppkey INTEGER NOT NULL, l_linenumber INTEGER NOT NULL, l_quantity REAL NOT NULL, l_extendedprice REAL NOT NULL, l_discount REAL NOT NULL, l_tax REAL NOT NULL, l_returnflag TEXT NOT NULL, l_linestatus TEXT NOT NULL, l_shipdate TEXT NOT NULL, l_commitdate TEXT NOT NULL, l_receiptdate TEXT NOT NULL, l_shipinstruct TEXT NOT NULL, l_shipmode TEXT NOT NULL, l_comment TEXT, PRIMARY KEY (l_orderkey, l_linenumber));
";

const INDEXES: &str = "
CREATE INDEX idx_lineitem_shipdate ON lineitem(l_shipdate);
CREATE INDEX idx_lineitem_orderkey ON lineitem(l_orderkey);
CREATE INDEX idx_orders_custkey ON orders(o_custkey);
CREATE INDEX idx_orders_orderdate ON orders(o_orderdate);
CREATE INDEX idx_customer_nationkey ON customer(c_nationkey);
CREATE INDEX idx_supplier_nationkey ON supplier(s_nationkey);
CREATE INDEX idx_nation_regionkey ON nation(n_regionkey);
";

fn generate_data(conn: &Connection, scale: f64) {
    let n_supplier = (10_000.0 * scale).max(10.0) as i64;
    let n_customer = (150_000.0 * scale).max(100.0) as i64;
    let n_part = (200_000.0 * scale).max(100.0) as i64;
    let n_orders = (1_500_000.0 * scale).max(1000.0) as i64;

    eprintln!("  Generating: {} suppliers, {} customers, {} parts, {} orders",
        format_number(n_supplier as usize), format_number(n_customer as usize),
        format_number(n_part as usize), format_number(n_orders as usize));

    let tx = conn.unchecked_transaction().unwrap();

    // Region + Nation (static)
    for (i, name) in REGIONS.iter().enumerate() {
        tx.execute("INSERT INTO region VALUES (?1, ?2, ?3)",
            rusqlite::params![i as i64, name, format!("region {} comment", i)]).unwrap();
    }
    for &(key, name, rkey) in &NATIONS {
        tx.execute("INSERT INTO nation VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![key, name, rkey, format!("nation {} comment", key)]).unwrap();
    }

    // Supplier
    {
        let mut stmt = tx.prepare("INSERT INTO supplier VALUES (?1,?2,?3,?4,?5,?6,?7)").unwrap();
        for i in 0..n_supplier {
            let h = phash(i as u64);
            stmt.execute(rusqlite::params![
                i + 1,
                format!("Supplier#{:09}", i + 1),
                format!("{} Street, City {}", h % 9999, h % 100),
                (h % 25) as i64,
                format!("{:02}-{:03}-{:03}-{:04}", h % 90 + 10, h % 1000, h % 1000, h % 10000),
                (h % 200000) as f64 / 20.0 - 999.99,
                format!("supplier comment {}", i),
            ]).unwrap();
        }
    }

    // Customer
    {
        let mut stmt = tx.prepare("INSERT INTO customer VALUES (?1,?2,?3,?4,?5,?6,?7,?8)").unwrap();
        for i in 0..n_customer {
            let h = phash(i as u64 + 1_000_000);
            stmt.execute(rusqlite::params![
                i + 1,
                format!("Customer#{:09}", i + 1),
                format!("{} Ave, Town {}", h % 9999, h % 200),
                (h % 25) as i64,
                format!("{:02}-{:03}-{:03}-{:04}", h % 90 + 10, h % 1000, h % 1000, h % 10000),
                (h % 200000) as f64 / 20.0 - 999.99,
                SEGMENTS[(h >> 16) as usize % 5],
                format!("customer comment {}", i),
            ]).unwrap();
        }
    }

    // Part
    {
        let mut stmt = tx.prepare("INSERT INTO part VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)").unwrap();
        for i in 0..n_part {
            let h = phash(i as u64 + 2_000_000);
            stmt.execute(rusqlite::params![
                i + 1,
                format!("part-{}-{}-{}", h % 100, (h >> 8) % 100, (h >> 16) % 100),
                format!("Manufacturer#{}", (h % 5) + 1),
                format!("Brand#{}{}", (h % 5) + 1, (h >> 8) % 5 + 1),
                format!("type-{}-{}", (h >> 12) % 10, (h >> 16) % 10),
                (h % 50 + 1) as i64,
                format!("container-{}", (h >> 20) % 8),
                (900.0 + (i as f64) / 10.0 + (h % 100) as f64),
                format!("part comment {}", i),
            ]).unwrap();
        }
    }

    // PartSupp (4 suppliers per part)
    {
        let mut stmt = tx.prepare("INSERT INTO partsupp VALUES (?1,?2,?3,?4,?5)").unwrap();
        for i in 0..n_part {
            for j in 0..4i64 {
                let h = phash(i as u64 * 4 + j as u64 + 3_000_000);
                let suppkey = (i * 4 + j) % n_supplier + 1;
                stmt.execute(rusqlite::params![
                    i + 1, suppkey,
                    (h % 9999 + 1) as i64,
                    (h % 100000) as f64 / 100.0,
                    format!("partsupp comment {}-{}", i, j),
                ]).unwrap();
            }
        }
    }

    // Orders + Lineitem
    let mut lineitem_count: i64 = 0;
    {
        let mut ord_stmt = tx.prepare(
            "INSERT INTO orders VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)").unwrap();
        let mut li_stmt = tx.prepare(
            "INSERT INTO lineitem VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)").unwrap();

        // TPC-H date range: 1992-01-01 to 1998-08-02 = 2405 days
        for i in 0..n_orders {
            let h = phash(i as u64 + 5_000_000);
            let custkey = (h % n_customer as u64) as i64 + 1;
            let order_days = (h >> 8) % 2405;
            let orderdate = date_from_days(order_days as i64);
            let status = if order_days > 2100 { "O" } else { "F" };
            let n_items = (h >> 20) % 7 + 1; // 1-7 items per order

            let mut total = 0.0f64;
            for j in 0..n_items {
                let lh = phash(i as u64 * 7 + j + 6_000_000);
                let partkey = (lh % n_part as u64) as i64 + 1;
                let suppkey = (lh >> 8) % n_supplier as u64 + 1;
                let qty = (lh >> 16) % 50 + 1;
                let price = 900.0 + (partkey as f64) / 10.0;
                let extended = qty as f64 * price;
                let discount = ((lh >> 24) % 11) as f64 / 100.0; // 0.00-0.10
                let tax = ((lh >> 28) % 9) as f64 / 100.0; // 0.00-0.08
                let ship_days = order_days + (lh >> 32) % 121 + 1; // 1-121 days after order
                let commit_days = order_days + (lh >> 36) % 91 + 30; // 30-120 days after
                let receipt_days = ship_days + (lh >> 40) % 31; // 0-30 days after ship
                total += extended * (1.0 - discount) * (1.0 + tax);

                li_stmt.execute(rusqlite::params![
                    i + 1, partkey, suppkey as i64, j as i64 + 1,
                    qty as f64, extended, discount, tax,
                    RETURN_FLAGS[(lh >> 44) as usize % 3],
                    LINE_STATUSES[(lh >> 46) as usize % 2],
                    date_from_days(ship_days as i64),
                    date_from_days(commit_days as i64),
                    date_from_days(receipt_days as i64),
                    "DELIVER IN PERSON",
                    SHIP_MODES[(lh >> 48) as usize % 7],
                    format!("lineitem comment {}-{}", i, j),
                ]).unwrap();
                lineitem_count += 1;
            }

            ord_stmt.execute(rusqlite::params![
                i + 1, custkey, status, total, orderdate,
                PRIORITIES[(h >> 28) as usize % 5],
                format!("Clerk#{:09}", (h >> 32) % 1000 + 1),
                0i64,
                format!("order comment {}", i),
            ]).unwrap();
        }
    }
    tx.commit().unwrap();
    eprintln!("  Generated {} lineitem rows", format_number(lineitem_count as usize));
}

// =========================================================================
// TPC-H queries
// =========================================================================

/// Q1: Pricing summary (full lineitem scan + aggregation)
const Q1: &str = "
SELECT l_returnflag, l_linestatus, SUM(l_quantity), SUM(l_extendedprice),
       SUM(l_extendedprice * (1 - l_discount)),
       SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
       AVG(l_quantity), AVG(l_extendedprice), AVG(l_discount), COUNT(*)
FROM lineitem WHERE l_shipdate <= '1998-09-01'
GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus";

/// Q3: Shipping priority (3-way join: customer→orders→lineitem)
const Q3: &str = "
SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) as revenue,
       o_orderdate, o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey AND o_orderdate < '1995-03-15'
  AND l_shipdate > '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate LIMIT 10";

/// Q5: Local supplier volume (6-way join)
const Q5: &str = "
SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) as revenue
FROM customer, orders, lineitem, supplier, nation, region
WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey AND r_name = 'ASIA'
  AND o_orderdate >= '1994-01-01' AND o_orderdate < '1995-01-01'
GROUP BY n_name ORDER BY revenue DESC";

/// Q6: Forecasting revenue (filtered lineitem scan)
const Q6: &str = "
SELECT SUM(l_extendedprice * l_discount) as revenue
FROM lineitem
WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
  AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24";

/// Q9: Product type profit (6-way join with LIKE)
const Q9: &str = "
SELECT n_name, SUM(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) as profit
FROM part, supplier, lineitem, partsupp, orders, nation
WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey
  AND p_partkey = l_partkey AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey
  AND p_name LIKE '%part-5%'
GROUP BY n_name ORDER BY n_name";

/// Point lookup on orders
const Q_POINT: &str = "SELECT * FROM orders WHERE o_orderkey = ?1";

/// Range scan on lineitem by shipdate
const Q_RANGE: &str = "SELECT COUNT(*), SUM(l_extendedprice) FROM lineitem WHERE l_shipdate BETWEEN ?1 AND ?2";

struct QueryDef {
    label: &'static str,
    sql: &'static str,
}

const QUERIES: [QueryDef; 5] = [
    QueryDef { label: "Q1 scan+agg", sql: Q1 },
    QueryDef { label: "Q3 3-join", sql: Q3 },
    QueryDef { label: "Q5 6-join", sql: Q5 },
    QueryDef { label: "Q6 filter", sql: Q6 },
    QueryDef { label: "Q9 6-join", sql: Q9 },
];

fn bench_query(conn: &Connection, label: &str, sql: &str, iterations: usize) -> BenchResult {
    let mut latencies = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        let mut stmt = conn.prepare_cached(sql).expect("prepare failed");
        let _rows: Vec<Vec<rusqlite::types::Value>> = stmt
            .query_map([], |row| {
                let n = row.as_ref().column_count();
                let mut vals = Vec::with_capacity(n);
                for i in 0..n { vals.push(row.get::<_, rusqlite::types::Value>(i)?); }
                Ok(vals)
            }).unwrap().map(|r| r.unwrap()).collect();
        latencies.push(start.elapsed().as_micros() as f64);
    }
    BenchResult { label: label.to_string(), ops: iterations, latencies_us: latencies }
}

fn bench_point(conn: &Connection, n_orders: i64, iterations: usize, label: &str) -> BenchResult {
    let mut latencies = Vec::with_capacity(iterations);
    let mut idx: u64 = 7;
    for _ in 0..iterations {
        let orderkey = (idx % n_orders as u64) as i64 + 1;
        let start = Instant::now();
        conn.query_row(Q_POINT, [orderkey], |_row| Ok(())).expect("point lookup failed");
        latencies.push(start.elapsed().as_micros() as f64);
        idx = phash(idx) % n_orders as u64;
    }
    BenchResult { label: label.to_string(), ops: iterations, latencies_us: latencies }
}

fn bench_range(conn: &Connection, iterations: usize, label: &str) -> BenchResult {
    let mut latencies = Vec::with_capacity(iterations);
    let mut day_offset: u64 = 0;
    for _ in 0..iterations {
        let d1 = date_from_days(day_offset as i64);
        let d2 = date_from_days(day_offset as i64 + 30);
        let start = Instant::now();
        conn.query_row(Q_RANGE, [&d1, &d2], |_row| Ok(())).expect("range query failed");
        latencies.push(start.elapsed().as_micros() as f64);
        day_offset = (day_offset + 60) % 2400;
    }
    BenchResult { label: label.to_string(), ops: iterations, latencies_us: latencies }
}

fn bench_cold_query(
    s3_prefix: &str, db_name: &str, label: &str, sql: &str,
    iterations: usize,
) -> BenchResult {
    let mut latencies = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let cache = TempDir::new().expect("temp dir");
        let vfs_name = unique_vfs_name("cold_q");
        let config = make_reader_config(s3_prefix, cache.path());
        let vfs = TurboliteVfs::new(config).expect("cold VFS");
        turbolite::tiered::register(&vfs_name, vfs).unwrap();
        let start = Instant::now();
        let conn = open_reader(db_name, &vfs_name, 1);
        let mut stmt = conn.prepare(sql).expect("prepare failed");
        let _rows: Vec<Vec<rusqlite::types::Value>> = stmt
            .query_map([], |row| {
                let n = row.as_ref().column_count();
                let mut vals = Vec::with_capacity(n);
                for i in 0..n { vals.push(row.get::<_, rusqlite::types::Value>(i)?); }
                Ok(vals)
            }).unwrap().map(|r| r.unwrap()).collect();
        latencies.push(start.elapsed().as_micros() as f64);
        drop(stmt);
        drop(conn);
    }
    BenchResult { label: label.to_string(), ops: iterations, latencies_us: latencies }
}

// =========================================================================
// Main
// =========================================================================

fn main() {
    let cli = Cli::parse();
    let scale = cli.scale;
    let _ppg_bytes: u64 = 2048 * 65536; // ~128MB page groups at 64KB pages

    let n_orders = (1_500_000.0 * scale).max(1000.0) as i64;
    let est_db_mb = scale * 1000.0; // rough: SF1 ≈ 1GB

    println!("=== TPC-H Benchmark (Tiered VFS) ===");
    println!("Scale factor: {} (~{:.0} MB estimated)", scale, est_db_mb);
    println!("Bucket: {}", test_bucket());
    println!("Endpoint: {}", endpoint_url());
    println!("Iterations: {} hot/warm, {} cold", cli.iterations, cli.cold_iterations);
    println!();

    // Setup: create VFS
    let cache_dir = TempDir::new().expect("temp dir");
    let config = make_config(&format!("sf_{}", scale), cache_dir.path());
    let s3_prefix = config.prefix.clone();
    let vfs_name = unique_vfs_name("write");
    let vfs = TurboliteVfs::new(config).expect("failed to create VFS");
    turbolite::tiered::register(&vfs_name, vfs).unwrap();

    let db_name = format!("tpch_sf{}.db", scale);
    let conn = Connection::open_with_flags_and_vfs(
        &db_name,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    ).expect("failed to open connection");

    conn.execute_batch("PRAGMA page_size=65536; PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;").unwrap();

    // Schema
    let schema_start = Instant::now();
    conn.execute_batch(SCHEMA).unwrap();
    conn.execute_batch(INDEXES).unwrap();
    println!("Schema:      {:.2}s", schema_start.elapsed().as_secs_f64());

    // Data generation
    let gen_start = Instant::now();
    generate_data(&conn, scale);
    println!("Data gen:    {:.2}s", gen_start.elapsed().as_secs_f64());

    // Checkpoint
    let cp_start = Instant::now();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    println!("Checkpoint:  {:.2}s", cp_start.elapsed().as_secs_f64());

    // =====================================================================
    // HOT: same connection, default page cache
    // =====================================================================
    println!("\n--- HOT (everything in RAM) ---");
    let mut hot_results: Vec<BenchResult> = Vec::new();
    for q in &QUERIES {
        hot_results.push(bench_query(&conn, &format!("Hot {}", q.label), q.sql, cli.iterations));
    }
    hot_results.push(bench_point(&conn, n_orders, cli.iterations, "Hot point"));
    hot_results.push(bench_range(&conn, cli.iterations, "Hot range"));
    drop(conn);

    // =====================================================================
    // WARM: full disk cache, 20% page cache
    // =====================================================================
    println!("--- WARM (disk cache + 20% page cache) ---");
    let warm_vfs_name = unique_vfs_name("warm");
    let warm_config = make_reader_config(&s3_prefix, cache_dir.path());
    let warm_vfs = TurboliteVfs::new(warm_config).expect("warm VFS");
    turbolite::tiered::register(&warm_vfs_name, warm_vfs).unwrap();

    let cache_pages = (est_db_mb * 1024.0 * 1024.0 * 0.2 / 65536.0) as i64;
    let warm_conn = open_reader(&db_name, &warm_vfs_name, cache_pages);
    let mut warm_results: Vec<BenchResult> = Vec::new();
    for q in &QUERIES {
        warm_results.push(bench_query(&warm_conn, &format!("Warm {}", q.label), q.sql, cli.iterations));
    }
    warm_results.push(bench_point(&warm_conn, n_orders, cli.iterations, "Warm point"));
    warm_results.push(bench_range(&warm_conn, cli.iterations, "Warm range"));
    drop(warm_conn);

    // =====================================================================
    // COLD: fresh VFS per query
    // =====================================================================
    println!("--- COLD (fresh VFS, empty cache) ---");
    let mut cold_results: Vec<BenchResult> = Vec::new();
    for q in &QUERIES {
        cold_results.push(bench_cold_query(
            &s3_prefix, &db_name, &format!("Cold {}", q.label), q.sql,
            cli.cold_iterations,
        ));
    }
    // Cold point
    {
        let mut latencies = Vec::new();
        let mut idx: u64 = 7;
        for _ in 0..cli.cold_iterations {
            let orderkey = (idx % n_orders as u64) as i64 + 1;
            let cache = TempDir::new().unwrap();
            let vn = unique_vfs_name("cold_pt");
            let cfg = make_reader_config(&s3_prefix, cache.path());
            let v = TurboliteVfs::new(cfg).expect("cold VFS");
            turbolite::tiered::register(&vn, v).unwrap();
            let start = Instant::now();
            let c = open_reader(&db_name, &vn, 1);
            c.query_row(Q_POINT, [orderkey], |_| Ok(())).unwrap();
            latencies.push(start.elapsed().as_micros() as f64);
            drop(c);
            idx = phash(idx) % n_orders as u64;
        }
        cold_results.push(BenchResult { label: "Cold point".to_string(), ops: cli.cold_iterations, latencies_us: latencies });
    }
    // Cold range
    {
        let mut latencies = Vec::new();
        let mut day_offset: u64 = 0;
        for _ in 0..cli.cold_iterations {
            let d1 = date_from_days(day_offset as i64);
            let d2 = date_from_days(day_offset as i64 + 30);
            let cache = TempDir::new().unwrap();
            let vn = unique_vfs_name("cold_rng");
            let cfg = make_reader_config(&s3_prefix, cache.path());
            let v = TurboliteVfs::new(cfg).expect("cold VFS");
            turbolite::tiered::register(&vn, v).unwrap();
            let start = Instant::now();
            let c = open_reader(&db_name, &vn, 1);
            c.query_row(Q_RANGE, [&d1, &d2], |_| Ok(())).unwrap();
            latencies.push(start.elapsed().as_micros() as f64);
            drop(c);
            day_offset = (day_offset + 60) % 2400;
        }
        cold_results.push(BenchResult { label: "Cold range".to_string(), ops: cli.cold_iterations, latencies_us: latencies });
    }

    // =====================================================================
    // Results
    // =====================================================================
    println!("\n=== Results (SF {}, ~{:.0} MB) ===\n", scale, est_db_mb);
    println!("  {:<20} {:>10} {:>10} {:>10} {:>10}",
        "", "ops/sec", "p50 (us)", "p90 (us)", "p99 (us)");
    println!("  {:-<20} {:->10} {:->10} {:->10} {:->10}", "", "", "", "", "");

    for (label, results) in [("HOT", &hot_results), ("WARM", &warm_results), ("COLD", &cold_results)] {
        for r in results {
            println!("  {:<20} {:>10} {:>10.0} {:>10.0} {:>10.0}",
                r.label, format_number(r.ops_per_sec() as usize), r.p50(), r.p90(), r.p99());
        }
        let _ = label;
        println!();
    }

    // Neon comparison context
    println!("  Neon cold start reference: ~500ms (us-east-2, compute pool)");
    println!("  Neon goal: ~50ms");
    println!();

    // Cleanup
    if !cli.no_cleanup {
        eprint!("  Cleaning up S3... ");
        let cleanup_cache = TempDir::new().unwrap();
        let cleanup_config = TurboliteConfig {
            bucket: test_bucket(), prefix: s3_prefix,
            cache_dir: cleanup_cache.path().to_path_buf(), compression_level: 3,
            endpoint_url: Some(endpoint_url()), region: Some("auto".to_string()),
            ..Default::default()
        };
        let cleanup_vfs = TurboliteVfs::new(cleanup_config).expect("cleanup VFS");
        cleanup_vfs.destroy_s3().expect("S3 cleanup failed");
        eprintln!("done");
    }

    println!("Done.");
}
