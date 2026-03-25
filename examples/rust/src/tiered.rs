//! turbolite example — Rust S3 tiered (native API)
//!
//! A small CLI that stores and queries books in an S3 tiered SQLite database.
//! Pages are compressed locally and synced to S3 for durability.
//!
//! Run:
//!   TURBOLITE_BUCKET=my-bucket cargo run --bin tiered
//!
//! Required:
//!   TURBOLITE_BUCKET          S3 bucket name
//!
//! Optional:
//!   TURBOLITE_ENDPOINT_URL    S3 endpoint (Tigris, MinIO, etc.)
//!   TURBOLITE_REGION          AWS region
//!   AWS_ACCESS_KEY_ID         S3 credentials
//!   AWS_SECRET_ACCESS_KEY     S3 credentials

use rusqlite::{params, Connection, OpenFlags};
use turbolite::tiered::{register, TieredConfig, TieredVfs};
use tempfile::TempDir;

fn main() {
    let bucket = std::env::var("TURBOLITE_BUCKET")
        .expect("TURBOLITE_BUCKET is required");

    let cache_dir = TempDir::new().expect("create temp dir");

    let config = TieredConfig {
        bucket,
        prefix: "books".to_string(),
        cache_dir: cache_dir.path().to_path_buf(),
        endpoint_url: std::env::var("TURBOLITE_ENDPOINT_URL").ok(),
        region: std::env::var("TURBOLITE_REGION").ok(),
        ..Default::default()
    };

    let vfs = TieredVfs::new(config).expect("create tiered VFS");
    register("tiered", vfs).expect("register tiered VFS");

    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE;
    let db_path = cache_dir.path().join("books.db");
    let conn =
        Connection::open_with_flags_and_vfs(&db_path, flags, "tiered").expect("open database");

    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS books (id INTEGER PRIMARY KEY, title TEXT NOT NULL, year INTEGER);
         INSERT INTO books VALUES (1, 'Dune', 1965);
         INSERT INTO books VALUES (2, 'Neuromancer', 1984);
         INSERT INTO books VALUES (3, 'Snow Crash', 1992);",
    )
    .expect("create and insert");

    let mut stmt = conn
        .prepare("SELECT id, title, year FROM books ORDER BY year")
        .expect("prepare");

    let books: Vec<(i64, String, i64)> = stmt
        .query_map(params![], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .expect("query")
        .collect::<Result<_, _>>()
        .expect("collect");

    for (id, title, year) in &books {
        println!("  {id}. {title} ({year})");
    }

    let recent: Vec<String> = conn
        .prepare("SELECT title FROM books WHERE year > ?1")
        .expect("prepare")
        .query_map(params![1980], |row| row.get(0))
        .expect("query")
        .collect::<Result<_, _>>()
        .expect("collect");

    println!("\nBooks after 1980: {recent:?}");
}
