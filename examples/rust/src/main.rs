//! turbolite basic example — Rust (native API)
//!
//! A small CLI that stores and queries books in a compressed SQLite database.
//!
//! Run:
//!   cd examples/rust && cargo run

use rusqlite::{params, Connection, OpenFlags};
use turbolite::{register, CompressedVfs};
use tempfile::TempDir;

fn main() {
    let data_dir = TempDir::new().expect("create temp dir");

    // Register a zstd-compressed VFS (level 3)
    let vfs = CompressedVfs::new(data_dir.path(), 3);
    register("demo", vfs).expect("register VFS");

    // Open a database through the VFS
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE;
    let db_path = data_dir.path().join("books.db");
    let conn =
        Connection::open_with_flags_and_vfs(&db_path, flags, "demo").expect("open database");

    // Create and populate
    conn.execute_batch(
        "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT NOT NULL, year INTEGER);
         INSERT INTO books VALUES (1, 'Dune', 1965);
         INSERT INTO books VALUES (2, 'Neuromancer', 1984);
         INSERT INTO books VALUES (3, 'Snow Crash', 1992);",
    )
    .expect("create and insert");

    // Query
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

    // Filtered query
    let recent: Vec<String> = conn
        .prepare("SELECT title FROM books WHERE year > ?1")
        .expect("prepare")
        .query_map(params![1980], |row| row.get(0))
        .expect("query")
        .collect::<Result<_, _>>()
        .expect("collect");

    println!("\nBooks after 1980: {recent:?}");
}
