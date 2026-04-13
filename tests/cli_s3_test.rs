//! CLI integration tests against Tigris (S3-compatible).
//!
//! Requires S3 credentials. Run with:
//!
//! ```bash
//! soup run --project ladybug --env development -- \
//!   TIERED_TEST_BUCKET=sqlces-test \
//!   AWS_ACCESS_KEY_ID=$TIGRIS_STORAGE_ACCESS_KEY_ID \
//!   AWS_SECRET_ACCESS_KEY=$TIGRIS_STORAGE_SECRET_ACCESS_KEY \
//!   AWS_ENDPOINT_URL=$TIGRIS_STORAGE_ENDPOINT \
//!   cargo test --test cli_s3_test --features cloud,zstd
//! ```

#![cfg(feature = "cloud")]

use std::path::PathBuf;
use std::process::Command;

fn turbolite_bin() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("target");
    path.push("debug");
    path.push("turbolite");
    path
}

fn build_bin() {
    let status = Command::new("cargo")
        .args(["build", "--bin", "turbolite", "--features", "cloud,zstd"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()
        .expect("failed to build turbolite binary");
    assert!(status.success(), "cargo build failed");
}

fn test_bucket() -> String {
    std::env::var("TIERED_TEST_BUCKET")
        .expect("TIERED_TEST_BUCKET env var required for S3 CLI tests")
}

fn endpoint_url() -> String {
    std::env::var("AWS_ENDPOINT_URL")
        .unwrap_or_else(|_| "https://t3.storage.dev".to_string())
}

fn unique_prefix() -> String {
    format!(
        "test/cli/{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    )
}

/// Create a plain SQLite database for import testing.
fn create_plain_db(dir: &std::path::Path, name: &str) -> PathBuf {
    let db_path = dir.join(name);
    let conn = rusqlite::Connection::open(&db_path).expect("create plain db");
    conn.execute_batch("PRAGMA page_size=65536; PRAGMA journal_mode=DELETE;")
        .expect("set page size");
    conn.execute_batch(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);
         INSERT INTO users VALUES (1, 'alice', 'alice@example.com');
         INSERT INTO users VALUES (2, 'bob', 'bob@example.com');
         INSERT INTO users VALUES (3, 'charlie', 'charlie@example.com');
         CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT, body TEXT);
         INSERT INTO posts VALUES (1, 1, 'Hello World', 'First post content');
         INSERT INTO posts VALUES (2, 2, 'Testing', 'Test post body');
         INSERT INTO posts VALUES (3, 1, 'Another', 'More content here');
         CREATE INDEX idx_posts_user ON posts(user_id);",
    )
    .expect("populate plain db");
    conn.close().expect("close plain db");
    db_path
}

/// Run a CLI command with S3 env vars inherited from the test process.
fn run_cli(args: &[&str]) -> std::process::Output {
    Command::new(turbolite_bin())
        .args(args)
        .output()
        .expect("failed to run turbolite CLI")
}

// ── import -> info -> export roundtrip ─────────────────────────────

#[test]
fn test_import_info_export_roundtrip() {
    build_bin();

    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let plain_db = create_plain_db(tmpdir.path(), "source.db");
    let bucket = test_bucket();
    let endpoint = endpoint_url();
    let prefix = unique_prefix();

    // 1. Import plain SQLite to Tigris
    let output = run_cli(&[
        "import",
        "--input",
        plain_db.to_str().expect("path"),
        "--bucket",
        &bucket,
        "--prefix",
        &prefix,
        "--endpoint",
        &endpoint,
    ]);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "import failed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );
    assert!(stdout.contains("import:"), "should print import summary");
    assert!(stdout.contains("pages"), "should mention page count");
    assert!(stdout.contains("groups"), "should mention group count");

    // 2. Info on the imported database
    let cache_dir = tmpdir.path().join("info-cache");
    let db_path = tmpdir.path().join("info.db");
    let output = run_cli(&[
        "info",
        "--db",
        db_path.to_str().expect("path"),
        "--bucket",
        &bucket,
        "--prefix",
        &prefix,
        "--endpoint",
        &endpoint,
        "--cache-dir",
        cache_dir.to_str().expect("path"),
    ]);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "info failed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );
    assert!(stdout.contains("S3"), "should show S3 storage");
    assert!(stdout.contains("pages:"), "should show page count");
    assert!(stdout.contains("page groups:"), "should show group count");

    // 3. Export back to plain SQLite
    let export_dir = tempfile::tempdir().expect("export tmpdir");
    let exported_db = export_dir.path().join("exported.db");
    let export_cache = tmpdir.path().join("export-cache");
    let export_db_path = tmpdir.path().join("export.db");

    let output = run_cli(&[
        "export",
        "--db",
        export_db_path.to_str().expect("path"),
        "--output",
        exported_db.to_str().expect("path"),
        "--bucket",
        &bucket,
        "--prefix",
        &prefix,
        "--endpoint",
        &endpoint,
        "--cache-dir",
        export_cache.to_str().expect("path"),
    ]);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "export failed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );
    assert!(stdout.contains("integrity ok"), "should pass integrity check");

    // 4. Verify exported data matches original
    let conn = rusqlite::Connection::open(&exported_db).expect("open exported db");

    let user_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM users", [], |row| row.get(0))
        .expect("count users");
    assert_eq!(user_count, 3, "should have 3 users");

    let post_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM posts", [], |row| row.get(0))
        .expect("count posts");
    assert_eq!(post_count, 3, "should have 3 posts");

    let alice_email: String = conn
        .query_row(
            "SELECT email FROM users WHERE name = 'alice'",
            [],
            |row| row.get(0),
        )
        .expect("query alice");
    assert_eq!(alice_email, "alice@example.com");

    // Verify index survived roundtrip
    let has_index: bool = conn
        .query_row(
            "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='index' AND name='idx_posts_user'",
            [],
            |row| row.get(0),
        )
        .expect("check index");
    assert!(has_index, "index should survive import/export roundtrip");
}

// ── shell against S3 database ──────────────────────────────────────

#[test]
fn test_shell_s3_query() {
    build_bin();

    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let plain_db = create_plain_db(tmpdir.path(), "shell-source.db");
    let bucket = test_bucket();
    let endpoint = endpoint_url();
    let prefix = unique_prefix();

    // Import first
    let output = run_cli(&[
        "import",
        "--input",
        plain_db.to_str().expect("path"),
        "--bucket",
        &bucket,
        "--prefix",
        &prefix,
        "--endpoint",
        &endpoint,
    ]);
    assert!(output.status.success(), "import failed for shell test");

    // Query via shell
    let cache_dir = tmpdir.path().join("shell-cache");
    let db_path = tmpdir.path().join("shell.db");

    let output = Command::new(turbolite_bin())
        .args([
            "shell",
            "--db",
            db_path.to_str().expect("path"),
            "--bucket",
            &bucket,
            "--prefix",
            &prefix,
            "--endpoint",
            &endpoint,
            "--cache-dir",
            cache_dir.to_str().expect("path"),
            "--read-only",
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            if let Some(ref mut stdin) = child.stdin {
                stdin.write_all(
                    b"SELECT name FROM users ORDER BY id;\nSELECT COUNT(*) FROM posts;\n.quit\n",
                )?;
            }
            child.wait_with_output()
        })
        .expect("failed to run shell");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "shell failed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );
    assert!(stdout.contains("alice"), "should contain alice");
    assert!(stdout.contains("bob"), "should contain bob");
    assert!(stdout.contains("charlie"), "should contain charlie");
    assert!(stdout.contains("3"), "should show post count of 3");
}

// ── validate against S3 database ───────────────────────────────────

#[test]
fn test_validate_clean_database() {
    build_bin();

    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let plain_db = create_plain_db(tmpdir.path(), "validate-source.db");
    let bucket = test_bucket();
    let endpoint = endpoint_url();
    let prefix = unique_prefix();

    // Import
    let output = run_cli(&[
        "import",
        "--input",
        plain_db.to_str().expect("path"),
        "--bucket",
        &bucket,
        "--prefix",
        &prefix,
        "--endpoint",
        &endpoint,
    ]);
    assert!(output.status.success(), "import failed for validate test");

    // Validate
    let cache_dir = tmpdir.path().join("validate-cache");
    let db_path = tmpdir.path().join("validate.db");

    let output = run_cli(&[
        "validate",
        "--db",
        db_path.to_str().expect("path"),
        "--bucket",
        &bucket,
        "--prefix",
        &prefix,
        "--endpoint",
        &endpoint,
        "--cache-dir",
        cache_dir.to_str().expect("path"),
    ]);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "validate failed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );
    assert!(stdout.contains("validate: passed"), "should pass validation");
    assert!(stdout.contains("present"), "should show present counts");
    assert!(stdout.contains("integrity_check"), "should run integrity check");
    assert!(stdout.contains("ok"), "integrity check should be ok");
}
