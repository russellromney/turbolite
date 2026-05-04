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
//!   cargo test --test cli_s3_test --features cli-s3,zstd
//! ```

#![cfg(feature = "cli-s3")]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Once;

fn turbolite_bin() -> PathBuf {
    let mut path = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            path.push("target");
            path
        });
    path.push("debug");
    path.push("turbolite");
    path
}

fn build_bin() {
    static BUILD: Once = Once::new();
    BUILD.call_once(|| {
        let status = Command::new("cargo")
            .args(["build", "--bin", "turbolite", "--features", "cli-s3,zstd"])
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .status()
            .expect("failed to build turbolite binary");
        assert!(status.success(), "cargo build failed");
    });
}

fn test_bucket() -> String {
    std::env::var("TIERED_TEST_BUCKET")
        .expect("TIERED_TEST_BUCKET env var required for S3 CLI tests")
}

fn endpoint_url() -> Option<String> {
    std::env::var("AWS_ENDPOINT_URL")
        .ok()
        .filter(|value| !value.is_empty())
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

/// Create a user-shaped SQLite database for import testing.
///
/// Negative control: replacing this with the old three-row fixture makes this
/// test much weaker; it no longer forces multi-page blobs, page growth,
/// secondary indexes, deletes, ANALYZE, or VACUUM through the S3 path.
fn create_plain_db(dir: &Path, name: &str) -> PathBuf {
    let db_path = dir.join(name);
    let conn = rusqlite::Connection::open(&db_path).expect("create plain db");
    conn.execute_batch(
        "PRAGMA page_size=4096;
         PRAGMA journal_mode=DELETE;
         PRAGMA foreign_keys=ON;",
    )
    .expect("set page size");
    conn.execute_batch(
        "CREATE TABLE users (
             id INTEGER PRIMARY KEY,
             name TEXT NOT NULL,
             email TEXT NOT NULL,
             row_version INTEGER NOT NULL
         );
         CREATE TABLE posts (
             id INTEGER PRIMARY KEY,
             user_id INTEGER NOT NULL REFERENCES users(id),
             title TEXT NOT NULL,
             body TEXT NOT NULL,
             payload BLOB NOT NULL,
             row_version INTEGER NOT NULL
         );
         CREATE TABLE events (
             id INTEGER PRIMARY KEY,
             post_id INTEGER NOT NULL REFERENCES posts(id),
             kind TEXT NOT NULL,
             meta TEXT NOT NULL,
             row_version INTEGER NOT NULL
         );
         CREATE INDEX idx_users_email ON users(email);
         CREATE INDEX idx_posts_user ON posts(user_id);
         CREATE INDEX idx_events_post_kind ON events(post_id, kind);
         BEGIN IMMEDIATE;",
    )
    .expect("create canonical schema");

    for i in 1..=128 {
        conn.execute(
            "INSERT INTO users VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![
                i,
                format!("user_{i:04}"),
                format!("user_{i:04}@example.com"),
                1_i64
            ],
        )
        .expect("insert user");
    }

    for i in 1..=768 {
        let payload = vec![(i % 251) as u8; 1024 + ((i % 7) as usize * 31)];
        conn.execute(
            "INSERT INTO posts VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                i,
                ((i - 1) % 128) + 1,
                format!("post_{i:04}"),
                format!("body for post {i:04} with enough text to span real pages"),
                payload,
                1_i64
            ],
        )
        .expect("insert post");
    }

    for i in 1..=512 {
        conn.execute(
            "INSERT INTO events VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                i,
                ((i - 1) % 768) + 1,
                if i % 2 == 0 { "view" } else { "edit" },
                format!(r#"{{"event":{i},"bucket":{}}}"#, i % 13),
                1_i64
            ],
        )
        .expect("insert event");
    }

    conn.execute_batch(
        "COMMIT;
         UPDATE users SET row_version = row_version + 10 WHERE id % 17 = 0;
         UPDATE posts SET row_version = row_version + 3, body = body || ' updated'
           WHERE id % 19 = 0;
         DELETE FROM events WHERE id % 11 = 0;",
    )
    .expect("mutate canonical data");

    for i in 10001..=10032 {
        let payload = vec![(i % 199) as u8; 1500];
        conn.execute(
            "INSERT INTO posts VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                i,
                ((i - 10001) % 128) + 1,
                format!("late_post_{i}"),
                "late insert after delete/update churn",
                payload,
                7_i64
            ],
        )
        .expect("insert late post");
    }

    conn.execute_batch("ANALYZE; VACUUM;")
        .expect("analyze and vacuum canonical data");
    assert_eq!(
        conn.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
            .expect("integrity_check"),
        "ok"
    );
    conn.close().expect("close plain db");
    db_path
}

/// SQL version of the canonical workload, driven through the Turbolite VFS.
///
/// Negative control: removing the WAL checkpoints makes this a local-cache
/// test, not a remote persistence test. Reopening from a fresh cache below must
/// prove the manifest and page groups reached object storage.
fn canonical_live_vfs_sql_script() -> String {
    let mut sql = String::new();
    sql.push_str("PRAGMA page_size=4096;\n");
    sql.push_str("PRAGMA journal_mode=WAL;\n");
    sql.push_str("PRAGMA foreign_keys=ON;\n");
    sql.push_str("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL, row_version INTEGER NOT NULL);\n");
    sql.push_str("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id), title TEXT NOT NULL, body TEXT NOT NULL, payload BLOB NOT NULL, row_version INTEGER NOT NULL);\n");
    sql.push_str("CREATE TABLE events (id INTEGER PRIMARY KEY, post_id INTEGER NOT NULL REFERENCES posts(id), kind TEXT NOT NULL, meta TEXT NOT NULL, row_version INTEGER NOT NULL);\n");
    sql.push_str("CREATE INDEX idx_users_email ON users(email);\n");
    sql.push_str("CREATE INDEX idx_posts_user ON posts(user_id);\n");
    sql.push_str("CREATE INDEX idx_events_post_kind ON events(post_id, kind);\n");
    sql.push_str("BEGIN IMMEDIATE;\n");

    for i in 1..=128 {
        sql.push_str(&format!(
            "INSERT INTO users VALUES ({i}, 'user_{i:04}', 'user_{i:04}@example.com', 1);\n"
        ));
    }

    for i in 1..=768 {
        let user_id = ((i - 1) % 128) + 1;
        let payload_len = 1024 + ((i % 7) * 31);
        sql.push_str(&format!(
            "INSERT INTO posts VALUES ({i}, {user_id}, 'post_{i:04}', 'body for post {i:04} with enough text to span real pages', zeroblob({payload_len}), 1);\n"
        ));
    }

    for i in 1..=512 {
        let post_id = ((i - 1) % 768) + 1;
        let kind = if i % 2 == 0 { "view" } else { "edit" };
        let bucket = i % 13;
        sql.push_str(&format!(
            "INSERT INTO events VALUES ({i}, {post_id}, '{kind}', '{{\"event\":{i},\"bucket\":{bucket}}}', 1);\n"
        ));
    }

    sql.push_str("COMMIT;\n");
    sql.push_str("PRAGMA wal_checkpoint(TRUNCATE);\n");
    sql.push_str("UPDATE users SET row_version = row_version + 10 WHERE id % 17 = 0;\n");
    sql.push_str(
        "UPDATE posts SET row_version = row_version + 3, body = body || ' updated' WHERE id % 19 = 0;\n",
    );
    sql.push_str("DELETE FROM events WHERE id % 11 = 0;\n");

    for i in 10001..=10032 {
        let user_id = ((i - 10001) % 128) + 1;
        sql.push_str(&format!(
            "INSERT INTO posts VALUES ({i}, {user_id}, 'late_post_{i}', 'late insert after delete/update churn', zeroblob(1500), 7);\n"
        ));
    }

    sql.push_str("ANALYZE;\n");
    sql.push_str("PRAGMA wal_checkpoint(TRUNCATE);\n");
    sql.push_str("PRAGMA integrity_check;\n");
    sql.push_str(".quit\n");
    sql
}

fn canonical_checksum(conn: &rusqlite::Connection) -> String {
    assert_eq!(
        conn.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
            .expect("integrity_check"),
        "ok"
    );
    let mut fk_stmt = conn
        .prepare("PRAGMA foreign_key_check")
        .expect("prepare foreign_key_check");
    let mut fk_rows = fk_stmt.query([]).expect("run foreign_key_check");
    let mut fk_violations = 0;
    while fk_rows.next().expect("read foreign_key_check").is_some() {
        fk_violations += 1;
    }
    assert_eq!(fk_violations, 0, "foreign_key_check should be clean");

    let users = conn
        .query_row(
            "SELECT COUNT(*), SUM(id), SUM(row_version), SUM(length(name) + length(email)) FROM users",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .expect("users checksum");
    let posts = conn
        .query_row(
            "SELECT COUNT(*), SUM(id), SUM(user_id), SUM(row_version), SUM(length(body)), SUM(length(payload)) FROM posts",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            },
        )
        .expect("posts checksum");
    let events = conn
        .query_row(
            "SELECT COUNT(*), SUM(id), SUM(post_id), SUM(row_version), SUM(length(kind) + length(meta)) FROM events",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )
        .expect("events checksum");
    let indexes = conn
        .query_row(
            "SELECT COUNT(*), SUM(length(name)) FROM sqlite_master
             WHERE type='index'
               AND name IN ('idx_users_email', 'idx_posts_user', 'idx_events_post_kind')",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .expect("index checksum");
    assert_eq!(
        indexes.0, 3,
        "canonical fixture should preserve all secondary indexes"
    );

    format!("users={users:?};posts={posts:?};events={events:?};indexes={indexes:?}")
}

fn checksum_for_db(path: &Path) -> String {
    let conn = rusqlite::Connection::open(path).expect("open db for checksum");
    canonical_checksum(&conn)
}

fn s3_args(base: &[&str], endpoint: &Option<String>) -> Vec<String> {
    let mut args: Vec<String> = base.iter().map(|arg| (*arg).to_string()).collect();
    if let Some(endpoint) = endpoint {
        args.push("--endpoint".to_string());
        args.push(endpoint.clone());
    }
    args
}

/// Run a CLI command with S3 env vars inherited from the test process.
fn run_cli(args: &[String]) -> std::process::Output {
    Command::new(turbolite_bin())
        .args(args)
        .output()
        .expect("failed to run turbolite CLI")
}

fn run_shell(args: &[String], script: &str) -> std::process::Output {
    Command::new(turbolite_bin())
        .args(args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            if let Some(ref mut stdin) = child.stdin {
                stdin.write_all(script.as_bytes())?;
            }
            child.wait_with_output()
        })
        .expect("failed to run turbolite shell")
}

// ── import -> info -> export roundtrip ─────────────────────────────

#[test]
fn test_import_info_export_roundtrip() {
    build_bin();

    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let plain_db = create_plain_db(tmpdir.path(), "source.db");
    let source_checksum = checksum_for_db(&plain_db);
    let bucket = test_bucket();
    let endpoint = endpoint_url();
    let prefix = unique_prefix();

    // 1. Import plain SQLite to Tigris
    let output = run_cli(&s3_args(
        &[
            "import",
            "--input",
            plain_db.to_str().expect("path"),
            "--bucket",
            &bucket,
            "--prefix",
            &prefix,
        ],
        &endpoint,
    ));

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
    let output = run_cli(&s3_args(
        &[
            "info",
            "--db",
            db_path.to_str().expect("path"),
            "--bucket",
            &bucket,
            "--prefix",
            &prefix,
            "--cache-dir",
            cache_dir.to_str().expect("path"),
        ],
        &endpoint,
    ));

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

    let output = run_cli(&s3_args(
        &[
            "export",
            "--db",
            export_db_path.to_str().expect("path"),
            "--output",
            exported_db.to_str().expect("path"),
            "--bucket",
            &bucket,
            "--prefix",
            &prefix,
            "--cache-dir",
            export_cache.to_str().expect("path"),
        ],
        &endpoint,
    ));

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "export failed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );
    assert!(
        stdout.contains("integrity ok"),
        "should pass integrity check"
    );

    // 4. Verify exported data matches original
    let conn = rusqlite::Connection::open(&exported_db).expect("open exported db");
    assert_eq!(
        canonical_checksum(&conn),
        source_checksum,
        "exported database checksum should match source"
    );

    let user_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM users", [], |row| row.get(0))
        .expect("count users");
    assert_eq!(user_count, 128, "should have 128 users");

    let post_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM posts", [], |row| row.get(0))
        .expect("count posts");
    assert_eq!(post_count, 800, "should have 800 posts");

    let user_email: String = conn
        .query_row(
            "SELECT email FROM users WHERE name = 'user_0042'",
            [],
            |row| row.get(0),
        )
        .expect("query user");
    assert_eq!(user_email, "user_0042@example.com");

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

// ── live VFS write -> checkpoint -> cold export from S3 ────────────

#[test]
fn test_live_vfs_write_checkpoint_reopen_export_on_s3() {
    build_bin();

    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let plain_db = create_plain_db(tmpdir.path(), "expected-live.db");
    let source_checksum = checksum_for_db(&plain_db);
    let bucket = test_bucket();
    let endpoint = endpoint_url();
    let prefix = unique_prefix();

    let write_cache = tmpdir.path().join("live-write-cache");
    let write_db = tmpdir.path().join("live-write.db");
    let write_args = s3_args(
        &[
            "shell",
            "--db",
            write_db.to_str().expect("path"),
            "--bucket",
            &bucket,
            "--prefix",
            &prefix,
            "--cache-dir",
            write_cache.to_str().expect("path"),
        ],
        &endpoint,
    );
    let output = run_shell(&write_args, &canonical_live_vfs_sql_script());

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "live VFS shell write failed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );
    assert!(stdout.contains("ok"), "integrity_check should be ok");

    let info_cache = tmpdir.path().join("live-cold-info-cache");
    let info_db = tmpdir.path().join("live-cold-info.db");
    let output = run_cli(&s3_args(
        &[
            "info",
            "--db",
            info_db.to_str().expect("path"),
            "--bucket",
            &bucket,
            "--prefix",
            &prefix,
            "--cache-dir",
            info_cache.to_str().expect("path"),
        ],
        &endpoint,
    ));
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "cold info failed after live VFS write.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );
    assert!(
        stdout.contains("page groups:"),
        "cold info should see groups"
    );

    let exported_db = tmpdir.path().join("live-exported.db");
    let export_cache = tmpdir.path().join("live-cold-export-cache");
    let export_db_path = tmpdir.path().join("live-export.db");
    let output = run_cli(&s3_args(
        &[
            "export",
            "--db",
            export_db_path.to_str().expect("path"),
            "--output",
            exported_db.to_str().expect("path"),
            "--bucket",
            &bucket,
            "--prefix",
            &prefix,
            "--cache-dir",
            export_cache.to_str().expect("path"),
        ],
        &endpoint,
    ));
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "cold export failed after live VFS write.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );
    assert!(
        stdout.contains("integrity ok"),
        "cold export should pass integrity"
    );

    assert_eq!(
        checksum_for_db(&exported_db),
        source_checksum,
        "live VFS write/checkpoint/cold-export checksum should match the canonical plain DB"
    );
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
    let output = run_cli(&s3_args(
        &[
            "import",
            "--input",
            plain_db.to_str().expect("path"),
            "--bucket",
            &bucket,
            "--prefix",
            &prefix,
        ],
        &endpoint,
    ));
    assert!(output.status.success(), "import failed for shell test");

    // Query via shell
    let cache_dir = tmpdir.path().join("shell-cache");
    let db_path = tmpdir.path().join("shell.db");

    let args = s3_args(
        &[
            "shell",
            "--db",
            db_path.to_str().expect("path"),
            "--bucket",
            &bucket,
            "--prefix",
            &prefix,
            "--cache-dir",
            cache_dir.to_str().expect("path"),
            "--read-only",
        ],
        &endpoint,
    );
    let output = Command::new(turbolite_bin())
        .args(&args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            if let Some(ref mut stdin) = child.stdin {
                stdin.write_all(
                    b"SELECT name FROM users WHERE id = 42;\nSELECT COUNT(*) FROM users;\nSELECT COUNT(*) FROM posts;\nSELECT COUNT(*) FROM events;\n.quit\n",
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
    assert!(stdout.contains("user_0042"), "should contain user_0042");
    assert!(stdout.contains("128"), "should show user count of 128");
    assert!(stdout.contains("800"), "should show post count of 800");
    assert!(stdout.contains("466"), "should show event count of 466");
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
    let output = run_cli(&s3_args(
        &[
            "import",
            "--input",
            plain_db.to_str().expect("path"),
            "--bucket",
            &bucket,
            "--prefix",
            &prefix,
        ],
        &endpoint,
    ));
    assert!(output.status.success(), "import failed for validate test");

    // Validate
    let cache_dir = tmpdir.path().join("validate-cache");
    let db_path = tmpdir.path().join("validate.db");

    let output = run_cli(&s3_args(
        &[
            "validate",
            "--db",
            db_path.to_str().expect("path"),
            "--bucket",
            &bucket,
            "--prefix",
            &prefix,
            "--cache-dir",
            cache_dir.to_str().expect("path"),
        ],
        &endpoint,
    ));

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "validate failed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );
    assert!(
        stdout.contains("validate: passed"),
        "should pass validation"
    );
    assert!(stdout.contains("present"), "should show present counts");
    assert!(
        stdout.contains("integrity_check"),
        "should run integrity check"
    );
    assert!(stdout.contains("ok"), "integrity check should be ok");
}

// ── download from S3 ───────────────────────────────────────────────

#[test]
fn test_download_s3_database() {
    build_bin();

    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let plain_db = create_plain_db(tmpdir.path(), "download-source.db");
    let bucket = test_bucket();
    let endpoint = endpoint_url();
    let prefix = unique_prefix();

    // Import first
    let output = run_cli(&s3_args(
        &[
            "import",
            "--input",
            plain_db.to_str().expect("path"),
            "--bucket",
            &bucket,
            "--prefix",
            &prefix,
        ],
        &endpoint,
    ));
    assert!(output.status.success(), "import failed for download test");

    // Download entire database to local cache
    let cache_dir = tmpdir.path().join("download-cache");
    let db_path = tmpdir.path().join("download.db");

    let output = run_cli(&s3_args(
        &[
            "download",
            "--db",
            db_path.to_str().expect("path"),
            "--bucket",
            &bucket,
            "--prefix",
            &prefix,
            "--cache-dir",
            cache_dir.to_str().expect("path"),
        ],
        &endpoint,
    ));

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "download failed.\nstdout: {}\nstderr: {}",
        stdout,
        stderr,
    );
    assert!(stdout.contains("download:"), "should show download summary");
    assert!(stdout.contains("tables"), "should mention tables");
    assert!(stdout.contains("groups"), "should mention groups");

    // Verify the data is queryable after download (should be cached locally)
    let args = s3_args(
        &[
            "shell",
            "--db",
            db_path.to_str().expect("path"),
            "--bucket",
            &bucket,
            "--prefix",
            &prefix,
            "--cache-dir",
            cache_dir.to_str().expect("path"),
            "--read-only",
        ],
        &endpoint,
    );
    let output = Command::new(turbolite_bin())
        .args(&args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            if let Some(ref mut stdin) = child.stdin {
                stdin.write_all(
                    b"SELECT COUNT(*) FROM users;\nSELECT COUNT(*) FROM posts;\n.quit\n",
                )?;
            }
            child.wait_with_output()
        })
        .expect("failed to run shell after download");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("128") && stdout.contains("800"),
        "should have canonical row counts after download.\nstdout: {}",
        stdout,
    );
}
