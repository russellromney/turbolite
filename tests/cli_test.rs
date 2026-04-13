//! Integration tests for the turbolite CLI binary.
//!
//! These tests invoke the compiled binary and check output/exit codes.
//! They test local-mode commands (no S3 required).

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

/// Create a turbolite database by piping SQL through the shell command.
/// Returns the db path.
fn create_turbolite_db(dir: &std::path::Path, name: &str) -> PathBuf {
    let db_path = dir.join(name);
    let cache_dir = dir.join(format!("{}-cache", name));

    let output = Command::new(turbolite_bin())
        .args([
            "shell",
            "--db",
            db_path.to_str().expect("path"),
            "--cache-dir",
            cache_dir.to_str().expect("cache path"),
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            if let Some(ref mut stdin) = child.stdin {
                stdin.write_all(
                    b"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);\n\
                      INSERT INTO users VALUES (1, 'alice', 'alice@example.com');\n\
                      INSERT INTO users VALUES (2, 'bob', 'bob@example.com');\n\
                      INSERT INTO users VALUES (3, 'charlie', 'charlie@example.com');\n\
                      CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);\n\
                      INSERT INTO posts VALUES (1, 1, 'Hello World');\n\
                      INSERT INTO posts VALUES (2, 2, 'Testing');\n\
                      .quit\n",
                )?;
            }
            child.wait_with_output()
        })
        .expect("failed to create test db via shell");

    assert!(
        output.status.success(),
        "failed to create test db. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    db_path
}

// ── version ────────────────────────────────────────────────────────

#[test]
fn test_version_subcommand() {
    build_bin();
    let output = Command::new(turbolite_bin())
        .arg("version")
        .output()
        .expect("failed to run turbolite version");

    assert!(output.status.success(), "exit code was not 0");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("turbolite"),
        "expected 'turbolite' in output, got: {}",
        stdout
    );
    assert!(
        stdout.contains('.'),
        "expected version number with '.', got: {}",
        stdout
    );
}

#[test]
fn test_version_flag() {
    build_bin();
    let output = Command::new(turbolite_bin())
        .arg("--version")
        .output()
        .expect("failed to run turbolite --version");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("turbolite"));
}

// ── help ───────────────────────────────────────────────────────────

#[test]
fn test_help_flag() {
    build_bin();
    let output = Command::new(turbolite_bin())
        .arg("--help")
        .output()
        .expect("failed to run turbolite --help");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("info"), "help should mention info command");
    assert!(stdout.contains("shell"), "help should mention shell command");
    assert!(stdout.contains("gc"), "help should mention gc command");
    assert!(stdout.contains("export"), "help should mention export command");
    assert!(stdout.contains("import"), "help should mention import command");
    assert!(
        stdout.contains("checkpoint"),
        "help should mention checkpoint command"
    );
    assert!(
        stdout.contains("download"),
        "help should mention download command"
    );
}

// ── info (local mode) ──────────────────────────────────────────────

#[test]
fn test_info_local() {
    build_bin();
    let tmpdir = tempfile::tempdir().expect("failed to create tmpdir");
    let db_path = create_turbolite_db(tmpdir.path(), "info.db");
    let cache_dir = tmpdir.path().join("info.db-cache");

    let output = Command::new(turbolite_bin())
        .args([
            "info",
            "--db",
            db_path.to_str().expect("path"),
            "--cache-dir",
            cache_dir.to_str().expect("path"),
        ])
        .output()
        .expect("failed to run turbolite info");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "info failed. stdout: {}\nstderr: {}",
        stdout,
        stderr
    );
    assert!(stdout.contains("pages:"), "should show page count");
    assert!(stdout.contains("page size:"), "should show page size");
    assert!(stdout.contains("local"), "should show storage type as local");
    assert!(
        stdout.contains("manifest version:"),
        "should show manifest version"
    );
}

// ── export ─────────────────────────────────────────────────────────

#[test]
fn test_export_local() {
    build_bin();
    let tmpdir = tempfile::tempdir().expect("failed to create tmpdir");
    let db_path = create_turbolite_db(tmpdir.path(), "export-src.db");
    let cache_dir = tmpdir.path().join("export-src.db-cache");
    let export_dir = tempfile::tempdir().expect("export tmpdir");
    let output_path = export_dir.path().join("exported.db");

    let output = Command::new(turbolite_bin())
        .args([
            "export",
            "--db",
            db_path.to_str().expect("path"),
            "--output",
            output_path.to_str().expect("path"),
            "--cache-dir",
            cache_dir.to_str().expect("path"),
        ])
        .output()
        .expect("failed to run turbolite export");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "export failed. stdout: {}\nstderr: {}",
        stdout,
        stderr
    );
    assert!(stdout.contains("integrity ok"), "should pass integrity check");

    // Verify the exported file has correct data (plain SQLite, no VFS needed)
    let conn =
        rusqlite::Connection::open(&output_path).expect("open exported db");
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM users", [], |row| row.get(0))
        .expect("query exported db");
    assert_eq!(count, 3, "exported db should have 3 users");

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM posts", [], |row| row.get(0))
        .expect("query exported db");
    assert_eq!(count, 2, "exported db should have 2 posts");
}

// ── shell (non-interactive, piped input) ───────────────────────────

#[test]
fn test_shell_piped_query() {
    build_bin();
    let tmpdir = tempfile::tempdir().expect("failed to create tmpdir");
    let db_path = create_turbolite_db(tmpdir.path(), "shell-query.db");
    let cache_dir = tmpdir.path().join("shell-query.db-cache");

    let output = Command::new(turbolite_bin())
        .args([
            "shell",
            "--db",
            db_path.to_str().expect("path"),
            "--cache-dir",
            cache_dir.to_str().expect("path"),
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            if let Some(ref mut stdin) = child.stdin {
                stdin.write_all(b"SELECT name FROM users ORDER BY id;\n.quit\n")?;
            }
            child.wait_with_output()
        })
        .expect("failed to run turbolite shell");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "shell failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(stdout.contains("alice"), "should contain alice");
    assert!(stdout.contains("bob"), "should contain bob");
    assert!(stdout.contains("charlie"), "should contain charlie");
}

#[test]
fn test_shell_dot_tables() {
    build_bin();
    let tmpdir = tempfile::tempdir().expect("failed to create tmpdir");
    let db_path = create_turbolite_db(tmpdir.path(), "tables.db");
    let cache_dir = tmpdir.path().join("tables.db-cache");

    let output = Command::new(turbolite_bin())
        .args([
            "shell",
            "--db",
            db_path.to_str().expect("path"),
            "--cache-dir",
            cache_dir.to_str().expect("path"),
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            if let Some(ref mut stdin) = child.stdin {
                stdin.write_all(b".tables\n.quit\n")?;
            }
            child.wait_with_output()
        })
        .expect("failed to run turbolite shell");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success());
    assert!(stdout.contains("users"), "should list users table");
    assert!(stdout.contains("posts"), "should list posts table");
}

#[test]
fn test_shell_dot_schema() {
    build_bin();
    let tmpdir = tempfile::tempdir().expect("failed to create tmpdir");
    let db_path = create_turbolite_db(tmpdir.path(), "schema.db");
    let cache_dir = tmpdir.path().join("schema.db-cache");

    let output = Command::new(turbolite_bin())
        .args([
            "shell",
            "--db",
            db_path.to_str().expect("path"),
            "--cache-dir",
            cache_dir.to_str().expect("path"),
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            if let Some(ref mut stdin) = child.stdin {
                stdin.write_all(b".schema\n.quit\n")?;
            }
            child.wait_with_output()
        })
        .expect("failed to run turbolite shell");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success());
    assert!(
        stdout.contains("CREATE TABLE"),
        "should show CREATE TABLE statements"
    );
}

#[test]
fn test_shell_write_and_read() {
    build_bin();
    let tmpdir = tempfile::tempdir().expect("failed to create tmpdir");
    let db_path = create_turbolite_db(tmpdir.path(), "write.db");
    let cache_dir = tmpdir.path().join("write.db-cache");

    let output = Command::new(turbolite_bin())
        .args([
            "shell",
            "--db",
            db_path.to_str().expect("path"),
            "--cache-dir",
            cache_dir.to_str().expect("path"),
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            if let Some(ref mut stdin) = child.stdin {
                stdin.write_all(
                    b"INSERT INTO users VALUES (4, 'dave', 'dave@example.com');\nSELECT COUNT(*) FROM users;\n.quit\n",
                )?;
            }
            child.wait_with_output()
        })
        .expect("failed to run turbolite shell");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success());
    // After inserting dave, count should be 4
    assert!(stdout.contains("4"), "should show count of 4 after insert");
}

// ── missing args ───────────────────────────────────────────────────

#[test]
fn test_info_missing_db_arg() {
    build_bin();
    let output = Command::new(turbolite_bin())
        .arg("info")
        .output()
        .expect("failed to run turbolite info");

    assert!(!output.status.success(), "should fail without --db");
}

#[test]
fn test_gc_missing_bucket() {
    build_bin();
    let tmpdir = tempfile::tempdir().expect("failed to create tmpdir");
    let db_path = tmpdir.path().join("gc.db");

    // gc requires --bucket, should fail without it
    let output = Command::new(turbolite_bin())
        .args(["gc", "--db", db_path.to_str().expect("path")])
        .output()
        .expect("failed to run turbolite gc");

    assert!(!output.status.success(), "gc should fail without --bucket");
}

// ── no subcommand shows error ──────────────────────────────────────

#[test]
fn test_no_subcommand_shows_error() {
    build_bin();
    let output = Command::new(turbolite_bin())
        .output()
        .expect("failed to run turbolite");

    assert!(!output.status.success(), "should fail with no subcommand");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Usage") || stderr.contains("turbolite"),
        "should show usage info"
    );
}

// ── export nonexistent db ──────────────────────────────────────────

#[test]
fn test_export_nonexistent_db() {
    build_bin();
    let tmpdir = tempfile::tempdir().expect("failed to create tmpdir");
    let output_path = tmpdir.path().join("exported.db");
    let cache_dir = tmpdir.path().join("cache");

    let output = Command::new(turbolite_bin())
        .args([
            "export",
            "--db",
            "/nonexistent/path/db.sqlite",
            "--output",
            output_path.to_str().expect("path"),
            "--cache-dir",
            cache_dir.to_str().expect("path"),
        ])
        .output()
        .expect("failed to run turbolite export");

    // Should fail (the VFS will create a new empty db, then VACUUM INTO will produce
    // a valid but empty file, or it may fail depending on the path). Either way,
    // verify the binary doesn't crash.
    // The actual behavior depends on whether the path is writable.
    // For a truly nonexistent parent dir, it should fail.
    assert!(
        !output.status.success(),
        "should fail for nonexistent db path"
    );
}
