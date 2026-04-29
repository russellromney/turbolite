//! Phase Cirrus h2 regression tests: `turbolite_install_config_functions`
//! binds `turbolite_config_set(key, value)` to the calling connection's
//! handle queue via `sqlite3_create_function_v2`'s `pApp`.
//!
//! Before h2 the `turbolite_config_set` scalar was auto-registered at
//! extension load with `pApp = NULL` and pushed into the thread-local
//! top-of-stack handle queue at call time — correct for one connection
//! per thread but silently wrong for multi-connection-per-thread. h2
//! drops the auto-registration, adds `turbolite_install_config_functions`,
//! and has the scalar push into the captured queue instead.
//!
//! This mirrors `turbolite/tests/config_set_per_handle.rs`, which
//! covers the rusqlite-based Rust install. These tests exercise the
//! C-callable `turbolite_install_config_functions` entry point via
//! rusqlite's `Connection::handle()`, so the same coverage carries
//! into the loadable-extension and language-binding paths.

#![cfg(feature = "bundled-sqlite")]

use std::ffi::c_void;
use std::sync::atomic::{AtomicU32, Ordering};

use rusqlite::{Connection, OpenFlags};
use tempfile::TempDir;
use turbolite::tiered::{self, settings, TurboliteConfig, TurboliteVfs};
use turbolite_ffi::turbolite_install_config_functions;

static TEST_COUNTER: AtomicU32 = AtomicU32::new(0);

const SQLITE_OK: i32 = 0;
const SQLITE_MISUSE: i32 = 21;

fn unique_name(prefix: &str) -> String {
    let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}_{}_{}", prefix, std::process::id(), n)
}

fn open_connection(vfs_name: &str, db_file: &str) -> Connection {
    let conn = Connection::open_with_flags_and_vfs(
        &format!("file:{}?vfs={}", db_file, vfs_name),
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        vfs_name,
    )
    .expect("open turbolite connection");
    // Force the VFS xOpen so a TurboliteHandle lives on the thread-local
    // active-handle stack. `install` also does this internally, but
    // bootstrapping here gives every test a known drain point for
    // subsequent queries.
    conn.execute("CREATE TABLE IF NOT EXISTS _bootstrap (x INTEGER)", [])
        .expect("bootstrap table");
    conn
}

/// Call `turbolite_install_config_functions` on the given rusqlite
/// connection via its raw sqlite3 handle.
fn install(conn: &Connection) -> i32 {
    unsafe { turbolite_install_config_functions(conn.handle() as *mut c_void) }
}

#[test]
fn install_and_push_success_on_turbolite_connection() {
    let tmp = TempDir::new().unwrap();
    let vfs_name = unique_name("tf_smoke");

    let vfs = TurboliteVfs::new_local(TurboliteConfig {
        cache_dir: tmp.path().to_path_buf(),
        ..Default::default()
    })
    .expect("vfs");
    tiered::register(&vfs_name, vfs).expect("register");

    let conn = open_connection(&vfs_name, "smoke.db");
    assert_eq!(install(&conn), SQLITE_OK, "install should succeed");

    let rc: i64 = conn
        .query_row(
            "SELECT turbolite_config_set('prefetch_search', '0.5,0.5')",
            [],
            |row| row.get(0),
        )
        .expect("turbolite_config_set call");
    assert_eq!(rc, 0, "push should succeed");

    assert_eq!(
        settings::peek_top_for_key("prefetch_search").as_deref(),
        Some("0.5,0.5"),
        "update should be on this connection's queue"
    );
}

#[test]
fn install_returns_misuse_on_non_turbolite_connection() {
    // In-memory db is not backed by a turbolite VFS, so no handle lives
    // on the thread-local stack after the PRAGMA probe.
    let conn = Connection::open_in_memory().expect("open memory db");
    assert_eq!(
        install(&conn),
        SQLITE_MISUSE,
        "install must surface SQLITE_MISUSE when no turbolite handle is active"
    );
}

#[test]
fn config_set_rejects_bad_input_via_install_path() {
    let tmp = TempDir::new().unwrap();
    let vfs_name = unique_name("tf_bad");

    let vfs = TurboliteVfs::new_local(TurboliteConfig {
        cache_dir: tmp.path().to_path_buf(),
        ..Default::default()
    })
    .expect("vfs");
    tiered::register(&vfs_name, vfs).expect("register");

    let conn = open_connection(&vfs_name, "bad.db");
    assert_eq!(install(&conn), SQLITE_OK);

    let err = conn
        .query_row("SELECT turbolite_config_set('nope', 'x')", [], |row| {
            row.get::<_, i64>(0)
        })
        .expect_err("unknown key should error");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("invalid key or value"),
        "expected invalid-key error, got {msg}"
    );

    let err = conn
        .query_row(
            "SELECT turbolite_config_set('prefetch_search', 'not,numbers')",
            [],
            |row| row.get::<_, i64>(0),
        )
        .expect_err("bad value should error");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("invalid key or value"),
        "expected invalid-value error, got {msg}"
    );
}

/// The h2 correctness proof. Two connections open on the same thread
/// against different turbolite VFSes, each installs `turbolite_config_set`
/// with its own queue captured as `pApp`. Each connection's pushes
/// must land on its own queue — regardless of which was opened last.
#[test]
fn multi_connection_same_thread_routes_per_connection_c_path() {
    let tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();
    let vfs_a = unique_name("tf_multi_a");
    let vfs_b = unique_name("tf_multi_b");

    let a = TurboliteVfs::new_local(TurboliteConfig {
        cache_dir: tmp_a.path().to_path_buf(),
        ..Default::default()
    })
    .expect("vfs a");
    tiered::register(&vfs_a, a).expect("register a");

    let b = TurboliteVfs::new_local(TurboliteConfig {
        cache_dir: tmp_b.path().to_path_buf(),
        ..Default::default()
    })
    .expect("vfs b");
    tiered::register(&vfs_b, b).expect("register b");

    // Open in order; both stay open concurrently on the main thread.
    let conn_a = open_connection(&vfs_a, "multi_a.db");
    assert_eq!(install(&conn_a), SQLITE_OK, "install A");
    let conn_b = open_connection(&vfs_b, "multi_b.db");
    assert_eq!(install(&conn_b), SQLITE_OK, "install B");

    // Thread-local stack is now [queue_a, queue_b] with queue_b on
    // top. Each scalar function's pApp is the queue captured when its
    // connection was installed — A holds queue_a, B holds queue_b.
    let _: i64 = conn_a
        .query_row(
            "SELECT turbolite_config_set('prefetch_search', '0.11,0.22')",
            [],
            |row| row.get(0),
        )
        .expect("A push");

    let _: i64 = conn_b
        .query_row(
            "SELECT turbolite_config_set('prefetch_search', '0.99,0.88')",
            [],
            |row| row.get(0),
        )
        .expect("B push");

    // Drain both queues via a real page read so subsequent peek calls
    // start from a clean slate.
    conn_a
        .execute("INSERT INTO _bootstrap VALUES (1)", [])
        .unwrap();
    conn_b
        .execute("INSERT INTO _bootstrap VALUES (2)", [])
        .unwrap();

    // Push AGAIN on A, without touching B. B's queue (top-of-stack)
    // must not carry A's push — the scalar function captured queue_a
    // at install time and pushes there regardless of stack order.
    let _: i64 = conn_a
        .query_row(
            "SELECT turbolite_config_set('prefetch_search', '0.55,0.44')",
            [],
            |row| row.get(0),
        )
        .expect("A push again");

    assert!(
        settings::peek_top_for_key("prefetch_search").is_none(),
        "queue_b (top of stack) must not carry A's push — the scalar \
         closure pushes directly to queue_a via its captured pApp"
    );
}

/// Sequential connections on the same VFS. After A drops, B opens
/// fresh — the install helper captures B's new queue, not a stale
/// copy of A's.
#[test]
fn sequential_connections_dont_share_queue_c_path() {
    let tmp = TempDir::new().unwrap();
    let vfs_name = unique_name("tf_seq");

    let vfs = TurboliteVfs::new_local(TurboliteConfig {
        cache_dir: tmp.path().to_path_buf(),
        ..Default::default()
    })
    .expect("vfs");
    tiered::register(&vfs_name, vfs).expect("register");

    {
        let conn = open_connection(&vfs_name, "seq_a.db");
        assert_eq!(install(&conn), SQLITE_OK);
        let _: i64 = conn
            .query_row(
                "SELECT turbolite_config_set('prefetch_search', '0.11,0.22')",
                [],
                |row| row.get(0),
            )
            .expect("A push");
        assert_eq!(
            settings::peek_top_for_key("prefetch_search").as_deref(),
            Some("0.11,0.22"),
        );
        // Drop `conn` and its TurboliteHandle → leave_handle fires.
    }

    let conn = open_connection(&vfs_name, "seq_b.db");
    assert_eq!(install(&conn), SQLITE_OK);
    assert!(
        settings::peek_top_for_key("prefetch_search").is_none(),
        "B's queue must not inherit A's pending update"
    );

    let _: i64 = conn
        .query_row(
            "SELECT turbolite_config_set('prefetch_search', '0.33,0.44')",
            [],
            |row| row.get(0),
        )
        .expect("B push");
    assert_eq!(
        settings::peek_top_for_key("prefetch_search").as_deref(),
        Some("0.33,0.44"),
    );
}
