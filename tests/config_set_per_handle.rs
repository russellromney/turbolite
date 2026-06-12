//! Phase Cirrus a.2 regression tests: the restored `turbolite_config_set`
//! SQL function routes per-connection via the thread-local active-handle
//! stack, not via a process-global queue.
//!
//! The old pre-Cirrus design had a single `static SETTINGS_QUEUE` that
//! every handle drained on each xRead — connection A's
//! `SELECT turbolite_config_set(...)` could silently land on connection
//! B's handle if B's xRead ran first. These tests exercise the end-to-end
//! path (rusqlite -> scalar function -> FFI -> thread-local -> per-handle
//! queue) to prove that no such cross-connection leak can happen.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::thread;

use rusqlite::{Connection, OpenFlags};
use tempfile::TempDir;
use turbolite::tiered::{self, settings, TurboliteConfig, TurboliteVfs};

static TEST_COUNTER: AtomicU32 = AtomicU32::new(0);

fn unique_name(prefix: &str) -> String {
    let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}_{}_{}", prefix, std::process::id(), n)
}

fn open_connection(vfs_name: &str, db_file: &str) -> Connection {
    let conn = Connection::open_with_flags_and_vfs(
        format!("file:{}?vfs={}", db_file, vfs_name),
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        vfs_name,
    )
    .expect("open turbolite connection");
    turbolite::install_config_functions(&conn).expect("install config functions");
    // Forces xOpen on the main-db file; without this, no TurboliteHandle
    // has been constructed on this thread yet and the thread-local stack
    // is empty.
    conn.execute("CREATE TABLE IF NOT EXISTS _bootstrap (x INTEGER)", [])
        .expect("bootstrap table");
    conn
}

/// Smoke test: the SQL function exists, accepts valid input, returns 0 on
/// a turbolite-backed connection. This proves the rusqlite wiring
/// (create_scalar_function -> tiered::settings::set -> push_to_current)
/// works end-to-end on a real handle.
#[test]
fn turbolite_config_set_returns_ok_on_live_connection() {
    let tmp = TempDir::new().unwrap();
    let vfs_name = unique_name("cs_smoke");

    let vfs = TurboliteVfs::new_local(TurboliteConfig {
        cache_dir: tmp.path().to_path_buf(),
        ..Default::default()
    })
    .expect("new_local vfs");
    tiered::register(&vfs_name, vfs).expect("register vfs");

    let conn = open_connection(&vfs_name, "smoke.db");

    // rc = 0 means the update was queued onto THIS connection's handle.
    let rc: i64 = conn
        .query_row(
            "SELECT turbolite_config_set('prefetch_search', '0.5,0.5')",
            [],
            |row| row.get(0),
        )
        .expect("turbolite_config_set should succeed");
    assert_eq!(rc, 0, "expected 0 from turbolite_config_set");

    // Peek confirms the push landed on THIS thread's active queue.
    let peeked =
        settings::peek_top_for_key("prefetch_search").expect("queue should have a pending update");
    assert_eq!(peeked, "0.5,0.5");

    // A subsequent real query drains the queue (handle's xRead applies it).
    // We can't observe the applied value directly without reaching into
    // the handle's private state, but we can assert the query succeeds
    // (no panic / no error on drain).
    conn.execute("INSERT INTO _bootstrap VALUES (1)", [])
        .expect("insert");
    let v: i64 = conn
        .query_row("SELECT x FROM _bootstrap", [], |row| row.get(0))
        .expect("select");
    assert_eq!(v, 1);

    // After the drain, peek sees no pending update for this key.
    assert!(settings::peek_top_for_key("prefetch_search").is_none());
}

/// Validation errors surface as SQL errors through the scalar function.
#[test]
fn turbolite_config_set_rejects_bad_input() {
    let tmp = TempDir::new().unwrap();
    let vfs_name = unique_name("cs_bad");

    let vfs = TurboliteVfs::new_local(TurboliteConfig {
        cache_dir: tmp.path().to_path_buf(),
        ..Default::default()
    })
    .expect("new_local vfs");
    tiered::register(&vfs_name, vfs).expect("register vfs");

    let conn = open_connection(&vfs_name, "bad.db");

    // Unknown key.
    let err = conn
        .query_row("SELECT turbolite_config_set('nope', 'x')", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("unknown key"),
        "expected unknown-key error, got {err:?}"
    );

    // Bad value for a known key.
    let err = conn
        .query_row(
            "SELECT turbolite_config_set('prefetch_search', 'not,numbers')",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("invalid hop schedule"),
        "expected invalid-hop-schedule error, got {err:?}"
    );

    // Bad boolean for lookahead.
    let err = conn
        .query_row(
            "SELECT turbolite_config_set('lookahead', 'maybe')",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("expected true/false/1/0"),
        "expected boolean validation error, got {err:?}"
    );
}

/// The heisenbug regression test. Two connections, each on its OWN
/// thread, each opens its own turbolite VFS + handle, each pushes a
/// distinct schedule via the SQL function. Each thread peeks its own
/// queue afterwards and must see its OWN value — no cross-thread leak
/// across what used to be a shared process-global queue.
///
/// Separate threads is how the old design failed most visibly: each
/// thread has its own active-handle stack, so a correct implementation
/// must have zero cross-thread interaction in the push path.
#[test]
fn two_threads_isolated_via_sql_function() {
    let tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();
    let path_a = tmp_a.path().to_path_buf();
    let path_b = tmp_b.path().to_path_buf();

    let barrier = Arc::new(std::sync::Barrier::new(2));
    let ba = Arc::clone(&barrier);
    let bb = Arc::clone(&barrier);

    let t_a = thread::spawn(move || {
        let vfs_name = unique_name("iso_a");
        let vfs = TurboliteVfs::new_local(TurboliteConfig {
            cache_dir: path_a,
            ..Default::default()
        })
        .expect("A vfs");
        tiered::register(&vfs_name, vfs).expect("A register");
        let conn = open_connection(&vfs_name, "a.db");

        // Sync so both threads set their schedules roughly concurrently —
        // exercises the thread-local isolation under contention.
        ba.wait();

        let rc: i64 = conn
            .query_row(
                "SELECT turbolite_config_set('prefetch_search', '0.10,0.20,0.30')",
                [],
                |row| row.get(0),
            )
            .expect("A set");
        assert_eq!(rc, 0);

        // A must see A's own value on its queue.
        settings::peek_top_for_key("prefetch_search").expect("A queue has pending")
    });

    let t_b = thread::spawn(move || {
        let vfs_name = unique_name("iso_b");
        let vfs = TurboliteVfs::new_local(TurboliteConfig {
            cache_dir: path_b,
            ..Default::default()
        })
        .expect("B vfs");
        tiered::register(&vfs_name, vfs).expect("B register");
        let conn = open_connection(&vfs_name, "b.db");

        bb.wait();

        let rc: i64 = conn
            .query_row(
                "SELECT turbolite_config_set('prefetch_search', '0.90,0.90,0.90')",
                [],
                |row| row.get(0),
            )
            .expect("B set");
        assert_eq!(rc, 0);

        settings::peek_top_for_key("prefetch_search").expect("B queue has pending")
    });

    let a_value = t_a.join().expect("thread A panicked");
    let b_value = t_b.join().expect("thread B panicked");

    assert_eq!(a_value, "0.10,0.20,0.30", "A saw its own value");
    assert_eq!(b_value, "0.90,0.90,0.90", "B saw its own value");
    assert_ne!(a_value, b_value, "threads must not share queue state");
}

/// Two connections open concurrently on the *same* thread: each
/// connection's `turbolite_config_set` push lands on its *own* queue,
/// regardless of open order.
///
/// Mechanism: `install_config_functions` captures the calling
/// connection's handle queue at install time (via the thread-local
/// active-handle stack, which top-of-stack is THIS connection's queue
/// because install is called immediately after open) and the scalar
/// function's closure pushes into that captured queue directly. No
/// thread-local lookup happens at scalar-function-call time, so stack
/// order at call time doesn't matter.
///
/// This is the regression guard for the multi-connection-per-thread
/// case — rare in rusqlite production use (pool patterns enforce one
/// connection per thread) but the right thing to get right. The
/// equivalent C-side fix lives in the turbolite-ffi extraction (Cirrus
/// step h); loadable-extension users keep the "top-of-stack wins"
/// behavior until then.
#[test]
fn multi_connection_same_thread_routes_per_connection() {
    let tmp = TempDir::new().unwrap();
    let vfs_name = unique_name("multi_same_thread");

    let vfs = TurboliteVfs::new_local(TurboliteConfig {
        cache_dir: tmp.path().to_path_buf(),
        ..Default::default()
    })
    .expect("vfs");
    tiered::register(&vfs_name, vfs).expect("register");

    let conn_a = open_connection(&vfs_name, "multi_a.db");
    let conn_b = open_connection(&vfs_name, "multi_b.db");
    // Thread-local stack is now [queue_a, queue_b], B on top.
    // Each connection's install_config_functions captured its own queue
    // at the moment just after its own open — so A's scalar function
    // holds an Arc to queue_a and B's holds an Arc to queue_b.

    // A pushes — must land on A's queue, not B's (the top of the stack).
    let _: i64 = conn_a
        .query_row(
            "SELECT turbolite_config_set('prefetch_search', '0.11,0.22')",
            [],
            |row| row.get(0),
        )
        .expect("A set");

    // B pushes — must land on B's queue.
    let _: i64 = conn_b
        .query_row(
            "SELECT turbolite_config_set('prefetch_search', '0.99,0.88')",
            [],
            |row| row.get(0),
        )
        .expect("B set");

    // Verify by draining each connection's next xRead. Force a real
    // page read on each; after the drain, peek should show no pending
    // update for that connection's queue (drain removed it).
    conn_a
        .execute("INSERT INTO _bootstrap VALUES (1)", [])
        .unwrap();
    conn_b
        .execute("INSERT INTO _bootstrap VALUES (2)", [])
        .unwrap();

    // The real proof: push again on A, without touching B. Then peek
    // the top of the stack (which is queue_b since B was opened most
    // recently) and verify no `prefetch_search` entry is pending there —
    // A's second push must have landed on A's queue, not B's.
    let _: i64 = conn_a
        .query_row(
            "SELECT turbolite_config_set('prefetch_search', '0.55,0.44')",
            [],
            |row| row.get(0),
        )
        .expect("A set (again)");

    // peek_top_for_key inspects top of stack = queue_b.
    // B's queue must NOT have A's latest push.
    assert!(
        settings::peek_top_for_key("prefetch_search").is_none(),
        "queue_b (top of stack) must not carry A's push — the closure \
         bypasses the thread-local and pushes directly to queue_a"
    );
}

/// Same-thread routing for the lookahead key specifically. The generic
/// schedule tests above prove the queue mechanism; this pins the exact
/// per-query lookahead knob so connection A cannot accidentally enable
/// lookahead on connection B.
#[test]
fn lookahead_key_routes_per_connection() {
    let tmp = TempDir::new().unwrap();
    let vfs_name = unique_name("lookahead_route");

    let vfs = TurboliteVfs::new_local(TurboliteConfig {
        cache_dir: tmp.path().to_path_buf(),
        ..Default::default()
    })
    .expect("vfs");
    tiered::register(&vfs_name, vfs).expect("register");

    let conn_a = open_connection(&vfs_name, "lookahead_a.db");
    let conn_b = open_connection(&vfs_name, "lookahead_b.db");
    // Stack top is B, but A's SQL function must still push to A's
    // captured queue.

    let rc: i64 = conn_a
        .query_row(
            "SELECT turbolite_config_set('lookahead', 'true')",
            [],
            |row| row.get(0),
        )
        .expect("A set lookahead");
    assert_eq!(rc, 0);

    assert!(
        settings::peek_top_for_key("lookahead").is_none(),
        "B is top-of-stack and must not inherit A's lookahead=true push"
    );

    let rc: i64 = conn_b
        .query_row(
            "SELECT turbolite_config_set('lookahead', 'false')",
            [],
            |row| row.get(0),
        )
        .expect("B set lookahead");
    assert_eq!(rc, 0);

    assert_eq!(
        settings::peek_top_for_key("lookahead").as_deref(),
        Some("false"),
        "B should see only B's own lookahead=false push"
    );
}

/// Single thread, two sequential connections on the same VFS. After
/// dropping connection A and opening B, B's queue is separate — A's
/// pushes do not appear in B's queue. Exercises `leave_handle` on Drop
/// so the stack doesn't carry zombie queues.
#[test]
fn sequential_connections_dont_share_queue() {
    let tmp = TempDir::new().unwrap();
    let vfs_name = unique_name("cs_seq");

    let vfs = TurboliteVfs::new_local(TurboliteConfig {
        cache_dir: tmp.path().to_path_buf(),
        ..Default::default()
    })
    .expect("vfs");
    tiered::register(&vfs_name, vfs).expect("register");

    {
        let conn = open_connection(&vfs_name, "seq_a.db");
        let rc: i64 = conn
            .query_row(
                "SELECT turbolite_config_set('prefetch_search', '0.11,0.22')",
                [],
                |row| row.get(0),
            )
            .expect("A set");
        assert_eq!(rc, 0);
        let a_val = settings::peek_top_for_key("prefetch_search").expect("A queue has pending");
        assert_eq!(a_val, "0.11,0.22");
        // conn (and its TurboliteHandle) drop here → leave_handle fires
    }

    // With A dropped, opening B builds a fresh queue. Stack depth is 1.
    let conn = open_connection(&vfs_name, "seq_b.db");
    // B hasn't pushed anything yet; peek on B's queue returns None for
    // our key. If A's queue had leaked, we'd see "0.11,0.22" here.
    assert!(
        settings::peek_top_for_key("prefetch_search").is_none(),
        "B's queue must not carry A's pending update"
    );

    let rc: i64 = conn
        .query_row(
            "SELECT turbolite_config_set('prefetch_search', '0.33,0.44')",
            [],
            |row| row.get(0),
        )
        .expect("B set");
    assert_eq!(rc, 0);
    let b_val = settings::peek_top_for_key("prefetch_search").expect("B queue has pending");
    assert_eq!(b_val, "0.33,0.44");
}
