//! Does process-global sqlite3_auto_extension cause bleed-over?
//!
//! Scenario: a turbolite connection is alive on this thread (its
//! TurboliteHandle is pushed on the thread-local queue stack). On the
//! same thread we open a plain (non-turbolite) rusqlite connection.
//! `sqlite3_open_v2` for the plain connection fires our auto-extension
//! callback, which does `settings::top_queue()`. Since the turbolite
//! handle is still alive, top_queue returns Some(turbolite's queue).
//! If we naively install `turbolite_config_set` on the plain connection
//! with THAT queue as pApp, pushes via the plain connection would land
//! on the turbolite connection's queue — cross-connection leak.

use std::sync::atomic::{AtomicU32, Ordering};

use rusqlite::{Connection, OpenFlags};
use tempfile::TempDir;
use turbolite::tiered::{self, TurboliteConfig, TurboliteVfs};

static TEST_COUNTER: AtomicU32 = AtomicU32::new(0);

fn unique_name(prefix: &str) -> String {
    let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}_{}_{}", prefix, std::process::id(), n)
}

#[test]
fn plain_connection_does_not_inherit_scalar_from_alive_turbolite_handle() {
    let tmp = TempDir::new().unwrap();
    let vfs_name = unique_name("bleed");
    let vfs = TurboliteVfs::new_local(TurboliteConfig {
        cache_dir: tmp.path().to_path_buf(),
        ..Default::default()
    })
    .unwrap();
    tiered::register(&vfs_name, vfs).unwrap();

    // Turbolite connection stays alive — its queue is on the thread-local stack.
    let tlite = Connection::open_with_flags_and_vfs(
        format!("file:{}/tlite.db?vfs={}", tmp.path().display(), vfs_name),
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        &vfs_name,
    )
    .unwrap();
    tlite
        .execute("CREATE TABLE IF NOT EXISTS t (x INTEGER)", [])
        .unwrap();

    // Plain sqlite connection on the same thread. Auto-extension fires;
    // top_queue() returns tlite's queue.
    let plain = Connection::open_in_memory().unwrap();
    plain.execute("CREATE TABLE t (x INTEGER)", []).unwrap();

    // If the bug exists: `plain.turbolite_config_set(...)` succeeds and
    // the push lands on tlite's queue. If the install-hook correctly
    // skips non-turbolite connections: the call errors with "no such
    // function".
    let result = plain.query_row(
        "SELECT turbolite_config_set('prefetch_search', '0.77,0.23')",
        [],
        |r| r.get::<_, i64>(0),
    );

    match result {
        Err(e) => {
            let msg = format!("{e}");
            assert!(
                msg.contains("no such function") || msg.contains("turbolite_config_set"),
                "expected 'no such function', got: {msg}"
            );
            println!("OK: plain connection does NOT have turbolite_config_set bound");
        }
        Ok(_) => {
            // Bug confirmed: the scalar was installed on the plain
            // connection pointing at tlite's queue.
            panic!(
                "BUG: turbolite_config_set is bound to a NON-turbolite connection. \
                 The install-hook's auto-extension callback fired during plain's \
                 sqlite3_open_v2, saw a turbolite handle active on the thread, \
                 and installed the scalar on the plain connection with the \
                 turbolite handle's queue as pApp. Need a VFS-name guard \
                 in install_hook.rs."
            );
        }
    }
}
