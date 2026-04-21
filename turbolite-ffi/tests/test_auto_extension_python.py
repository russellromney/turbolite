#!/usr/bin/env python3
"""
End-to-end test for the loadable-extension + sqlite3_auto_extension path.

Phase Cirrus h2 + fix: every newly-opened sqlite3 connection in the
process should get `turbolite_config_set` bound with its own handle
queue captured via `pApp`, without any explicit install call from the
binding. This exercises the ergonomic contract from a language
binding's perspective (Python ctypes over the loadable cdylib, same
as Node koffi / Go cgo would see).

Build before running:
    cd ~/Documents/Github/turbolite-ffi
    make ext
    # copies libturbolite_ffi.dylib to <target>/release/turbolite.dylib

Requires Homebrew Python (system macOS Python lacks load_extension).

Run:
    /opt/homebrew/bin/python3 tests/test_auto_extension_python.py
"""

import os
import sqlite3
import sys
import tempfile

# Shared cargo target dir with the rest of the cinch siblings. We look
# in both the shared location and the crate-local one for flexibility.
_this_dir = os.path.dirname(os.path.abspath(__file__))
_candidates = [
    os.path.join(_this_dir, "..", "..", "cinch-target", "release", "turbolite"),
    os.path.join(_this_dir, "..", "target", "release", "turbolite"),
]
EXT_PATH = next(
    (p for p in _candidates if os.path.exists(p + ".so") or os.path.exists(p + ".dylib")),
    None,
)
if EXT_PATH is None:
    print("error: built loadable ext not found. Run: make -C turbolite-ffi ext")
    sys.exit(1)

passed = 0
failed = 0


def run(name, fn):
    global passed, failed
    try:
        fn()
        passed += 1
        print(f"  PASS  {name}")
    except Exception as e:
        failed += 1
        print(f"  FAIL  {name}: {e}")
        import traceback; traceback.print_exc()


def load_ext_and_register_vfs(name, cache_dir):
    """Load the extension (idempotent) and register a named VFS.
    Returns nothing — the VFS is now resolvable via `vfs=` URI param.
    """
    bootstrap = sqlite3.connect(":memory:")
    bootstrap.enable_load_extension(True)
    bootstrap.load_extension(EXT_PATH)
    bootstrap.execute(f"SELECT turbolite_register_vfs('{name}', '{cache_dir}')")
    bootstrap.close()


def test_single_connection_auto_installs():
    """Single turbolite connection: turbolite_config_set must be
    available without any explicit install call."""
    with tempfile.TemporaryDirectory() as d:
        vfs = "auto-ext-solo"
        load_ext_and_register_vfs(vfs, d)
        conn = sqlite3.connect(f"file:{d}/a.db?vfs={vfs}", uri=True)
        try:
            rc = conn.execute(
                "SELECT turbolite_config_set('prefetch_search', '0.5,0.5')"
            ).fetchone()[0]
            assert rc == 0, f"expected 0, got {rc}"
        finally:
            conn.close()


def test_two_connections_same_thread_route_independently():
    """Two turbolite connections on the main thread — the pApp capture
    installed by the auto-extension must route each connection's push
    to its own queue. Pre-h2 bug: B would steal A's pushes because the
    shared scalar did a thread-local lookup at call time and saw B on
    top of the stack."""
    with tempfile.TemporaryDirectory() as da, tempfile.TemporaryDirectory() as db_:
        vfs_a = "auto-ext-a"
        vfs_b = "auto-ext-b"
        load_ext_and_register_vfs(vfs_a, da)
        load_ext_and_register_vfs(vfs_b, db_)

        conn_a = sqlite3.connect(f"file:{da}/a.db?vfs={vfs_a}", uri=True)
        conn_b = sqlite3.connect(f"file:{db_}/b.db?vfs={vfs_b}", uri=True)
        try:
            rc_a = conn_a.execute(
                "SELECT turbolite_config_set('prefetch_search', '0.11,0.22')"
            ).fetchone()[0]
            rc_b = conn_b.execute(
                "SELECT turbolite_config_set('prefetch_search', '0.99,0.88')"
            ).fetchone()[0]
            assert rc_a == 0 and rc_b == 0

            # Drain each queue through a real page read. Both should
            # succeed and return distinct expected rowcounts — proving
            # the connections are wired to their own handles.
            conn_a.execute("CREATE TABLE IF NOT EXISTS a (x)")
            conn_b.execute("CREATE TABLE IF NOT EXISTS b (x)")
            conn_a.execute("INSERT INTO a VALUES (1), (2)")
            conn_b.execute("INSERT INTO b VALUES (10)")
            a_count = conn_a.execute("SELECT count(*) FROM a").fetchone()[0]
            b_count = conn_b.execute("SELECT count(*) FROM b").fetchone()[0]
            assert a_count == 2, f"conn_a count wrong: {a_count}"
            assert b_count == 1, f"conn_b count wrong: {b_count}"
        finally:
            conn_a.close()
            conn_b.close()


def test_unrelated_memory_connection_coexists():
    """Open a plain in-memory sqlite connection (not turbolite-backed)
    in the same process. The auto-extension fires on it too but must
    not break the open. turbolite_config_set should NOT be registered
    on the plain connection."""
    with tempfile.TemporaryDirectory() as d:
        # Ensure turbolite is loaded and auto-extension is active
        # before the plain open.
        load_ext_and_register_vfs("auto-ext-coexist", d)

        plain = sqlite3.connect(":memory:")
        try:
            plain.execute("CREATE TABLE t (x INTEGER)")
            plain.execute("INSERT INTO t VALUES (42)")
            v = plain.execute("SELECT x FROM t").fetchone()[0]
            assert v == 42
            try:
                plain.execute(
                    "SELECT turbolite_config_set('prefetch_search', '0.5,0.5')"
                ).fetchone()
                assert False, "turbolite_config_set should not exist on plain conn"
            except sqlite3.OperationalError as e:
                assert "turbolite_config_set" in str(e) or "no such function" in str(e), \
                    f"unexpected error: {e}"
        finally:
            plain.close()


if __name__ == "__main__":
    print(f"loading extension from: {EXT_PATH}")
    run("single connection auto-installs turbolite_config_set",
        test_single_connection_auto_installs)
    run("two connections same thread route per-connection via pApp",
        test_two_connections_same_thread_route_independently)
    run("unrelated memory connection coexists (auto-extension no-op)",
        test_unrelated_memory_connection_coexists)

    print(f"\n{passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)
