#!/usr/bin/env python3
"""
End-to-end integration test for turbolite shared library (.so/.dylib).

Loads the shared library via ctypes and exercises the full pipeline:
  register VFS → open database → create table → insert → query → verify → close

Run:
    make lib-bundled   # or: make lib  (needs system SQLite)
    python3 tests/test_ffi_python.py
"""

import ctypes
import json
import os
import platform
import sys
import tempfile

# ── Load the shared library ──────────────────────────────────────────

def find_lib():
    """Find the turbolite shared library."""
    ext = "dylib" if platform.system() == "Darwin" else "so"
    name = f"libsqlite_compress_encrypt_vfs.{ext}"
    # Check release and debug build dirs.
    for profile in ("release", "debug"):
        path = os.path.join(
            os.path.dirname(__file__), "..", "target", profile, name
        )
        if os.path.exists(path):
            return os.path.abspath(path)
    print(f"ERROR: {name} not found. Run `make lib-bundled` first.", file=sys.stderr)
    sys.exit(1)


lib_path = find_lib()
lib = ctypes.CDLL(lib_path)

# ── Declare FFI signatures ───────────────────────────────────────────

# Error handling
lib.turbolite_last_error.restype = ctypes.c_char_p
lib.turbolite_last_error.argtypes = []

# Version
lib.turbolite_version.restype = ctypes.c_char_p
lib.turbolite_version.argtypes = []

# Registration
lib.turbolite_register_compressed.restype = ctypes.c_int
lib.turbolite_register_compressed.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int]

lib.turbolite_register_passthrough.restype = ctypes.c_int
lib.turbolite_register_passthrough.argtypes = [ctypes.c_char_p, ctypes.c_char_p]

# Database operations
lib.turbolite_open.restype = ctypes.c_void_p
lib.turbolite_open.argtypes = [ctypes.c_char_p, ctypes.c_char_p]

lib.turbolite_exec.restype = ctypes.c_int
lib.turbolite_exec.argtypes = [ctypes.c_void_p, ctypes.c_char_p]

lib.turbolite_query_json.restype = ctypes.c_void_p  # *mut c_char (we manage it)
lib.turbolite_query_json.argtypes = [ctypes.c_void_p, ctypes.c_char_p]

lib.turbolite_free_string.restype = None
lib.turbolite_free_string.argtypes = [ctypes.c_void_p]

lib.turbolite_close.restype = None
lib.turbolite_close.argtypes = [ctypes.c_void_p]

# Utilities
lib.turbolite_clear_caches.restype = None
lib.turbolite_clear_caches.argtypes = []

lib.turbolite_invalidate_cache.restype = ctypes.c_int
lib.turbolite_invalidate_cache.argtypes = [ctypes.c_char_p]


def query_json(db, sql):
    """Helper: run a query and return parsed JSON, or raise on error."""
    ptr = lib.turbolite_query_json(db, sql.encode())
    if not ptr:
        err = lib.turbolite_last_error()
        raise RuntimeError(f"query failed: {err.decode() if err else 'unknown'}")
    try:
        raw = ctypes.cast(ptr, ctypes.c_char_p).value
        return json.loads(raw.decode())
    finally:
        lib.turbolite_free_string(ptr)


# ── Tests ─────────────────────────────────────────────────────────────

passed = 0
failed = 0


def test(name):
    """Decorator to register and run a test."""
    def decorator(fn):
        global passed, failed
        try:
            fn()
            print(f"  PASS  {name}")
            passed += 1
        except Exception as e:
            print(f"  FAIL  {name}: {e}")
            failed += 1
    return decorator


@test("version returns valid string")
def _():
    v = lib.turbolite_version()
    assert v is not None
    version = v.decode()
    assert len(version) > 0
    # Should be semver-ish (e.g. "0.1.0")
    parts = version.split(".")
    assert len(parts) >= 2, f"unexpected version format: {version}"


@test("null name returns error")
def _():
    rc = lib.turbolite_register_compressed(None, b"/tmp", 3)
    assert rc == -1
    err = lib.turbolite_last_error()
    assert err is not None
    assert b"name" in err


@test("null base_dir returns error")
def _():
    rc = lib.turbolite_register_compressed(b"null-test", None, 3)
    assert rc == -1
    err = lib.turbolite_last_error()
    assert b"base_dir" in err


@test("register compressed VFS")
def _():
    with tempfile.TemporaryDirectory() as d:
        rc = lib.turbolite_register_compressed(b"py-compressed", d.encode(), 3)
        assert rc == 0, f"register failed: {lib.turbolite_last_error()}"


@test("register passthrough VFS")
def _():
    with tempfile.TemporaryDirectory() as d:
        rc = lib.turbolite_register_passthrough(b"py-passthrough", d.encode())
        assert rc == 0, f"register failed: {lib.turbolite_last_error()}"


@test("open database with compressed VFS")
def _():
    with tempfile.TemporaryDirectory() as d:
        lib.turbolite_register_compressed(b"py-open-test", d.encode(), 3)
        db_path = os.path.join(d, "test.db").encode()
        db = lib.turbolite_open(db_path, b"py-open-test")
        assert db is not None, f"open failed: {lib.turbolite_last_error()}"
        lib.turbolite_close(db)


@test("exec CREATE TABLE + INSERT")
def _():
    with tempfile.TemporaryDirectory() as d:
        lib.turbolite_register_compressed(b"py-exec-test", d.encode(), 3)
        db_path = os.path.join(d, "test.db").encode()
        db = lib.turbolite_open(db_path, b"py-exec-test")
        assert db

        rc = lib.turbolite_exec(db, b"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        assert rc == 0, f"create table failed: {lib.turbolite_last_error()}"

        rc = lib.turbolite_exec(db, b"INSERT INTO t VALUES (1, 'alice')")
        assert rc == 0, f"insert failed: {lib.turbolite_last_error()}"

        lib.turbolite_close(db)


@test("full round-trip: create, insert, query, verify")
def _():
    with tempfile.TemporaryDirectory() as d:
        lib.turbolite_register_compressed(b"py-roundtrip", d.encode(), 3)
        db_path = os.path.join(d, "test.db").encode()
        db = lib.turbolite_open(db_path, b"py-roundtrip")
        assert db

        lib.turbolite_exec(db, b"""
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
            INSERT INTO users VALUES (1, 'alice', 30);
            INSERT INTO users VALUES (2, 'bob', 25);
            INSERT INTO users VALUES (3, 'carol', 35);
        """)

        rows = query_json(db, "SELECT * FROM users ORDER BY id")
        assert len(rows) == 3
        assert rows[0] == {"id": 1, "name": "alice", "age": 30}
        assert rows[1] == {"id": 2, "name": "bob", "age": 25}
        assert rows[2] == {"id": 3, "name": "carol", "age": 35}

        lib.turbolite_close(db)


@test("query with WHERE clause")
def _():
    with tempfile.TemporaryDirectory() as d:
        lib.turbolite_register_compressed(b"py-where", d.encode(), 3)
        db_path = os.path.join(d, "test.db").encode()
        db = lib.turbolite_open(db_path, b"py-where")
        assert db

        lib.turbolite_exec(db, b"""
            CREATE TABLE items (id INTEGER PRIMARY KEY, price REAL);
            INSERT INTO items VALUES (1, 9.99);
            INSERT INTO items VALUES (2, 19.99);
            INSERT INTO items VALUES (3, 29.99);
        """)

        rows = query_json(db, "SELECT * FROM items WHERE price > 15.0 ORDER BY id")
        assert len(rows) == 2
        assert rows[0]["id"] == 2
        assert rows[1]["price"] == 29.99

        lib.turbolite_close(db)


@test("reopen database preserves data (persistence)")
def _():
    with tempfile.TemporaryDirectory() as d:
        lib.turbolite_register_compressed(b"py-persist", d.encode(), 3)
        db_path = os.path.join(d, "persist.db").encode()

        # Write
        db = lib.turbolite_open(db_path, b"py-persist")
        assert db
        lib.turbolite_exec(db, b"CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)")
        lib.turbolite_exec(db, b"INSERT INTO kv VALUES ('hello', 'world')")
        lib.turbolite_close(db)

        # Re-open and read
        db = lib.turbolite_open(db_path, b"py-persist")
        assert db
        rows = query_json(db, "SELECT * FROM kv")
        assert len(rows) == 1
        assert rows[0] == {"k": "hello", "v": "world"}
        lib.turbolite_close(db)


@test("passthrough VFS round-trip")
def _():
    with tempfile.TemporaryDirectory() as d:
        lib.turbolite_register_passthrough(b"py-pt-roundtrip", d.encode())
        db_path = os.path.join(d, "test.db").encode()
        db = lib.turbolite_open(db_path, b"py-pt-roundtrip")
        assert db

        lib.turbolite_exec(db, b"CREATE TABLE t (x INTEGER)")
        lib.turbolite_exec(db, b"INSERT INTO t VALUES (42)")
        rows = query_json(db, "SELECT x FROM t")
        assert rows == [{"x": 42}]

        lib.turbolite_close(db)


@test("exec with bad SQL returns error")
def _():
    with tempfile.TemporaryDirectory() as d:
        lib.turbolite_register_compressed(b"py-badsql", d.encode(), 3)
        db_path = os.path.join(d, "test.db").encode()
        db = lib.turbolite_open(db_path, b"py-badsql")
        assert db

        rc = lib.turbolite_exec(db, b"NOT VALID SQL AT ALL")
        assert rc == -1
        err = lib.turbolite_last_error()
        assert err is not None

        lib.turbolite_close(db)


@test("clear_caches does not crash")
def _():
    lib.turbolite_clear_caches()


@test("invalidate_cache with null path returns error")
def _():
    rc = lib.turbolite_invalidate_cache(None)
    assert rc == -1


@test("open with null path returns NULL")
def _():
    db = lib.turbolite_open(None, b"any-vfs")
    assert db is None


@test("close with NULL is safe (no crash)")
def _():
    lib.turbolite_close(None)


@test("free_string with NULL is safe (no crash)")
def _():
    lib.turbolite_free_string(None)


# ── Tiered S3 tests (only run when TIERED_TEST_BUCKET is set) ─────────

HAS_TIERED = hasattr(lib, "turbolite_register_tiered")
TIERED_BUCKET = os.environ.get("TIERED_TEST_BUCKET")

if HAS_TIERED and TIERED_BUCKET:
    import time

    lib.turbolite_register_tiered.restype = ctypes.c_int
    lib.turbolite_register_tiered.argtypes = [
        ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p,
        ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p,
    ]

    TIERED_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL")
    TIERED_REGION = os.environ.get("AWS_REGION", "auto")

    def tiered_prefix(tag):
        return f"test/ffi-python/{tag}/{int(time.time() * 1e9)}"

    @test("tiered: register S3 VFS")
    def _():
        with tempfile.TemporaryDirectory() as d:
            rc = lib.turbolite_register_tiered(
                b"py-tiered-reg",
                TIERED_BUCKET.encode(),
                tiered_prefix("reg").encode(),
                d.encode(),
                TIERED_ENDPOINT.encode() if TIERED_ENDPOINT else None,
                TIERED_REGION.encode(),
            )
            assert rc == 0, f"register failed: {lib.turbolite_last_error()}"

    @test("tiered: full round-trip via S3")
    def _():
        with tempfile.TemporaryDirectory() as d:
            prefix = tiered_prefix("roundtrip")
            rc = lib.turbolite_register_tiered(
                b"py-tiered-rt",
                TIERED_BUCKET.encode(),
                prefix.encode(),
                d.encode(),
                TIERED_ENDPOINT.encode() if TIERED_ENDPOINT else None,
                TIERED_REGION.encode(),
            )
            assert rc == 0, f"register failed: {lib.turbolite_last_error()}"

            db_path = os.path.join(d, "test.db").encode()
            db = lib.turbolite_open(db_path, b"py-tiered-rt")
            assert db, f"open failed: {lib.turbolite_last_error()}"

            lib.turbolite_exec(db, b"""
                CREATE TABLE users (id INTEGER, name TEXT, age INTEGER);
                INSERT INTO users VALUES (1, 'alice', 30);
                INSERT INTO users VALUES (2, 'bob', 25);
                INSERT INTO users VALUES (3, 'carol', 35);
            """)

            rows = query_json(db, "SELECT * FROM users ORDER BY id")
            assert len(rows) == 3
            assert rows[0] == {"id": 1, "name": "alice", "age": 30}
            assert rows[1] == {"id": 2, "name": "bob", "age": 25}
            assert rows[2] == {"id": 3, "name": "carol", "age": 35}

            lib.turbolite_close(db)

    @test("tiered: null bucket returns error")
    def _():
        with tempfile.TemporaryDirectory() as d:
            rc = lib.turbolite_register_tiered(
                b"py-tiered-null", None, b"prefix", d.encode(), None, None,
            )
            assert rc == -1
            err = lib.turbolite_last_error()
            assert err is not None
            assert b"bucket" in err

elif HAS_TIERED:
    print("  SKIP  tiered tests (set TIERED_TEST_BUCKET to enable)")
else:
    print("  SKIP  tiered tests (library built without tiered feature)")


# ── Summary ───────────────────────────────────────────────────────────

print()
print(f"Loaded: {lib_path}")
print(f"Results: {passed} passed, {failed} failed")
if failed:
    sys.exit(1)
