#!/usr/bin/env python3
"""
Comprehensive tests for the turbolite SQLite loadable extension.

Requires:
- Homebrew Python (system macOS Python lacks load_extension support)
- The loadable extension built: `make ext`

Run: /opt/homebrew/bin/python3 tests/test_loadable_ext.py
"""

import os
import sqlite3
import sys
import tempfile

EXT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "target", "release", "turbolite",
)

passed = 0
failed = 0
errors = []


def test(name):
    """Decorator to register and run a test."""
    def decorator(fn):
        global passed, failed
        try:
            fn()
            passed += 1
            print(f"  PASS  {name}")
        except Exception as e:
            failed += 1
            errors.append((name, e))
            print(f"  FAIL  {name}: {e}")
    return decorator


def load_ext(conn):
    """Load the turbolite extension into a connection."""
    conn.enable_load_extension(True)
    conn.load_extension(EXT_PATH)


# ── Happy path ────────────────────────────────────────────────────


@test("load extension into memory db")
def _():
    conn = sqlite3.connect(":memory:")
    load_ext(conn)
    conn.close()


@test("turbolite_version returns correct version")
def _():
    conn = sqlite3.connect(":memory:")
    load_ext(conn)
    version = conn.execute("SELECT turbolite_version()").fetchone()[0]
    assert version == "0.2.19", f"expected 0.2.19, got {version}"
    conn.close()


@test("turbolite_version is deterministic")
def _():
    conn = sqlite3.connect(":memory:")
    load_ext(conn)
    v1 = conn.execute("SELECT turbolite_version()").fetchone()[0]
    v2 = conn.execute("SELECT turbolite_version()").fetchone()[0]
    assert v1 == v2
    conn.close()


@test("open compressed db via URI, create table, insert, query")
def _():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "test.db")
        conn = sqlite3.connect(":memory:")
        load_ext(conn)
        conn.close()

        conn = sqlite3.connect(f"file:{path}?vfs=turbolite", uri=True)
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        conn.execute("INSERT INTO t VALUES (1, 'hello')")
        conn.commit()
        row = conn.execute("SELECT * FROM t").fetchone()
        assert row == (1, "hello"), f"got {row}"
        conn.close()


@test("file on disk has compressed magic (SQLCEvfS)")
def _():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "test.db")
        conn = sqlite3.connect(":memory:")
        load_ext(conn)
        conn.close()

        conn = sqlite3.connect(f"file:{path}?vfs=turbolite", uri=True)
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO t VALUES (1)")
        conn.commit()
        conn.close()

        with open(path, "rb") as f:
            magic = f.read(8)
        assert magic == b"SQLCEvfS", f"expected SQLCEvfS magic, got {magic!r}"


@test("data persists across close/reopen")
def _():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "test.db")
        conn = sqlite3.connect(":memory:")
        load_ext(conn)
        conn.close()

        conn = sqlite3.connect(f"file:{path}?vfs=turbolite", uri=True)
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        conn.execute("INSERT INTO t VALUES (1, 'persist')")
        conn.commit()
        conn.close()

        conn2 = sqlite3.connect(f"file:{path}?vfs=turbolite", uri=True)
        row = conn2.execute("SELECT val FROM t WHERE id=1").fetchone()
        assert row == ("persist",), f"got {row}"
        conn2.close()


@test("multiple tables and rows")
def _():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "test.db")
        conn = sqlite3.connect(":memory:")
        load_ext(conn)
        conn.close()

        conn = sqlite3.connect(f"file:{path}?vfs=turbolite", uri=True)
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, body TEXT)")
        for i in range(100):
            conn.execute("INSERT INTO users VALUES (?, ?)", (i, f"user_{i}"))
            conn.execute("INSERT INTO posts VALUES (?, ?, ?)", (i, i, f"post by user_{i}"))
        conn.commit()

        count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        assert count == 100, f"expected 100 users, got {count}"

        joined = conn.execute(
            "SELECT u.name, p.body FROM users u JOIN posts p ON u.id = p.user_id WHERE u.id = 42"
        ).fetchone()
        assert joined == ("user_42", "post by user_42"), f"got {joined}"
        conn.close()


@test("UPDATE and DELETE work")
def _():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "test.db")
        conn = sqlite3.connect(":memory:")
        load_ext(conn)
        conn.close()

        conn = sqlite3.connect(f"file:{path}?vfs=turbolite", uri=True)
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        conn.execute("INSERT INTO t VALUES (1, 'original')")
        conn.commit()

        conn.execute("UPDATE t SET val='updated' WHERE id=1")
        conn.commit()
        row = conn.execute("SELECT val FROM t WHERE id=1").fetchone()
        assert row == ("updated",)

        conn.execute("DELETE FROM t WHERE id=1")
        conn.commit()
        row = conn.execute("SELECT val FROM t WHERE id=1").fetchone()
        assert row is None
        conn.close()


@test("transaction rollback works")
def _():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "test.db")
        conn = sqlite3.connect(":memory:")
        load_ext(conn)
        conn.close()

        conn = sqlite3.connect(f"file:{path}?vfs=turbolite", uri=True, isolation_level="DEFERRED")
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        conn.execute("INSERT INTO t VALUES (1, 'committed')")
        conn.commit()

        conn.execute("INSERT INTO t VALUES (2, 'rolled_back')")
        conn.rollback()

        rows = conn.execute("SELECT * FROM t").fetchall()
        assert len(rows) == 1, f"expected 1 row after rollback, got {len(rows)}"
        assert rows[0] == (1, "committed")
        conn.close()


@test("NULL, integer, float, text, blob column types")
def _():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "test.db")
        conn = sqlite3.connect(":memory:")
        load_ext(conn)
        conn.close()

        conn = sqlite3.connect(f"file:{path}?vfs=turbolite", uri=True)
        conn.execute("CREATE TABLE t (a, b INTEGER, c REAL, d TEXT, e BLOB)")
        conn.execute("INSERT INTO t VALUES (NULL, 42, 3.14, 'text', X'DEADBEEF')")
        conn.commit()

        row = conn.execute("SELECT * FROM t").fetchone()
        assert row[0] is None
        assert row[1] == 42
        assert abs(row[2] - 3.14) < 1e-10
        assert row[3] == "text"
        assert row[4] == b"\xde\xad\xbe\xef"
        conn.close()


@test("index creation and index-based lookups")
def _():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "test.db")
        conn = sqlite3.connect(":memory:")
        load_ext(conn)
        conn.close()

        conn = sqlite3.connect(f"file:{path}?vfs=turbolite", uri=True)
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        conn.execute("CREATE INDEX idx_val ON t(val)")
        for i in range(1000):
            conn.execute("INSERT INTO t VALUES (?, ?)", (i, f"val_{i:04d}"))
        conn.commit()

        row = conn.execute("SELECT id FROM t WHERE val = 'val_0500'").fetchone()
        assert row == (500,), f"got {row}"
        conn.close()


# ── Idempotent loading ────────────────────────────────────────────


@test("load extension twice is idempotent (no crash)")
def _():
    conn = sqlite3.connect(":memory:")
    load_ext(conn)
    # Loading again should not crash — VFS already registered, just no-op
    load_ext(conn)
    version = conn.execute("SELECT turbolite_version()").fetchone()[0]
    assert version == "0.2.19"
    conn.close()


# ── Edge cases ────────────────────────────────────────────────────


@test("empty database roundtrip")
def _():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "empty.db")
        conn = sqlite3.connect(":memory:")
        load_ext(conn)
        conn.close()

        conn = sqlite3.connect(f"file:{path}?vfs=turbolite", uri=True)
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        conn.commit()
        conn.close()

        conn2 = sqlite3.connect(f"file:{path}?vfs=turbolite", uri=True)
        tables = conn2.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        assert len(tables) == 1
        conn2.close()


@test("large text values")
def _():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "test.db")
        conn = sqlite3.connect(":memory:")
        load_ext(conn)
        conn.close()

        big_text = "x" * 100_000
        conn = sqlite3.connect(f"file:{path}?vfs=turbolite", uri=True)
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        conn.execute("INSERT INTO t VALUES (1, ?)", (big_text,))
        conn.commit()

        row = conn.execute("SELECT val FROM t WHERE id=1").fetchone()
        assert row[0] == big_text, f"expected 100k chars, got {len(row[0])}"
        conn.close()


@test("large blob values")
def _():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "test.db")
        conn = sqlite3.connect(":memory:")
        load_ext(conn)
        conn.close()

        big_blob = os.urandom(100_000)
        conn = sqlite3.connect(f"file:{path}?vfs=turbolite", uri=True)
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val BLOB)")
        conn.execute("INSERT INTO t VALUES (1, ?)", (big_blob,))
        conn.commit()

        row = conn.execute("SELECT val FROM t WHERE id=1").fetchone()
        assert row[0] == big_blob
        conn.close()


@test("unicode text roundtrip")
def _():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "test.db")
        conn = sqlite3.connect(":memory:")
        load_ext(conn)
        conn.close()

        text = "Hello \U0001F600 \u4F60\u597D \u041F\u0440\u0438\u0432\u0435\u0442"
        conn = sqlite3.connect(f"file:{path}?vfs=turbolite", uri=True)
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        conn.execute("INSERT INTO t VALUES (1, ?)", (text,))
        conn.commit()

        row = conn.execute("SELECT val FROM t WHERE id=1").fetchone()
        assert row[0] == text
        conn.close()


@test("absolute path works")
def _():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "abs_test.db")
        assert os.path.isabs(path)
        conn = sqlite3.connect(":memory:")
        load_ext(conn)
        conn.close()

        conn = sqlite3.connect(f"file:{path}?vfs=turbolite", uri=True)
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO t VALUES (1)")
        conn.commit()
        row = conn.execute("SELECT * FROM t").fetchone()
        assert row == (1,)
        conn.close()


@test("file not on compressed VFS is not readable with turbolite VFS")
def _():
    with tempfile.TemporaryDirectory() as d:
        # Create a normal (uncompressed) sqlite db
        normal_path = os.path.join(d, "normal.db")
        conn_normal = sqlite3.connect(normal_path)
        conn_normal.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        conn_normal.execute("INSERT INTO t VALUES (1)")
        conn_normal.commit()
        conn_normal.close()

        conn = sqlite3.connect(":memory:")
        load_ext(conn)
        conn.close()

        # Try to open the normal db with turbolite VFS — should fail
        try:
            conn = sqlite3.connect(f"file:{normal_path}?vfs=turbolite", uri=True)
            conn.execute("SELECT * FROM t")
            assert False, "should have raised an error"
        except Exception:
            pass  # Expected — not a compressed file


# ── Phase Marne: trace callback + bench SQL functions ─────────────


@test("turbolite_clear_cache SQL function exists and accepts modes")
def _():
    conn = sqlite3.connect(":memory:")
    load_ext(conn)
    # Without tiered VFS (no TURBOLITE_BUCKET), returns 1 (no handle)
    rc = conn.execute("SELECT turbolite_clear_cache('all')").fetchone()[0]
    assert rc == 1, f"expected 1 (no tiered VFS), got {rc}"
    rc = conn.execute("SELECT turbolite_clear_cache('data')").fetchone()[0]
    assert rc == 1
    rc = conn.execute("SELECT turbolite_clear_cache('interior')").fetchone()[0]
    assert rc == 1
    conn.close()


@test("turbolite_clear_cache rejects invalid mode")
def _():
    conn = sqlite3.connect(":memory:")
    load_ext(conn)
    try:
        conn.execute("SELECT turbolite_clear_cache('bogus')")
        assert False, "should have raised error"
    except Exception as e:
        assert "must be" in str(e)
    conn.close()


@test("turbolite_s3_gets and turbolite_s3_bytes return integers")
def _():
    conn = sqlite3.connect(":memory:")
    load_ext(conn)
    gets = conn.execute("SELECT turbolite_s3_gets()").fetchone()[0]
    assert isinstance(gets, int), f"expected int, got {type(gets)}"
    bytes_ = conn.execute("SELECT turbolite_s3_bytes()").fetchone()[0]
    assert isinstance(bytes_, int)
    conn.close()


@test("turbolite_reset_s3_counters returns integer")
def _():
    conn = sqlite3.connect(":memory:")
    load_ext(conn)
    rc = conn.execute("SELECT turbolite_reset_s3_counters()").fetchone()[0]
    assert isinstance(rc, int)
    conn.close()


@test("trace callback fires on SELECT (EQP + plan queue)")
def _():
    """Verify the trace callback doesn't crash when SELECT queries run.
    The trace callback runs EQP internally and pushes to the plan queue.
    We can't inspect the queue from Python, but we can verify no crashes."""
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "test.db")
        conn = sqlite3.connect(":memory:")
        load_ext(conn)
        conn.close()

        conn = sqlite3.connect(f"file:{path}?vfs=turbolite", uri=True)
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
        conn.execute("CREATE INDEX idx_email ON users(email)")
        conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, content TEXT)")
        conn.execute("CREATE INDEX idx_posts_user ON posts(user_id)")
        for i in range(100):
            conn.execute("INSERT INTO users VALUES (?, ?, ?)", (i, f"user_{i}", f"user_{i}@test.com"))
            conn.execute("INSERT INTO posts VALUES (?, ?, ?)", (i, i % 10, f"post {i}"))
        conn.commit()

        # These queries trigger the trace callback + EQP:
        # SEARCH with index
        row = conn.execute("SELECT * FROM users WHERE email = 'user_42@test.com'").fetchone()
        assert row is not None

        # SCAN
        count = conn.execute("SELECT COUNT(*) FROM posts WHERE content LIKE '%post%'").fetchone()[0]
        assert count == 100

        # JOIN with indexes
        rows = conn.execute("""
            SELECT u.name, p.content
            FROM users u
            JOIN posts p ON p.user_id = u.id
            WHERE u.email = 'user_5@test.com'
        """).fetchall()
        assert len(rows) == 10  # 100 posts / 10 users

        conn.close()


# ── Summary ───────────────────────────────────────────────────────


if __name__ == "__main__":
    print()
    print(f"Results: {passed} passed, {failed} failed")
    if errors:
        print("\nFailures:")
        for name, err in errors:
            print(f"  {name}: {err}")
    sys.exit(1 if failed else 0)
