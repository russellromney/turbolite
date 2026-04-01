"""Tests for turbolite Python package: local mode + S3 mode."""
import os
import sqlite3
import tempfile
import shutil

import turbolite


def test_local_basic_crud():
    """Local mode: create table, insert, query."""
    print("test: local mode basic CRUD...")
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, "local.db")
        conn = turbolite.connect(db_path)
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        conn.execute("INSERT INTO t VALUES (1, 'hello')")
        conn.execute("INSERT INTO t VALUES (2, 'world')")
        conn.commit()
        rows = conn.execute("SELECT * FROM t ORDER BY id").fetchall()
        assert len(rows) == 2
        assert rows[0][1] == "hello"
        assert rows[1][1] == "world"
        conn.close()
    print("  PASS")


def test_s3_write_read():
    """S3 mode: write, checkpoint, verify page_size and WAL."""
    bucket = os.environ.get("TIERED_TEST_BUCKET") or os.environ.get("TURBOLITE_BUCKET")
    if not bucket:
        print("SKIP: S3 tests (no TIERED_TEST_BUCKET set)")
        return

    print("test: S3 mode write + read...")
    d = tempfile.mkdtemp()
    try:
        db_path = os.path.join(d, "s3_test.db")
        prefix = f"test/python-{os.getpid()}"
        conn = turbolite.connect(
            db_path,
            mode="s3",
            bucket=bucket,
            prefix=prefix,
            endpoint=os.environ.get("AWS_ENDPOINT_URL"),
            region="auto",
            cache_dir=d,
        )

        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        for i in range(50):
            conn.execute("INSERT INTO items VALUES (?, ?)", (i, f"item_{i}"))
        conn.commit()

        # Verify page size is 64KB
        page_size = conn.execute("PRAGMA page_size").fetchone()[0]
        assert page_size == 65536, f"expected 64KB pages, got {page_size}"

        # Verify WAL mode
        journal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert journal_mode == "wal", f"expected WAL mode, got {journal_mode}"

        # Checkpoint
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

        count = conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]
        assert count == 50

        name = conn.execute("SELECT name FROM items WHERE id = 25").fetchone()[0]
        assert name == "item_25"

        conn.close()
        print("  PASS")
    finally:
        shutil.rmtree(d, ignore_errors=True)


def test_s3_requires_bucket():
    """S3 mode without bucket should raise ValueError."""
    print("test: S3 mode requires bucket...")
    # Clear env to ensure no fallback
    old = os.environ.pop("TURBOLITE_BUCKET", None)
    try:
        turbolite.connect("/tmp/test.db", mode="s3")
        assert False, "should have raised ValueError"
    except ValueError as e:
        assert "bucket" in str(e).lower()
        print("  PASS")
    finally:
        if old is not None:
            os.environ["TURBOLITE_BUCKET"] = old


if __name__ == "__main__":
    # S3 write test must run FIRST: the extension init only runs once per
    # process, and TURBOLITE_BUCKET must be set before loading to register
    # turbolite-s3. Local mode works regardless of load order.
    test_s3_write_read()
    test_local_basic_crud()
    test_s3_requires_bucket()
    print("\nAll tests passed!")
