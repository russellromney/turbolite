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


def test_local_file_first_layout():
    """Local mode: app.db is the user-visible artifact and the sidecar lives
    at app.db-turbolite/local_state.msgpack — old split tracking files are
    not produced or required."""
    print("test: file-first layout...")
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, "app.db")
        conn = turbolite.connect(db_path)
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        conn.execute("INSERT INTO t VALUES (1, 'one')")
        conn.execute("INSERT INTO t VALUES (2, 'two')")
        conn.commit()
        conn.close()

        assert os.path.isfile(db_path), \
            f"file-first: {db_path} must exist after close"

        sidecar = turbolite.state_dir_for_database_path(db_path)
        assert sidecar == db_path + "-turbolite"
        assert os.path.isdir(sidecar), \
            f"file-first: sidecar dir {sidecar} must exist"

        # The consolidated local-state file is the one promised name.
        local_state = os.path.join(sidecar, "local_state.msgpack")
        assert os.path.isfile(local_state), (
            f"file-first: {local_state} must exist; the file-first contract "
            f"is one consolidated local-state file, not split tracking files."
        )

        # The pre-PR-#27 split DiskCache tracking files must not be produced.
        # In pure-local mode the local backend still writes its own
        # `manifest.msgpack` under the sidecar; that is a different
        # conceptual artifact (backend manifest vs. legacy local-cache
        # tracking) and is expected.
        for legacy in ("page_bitmap", "sub_chunk_tracker",
                       "cache_index.json", "dirty_groups.msgpack"):
            assert not os.path.isfile(os.path.join(sidecar, legacy)), (
                f"file-first: legacy split file {legacy} should not be "
                f"produced"
            )

        # Reopen and re-read the data.
        conn = turbolite.connect(db_path)
        rows = conn.execute("SELECT * FROM t ORDER BY id").fetchall()
        assert rows == [(1, "one"), (2, "two")]
        conn.close()
    print("  PASS")


def test_local_relative_path():
    """Local mode: connect resolves relative paths and lays the sidecar
    next to the resolved absolute path, not next to whatever cwd was."""
    print("test: file-first relative path...")
    with tempfile.TemporaryDirectory() as d:
        cwd = os.getcwd()
        try:
            os.chdir(d)
            conn = turbolite.connect("app.db")
            conn.execute("CREATE TABLE t (id INTEGER)")
            conn.execute("INSERT INTO t VALUES (42)")
            conn.commit()
            conn.close()
            assert os.path.isfile(os.path.join(d, "app.db"))
            assert os.path.isdir(os.path.join(d, "app.db-turbolite"))
        finally:
            os.chdir(cwd)
    print("  PASS")


def test_local_rejects_nonexistent_parent_dir():
    """connect() must surface a clear error when the parent directory
    doesn't exist, instead of producing a confusing SQLite error from
    inside the VFS."""
    print("test: file-first rejects nonexistent parent dir...")
    bogus = "/this-path-does-not-exist-12345/app.db"
    raised = False
    try:
        turbolite.connect(bogus)
    except FileNotFoundError as e:
        raised = "directory does not exist" in str(e)
    assert raised, "must raise FileNotFoundError for missing parent dir"
    print("  PASS")


def test_local_two_databases_isolated():
    """Local mode: two app.db files in different directories get distinct
    sidecars and do not share manifest state."""
    print("test: two file-first databases isolated...")
    with tempfile.TemporaryDirectory() as d:
        a = os.path.join(d, "a")
        b = os.path.join(d, "b")
        os.makedirs(a)
        os.makedirs(b)

        conn_a = turbolite.connect(os.path.join(a, "app.db"))
        conn_a.execute("CREATE TABLE t (v TEXT)")
        conn_a.execute("INSERT INTO t VALUES ('from-a')")
        conn_a.commit()
        conn_a.close()

        conn_b = turbolite.connect(os.path.join(b, "app.db"))
        conn_b.execute("CREATE TABLE t (v TEXT)")
        conn_b.execute("INSERT INTO t VALUES ('from-b')")
        conn_b.commit()
        conn_b.close()

        assert os.path.isdir(os.path.join(a, "app.db-turbolite"))
        assert os.path.isdir(os.path.join(b, "app.db-turbolite"))

        conn_a = turbolite.connect(os.path.join(a, "app.db"))
        assert conn_a.execute("SELECT v FROM t").fetchone() == ("from-a",)
        conn_a.close()

        conn_b = turbolite.connect(os.path.join(b, "app.db"))
        assert conn_b.execute("SELECT v FROM t").fetchone() == ("from-b",)
        conn_b.close()
    print("  PASS")


def test_local_export_to_stock_sqlite():
    """Local mode: iterdump replays through stock sqlite3 to produce a
    file the standard sqlite3 CLI can open. This is the supported export
    path; ``VACUUM INTO`` is not — file-first VFS rejects mismatched
    target paths to keep the local image identity sound."""
    print("test: file-first export to stock SQLite via iterdump...")
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, "app.db")
        export_path = os.path.join(d, "exported.sqlite")

        conn = turbolite.connect(db_path)
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO t VALUES (1, 'alpha')")
        conn.execute("INSERT INTO t VALUES (2, 'beta')")
        conn.commit()

        # iterdump emits its own BEGIN/COMMIT around DDL+DML; replay it as
        # one script so executescript()'s auto-commit doesn't fight it.
        dump = "\n".join(conn.iterdump())
        plain = sqlite3.connect(export_path)
        try:
            plain.executescript(dump)
        finally:
            plain.close()
        conn.close()

        assert os.path.isfile(export_path)
        # Stock sqlite3 (no turbolite VFS) opens the export.
        plain = sqlite3.connect(export_path)
        rows = plain.execute("SELECT id, name FROM t ORDER BY id").fetchall()
        assert rows == [(1, "alpha"), (2, "beta")]
        plain.close()
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


def test_s3_two_prefixes_same_process_do_not_cross():
    """S3 mode: two prefixes in one process must get distinct VFS identities."""
    bucket = os.environ.get("TIERED_TEST_BUCKET") or os.environ.get("TURBOLITE_BUCKET")
    if not bucket:
        print("SKIP: S3 tests (no TIERED_TEST_BUCKET set)")
        return

    print("test: S3 two-prefix isolation in one process...")
    d = tempfile.mkdtemp()
    try:
        prefix_base = f"test/python-isolation-{os.getpid()}"
        conn_a = turbolite.connect(
            os.path.join(d, "a.db"),
            mode="s3",
            bucket=bucket,
            prefix=f"{prefix_base}/a",
            endpoint=os.environ.get("AWS_ENDPOINT_URL"),
            region="auto",
            cache_dir=os.path.join(d, "a-cache"),
        )
        conn_b = turbolite.connect(
            os.path.join(d, "b.db"),
            mode="s3",
            bucket=bucket,
            prefix=f"{prefix_base}/b",
            endpoint=os.environ.get("AWS_ENDPOINT_URL"),
            region="auto",
            cache_dir=os.path.join(d, "b-cache"),
        )

        conn_a.execute("CREATE TABLE marker (value TEXT)")
        conn_a.execute("INSERT INTO marker VALUES ('from-a')")
        conn_a.commit()
        conn_b.execute("CREATE TABLE marker (value TEXT)")
        conn_b.execute("INSERT INTO marker VALUES ('from-b')")
        conn_b.commit()

        assert conn_a.execute("SELECT value FROM marker").fetchone()[0] == "from-a"
        assert conn_b.execute("SELECT value FROM marker").fetchone()[0] == "from-b"

        conn_a.close()
        conn_b.close()
        print("  PASS")
    finally:
        shutil.rmtree(d, ignore_errors=True)


def test_s3_default_prefixes_do_not_cross():
    """S3 mode: two paths with default prefixes must not share remote state."""
    bucket = os.environ.get("TIERED_TEST_BUCKET") or os.environ.get("TURBOLITE_BUCKET")
    if not bucket:
        print("SKIP: S3 tests (no TIERED_TEST_BUCKET set)")
        return

    print("test: S3 default-prefix isolation in one process...")
    d = tempfile.mkdtemp()
    try:
        conn_a = turbolite.connect(
            os.path.join(d, "default-a.db"),
            mode="s3",
            bucket=bucket,
            endpoint=os.environ.get("AWS_ENDPOINT_URL"),
            region="auto",
            cache_dir=os.path.join(d, "default-a-cache"),
        )
        conn_b = turbolite.connect(
            os.path.join(d, "default-b.db"),
            mode="s3",
            bucket=bucket,
            endpoint=os.environ.get("AWS_ENDPOINT_URL"),
            region="auto",
            cache_dir=os.path.join(d, "default-b-cache"),
        )

        conn_a.execute("CREATE TABLE marker (value TEXT)")
        conn_a.execute("INSERT INTO marker VALUES ('default-a')")
        conn_a.commit()
        conn_b.execute("CREATE TABLE marker (value TEXT)")
        conn_b.execute("INSERT INTO marker VALUES ('default-b')")
        conn_b.commit()

        assert conn_a.execute("SELECT value FROM marker").fetchone()[0] == "default-a"
        assert conn_b.execute("SELECT value FROM marker").fetchone()[0] == "default-b"

        conn_a.close()
        conn_b.close()
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
    # S3 tests can run before or after local tests: S3 mode now registers
    # a per-volume VFS instead of relying on process-global turbolite-s3.
    test_s3_write_read()
    test_s3_two_prefixes_same_process_do_not_cross()
    test_s3_default_prefixes_do_not_cross()
    test_local_basic_crud()
    test_local_file_first_layout()
    test_local_relative_path()
    test_local_rejects_nonexistent_parent_dir()
    test_local_two_databases_isolated()
    test_local_export_to_stock_sqlite()
    test_s3_requires_bucket()
    print("\nAll tests passed!")
