"""
turbolite -- SQLite with compressed page groups and optional S3 cloud storage for Python.

Local mode (default), file-first::

    import turbolite
    conn = turbolite.connect("/data/app.db")
    # /data/app.db is the local page image (turbolite-owned).
    # /data/app.db-turbolite/ holds hidden implementation state
    # (manifest, cache, staging logs).

S3 cloud mode::

    conn = turbolite.connect("/data/app.db", mode="s3",
        bucket="my-bucket",
        endpoint="https://t3.storage.dev")

Note: ``app.db`` is turbolite's compressed page image. It is not promised
to be opened directly by stock ``sqlite3``. To produce a stock SQLite
file, replay ``conn.iterdump()`` against a fresh ``sqlite3.connect``.
``VACUUM INTO`` is not supported as an export path — the file-first VFS
rejects the alias open of a different target file by design.
"""

from __future__ import annotations

import itertools
import os
import platform
import sqlite3
import sys

__version__ = "0.4.1"


# Per-database VFS counter used to keep file-first registrations isolated.
# Each connect() call gets its own VFS so manifest/cache/sidecar state stays
# pinned to one database file.
_vfs_counter = itertools.count()


def _find_ext() -> str:
    """Find the bundled loadable extension binary."""
    pkg_dir = os.path.dirname(os.path.abspath(__file__))
    system = platform.system()

    if system == "Darwin":
        name = "turbolite.dylib"
    elif system == "Windows":
        name = "turbolite.dll"
    else:
        name = "turbolite.so"

    path = os.path.join(pkg_dir, name)
    if os.path.isfile(path):
        return os.path.splitext(path)[0]

    raise FileNotFoundError(
        f"turbolite extension not found at {path}. "
        "Ensure the package was installed with the platform-specific binary."
    )


_loaded_local = False
_loaded_s3 = False


def load(conn: sqlite3.Connection) -> None:
    """
    Load the turbolite extension into a sqlite3 connection.

    After loading, the "turbolite" VFS (local mode) is always registered.
    If TURBOLITE_BUCKET is set in the environment, "turbolite-s3"
    (S3 cloud mode) is also registered.

    Args:
        conn: Any open sqlite3.Connection (can be :memory:).
    """
    global _loaded_local, _loaded_s3
    ext_path = _find_ext()
    conn.enable_load_extension(True)
    conn.load_extension(ext_path)
    _loaded_local = True
    if os.environ.get("TURBOLITE_BUCKET"):
        _loaded_s3 = True


# Bootstrap connection for extension loading and per-database VFS
# registration. Lazily created and kept alive for the process lifetime so
# every connect() call can run turbolite_register_file_first_vfs(...).
_bootstrap: sqlite3.Connection | None = None


def _ensure_bootstrap() -> sqlite3.Connection:
    global _bootstrap
    if _bootstrap is not None:
        return _bootstrap
    _bootstrap = sqlite3.connect(":memory:")
    load(_bootstrap)
    return _bootstrap


def state_dir_for_database_path(path: str) -> str:
    """Return the hidden sidecar directory for a file-first database path.

    For ``/data/app.db`` this is ``/data/app.db-turbolite/``. Bindings can
    use it to assert layout in tests or to resolve sibling artifacts.
    """
    return f"{os.fspath(path)}-turbolite"


def connect(
    path: str,
    *,
    mode: str = "local",
    bucket: str | None = None,
    prefix: str | None = None,
    endpoint: str | None = None,
    region: str | None = None,
    cache_dir: str | None = None,
    compression_level: int | None = None,
    prefetch_threads: int | None = None,
    read_only: bool = False,
    page_cache: str = "64MB",
) -> sqlite3.Connection:
    """
    Open a turbolite database, file-first.

    The user-supplied ``path`` (e.g. ``/data/app.db``) is the local page
    image. Hidden implementation state (manifest, cache, staging logs) lives
    next to it under ``<path>-turbolite/``.

    Args:
        path: Path to the database file. May be relative or absolute.
        mode: "local" for local VFS, "s3" for S3 cloud VFS.
        bucket: S3 bucket (required for mode="s3", or set TURBOLITE_BUCKET).
        prefix: S3 key prefix (default "turbolite").
        endpoint: S3 endpoint URL (Tigris, MinIO). Falls back to AWS_ENDPOINT_URL.
        region: AWS region. Falls back to AWS_REGION.
        cache_dir: Lower-level override for the sidecar directory. When unset
            (the default) the sidecar lives at ``<path>-turbolite``.
        compression_level: Zstd level 1-22 (default 3).
        prefetch_threads: Prefetch worker threads (default num_cpus + 1).
        read_only: Open in read-only mode.
        page_cache: In-memory page cache size (default "64MB"). turbolite manages
            its own manifest-aware page cache. Set to "0" to disable.

    Returns:
        An open sqlite3.Connection.

    Raises:
        ValueError: If mode="s3" but no bucket is configured.
        RuntimeError: If the S3 VFS fails to initialize.
    """
    if mode not in ("local", "s3"):
        raise ValueError(f"mode must be 'local' or 's3', got {mode!r}")

    abs_path = os.path.abspath(path)
    parent = os.path.dirname(abs_path)
    if parent and not os.path.isdir(parent):
        raise FileNotFoundError(
            f"turbolite: directory does not exist: {parent}"
        )

    if mode == "s3":
        # Set env vars BEFORE loading the extension. The C init function
        # checks TURBOLITE_BUCKET to decide whether to register turbolite-s3.
        # Once loaded, the VFS is registered for the process lifetime.
        effective_bucket = bucket or os.environ.get("TURBOLITE_BUCKET")
        if not effective_bucket:
            raise ValueError(
                "mode='s3' requires a bucket. Pass bucket= or set TURBOLITE_BUCKET."
            )
        os.environ["TURBOLITE_BUCKET"] = effective_bucket
        if prefix is not None:
            os.environ["TURBOLITE_PREFIX"] = prefix
        if endpoint is not None:
            os.environ["TURBOLITE_ENDPOINT_URL"] = endpoint
        if region is not None:
            os.environ["TURBOLITE_REGION"] = region
        if cache_dir is not None:
            os.environ["TURBOLITE_CACHE_DIR"] = cache_dir
        if read_only:
            os.environ["TURBOLITE_READ_ONLY"] = "true"

    if compression_level is not None:
        os.environ["TURBOLITE_COMPRESSION_LEVEL"] = str(compression_level)
    if prefetch_threads is not None:
        os.environ["TURBOLITE_PREFETCH_THREADS"] = str(prefetch_threads)
    os.environ["TURBOLITE_MEM_CACHE_BUDGET"] = page_cache

    # Load the extension once. S3 env vars must be set BEFORE first load
    # because the C init function only runs once per process.
    if mode == "s3" and _loaded_local and not _loaded_s3:
        raise RuntimeError(
            "Cannot switch to S3 mode after local mode was already loaded. "
            "The SQLite extension init only runs once per process. "
            "Use mode='s3' on the first turbolite.connect() call, or set "
            "TURBOLITE_BUCKET in the environment before importing turbolite."
        )
    boot = _ensure_bootstrap()

    if mode == "local":
        # Per-database VFS keyed to the file path. The SQL function
        # turbolite_register_file_first_vfs makes the user-supplied path
        # the local image and puts sidecar state at <path>-turbolite/.
        vfs = f"turbolite-py-{next(_vfs_counter)}"
        rc = boot.execute(
            "SELECT turbolite_register_file_first_vfs(?, ?)",
            (vfs, abs_path),
        ).fetchone()[0]
        if rc != 0:
            raise RuntimeError(
                f"turbolite: failed to register file-first VFS '{vfs}' for {abs_path}"
            )
    else:
        if not _loaded_s3:
            raise RuntimeError(
                "Cannot use S3 mode: TURBOLITE_BUCKET was not set when the extension "
                "was first loaded. Set TURBOLITE_BUCKET in the environment before "
                "the first turbolite.connect() call."
            )
        vfs = "turbolite-s3"

    conn = sqlite3.connect(f"file:{abs_path}?vfs={vfs}", uri=True)

    # turbolite manages its own manifest-aware page cache. Disable SQLite's
    # built-in page cache so all reads go through turbolite's VFS, which
    # correctly invalidates on manifest change (replication, checkpoint).
    conn.execute("PRAGMA cache_size=0")

    if mode == "s3":
        # 64KB pages for fewer S3 round trips, WAL mode for concurrent reads
        conn.execute("PRAGMA page_size=65536")
        conn.execute("PRAGMA journal_mode=WAL")

    return conn
