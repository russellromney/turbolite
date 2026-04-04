"""
turbolite -- SQLite with compressed page groups and optional S3 cloud storage for Python.

Local mode (default)::

    import turbolite
    conn = turbolite.connect("my.db")

S3 cloud mode::

    conn = turbolite.connect("my.db", mode="s3",
        bucket="my-bucket",
        endpoint="https://t3.storage.dev")
"""

from __future__ import annotations

import os
import platform
import sqlite3
import sys

__version__ = "0.3.0"


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
) -> sqlite3.Connection:
    """
    Open a turbolite database.

    Args:
        path: Path to the database file.
        mode: "local" for local VFS, "s3" for S3 cloud VFS.
        bucket: S3 bucket (required for mode="s3", or set TURBOLITE_BUCKET).
        prefix: S3 key prefix (default "turbolite").
        endpoint: S3 endpoint URL (Tigris, MinIO). Falls back to AWS_ENDPOINT_URL.
        region: AWS region. Falls back to AWS_REGION.
        cache_dir: Local cache directory (default /tmp/turbolite).
        compression_level: Zstd level 1-22 (default 3).
        prefetch_threads: Prefetch worker threads (default num_cpus + 1).
        read_only: Open in read-only mode.

    Returns:
        An open sqlite3.Connection.

    Raises:
        ValueError: If mode="s3" but no bucket is configured.
        RuntimeError: If the S3 VFS fails to initialize.
    """
    if mode not in ("local", "s3"):
        raise ValueError(f"mode must be 'local' or 's3', got {mode!r}")

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

    # Load extension. S3 env vars must be set BEFORE first load because the
    # C init function only runs once per process. If local mode was loaded
    # first without TURBOLITE_BUCKET, the S3 VFS won't be registered and
    # there's no way to add it later.
    needs_load = not _loaded_local or (mode == "s3" and not _loaded_s3)
    if needs_load:
        if _loaded_local and mode == "s3" and not _loaded_s3:
            raise RuntimeError(
                "Cannot switch to S3 mode after local mode was already loaded. "
                "The SQLite extension init only runs once per process. "
                "Use mode='s3' on the first turbolite.connect() call, or set "
                "TURBOLITE_BUCKET in the environment before importing turbolite."
            )
        bootstrap = sqlite3.connect(":memory:")
        load(bootstrap)
        bootstrap.close()

    vfs = "turbolite-s3" if mode == "s3" else "turbolite"
    conn = sqlite3.connect(f"file:{path}?vfs={vfs}", uri=True)

    if mode == "s3":
        # 64KB pages for fewer S3 round trips, WAL mode for concurrent reads
        conn.execute("PRAGMA page_size=65536")
        conn.execute("PRAGMA journal_mode=WAL")

    return conn
