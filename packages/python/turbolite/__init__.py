"""
turbolite — compressed SQLite for Python.

Usage::

    import sqlite3
    import turbolite

    conn = sqlite3.connect(":memory:")
    turbolite.load(conn)

    # Open a compressed database via URI
    conn2 = sqlite3.connect("file:my.db?vfs=turbolite", uri=True)

Or use the convenience wrapper::

    conn = turbolite.connect("my.db")
"""

from __future__ import annotations

import os
import platform
import sqlite3
import sys

__version__ = "0.1.0"


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
        # Return without extension — sqlite3.load_extension appends it
        return os.path.splitext(path)[0]

    raise FileNotFoundError(
        f"turbolite extension not found at {path}. "
        "Ensure the package was installed with the platform-specific binary."
    )


_loaded = False


def load(conn: sqlite3.Connection) -> None:
    """
    Load the turbolite extension into a sqlite3 connection.

    After loading, the "turbolite" VFS is registered process-wide.
    Open compressed databases with::

        sqlite3.connect("file:path.db?vfs=turbolite", uri=True)

    Args:
        conn: Any open sqlite3.Connection (can be :memory:).
    """
    global _loaded
    ext_path = _find_ext()
    conn.enable_load_extension(True)
    conn.load_extension(ext_path)
    _loaded = True


def connect(
    path: str,
    *,
    vfs: str = "turbolite",
) -> sqlite3.Connection:
    """
    Convenience: load the extension and open a compressed database.

    Equivalent to::

        conn = sqlite3.connect(":memory:")
        turbolite.load(conn)
        conn.close()
        return sqlite3.connect(f"file:{path}?vfs={vfs}", uri=True)

    Args:
        path: Path to the database file.
        vfs: VFS name (default "turbolite").

    Returns:
        An open sqlite3.Connection using the compressed VFS.
    """
    global _loaded
    if not _loaded:
        bootstrap = sqlite3.connect(":memory:")
        load(bootstrap)
        bootstrap.close()
    return sqlite3.connect(f"file:{path}?vfs={vfs}", uri=True)
