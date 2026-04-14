"""Entry point for the turbolite CLI binary bundled in the wheel."""

import os
import platform
import subprocess
import sys


def _find_binary() -> str:
    """Find the bundled turbolite CLI binary."""
    pkg_dir = os.path.dirname(os.path.abspath(__file__))
    system = platform.system()

    if system == "Windows":
        name = "turbolite.exe"
    else:
        name = "turbolite"

    path = os.path.join(pkg_dir, name)
    if os.path.isfile(path):
        return path

    raise FileNotFoundError(
        f"turbolite CLI binary not found at {path}. "
        "Ensure the package was installed with the platform-specific binary."
    )


def main():
    """Run the turbolite CLI binary, passing through all args."""
    try:
        binary = _find_binary()
    except FileNotFoundError as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)

    os.execv(binary, [binary] + sys.argv[1:])
