# turbolite

Compressed SQLite for Python. Transparent zstd compression via a custom VFS — works with the standard `sqlite3` module.

## Install

```bash
pip install turbolite
```

## Usage

```python
import sqlite3
import turbolite

# Load the extension (registers the "turbolite" VFS process-wide)
conn = sqlite3.connect(":memory:")
turbolite.load(conn)
conn.close()

# Open a compressed database
conn = sqlite3.connect("file:my.db?vfs=turbolite", uri=True)
conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
conn.execute("INSERT INTO users VALUES (1, 'alice')")
conn.commit()

rows = conn.execute("SELECT * FROM users").fetchall()
print(rows)  # [(1, 'alice')]
conn.close()
```

### Convenience wrapper

```python
import turbolite

conn = turbolite.connect("my.db")
conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
conn.commit()
conn.close()
```

### SQL functions

The extension registers helper SQL functions:

```python
conn.execute("SELECT turbolite_version()").fetchone()
# ('0.1.0',)
```

## How it works

`turbolite.load(conn)` loads a platform-specific SQLite loadable extension (`.so`/`.dylib`) bundled inside the Python package. The extension registers a custom VFS named "turbolite" that transparently compresses pages with zstd.

Your existing `sqlite3` code works unchanged — just open with `?vfs=turbolite` in a URI connection string.

## Build from source

```bash
# Build the loadable extension
cd ../..
make ext

# Copy into the package
cp target/release/turbolite.dylib packages/python/turbolite/
# or on Linux: cp target/release/turbolite.so packages/python/turbolite/

# Install in development mode
cd packages/python
pip install -e .
```
