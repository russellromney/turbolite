# turbolite

Compressed SQLite for Python. Transparent zstd compression via a custom VFS — works with the standard `sqlite3` module.

## Install

```bash
pip install turbolite
```

## Usage

```python
import turbolite

# File-first: /data/app.db is the local page image (turbolite-owned).
# /data/app.db-turbolite/ holds hidden implementation state
# (manifest, cache, staging logs).
conn = turbolite.connect("/data/app.db")
conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
conn.execute("INSERT INTO users VALUES (1, 'alice')")
conn.commit()

rows = conn.execute("SELECT * FROM users").fetchall()
print(rows)  # [(1, 'alice')]
conn.close()
```

`app.db` is turbolite's compressed page image. It is not promised to be
opened directly by stock `sqlite3`. To produce a normal SQLite file the
standard `sqlite3` CLI can read, use `iterdump`:

```python
src = turbolite.connect("/data/app.db")
dst = sqlite3.connect("/data/exported.sqlite")
dst.executescript("\n".join(src.iterdump()))
dst.close()
src.close()
```

### Lower-level entry point

If you need direct access to the SQLite extension (e.g. to register
multiple per-tenant VFSes from a single connection), call
`turbolite.load(conn)` and use the `turbolite_register_file_first_vfs(name,
db_path)` SQL function yourself. The bare `cache_dir`-based
`turbolite_register_vfs(name, cache_dir)` is still available for embedders
who want to manage the cache layout directly, but new code should use
`turbolite.connect()` or the file-first SQL function.

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
