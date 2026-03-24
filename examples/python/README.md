# turbolite — Python example

A FastAPI server backed by turbolite-compressed SQLite. Uses `uv` for dependency management.

## Run

```bash
make lib-bundled
uv run examples/python/basic.py
```

Or via Make:

```bash
make example-python
```

## Try it

```bash
curl -X POST localhost:8000/books \
     -H 'Content-Type: application/json' \
     -d '{"title": "Dune", "year": 1965}'

curl localhost:8000/books
```

## How it works

1. Load `libsqlite_compress_encrypt_vfs.dylib` / `.so` via `ctypes.CDLL`
2. `turbolite_register_compressed` — register a zstd-compressed VFS
3. `turbolite_open` — open a database through the VFS
4. FastAPI routes call `turbolite_exec` (writes) and `turbolite_query_json` (reads)
