# turbolite — Rust example

Uses the native Rust API directly — no FFI, no shared library needed.

## Run

```bash
cd examples/rust && cargo run
```

Or via Make:

```bash
make example-rust
```

## What it does

A small CLI that creates a compressed SQLite database, inserts books, and queries them.
Shows the core pattern: `CompressedVfs::new` → `register` → `Connection::open_with_flags_and_vfs` → normal rusqlite usage.

## How it works

1. `CompressedVfs::new(data_dir, 3)` — create a zstd-compressed VFS (level 3)
2. `register("demo", vfs)` — register it with SQLite's VFS layer
3. `Connection::open_with_flags_and_vfs(path, flags, "demo")` — open a database through the VFS
4. Standard `rusqlite` from here — `execute_batch`, `prepare`, `query_map`
