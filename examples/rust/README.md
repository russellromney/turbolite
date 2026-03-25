# turbolite — Rust examples

Uses the native Rust API directly, no FFI or shared library needed.

## Local compressed

```bash
cd examples/rust && cargo run --bin local
```

## S3 tiered

```bash
cd examples/rust && TURBOLITE_BUCKET=my-bucket cargo run --bin tiered
```

Or via Make:

```bash
make example-rust           # local
make example-rust-tiered    # S3 tiered
```

## What they do

Small CLIs that create a turbolite SQLite database, insert books, and query them.

## How it works

**Local** (`src/local.rs`):
1. `CompressedVfs::new(data_dir, 3)` - create a zstd-compressed VFS (level 3)
2. `register("demo", vfs)` - register it with SQLite's VFS layer
3. `Connection::open_with_flags_and_vfs(path, flags, "demo")` - open through the VFS
4. Standard `rusqlite` from here

**Tiered** (`src/tiered.rs`):
1. `TieredConfig { bucket, prefix, cache_dir, ... }` - configure S3 tiered storage
2. `TieredVfs::new(config)` - create the tiered VFS
3. `register("tiered", vfs)` - register it
4. Same `rusqlite` API from here
