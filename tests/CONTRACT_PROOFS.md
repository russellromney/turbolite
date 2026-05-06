# Turbolite Contract Proofs

Last updated: 2026-05-06

Turbolite's current user promise is:

> A user can put a SQLite database in object storage, cold-open it through
> Turbolite, query normal SQLite data, and export/download/validate the same
> database without silent corruption.

This file records the committed tests that prove that promise and the exact
commands used for the latest proof pass.

## Canonical Workload

The canonical CLI workload lives in `tests/cli_s3_test.rs`.

It creates a normal SQLite database with:

- `users`, `posts`, and `events` tables
- foreign keys and secondary indexes
- blob/text payloads large enough to force many pages
- updates, deletes, reinserts, `ANALYZE`, and `VACUUM`
- `PRAGMA integrity_check` and `PRAGMA foreign_key_check`
- a deterministic logical checksum over table counts, ids, row versions,
  payload lengths, and expected indexes

Negative control:

- `test_validate_fails_when_page_group_object_is_missing` deletes a live page
  group and requires validation to fail loudly.
- `test_live_vfs_write_checkpoint_reopen_export_on_s3` writes through the VFS,
  checkpoints, cold-reopens from an empty cache, exports, and compares the
  canonical checksum. Removing the checkpoint would turn this into a local-cache
  test and the cold export would fail to prove object-store durability.

## Compatibility Boundary

Turbolite is still experimental and does not keep migration shims for old
internal object formats or old split local metadata. Current proofs require:

- raw manifest payloads, not the removed local wrapper format
- pure page/object payloads, not the removed hybrid payload tag
- unified `local_state.msgpack`, not legacy split sidecar files
- a Turbolite-owned local database image, not stock SQLite compatibility

The intended user-level compatibility path is export/import through normal
SQLite bytes. Directly opening a Turbolite local image with stock `sqlite3` is
not a supported contract.

## Latest Proof Pass

### Rust Library

Command:

```bash
cargo test -p turbolite --lib
```

Result:

- 530 passed
- Backend: local/in-memory test backends
- Proves: core VFS, manifest/page replay, local state, cache, compaction,
  checkpoint/delta contract validators, and regression invariants

### S3-Compatible Full Tiered Suite

Command:

```bash
source ~/.zshrc
soup run -p turbolite -- cargo test -p turbolite --features cli-s3,zstd,encryption --test tiered -- --test-threads=1 --nocapture
```

Result:

- 143 passed
- Backend: Tigris S3-compatible (`TIERED_TEST_BUCKET=sqlces-test`,
  `AWS_ENDPOINT_URL=https://t3.storage.dev`)
- Proves: full tiered behavior against real object storage compatibility shape,
  including cold reads, import/materialize, staging/crash flush, GC, compaction,
  prefetch/eviction, oracle comparisons, manifest persistence, compression, and
  encryption

### Primary Real AWS Full Tiered Suite

Command:

```bash
source ~/.zshrc
soup run -p turbolite -e real-aws -- sh -lc \
  'unset AWS_ENDPOINT_URL AWS_ENDPOINT_URL_S3; CARGO_TARGET_DIR=/tmp/turbolite-bedrock-target cargo test --features cli-s3,zstd,encryption --test tiered -- --test-threads=1 --nocapture'
```

Result:

- 143 passed
- Backend: AWS S3 primary proof (`turbolite-real-s3-bedrock-...` bucket,
  `us-east-2`; credentials supplied by Soup)
- Proves: the full tiered matrix against primary AWS S3, including durable and
  local-then-flush S3 modes, plain, zstd, encrypted, zstd+encrypted, crash
  flush, staging, compaction, GC, import/materialize, drift, eviction, and
  vanilla-SQLite oracle comparisons

### Local Crash Recovery Sidecar

Command:

```bash
cargo test -p turbolite --test crash_recovery_manifest -- --nocapture
```

Result:

- 2 passed
- Backend: in-memory backend plus local sidecar state
- Proves: `CheckpointMode::LocalThenFlush` pending work survives reopen through
  unified `local_state.msgpack`, and stale split dirty-group files are not part
  of the current recovery contract

### Primary Real AWS CLI S3 Contract

Command:

```bash
source ~/.zshrc
soup run -p turbolite -e real-aws -- sh -lc \
  'unset AWS_ENDPOINT_URL AWS_ENDPOINT_URL_S3; CARGO_TARGET_DIR=/tmp/turbolite-bedrock-target cargo test --features cli-s3,zstd --test cli_s3_test -- --test-threads=1 --nocapture'
```

Result:

- 6 passed
- Backend: AWS S3 primary proof (`turbolite-real-s3-bedrock-...` bucket,
  `us-east-2`; credentials supplied by Soup)
- Proves: CLI import/info/export, live VFS write/checkpoint/cold export,
  shell query, download, validate, and missing-object failure

### S3-Compatible CLI Contract

Command:

```bash
source ~/.zshrc
soup run -p turbolite -- cargo test --test cli_s3_test --features cli-s3,zstd -- --nocapture
```

Result:

- 6 passed
- Backend: Tigris S3-compatible
- Proves: same CLI contract as the AWS proof against the object-store shape used
  in local development

### Encryption Against Object Storage

Command:

```bash
source ~/.zshrc
soup run -p turbolite -- cargo test -p turbolite --features cli-s3,zstd,encryption --test tiered tiered::encryption -- --test-threads=1 --nocapture
```

Result:

- 9 passed
- Backend: Tigris S3-compatible
- Proves: encrypted write/cold-read, wrong-key failure, key rotation, add/remove
  encryption, GC after rotation, and all-page-type encrypted cold start

### Compression And Encryption Focus

Commands:

```bash
cargo test -p turbolite --features zstd,encryption --test property_compression -- --nocapture
cargo test -p turbolite --features zstd,encryption --test property_encryption -- --nocapture
cargo test -p turbolite --features zstd,encryption --lib tiered::encoding -- --nocapture
cargo test -p turbolite --features zstd,encryption --lib tiered::disk_cache::tests::test_compressed -- --nocapture
cargo test -p turbolite --features zstd,encryption --lib tiered::rotation::tests -- --nocapture
cargo test -p turbolite --features zstd,encryption --lib dict::tests -- --nocapture
```

Result:

- Property compression: 11 passed
- Property encryption: 14 passed
- Encoding: 46 passed
- Disk cache compression/encryption: 18 passed
- Rotation: 41 passed
- Dictionary compression: 2 passed
- Proves: zstd roundtrip/determinism/corruption behavior, dictionary training,
  encrypted page/cache/object encoding, wrong-key and tamper failures, nonce
  uniqueness, compressed encrypted persistence, and encryption add/remove/rotate
  behavior

### WAL Integration

Command:

```bash
source ~/.zshrc
soup run -p turbolite -- cargo test -p turbolite --features cli-s3,zstd,wal --test tiered tiered::wal_integration -- --test-threads=1 --nocapture
```

Result:

- 6 passed
- Backend: Tigris S3-compatible
- Proves: WAL crash recovery, checkpoint plus WAL recovery, large-data WAL
  recovery, no-WAL no-op, WAL GC after checkpoint, and manifest version/change
  counter agreement

### Loadable Extension

Command:

```bash
make -C turbolite-ffi test-ext
```

Result:

- 23 passed
- Backend: local extension VFS
- Proves: SQLite loadable extension loads, registers VFS, creates/updates/deletes
  data, handles multiple SQLite types, persists across reopen, exposes cache/S3
  counter functions, and fires trace callbacks

### FFI And Language Bindings

Command:

```bash
source ~/.zshrc
soup run -p turbolite -- make -C turbolite-ffi test-ffi-tiered
```

Result:

- Python ctypes: 18 passed
- C: 13 passed
- Go cgo: 14 passed
- Node.js: 15 passed
- Backend: local VFS plus Tigris S3-compatible tiered VFS
- Proves: language bindings can register local and S3-backed VFSes, open a
  database, execute SQL, query rows, persist across reconnect, and reject null
  tiered configuration

## Known Not-Yet-Proven Paths

- Browser/OPFS package tests are separate from the native SQLite VFS proof.
- Multi-node HA/failover is not a standalone Turbolite claim; it belongs in
  `haqlite-turbolite`.
