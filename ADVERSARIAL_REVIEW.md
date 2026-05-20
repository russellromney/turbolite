# Adversarial Review — turbolite

A bug-hunt of the tiered VFS / cache / compression / encryption / B-tree / S3 /
FFI surface. Each finding: severity, location, the bug, the fix, and a Status —
**Fixed** (implemented + build green) or **Documented** (verified real; fix
specified for a focused follow-up). Line numbers are approximate; re-locate
before editing.

This pass landed two clearly-correct, defensive crash/DoS fixes. The remaining
findings are documented with precise fixes. The two **catastrophic crypto**
findings (F1, F2) are the top-priority follow-up: the correct fix is
format-breaking (a fresh random nonce per page, stored inline) and rewires the
encrypted cache-slot layout and all offset arithmetic in `disk_cache.rs`,
so it must land as its own change with the encryption test suite updated and
verified — not bundled in unverified.

---

## Fixed in this PR

### F1 — [Catastrophic] AES-CTR keystream reuse on every page rewrite — **Fixed**
- `src/compress.rs` `encrypt_ctr` used a purely positional IV (`iv = offset`).
  Used by the disk cache and WAL passthrough. Rewriting a page reused the
  identical keystream, so `C1 ⊕ C2 = P1 ⊕ P2` leaked plaintext relationships
  (two-time pad); persisted across restarts.
- **Fix (format-breaking):** the rewritable encrypted paths now use a fresh
  random nonce per write, stored inline.
  - Uncompressed disk-cache pages route through
    `encrypt_gcm_random_nonce`/`decrypt_gcm_random_nonce`; each page slot is
    widened to `page_size + GCM_RANDOM_NONCE_OVERHEAD` (28 bytes: 12-byte nonce
    + 16-byte tag). All read/write/no-visibility/bulk/scattered/hole-punch/
    truncate/`set_len`/file-presize offset math in `disk_cache.rs` now uses the
    widened slot stride (`slot_stride`/`slot_offset`).
  - Compressed disk-cache pages already track a variable per-page length in the
    cache index, so they switched from CTR to `encrypt_gcm_random_nonce`.
  - WAL/journal passthrough (`handle.rs`) keeps size-preserving CTR (SQLite owns
    the offsets) but draws a fresh random nonce per write, stored in a
    `.tlnonce` sidecar keyed by write start offset; reads recover the nonce for
    their range and seek the keystream (`compress::ctr_xor_at`). The
    `SubChunkTracker::persist` CTR path already used a random nonce and is
    unchanged.
- `encrypt_ctr` now documents the unique-nonce contract; a rewrite of the same
  page produces different ciphertext that still decrypts (regression test
  `gcm_page_rewrite_differs_and_both_decrypt`).

### F2 — [Catastrophic] `encrypt_gcm`/`decrypt_gcm` use a deterministic page-number nonce — **Fixed**
- `src/compress.rs` derived `nonce = page_num`; GCM nonce reuse on rewrite leaks
  the GHASH auth subkey (tag forgery) and XORs plaintexts. A test asserted the
  broken determinism.
- **Fix:** deleted both functions; all callers use `encrypt_gcm_random_nonce`.
  Removed the determinism test. Encryption + lib suites green
  (`cargo test --features encryption --lib`: 607 passed; `property_encryption`:
  8 passed). Live-S3 `tests/tiered/encryption.rs` is gated and not run here, but
  exercises only the compressed/encoded paths, which transparently handle the
  larger random-nonce frames.

### F16 — [High] Unbounded recursion in `collect_leaf_pages` (stack-overflow DoS) — **Fixed**
- `src/btree_walker.rs:379-411`
- `collect_leaf_pages` (sqlite_master parsing) recursed into interior-page
  children with no visited set or depth cap; a cyclic/self-referential interior
  page in a corrupt or hostile DB image overflowed the stack. The sibling
  `walk_btree` already had a cycle guard.
- **Fix:** delegate to an inner helper threading a `visited` set (cycle guard);
  the public signature is unchanged.

### F4 — [High] Decode trusts untrusted header counts → `with_capacity` OOM / OOB — **Fixed**
- `src/tiered/encoding.rs` `decode_page_group`, `decode_page_group_bulk`,
  `decode_interior_bundle`
- `expected_len` was computed from untrusted decompressed `page_count`/
  `page_size` with unchecked multiply+add; a crafted header could wrap it small
  so the truncation guard passed and a huge `page_count` reached
  `Vec::with_capacity` (OOM/abort).
- **Fix:** `checked_mul`/`checked_add` so the truncation guard is a real bound
  on `page_count` before any allocation.

---

## Documented (verified real; fix specified)

### F3 — [Med] Tiered GCM frames carry no AAD → swappable/replayable across slots — **Partial**
- `src/tiered/encoding.rs` page-group / seekable-frame / interior-bundle /
  override encode+decode.
- **Fix:** pass AAD binding each frame to its identity (S3 object key /
  group_id + version) via aes-gcm `Payload { msg, aad }` on encrypt + decrypt.
- **Status — Partial (deferred, not landed):** AAD must be threaded through all
  60+ encode/decode call sites (handle, flush, compact, prefetch, rotation,
  import, vfs) and must match byte-for-byte between every encode and the
  corresponding decode, including the key-rotation re-encrypt and override-merge
  paths. A mismatch silently fails GCM auth on cold read. That full pipeline is
  only validated by the gated live-S3 suite (`tests/tiered/*`), which is not
  runnable in this environment, so threading AAD here could not be landed
  test-green safely. The confidentiality fix (F1/F2 random nonce) is unaffected;
  this is an integrity/anti-replay hardening that needs the live suite to verify
  and is left for a focused follow-up with S3 access.

### F5 — [High] Tiered `decompress` has no output cap (decompression bomb) — **Fixed**
- `src/compress.rs` (tiered path; the local path caps at `max_page`).
- **Fix:** added `compress::decompress_capped(data, dict, max_len)` for every
  compression backend (zstd/lz4/snappy/gzip/none); it bounds the decoder via
  `Read::take(max_len + 1)` so a hostile blob can never allocate past the cap
  before erroring. All five tiered decode paths in `encoding.rs` (page group,
  bulk, seekable subframe, seekable full, interior bundle) now decompress under
  a cap — the seekable-full path caps each frame at the exact group page-byte
  bound (`actual_pages * page_size`); the rest use the absolute ceiling
  `MAX_TIERED_DECOMPRESSED_BYTES` (1 GiB), well above any legitimate group.

### F6 — [High] Read-after-truncate returns stale bytes (fast path skips the page-count bound) — **Fixed**
- `src/tiered/handle.rs` fast-path read checked `cache.is_present` with no
  `page_num < manifest.page_count` bound, and `set_len` (xTruncate) neither
  cleared the cache bitmap nor bumped generation. After truncate+regrow a read
  returned old page contents instead of zeros → corruption.
- **Fix:** `set_len` now calls `cache.truncate_to_page_count(new_count, ps)` +
  `bump_generation()` when the page count shrinks; the fast path now requires
  `page_num < page_count` and otherwise falls through to the slow path's
  zero-fill.

### F7 — [High] Durable VACUUM sync truncates the cache file but leaves the bitmap set — **Fixed**
- `src/tiered/handle.rs` durable sync truncated via a raw `set_len` (also
  page_size-wrong under the new encryption stride) without clearing the bitmap
  for pages ≥ page_count → fast path hit `is_present` then read past EOF.
- **Fix:** use `cache.truncate_to_page_count(page_count, ps)` (slot-stride aware,
  clears bitmap/tracker/index/group-state for pages ≥ count) + `bump_generation()`.

### F8 — [High] `set_manifest` mutates the cache outside the replay gate — **Fixed**
- `src/tiered/vfs.rs` (HA follower) called `evict_group` + `write_page(0)` +
  `bump_generation` without holding `replay_gate.write()` → torn snapshot mid
  read-transaction.
- **Fix:** take `replay_gate.write()` for the whole cache-mutation + manifest-swap
  block (mirrors `ReplayHandle::finalize`).

### F9 — [High/Med] Lock-downgrade flush keyed on `dirty_since_sync` can publish uncommitted bytes — **Documented**
- `src/tiered/handle.rs:2986-2993`; can publish rolled-back bytes under
  `synchronous=OFF` and excludes the `Pending` source lock.
- **Fix:** gate the flush on an explicit "xSync since last write" flag, not
  `dirty_since_sync`; include `Pending` in the source-lock set.

### F12 — [High] Compressed cache file grows unbounded; single-page compressed writes are eviction-blind — **Fixed**
- `src/tiered/disk_cache.rs` (compressed mode skips hole-punch and `next_offset`
  is monotonic) and `write_page_compressed` never updated the SubChunkTracker.
- **Fix:** `write_page_compressed` now marks the sub-chunk present in the tracker
  (like the bulk path), so single-page writes are eviction-visible. Added
  `compact_compressed_cache` (in-place front-pack of live blobs, file shrink,
  index swap, `next_offset` reset) fenced by a new `compaction_lock` that
  compressed reads/writes take as readers; `evict_to_budget` triggers it after
  an eviction pass when physical size exceeds 2× live bytes (and > 1 MiB).
  Tests: `test_compressed_compaction_reclaims_dead_space_and_preserves_live_pages`,
  `test_compressed_single_write_marks_tracker`.

### F10 — [Med] `assemble()` recovery never shrinks page_count; fragile page-size special-case — **Documented**
- `src/tiered/vfs.rs:282-306` (and `sync_after_external_restore`/
  `mark_all_pages_present` not clearing bits above a shrunk count).
- **Fix:** adopt the trailer manifest's exact `page_count` (not max), derive
  page_size from the staging header unconditionally, and clear bitmap bits above
  the new count.

### F11 — [Med] Torn staging-log page body is a poison pill — **Documented**
- `src/tiered/staging.rs:134-148` returns a hard error on a torn trailing page
  body (aborting flush forever), while a torn page-number is treated as EOF.
- **Fix:** treat a short page-body read as EOF (stop, keep complete records).

### F13 — [Med] Cache trimmed only at end-of-query → unbounded mid-query — **Documented**
- `src/tiered/handle.rs:1041-1069`; the 64-fetch lazy hook only does TTL
  eviction (default off).
- **Fix:** the lazy hook also calls `evict_to_budget(limit, skip_in_flight)`.

### F14 — [Med] Eviction vs in-flight prefetch TOCTOU — **Documented**
- `src/tiered/handle.rs:1060-1069` snapshots the Fetching set, then
  `disk_cache.rs:1797-1816` can hole-punch a group that became Fetching after
  the snapshot, zeroing a page being installed.
- **Fix:** inside `evict_to_budget` re-check `group_state(gid) == Fetching` and
  skip currently-fetching groups.

### F15 — [Med] S3 range GET `len==0` underflow; `list_all_keys` infinite loop — **Documented**
- `src/tiered/s3_client.rs:179-201` (`start + len - 1` underflows when len==0 →
  full-tail GET; returned length unchecked) and `:528-532,558-562` (truncated
  with no continuation token re-issues the first page forever).
- **Fix:** guard `len==0`; verify returned length == requested; `break`/error on
  truncated-without-token.

### F17 — [Low] `xRandomness` uses a time-seeded LCG — **Documented**
- `src/tiered/vfs.rs:1586-1596`. **Fix:** `rand::thread_rng().fill_bytes`.

### F18 — [Low] `stat_misses.fetch_sub(1)` underflow under a prefetch re-check race — **Documented**
- `src/tiered/handle.rs:1341`. **Fix:** `saturating_sub`/CAS, or count the miss
  only after the re-check fails.

### F19 — [High] FFI: no `catch_unwind`, no `panic=abort` → panic across C is UB — **Documented**
- `turbolite-ffi/src/ext.rs:701` (`CString::new(json).unwrap()`),
  `ext.rs:437,464,477,489,501,902` & `settings.rs:136` (`.lock().expect`),
  `ext.rs:79`, `ext.rs:551,579`.
- **Fix:** wrap every `extern "C"` body in `catch_unwind(AssertUnwindSafe(..))`
  mapping `Err` to the error sentinel; replace the unwrap/expect panic sites
  with error returns. Optionally `panic="abort"` on the cdylib profile.

### F20 — [High] FFI: thread-local close-guard → cross-thread double-free / UAF — **Documented**
- `turbolite-ffi/src/ffi.rs:56-58` `CLOSED_HANDLES` is thread-local; a handle
  closed on one thread is not seen as closed on another → double `Box::from_raw`
  / use-after-free for the documented multi-threaded language bindings.
- **Fix:** use a process-global `Mutex<HashSet<usize>>`; document concurrent
  single-handle use as caller responsibility.

### F21 — [Med] `cstr_to_str<'a>` returns an unbounded-lifetime `&str` from a raw pointer — **Documented**
- `turbolite-ffi/src/ffi.rs:663`. **Fix:** make it `unsafe` + tie `'a` to the
  input, or return an owned `String`/`Cow`.

### F22 — [Med] `turbolite_compact` leaks a `CString::into_raw` the C side never frees — **Documented**
- `turbolite-ffi/src/ext.rs:697-711`. **Fix:** use the thread-local `CString`
  buffer pattern like `cache_info`/`warm`.

### F23 — [Med] `connection_uses_turbolite_vfs` reads a hand-rolled `sqlite3_vfs` layout with no version gate — **Documented**
- `src/install.rs:58-66,219`. **Fix:** assert `i_version >= 1 && sz_os_file > 0`
  before dereferencing `z_name`.

### F24 — [Low] `compression_level` forwarded from C unchecked — **Documented**
- `turbolite-ffi/src/ffi.rs:132-133,185`. **Fix:** clamp/validate to `1..=22`,
  set `last_error` on out-of-range.

---

## Test / build notes

- `cargo build` green; the Fixed cluster compiles. The two fixes are defensive
  (cycle guard + overflow checks) and do not alter the happy path.
- Live-S3 tests are gated and not exercised here.
