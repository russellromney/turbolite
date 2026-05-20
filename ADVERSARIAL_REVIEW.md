# Adversarial Review — turbolite

A bug-hunt of the tiered VFS / cache / compression / encryption / B-tree / S3 /
FFI surface. Each finding: severity, location, the bug, the fix, and a Status —
**Fixed** (implemented + build green) or **Documented** (verified real; fix
specified for a focused follow-up). Line numbers are approximate; re-locate
before editing.

This pass landed the catastrophic crypto fixes (F1, F2), several defensive
crash/DoS fixes, and the full FFI soundness cluster (F19–F24): the C boundary
is now panic-safe (`catch_unwind` on every `extern "C"` body + `panic = "abort"`
on the release profile), the close-guard is process-global (no cross-thread
double-free), and the smaller FFI lifetime/leak/validation issues are closed.
The remaining findings below are documented with precise fixes for focused
follow-ups, several gated behind the live-S3 suite that does not run in this
environment.

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

### F10 — [Med] `assemble()` recovery never shrinks page_count; fragile page-size special-case — **Fixed**
- `src/tiered/vfs.rs` recovery path (and `sync_after_external_restore`/
  `mark_all_pages_present` not clearing bits above a shrunk count).
- **Fix:** recovery anchors `page_count` to the adopted trailer manifest (not
  the max of stray staging page numbers), derives `page_size` from the staging
  header unconditionally, and clears bitmap bits at or above the committed count.
  `mark_all_pages_present` clears bits above a shrunk count; new
  `clear_pages_at_or_above` drops stray-page state on recovery. Tests:
  `mark_all_pages_present` shrink clears bits above count;
  `clear_pages_at_or_above` clears stray pages.

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

### F15 — [Med] S3 range GET `len==0` underflow; `list_all_keys` infinite loop — **Fixed**
- `src/tiered/s3_client.rs` (`start + len - 1` underflowed when len==0 →
  full-tail GET; returned length unchecked) and `list_all_keys`/
  `list_all_keys_with_prefix` (truncated with no continuation token re-issued
  the first page forever).
- **Fix:** `range_get_async` guards `len == 0` (no underflow into a full-tail
  GET) and verifies the returned body length matches the requested range;
  `list_all_keys`/`list_all_keys_with_prefix` error instead of looping forever
  when a response is truncated but carries no continuation token.

### F17 — [Low] `xRandomness` uses a time-seeded LCG — **Fixed**
- `src/tiered/vfs.rs` `xRandomness`. **Fix:** fills from `rand::thread_rng()`
  (OS CSPRNG) via `fill_bytes` instead of a time-seeded LCG; `rand` is now a
  non-optional dependency.

### F18 — [Low] `stat_misses.fetch_sub(1)` underflow under a prefetch re-check race — **Fixed**
- `src/tiered/handle.rs` prefetch re-check. A miss is counted before the slow
  path; the re-check then `fetch_sub`'d it when a sibling prefetch landed.
  Under a race the subtraction could outrun the additions and wrap the `u64`
  counter to `u64::MAX`.
- **Fix:** the re-check now reconciles the miss via a saturating `fetch_update`
  (`m.saturating_sub(1)`) so the counter can never wrap past zero.

### F19 — [High] FFI: no `catch_unwind`, no `panic=abort` → panic across C is UB — **Fixed**
- `turbolite-ffi/src/ext.rs:701` (`CString::new(json).unwrap()`),
  `ext.rs:437,464,477,489,501,902` & `settings.rs:136` (`.lock().expect`),
  `ext.rs:79`, `ext.rs:551,579`.
- **Fix:** every `extern "C"` body across `ffi.rs`, `ext.rs`, `settings.rs`,
  and `install.rs` now routes through a `catch_unwind(AssertUnwindSafe(..))`
  guard (`ffi_guard`/`ext_guard`/`settings_guard`/`install_guard`) that maps a
  caught panic to that function's error sentinel (`-1` / null / `SQLITE_MISUSE`
  / a SQLite result error for the scalar callback). The concrete panic sites
  are gone: `s3_runtime()` returns `Option` instead of `.expect`-panicking;
  `register_tiered`'s `TURBOLITE_BUCKET` `.expect` is an `io::Error`; every
  `.lock().expect("...poisoned")` recovers the poisoned guard via
  `unwrap_or_else(|e| e.into_inner())`; the `CString::new("").unwrap()`
  thread-local initializers use `CString::default()`; and `turbolite_compact`
  no longer `unwrap`s `CString::new(json)` (see F22). Belt-and-suspenders:
  `panic = "abort"` on the release profile (the shipping cdylib) in the root
  `Cargo.toml`; dev/test builds keep unwinding so the harness can catch
  panics.

### F20 — [High] FFI: thread-local close-guard → cross-thread double-free / UAF — **Fixed**
- `turbolite-ffi/src/ffi.rs:56-58` `CLOSED_HANDLES` is thread-local; a handle
  closed on one thread is not seen as closed on another → double `Box::from_raw`
  / use-after-free for the documented multi-threaded language bindings.
- **Fix:** `CLOSED_HANDLES` is now a process-global `Mutex<HashSet<usize>>`
  behind a `OnceLock`. `turbolite_close` atomically claims the free with
  `HashSet::insert`: only the caller that transitions the address open->closed
  runs `Box::from_raw`, so the pointer is dropped exactly once across all
  threads; a concurrent/duplicate close (any thread) sees the address present
  and returns. `turbolite_open`/`turbolite_open_local` clear the marker on
  reuse, and `turbolite_exec`/`turbolite_query_json` consult the global guard.
  Genuine *concurrent* use of a single handle remains caller responsibility
  (a `TurboliteDb` is not internally synchronized); the guard only prevents the
  cross-thread double-close/UAF, not a true data race. Regression tests
  `test_close_then_close_from_other_thread_is_rejected` and
  `test_exec_from_other_thread_after_close_is_rejected` close on one thread and
  drive a second close / exec from another, asserting rejection rather than a
  double-free.

### F21 — [Med] `cstr_to_str<'a>` returns an unbounded-lifetime `&str` from a raw pointer — **Fixed**
- `turbolite-ffi/src/ffi.rs:663`. **Fix:** `cstr_to_str` is now `unsafe` and
  takes `ptr: &'a *const c_char`, tying the returned `&'a str` to the scope of
  the pointer binding so the borrow checker rejects any use that outlives the
  C buffer (previously `'a` was synthesized freely from a raw pointer). Same
  treatment for `nullable_cstr_to_option`. All call sites pass `&ptr` inside an
  `unsafe` block.

### F22 — [Med] `turbolite_compact` leaks a `CString::into_raw` the C side never frees — **Fixed**
- `turbolite-ffi/src/ext.rs:697-711`. **Fix:** `turbolite_compact` now writes
  the JSON report into a thread-local `CString` buffer and returns its pointer
  (valid until the next call on the same thread, treat as SQLITE_TRANSIENT),
  matching `cache_info`/`warm`. No more per-call leak, and the
  `CString::new(json).unwrap()` (a panic on NUL-containing JSON) is replaced by
  a `match` that returns null on error.

### F23 — [Med] `connection_uses_turbolite_vfs` reads a hand-rolled `sqlite3_vfs` layout with no version gate — **Fixed**
- `turbolite-ffi/src/install.rs:58-66,219`. **Fix:** `connection_uses_turbolite_vfs`
  now reads `i_version` (the first field, always in bounds) and `sz_os_file`
  and bails (`false`) unless `i_version >= 1 && sz_os_file > 0` before
  dereferencing `z_name`, so an unexpected struct shape can't make us read
  `z_name` at the wrong offset.

### F24 — [Low] `compression_level` forwarded from C unchecked — **Fixed**
- `turbolite-ffi/src/ffi.rs:132-133,185`. **Fix:** `validate_compression_level`
  rejects any value outside `1..=22` at the boundary (sets `last_error`,
  returns `-1`); `turbolite_register_local_file_first` and
  `turbolite_register_local` validate before forwarding the level into the
  compression config.

---

## Test / build notes

- `cargo build` green; the Fixed cluster compiles. The two fixes are defensive
  (cycle guard + overflow checks) and do not alter the happy path.
- Live-S3 tests are gated and not exercised here.
