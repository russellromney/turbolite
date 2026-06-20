# Turbolite Quint Specs

These specs model small protocol contracts at the Turbolite/HADB
boundary. They are not part of the normal Rust build, and they do not
model SQLite page contents, compression, encryption, object-store
consistency, or full object-store behavior. `cloud_scan_prefetch.qnt`
models the small fixed-window prefetch scheduler contract introduced for
cloud scan pressure.

The first safety spec is `cursor_chain.qnt`. It models the replay cursor
base/delta contract: ordered delta application, writer and lease fencing,
chain anchors, promotion, and fatal ambiguity handling.

`cloud_scan_prefetch.qnt` models the cloud scan scheduler state machine:
queued optional work, started remote prefetch, foreground full-group
demand, foreground waits, and scan-range duplication. It intentionally
does not model SQLite's B-tree traversal or backend retry timing.

`manifest_cas.qnt` models the manifest upload compare-and-swap contract
introduced in PR #46: `put_manifest` always reads the remote manifest and
rejects the upload if the remote version is already >= the local version.
The model tracks only the version counter and the writer's local view; it
does not model page groups, chain anchors, or object-store semantics.

## Tools

Use Quint 0.32.0 through `npx`:

```sh
npx -y @informalsystems/quint@0.32.0 --version
```

`quint run` and `quint typecheck` work without Java. `quint verify`
uses Apalache by default for bounded safety checks and TLC for the
temporal liveness check. Both verifier backends require a local Java
runtime. On macOS, Homebrew OpenJDK is enough:

```sh
brew install openjdk
```

## Local Checks

From the Turbolite repo root:

```sh
make specs
```

This runs:

```sh
npx -y @informalsystems/quint@0.32.0 typecheck specs/cursor_chain.qnt
npx -y @informalsystems/quint@0.32.0 typecheck specs/cursor_chain_liveness.qnt
npx -y @informalsystems/quint@0.32.0 typecheck specs/cloud_scan_prefetch.qnt
npx -y @informalsystems/quint@0.32.0 typecheck specs/manifest_cas.qnt
npx -y @informalsystems/quint@0.32.0 run specs/cursor_chain.qnt \
  --max-samples=200 --max-steps=8 --invariants=safety
npx -y @informalsystems/quint@0.32.0 run specs/cloud_scan_prefetch.qnt \
  --max-samples=500 --max-steps=8 --invariants=safety --verbosity=0
npx -y @informalsystems/quint@0.32.0 run specs/manifest_cas.qnt \
  --max-samples=500 --max-steps=8 --invariants=safety --verbosity=0
```

Expected result: typecheck succeeds, simulator runs without invariant
violations.

The progress check is deterministic:

```sh
make specs-progress
```

This seeds a three-delta valid prefix and uses the `progressStep`
relation. It is a bounded no-stutter scenario, not a proof of general
fair liveness for the normal `step` relation. `boundedProgress`
requires the follower to apply all three seeded deltas within the
bound while preserving `safety`.

To run the positive safety simulation, deterministic progress scenario,
expected-failure counterexamples, Rust bridge tests, bounded model
checker, and TLC liveness checks:

```sh
make specs-all
```

This is the full local verification target, not a quick smoke check. It
requires Java for Apalache/TLC and intentionally exercises the Rust replay
cursor bridge after real replay finalization.

## Model Checker

When Java is available, run the bounded model checker:

```sh
make specs-verify
```

This runs:

```sh
PATH="$(brew --prefix openjdk)/bin:$PATH" \
  npx -y @informalsystems/quint@0.32.0 verify specs/cursor_chain.qnt \
    --max-steps=8 --invariants=safety
PATH="$(brew --prefix openjdk)/bin:$PATH" \
  npx -y @informalsystems/quint@0.32.0 verify specs/cursor_chain.qnt \
    --step=progressStep --max-steps=4 --invariants=safety boundedProgress
```

Expected result: verification succeeds over the bounded state space.

## Temporal Liveness

`cursor_chain_liveness.qnt` is a separate, smaller temporal model. It
does not claim object-store, root-publish, writer-election, or full
promotion liveness. Its environment assumptions are explicit:

- the root/lease fence is stable,
- a three-delta valid prefix remains visible,
- no fatal equivocation/collision is present,
- the follower keeps polling,
- `applyNext` is weakly fair when it stays enabled.

Run the positive temporal check with TLC:

```sh
make specs-liveness
```

This checks:

```sh
npx -y @informalsystems/quint@0.32.0 verify specs/cursor_chain_liveness.qnt \
  --backend=tlc --max-steps=8 --invariants=safety \
  --temporal=fairStablePrefixProgress
```

Expected result: TLC checks the complete small state space and finds no
temporal violation. The property is: under weak fairness of `applyNext`,
a stable available prefix eventually leads to the follower catching up.

The companion negative check proves the fairness assumption is doing real
work:

```sh
make specs-liveness-negative
```

Expected result: `badUnfairPollingProgress` violates temporal progress,
because without fairness the follower can poll/stutter forever.

## Negative Counterexamples

The spec contains intentionally bad step actions. They should all
violate `safety` and produce short counterexamples:

```sh
make specs-negative
```

Expected result: each named bad step reports an expected invariant
violation:

- `badWrongEpochStep`
- `badWrongWriterStep`
- `badSkippedSeqStep`
- `badPrevChecksumStep`
- `badEquivocationStep`
- `badChecksumCollisionStep`
- `badStaleWriterAfterPromotionStep`
- `badPromotionCursorStep`
- `badPromotionPageCountStep`
- `badClaimBeforePermit`
- `badWorkerBlocksBeforePermit`
- `badRangeAndFullDuplicate`
- `badTreeNameGatedScanRange`
- `badSkipCasOnFirstWrite`
- `badBlindOverwrite`

`badChecksumCollisionStep` uses the spec's compact
`payloadFingerprint` field to stand in for the full production delta
envelope: changed page bytes, commit boundary, idempotency key, page
counts, sequence, writer, epoch, and chain link. The model therefore
checks the intended rule, "one checksum cannot name two different
payload envelopes," without expanding page bytes in Quint.

The cloud-scan negative steps stand in for the scheduler bugs this
phase is guarding against: optional work claiming `Fetching` before it
has a remote I/O permit, optional workers parking behind unavailable
prefetch permits, and planned SCAN demand doing foreground range I/O
while also scheduling same-group full prefetch. The tree-name-gated
negative covers the case where a group is part of a planned SCAN but
the demand read misses the inferred current tree name and incorrectly
falls back to seekable range I/O.

The manifest-CAS negative steps model the regression PR #46 closed.
`badSkipCasOnFirstWrite` represents the old behavior where `put_manifest`
for version 1 skipped the remote version check and could overwrite an
already-published manifest. `badBlindOverwrite` represents publishing
without reading the remote manifest at all.

For a full counterexample trace, run an individual step without
`--verbosity=0`, for example:

```sh
npx -y @informalsystems/quint@0.32.0 run specs/cursor_chain.qnt \
  --max-samples=20 --max-steps=2 --step=badWrongEpochStep \
  --invariants=safety
```

## Rust Mapping

`cursor_chain.qnt` models these Rust surfaces:

- `src/tiered/manifest.rs`
  - `ReplayCursor`
  - `Manifest::writer_id`
- `src/tiered/wire.rs`
  - canonical manifest bytes
  - `base_anchor_checksum`
- `src/tiered/vfs.rs`
  - `manifest_bytes_with_replay_cursor_anchor`
  - `update_replay_cursor`
  - `publish_replayed_base_with_replay_cursor_anchor`
- `src/tiered/replay.rs`
  - direct page replay lifecycle

`cloud_scan_prefetch.qnt` models these Rust surfaces:

- `src/tiered/handle.rs`
  - `ActiveScanPrefetch`
  - planned SCAN group-membership full demand versus seekable range demand
  - foreground waits only on started optional prefetch
- `src/tiered/prefetch.rs`
  - `PrefetchPool::submit_optional`
  - `RemoteIoBudget`
  - queued versus active optional groups

`manifest_cas.qnt` models this Rust surface:

- `src/tiered/storage.rs`
  - `put_manifest`
  - the remote manifest version check that rejects uploads when
    `remote.version >= local.version`

The modeled Rust-facing properties have a matching Rust bridge target:

```sh
make specs-rust
```

This runs `cargo test --features zstd,bundled-sqlite replay_cursor_contract`.
Those tests bridge the first Quint model to production Rust surfaces:

- `replay_cursor_contract_manifest_bytes_stamps_cursor_writer_and_anchor`
  checks the VFS publisher path stamps seq/epoch/writer and the base
  chain anchor.
- `replay_cursor_contract_begin_replay_uses_installed_cursor_floor`
  checks the default replay handle resumes after the installed cursor,
  not behind it.
- `replay_cursor_contract_manifest_bytes_rejects_invalid_cursor_inputs`
  checks anchored cursor publication rejects zero epochs, blank writers, and
  backward cursor movement.
- `replay_cursor_contract_update_replay_cursor_preserves_base_shape_and_writer`
  checks follower cursor advancement does not rewrite the base shape.
- `replay_cursor_contract_update_replay_cursor_rejects_malformed_or_backward_cursor`
  checks follower cursor advancement rejects malformed anchors, missing
  writer identity, and backward movement.
- `replay_cursor_contract_publish_replayed_base_stamps_final_cursor_anchor`
  checks promotion stamps seq/epoch/writer and recomputes the anchor
  over the final published base.
- `replay_cursor_contract_publish_replayed_base_after_replay_carries_post_replay_state`
  checks the anchored publisher after a real replay finalize, including
  non-regressing legacy change counter, exact replay cursor, writer, and
  recomputed base anchor.
- `replay_cursor_contract_base_anchor_binds_cursor_epoch_and_writer`
  checks the wire hash domain includes seq, lease epoch, and writer.
- `replay_cursor_contract_recovery_plan_applies_contiguous_prefix_and_page_counts`
  checks a three-delta prefix advances contiguous sequence and
  grow/shrink page counts.
- `replay_cursor_contract_recovery_plan_rejects_stale_epoch_candidate`
  checks stale-epoch candidates do not satisfy the root fence.

Existing Sashimono tests in `src/tiered/sashimono.rs` also cover wrong
writer, missing gaps, duplicate same-seq candidates, wrong base page
count, page-size mismatch, and corrupt/torn delta payloads.
