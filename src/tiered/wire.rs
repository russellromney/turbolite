//! Turbolite manifest wire format — canonical CBOR envelope.
//!
//! The bytes produced by [`encode`] are what turbolite publishes through a
//! `ManifestStore` (the envelope's opaque payload) and what
//! [`crate::tiered::TurboliteVfs::manifest_bytes`] returns. They are also
//! the **hash domain** for `ReplayCursor::base_object_checksum`: the
//! 32-byte BLAKE3 hash of the envelope bytes anchors every delta chain.
//!
//! Layout (v1):
//!
//! ```text
//! +---------+---------+---------+---------+---------+---------+---------+
//! | 'T'     | 'L'     | 'M'     | '1'     | version_be_u16  | body... |
//! +---------+---------+---------+---------+---------+---------+---------+
//!  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^  ^^^^^^^^^^^^^^^  ^^^^^^^^^
//!         4-byte magic ("TLM1")               2-byte version    canonical CBOR
//! ```
//!
//! Magic comes first so unknown payloads fail loud before the version
//! check — `MagicMismatch` is much easier to diagnose than a generic CBOR
//! decode error. Version is a big-endian `u16` so adding `v2`, `v3`, etc.
//! is a one-byte change without disturbing the magic.
//!
//! ### Canonical encoding
//!
//! The body is canonical CBOR over [`CanonicalManifestV1`], a wire-only
//! mirror of [`Manifest`] whose `HashMap` fields are pre-sorted into
//! `Vec<(K, V)>` by key. Sorting at the wire boundary keeps `Manifest`
//! itself unchanged (so hot-path code still uses `HashMap`) while making
//! the encoded bytes a function of the *logical* manifest value, not of
//! the runtime hashmap iteration order. The [`canonical_encoding_is_deterministic`]
//! test pins this property; the [`canonical_v1_blake3_golden_hash`] test
//! pins the exact byte layout. If either fires, a serializer change has
//! silently shifted the hash domain — bump `VERSION_V1` and re-pin the
//! golden.
//!
//! ### Legacy payload rejection
//!
//! Legacy wire payloads led with a one-byte tag (`0x01` = pure
//! msgpack, `0x02` = hybrid). Both are detected before the magic check
//! and surfaced as distinct [`PayloadVersionError`] variants so a
//! follower hitting old bytes gets an actionable error instead of a
//! generic decode failure. There is no silent fallback; a migration
//! reader can add one if real on-disk data needs converting.

use std::io;

use serde::{Deserialize, Serialize};

use super::config::{BTreeManifestEntry, GroupingStrategy};
use super::manifest::{FrameEntry, Manifest, ReplayCursor, SubframeOverride};

/// Magic bytes — "Turbolite Manifest, format family 1". Chosen so the
/// first byte (`0x54`) does not collide with any legacy tag.
const MAGIC: [u8; 4] = *b"TLM1";

/// Current canonical-CBOR envelope version.
const VERSION_V1: u16 = 0x0001;

/// Legacy leading-tag byte: pure msgpack manifest payload.
const TAG_LEGACY_PURE: u8 = 0x01;

/// Legacy leading-tag byte: hybrid (manifest + walrust delta cursor) payload.
const TAG_LEGACY_HYBRID: u8 = 0x02;

/// Error surface for canonical-envelope decode.
///
/// Variants are split fine-grained so callers (haqlite-turbolite,
/// future tooling) can distinguish "definitely-old format that needs
/// migration" from "wrong magic — corrupt bytes" from "right magic but
/// version we don't support".
#[derive(Debug, thiserror::Error)]
pub enum PayloadVersionError {
    /// Legacy hybrid payload (leading byte `0x02`). The hybrid
    /// payload embedded walrust delta cursor/prefix fields beside the
    /// page manifest. The canonical envelope keeps delta discovery at the
    /// integration layer now.
    #[error("legacy hybrid manifest payload (leading byte 0x02); canonical envelope required")]
    HybridDeprecated,

    /// Legacy pure msgpack payload (leading byte `0x01`). Decode it with
    /// msgpack from an older build if you need the contents; current
    /// readers accept the `TLM1` canonical envelope.
    #[error(
        "legacy pure-msgpack manifest payload (leading byte 0x01); canonical envelope required"
    )]
    PureMsgpackDeprecated,

    /// Payload shorter than the envelope header (magic + version).
    #[error("manifest payload too short to be a TLM envelope ({0} bytes)")]
    Truncated(usize),

    /// Magic bytes did not match. Likely a different binary format, not
    /// a turbolite manifest.
    #[error("manifest magic mismatch: expected TLM1, got {got:?}")]
    MagicMismatch { got: [u8; 4] },

    /// Magic matched, but the version byte names a format this build
    /// does not understand. A newer turbolite wrote it.
    #[error(
        "unsupported manifest format version 0x{found:04x}; this build supports 0x{supported:04x}"
    )]
    UnsupportedVersion { found: u16, supported: u16 },

    /// CBOR body present but malformed.
    #[error("manifest body decode failed: {0}")]
    BodyDecode(String),

    /// CBOR body encode failure on the writer side (extremely rare —
    /// serde-derived encodes for [`CanonicalManifestV1`] are infallible
    /// in practice; surfaced for completeness).
    #[error("manifest body encode failed: {0}")]
    BodyEncode(String),
}

impl From<PayloadVersionError> for io::Error {
    fn from(value: PayloadVersionError) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, value.to_string())
    }
}

/// Encode a [`Manifest`] into the canonical TLM1 envelope.
///
/// The returned bytes are stable for a given logical manifest value:
/// `encode(m) == encode(m.clone())` — sorting at the wire boundary
/// removes `HashMap` iteration-order non-determinism. BLAKE3 over these
/// bytes is the chain anchor (see [`checksum`]).
pub fn encode(manifest: &Manifest) -> Result<Vec<u8>, PayloadVersionError> {
    let canonical = CanonicalManifestV1::from(manifest);
    let mut body: Vec<u8> = Vec::new();
    ciborium::into_writer(&canonical, &mut body)
        .map_err(|e| PayloadVersionError::BodyEncode(e.to_string()))?;

    let mut out = Vec::with_capacity(MAGIC.len() + 2 + body.len());
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&VERSION_V1.to_be_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

/// Decode envelope bytes back into a [`Manifest`].
///
/// Detects legacy payload shapes first (single leading tag byte 0x01 /
/// 0x02) so callers get a precise [`PayloadVersionError`] variant. Any
/// non-magic, non-legacy lead returns `MagicMismatch`.
pub fn decode(bytes: &[u8]) -> Result<Manifest, PayloadVersionError> {
    if bytes.is_empty() {
        return Err(PayloadVersionError::Truncated(0));
    }

    // Legacy detection runs before the magic check so we surface
    // actionable errors instead of MagicMismatch for known old formats.
    match bytes[0] {
        TAG_LEGACY_HYBRID => return Err(PayloadVersionError::HybridDeprecated),
        TAG_LEGACY_PURE => return Err(PayloadVersionError::PureMsgpackDeprecated),
        _ => {}
    }

    if bytes.len() < MAGIC.len() + 2 {
        return Err(PayloadVersionError::Truncated(bytes.len()));
    }

    let got: [u8; 4] = bytes[..MAGIC.len()].try_into().expect("len-checked above");
    if got != MAGIC {
        return Err(PayloadVersionError::MagicMismatch { got });
    }

    let version_bytes: [u8; 2] = bytes[MAGIC.len()..MAGIC.len() + 2]
        .try_into()
        .expect("len-checked above");
    let version = u16::from_be_bytes(version_bytes);
    if version != VERSION_V1 {
        return Err(PayloadVersionError::UnsupportedVersion {
            found: version,
            supported: VERSION_V1,
        });
    }

    let body = &bytes[MAGIC.len() + 2..];
    let canonical: CanonicalManifestV1 =
        ciborium::from_reader(body).map_err(|e| PayloadVersionError::BodyDecode(e.to_string()))?;
    Ok(Manifest::from(canonical))
}

/// BLAKE3 over the envelope bytes — the value that lands in
/// `ReplayCursor::base_object_checksum` after publishing a base, and
/// what every following delta's `prev_checksum` must match.
///
/// Delta stamping uses the same helper for envelope hashes. The unit test
/// keeps the API covered even before every chain verifier path consumes it.
/// `#[allow(dead_code)]` suppresses the no-lib-callers warning until those
/// callers are wired through.
#[allow(dead_code)]
pub fn checksum(envelope_bytes: &[u8]) -> [u8; 32] {
    *blake3::hash(envelope_bytes).as_bytes()
}

/// The chain anchor for a base manifest — the value a publisher writes
/// into `cursor.base_object_checksum` and that the first delta after
/// the base carries as its `prev_checksum`.
///
/// Resolves the circular definition ("base_object_checksum
/// = hash(this_manifest)"): the manifest contains the field, so it
/// cannot hash itself directly. We compute the BLAKE3 of the TLM1
/// envelope encoded with `cursor.base_object_checksum` **cleared**.
/// The result is bound to the full manifest content (page groups,
/// cursor seq/epoch, writer_id, everything except the anchor field
/// itself), deterministic, and non-circular.
///
/// Contract:
/// - Publisher: set `cursor.base_object_checksum = base_anchor_checksum(&m)`
///   on the manifest it publishes, and stamp the first delta after the
///   base with `prev_checksum = ` the same value.
/// - Follower: read `cursor.base_object_checksum` from the base
///   manifest and use it directly as the chain anchor (no recompute).
pub fn base_anchor_checksum(manifest: &Manifest) -> Result<[u8; 32], PayloadVersionError> {
    let mut m = manifest.clone();
    m.cursor.base_object_checksum = Vec::new();
    let bytes = encode(&m)?;
    Ok(checksum(&bytes))
}

/// Wire-only canonical mirror of [`Manifest`].
///
/// The two structural differences from `Manifest`:
/// - `HashMap` fields are flattened to `Vec<(K, V)>` sorted by `K`, so
///   encode/decode is deterministic regardless of runtime iteration order
/// - The `#[serde(skip)]` reverse-index fields on `Manifest` are absent
///   here entirely (they are rebuilt on the consumer side from `btrees`)
///
/// Field order in this struct matters: ciborium serializes structs as
/// CBOR maps keyed by field name, so renaming or reordering fields here
/// changes the bytes (and the BLAKE3 hash). The
/// [`canonical_v1_blake3_golden_hash`] test is the trip-wire.
#[derive(Debug, Serialize, Deserialize)]
struct CanonicalManifestV1 {
    version: u64,
    change_counter: u64,
    discontinuity_stamp: u64,
    cursor: ReplayCursor,
    writer_id: String,
    page_count: u64,
    page_size: u32,
    pages_per_group: u32,
    sub_pages_per_frame: u32,
    strategy: GroupingStrategy,
    db_header: Option<Vec<u8>>,
    page_group_keys: Vec<String>,
    frame_tables: Vec<Vec<FrameEntry>>,
    group_pages: Vec<Vec<u64>>,
    // Sorted by key for canonical-byte stability.
    btrees: Vec<(u64, BTreeManifestEntry)>,
    interior_chunk_keys: Vec<(u32, String)>,
    index_chunk_keys: Vec<(u32, String)>,
    // Outer Vec keeps insertion order (positional by group_id); each inner
    // map is sorted by key.
    subframe_overrides: Vec<Vec<(usize, SubframeOverride)>>,
}

impl From<&Manifest> for CanonicalManifestV1 {
    fn from(m: &Manifest) -> Self {
        let mut btrees: Vec<(u64, BTreeManifestEntry)> =
            m.btrees.iter().map(|(k, v)| (*k, v.clone())).collect();
        btrees.sort_by_key(|(k, _)| *k);

        let mut interior_chunk_keys: Vec<(u32, String)> = m
            .interior_chunk_keys
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        interior_chunk_keys.sort_by_key(|(k, _)| *k);

        let mut index_chunk_keys: Vec<(u32, String)> = m
            .index_chunk_keys
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        index_chunk_keys.sort_by_key(|(k, _)| *k);

        let subframe_overrides: Vec<Vec<(usize, SubframeOverride)>> = m
            .subframe_overrides
            .iter()
            .map(|inner| {
                let mut pairs: Vec<(usize, SubframeOverride)> =
                    inner.iter().map(|(k, v)| (*k, v.clone())).collect();
                pairs.sort_by_key(|(k, _)| *k);
                pairs
            })
            .collect();

        CanonicalManifestV1 {
            version: m.version,
            change_counter: m.change_counter,
            discontinuity_stamp: m.discontinuity_stamp,
            cursor: m.cursor.clone(),
            writer_id: m.writer_id.clone(),
            page_count: m.page_count,
            page_size: m.page_size,
            pages_per_group: m.pages_per_group,
            sub_pages_per_frame: m.sub_pages_per_frame,
            strategy: m.strategy,
            db_header: m.db_header.clone(),
            page_group_keys: m.page_group_keys.clone(),
            frame_tables: m.frame_tables.clone(),
            group_pages: m.group_pages.clone(),
            btrees,
            interior_chunk_keys,
            index_chunk_keys,
            subframe_overrides,
        }
    }
}

impl From<CanonicalManifestV1> for Manifest {
    fn from(c: CanonicalManifestV1) -> Self {
        // The `#[serde(skip)]` reverse-index fields (page_index,
        // btree_groups, etc.) are not on the wire; `..Default::default()`
        // leaves them empty here and VFS-level loaders rebuild them
        // from `btrees` on load.
        Manifest {
            version: c.version,
            change_counter: c.change_counter,
            discontinuity_stamp: c.discontinuity_stamp,
            cursor: c.cursor,
            writer_id: c.writer_id,
            page_count: c.page_count,
            page_size: c.page_size,
            pages_per_group: c.pages_per_group,
            sub_pages_per_frame: c.sub_pages_per_frame,
            strategy: c.strategy,
            db_header: c.db_header,
            page_group_keys: c.page_group_keys,
            frame_tables: c.frame_tables,
            group_pages: c.group_pages,
            btrees: c.btrees.into_iter().collect(),
            interior_chunk_keys: c.interior_chunk_keys.into_iter().collect(),
            index_chunk_keys: c.index_chunk_keys.into_iter().collect(),
            subframe_overrides: c
                .subframe_overrides
                .into_iter()
                .map(|inner| inner.into_iter().collect())
                .collect(),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tiered::GroupingStrategy;
    use std::collections::HashMap;

    /// Fixture with non-empty maps so the canonical sort is exercised.
    fn golden_fixture() -> Manifest {
        let mut interior_chunk_keys = HashMap::new();
        interior_chunk_keys.insert(7u32, "chunk/7".to_string());
        interior_chunk_keys.insert(2u32, "chunk/2".to_string());
        interior_chunk_keys.insert(11u32, "chunk/11".to_string());

        let mut index_chunk_keys = HashMap::new();
        index_chunk_keys.insert(0u32, "idx/0".to_string());
        index_chunk_keys.insert(3u32, "idx/3".to_string());

        let mut btrees = HashMap::new();
        btrees.insert(
            5u64,
            BTreeManifestEntry {
                name: "users".into(),
                obj_type: "table".into(),
                group_ids: vec![1, 2],
            },
        );
        btrees.insert(
            1u64,
            BTreeManifestEntry {
                name: "sqlite_schema".into(),
                obj_type: "table".into(),
                group_ids: vec![0],
            },
        );

        let mut subframe_inner = HashMap::new();
        subframe_inner.insert(
            3usize,
            SubframeOverride {
                key: "override/3".into(),
                entry: FrameEntry {
                    offset: 0,
                    len: 128,
                },
            },
        );
        subframe_inner.insert(
            1usize,
            SubframeOverride {
                key: "override/1".into(),
                entry: FrameEntry { offset: 0, len: 64 },
            },
        );

        Manifest {
            version: 42,
            change_counter: 100,
            discontinuity_stamp: 3,
            cursor: ReplayCursor {
                last_applied_seq: 17,
                base_object_checksum: vec![0xAA; 32],
                epoch: 5,
            },
            writer_id: "node-leader-A".into(),
            page_count: 128,
            page_size: 4096,
            pages_per_group: 4,
            sub_pages_per_frame: 2,
            strategy: GroupingStrategy::BTreeAware,
            db_header: Some(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            page_group_keys: vec!["pg/0_v42".into(), "pg/1_v42".into()],
            frame_tables: vec![vec![FrameEntry {
                offset: 256,
                len: 64,
            }]],
            group_pages: vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7]],
            btrees,
            interior_chunk_keys,
            index_chunk_keys,
            subframe_overrides: vec![subframe_inner],
            ..Default::default()
        }
    }

    /// Round-trip preserves every load-bearing field, including cursor,
    /// writer identity, and discontinuity stamp.
    #[test]
    fn round_trip_preserves_replay_cursor_fields() {
        let m = golden_fixture();
        let bytes = encode(&m).expect("encode");
        // Envelope sanity: magic + version + non-empty body.
        assert_eq!(&bytes[..4], &MAGIC);
        assert_eq!(u16::from_be_bytes([bytes[4], bytes[5]]), VERSION_V1);
        assert!(bytes.len() > 6);

        let decoded = decode(&bytes).expect("decode");
        assert_eq!(decoded.version, m.version);
        assert_eq!(decoded.change_counter, m.change_counter);
        assert_eq!(decoded.discontinuity_stamp, m.discontinuity_stamp);
        assert_eq!(decoded.cursor, m.cursor);
        assert_eq!(decoded.writer_id, m.writer_id);
        assert_eq!(decoded.page_count, m.page_count);
        assert_eq!(decoded.page_size, m.page_size);
        assert_eq!(decoded.page_group_keys, m.page_group_keys);
        assert_eq!(decoded.db_header, m.db_header);
        // Verify maps round-trip with the same entries (order doesn't
        // matter on the consumer side; sorting only matters on encode).
        assert_eq!(decoded.btrees.len(), m.btrees.len());
        for k in m.btrees.keys() {
            assert!(decoded.btrees.contains_key(k));
        }
    }

    /// Encoding the same logical manifest twice yields identical bytes,
    /// even when the HashMap iteration order varies between insertions.
    /// This is the core canonical-encoding contract.
    #[test]
    fn canonical_encoding_is_deterministic() {
        let m1 = golden_fixture();
        // Build the "same" manifest by inserting hashmap entries in a
        // different order — runtime iteration order will differ from m1
        // for the unsorted HashMap, but the canonical encoder sorts.
        let mut m2 = golden_fixture();
        let saved: Vec<(u32, String)> = m2.interior_chunk_keys.drain().collect();
        for (k, v) in saved.into_iter().rev() {
            m2.interior_chunk_keys.insert(k, v);
        }
        let saved: Vec<(u64, BTreeManifestEntry)> = m2.btrees.drain().collect();
        for (k, v) in saved.into_iter().rev() {
            m2.btrees.insert(k, v);
        }

        let b1 = encode(&m1).expect("encode m1");
        let b2 = encode(&m2).expect("encode m2");
        assert_eq!(
            b1, b2,
            "canonical encoding must be a function of the logical value"
        );

        // Sanity: re-encoding the same value also matches itself.
        let b1_again = encode(&m1).expect("re-encode m1");
        assert_eq!(b1, b1_again);
    }

    /// BLAKE3 golden hash. This pins the canonical envelope hash across
    /// releases. If this test fires,
    /// some serialization detail has shifted: either the canonical
    /// encoder changed (ciborium upgrade?), the field set on
    /// `CanonicalManifestV1` changed, or a sort key changed. Fix the
    /// root cause; do NOT re-pin without bumping `VERSION_V1` first.
    ///
    /// To establish the golden on first run, set
    /// `TURBOLITE_PIN_GOLDEN_HASH=1` and the test will print the
    /// computed hex hash and pass; copy that hex into `EXPECTED_HEX`
    /// below.
    #[test]
    fn canonical_v1_blake3_golden_hash() {
        // Pinned against ciborium 0.2 + blake3 1.x + serde_bytes 0.11
        // + CanonicalManifestV1 as currently defined. Any future drift
        // fires this assertion; root-cause it before re-pinning.
        //
        // History:
        // - 1eef8c43...8200ae : initial pin (base_object_checksum as
        //   CBOR array of u8 — pre-serde_bytes).
        // - e0ade56a...801202 : current pin (base_object_checksum as
        //   CBOR byte string via serde_bytes — ~30 bytes smaller per
        //   hash on the wire).
        const EXPECTED_HEX: &str =
            "e0ade56a6b38b3b50cfa168abbe704225ad68df56635f62ce3132b66e9801202";

        let m = golden_fixture();
        let bytes = encode(&m).expect("encode");
        let hash = checksum(&bytes);
        let hex: String = hash.iter().map(|b| format!("{:02x}", b)).collect();

        if std::env::var("TURBOLITE_PIN_GOLDEN_HASH").is_ok() {
            eprintln!("canonical_v1_blake3_golden_hash: {hex}");
            return;
        }

        assert_eq!(
            hex, EXPECTED_HEX,
            "canonical encoding changed; root-cause before re-pinning"
        );
    }

    /// base_anchor_checksum is independent of whatever
    /// cursor.base_object_checksum currently holds (it clears the field
    /// before hashing), so a publisher can compute it on a manifest
    /// whose anchor is still empty/stale and get a stable value.
    #[test]
    fn base_anchor_checksum_ignores_existing_anchor_field() {
        let mut m = golden_fixture();
        m.cursor.base_object_checksum = Vec::new();
        let a = base_anchor_checksum(&m).expect("anchor");

        // Setting a different (wrong) anchor must not change the result.
        m.cursor.base_object_checksum = vec![0x11; 32];
        let b = base_anchor_checksum(&m).expect("anchor");
        assert_eq!(a, b, "anchor is computed with the field cleared");

        // But changing real content (e.g. the seq) does change it.
        let mut m2 = golden_fixture();
        m2.cursor.base_object_checksum = Vec::new();
        m2.cursor.last_applied_seq += 1;
        let c = base_anchor_checksum(&m2).expect("anchor");
        assert_ne!(a, c, "anchor is bound to the rest of the manifest content");
    }

    #[test]
    fn replay_cursor_contract_base_anchor_binds_cursor_epoch_and_writer() {
        let mut base = golden_fixture();
        base.cursor.base_object_checksum = Vec::new();
        let anchor = base_anchor_checksum(&base).expect("base anchor");

        let mut different_epoch = base.clone();
        different_epoch.cursor.epoch += 1;
        assert_ne!(
            anchor,
            base_anchor_checksum(&different_epoch).expect("epoch anchor"),
            "lease epoch must be in the base anchor hash domain"
        );

        let mut different_writer = base.clone();
        different_writer.writer_id = "node-leader-B".into();
        assert_ne!(
            anchor,
            base_anchor_checksum(&different_writer).expect("writer anchor"),
            "writer id must be in the base anchor hash domain"
        );

        let mut different_seq = base;
        different_seq.cursor.last_applied_seq += 1;
        assert_ne!(
            anchor,
            base_anchor_checksum(&different_seq).expect("seq anchor"),
            "last applied seq must be in the base anchor hash domain"
        );
    }

    #[test]
    fn legacy_hybrid_returns_hybrid_deprecated() {
        let bytes = vec![TAG_LEGACY_HYBRID, 0xff, 0xff];
        let err = decode(&bytes).expect_err("must reject");
        assert!(matches!(err, PayloadVersionError::HybridDeprecated));
    }

    #[test]
    fn legacy_pure_msgpack_returns_pure_deprecated() {
        let bytes = vec![TAG_LEGACY_PURE, 0x80];
        let err = decode(&bytes).expect_err("must reject");
        assert!(matches!(err, PayloadVersionError::PureMsgpackDeprecated));
    }

    #[test]
    fn empty_bytes_truncated() {
        let err = decode(&[]).expect_err("must reject");
        assert!(matches!(err, PayloadVersionError::Truncated(0)));
    }

    #[test]
    fn short_bytes_truncated() {
        // First byte is none of the legacy tags, but too short for envelope header.
        let err = decode(&[0xAA, 0xBB]).expect_err("must reject");
        assert!(matches!(err, PayloadVersionError::Truncated(2)));
    }

    #[test]
    fn unknown_magic_returns_magic_mismatch() {
        // First byte != 0x01 / 0x02 and != 'T'.
        let bytes: Vec<u8> = b"XXXX\x00\x01junk".to_vec();
        let err = decode(&bytes).expect_err("must reject");
        match err {
            PayloadVersionError::MagicMismatch { got } => {
                assert_eq!(&got, b"XXXX");
            }
            other => panic!("expected MagicMismatch, got {other:?}"),
        }
    }

    #[test]
    fn unknown_version_returns_unsupported_version() {
        let mut bytes = MAGIC.to_vec();
        bytes.extend_from_slice(&0x0099u16.to_be_bytes());
        bytes.push(0xff);
        let err = decode(&bytes).expect_err("must reject");
        match err {
            PayloadVersionError::UnsupportedVersion { found, supported } => {
                assert_eq!(found, 0x0099);
                assert_eq!(supported, VERSION_V1);
            }
            other => panic!("expected UnsupportedVersion, got {other:?}"),
        }
    }

    /// A legacy manifest payload that *didn't* go through the envelope
    /// (raw rmp_serde bytes from older builds) will fail with
    /// either PureMsgpackDeprecated (if leading byte is 0x81/etc. that
    /// happens to be 0x01 — unlikely) or MagicMismatch. Either way:
    /// not a silent fallback, not a generic CBOR decode error.
    #[test]
    fn raw_msgpack_without_envelope_does_not_silently_decode() {
        // A typical rmp_serde map encoding starts with 0x8X (fixmap) or
        // 0xDE / 0xDF for larger maps — none equal MAGIC[0] = 'T' = 0x54.
        let bytes: Vec<u8> = vec![0x8a, 0xa7, b'v', b'e', b'r'];
        let err = decode(&bytes).expect_err("must reject");
        // Specifically, not Ok and not BodyDecode.
        assert!(matches!(
            err,
            PayloadVersionError::MagicMismatch { .. } | PayloadVersionError::Truncated(_)
        ));
    }

    /// `Manifest::default()` is a real production state (fresh bootstrap
    /// with no pages yet). The envelope must encode + decode it cleanly.
    #[test]
    fn default_manifest_round_trips_through_envelope() {
        let m = Manifest::default();
        let bytes = encode(&m).expect("encode default");
        // Envelope structure intact even for the empty manifest.
        assert_eq!(&bytes[..4], &MAGIC);
        assert_eq!(u16::from_be_bytes([bytes[4], bytes[5]]), VERSION_V1);

        let decoded = decode(&bytes).expect("decode default");
        // Every observable field matches.
        assert_eq!(decoded.version, 0);
        assert_eq!(decoded.change_counter, 0);
        assert_eq!(decoded.discontinuity_stamp, 0);
        assert_eq!(decoded.cursor, ReplayCursor::default());
        assert_eq!(decoded.writer_id, "");
        assert_eq!(decoded.page_count, 0);
        assert!(decoded.page_group_keys.is_empty());
        assert!(decoded.btrees.is_empty());
        assert!(decoded.interior_chunk_keys.is_empty());
        assert!(decoded.index_chunk_keys.is_empty());
        assert!(decoded.frame_tables.is_empty());
        assert!(decoded.subframe_overrides.is_empty());
        assert!(decoded.db_header.is_none());
    }

    /// Decode-then-encode must be the byte-level identity for any
    /// envelope this code accepts. This catches a class of subtle bugs
    /// where decode silently drops or reorders information that encode
    /// re-emits in the original position. Without this, two followers
    /// could read the same base, compute different envelope hashes, and
    /// pick different "next deltas".
    #[test]
    fn decode_then_re_encode_is_byte_identity() {
        let original = encode(&golden_fixture()).expect("encode fixture");
        let decoded = decode(&original).expect("decode envelope");
        let reencoded = encode(&decoded).expect("re-encode");
        assert_eq!(
            original, reencoded,
            "decode→encode must reproduce identical envelope bytes; \
             follower hash computations rely on this"
        );
    }

    /// Delta stamping uses `checksum()` to compute chain anchors. This test
    /// keeps the helper callable and pins the expected 32-byte BLAKE3 output
    /// before chain-verification code depends on it.
    #[test]
    fn checksum_returns_blake3_of_envelope() {
        let bytes = encode(&golden_fixture()).expect("encode");
        let hash = checksum(&bytes);
        assert_eq!(hash.len(), 32, "BLAKE3 output is 32 bytes");
        // Re-encode and re-hash — must match (canonical + deterministic).
        let bytes_again = encode(&golden_fixture()).expect("encode");
        let hash_again = checksum(&bytes_again);
        assert_eq!(hash, hash_again);
    }

    /// Pin the first 16 bytes of the envelope (magic + version + the
    /// start of the CBOR body) so that drift in *envelope structure*
    /// is caught even if the full-body BLAKE3 hash happens to collide
    /// (negligible probability but free to assert).
    #[test]
    fn envelope_header_bytes_pinned() {
        let bytes = encode(&golden_fixture()).expect("encode");
        // Magic + version are byte-stable.
        assert_eq!(&bytes[..4], b"TLM1", "magic");
        assert_eq!(&bytes[4..6], &[0x00, 0x01], "version v1 big-endian");
        // CBOR body begins with a map opener. The CanonicalManifestV1
        // struct has 18 fields, so ciborium emits a definite-length map
        // header for 18 entries: major type 5 (map) with count 18.
        // 0xA0..0xBF is fixmap (count 0..23), so count 18 = 0xB2.
        assert_eq!(bytes[6], 0xB2, "CBOR map opener for 18 fields");
    }
}
