//! Turbolite manifest wire format.
//!
//! These bytes are what turbolite publishes through a
//! `turbodb::ManifestStore` (the envelope's opaque `payload`). Keeping
//! the codec here means turbolite alone owns its persisted shape, and
//! consumers (haqlite, a future haqlite-turbolite sibling crate) carry
//! raw `Vec<u8>` between the VFS and the manifest store.
//!
//! One active shape, discriminated by a one-byte tag:
//!
//! - `TAG_PURE` = pure `Manifest` — produced by `manifest_bytes`.
//!
//! Older experimental builds also wrote a hybrid payload that embedded walrust
//! delta cursor/prefix fields beside the page manifest. That is intentionally
//! not part of the active Turbolite wire contract: page manifests describe
//! checkpointed page/base state only. Higher integration layers discover and
//! replay delta objects from their own configured storage.
//!
//! Leading tag (vs. field-presence discrimination) makes decoding
//! unambiguous without peeking at the msgpack structure and cheap —
//! one byte compare on the fast path.

use std::io;

use super::manifest::Manifest;

const TAG_PURE: u8 = 0x01;
const TAG_LEGACY_HYBRID: u8 = 0x02;

/// Serialize a pure turbolite manifest to wire bytes.
pub(crate) fn encode_pure(manifest: &Manifest) -> io::Result<Vec<u8>> {
    let body = rmp_serde::to_vec(manifest)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("manifest encode: {e}")))?;
    let mut out = Vec::with_capacity(body.len() + 1);
    out.push(TAG_PURE);
    out.extend_from_slice(&body);
    Ok(out)
}

/// Decode wire bytes produced by `encode_pure`.
///
/// An empty input decodes to an empty `Manifest` with no walrust delta —
/// the ManifestStore returning `None` (no manifest yet) should be
/// mapped to `Ok(None)` by the caller before getting here; but decoding
/// legitimately-empty manifests shouldn't crash.
pub(crate) fn decode(bytes: &[u8]) -> io::Result<Manifest> {
    if bytes.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "manifest wire bytes are empty",
        ));
    }
    match bytes[0] {
        TAG_PURE => {
            let manifest: Manifest = rmp_serde::from_slice(&bytes[1..]).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("pure manifest decode: {e}"),
                )
            })?;
            Ok(manifest)
        }
        TAG_LEGACY_HYBRID => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "legacy hybrid manifest payloads are no longer accepted by Turbolite; delta replay is owned by the integration layer",
        )),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown turbolite manifest wire tag: 0x{other:02x}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tiered::GroupingStrategy;
    use std::collections::HashMap;

    fn sample_manifest() -> Manifest {
        Manifest {
            version: 7,
            change_counter: 42,
            page_count: 128,
            page_size: 4096,
            pages_per_group: 4,
            sub_pages_per_frame: 2,
            strategy: GroupingStrategy::BTreeAware,
            page_group_keys: vec!["pg/0_v7".into(), "pg/1_v7".into()],
            frame_tables: Vec::new(),
            group_pages: vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7]],
            btrees: HashMap::new(),
            interior_chunk_keys: HashMap::new(),
            index_chunk_keys: HashMap::new(),
            subframe_overrides: Vec::new(),
            page_index: HashMap::new(),
            btree_groups: HashMap::new(),
            page_to_tree_name: HashMap::new(),
            tree_name_to_groups: HashMap::new(),
            group_to_tree_name: HashMap::new(),
            db_header: None,
            epoch: 3,
        }
    }

    #[test]
    fn pure_round_trip() {
        let m = sample_manifest();
        let bytes = encode_pure(&m).expect("encode");
        assert_eq!(bytes[0], TAG_PURE);
        let decoded = decode(&bytes).expect("decode");
        assert_eq!(decoded.version, m.version);
        assert_eq!(decoded.change_counter, m.change_counter);
        assert_eq!(decoded.epoch, m.epoch);
        assert_eq!(decoded.page_count, m.page_count);
        assert_eq!(decoded.page_group_keys, m.page_group_keys);
    }

    #[test]
    fn legacy_hybrid_payload_rejected() {
        let mut bytes = vec![TAG_LEGACY_HYBRID];
        bytes.extend_from_slice(b"not-current-contract");
        let err = decode(&bytes).expect_err("legacy hybrid must fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("legacy hybrid manifest payloads"));
    }

    #[test]
    fn unknown_tag_rejected() {
        let err = decode(&[0xff, 0x01, 0x02]).expect_err("should fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn empty_rejected() {
        let err = decode(&[]).expect_err("should fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
