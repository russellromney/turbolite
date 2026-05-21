//! Turbolite's on-backend key layout.
//!
//! These are the key strings turbolite uses when calling `StorageBackend`.
//! Backends that want tenant/database scoping apply their own prefix; keys
//! here are always relative.

/// The manifest is the atomic commit point for a turbolite database. Backends
/// see this as an ordinary object; the serialisation format (msgpack of
/// `Manifest`) is a turbolite concern.
pub const MANIFEST_KEY: &str = "manifest.msgpack";

/// Data page group key: `p/d/{group_id}_v{version}`.
pub fn page_group_key(group_id: u64, version: u64) -> String {
    format!("p/d/{group_id}_v{version}")
}

/// Interior (B-tree inner node) bundle key: `p/it/{chunk_id}_v{version}`.
pub fn interior_chunk_key(chunk_id: u32, version: u64) -> String {
    format!("p/it/{chunk_id}_v{version}")
}

/// Index leaf bundle key: `p/ix/{chunk_id}_v{version}`.
pub fn index_chunk_key(chunk_id: u32, version: u64) -> String {
    format!("p/ix/{chunk_id}_v{version}")
}

/// Override frame key (subframe-level dirty update).
pub fn override_frame_key(group_id: u64, frame_idx: usize, version: u64) -> String {
    format!("p/d/{group_id}_f{frame_idx}_v{version}")
}

// ===== GCM additional authenticated data (AAD) =====
//
// Each encrypted frame is bound to its slot identity via AES-GCM AAD. The AAD
// is NOT encrypted, but the GCM tag authenticates it: a frame encrypted for one
// slot fails to decrypt when presented under another slot's AAD. This stops a
// valid encrypted blob from being swapped across slots (e.g. relocating page
// group 1's bytes onto page group 2's key, or an index bundle onto an interior
// slot).
//
// The AAD is slot-bound but version-INDEPENDENT: it carries the object identity
// (group id, chunk id, frame index, kind) but never the version number. Cross-
// version reads are routine — compaction, merge, and rotation all read an old
// versioned object and re-encode it under a new version — so binding to version
// would break those reads. Identity is enough to defeat cross-slot swaps; the
// manifest already authenticates which version a slot points at.
//
// A leading domain tag keeps the namespaces disjoint so e.g. a page group can
// never be confused with an override frame that happens to share a numeric id.

/// AAD binding an encrypted page-group blob (whole or seekable frame) to its
/// group id. Used for both `encode_page_group` and `encode_page_group_seekable`
/// frames; every frame in a group shares the group's AAD.
pub fn aad_page_group(gid: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(1 + 8);
    v.push(b'g'); // page group
    v.extend_from_slice(&gid.to_le_bytes());
    v
}

/// AAD binding an encrypted interior (B-tree inner) bundle to its chunk id.
pub fn aad_interior_bundle(chunk_id: u32) -> Vec<u8> {
    let mut v = Vec::with_capacity(1 + 4);
    v.push(b'i'); // interior bundle
    v.extend_from_slice(&chunk_id.to_le_bytes());
    v
}

/// AAD binding an encrypted index leaf bundle to its chunk id. Distinct domain
/// tag from `aad_interior_bundle` so the two bundle kinds can never cross-swap
/// even at the same numeric chunk id.
pub fn aad_index_bundle(chunk_id: u32) -> Vec<u8> {
    let mut v = Vec::with_capacity(1 + 4);
    v.push(b'x'); // index bundle
    v.extend_from_slice(&chunk_id.to_le_bytes());
    v
}

/// AAD binding an encrypted override frame to its (group id, frame index) slot.
pub fn aad_override_frame(gid: u64, frame_idx: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(1 + 8 + 8);
    v.push(b'o'); // override frame
    v.extend_from_slice(&gid.to_le_bytes());
    v.extend_from_slice(&(frame_idx as u64).to_le_bytes());
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_shapes_are_stable() {
        assert_eq!(page_group_key(0, 1), "p/d/0_v1");
        assert_eq!(page_group_key(42, 7), "p/d/42_v7");
        assert_eq!(interior_chunk_key(3, 5), "p/it/3_v5");
        assert_eq!(index_chunk_key(1, 2), "p/ix/1_v2");
        assert_eq!(override_frame_key(5, 3, 10), "p/d/5_f3_v10");
    }
}
