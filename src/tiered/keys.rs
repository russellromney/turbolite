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
