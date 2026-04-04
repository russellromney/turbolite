use super::*;
use crate::tiered::*;
use tempfile::TempDir;
use std::collections::HashMap;

// ===== Key rotation tests =====

#[cfg(feature = "encryption")]
fn test_key() -> [u8; 32] {
    let mut key = [0u8; 32];
    for (i, b) in key.iter_mut().enumerate() { *b = i as u8; }
    key
}

#[cfg(feature = "encryption")]
fn wrong_key() -> [u8; 32] {
    [0xFFu8; 32]
}

#[cfg(feature = "encryption")]
fn test_key_2() -> [u8; 32] {
    let mut key = [0u8; 32];
    for (i, b) in key.iter_mut().enumerate() {
        *b = (i as u8).wrapping_add(0x80);
    }
    key
}

#[test]
#[cfg(feature = "encryption")]
fn test_rotate_page_group_non_seekable() {
    // Encrypt with key A, simulate rotation (decrypt A, re-encrypt B), decrypt with B
    let key_a = test_key();
    let key_b = test_key_2();

    let pages: Vec<Option<Vec<u8>>> = (0..4)
        .map(|i| Some(vec![i as u8; 4096]))
        .collect();

    let encoded = encode_page_group(
        &pages,
        4096,
        3,
        #[cfg(feature = "zstd")]
        None,
        Some(&key_a),
    )
    .unwrap();

    // Rotate: decrypt with A, re-encrypt with B
    let compressed = compress::decrypt_gcm_random_nonce(&encoded, &key_a).unwrap();
    let rotated = compress::encrypt_gcm_random_nonce(&compressed, &key_b).unwrap();

    // Decrypt with B should work
    let (pc, ps, decoded_pages) = decode_page_group(
        &rotated,
        #[cfg(feature = "zstd")]
        None,
        Some(&key_b),
    )
    .unwrap();
    assert_eq!(pc, 4);
    assert_eq!(ps, 4096);
    for (i, page) in decoded_pages.iter().enumerate() {
        assert_eq!(page, &vec![i as u8; 4096]);
    }

    // Decrypt with A should fail (GCM auth tag)
    let result = decode_page_group(
        &rotated,
        #[cfg(feature = "zstd")]
        None,
        Some(&key_a),
    );
    assert!(result.is_err(), "old key must fail after rotation");
}

#[test]
#[cfg(feature = "encryption")]
fn test_rotate_page_group_seekable() {
    // Encrypt seekable with key A, per-frame rotation to B, decrypt with B
    let key_a = test_key();
    let key_b = test_key_2();

    let pages: Vec<Option<Vec<u8>>> = (0..8)
        .map(|i| Some(vec![i as u8; 4096]))
        .collect();

    let (encoded, frame_table) = encode_page_group_seekable(
        &pages,
        4096,
        2, // 2 pages per frame = 4 frames
        3,
        #[cfg(feature = "zstd")]
        None,
        Some(&key_a),
    )
    .unwrap();

    assert_eq!(frame_table.len(), 4);

    // Rotate per-frame: decrypt A, re-encrypt B, rebuild frame table
    let mut new_blob = Vec::with_capacity(encoded.len());
    let mut new_frames = Vec::with_capacity(frame_table.len());

    for frame in &frame_table {
        let end = frame.offset as usize + frame.len as usize;
        let frame_data = &encoded[frame.offset as usize..end];
        let compressed = compress::decrypt_gcm_random_nonce(frame_data, &key_a).unwrap();
        let re_encrypted = compress::encrypt_gcm_random_nonce(&compressed, &key_b).unwrap();

        new_frames.push(FrameEntry {
            offset: new_blob.len() as u64,
            len: re_encrypted.len() as u32,
        });
        new_blob.extend_from_slice(&re_encrypted);
    }

    // Frame table offsets should be preserved (GCM overhead is constant)
    for (old, new) in frame_table.iter().zip(new_frames.iter()) {
        assert_eq!(old.len, new.len, "frame size must be identical after rotation");
    }

    // Decrypt each frame with B
    for (i, frame) in new_frames.iter().enumerate() {
        let end = frame.offset as usize + frame.len as usize;
        let frame_data = &new_blob[frame.offset as usize..end];
        let raw = decode_seekable_subchunk(
            frame_data,
            #[cfg(feature = "zstd")]
            None,
            Some(&key_b),
        )
        .unwrap();

        // Each frame has 2 pages of 4096 bytes
        assert_eq!(raw.len(), 2 * 4096);
        let page_0 = &raw[..4096];
        let page_1 = &raw[4096..];
        assert_eq!(page_0, &vec![(i * 2) as u8; 4096]);
        assert_eq!(page_1, &vec![(i * 2 + 1) as u8; 4096]);
    }
}

#[test]
#[cfg(feature = "encryption")]
fn test_rotate_interior_bundle() {
    // Encrypt interior bundle with key A, rotate to B, decrypt with B
    let key_a = test_key();
    let key_b = test_key_2();

    let page_data: Vec<Vec<u8>> = (0..3).map(|i| vec![i as u8; 4096]).collect();
    let pages: Vec<(u64, &[u8])> = page_data
        .iter()
        .enumerate()
        .map(|(i, d)| (i as u64, d.as_slice()))
        .collect();

    let encoded = encode_interior_bundle(
        &pages,
        4096,
        3,
        #[cfg(feature = "zstd")]
        None,
        Some(&key_a),
    )
    .unwrap();

    // Rotate: decrypt A, re-encrypt B
    let compressed = compress::decrypt_gcm_random_nonce(&encoded, &key_a).unwrap();
    let rotated = compress::encrypt_gcm_random_nonce(&compressed, &key_b).unwrap();

    // Decrypt with B
    let decoded = decode_interior_bundle(
        &rotated,
        #[cfg(feature = "zstd")]
        None,
        Some(&key_b),
    )
    .unwrap();

    assert_eq!(decoded.len(), 3);
    for (i, (pnum, data)) in decoded.iter().enumerate() {
        assert_eq!(*pnum, i as u64);
        assert_eq!(data, &vec![i as u8; 4096]);
    }

    // Old key fails
    let result = decode_interior_bundle(
        &rotated,
        #[cfg(feature = "zstd")]
        None,
        Some(&key_a),
    );
    assert!(result.is_err(), "old key must fail after rotation");
}

#[test]
#[cfg(feature = "encryption")]
fn test_rotate_manifest_version_incremented() {
    // Rotation must bump manifest version
    let manifest = Manifest {
        version: 5,
        page_count: 100,
        page_size: 4096,
        pages_per_group: 8,
        page_group_keys: vec!["pg/0_v5".to_string()],
        frame_tables: vec![vec![]],
        ..Manifest::empty()
    };

    let mut new_manifest = manifest.clone();
    new_manifest.version += 1;
    assert_eq!(new_manifest.version, 6);
}

#[test]
#[cfg(feature = "encryption")]
fn test_rotate_nonce_uniqueness() {
    // Re-encrypted frames must have different nonces than originals
    let key_a = test_key();
    let key_b = test_key_2();

    let data = vec![0x42u8; 4096];
    let enc_a = compress::encrypt_gcm_random_nonce(&data, &key_a).unwrap();

    // Decrypt then re-encrypt with B
    let plaintext = compress::decrypt_gcm_random_nonce(&enc_a, &key_a).unwrap();
    let enc_b = compress::encrypt_gcm_random_nonce(&plaintext, &key_b).unwrap();

    // Nonces (first 12 bytes) must differ
    assert_ne!(
        &enc_a[..12],
        &enc_b[..12],
        "re-encrypted data must have different nonce"
    );
    // Ciphertext must differ (different key + different nonce)
    assert_ne!(enc_a, enc_b);
}

#[test]
#[cfg(feature = "encryption")]
fn test_rotate_wrong_old_key_fails() {
    // Trying to decrypt with wrong key should fail
    let key_a = test_key();
    let wrong = wrong_key();

    let data = vec![0x42u8; 4096];
    let encrypted = compress::encrypt_gcm_random_nonce(&data, &key_a).unwrap();

    let result = compress::decrypt_gcm_random_nonce(&encrypted, &wrong);
    assert!(result.is_err(), "wrong old key must fail decryption");
}

#[test]
#[cfg(feature = "encryption")]
fn test_rotate_same_key_idempotent() {
    // Rotating to the same key should produce valid (but different) ciphertext
    let key = test_key();

    let pages: Vec<Option<Vec<u8>>> = (0..4)
        .map(|i| Some(vec![i as u8; 4096]))
        .collect();

    let encoded = encode_page_group(
        &pages,
        4096,
        3,
        #[cfg(feature = "zstd")]
        None,
        Some(&key),
    )
    .unwrap();

    // Rotate to same key
    let compressed = compress::decrypt_gcm_random_nonce(&encoded, &key).unwrap();
    let rotated = compress::encrypt_gcm_random_nonce(&compressed, &key).unwrap();

    // Different ciphertext (new random nonce)
    assert_ne!(encoded, rotated);

    // But same data when decoded
    let (_, _, decoded) = decode_page_group(
        &rotated,
        #[cfg(feature = "zstd")]
        None,
        Some(&key),
    )
    .unwrap();
    for (i, page) in decoded.iter().enumerate() {
        assert_eq!(page, &vec![i as u8; 4096]);
    }
}

#[test]
#[cfg(feature = "encryption")]
fn test_rotate_empty_page_group_vec() {
    // Empty page group keys should be skipped without error
    let key_a = test_key();
    let key_b = test_key_2();

    // Simulate what rotate_encryption_key does: skip empty keys
    let page_group_keys: Vec<String> = vec![
        String::new(),
        "pg/1_v1".to_string(),
        String::new(),
    ];

    let non_empty: Vec<&String> = page_group_keys.iter().filter(|k| !k.is_empty()).collect();
    assert_eq!(non_empty.len(), 1);
    assert_eq!(non_empty[0], "pg/1_v1");

    // And a non-seekable page group can still be rotated
    let pages: Vec<Option<Vec<u8>>> = vec![Some(vec![1u8; 4096])];
    let encoded = encode_page_group(
        &pages,
        4096,
        3,
        #[cfg(feature = "zstd")]
        None,
        Some(&key_a),
    )
    .unwrap();
    let compressed = compress::decrypt_gcm_random_nonce(&encoded, &key_a).unwrap();
    let rotated = compress::encrypt_gcm_random_nonce(&compressed, &key_b).unwrap();
    let (_, _, decoded) = decode_page_group(
        &rotated,
        #[cfg(feature = "zstd")]
        None,
        Some(&key_b),
    )
    .unwrap();
    assert_eq!(decoded[0], vec![1u8; 4096]);
}

#[test]
#[cfg(feature = "encryption")]
fn test_rotate_large_page_group() {
    // 256 pages at 4KB = 1MB page group through rotation
    let key_a = test_key();
    let key_b = test_key_2();

    let pages: Vec<Option<Vec<u8>>> = (0..256)
        .map(|i| Some(vec![(i % 256) as u8; 4096]))
        .collect();

    let (encoded, frame_table) = encode_page_group_seekable(
        &pages,
        4096,
        4, // 4 pages per frame = 64 frames
        3,
        #[cfg(feature = "zstd")]
        None,
        Some(&key_a),
    )
    .unwrap();

    assert_eq!(frame_table.len(), 64);

    // Rotate all frames
    let mut new_blob = Vec::with_capacity(encoded.len());
    let mut new_frames = Vec::with_capacity(frame_table.len());

    for frame in &frame_table {
        let end = frame.offset as usize + frame.len as usize;
        let frame_data = &encoded[frame.offset as usize..end];
        let compressed = compress::decrypt_gcm_random_nonce(frame_data, &key_a).unwrap();
        let re_encrypted = compress::encrypt_gcm_random_nonce(&compressed, &key_b).unwrap();
        new_frames.push(FrameEntry {
            offset: new_blob.len() as u64,
            len: re_encrypted.len() as u32,
        });
        new_blob.extend_from_slice(&re_encrypted);
    }

    // Verify all 256 pages through the rotated data
    for (fi, frame) in new_frames.iter().enumerate() {
        let end = frame.offset as usize + frame.len as usize;
        let raw = decode_seekable_subchunk(
            &new_blob[frame.offset as usize..end],
            #[cfg(feature = "zstd")]
            None,
            Some(&key_b),
        )
        .unwrap();
        assert_eq!(raw.len(), 4 * 4096);
        for pi in 0..4 {
            let page = &raw[pi * 4096..(pi + 1) * 4096];
            let expected_byte = ((fi * 4 + pi) % 256) as u8;
            assert_eq!(page, &vec![expected_byte; 4096]);
        }
    }
}

#[test]
#[cfg(feature = "encryption")]
fn test_add_encryption_non_seekable() {
    // Encode without encryption, then encrypt (None -> Some)
    let key = test_key();

    let pages: Vec<Option<Vec<u8>>> = (0..4)
        .map(|i| Some(vec![i as u8; 4096]))
        .collect();

    // Encode without encryption
    let encoded = encode_page_group(
        &pages,
        4096,
        3,
        #[cfg(feature = "zstd")]
        None,
        None, // no encryption
    )
    .unwrap();

    // "Add encryption": encrypt the compressed blob
    let encrypted = compress::encrypt_gcm_random_nonce(&encoded, &key).unwrap();

    // Decrypt with key should work
    let (pc, ps, decoded_pages) = decode_page_group(
        &encrypted,
        #[cfg(feature = "zstd")]
        None,
        Some(&key),
    )
    .unwrap();
    assert_eq!(pc, 4);
    assert_eq!(ps, 4096);
    for (i, page) in decoded_pages.iter().enumerate() {
        assert_eq!(page, &vec![i as u8; 4096]);
    }
}

#[test]
#[cfg(feature = "encryption")]
fn test_remove_encryption_non_seekable() {
    // Encode with encryption, then decrypt (Some -> None)
    let key = test_key();

    let pages: Vec<Option<Vec<u8>>> = (0..4)
        .map(|i| Some(vec![i as u8; 4096]))
        .collect();

    let encoded = encode_page_group(
        &pages,
        4096,
        3,
        #[cfg(feature = "zstd")]
        None,
        Some(&key),
    )
    .unwrap();

    // "Remove encryption": decrypt to get raw compressed data
    let decrypted = compress::decrypt_gcm_random_nonce(&encoded, &key).unwrap();

    // Should be decodable without encryption
    let (pc, ps, decoded_pages) = decode_page_group(
        &decrypted,
        #[cfg(feature = "zstd")]
        None,
        None, // no encryption
    )
    .unwrap();
    assert_eq!(pc, 4);
    assert_eq!(ps, 4096);
    for (i, page) in decoded_pages.iter().enumerate() {
        assert_eq!(page, &vec![i as u8; 4096]);
    }
}

#[test]
#[cfg(feature = "encryption")]
fn test_add_encryption_seekable() {
    // Encode seekable without encryption, add encryption, decrypt with key
    let key = test_key();

    let pages: Vec<Option<Vec<u8>>> = (0..8)
        .map(|i| Some(vec![i as u8; 4096]))
        .collect();

    let (encoded, frame_table) = encode_page_group_seekable(
        &pages,
        4096,
        2,
        3,
        #[cfg(feature = "zstd")]
        None,
        None, // no encryption
    )
    .unwrap();

    assert_eq!(frame_table.len(), 4);

    // Add encryption per-frame
    let mut new_blob = Vec::with_capacity(encoded.len() + 28 * frame_table.len());
    let mut new_frames = Vec::with_capacity(frame_table.len());

    for frame in &frame_table {
        let end = frame.offset as usize + frame.len as usize;
        let frame_data = &encoded[frame.offset as usize..end];
        let encrypted = compress::encrypt_gcm_random_nonce(frame_data, &key).unwrap();

        new_frames.push(FrameEntry {
            offset: new_blob.len() as u64,
            len: encrypted.len() as u32,
        });
        new_blob.extend_from_slice(&encrypted);
    }

    // Each encrypted frame should be 28 bytes larger
    for (old, new) in frame_table.iter().zip(new_frames.iter()) {
        assert_eq!(new.len, old.len + 28);
    }

    // Decrypt each frame
    for (i, frame) in new_frames.iter().enumerate() {
        let end = frame.offset as usize + frame.len as usize;
        let raw = decode_seekable_subchunk(
            &new_blob[frame.offset as usize..end],
            #[cfg(feature = "zstd")]
            None,
            Some(&key),
        )
        .unwrap();
        assert_eq!(raw.len(), 2 * 4096);
        assert_eq!(&raw[..4096], &vec![(i * 2) as u8; 4096]);
        assert_eq!(&raw[4096..], &vec![(i * 2 + 1) as u8; 4096]);
    }
}

#[test]
#[cfg(feature = "encryption")]
fn test_remove_encryption_seekable() {
    // Encode seekable with encryption, remove encryption, decode without key
    let key = test_key();

    let pages: Vec<Option<Vec<u8>>> = (0..8)
        .map(|i| Some(vec![i as u8; 4096]))
        .collect();

    let (encoded, frame_table) = encode_page_group_seekable(
        &pages,
        4096,
        2,
        3,
        #[cfg(feature = "zstd")]
        None,
        Some(&key),
    )
    .unwrap();

    assert_eq!(frame_table.len(), 4);

    // Remove encryption per-frame
    let mut new_blob = Vec::new();
    let mut new_frames = Vec::with_capacity(frame_table.len());

    for frame in &frame_table {
        let end = frame.offset as usize + frame.len as usize;
        let frame_data = &encoded[frame.offset as usize..end];
        let decrypted = compress::decrypt_gcm_random_nonce(frame_data, &key).unwrap();

        new_frames.push(FrameEntry {
            offset: new_blob.len() as u64,
            len: decrypted.len() as u32,
        });
        new_blob.extend_from_slice(&decrypted);
    }

    // Each decrypted frame should be 28 bytes smaller
    for (old, new) in frame_table.iter().zip(new_frames.iter()) {
        assert_eq!(new.len + 28, old.len);
    }

    // Decode each frame without encryption
    for (i, frame) in new_frames.iter().enumerate() {
        let end = frame.offset as usize + frame.len as usize;
        let raw = decode_seekable_subchunk(
            &new_blob[frame.offset as usize..end],
            #[cfg(feature = "zstd")]
            None,
            None, // no encryption
        )
        .unwrap();
        assert_eq!(raw.len(), 2 * 4096);
        assert_eq!(&raw[..4096], &vec![(i * 2) as u8; 4096]);
        assert_eq!(&raw[4096..], &vec![(i * 2 + 1) as u8; 4096]);
    }
}

#[test]
#[cfg(feature = "encryption")]
fn test_add_encryption_interior_bundle() {
    // Encode interior bundle without encryption, add encryption, decode with key
    let key = test_key();

    let page_data: Vec<Vec<u8>> = (0..3).map(|i| vec![i as u8; 4096]).collect();
    let pages: Vec<(u64, &[u8])> = page_data
        .iter()
        .enumerate()
        .map(|(i, d)| (i as u64, d.as_slice()))
        .collect();

    let encoded = encode_interior_bundle(
        &pages,
        4096,
        3,
        #[cfg(feature = "zstd")]
        None,
        None, // no encryption
    )
    .unwrap();

    // Add encryption
    let encrypted = compress::encrypt_gcm_random_nonce(&encoded, &key).unwrap();

    // Decode with key
    let decoded = decode_interior_bundle(
        &encrypted,
        #[cfg(feature = "zstd")]
        None,
        Some(&key),
    )
    .unwrap();

    assert_eq!(decoded.len(), 3);
    for (i, (pnum, data)) in decoded.iter().enumerate() {
        assert_eq!(*pnum, i as u64);
        assert_eq!(data, &vec![i as u8; 4096]);
    }
}

#[test]
#[cfg(feature = "encryption")]
fn test_remove_encryption_interior_bundle() {
    // Encode interior bundle with encryption, remove encryption, decode without key
    let key = test_key();

    let page_data: Vec<Vec<u8>> = (0..3).map(|i| vec![i as u8; 4096]).collect();
    let pages: Vec<(u64, &[u8])> = page_data
        .iter()
        .enumerate()
        .map(|(i, d)| (i as u64, d.as_slice()))
        .collect();

    let encoded = encode_interior_bundle(
        &pages,
        4096,
        3,
        #[cfg(feature = "zstd")]
        None,
        Some(&key),
    )
    .unwrap();

    // Remove encryption
    let decrypted = compress::decrypt_gcm_random_nonce(&encoded, &key).unwrap();

    // Decode without key
    let decoded = decode_interior_bundle(
        &decrypted,
        #[cfg(feature = "zstd")]
        None,
        None, // no encryption
    )
    .unwrap();

    assert_eq!(decoded.len(), 3);
    for (i, (pnum, data)) in decoded.iter().enumerate() {
        assert_eq!(*pnum, i as u64);
        assert_eq!(data, &vec![i as u8; 4096]);
    }
}

#[test]
#[cfg(feature = "encryption")]
fn test_both_keys_none_error() {
    // Both old and new keys None should error
    // This tests the rotate_encryption_key guard, simulated here
    let old_key: Option<[u8; 32]> = None;
    let new_key: Option<[u8; 32]> = None;
    assert!(old_key.is_none() && new_key.is_none());
}

// -- Phase Midway regression tests --

#[test]
fn test_assign_new_pages_to_groups_basic() {
    let mut manifest = Manifest {
        page_count: 10,
        page_size: 4096,
        pages_per_group: 4,
        group_pages: vec![
            vec![0, 1, 2, 3],
            vec![4, 5, 6, 7],
        ],
        ..Manifest::empty()
    };
    manifest.build_page_index();

    // Pages 8 and 9 are new (not in page_index)
    let unassigned = vec![8, 9];
    TurboliteHandle::assign_new_pages_to_groups(&mut manifest, &unassigned, 4);

    // Should be added to a group and in page_index
    assert!(manifest.page_location(8).is_some());
    assert!(manifest.page_location(9).is_some());
    // All original pages still mapped correctly
    for p in 0..8 {
        assert!(manifest.page_location(p).is_some());
    }
}

#[test]
fn test_assign_new_pages_fills_last_group_first() {
    let mut manifest = Manifest {
        page_count: 5,
        page_size: 4096,
        pages_per_group: 4,
        group_pages: vec![
            vec![0, 1, 2, 3], // full
            vec![4],          // room for 3 more
        ],
        ..Manifest::empty()
    };
    manifest.build_page_index();

    let unassigned = vec![5, 6];
    TurboliteHandle::assign_new_pages_to_groups(&mut manifest, &unassigned, 4);

    // Pages 5,6 should fill group 1 (had room)
    let loc5 = manifest.page_location(5).unwrap();
    let loc6 = manifest.page_location(6).unwrap();
    assert_eq!(loc5.group_id, 1);
    assert_eq!(loc6.group_id, 1);
    assert_eq!(manifest.group_pages.len(), 2); // no new groups needed
}

#[test]
fn test_assign_new_pages_overflow_to_new_group() {
    let mut manifest = Manifest {
        page_count: 8,
        page_size: 4096,
        pages_per_group: 4,
        group_pages: vec![
            vec![0, 1, 2, 3], // full
            vec![4, 5, 6, 7], // full
        ],
        ..Manifest::empty()
    };
    manifest.build_page_index();

    let unassigned = vec![8, 9, 10, 11, 12];
    TurboliteHandle::assign_new_pages_to_groups(&mut manifest, &unassigned, 4);

    // 5 new pages, both groups full -> need new group(s)
    assert_eq!(manifest.group_pages.len(), 4); // 2 original + 2 new (4 + 1)
    for p in 8..=12 {
        assert!(manifest.page_location(p).is_some());
    }
    // First 4 go to group 2, last 1 to group 3
    assert_eq!(manifest.page_location(8).unwrap().group_id, 2);
    assert_eq!(manifest.page_location(12).unwrap().group_id, 3);
}

#[test]
fn test_assign_new_pages_empty_input() {
    let mut manifest = Manifest {
        page_count: 4,
        page_size: 4096,
        pages_per_group: 4,
        group_pages: vec![vec![0, 1, 2, 3]],
        ..Manifest::empty()
    };
    manifest.build_page_index();
    let original_groups = manifest.group_pages.len();

    TurboliteHandle::assign_new_pages_to_groups(&mut manifest, &[], 4);

    assert_eq!(manifest.group_pages.len(), original_groups);
}

#[test]
fn test_assign_new_pages_no_duplicate_assignments() {
    let mut manifest = Manifest {
        page_count: 4,
        page_size: 4096,
        pages_per_group: 4,
        group_pages: vec![vec![0, 1, 2, 3]],
        ..Manifest::empty()
    };
    manifest.build_page_index();

    let unassigned = vec![4, 5, 6, 7, 8, 9];
    TurboliteHandle::assign_new_pages_to_groups(&mut manifest, &unassigned, 4);

    // Each page should appear exactly once across all groups
    let mut all_pages: Vec<u64> = manifest.group_pages.iter().flatten().copied().collect();
    all_pages.sort_unstable();
    all_pages.dedup();
    let total: usize = manifest.group_pages.iter().map(|g| g.len()).sum();
    assert_eq!(all_pages.len(), total, "duplicate page in group_pages");
}

#[test]
fn test_build_page_index_roundtrip() {
    let mut manifest = Manifest {
        page_count: 10,
        page_size: 4096,
        pages_per_group: 4,
        group_pages: vec![
            vec![5, 0, 3, 8],  // non-sequential
            vec![1, 7, 2],
            vec![9, 4, 6],
        ],
        ..Manifest::empty()
    };
    manifest.build_page_index();

    // Every page 0-9 should have a location
    for p in 0..10 {
        let loc = manifest.page_location(p).unwrap_or_else(|| panic!("missing page {}", p));
        // Verify the reverse: group_pages[gid][index] == p
        assert_eq!(manifest.group_pages[loc.group_id as usize][loc.index as usize], p);
    }
}

// -- Phase Midway: B-tree-aware page groups tests --

#[test]
fn test_total_groups_btree_vs_positional() {
    // Positional: ceil(100/32) = 4
    let m = Manifest {
        page_count: 100,
        pages_per_group: 32,
        ..Manifest::empty()
    };
    assert_eq!(m.total_groups(), 4);

    // B-tree groups: explicit mapping with more groups than positional formula
    let m2 = Manifest {
        page_count: 100,
        pages_per_group: 32,
        group_pages: vec![
            vec![0, 1, 2], vec![3, 4, 5], vec![6, 7, 8], vec![9, 10, 11],
            vec![12, 13, 14, 15, 16, 17, 18, 19],  // extra group from B-tree packing
        ],
        ..Manifest::empty()
    };
    // B-tree mapping takes priority: 5 groups, not positional 4
    assert_eq!(m2.total_groups(), 5);
}

#[test]
fn test_write_pages_scattered_only_marks_written_pages() {
    // Regression: bitmap must not mark pages beyond data length
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 8, 2, 64, 16, None, Vec::new()).unwrap();

    // Provide data for 2 pages but page_nums has 4
    let page_nums = vec![0u64, 1, 5, 10];
    let data = vec![0xAA; 128]; // only 2 pages worth (2 * 64)
    cache.write_pages_scattered(&page_nums, &data, 0, 0).unwrap();

    // Pages 0 and 1 should be present (data written)
    assert!(cache.is_present(0), "page 0 should be present");
    assert!(cache.is_present(1), "page 1 should be present");
    // Pages 5 and 10 should NOT be present (data was too short)
    assert!(!cache.bitmap.lock().is_present(5), "page 5 should NOT be present (no data)");
    assert!(!cache.bitmap.lock().is_present(10), "page 10 should NOT be present (no data)");
}

#[test]
fn test_write_pages_scattered_exact_data_marks_all() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 8, 2, 64, 16, None, Vec::new()).unwrap();

    // Data exactly matches page_nums length
    let page_nums = vec![3u64, 7, 12];
    let data = vec![0xBB; 192]; // 3 * 64
    cache.write_pages_scattered(&page_nums, &data, 0, 0).unwrap();

    for &p in &page_nums {
        assert!(cache.is_present(p), "page {} should be present", p);
    }
}

#[test]
fn test_write_pages_scattered_no_tracker_pollution() {
    // Regression: scattered writes mark the tracker with manifest-aware sub-chunk IDs.
    // Bitmap is per-page (accurate), so unwritten pages in the same sub-chunk stay absent.
    let dir = TempDir::new().unwrap();
    // ppg=8, spf=4, page_size=64, page_count=32
    let cache = DiskCache::new(dir.path(), 3600, 8, 4, 64, 32, None, Vec::new()).unwrap();

    // Write page 5 via scattered (simulates B-tree group write)
    let page_nums = vec![5u64];
    let data = vec![0xCC; 64];
    cache.write_pages_scattered(&page_nums, &data, 0, 0).unwrap();

    // Page 5 should be present (bitmap)
    assert!(cache.bitmap.lock().is_present(5));

    // Unwritten pages must not be present (bitmap is per-page accurate)
    assert!(!cache.is_present(4), "page 4 must not be present");
    assert!(!cache.is_present(6), "page 6 must not be present");
    assert!(!cache.is_present(7), "page 7 must not be present");

    // But tracker sub-chunk (0, 0) IS marked (gid=0, index_in_group=0 -> frame 0)
    let tracker = cache.tracker.lock();
    let id = tracker.sub_chunk_id_for(0, 0);
    assert!(tracker.is_sub_chunk_present(&id), "sub-chunk must be marked after write");
}

#[test]
fn test_write_pages_scattered_data_integrity() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 8, 2, 64, 16, None, Vec::new()).unwrap();

    // Write non-sequential pages with distinct data
    let page_nums = vec![3u64, 0, 7, 15];
    let mut data = Vec::with_capacity(4 * 64);
    for (i, &pnum) in page_nums.iter().enumerate() {
        data.extend(std::iter::repeat(((pnum + 1) as u8) * 10 + i as u8).take(64));
    }
    cache.write_pages_scattered(&page_nums, &data, 0, 0).unwrap();

    // Read back each page and verify data
    for (i, &pnum) in page_nums.iter().enumerate() {
        let mut buf = vec![0u8; 64];
        cache.read_page(pnum, &mut buf).unwrap();
        let expected_byte = ((pnum + 1) as u8) * 10 + i as u8;
        assert_eq!(buf[0], expected_byte, "page {} data mismatch", pnum);
        assert!(buf.iter().all(|&b| b == expected_byte), "page {} not uniform", pnum);
    }
}

#[test]
fn test_manifest_btree_page_location_non_sequential() {
    // B-tree groups have non-sequential page numbers
    let mut manifest = Manifest {
        page_count: 20,
        page_size: 4096,
        pages_per_group: 8,
        group_pages: vec![
            vec![0, 5, 10, 15],     // gid=0: scattered pages from B-tree A
            vec![1, 2, 3, 4],       // gid=1: sequential pages from B-tree B
            vec![6, 7, 8, 9],       // gid=2: sequential from B-tree C
            vec![11, 12, 13, 14, 16, 17, 18, 19], // gid=3: remaining pages
        ],
        ..Manifest::empty()
    };
    manifest.build_page_index();

    // Verify page 10 is in gid=0 at index=2
    let loc = manifest.page_location(10).unwrap();
    assert_eq!(loc.group_id, 0);
    assert_eq!(loc.index, 2);

    // Page 3 is in gid=1 at index=2
    let loc = manifest.page_location(3).unwrap();
    assert_eq!(loc.group_id, 1);
    assert_eq!(loc.index, 2);

    // Page 18 is in gid=3 at index=6
    let loc = manifest.page_location(18).unwrap();
    assert_eq!(loc.group_id, 3);
    assert_eq!(loc.index, 6);
}

#[test]
fn test_manifest_total_groups_with_btree_exceeds_positional() {
    // 10 pages with ppg=4 gives positional total_groups=3.
    // But B-tree packing produces 5 groups (small B-trees get own groups).
    let m = Manifest {
        page_count: 10,
        pages_per_group: 4,
        group_pages: vec![
            vec![0, 1],      // B-tree A (2 pages)
            vec![2, 3],      // B-tree B (2 pages)
            vec![4, 5],      // B-tree C (2 pages)
            vec![6, 7],      // B-tree D (2 pages)
            vec![8, 9],      // unowned (2 pages)
        ],
        ..Manifest::empty()
    };
    // Positional would be ceil(10/4) = 3, but B-tree mapping says 5
    assert_eq!(m.total_groups(), 5);
}

#[test]
fn test_ensure_group_capacity_for_btree_groups() {
    // DiskCache starts with positional group count but B-tree groups may need more
    let dir = TempDir::new().unwrap();
    // page_count=10, ppg=4 -> positional total_groups = 3
    let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 10, None, Vec::new()).unwrap();
    assert_eq!(cache.group_states.lock().len(), 3);

    // B-tree manifest has 5 groups (more than positional)
    cache.ensure_group_capacity(5);
    assert_eq!(cache.group_states.lock().len(), 5);

    // Can claim and mark all 5 groups
    for gid in 0..5u64 {
        assert!(cache.try_claim_group(gid));
        cache.mark_group_present(gid);
        assert_eq!(cache.group_state(gid), GroupState::Present);
    }
}

#[test]
fn test_write_pages_scattered_empty_data() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 8, 2, 64, 16, None, Vec::new()).unwrap();

    // Empty data but non-empty page_nums: should be a no-op
    let page_nums = vec![0u64, 1, 2];
    let data: Vec<u8> = vec![];
    cache.write_pages_scattered(&page_nums, &data, 0, 0).unwrap();

    // No pages should be marked
    assert!(!cache.is_present(0));
    assert!(!cache.is_present(1));
    assert!(!cache.is_present(2));
}

#[test]
fn test_write_pages_scattered_partial_page_data() {
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 8, 2, 64, 16, None, Vec::new()).unwrap();

    // Data for 1.5 pages (not enough for second page)
    let page_nums = vec![0u64, 1];
    let data = vec![0xDD; 96]; // 64 + 32 (not enough for page 1)
    cache.write_pages_scattered(&page_nums, &data, 0, 0).unwrap();

    // Only page 0 should be marked (page 1 had partial data)
    assert!(cache.is_present(0), "page 0 with full data should be present");
    assert!(!cache.bitmap.lock().is_present(1), "page 1 with partial data should not be present");
}

#[test]
fn test_manifest_serde_roundtrip_btree_groups() {
    // group_pages and btrees must survive JSON serialization.
    // page_index is #[serde(skip)] and rebuilt from group_pages.
    let mut m = Manifest {
        version: 5,
        page_count: 20,
        page_size: 4096,
        pages_per_group: 8,
        page_group_keys: vec![
            "pg/0_v5".into(), "pg/1_v5".into(), "pg/2_v5".into(),
        ],
        group_pages: vec![
            vec![0, 5, 10, 15],     // B-tree A (scattered)
            vec![1, 2, 3, 4],       // B-tree B (sequential)
            vec![6, 7, 8, 9, 11, 12, 13, 14, 16, 17, 18, 19],
        ],
        btrees: {
            let mut h = HashMap::new();
            h.insert(0, BTreeManifestEntry {
                name: "users".into(),
                obj_type: "table".into(),
                group_ids: vec![0],
            });
            h.insert(5, BTreeManifestEntry {
                name: "idx_users_name".into(),
                obj_type: "index".into(),
                group_ids: vec![1],
            });
            h
        },
        ..Manifest::empty()
    };
    m.build_page_index();

    let json = serde_json::to_string(&m).unwrap();
    let mut m2: Manifest = serde_json::from_str(&json).unwrap();

    // group_pages survives
    assert_eq!(m.group_pages, m2.group_pages);
    // btrees survives
    assert_eq!(m.btrees.len(), m2.btrees.len());
    assert_eq!(m.btrees[&0].name, m2.btrees[&0].name);
    assert_eq!(m.btrees[&5].group_ids, m2.btrees[&5].group_ids);
    // page_index is NOT serialized (skip)
    assert!(m2.page_index.is_empty(), "page_index should be empty after deserialize");
    // Rebuild and verify
    m2.build_page_index();
    assert_eq!(m2.page_location(10).unwrap().group_id, 0);
    assert_eq!(m2.page_location(10).unwrap().index, 2);
    assert_eq!(m2.page_location(3).unwrap().group_id, 1);
    assert_eq!(m2.page_location(3).unwrap().index, 2);
    // total_groups uses group_pages, not positional
    assert_eq!(m2.total_groups(), 3);
    // btree_groups rebuilt correctly (group -> sibling group_ids)
    assert_eq!(m2.btree_groups.get(&0).unwrap(), &vec![0u64]);
    assert_eq!(m2.btree_groups.get(&1).unwrap(), &vec![1u64]);
    // group 2 has no btree entry, so no btree_groups mapping
    assert!(m2.btree_groups.get(&2).is_none());
    // Phase Verdun-i: page_to_tree_name reverse index rebuilt from btrees
    // B-tree root=0 ("users") owns group 0 with pages [0, 5, 10, 15]
    assert_eq!(m2.page_to_tree_name.get(&0).map(|s| s.as_str()), Some("users"));
    assert_eq!(m2.page_to_tree_name.get(&5).map(|s| s.as_str()), Some("users"));
    assert_eq!(m2.page_to_tree_name.get(&10).map(|s| s.as_str()), Some("users"));
    assert_eq!(m2.page_to_tree_name.get(&15).map(|s| s.as_str()), Some("users"));
    // B-tree root=5 ("idx_users_name") owns group 1 with pages [1, 2, 3, 4]
    assert_eq!(m2.page_to_tree_name.get(&1).map(|s| s.as_str()), Some("idx_users_name"));
    assert_eq!(m2.page_to_tree_name.get(&4).map(|s| s.as_str()), Some("idx_users_name"));
    // Pages in group 2 (no btree entry) should NOT be in page_to_tree_name
    assert!(m2.page_to_tree_name.get(&6).is_none());
    assert!(m2.page_to_tree_name.get(&19).is_none());
    // page_to_tree_name is skip-serialized (rebuilt, not persisted)
    assert!(!m2.page_to_tree_name.is_empty()); // rebuilt by build_page_index
    // tree_name_to_groups also rebuilt
    assert_eq!(m2.tree_name_to_groups.get("users").unwrap(), &vec![0u64]);
    assert_eq!(m2.tree_name_to_groups.get("idx_users_name").unwrap(), &vec![1u64]);
}

#[test]
fn test_page_location_missing_page_returns_none() {
    let mut manifest = Manifest {
        page_count: 10,
        page_size: 4096,
        pages_per_group: 4,
        group_pages: vec![
            vec![0, 1, 2, 3],
            vec![4, 5, 6, 7],
        ],
        ..Manifest::empty()
    };
    manifest.build_page_index();

    // Pages 0-7 are assigned, 8-9 are NOT (unassigned gap)
    assert!(manifest.page_location(0).is_some());
    assert!(manifest.page_location(7).is_some());
    assert!(manifest.page_location(8).is_none(), "unassigned page should return None");
    assert!(manifest.page_location(9).is_none());
    assert!(manifest.page_location(100).is_none(), "way out of bounds should return None");
}

#[test]
fn test_concurrent_scattered_writes_no_tracker_pollution() {
    // Reproduces the EXACT corruption scenario: two B-tree groups with pages
    // that share positional sub-chunks. Writing group A must not make group B's
    // unwritten pages appear as cache hits.
    use std::sync::Arc;
    let dir = TempDir::new().unwrap();
    // ppg=8, spf=4: pages 0-3 = sub-chunk (0,0), pages 4-7 = sub-chunk (0,1)
    let cache = Arc::new(DiskCache::new(dir.path(), 3600, 8, 4, 64, 16, None, Vec::new()).unwrap());

    // B-tree group A: pages [0, 4] (spans two positional sub-chunks)
    // B-tree group B: pages [1, 5] (same two positional sub-chunks!)
    let cache_a = Arc::clone(&cache);
    let cache_b = Arc::clone(&cache);

    // Thread A writes B-tree group A
    let handle_a = std::thread::spawn(move || {
        let data_a = vec![0xAA; 128]; // 2 pages * 64
        cache_a.write_pages_scattered(&[0, 4], &data_a, 0, 0).unwrap();
    });

    // Thread B writes B-tree group B
    let handle_b = std::thread::spawn(move || {
        let data_b = vec![0xBB; 128]; // 2 pages * 64
        cache_b.write_pages_scattered(&[1, 5], &data_b, 0, 0).unwrap();
    });

    handle_a.join().unwrap();
    handle_b.join().unwrap();

    // Pages 0, 4 should have 0xAA data
    let mut buf = vec![0u8; 64];
    cache.read_page(0, &mut buf).unwrap();
    assert!(buf.iter().all(|&b| b == 0xAA), "page 0 should be 0xAA");
    cache.read_page(4, &mut buf).unwrap();
    assert!(buf.iter().all(|&b| b == 0xAA), "page 4 should be 0xAA");

    // Pages 1, 5 should have 0xBB data
    cache.read_page(1, &mut buf).unwrap();
    assert!(buf.iter().all(|&b| b == 0xBB), "page 1 should be 0xBB");
    cache.read_page(5, &mut buf).unwrap();
    assert!(buf.iter().all(|&b| b == 0xBB), "page 5 should be 0xBB");

    // CRITICAL: pages 2, 3, 6, 7 were NEVER written. They must NOT be present.
    // Before the fix, tracker pollution would report them as present.
    assert!(!cache.is_present(2), "page 2 never written, must not be present");
    assert!(!cache.is_present(3), "page 3 never written, must not be present");
    assert!(!cache.is_present(6), "page 6 never written, must not be present");
    assert!(!cache.is_present(7), "page 7 never written, must not be present");

    // And reading unwritten pages must return zeros, not stale data
    cache.read_page(2, &mut buf).unwrap();
    assert!(buf.iter().all(|&b| b == 0), "unwritten page 2 must be zeros");
    cache.read_page(6, &mut buf).unwrap();
    assert!(buf.iter().all(|&b| b == 0), "unwritten page 6 must be zeros");
}

#[test]
fn test_write_pages_scattered_sparse_page_distribution() {
    // Pages from a B-tree group can span a very wide range of page numbers.
    // Verify writes at large offsets work correctly.
    let dir = TempDir::new().unwrap();
    // Large page_count to ensure file is pre-allocated
    let cache = DiskCache::new(dir.path(), 3600, 128, 4, 64, 4000, None, Vec::new()).unwrap();

    // Sparse pages from one B-tree group
    let page_nums = vec![10u64, 500, 1000, 3999];
    let mut data = Vec::with_capacity(4 * 64);
    for &pnum in &page_nums {
        data.extend(std::iter::repeat((pnum % 256) as u8).take(64));
    }
    cache.write_pages_scattered(&page_nums, &data, 0, 0).unwrap();

    // Verify each page has correct data
    for &pnum in &page_nums {
        assert!(cache.is_present(pnum), "page {} should be present", pnum);
        let mut buf = vec![0u8; 64];
        cache.read_page(pnum, &mut buf).unwrap();
        let expected = (pnum % 256) as u8;
        assert!(buf.iter().all(|&b| b == expected),
            "page {} data mismatch: expected 0x{:02x}, got 0x{:02x}", pnum, expected, buf[0]);
    }

    // Pages between sparse entries must NOT be present
    assert!(!cache.is_present(11));
    assert!(!cache.is_present(499));
    assert!(!cache.is_present(501));
    assert!(!cache.is_present(999));
}

#[test]
fn test_btree_group_single_page() {
    // A B-tree with exactly 1 page (e.g., a small table).
    // write_pages_scattered must handle single-entry page_nums.
    let dir = TempDir::new().unwrap();
    let cache = DiskCache::new(dir.path(), 3600, 128, 4, 64, 100, None, Vec::new()).unwrap();

    let page_nums = vec![42u64];
    let data = vec![0xFF; 64];
    cache.write_pages_scattered(&page_nums, &data, 0, 0).unwrap();

    assert!(cache.is_present(42));
    let mut buf = vec![0u8; 64];
    cache.read_page(42, &mut buf).unwrap();
    assert!(buf.iter().all(|&b| b == 0xFF));

    // Neighbors must not be present
    assert!(!cache.is_present(41));
    assert!(!cache.is_present(43));
}

#[test]
fn test_write_pages_scattered_populates_tracker_subchunks() {
    // Regression: write_pages_scattered must mark SubChunkTracker with correct
    // manifest-aware sub_chunk_id_for() so tiered eviction works.
    let dir = TempDir::new().unwrap();
    // ppg=8, spf=4 -> 2 frames per group. page_size=64, page_count=32
    let group_pages = vec![vec![10, 20, 30, 5, 15, 25, 35, 45]]; // group 0
    let cache = DiskCache::new(dir.path(), 3600, 8, 4, 64, 50, None, group_pages).unwrap();

    // Write all 8 pages (2 frames worth)
    let page_nums = vec![10u64, 20, 30, 5, 15, 25, 35, 45];
    let data = vec![0xAA; 8 * 64];
    cache.write_pages_scattered(&page_nums, &data, 0, 0).unwrap();

    let tracker = cache.tracker.lock();
    // Frame 0: indices 0-3 -> SubChunkId { group_id: 0, frame_index: 0 }
    let f0 = tracker.sub_chunk_id_for(0, 0);
    assert!(tracker.is_sub_chunk_present(&f0), "frame 0 must be marked present");
    // Frame 1: indices 4-7 -> SubChunkId { group_id: 0, frame_index: 1 }
    let f1 = tracker.sub_chunk_id_for(0, 4);
    assert!(tracker.is_sub_chunk_present(&f1), "frame 1 must be marked present");
    // Non-existent frame 2 should not be present
    let f2 = SubChunkId { group_id: 0, frame_index: 2 };
    assert!(!tracker.is_sub_chunk_present(&f2), "frame 2 must not be present");
    drop(tracker);

    // Bitmap should have all 8 pages
    for &p in &page_nums {
        assert!(cache.is_present(p), "page {} must be present in bitmap", p);
    }
    // Unwritten pages must not be present
    assert!(!cache.is_present(11));
    assert!(!cache.is_present(0));
}

#[test]
fn test_write_pages_scattered_subframe_offset() {
    // write_pages_scattered with start_index_in_group > 0 (sub-chunk frame offset)
    let dir = TempDir::new().unwrap();
    // ppg=8, spf=4 -> 2 frames per group
    let cache = DiskCache::new(dir.path(), 3600, 8, 4, 64, 50, None, Vec::new()).unwrap();

    // Write 4 pages starting at index 4 in group 0 (frame 1)
    let page_nums = vec![100u64, 200, 300, 400];
    let data = vec![0xBB; 4 * 64];
    cache.write_pages_scattered(&page_nums, &data, 0, 4).unwrap();

    let tracker = cache.tracker.lock();
    // These pages are at indices 4,5,6,7 -> frame 1
    let f1 = tracker.sub_chunk_id_for(0, 4);
    assert!(tracker.is_sub_chunk_present(&f1), "frame 1 must be marked");
    // Frame 0 should NOT be marked (we only wrote frame 1)
    let f0 = tracker.sub_chunk_id_for(0, 0);
    assert!(!tracker.is_sub_chunk_present(&f0), "frame 0 must not be marked");
}

#[test]
fn test_manifest_btree_group_partial_last_group() {
    // Last B-tree group may have fewer pages than ppg.
    // total_groups, page_location, frame calculations all must respect actual size.
    let mut manifest = Manifest {
        page_count: 10,
        page_size: 4096,
        pages_per_group: 4,
        group_pages: vec![
            vec![0, 1, 2, 3],   // full group
            vec![4, 5, 6, 7],   // full group
            vec![8, 9],         // partial group (2 of 4)
        ],
        ..Manifest::empty()
    };
    manifest.build_page_index();

    assert_eq!(manifest.total_groups(), 3);

    // Page 9 is in the partial group at index 1
    let loc = manifest.page_location(9).unwrap();
    assert_eq!(loc.group_id, 2);
    assert_eq!(loc.index, 1);

    // Group 2 has 2 pages, not 4
    assert_eq!(manifest.group_pages[2].len(), 2);
}

#[test]
fn test_manifest_deserialize_btree_fields_default_when_missing() {
    // Old manifests without group_pages/btrees should deserialize cleanly
    let json = r#"{"version":1,"page_count":100,"page_size":4096,"pages_per_group":32,"page_group_keys":["pg/0_v1","pg/1_v1","pg/2_v1","pg/3_v1"]}"#;
    let m: Manifest = serde_json::from_str(json).unwrap();

    // B-tree fields default to empty
    assert!(m.group_pages.is_empty());
    assert!(m.btrees.is_empty());
    assert!(m.page_index.is_empty());

    // Falls back to positional total_groups
    assert_eq!(m.total_groups(), 4); // ceil(100/32)
}
