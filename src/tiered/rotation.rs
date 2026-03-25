//! Encryption key rotation: re-encrypt all S3 data with a new key.

use std::io;

use super::*;
use crate::compress;

/// Re-encrypt, encrypt, or decrypt all S3 data.
///
/// Three modes based on `config.encryption_key` (old) and `new_key`:
/// - `Some(old), Some(new)`: key rotation (decrypt with old, re-encrypt with new)
/// - `Some(old), None`: remove encryption (decrypt, store plaintext compressed data)
/// - `None, Some(new)`: add encryption (encrypt previously plaintext data)
///
/// Downloads each S3 object, transforms it, and uploads as a new versioned object.
/// The manifest upload is the atomic commit point. Old objects are GC'd after.
/// Local cache is cleared (ephemeral, repopulates on next open).
///
/// This does NOT decompress/recompress, only changes the encryption layer.
/// Frame table offsets are recalculated (GCM adds/removes 28 bytes/frame).
///
/// **Offline operation**: close all connections before rotating. Open connections
/// hold an in-memory manifest pointing to old S3 keys; after GC deletes those
/// keys, uncached reads from stale connections will fail with NotFound.
#[cfg(feature = "encryption")]
pub fn rotate_encryption_key(
    config: &TieredConfig,
    new_key: Option<[u8; 32]>,
) -> io::Result<()> {
    let old_key = config.encryption_key;

    if old_key.is_none() && new_key.is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "rotate_encryption_key: both old and new keys are None (nothing to do)",
        ));
    }

    if old_key == new_key {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "rotate_encryption_key: old and new keys are identical (nothing to do)",
        ));
    }

    // Helpers: decrypt if old_key present, encrypt if new_key present.
    // When key is None, data passes through unchanged.
    let maybe_decrypt = |data: &[u8]| -> io::Result<Vec<u8>> {
        match old_key.as_ref() {
            Some(k) => compress::decrypt_gcm_random_nonce(data, k),
            None => Ok(data.to_vec()),
        }
    };
    let maybe_encrypt = |data: &[u8]| -> io::Result<Vec<u8>> {
        match new_key.as_ref() {
            Some(k) => compress::encrypt_gcm_random_nonce(data, k),
            None => Ok(data.to_vec()),
        }
    };

    let runtime = tokio::runtime::Runtime::new()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let handle = runtime.handle().clone();

    let s3_cfg = TieredConfig {
        bucket: config.bucket.clone(),
        prefix: config.prefix.clone(),
        endpoint_url: config.endpoint_url.clone(),
        region: config.region.clone(),
        runtime_handle: Some(handle.clone()),
        ..Default::default()
    };
    let s3 = S3Client::block_on(&handle, S3Client::new_async(&s3_cfg))?;

    // Fetch manifest
    let manifest = s3.get_manifest()?.ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, "No manifest found in S3")
    })?;

    let mut new_manifest = manifest.clone();
    new_manifest.version += 1;
    let new_version = new_manifest.version;
    let mut replaced_keys: Vec<String> = Vec::new();
    let mut validated_old_key = old_key.is_none(); // skip validation when no old key

    let pg_count = manifest
        .page_group_keys
        .iter()
        .filter(|k| !k.is_empty())
        .count();
    let mode = match (&old_key, &new_key) {
        (Some(_), Some(_)) => "key rotation",
        (Some(_), None) => "removing encryption",
        (None, Some(_)) => "adding encryption",
        (None, None) => unreachable!(),
    };
    eprintln!(
        "[rotate] starting {}: {} page groups, {} interior chunks, {} index chunks",
        mode,
        pg_count,
        manifest.interior_chunk_keys.len(),
        manifest.index_chunk_keys.len(),
    );

    // Re-encrypt page groups
    for (gid, old_s3_key) in manifest.page_group_keys.iter().enumerate() {
        if old_s3_key.is_empty() {
            continue;
        }

        let blob = s3.get_page_group(old_s3_key)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Page group {} not found", old_s3_key),
            )
        })?;

        // Validate old key on first page group (fail fast before uploading anything)
        if !validated_old_key {
            let has_ft =
                gid < manifest.frame_tables.len() && !manifest.frame_tables[gid].is_empty();
            let test_data = if has_ft {
                let frame = &manifest.frame_tables[gid][0];
                &blob[frame.offset as usize..(frame.offset as usize + frame.len as usize)]
            } else {
                &blob[..]
            };
            // old_key is Some here (validated_old_key starts true when old_key is None)
            compress::decrypt_gcm_random_nonce(test_data, old_key.as_ref().expect("old_key must be Some for validation")).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Old encryption key failed to decrypt existing data. Wrong key?",
                )
            })?;
            validated_old_key = true;
        }

        let has_frames =
            gid < manifest.frame_tables.len() && !manifest.frame_tables[gid].is_empty();

        if has_frames {
            // Seekable: per-frame decrypt + re-encrypt
            let frames = &manifest.frame_tables[gid];
            let mut new_blob = Vec::with_capacity(blob.len());
            let mut new_frames = Vec::with_capacity(frames.len());

            for frame in frames {
                let end = frame.offset as usize + frame.len as usize;
                let frame_data = &blob[frame.offset as usize..end];
                let compressed = maybe_decrypt(frame_data)?;
                let output = maybe_encrypt(&compressed)?;

                new_frames.push(FrameEntry {
                    offset: new_blob.len() as u64,
                    len: output.len() as u32,
                });
                new_blob.extend_from_slice(&output);
            }

            let new_s3_key = s3.page_group_key(gid as u64, new_version);
            s3.put_page_groups(&[(new_s3_key.clone(), new_blob)])?;

            replaced_keys.push(old_s3_key.clone());
            new_manifest.page_group_keys[gid] = new_s3_key;
            new_manifest.frame_tables[gid] = new_frames;
        } else {
            // Non-seekable: whole-blob decrypt + re-encrypt
            let compressed = maybe_decrypt(&blob)?;
            let output = maybe_encrypt(&compressed)?;

            let new_s3_key = s3.page_group_key(gid as u64, new_version);
            s3.put_page_groups(&[(new_s3_key.clone(), output)])?;

            replaced_keys.push(old_s3_key.clone());
            new_manifest.page_group_keys[gid] = new_s3_key;
        }
    }

    eprintln!(
        "[rotate] processed {} page groups",
        manifest
            .page_group_keys
            .iter()
            .filter(|k| !k.is_empty())
            .count()
    );

    // Re-encrypt interior bundles
    let interior_keys: Vec<(u32, String)> = manifest
        .interior_chunk_keys
        .iter()
        .map(|(&id, k)| (id, k.clone()))
        .collect();
    for (chunk_id, old_s3_key) in &interior_keys {
        let blob = s3.get_page_group(old_s3_key)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Interior chunk {} not found", old_s3_key),
            )
        })?;
        let compressed = maybe_decrypt(&blob)?;
        let output = maybe_encrypt(&compressed)?;

        let new_s3_key = s3.interior_chunk_key(*chunk_id, new_version);
        s3.put_page_groups(&[(new_s3_key.clone(), output)])?;

        replaced_keys.push(old_s3_key.clone());
        new_manifest
            .interior_chunk_keys
            .insert(*chunk_id, new_s3_key);
    }

    eprintln!("[rotate] processed {} interior chunks", interior_keys.len());

    // Re-encrypt index bundles
    let index_keys: Vec<(u32, String)> = manifest
        .index_chunk_keys
        .iter()
        .map(|(&id, k)| (id, k.clone()))
        .collect();
    for (chunk_id, old_s3_key) in &index_keys {
        let blob = s3.get_page_group(old_s3_key)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Index chunk {} not found", old_s3_key),
            )
        })?;
        let compressed = maybe_decrypt(&blob)?;
        let output = maybe_encrypt(&compressed)?;

        let new_s3_key = s3.index_chunk_key(*chunk_id, new_version);
        s3.put_page_groups(&[(new_s3_key.clone(), output)])?;

        replaced_keys.push(old_s3_key.clone());
        new_manifest
            .index_chunk_keys
            .insert(*chunk_id, new_s3_key);
    }

    eprintln!("[rotate] processed {} index chunks", index_keys.len());

    // VERIFY: re-download and decode one new page group before committing.
    // Guards against silent S3 corruption or encode bugs.
    if let Some(verify_key) = new_manifest.page_group_keys.iter().find(|k| !k.is_empty()) {
        let verify_blob = s3.get_page_group(verify_key)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Other,
                "verification failed: newly uploaded page group not found in S3",
            )
        })?;
        // Find the gid for this key to check frame table
        let verify_gid = new_manifest
            .page_group_keys
            .iter()
            .position(|k| k == verify_key)
            .expect("key must be in manifest");
        let has_ft = verify_gid < new_manifest.frame_tables.len()
            && !new_manifest.frame_tables[verify_gid].is_empty();
        if has_ft {
            // Verify first frame can be decrypted and decompressed
            let frame = &new_manifest.frame_tables[verify_gid][0];
            let end = frame.offset as usize + frame.len as usize;
            if end > verify_blob.len() {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "verification failed: frame extends beyond blob",
                ));
            }
            let frame_data = &verify_blob[frame.offset as usize..end];
            let decrypted = decrypt_if_needed(frame_data, new_key.as_ref())?;
            compress::decompress(
                &decrypted,
                #[cfg(feature = "zstd")]
                None,
                #[cfg(not(feature = "zstd"))]
                None,
            )
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("verification failed: cannot decompress new data: {}", e),
                )
            })?;
        } else {
            // Non-seekable: verify whole blob
            let decrypted = decrypt_if_needed(&verify_blob, new_key.as_ref())?;
            compress::decompress(
                &decrypted,
                #[cfg(feature = "zstd")]
                None,
                #[cfg(not(feature = "zstd"))]
                None,
            )
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("verification failed: cannot decompress new data: {}", e),
                )
            })?;
        }
        eprintln!("[rotate] verification passed: new data is readable");
    }

    // COMMIT POINT: upload new manifest
    s3.put_manifest(&new_manifest)?;
    eprintln!("[rotate] manifest uploaded (version {})", new_version);

    // GC old objects
    if !replaced_keys.is_empty() {
        s3.delete_objects(&replaced_keys)?;
        eprintln!("[rotate] deleted {} old S3 objects", replaced_keys.len());
    }

    // Clear local cache (simpler than re-encrypting, cache repopulates on next open)
    let _ = std::fs::remove_file(config.cache_dir.join("data.cache"));
    let _ = std::fs::remove_file(config.cache_dir.join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(config.cache_dir.join("page_bitmap"));
    eprintln!("[rotate] cleared local cache");

    eprintln!("[rotate] {} complete", mode);
    Ok(())
}
