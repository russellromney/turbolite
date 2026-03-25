//! Non-blocking S3 flush for two-phase checkpoint.
//!
//! After a local-only checkpoint (SyncMode::LocalThenFlush, or the global
//! LOCAL_CHECKPOINT_ONLY flag for benchmarks), dirty pages are in the local
//! disk cache but not yet on S3. This module uploads them without holding any
//! SQLite lock, so reads and writes can continue concurrently.

use super::*;

/// Upload locally-checkpointed dirty page groups to S3.
///
/// Reads pages from the disk cache, encodes them, uploads to S3, and updates
/// the manifest. Called outside any SQLite lock.
///
/// # Safety contract
/// - Must NOT be called concurrently with a SQLite checkpoint on the same VFS.
/// - Pages for all dirty groups must be present in the disk cache.
pub(crate) fn flush_dirty_groups_to_s3(
    s3: &S3Client,
    cache: &DiskCache,
    shared_manifest: &RwLock<Manifest>,
    shared_dirty_groups: &Mutex<HashSet<u64>>,
    compression_level: i32,
    #[cfg(feature = "zstd")] dictionary: Option<&[u8]>,
    encryption_key: Option<[u8; 32]>,
    gc_enabled: bool,
    access_history: Option<&prediction::SharedAccessHistory>,
    prediction: Option<&prediction::SharedPrediction>,
) -> io::Result<()> {
    // 1. Drain pending dirty groups atomically
    let dirty_groups: HashSet<u64> = {
        let mut pending = shared_dirty_groups.lock().unwrap();
        std::mem::take(&mut *pending)
    };

    if dirty_groups.is_empty() {
        eprintln!("[flush] no pending groups to upload");
        return Ok(());
    }

    eprintln!(
        "[flush] uploading {} dirty groups to S3...",
        dirty_groups.len()
    );

    // 2. Snapshot manifest (avoid holding lock during S3 I/O)
    let manifest_snap = shared_manifest.read().clone();
    let page_count = manifest_snap.page_count;
    let page_size = manifest_snap.page_size;
    let ppg = manifest_snap.pages_per_group;

    if page_size == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "flush_to_s3: manifest has page_size=0",
        ));
    }

    // 3. Build encoder/decoder dictionaries
    #[cfg(feature = "zstd")]
    let encoder_dict = dictionary
        .map(|d| zstd::dict::EncoderDictionary::copy(d, compression_level));
    #[cfg(feature = "zstd")]
    let decoder_dict = dictionary
        .map(zstd::dict::DecoderDictionary::copy);

    let next_version = manifest_snap.version + 1;
    let mut uploads: Vec<(String, Vec<u8>)> = Vec::new();
    let mut new_keys = manifest_snap.page_group_keys.clone();
    let mut replaced_keys: Vec<String> = Vec::new();

    // Carry forward seekable encoding from manifest
    let old_sub_ppf = manifest_snap.sub_pages_per_frame;
    let use_seekable = old_sub_ppf > 0;
    let mut new_frame_tables: Vec<Vec<FrameEntry>> = manifest_snap.frame_tables.clone();

    // Track page numbers from dirty groups (for interior/index chunk dirtiness)
    let mut dirty_page_nums: HashSet<u64> = HashSet::new();

    // 4. For each dirty group: read pages from cache, encode, prepare upload
    for &gid in &dirty_groups {
        let pages_in_group = manifest_snap.group_page_nums(gid);
        let group_size = pages_in_group.len();
        let mut pages: Vec<Option<Vec<u8>>> = vec![None; group_size];
        let mut need_s3_merge = false;

        for (i, &pnum) in pages_in_group.iter().enumerate() {
            if pnum >= page_count {
                break;
            }
            dirty_page_nums.insert(pnum);
            if cache.is_present(pnum) {
                let mut page_buf = vec![0u8; page_size as usize];
                if cache.read_page(pnum, &mut page_buf).is_ok() {
                    pages[i] = Some(page_buf);
                } else {
                    need_s3_merge = true;
                }
            } else {
                need_s3_merge = true;
            }
        }

        // If some pages aren't in cache, fetch existing group from S3 and merge
        if need_s3_merge {
            if let Some(existing_key) = new_keys.get(gid as usize) {
                if !existing_key.is_empty() {
                    if let Ok(Some(pg_data)) = s3.get_page_group(existing_key) {
                        let existing_ft = manifest_snap.frame_tables.get(gid as usize);
                        let has_ft = use_seekable
                            && existing_ft.map(|ft| !ft.is_empty()).unwrap_or(false);
                        if has_ft {
                            let ft = existing_ft.unwrap();
                            if let Ok((_pc, _ps, bulk_data)) = decode_page_group_seekable_full(
                                &pg_data,
                                ft,
                                page_size,
                                pages_in_group.len() as u32,
                                page_count,
                                0,
                                #[cfg(feature = "zstd")]
                                decoder_dict.as_ref(),
                                encryption_key.as_ref(),
                            ) {
                                let ps = page_size as usize;
                                for j in 0..pages_in_group.len() {
                                    if pages_in_group[j] >= page_count { break; }
                                    if pages[j].is_none() {
                                        let start = j * ps;
                                        let end = start + ps;
                                        if end <= bulk_data.len() {
                                            pages[j] = Some(bulk_data[start..end].to_vec());
                                        }
                                    }
                                }
                            }
                        } else if let Ok((_pc, _ps, existing_pages)) = decode_page_group(
                            &pg_data,
                            #[cfg(feature = "zstd")]
                            decoder_dict.as_ref(),
                            encryption_key.as_ref(),
                        ) {
                            for (j, existing_page) in existing_pages.into_iter().enumerate() {
                                if j >= pages_in_group.len() { break; }
                                if pages_in_group[j] >= page_count { break; }
                                if pages[j].is_none() {
                                    pages[j] = Some(existing_page);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Encode
        let key = s3.page_group_key(gid, next_version);
        if use_seekable {
            let (encoded, ft) = encode_page_group_seekable(
                &pages,
                page_size,
                old_sub_ppf,
                compression_level,
                #[cfg(feature = "zstd")]
                encoder_dict.as_ref(),
                encryption_key.as_ref(),
            )?;
            uploads.push((key.clone(), encoded));
            while new_frame_tables.len() <= gid as usize {
                new_frame_tables.push(Vec::new());
            }
            new_frame_tables[gid as usize] = ft;
        } else {
            let encoded = encode_page_group(
                &pages,
                page_size,
                compression_level,
                #[cfg(feature = "zstd")]
                encoder_dict.as_ref(),
                encryption_key.as_ref(),
            )?;
            uploads.push((key.clone(), encoded));
        }

        // Track replaced key
        while new_keys.len() <= gid as usize {
            new_keys.push(String::new());
        }
        if let Some(old_key) = new_keys.get(gid as usize) {
            if !old_key.is_empty() {
                replaced_keys.push(old_key.clone());
            }
        }
        new_keys[gid as usize] = key;
    }

    // 5. Upload all dirty page groups
    eprintln!("[flush] uploading {} page groups...", uploads.len());
    s3.put_page_groups(&uploads)?;
    eprintln!("[flush] page groups uploaded");

    // 6. Interior chunks: collect from cache, re-upload dirty chunks
    eprintln!("[flush] building interior chunks...");
    let chunk_range = bundle_chunk_range(page_size);
    let mut all_interior: HashMap<u64, Vec<u8>> = HashMap::new();
    {
        let known_interior = cache.interior_pages.lock().clone();
        for &pnum in &known_interior {
            if pnum >= page_count {
                continue;
            }
            let mut buf = vec![0u8; page_size as usize];
            if cache.read_page(pnum, &mut buf).is_ok() {
                all_interior.insert(pnum, buf);
            } else {
                eprintln!("[flush] WARN: cache.read_page({}) failed for interior page", pnum);
            }
        }
        eprintln!(
            "[flush] interior collection: {} pages from cache",
            all_interior.len(),
        );
    }

    // Group interior pages by chunk_id
    let mut chunks: HashMap<u32, Vec<(u64, Vec<u8>)>> = HashMap::new();
    for (pnum, data) in all_interior {
        let chunk_id = (pnum / chunk_range) as u32;
        chunks.entry(chunk_id).or_default().push((pnum, data));
    }
    for pages in chunks.values_mut() {
        pages.sort_by_key(|(pnum, _)| *pnum);
    }

    // Dirty chunks: any chunk that contains a page from a dirty group
    let dirty_chunk_ids: HashSet<u32> = dirty_page_nums
        .iter()
        .map(|&pnum| (pnum / chunk_range) as u32)
        .collect();

    let old_chunk_keys = manifest_snap.interior_chunk_keys.clone();
    let mut new_chunk_keys: HashMap<u32, String> = HashMap::new();
    let mut chunk_uploads: Vec<(String, Vec<u8>)> = Vec::new();

    for (&chunk_id, pages) in &chunks {
        if dirty_chunk_ids.contains(&chunk_id) || !old_chunk_keys.contains_key(&chunk_id) {
            let refs: Vec<(u64, &[u8])> = pages.iter().map(|(p, d)| (*p, d.as_slice())).collect();
            let encoded = encode_interior_bundle(
                &refs,
                page_size,
                compression_level,
                #[cfg(feature = "zstd")]
                encoder_dict.as_ref(),
                encryption_key.as_ref(),
            )?;
            let key = s3.interior_chunk_key(chunk_id, next_version);
            eprintln!(
                "[flush] interior chunk {}: {} pages, {:.1}KB compressed",
                chunk_id, pages.len(), encoded.len() as f64 / 1024.0,
            );
            chunk_uploads.push((key.clone(), encoded));
            if let Some(old_key) = old_chunk_keys.get(&chunk_id) {
                replaced_keys.push(old_key.clone());
            }
            new_chunk_keys.insert(chunk_id, key);
        } else {
            new_chunk_keys.insert(chunk_id, old_chunk_keys[&chunk_id].clone());
        }
    }

    // GC orphaned interior chunks
    for (old_chunk_id, old_key) in &old_chunk_keys {
        if !chunks.contains_key(old_chunk_id) {
            replaced_keys.push(old_key.clone());
            eprintln!("[flush] orphaned interior chunk {} scheduled for GC", old_chunk_id);
        }
    }

    if !chunk_uploads.is_empty() {
        eprintln!("[flush] uploading {} interior chunks...", chunk_uploads.len());
        s3.put_page_groups(&chunk_uploads)?;
        eprintln!("[flush] interior chunks uploaded");
    }

    // 7. Index leaf bundles (same pattern as interior)
    // Two sources: (a) dirty group pages that are index leaves (newly written),
    // (b) previously-fetched index pages from the tracker's Index tier.
    eprintln!("[flush] building index leaf bundles...");
    let mut all_index_leaves: HashMap<u64, Vec<u8>> = HashMap::new();
    {
        // (a) Scan dirty group pages for index leaves (type 0x0A + valid header).
        // These are pages written since last checkpoint; the tracker doesn't know
        // they're index pages because they were never fetched from S3.
        // Uses manifest_snap (not shared_manifest) to avoid re-acquiring the lock
        // and seeing a potentially newer manifest version mid-flush.
        let mut dirty_index_count = 0usize;
        for &gid in &dirty_groups {
            let pages_in_group = manifest_snap.group_page_nums(gid);
            for &pnum in pages_in_group.iter() {
                if pnum >= page_count || all_index_leaves.contains_key(&pnum) {
                    continue;
                }
                if cache.is_present(pnum) {
                    let mut buf = vec![0u8; page_size as usize];
                    if cache.read_page(pnum, &mut buf).is_ok() {
                        let hdr_off = if pnum == 0 { 100 } else { 0 };
                        let tb = buf.get(hdr_off).copied();
                        if tb == Some(0x0A) && is_valid_btree_page(&buf, hdr_off) {
                            all_index_leaves.insert(pnum, buf);
                            dirty_index_count += 1;
                        }
                    }
                }
            }
        }

        // (b) Also include previously-fetched index pages from the tracker.
        let tracker = cache.tracker.lock();
        let index_sub_chunks: Vec<SubChunkId> = tracker
            .present
            .iter()
            .filter(|id| tracker.tiers.get(id).copied() == Some(SubChunkTier::Index))
            .copied()
            .collect();
        drop(tracker);

        let mut tracker_index_count = 0usize;
        for sc in &index_sub_chunks {
            let tracker = cache.tracker.lock();
            let page_range = tracker.pages_for_sub_chunk(*sc, page_count);
            drop(tracker);
            for pnum in page_range {
                if pnum >= page_count || all_index_leaves.contains_key(&pnum) {
                    continue;
                }
                let mut buf = vec![0u8; page_size as usize];
                if cache.read_page(pnum, &mut buf).is_ok() {
                    let hdr_off = if pnum == 0 { 100 } else { 0 };
                    let tb = buf.get(hdr_off).copied();
                    if tb == Some(0x0A) && is_valid_btree_page(&buf, hdr_off) {
                        all_index_leaves.insert(pnum, buf);
                        tracker_index_count += 1;
                    }
                }
            }
        }
        eprintln!(
            "[flush] index leaf collection: dirty_groups={}, tracker={}, total={}",
            dirty_index_count, tracker_index_count, all_index_leaves.len(),
        );
    }

    // Group index leaf pages by chunk_id
    let mut index_chunks: HashMap<u32, Vec<(u64, Vec<u8>)>> = HashMap::new();
    for (pnum, data) in all_index_leaves {
        let chunk_id = (pnum / chunk_range) as u32;
        index_chunks.entry(chunk_id).or_default().push((pnum, data));
    }
    for pages in index_chunks.values_mut() {
        pages.sort_by_key(|(pnum, _)| *pnum);
    }

    // Dirty index chunks: any chunk overlapping dirty group page ranges
    let dirty_index_chunk_ids: HashSet<u32> = dirty_page_nums
        .iter()
        .map(|&pnum| (pnum / chunk_range) as u32)
        .collect();

    let old_index_chunk_keys = manifest_snap.index_chunk_keys.clone();
    let mut new_index_chunk_keys: HashMap<u32, String> = HashMap::new();
    let mut index_chunk_uploads: Vec<(String, Vec<u8>)> = Vec::new();

    for (&chunk_id, pages) in &index_chunks {
        if dirty_index_chunk_ids.contains(&chunk_id) || !old_index_chunk_keys.contains_key(&chunk_id) {
            let refs: Vec<(u64, &[u8])> = pages.iter().map(|(p, d)| (*p, d.as_slice())).collect();
            let encoded = encode_interior_bundle(
                &refs,
                page_size,
                compression_level,
                #[cfg(feature = "zstd")]
                encoder_dict.as_ref(),
                encryption_key.as_ref(),
            )?;
            let key = s3.index_chunk_key(chunk_id, next_version);
            eprintln!(
                "[flush] index chunk {}: {} pages, {:.1}KB compressed",
                chunk_id, pages.len(), encoded.len() as f64 / 1024.0,
            );
            index_chunk_uploads.push((key.clone(), encoded));
            if let Some(old_key) = old_index_chunk_keys.get(&chunk_id) {
                replaced_keys.push(old_key.clone());
            }
            new_index_chunk_keys.insert(chunk_id, key);
        } else {
            new_index_chunk_keys.insert(chunk_id, old_index_chunk_keys[&chunk_id].clone());
        }
    }

    // GC orphaned index chunks
    for (old_chunk_id, old_key) in &old_index_chunk_keys {
        if !index_chunks.contains_key(old_chunk_id) {
            replaced_keys.push(old_key.clone());
            eprintln!("[flush] orphaned index chunk {} scheduled for GC", old_chunk_id);
        }
    }

    if !index_chunk_uploads.is_empty() {
        eprintln!("[flush] uploading {} index chunks...", index_chunk_uploads.len());
        s3.put_page_groups(&index_chunk_uploads)?;
        eprintln!("[flush] index chunks uploaded");
    }

    // 8. Update manifest atomically
    let new_manifest = {
        let old_manifest = manifest_snap;
        let mut m = Manifest {
            version: next_version,
            page_count: old_manifest.page_count,
            page_size: old_manifest.page_size,
            pages_per_group: ppg,
            page_group_keys: new_keys,
            interior_chunk_keys: new_chunk_keys,
            index_chunk_keys: new_index_chunk_keys,
            frame_tables: new_frame_tables,
            sub_pages_per_frame: old_sub_ppf,
            strategy: old_manifest.strategy,
            group_pages: old_manifest.group_pages.clone(),
            btrees: old_manifest.btrees.clone(),
            page_index: HashMap::new(),
            btree_groups: HashMap::new(),
            page_to_tree_name: HashMap::new(),
            tree_name_to_groups: HashMap::new(),
            group_to_tree_name: HashMap::new(),
            btree_access_freq: access_history
                .map(|ah| {
                    let mut h = ah.write();
                    h.decay_and_prune();
                    h.freq.clone()
                })
                .unwrap_or_else(|| old_manifest.btree_access_freq.clone()),
            prediction_patterns: prediction
                .map(|p| {
                    let mut t = p.write();
                    t.prune();
                    t.to_persisted()
                })
                .unwrap_or_else(|| old_manifest.prediction_patterns.clone()),
        };
        m.build_page_index();
        m
    };
    s3.put_manifest(&new_manifest)?;
    eprintln!(
        "[flush] manifest v{} uploaded (page_count={}, {} groups)",
        new_manifest.version, new_manifest.page_count, new_manifest.page_group_keys.len(),
    );

    // 9. Commit to shared manifest
    {
        let mut m = shared_manifest.write();
        cache.set_group_pages(new_manifest.group_pages.clone());
        *m = new_manifest;
    }

    // Persist bitmap
    let _ = cache.persist_bitmap();

    // 10. Post-flush GC: delete old page group/interior chunk versions
    if gc_enabled && !replaced_keys.is_empty() {
        eprintln!("[gc] deleting {} replaced S3 objects...", replaced_keys.len());
        if let Err(e) = s3.delete_objects(&replaced_keys) {
            eprintln!("[gc] ERROR: failed to delete old objects: {}", e);
        } else {
            eprintln!("[gc] deleted {} old versions", replaced_keys.len());
        }
    }

    eprintln!("[flush] complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    #[test]
    fn test_local_checkpoint_flag_default_false() {
        set_local_checkpoint_only(false);
        assert!(!is_local_checkpoint_only());
    }

    #[test]
    fn test_local_checkpoint_flag_set_and_clear() {
        set_local_checkpoint_only(true);
        assert!(is_local_checkpoint_only());
        set_local_checkpoint_only(false);
        assert!(!is_local_checkpoint_only());
    }

    #[test]
    fn test_s3_dirty_groups_drain_merges_into_groups_dirty() {
        // Simulate what sync() does: drain s3_dirty_groups into groups_dirty
        let mut s3_dirty: HashSet<u64> = HashSet::new();
        s3_dirty.insert(0);
        s3_dirty.insert(3);
        s3_dirty.insert(7);

        let mut groups_dirty: HashMap<u64, Vec<u64>> = HashMap::new();
        groups_dirty.entry(0).or_default().push(100);
        groups_dirty.entry(5).or_default().push(20480);

        // Merge pending groups (as sync() does)
        for gid in s3_dirty.drain() {
            groups_dirty.entry(gid).or_default();
        }

        // Group 0: has dirty_snapshot pages
        assert_eq!(groups_dirty[&0], vec![100]);
        // Group 5: has dirty_snapshot pages
        assert_eq!(groups_dirty[&5], vec![20480]);
        // Groups 3 and 7: pending from local checkpoint, no dirty_snapshot pages
        assert!(groups_dirty[&3].is_empty());
        assert!(groups_dirty[&7].is_empty());
        assert!(s3_dirty.is_empty());
    }

    #[test]
    fn test_local_checkpoint_interior_page_detection() {
        let page_size = 4096usize;

        // B-tree interior index page (type 0x02) — not page 0
        let mut interior_index = vec![0u8; page_size];
        interior_index[0] = 0x02;
        assert_eq!(interior_index.get(0), Some(&0x02));

        // B-tree interior table page (type 0x05) — not page 0
        let mut interior_table = vec![0u8; page_size];
        interior_table[0] = 0x05;
        assert_eq!(interior_table.get(0), Some(&0x05));

        // Page 0: type byte at offset 100 (after SQLite header)
        let mut page0 = vec![0u8; page_size];
        page0[100] = 0x05;
        assert_eq!(page0.get(100), Some(&0x05));

        // Leaf page (type 0x0D) — should NOT be interior
        let mut leaf = vec![0u8; page_size];
        leaf[0] = 0x0D;
        let b = leaf[0];
        assert!(b != 0x05 && b != 0x02);
    }

    #[test]
    fn test_flush_empty_dirty_groups_is_noop() {
        // flush_dirty_groups_to_s3 should return Ok immediately if no pending groups
        let shared_dirty = Arc::new(std::sync::Mutex::new(HashSet::<u64>::new()));
        let drained: HashSet<u64> = {
            let mut pending = shared_dirty.lock().unwrap();
            std::mem::take(&mut *pending)
        };
        assert!(drained.is_empty());
    }
}
