//! Non-blocking S3 flush for two-phase checkpoint (Phase Kursk).
//!
//! After a local-only checkpoint (SyncMode::LocalThenFlush), dirty pages are
//! captured in staging log files on disk. This module reads from staging logs
//! (not the live disk cache), encodes page groups, and uploads to S3 without
//! holding any SQLite lock.
//!
//! Staging logs guarantee correctness: even if a subsequent checkpoint overwrites
//! pages in the disk cache, the staging log has the exact contents from the
//! original checkpoint.

use super::*;

/// Upload locally-checkpointed dirty page groups to S3.
///
/// Reads dirty pages from staging logs (Phase Kursk), non-dirty pages from
/// cache or S3. Called outside any SQLite lock.
///
/// # Safety contract
/// - Must NOT be called concurrently with itself (caller holds flush_lock).
/// - Staging log files must exist for all pending flushes.
pub(crate) fn flush_dirty_groups_to_s3(
    s3: &S3Client,
    cache: &DiskCache,
    shared_manifest: &RwLock<Manifest>,
    shared_dirty_groups: &Mutex<HashSet<u64>>,
    pending_flushes: &Mutex<Vec<staging::PendingFlush>>,
    compression_level: i32,
    #[cfg(feature = "zstd")] dictionary: Option<&[u8]>,
    encryption_key: Option<[u8; 32]>,
    gc_enabled: bool,
) -> io::Result<()> {
    // 1. Drain pending flushes (staging logs) + legacy dirty groups
    let flushes: Vec<staging::PendingFlush> = {
        let mut pf = pending_flushes.lock().unwrap();
        std::mem::take(&mut *pf)
    };
    let legacy_dirty_groups: HashSet<u64> = {
        let mut pending = shared_dirty_groups.lock().unwrap();
        std::mem::take(&mut *pending)
    };

    let has_staging = !flushes.is_empty();
    let has_legacy = !legacy_dirty_groups.is_empty();

    if !has_staging && !has_legacy {
        eprintln!("[flush] no pending groups to upload");
        return Ok(());
    }

    // Run the actual flush, restoring drained state on error so nothing is lost.
    let result = flush_inner(
        s3, cache, shared_manifest, compression_level,
        #[cfg(feature = "zstd")] dictionary,
        encryption_key, gc_enabled,
        &flushes, &legacy_dirty_groups,
    );

    if let Err(ref e) = result {
        eprintln!("[flush] ERROR: flush failed, restoring pending state: {}", e);
        // Push staging logs back so next flush_to_s3() retries them
        if !flushes.is_empty() {
            pending_flushes.lock().unwrap().extend(flushes);
        }
        // Push legacy dirty groups back
        if !legacy_dirty_groups.is_empty() {
            shared_dirty_groups.lock().unwrap().extend(legacy_dirty_groups);
        }
    }

    result
}

/// Inner flush logic. Separated so the outer function can restore state on error.
fn flush_inner(
    s3: &S3Client,
    cache: &DiskCache,
    shared_manifest: &RwLock<Manifest>,
    compression_level: i32,
    #[cfg(feature = "zstd")] dictionary: Option<&[u8]>,
    encryption_key: Option<[u8; 32]>,
    gc_enabled: bool,
    flushes: &[staging::PendingFlush],
    legacy_dirty_groups: &HashSet<u64>,
) -> io::Result<()> {
    // 2. Read all staging logs and merge into a single page map.
    // Later staging logs overwrite earlier ones (correct: later checkpoint wins).
    let mut staged_pages: HashMap<u64, Vec<u8>> = HashMap::new();
    let mut staging_paths: Vec<std::path::PathBuf> = Vec::new();
    for flush_entry in flushes {
        let pages = staging::read_staging_log(
            &flush_entry.staging_path,
            flush_entry.page_size,
        )?;
        eprintln!(
            "[flush] read staging log v{}: {} pages from {}",
            flush_entry.version, pages.len(), flush_entry.staging_path.display(),
        );
        staging_paths.push(flush_entry.staging_path.clone());
        staged_pages.extend(pages);
    }

    eprintln!(
        "[flush] {} staged pages from {} logs, {} legacy dirty groups",
        staged_pages.len(), flushes.len(), legacy_dirty_groups.len(),
    );

    // 3. Snapshot manifest
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

    // 4. Build encoder/decoder dictionaries
    #[cfg(feature = "zstd")]
    let encoder_dict = dictionary
        .map(|d| zstd::dict::EncoderDictionary::copy(d, compression_level));
    #[cfg(feature = "zstd")]
    let decoder_dict = dictionary
        .map(zstd::dict::DecoderDictionary::copy);

    // 5. Determine all dirty groups: from staged pages + legacy dirty groups
    let mut dirty_groups: HashSet<u64> = legacy_dirty_groups.clone();
    for &pnum in staged_pages.keys() {
        if let Some(loc) = manifest_snap.page_location(pnum) {
            dirty_groups.insert(loc.group_id);
        }
    }

    if dirty_groups.is_empty() {
        eprintln!("[flush] no dirty groups after manifest lookup");
        // Clean up staging logs even if no groups (edge case: empty checkpoint)
        for path in &staging_paths {
            staging::remove_staging_log(path);
        }
        return Ok(());
    }

    eprintln!(
        "[flush] uploading {} dirty groups to S3...",
        dirty_groups.len(),
    );

    let next_version = read_change_counter_from_cache(cache, manifest_snap.page_size);
    let mut uploads: Vec<(String, Vec<u8>)> = Vec::new();
    let mut new_keys = manifest_snap.page_group_keys.clone();
    let mut replaced_keys: Vec<String> = Vec::new();

    // Carry forward seekable encoding from manifest
    let old_sub_ppf = manifest_snap.sub_pages_per_frame;
    let use_seekable = old_sub_ppf > 0;
    let mut new_frame_tables: Vec<Vec<FrameEntry>> = manifest_snap.frame_tables.clone();

    // Track page numbers from dirty groups (for interior/index chunk dirtiness)
    let mut dirty_page_nums: HashSet<u64> = HashSet::new();

    // 6. For each dirty group: read pages from staging/cache/S3, encode, prepare upload
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

            // Priority: staging log > cache > S3 merge
            if let Some(staged_data) = staged_pages.get(&pnum) {
                pages[i] = Some(staged_data.clone());
            } else if cache.is_present(pnum) {
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

        // If some pages aren't in staging or cache, fetch existing group from S3 and merge
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

    // 7. Upload all dirty page groups
    eprintln!("[flush] uploading {} page groups...", uploads.len());
    s3.put_page_groups(&uploads)?;
    eprintln!("[flush] page groups uploaded");

    // 8. Interior chunks: collect from staging + cache, re-upload dirty chunks
    eprintln!("[flush] building interior chunks...");
    let chunk_range = bundle_chunk_range(page_size);
    let mut all_interior: HashMap<u64, Vec<u8>> = HashMap::new();
    {
        let known_interior = cache.interior_pages.lock().clone();
        for &pnum in &known_interior {
            if pnum >= page_count {
                continue;
            }
            // Staging wins over cache for interior pages too
            if let Some(staged_data) = staged_pages.get(&pnum) {
                all_interior.insert(pnum, staged_data.clone());
            } else {
                let mut buf = vec![0u8; page_size as usize];
                if cache.read_page(pnum, &mut buf).is_ok() {
                    all_interior.insert(pnum, buf);
                } else {
                    eprintln!("[flush] WARN: cache.read_page({}) failed for interior page", pnum);
                }
            }
        }
        eprintln!(
            "[flush] interior collection: {} pages",
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

    // 9. Index leaf bundles (same pattern as interior)
    // Two sources: (a) staged/cache dirty group pages that are index leaves,
    // (b) previously-fetched index pages from the tracker's Index tier.
    eprintln!("[flush] building index leaf bundles...");
    let mut all_index_leaves: HashMap<u64, Vec<u8>> = HashMap::new();
    {
        // (a) Scan dirty group pages for index leaves (type 0x0A + valid header).
        let mut dirty_index_count = 0usize;
        for &gid in &dirty_groups {
            let pages_in_group = manifest_snap.group_page_nums(gid);
            for &pnum in pages_in_group.iter() {
                if pnum >= page_count || all_index_leaves.contains_key(&pnum) {
                    continue;
                }
                // Try staging first, then cache
                let buf = if let Some(staged_data) = staged_pages.get(&pnum) {
                    Some(staged_data.clone())
                } else if cache.is_present(pnum) {
                    let mut b = vec![0u8; page_size as usize];
                    if cache.read_page(pnum, &mut b).is_ok() { Some(b) } else { None }
                } else {
                    None
                };
                if let Some(buf) = buf {
                    let hdr_off = if pnum == 0 { 100 } else { 0 };
                    let tb = buf.get(hdr_off).copied();
                    if tb == Some(0x0A) && is_valid_btree_page(&buf, hdr_off) {
                        all_index_leaves.insert(pnum, buf);
                        dirty_index_count += 1;
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
                // Staging wins for tracker pages too
                let buf = if let Some(staged_data) = staged_pages.get(&pnum) {
                    Some(staged_data.clone())
                } else {
                    let mut b = vec![0u8; page_size as usize];
                    if cache.read_page(pnum, &mut b).is_ok() { Some(b) } else { None }
                };
                if let Some(buf) = buf {
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

    // 10. Update manifest atomically
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
        };
        m.build_page_index();
        m
    };
    s3.put_manifest(&new_manifest)?;
    eprintln!(
        "[flush] manifest v{} uploaded (page_count={}, {} groups)",
        new_manifest.version, new_manifest.page_count, new_manifest.page_group_keys.len(),
    );

    // 11. Commit to shared manifest
    {
        let mut m = shared_manifest.write();
        cache.set_group_pages(new_manifest.group_pages.clone());
        *m = new_manifest;
    }

    // Persist bitmap
    let _ = cache.persist_bitmap();

    // 12. Post-flush GC: delete old page group/interior chunk versions.
    if gc_enabled && !replaced_keys.is_empty() {
        eprintln!("[gc] deleting {} replaced S3 objects...", replaced_keys.len());
        if let Err(e) = s3.delete_objects(&replaced_keys) {
            eprintln!("[gc] ERROR: failed to delete old objects: {}", e);
        } else {
            eprintln!("[gc] deleted {} old versions", replaced_keys.len());
        }
    }

    // 13. Clean up staging logs (all successfully uploaded)
    for path in &staging_paths {
        staging::remove_staging_log(path);
    }

    // Phase Gallipoli: persist local manifest with empty dirty_groups (flush complete)
    {
        let m = shared_manifest.read().clone();
        let local = super::manifest::LocalManifest { manifest: m, dirty_groups: Vec::new() };
        if let Err(e) = local.persist(&cache.cache_dir) {
            eprintln!("[flush] ERROR: failed to persist local manifest: {}", e);
        }
    }

    eprintln!("[flush] complete");
    Ok(())
}

#[cfg(test)]
#[path = "test_flush.rs"]
mod tests;
