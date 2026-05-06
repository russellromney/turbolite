//! Non-blocking backend flush for the two-phase checkpoint path.
//!
//! After a local-only checkpoint (runtime `LOCAL_CHECKPOINT_ONLY` flag, or
//! recovery of staging logs written by an older turbolite), dirty pages are
//! captured in staging log files on disk. This module reads from those logs
//! (not the live disk cache), encodes page groups, and uploads to the backend
//! without holding any SQLite lock. The flush is backend-agnostic: an
//! `Arc<dyn StorageBackend>` driven through a tokio runtime handle.

use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::Arc;
use std::sync::Mutex;

use arc_swap::ArcSwap;
use hadb_storage::StorageBackend;
use tokio::runtime::Handle as TokioHandle;

use super::storage as storage_helpers;
use super::{
    bundle_chunk_range, compact, decode_page_group, decode_page_group_seekable_full,
    encode_interior_bundle, encode_override_frame, encode_page_group, encode_page_group_seekable,
    keys, manifest, staging, try_read_change_counter_from_cache, DiskCache, FrameEntry, Manifest,
    SubChunkId, SubChunkTier,
};

/// Upload locally-checkpointed dirty page groups to the backend.
///
/// Backend-agnostic: works for both local filesystem and remote storage.
/// Reads dirty pages from staging logs, non-dirty pages from the cache, and
/// merges with base page groups fetched from the backend when needed.
///
/// # Safety contract
/// - Must NOT be called concurrently with itself (caller holds flush_lock).
/// - Staging log files must exist for all pending flushes.
pub(crate) fn flush_dirty_groups(
    backend: &dyn StorageBackend,
    runtime: &TokioHandle,
    is_local: bool,
    cache: &DiskCache,
    shared_manifest: &ArcSwap<Manifest>,
    shared_dirty_groups: &Mutex<HashSet<u64>>,
    pending_flushes: &Mutex<Vec<staging::PendingFlush>>,
    compression_level: i32,
    #[cfg(feature = "zstd")] dictionary: Option<&[u8]>,
    encryption_key: Option<[u8; 32]>,
    gc_enabled: bool,
    override_threshold: u32,
    compaction_threshold: u32,
) -> io::Result<()> {
    let flushes: Vec<staging::PendingFlush> = {
        let mut pf = pending_flushes.lock().unwrap();
        std::mem::take(&mut *pf)
    };
    let legacy_dirty_groups: HashSet<u64> = {
        let mut pending = shared_dirty_groups.lock().unwrap();
        std::mem::take(&mut *pending)
    };

    if flushes.is_empty() && legacy_dirty_groups.is_empty() {
        turbolite_debug!("[flush] no pending groups to upload");
        return Ok(());
    }

    let result = flush_inner(
        backend,
        runtime,
        is_local,
        cache,
        shared_manifest,
        compression_level,
        #[cfg(feature = "zstd")]
        dictionary,
        encryption_key,
        gc_enabled,
        &flushes,
        &legacy_dirty_groups,
        override_threshold,
        compaction_threshold,
    );

    if let Err(ref e) = result {
        eprintln!(
            "[flush] ERROR: flush failed, restoring pending state: {}",
            e
        );
        if !flushes.is_empty() {
            pending_flushes.lock().unwrap().extend(flushes);
        }
        if !legacy_dirty_groups.is_empty() {
            shared_dirty_groups
                .lock()
                .unwrap()
                .extend(legacy_dirty_groups);
        }
    }

    result
}

/// Inner flush logic. Separated so the outer function can restore state on error.
fn flush_inner(
    backend: &dyn StorageBackend,
    runtime: &TokioHandle,
    is_local: bool,
    cache: &DiskCache,
    shared_manifest: &ArcSwap<Manifest>,
    compression_level: i32,
    #[cfg(feature = "zstd")] dictionary: Option<&[u8]>,
    encryption_key: Option<[u8; 32]>,
    gc_enabled: bool,
    flushes: &[staging::PendingFlush],
    legacy_dirty_groups: &HashSet<u64>,
    override_threshold: u32,
    compaction_threshold: u32,
) -> io::Result<()> {
    // 1. Read all staging logs and merge into a single page map.
    let mut staged_pages: HashMap<u64, Vec<u8>> = HashMap::new();
    let mut staging_paths: Vec<std::path::PathBuf> = Vec::new();
    for flush_entry in flushes {
        let pages = staging::read_staging_log(&flush_entry.staging_path, flush_entry.page_size)?;
        turbolite_debug!(
            "[flush] read staging log v{}: {} pages from {}",
            flush_entry.version,
            pages.len(),
            flush_entry.staging_path.display(),
        );
        staging_paths.push(flush_entry.staging_path.clone());
        staged_pages.extend(pages);
    }

    turbolite_debug!(
        "[flush] {} staged pages from {} logs, {} legacy dirty groups",
        staged_pages.len(),
        flushes.len(),
        legacy_dirty_groups.len(),
    );

    // 2. Snapshot manifest
    let manifest_snap = (**shared_manifest.load()).clone();
    let page_count = manifest_snap.page_count;
    let page_size = manifest_snap.page_size;
    let ppg = manifest_snap.pages_per_group;

    if page_size == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "flush: manifest has page_size=0",
        ));
    }

    #[cfg(feature = "zstd")]
    let encoder_dict =
        dictionary.map(|d| zstd::dict::EncoderDictionary::copy(d, compression_level));
    #[cfg(feature = "zstd")]
    let decoder_dict = dictionary.map(zstd::dict::DecoderDictionary::copy);

    // 3. Determine all dirty groups.
    let mut dirty_groups: HashSet<u64> = legacy_dirty_groups.clone();
    for &pnum in staged_pages.keys() {
        if let Some(loc) = manifest_snap.page_location(pnum) {
            dirty_groups.insert(loc.group_id);
        }
    }

    if dirty_groups.is_empty() {
        turbolite_debug!("[flush] no dirty groups after manifest lookup");
        for path in &staging_paths {
            staging::remove_staging_log(path);
        }
        return Ok(());
    }

    turbolite_debug!("[flush] uploading {} dirty groups...", dirty_groups.len(),);

    let next_version = manifest_snap.version + 1;
    let cache_change_counter = try_read_change_counter_from_cache(cache, manifest_snap.page_size)?;
    let change_counter = cache_change_counter.max(manifest_snap.change_counter);
    if change_counter == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "file change counter must be > 0 at checkpoint/flush time",
        ));
    }
    let mut uploads: Vec<(String, Vec<u8>)> = Vec::new();
    let mut new_keys = manifest_snap.page_group_keys.clone();
    let mut replaced_keys: Vec<String> = Vec::new();

    let old_sub_ppf = manifest_snap.sub_pages_per_frame;
    let use_seekable = old_sub_ppf > 0;
    let mut new_frame_tables: Vec<Vec<FrameEntry>> = manifest_snap.frame_tables.clone();

    let mut new_subframe_overrides = manifest_snap.subframe_overrides.clone();
    let effective_threshold = if override_threshold > 0 {
        override_threshold as usize
    } else if use_seekable && old_sub_ppf > 0 {
        let frames_per_group = (ppg as usize + old_sub_ppf as usize - 1) / old_sub_ppf as usize;
        std::cmp::max(1, frames_per_group / 4)
    } else {
        0
    };
    let mut full_rewrite_groups: HashSet<u64> = HashSet::new();
    let mut dirty_page_nums: HashSet<u64> = HashSet::new();

    // 4. For each dirty group: read pages from staging/cache/backend, encode.
    for &gid in &dirty_groups {
        let pages_in_group = manifest_snap.group_page_nums(gid);
        let group_size = pages_in_group.len();

        for &pnum in pages_in_group.iter() {
            if pnum < page_count {
                dirty_page_nums.insert(pnum);
            }
        }

        let group_dirty_pnums: Vec<u64> = pages_in_group
            .iter()
            .filter(|&&pnum| pnum < page_count && staged_pages.contains_key(&pnum))
            .copied()
            .collect();
        let frame_table_ref = manifest_snap.frame_tables.get(gid as usize);
        let has_frame_table =
            use_seekable && frame_table_ref.map(|ft| !ft.is_empty()).unwrap_or(false);
        let dirty_frames = if has_frame_table && effective_threshold > 0 {
            manifest::dirty_frames_for_group(
                &group_dirty_pnums,
                &pages_in_group,
                frame_table_ref.expect("checked above"),
                old_sub_ppf,
            )
        } else {
            Vec::new()
        };
        let use_override = has_frame_table
            && effective_threshold > 0
            && !dirty_frames.is_empty()
            && dirty_frames.len() < effective_threshold;

        if use_override {
            while new_subframe_overrides.len() <= gid as usize {
                new_subframe_overrides.push(HashMap::new());
            }
            for &frame_idx in &dirty_frames {
                let frame_start = frame_idx * old_sub_ppf as usize;
                let frame_end = std::cmp::min(frame_start + old_sub_ppf as usize, group_size);
                let mut frame_pages: Vec<(u64, Vec<u8>)> = Vec::new();
                for pos in frame_start..frame_end {
                    let pnum = pages_in_group[pos];
                    if pnum >= page_count {
                        break;
                    }
                    let data = if let Some(staged) = staged_pages.get(&pnum) {
                        staged.clone()
                    } else if cache.is_present(pnum) {
                        let mut buf = vec![0u8; page_size as usize];
                        if cache.read_page(pnum, &mut buf).is_ok() {
                            buf
                        } else {
                            vec![0u8; page_size as usize]
                        }
                    } else {
                        vec![0u8; page_size as usize]
                    };
                    frame_pages.push((pnum, data));
                }
                let override_key = keys::override_frame_key(gid, frame_idx, next_version);
                let encoded = encode_override_frame(
                    &frame_pages,
                    page_size,
                    compression_level,
                    #[cfg(feature = "zstd")]
                    encoder_dict.as_ref(),
                    encryption_key.as_ref(),
                )?;
                if let Some(old_ov) = new_subframe_overrides[gid as usize].get(&frame_idx) {
                    replaced_keys.push(old_ov.key.clone());
                }
                uploads.push((override_key.clone(), encoded.clone()));
                new_subframe_overrides[gid as usize].insert(
                    frame_idx,
                    manifest::SubframeOverride {
                        key: override_key,
                        entry: FrameEntry {
                            offset: 0,
                            len: encoded.len() as u32,
                        },
                    },
                );
            }
        } else {
            full_rewrite_groups.insert(gid);
            let mut pages: Vec<Option<Vec<u8>>> = vec![None; group_size];
            let mut need_merge = false;

            for (i, &pnum) in pages_in_group.iter().enumerate() {
                if pnum >= page_count {
                    break;
                }
                if let Some(staged_data) = staged_pages.get(&pnum) {
                    pages[i] = Some(staged_data.clone());
                } else if cache.is_present(pnum) {
                    let mut page_buf = vec![0u8; page_size as usize];
                    if cache.read_page(pnum, &mut page_buf).is_ok() {
                        pages[i] = Some(page_buf);
                    } else {
                        need_merge = true;
                    }
                } else {
                    need_merge = true;
                }
            }

            // Backend merge: for remote mode, if we're missing pages, pull the
            // existing page group down and use its pages for the holes. Local
            // mode skips this (all pages should already be in the cache).
            if need_merge && !is_local {
                if let Some(existing_key) = new_keys.get(gid as usize) {
                    if !existing_key.is_empty() {
                        if let Ok(Some(pg_data)) =
                            storage_helpers::get_page_group(backend, runtime, existing_key)
                        {
                            let existing_ft = manifest_snap.frame_tables.get(gid as usize);
                            let has_ft = use_seekable
                                && existing_ft.map(|ft| !ft.is_empty()).unwrap_or(false);
                            if has_ft {
                                let ft = existing_ft.expect("checked above");
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
                                        if pages_in_group[j] >= page_count {
                                            break;
                                        }
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
                                    if j >= pages_in_group.len() {
                                        break;
                                    }
                                    if pages_in_group[j] >= page_count {
                                        break;
                                    }
                                    if pages[j].is_none() {
                                        pages[j] = Some(existing_page);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            let key = keys::page_group_key(gid, next_version);
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
    }

    // 5. Interior + index bundles. Only remote mode builds these into the
    //    manifest; local mode leaves them empty (everything is on disk).
    let page_group_upload_count = uploads.len();
    let chunk_range = bundle_chunk_range(page_size);

    let mut new_chunk_keys: HashMap<u32, String> = manifest_snap.interior_chunk_keys.clone();
    let mut new_index_chunk_keys: HashMap<u32, String> = manifest_snap.index_chunk_keys.clone();
    let interior_count;
    let index_chunk_count;

    if !is_local {
        let mut all_interior: HashMap<u64, Vec<u8>> = HashMap::new();
        {
            let known_interior = cache.interior_pages.lock().clone();
            for &pnum in &known_interior {
                if pnum >= page_count {
                    continue;
                }
                if let Some(staged_data) = staged_pages.get(&pnum) {
                    all_interior.insert(pnum, staged_data.clone());
                } else {
                    let mut buf = vec![0u8; page_size as usize];
                    if cache.read_page(pnum, &mut buf).is_ok() {
                        all_interior.insert(pnum, buf);
                    } else {
                        eprintln!(
                            "[flush] WARN: cache.read_page({}) failed for interior page",
                            pnum
                        );
                    }
                }
            }
        }

        let mut chunks: HashMap<u32, Vec<(u64, Vec<u8>)>> = HashMap::new();
        for (pnum, data) in all_interior {
            let chunk_id = (pnum / chunk_range) as u32;
            chunks.entry(chunk_id).or_default().push((pnum, data));
        }
        for pages in chunks.values_mut() {
            pages.sort_by_key(|(pnum, _)| *pnum);
        }

        let dirty_chunk_ids: HashSet<u32> = dirty_page_nums
            .iter()
            .map(|&pnum| (pnum / chunk_range) as u32)
            .collect();

        let old_chunk_keys = manifest_snap.interior_chunk_keys.clone();
        new_chunk_keys.clear();
        let mut chunk_uploads: Vec<(String, Vec<u8>)> = Vec::new();

        for (&chunk_id, pages) in &chunks {
            if dirty_chunk_ids.contains(&chunk_id) || !old_chunk_keys.contains_key(&chunk_id) {
                let refs: Vec<(u64, &[u8])> =
                    pages.iter().map(|(p, d)| (*p, d.as_slice())).collect();
                let encoded = encode_interior_bundle(
                    &refs,
                    page_size,
                    compression_level,
                    #[cfg(feature = "zstd")]
                    encoder_dict.as_ref(),
                    encryption_key.as_ref(),
                )?;
                let key = keys::interior_chunk_key(chunk_id, next_version);
                chunk_uploads.push((key.clone(), encoded));
                if let Some(old_key) = old_chunk_keys.get(&chunk_id) {
                    replaced_keys.push(old_key.clone());
                }
                new_chunk_keys.insert(chunk_id, key);
            } else {
                new_chunk_keys.insert(chunk_id, old_chunk_keys[&chunk_id].clone());
            }
        }

        for (old_chunk_id, old_key) in &old_chunk_keys {
            if !chunks.contains_key(old_chunk_id) {
                replaced_keys.push(old_key.clone());
            }
        }

        uploads.extend(chunk_uploads);

        // Index leaf bundles (same pattern as interior)
        let mut all_index_leaves: HashMap<u64, Vec<u8>> = HashMap::new();
        {
            for &gid in &dirty_groups {
                let pages_in_group = manifest_snap.group_page_nums(gid);
                for &pnum in pages_in_group.iter() {
                    if pnum >= page_count || all_index_leaves.contains_key(&pnum) {
                        continue;
                    }
                    let buf = if let Some(staged_data) = staged_pages.get(&pnum) {
                        Some(staged_data.clone())
                    } else if cache.is_present(pnum) {
                        let mut b = vec![0u8; page_size as usize];
                        if cache.read_page(pnum, &mut b).is_ok() {
                            Some(b)
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    if let Some(buf) = buf {
                        let hdr_off = if pnum == 0 { 100 } else { 0 };
                        let tb = buf.get(hdr_off).copied();
                        if tb == Some(0x0A) && super::is_valid_btree_page(&buf, hdr_off) {
                            all_index_leaves.insert(pnum, buf);
                        }
                    }
                }
            }

            let tracker = cache.tracker.lock();
            let index_sub_chunks: Vec<SubChunkId> = tracker
                .present
                .iter()
                .filter(|id| tracker.tiers.get(id).copied() == Some(SubChunkTier::Index))
                .copied()
                .collect();
            drop(tracker);

            for sc in &index_sub_chunks {
                let tracker = cache.tracker.lock();
                let page_range = tracker.pages_for_sub_chunk(*sc, page_count);
                drop(tracker);
                for pnum in page_range {
                    if pnum >= page_count || all_index_leaves.contains_key(&pnum) {
                        continue;
                    }
                    let buf = if let Some(staged_data) = staged_pages.get(&pnum) {
                        Some(staged_data.clone())
                    } else {
                        let mut b = vec![0u8; page_size as usize];
                        if cache.read_page(pnum, &mut b).is_ok() {
                            Some(b)
                        } else {
                            None
                        }
                    };
                    if let Some(buf) = buf {
                        let hdr_off = if pnum == 0 { 100 } else { 0 };
                        let tb = buf.get(hdr_off).copied();
                        if tb == Some(0x0A) && super::is_valid_btree_page(&buf, hdr_off) {
                            all_index_leaves.insert(pnum, buf);
                        }
                    }
                }
            }
        }

        let mut index_chunks: HashMap<u32, Vec<(u64, Vec<u8>)>> = HashMap::new();
        for (pnum, data) in all_index_leaves {
            let chunk_id = (pnum / chunk_range) as u32;
            index_chunks.entry(chunk_id).or_default().push((pnum, data));
        }
        for pages in index_chunks.values_mut() {
            pages.sort_by_key(|(pnum, _)| *pnum);
        }

        let dirty_index_chunk_ids: HashSet<u32> = dirty_page_nums
            .iter()
            .map(|&pnum| (pnum / chunk_range) as u32)
            .collect();

        let old_index_chunk_keys = manifest_snap.index_chunk_keys.clone();
        new_index_chunk_keys.clear();
        let mut index_chunk_uploads: Vec<(String, Vec<u8>)> = Vec::new();

        for (&chunk_id, pages) in &index_chunks {
            if dirty_index_chunk_ids.contains(&chunk_id)
                || !old_index_chunk_keys.contains_key(&chunk_id)
            {
                let refs: Vec<(u64, &[u8])> =
                    pages.iter().map(|(p, d)| (*p, d.as_slice())).collect();
                let encoded = encode_interior_bundle(
                    &refs,
                    page_size,
                    compression_level,
                    #[cfg(feature = "zstd")]
                    encoder_dict.as_ref(),
                    encryption_key.as_ref(),
                )?;
                let key = keys::index_chunk_key(chunk_id, next_version);
                index_chunk_uploads.push((key.clone(), encoded));
                if let Some(old_key) = old_index_chunk_keys.get(&chunk_id) {
                    replaced_keys.push(old_key.clone());
                }
                new_index_chunk_keys.insert(chunk_id, key);
            } else {
                new_index_chunk_keys.insert(chunk_id, old_index_chunk_keys[&chunk_id].clone());
            }
        }

        for (old_chunk_id, old_key) in &old_index_chunk_keys {
            if !index_chunks.contains_key(old_chunk_id) {
                replaced_keys.push(old_key.clone());
            }
        }

        index_chunk_count = index_chunk_uploads.len();
        uploads.extend(index_chunk_uploads);
        interior_count = uploads.len() - page_group_upload_count - index_chunk_count;
    } else {
        interior_count = 0;
        index_chunk_count = 0;
    }

    turbolite_debug!(
        "[flush] uploading {} objects ({} page groups + {} interior + {} index)...",
        uploads.len(),
        page_group_upload_count,
        interior_count,
        index_chunk_count,
    );
    storage_helpers::put_page_groups(backend, runtime, &uploads)?;

    // 6. Update manifest atomically
    while new_subframe_overrides.len() < new_keys.len() {
        new_subframe_overrides.push(HashMap::new());
    }
    for &gid in &full_rewrite_groups {
        if let Some(group_ovs) = new_subframe_overrides.get_mut(gid as usize) {
            for (_, ov) in group_ovs.drain() {
                replaced_keys.push(ov.key);
            }
        }
    }
    let new_manifest = {
        let old_manifest = manifest_snap;
        let mut m = Manifest {
            version: next_version,
            change_counter,
            epoch: old_manifest.epoch,
            page_count: old_manifest.page_count,
            page_size: old_manifest.page_size,
            pages_per_group: ppg,
            page_group_keys: new_keys,
            interior_chunk_keys: new_chunk_keys,
            index_chunk_keys: new_index_chunk_keys,
            frame_tables: new_frame_tables,
            sub_pages_per_frame: old_sub_ppf,
            subframe_overrides: new_subframe_overrides,
            strategy: old_manifest.strategy,
            group_pages: old_manifest.group_pages.clone(),
            btrees: old_manifest.btrees.clone(),
            page_index: HashMap::new(),
            btree_groups: HashMap::new(),
            page_to_tree_name: HashMap::new(),
            tree_name_to_groups: HashMap::new(),
            group_to_tree_name: HashMap::new(),
            db_header: old_manifest.db_header.clone(),
        };
        m.build_page_index();
        m
    };
    storage_helpers::put_manifest(backend, runtime, &new_manifest)?;

    // 7. Commit to shared state
    cache.set_group_pages(new_manifest.group_pages.clone());
    shared_manifest.store(Arc::new(new_manifest.clone()));

    // 8. Persist local manifest + clear dirty_groups file (flush complete)
    if let Err(e) = manifest::persist_manifest_local(&cache.cache_dir, &new_manifest) {
        eprintln!("[flush] ERROR: failed to persist local manifest: {}", e);
    }
    if let Err(e) = manifest::persist_dirty_groups(&cache.cache_dir, &[]) {
        eprintln!("[flush] ERROR: failed to clear dirty_groups: {}", e);
    }

    let _ = cache.persist_bitmap();

    // 9. Post-flush GC
    if gc_enabled && !replaced_keys.is_empty() {
        turbolite_debug!("[gc] deleting {} replaced objects...", replaced_keys.len());
        if let Err(e) = storage_helpers::delete_objects(backend, runtime, &replaced_keys) {
            eprintln!("[gc] ERROR: failed to delete old objects: {}", e);
        } else {
            turbolite_debug!("[gc] deleted {} old versions", replaced_keys.len());
        }
    }

    // 10. Clean up staging logs. For local mode we already replayed + cleaned
    //     at VFS open time; this just covers any leftover entries.
    for path in &staging_paths {
        staging::remove_staging_log(path);
    }

    // 11. Auto-compact overrides if threshold reached
    if compaction_threshold > 0 {
        if let Err(e) = compact::auto_compact_overrides(
            backend,
            runtime,
            shared_manifest,
            compaction_threshold,
            compression_level,
            #[cfg(feature = "zstd")]
            dictionary,
            encryption_key,
        ) {
            eprintln!("[flush] compaction error (non-fatal): {}", e);
        }
    }

    let _ = override_threshold;
    turbolite_debug!("[flush] complete");
    Ok(())
}

#[cfg(test)]
#[path = "test_flush.rs"]
mod tests;
