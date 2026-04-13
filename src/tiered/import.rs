//! Bulk import: read a local SQLite file and upload to S3 as tiered page groups.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io;
use std::path::Path;

use super::*;

/// Import a local SQLite database file directly to S3 as tiered page groups.
///
/// Reads the file page-by-page, encodes page groups, detects interior B-tree pages,
/// and uploads everything to S3 with a manifest. This is much faster than writing
/// through the VFS because it avoids WAL overhead and checkpoint cycles.
///
/// Returns the manifest that was uploaded.
pub fn import_sqlite_file(
    config: &TurboliteConfig,
    file_path: &Path,
) -> io::Result<Manifest> {
    use std::os::unix::fs::FileExt;

    let runtime = tokio::runtime::Runtime::new()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let handle = runtime.handle().clone();

    // Build a minimal config for S3Client
    let s3_cfg = TurboliteConfig {
        bucket: config.bucket.clone(),
        prefix: config.prefix.clone(),
        endpoint_url: config.endpoint_url.clone(),
        region: config.region.clone(),
        runtime_handle: Some(handle.clone()),
        ..Default::default()
    };

    let s3 = S3Client::block_on(&handle, S3Client::new_async(&s3_cfg))?;

    // Open the file and read SQLite header to get page size and page count
    let file = File::open(file_path)?;
    let file_len = file.metadata()?.len();

    // SQLite header: page size at offset 16 (2 bytes, big-endian)
    let mut header = [0u8; 100];
    file.read_exact_at(&mut header, 0)?;
    let raw_page_size = u16::from_be_bytes([header[16], header[17]]);
    let page_size: u32 = if raw_page_size == 1 { 65536 } else { raw_page_size as u32 };
    let page_count = (file_len / page_size as u64) as u64;
    let ppg = config.pages_per_group;
    let total_groups = (page_count + ppg as u64 - 1) / ppg as u64;

    eprintln!(
        "[import] file={} size={:.1}MB page_size={} pages={} groups={}",
        file_path.display(),
        file_len as f64 / (1024.0 * 1024.0),
        page_size,
        page_count,
        total_groups,
    );

    let compression_level = config.compression_level;
    let sub_ppf = config.sub_pages_per_frame;
    let use_seekable = sub_ppf > 0;
    // Phase Somme: use SQLite's file change counter as manifest version.
    let version = read_file_change_counter(&header);
    assert!(version > 0, "file change counter must be > 0 for import (is this a valid SQLite DB with committed data?)");
    eprintln!(
        "[import] encoding: {} (sub_ppf={})",
        if use_seekable { "seekable multi-frame" } else { "legacy single-frame" },
        sub_ppf,
    );

    // Walk B-trees to discover page ownership and build groups
    let (group_pages_list, btrees_manifest) = {
            // Walk B-trees to discover page ownership
            eprintln!("[import] walking B-trees...");
            let walk_result = crate::btree_walker::walk_all_btrees(page_count, page_size, &|page_num| {
                let mut buf = vec![0u8; page_size as usize];
                file.read_exact_at(&mut buf, page_num * page_size as u64).ok()?;
                Some(buf)
            });
            eprintln!(
                "[import] found {} B-trees, {} unowned pages",
                walk_result.btrees.len(),
                walk_result.unowned_pages.len(),
            );

            // Pack pages by B-tree into groups.
            // Large B-trees (>= ppg/4 pages) get their own groups.
            // Small B-trees + unowned pages are bin-packed into shared groups.
            let mut group_pages_list: Vec<Vec<u64>> = Vec::new();

            // Sort B-trees by size descending (large first)
            let mut btree_list: Vec<(&u64, &crate::btree_walker::BTreeEntry)> =
                walk_result.btrees.iter().collect();
            btree_list.sort_by(|a, b| b.1.pages.len().cmp(&a.1.pages.len()));

            let threshold = std::cmp::max(ppg as usize / 4, 1);
            let mut small_pages: Vec<u64> = Vec::new();

            for (_root, entry) in &btree_list {
                let mut sorted_pages = entry.pages.clone();
                sorted_pages.sort_unstable();

                if sorted_pages.len() >= threshold {
                    for chunk in sorted_pages.chunks(ppg as usize) {
                        group_pages_list.push(chunk.to_vec());
                    }
                } else {
                    small_pages.extend_from_slice(&sorted_pages);
                }
            }

            let mut sorted_unowned = walk_result.unowned_pages.clone();
            sorted_unowned.sort_unstable();
            small_pages.extend_from_slice(&sorted_unowned);

            for chunk in small_pages.chunks(ppg as usize) {
                group_pages_list.push(chunk.to_vec());
            }

            eprintln!(
                "[import] packed into {} groups (was {} positional)",
                group_pages_list.len(), total_groups,
            );

            // Build btrees manifest map
            let mut page_to_gid: HashMap<u64, u64> = HashMap::new();
            for (gid, pages) in group_pages_list.iter().enumerate() {
                for &p in pages {
                    page_to_gid.insert(p, gid as u64);
                }
            }

            let mut btrees_manifest: HashMap<u64, BTreeManifestEntry> = HashMap::new();
            for (&root_page, entry) in &walk_result.btrees {
                let mut gid_set: HashSet<u64> = HashSet::new();
                for &p in &entry.pages {
                    if let Some(&gid) = page_to_gid.get(&p) {
                        gid_set.insert(gid);
                    }
                }
                let mut gids: Vec<u64> = gid_set.into_iter().collect();
                gids.sort_unstable();
                btrees_manifest.insert(root_page, BTreeManifestEntry {
                    name: entry.name.clone(),
                    obj_type: entry.obj_type.clone(),
                    group_ids: gids,
                });
            }

            (group_pages_list, btrees_manifest)
    };

    let actual_groups = group_pages_list.len();

    // Encode and collect uploads for each group
    let mut page_group_keys: Vec<String> = Vec::with_capacity(actual_groups);
    let mut frame_tables: Vec<Vec<FrameEntry>> = Vec::with_capacity(actual_groups);
    let mut uploads: Vec<(String, Vec<u8>)> = Vec::new();
    let mut interior_pages: Vec<(u64, Vec<u8>)> = Vec::new();
    let mut index_leaf_pages: Vec<(u64, Vec<u8>)> = Vec::new();

    for (gid, page_nums) in group_pages_list.iter().enumerate() {
        let mut pages: Vec<Option<Vec<u8>>> = vec![None; ppg as usize];

        for (idx, &pnum) in page_nums.iter().enumerate() {
            let mut buf = vec![0u8; page_size as usize];
            file.read_exact_at(&mut buf, pnum * page_size as u64)?;

            // Detect interior B-tree pages and index leaf pages
            let hdr_off = if pnum == 0 { 100 } else { 0 };
            let type_byte = buf[hdr_off];
            if type_byte == 0x05 || type_byte == 0x02 {
                interior_pages.push((pnum, buf.clone()));
            } else if type_byte == 0x0A && is_valid_btree_page(&buf, hdr_off) {
                index_leaf_pages.push((pnum, buf.clone()));
            }

            pages[idx] = Some(buf);
        }

        let key = s3.page_group_key(gid as u64, version);
        if use_seekable {
            let (encoded, ft) = encode_page_group_seekable(
                &pages,
                page_size,
                sub_ppf,
                compression_level,
                #[cfg(feature = "zstd")]
                None,
                config.encryption_key.as_ref(),
            )?;
            uploads.push((key.clone(), encoded));
            frame_tables.push(ft);
        } else {
            let encoded = encode_page_group(
                &pages,
                page_size,
                compression_level,
                #[cfg(feature = "zstd")]
                None,
                config.encryption_key.as_ref(),
            )?;
            uploads.push((key.clone(), encoded));
            frame_tables.push(Vec::new());
        }
        page_group_keys.push(key);

        // DEBUG: verify encode/decode roundtrip for every group
        if std::env::var("IMPORT_VERIFY").is_ok() {
            let encoded_data = &uploads.last().unwrap().1;
            let ft = frame_tables.last().unwrap();
            if use_seekable && !ft.is_empty() {
                let (dec_count, _dec_size, decoded) = decode_page_group_seekable_full(
                    encoded_data,
                    ft,
                    page_size,
                    page_nums.len() as u32,
                    page_count,
                    0,
                    #[cfg(feature = "zstd")]
                    None,
                    config.encryption_key.as_ref(),
                ).expect("decode roundtrip failed");
                assert_eq!(dec_count as usize, page_nums.len(), "gid={} page count mismatch", gid);
                for (idx, &pnum) in page_nums.iter().enumerate() {
                    let start = idx * page_size as usize;
                    let end = start + page_size as usize;
                    let decoded_page = &decoded[start..end];
                    let original = pages[idx].as_ref().unwrap();
                    assert_eq!(decoded_page, original.as_slice(),
                        "gid={} page {} (idx={}) roundtrip mismatch: first differing byte at {}",
                        gid, pnum, idx,
                        decoded_page.iter().zip(original.iter()).position(|(a, b)| a != b).unwrap_or(0),
                    );
                }
            }
            if gid == 0 {
                eprintln!("[import] IMPORT_VERIFY: roundtrip checks enabled");
            }
        }

        if (gid + 1) % 50 == 0 || gid + 1 == actual_groups {
            eprintln!(
                "[import] encoded {}/{} groups ({} interior pages so far)",
                gid + 1, actual_groups, interior_pages.len(),
            );
        }
    }

    // Upload page groups in batches of 50 (parallel within each batch)
    let batch_size = 50;
    let total_uploads = uploads.len();
    for (batch_idx, batch) in uploads.chunks(batch_size).enumerate() {
        s3.put_page_groups(batch)?;
        let done = std::cmp::min((batch_idx + 1) * batch_size, total_uploads);
        eprintln!("[import] uploaded {}/{} page groups", done, total_uploads);
    }

    // Build interior chunks
    let mut chunks: HashMap<u32, Vec<(u64, Vec<u8>)>> = HashMap::new();
    for (pnum, data) in interior_pages {
        let chunk_id = (pnum / bundle_chunk_range(page_size)) as u32;
        chunks.entry(chunk_id).or_default().push((pnum, data));
    }
    for pages in chunks.values_mut() {
        pages.sort_by_key(|(pnum, _)| *pnum);
    }

    let mut interior_chunk_keys: HashMap<u32, String> = HashMap::new();
    let mut chunk_uploads: Vec<(String, Vec<u8>)> = Vec::new();
    for (&chunk_id, pages) in &chunks {
        let refs: Vec<(u64, &[u8])> = pages.iter().map(|(p, d)| (*p, d.as_slice())).collect();
        let encoded = encode_interior_bundle(
            &refs,
            page_size,
            compression_level,
            #[cfg(feature = "zstd")]
            None,
            config.encryption_key.as_ref(),
        )?;
        let key = s3.interior_chunk_key(chunk_id, version);
        eprintln!(
            "[import] interior chunk {}: {} pages, {:.1}KB",
            chunk_id, pages.len(), encoded.len() as f64 / 1024.0,
        );
        chunk_uploads.push((key.clone(), encoded));
        interior_chunk_keys.insert(chunk_id, key);
    }

    if !chunk_uploads.is_empty() {
        s3.put_page_groups(&chunk_uploads)?;
        eprintln!("[import] uploaded {} interior chunks", chunk_uploads.len());
    }

    // Build index leaf chunks (same pattern as interior)
    let mut ix_chunks: HashMap<u32, Vec<(u64, Vec<u8>)>> = HashMap::new();
    for (pnum, data) in index_leaf_pages {
        let chunk_id = (pnum / bundle_chunk_range(page_size)) as u32;
        ix_chunks.entry(chunk_id).or_default().push((pnum, data));
    }
    for pages in ix_chunks.values_mut() {
        pages.sort_by_key(|(pnum, _)| *pnum);
    }

    let mut index_chunk_keys: HashMap<u32, String> = HashMap::new();
    let mut ix_chunk_uploads: Vec<(String, Vec<u8>)> = Vec::new();
    for (&chunk_id, pages) in &ix_chunks {
        let refs: Vec<(u64, &[u8])> = pages.iter().map(|(p, d)| (*p, d.as_slice())).collect();
        let encoded = encode_interior_bundle(
            &refs,
            page_size,
            compression_level,
            #[cfg(feature = "zstd")]
            None,
            config.encryption_key.as_ref(),
        )?;
        let key = s3.index_chunk_key(chunk_id, version);
        eprintln!(
            "[import] index chunk {}: {} pages, {:.1}KB",
            chunk_id, pages.len(), encoded.len() as f64 / 1024.0,
        );
        ix_chunk_uploads.push((key.clone(), encoded));
        index_chunk_keys.insert(chunk_id, key);
    }

    if !ix_chunk_uploads.is_empty() {
        s3.put_page_groups(&ix_chunk_uploads)?;
        eprintln!("[import] uploaded {} index leaf chunks", ix_chunk_uploads.len());
    }

    // Build and upload manifest (Phase Midway: explicit B-tree-aware groups)
    let mut manifest = Manifest {
        version,
        change_counter: version, // import uses the same value; walrust not relevant for import
        page_count,
        page_size,
        pages_per_group: ppg,
        page_group_keys,
        interior_chunk_keys,
        index_chunk_keys,
        frame_tables,
        sub_pages_per_frame: if use_seekable { sub_ppf } else { 0 },
        subframe_overrides: Vec::new(),
        strategy: GroupingStrategy::BTreeAware,
        group_pages: group_pages_list,
        btrees: btrees_manifest,
        page_index: HashMap::new(),
        btree_groups: HashMap::new(),
        page_to_tree_name: HashMap::new(),
        tree_name_to_groups: HashMap::new(),
        group_to_tree_name: HashMap::new(),
        db_header: None, // import doesn't need db_header (fresh import)
    };
    manifest.build_page_index();
    s3.put_manifest(&manifest)?;
    eprintln!(
        "[import] manifest uploaded: version={} pages={} groups={} interior_chunks={} seekable={}",
        manifest.version, manifest.page_count, manifest.page_group_keys.len(),
        manifest.interior_chunk_keys.len(), use_seekable,
    );

    Ok(manifest)
}
