//! Page group compaction: repack B-tree groups to eliminate dead pages.
//!
//! Dead pages accumulate when SQLite deletes rows (pages go on freelist) or
//! when VACUUM reorganizes page numbers. These pages stay in S3 page groups
//! as wasted space. Compaction re-walks the B-trees, identifies groups with
//! high dead-page ratios, and repacks live pages into dense new groups.

use std::collections::HashSet;
use std::io;

use super::*;

/// Per-B-tree dead space analysis.
#[derive(Debug, Clone)]
pub struct BTreeDeadSpace {
    pub name: String,
    pub root_page: u64,
    pub live_pages: usize,
    pub dead_pages: usize,
    pub total_pages_in_groups: usize,
    pub dead_ratio: f64,
    /// Group IDs that contain dead pages from this B-tree.
    pub affected_groups: Vec<u64>,
}

/// Result of analyzing dead space across all B-trees.
#[derive(Debug, Clone)]
pub struct DeadSpaceReport {
    pub btrees: Vec<BTreeDeadSpace>,
    pub total_live: usize,
    pub total_dead: usize,
    pub total_in_groups: usize,
    /// B-trees exceeding the compaction threshold.
    pub candidates: Vec<String>,
}

/// Analyze dead space by re-walking B-trees and comparing against manifest groups.
///
/// `read_page` reads a page from the local cache (or S3 if not cached).
/// Returns a report of dead space per B-tree.
pub fn analyze_dead_space(
    manifest: &Manifest,
    page_size: u32,
    read_page: &dyn Fn(u64) -> Option<Vec<u8>>,
    threshold: f64,
) -> DeadSpaceReport {
    let walk = crate::btree_walker::walk_all_btrees(manifest.page_count, page_size, read_page);

    let mut btrees = Vec::new();
    let mut total_live = 0usize;
    let mut total_dead = 0usize;
    let mut total_in_groups = 0usize;
    let mut candidates = Vec::new();

    for (&root_page, walk_entry) in &walk.btrees {
        let live_pages_set: HashSet<u64> = walk_entry.pages.iter().copied().collect();

        // Find this B-tree's groups from manifest
        let manifest_entry = match manifest.btrees.get(&root_page) {
            Some(e) => e,
            None => continue, // New B-tree not yet in manifest
        };

        let mut pages_in_groups = 0usize;
        let mut dead = 0usize;
        let mut affected = Vec::new();

        for &gid in &manifest_entry.group_ids {
            let group_pages = manifest.group_page_nums(gid);
            let mut group_dead = 0usize;
            for &pnum in group_pages.iter() {
                pages_in_groups += 1;
                if !live_pages_set.contains(&pnum) {
                    dead += 1;
                    group_dead += 1;
                }
            }
            if group_dead > 0 {
                affected.push(gid);
            }
        }

        let ratio = if pages_in_groups > 0 {
            dead as f64 / pages_in_groups as f64
        } else {
            0.0
        };

        let name = manifest_entry.name.clone();
        if ratio >= threshold {
            candidates.push(name.clone());
        }

        total_live += live_pages_set.len();
        total_dead += dead;
        total_in_groups += pages_in_groups;

        btrees.push(BTreeDeadSpace {
            name,
            root_page,
            live_pages: live_pages_set.len(),
            dead_pages: dead,
            total_pages_in_groups: pages_in_groups,
            dead_ratio: ratio,
            affected_groups: affected,
        });
    }

    // Also count unowned pages as dead if they're in B-tree groups
    // (pages moved to freelist but still assigned to a B-tree's group)

    DeadSpaceReport {
        btrees,
        total_live,
        total_dead,
        total_in_groups,
        candidates,
    }
}

/// Compact a B-tree's page groups: read all live pages, dense-pack into new groups.
///
/// Returns the new group_pages assignments, new S3 keys, and old keys to GC.
/// The caller is responsible for updating the manifest and uploading.
pub fn compact_btree(
    manifest: &Manifest,
    btree_root: u64,
    ppg: u32,
    page_size: u32,
    read_page: &dyn Fn(u64) -> Option<Vec<u8>>,
) -> io::Result<CompactResult> {
    let walk = crate::btree_walker::walk_all_btrees(manifest.page_count, page_size, read_page);

    let walk_entry = walk.btrees.get(&btree_root).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("B-tree root {} not found", btree_root))
    })?;

    let manifest_entry = manifest.btrees.get(&btree_root).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("B-tree root {} not in manifest", btree_root))
    })?;

    let live_pages: HashSet<u64> = walk_entry.pages.iter().copied().collect();

    // Collect old group IDs and their S3 keys for GC
    let old_group_ids: Vec<u64> = manifest_entry.group_ids.clone();
    let old_keys: Vec<String> = old_group_ids.iter()
        .filter_map(|&gid| manifest.page_group_keys.get(gid as usize).cloned())
        .filter(|k| !k.is_empty())
        .collect();

    // Gather all live pages from this B-tree's groups, sorted
    let mut sorted_live: Vec<u64> = Vec::new();
    for &gid in &old_group_ids {
        let group_pages = manifest.group_page_nums(gid);
        for &pnum in group_pages.iter() {
            if live_pages.contains(&pnum) {
                sorted_live.push(pnum);
            }
        }
    }
    sorted_live.sort_unstable();
    sorted_live.dedup();

    // Dense-pack into new groups
    let new_groups: Vec<Vec<u64>> = sorted_live.chunks(ppg as usize)
        .map(|chunk| chunk.to_vec())
        .collect();

    Ok(CompactResult {
        btree_name: manifest_entry.name.clone(),
        old_group_ids,
        old_keys,
        new_groups,
        pages_before: manifest_entry.group_ids.iter()
            .map(|&gid| manifest.group_size(gid))
            .sum(),
        pages_after: sorted_live.len(),
    })
}

/// Result of compacting a single B-tree.
#[derive(Debug)]
pub struct CompactResult {
    pub btree_name: String,
    pub old_group_ids: Vec<u64>,
    pub old_keys: Vec<String>,
    /// New dense-packed page lists (one per new group).
    pub new_groups: Vec<Vec<u64>>,
    pub pages_before: usize,
    pub pages_after: usize,
}

impl CompactResult {
    pub fn pages_freed(&self) -> usize {
        self.pages_before.saturating_sub(self.pages_after)
    }
}

// =========================================================================
// Phase Drift-d: Override compaction
// =========================================================================

/// Result of compacting overrides for a single group.
#[derive(Debug)]
pub struct OverrideCompactResult {
    pub gid: u64,
    pub new_key: String,
    pub new_frame_table: Vec<FrameEntry>,
    pub encoded: Vec<u8>,
    pub replaced_keys: Vec<String>,
}

/// Auto-compact groups exceeding threshold.
pub(crate) fn auto_compact_overrides(
    storage: &StorageClient,
    shared_manifest: &ArcSwap<Manifest>,
    compaction_threshold: u32,
    compression_level: i32,
    #[cfg(feature = "zstd")] dictionary: Option<&[u8]>,
    encryption_key: Option<[u8; 32]>,
) -> io::Result<usize> {
    compact_overrides_inner(storage, shared_manifest, Some(compaction_threshold), compression_level,
        #[cfg(feature = "zstd")] dictionary, encryption_key)
}

/// Compact ALL groups with any overrides (manual trigger).
#[allow(dead_code)]
pub(crate) fn compact_all_overrides(
    storage: &StorageClient,
    shared_manifest: &ArcSwap<Manifest>,
    compression_level: i32,
    #[cfg(feature = "zstd")] dictionary: Option<&[u8]>,
    encryption_key: Option<[u8; 32]>,
) -> io::Result<usize> {
    compact_overrides_inner(storage, shared_manifest, None, compression_level,
        #[cfg(feature = "zstd")] dictionary, encryption_key)
}

fn compact_overrides_inner(
    storage: &StorageClient,
    shared_manifest: &ArcSwap<Manifest>,
    threshold: Option<u32>,
    compression_level: i32,
    #[cfg(feature = "zstd")] dictionary: Option<&[u8]>,
    encryption_key: Option<[u8; 32]>,
) -> io::Result<usize> {
    let manifest_snap = (**shared_manifest.load()).clone();
    let groups_to_compact: Vec<u64> = manifest_snap.subframe_overrides.iter()
        .enumerate()
        .filter(|(_, ovs)| {
            if ovs.is_empty() { return false; }
            match threshold {
                Some(t) => ovs.len() >= t as usize,
                None => true,
            }
        })
        .map(|(gid, _)| gid as u64)
        .collect();

    if groups_to_compact.is_empty() { return Ok(0); }

    #[cfg(feature = "zstd")]
    let encoder_dict = dictionary.map(|d| zstd::dict::EncoderDictionary::copy(d, compression_level));
    #[cfg(feature = "zstd")]
    let decoder_dict = dictionary.map(zstd::dict::DecoderDictionary::copy);

    let next_version = manifest_snap.version + 1;
    let mut uploads: Vec<(String, Vec<u8>)> = Vec::new();
    let mut all_replaced_keys: Vec<String> = Vec::new();
    let mut compaction_results: Vec<OverrideCompactResult> = Vec::new();

    for &gid in &groups_to_compact {
        match compact_override_group(
            gid, &manifest_snap, storage, next_version, compression_level,
            #[cfg(feature = "zstd")] encoder_dict.as_ref(),
            #[cfg(feature = "zstd")] decoder_dict.as_ref(),
            encryption_key.as_ref(),
        ) {
            Ok(result) => {
                uploads.push((result.new_key.clone(), result.encoded.clone()));
                all_replaced_keys.extend(result.replaced_keys.iter().cloned());
                compaction_results.push(result);
            }
            Err(e) => { eprintln!("[compact] group {} failed: {}", gid, e); }
        }
    }

    if compaction_results.is_empty() { return Ok(0); }

    storage.put_page_groups(&uploads)?;

    {
        let mut m = (**shared_manifest.load()).clone();
        for result in &compaction_results {
            while m.page_group_keys.len() <= result.gid as usize { m.page_group_keys.push(String::new()); }
            m.page_group_keys[result.gid as usize] = result.new_key.clone();
            while m.frame_tables.len() <= result.gid as usize { m.frame_tables.push(Vec::new()); }
            m.frame_tables[result.gid as usize] = result.new_frame_table.clone();
            if let Some(ovs) = m.subframe_overrides.get_mut(result.gid as usize) { ovs.clear(); }
        }
        m.version = next_version;
        shared_manifest.store(Arc::new(m));
    }

    let manifest_for_persist = (**shared_manifest.load()).clone();
    storage.put_manifest(&manifest_for_persist, &[])?;

    if !all_replaced_keys.is_empty() {
        let _ = storage.delete_page_groups(&all_replaced_keys);
    }

    Ok(compaction_results.len())
}

/// Compact a single group's overrides back into its base group.
pub(crate) fn compact_override_group(
    gid: u64,
    manifest: &Manifest,
    storage: &StorageClient,
    next_version: u64,
    compression_level: i32,
    #[cfg(feature = "zstd")] encoder_dict: Option<&zstd::dict::EncoderDictionary<'static>>,
    #[cfg(feature = "zstd")] decoder_dict: Option<&zstd::dict::DecoderDictionary<'static>>,
    encryption_key: Option<&[u8; 32]>,
) -> io::Result<OverrideCompactResult> {
    let page_size = manifest.page_size;
    let sub_ppf = manifest.sub_pages_per_frame;
    let pages_in_group = manifest.group_page_nums(gid);
    let group_size = pages_in_group.len();

    let overrides = manifest.subframe_overrides.get(gid as usize)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, format!("no overrides for gid={}", gid)))?;
    if overrides.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, format!("empty overrides for gid={}", gid)));
    }

    let base_key = manifest.page_group_keys.get(gid as usize)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("no key for gid={}", gid)))?;
    let base_data = storage.get_page_group(base_key)?
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("base group not found: {}", base_key)))?;

    let ft = manifest.frame_tables.get(gid as usize);
    let has_ft = sub_ppf > 0 && ft.map(|f| !f.is_empty()).unwrap_or(false);

    let mut page_buffers: Vec<Option<Vec<u8>>> = vec![None; group_size];

    if has_ft {
        let (_pc, _ps, bulk_data) = decode_page_group_seekable_full(
            &base_data, ft.expect("checked"), page_size, group_size as u32,
            manifest.page_count, 0,
            #[cfg(feature = "zstd")] decoder_dict, encryption_key,
        )?;
        let ps = page_size as usize;
        for i in 0..group_size {
            let start = i * ps;
            let end = start + ps;
            if end <= bulk_data.len() { page_buffers[i] = Some(bulk_data[start..end].to_vec()); }
        }
    } else {
        let (_pc, _ps, pages) = decode_page_group(
            &base_data,
            #[cfg(feature = "zstd")] decoder_dict, encryption_key,
        )?;
        for (i, page) in pages.into_iter().enumerate() {
            if i < group_size { page_buffers[i] = Some(page); }
        }
    }

    let mut replaced_keys: Vec<String> = vec![base_key.clone()];

    for (&frame_idx, ovr) in overrides {
        replaced_keys.push(ovr.key.clone());
        let ovr_data = storage.get_page_group(&ovr.key)?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("override not found: {}", ovr.key)))?;
        let decompressed = decode_seekable_subchunk(
            &ovr_data,
            #[cfg(feature = "zstd")] decoder_dict, encryption_key,
        )?;
        let frame_start = frame_idx * sub_ppf as usize;
        let frame_end = std::cmp::min(frame_start + sub_ppf as usize, group_size);
        let ps = page_size as usize;
        for pos in frame_start..frame_end {
            let offset_in_frame = (pos - frame_start) * ps;
            let end = offset_in_frame + ps;
            if end <= decompressed.len() { page_buffers[pos] = Some(decompressed[offset_in_frame..end].to_vec()); }
        }
    }

    let new_key = StorageClient::page_group_key(gid, next_version);

    if sub_ppf > 0 {
        let (encoded, new_ft) = encode_page_group_seekable(
            &page_buffers, page_size, sub_ppf, compression_level,
            #[cfg(feature = "zstd")] encoder_dict, encryption_key,
        )?;
        Ok(OverrideCompactResult { gid, new_key, new_frame_table: new_ft, encoded, replaced_keys })
    } else {
        let encoded = encode_page_group(
            &page_buffers, page_size, compression_level,
            #[cfg(feature = "zstd")] encoder_dict, encryption_key,
        )?;
        Ok(OverrideCompactResult { gid, new_key, new_frame_table: Vec::new(), encoded, replaced_keys })
    }
}
