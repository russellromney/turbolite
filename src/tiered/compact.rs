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
