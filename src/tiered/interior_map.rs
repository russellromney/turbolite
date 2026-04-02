//! Phase Jena: Interior page introspection for precise prefetch.
//!
//! Interior B-tree pages are always cached (pinned on open). They contain
//! child pointers to leaf pages. By parsing these pointers, we can predict
//! exact leaf groups for any query without guessing.
//!
//! The `InteriorMap` replaces the hop-schedule heuristic with direct
//! structural knowledge of the B-tree.

use std::collections::HashMap;

use super::*;
use super::record_parser::{self, SqliteValue};

/// Key boundary between two child pages in an interior page.
/// For table interiors (0x05): key is a single integer rowid.
/// For index interiors (0x02): key is a composite record (multi-column).
///
/// The key K between child[i] and child[i+1] means:
/// - child[i] contains rows with key < K
/// - child[i+1] contains rows with key >= K
#[derive(Debug, Clone)]
pub(crate) struct KeyBoundary {
    /// The key values (one for table interior, multiple for composite index).
    pub values: Vec<SqliteValue>,
}

/// Parsed interior page structure for all B-trees.
/// Maps child pages to their parent interior page, and interior pages
/// to their ordered list of children with key boundaries.
#[derive(Debug, Clone, Default)]
pub(crate) struct InteriorMap {
    /// child_page (0-based) -> parent interior page (0-based).
    /// Includes both leaf and interior children.
    pub child_to_parent: HashMap<u64, u64>,

    /// interior_page (0-based) -> ordered list of child pages (0-based).
    /// Children are in B-tree key order (left to right).
    pub parent_to_children: HashMap<u64, Vec<u64>>,

    /// interior_page (0-based) -> key boundaries between children.
    /// boundaries[i] is the key between children[i] and children[i+1].
    /// Length = children.len() - 1 (no boundary after the rightmost child).
    pub key_boundaries: HashMap<u64, Vec<KeyBoundary>>,

    /// interior_page (0-based) -> true if table interior (0x05), false if index (0x02).
    pub is_table_interior: HashMap<u64, bool>,

    /// page (0-based) -> group_id. Cached for fast lookup during prefetch.
    /// Only populated for pages that appear in the map.
    pub page_to_group: HashMap<u64, u64>,
}

impl InteriorMap {
    /// Build an InteriorMap by parsing all cached interior pages.
    ///
    /// Reads interior page data from the disk cache, extracts child pointers,
    /// and builds parent/child relationships. Cost: ~100us for 15 interior
    /// pages (~4500 cells at 1M rows).
    pub fn build(cache: &DiskCache, manifest: &Manifest) -> Self {
        let interior_pages: Vec<u64> = cache.interior_pages.lock().iter().copied().collect();
        if interior_pages.is_empty() {
            return Self::default();
        }

        let page_size = manifest.page_size as usize;
        if page_size == 0 {
            return Self::default();
        }

        let mut child_to_parent: HashMap<u64, u64> = HashMap::new();
        let mut parent_to_children: HashMap<u64, Vec<u64>> = HashMap::new();
        let mut key_boundaries_map: HashMap<u64, Vec<KeyBoundary>> = HashMap::new();
        let mut is_table_interior_map: HashMap<u64, bool> = HashMap::new();
        let mut page_to_group: HashMap<u64, u64> = HashMap::new();

        let mut buf = vec![0u8; page_size];

        for &interior_page in &interior_pages {
            if cache.read_page(interior_page, &mut buf).is_err() {
                continue;
            }

            let hdr_off = if interior_page == 0 { 100 } else { 0 };
            let type_byte = buf.get(hdr_off).copied().unwrap_or(0);

            // Only parse interior pages (0x05 = table interior, 0x02 = index interior)
            let is_table = type_byte == 0x05;
            let is_index = type_byte == 0x02;
            if !is_table && !is_index {
                continue;
            }

            is_table_interior_map.insert(interior_page, is_table);

            // Extract children AND key boundaries from cells.
            // Each cell gives a left child pointer + key data.
            // The right child pointer comes from the page header.
            let cell_offs = record_parser::cell_offsets(&buf, hdr_off);
            let right_child = if buf.len() >= hdr_off + 12 {
                u32::from_be_bytes([buf[hdr_off + 8], buf[hdr_off + 9], buf[hdr_off + 10], buf[hdr_off + 11]])
            } else {
                0
            };

            let mut children_0based: Vec<u64> = Vec::with_capacity(cell_offs.len() + 1);
            let mut boundaries: Vec<KeyBoundary> = Vec::with_capacity(cell_offs.len());

            for &cell_off in &cell_offs {
                if let Some((left_child, key_values)) = record_parser::parse_interior_cell(&buf[cell_off..], is_table) {
                    if left_child > 0 {
                        children_0based.push((left_child - 1) as u64);
                    }
                    boundaries.push(KeyBoundary { values: key_values });
                }
            }
            // Right child (after all keys)
            if right_child > 0 {
                children_0based.push((right_child - 1) as u64);
            }

            if children_0based.is_empty() {
                continue;
            }

            for &child in &children_0based {
                child_to_parent.insert(child, interior_page);
            }
            parent_to_children.insert(interior_page, children_0based.clone());
            key_boundaries_map.insert(interior_page, boundaries);

            // Map interior page itself to its group
            if let Some(loc) = manifest.page_location(interior_page) {
                page_to_group.insert(interior_page, loc.group_id);
            }

            // Map children to their groups
            for &child in &children_0based {
                if let Some(loc) = manifest.page_location(child) {
                    page_to_group.insert(child, loc.group_id);
                }
            }
        }

        InteriorMap {
            child_to_parent,
            parent_to_children,
            key_boundaries: key_boundaries_map,
            is_table_interior: is_table_interior_map,
            page_to_group,
        }
    }

    /// Find sibling pages of a given page (pages sharing the same parent).
    /// Returns pages in B-tree key order, excluding the given page itself.
    pub fn siblings(&self, page: u64) -> Vec<u64> {
        let parent = match self.child_to_parent.get(&page) {
            Some(&p) => p,
            None => return Vec::new(),
        };
        match self.parent_to_children.get(&parent) {
            Some(children) => children.iter().copied().filter(|&p| p != page).collect(),
            None => Vec::new(),
        }
    }

    /// Find sibling GROUP IDs of a given page's group.
    /// Returns unique group IDs in B-tree order, excluding the given page's group.
    pub fn sibling_groups(&self, page: u64) -> Vec<u64> {
        let my_group = self.page_to_group.get(&page).copied();
        let siblings = self.siblings(page);
        let mut groups: Vec<u64> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        if let Some(g) = my_group {
            seen.insert(g);
        }
        for sib in &siblings {
            if let Some(&gid) = self.page_to_group.get(sib) {
                if seen.insert(gid) {
                    groups.push(gid);
                }
            }
        }
        groups
    }

    /// Find ALL leaf groups under a given interior page (recursive).
    /// Useful for SCAN: prefetch every leaf group in this subtree.
    pub fn subtree_groups(&self, interior_page: u64) -> Vec<u64> {
        let mut groups = Vec::new();
        let mut seen = std::collections::HashSet::new();
        self.collect_subtree_groups(interior_page, &mut groups, &mut seen);
        groups
    }

    fn collect_subtree_groups(
        &self,
        page: u64,
        groups: &mut Vec<u64>,
        seen: &mut std::collections::HashSet<u64>,
    ) {
        if let Some(children) = self.parent_to_children.get(&page) {
            for &child in children {
                if self.parent_to_children.contains_key(&child) {
                    // Child is also an interior page: recurse
                    self.collect_subtree_groups(child, groups, seen);
                } else {
                    // Child is a leaf page: add its group
                    if let Some(&gid) = self.page_to_group.get(&child) {
                        if seen.insert(gid) {
                            groups.push(gid);
                        }
                    }
                }
            }
        }
    }

    /// Total number of interior pages in the map.
    pub fn interior_count(&self) -> usize {
        self.parent_to_children.len()
    }

    /// Total number of child relationships.
    pub fn child_count(&self) -> usize {
        self.child_to_parent.len()
    }

    /// Check if the map is empty (no interior pages parsed).
    pub fn is_empty(&self) -> bool {
        self.parent_to_children.is_empty()
    }

    /// Predict the exact leaf page (and its group) for a given search key,
    /// starting from a root interior page.
    ///
    /// Binary searches key boundaries at each level of the B-tree to find
    /// the child that contains the target key. Descends through interior
    /// pages until reaching a leaf.
    ///
    /// Returns `Some((leaf_page, group_id))` if prediction succeeds.
    /// Returns `None` if the root has no boundaries, or the tree structure
    /// is incomplete (child page not in the map).
    pub fn predict_leaf(&self, root_page: u64, search_key: &[SqliteValue]) -> Option<(u64, u64)> {
        let mut current = root_page;

        // Descend through interior pages
        loop {
            let children = self.parent_to_children.get(&current)?;
            let boundaries = self.key_boundaries.get(&current)?;
            let is_table = self.is_table_interior.get(&current).copied().unwrap_or(true);

            if children.is_empty() {
                return None;
            }

            // Binary search: find the child index for the search key.
            // boundaries[i] is the key between children[i] and children[i+1].
            // We want the largest i such that search_key >= boundaries[i].
            // If search_key < boundaries[0], we go to children[0].
            let child_idx = self.binary_search_child(boundaries, search_key, is_table);
            let child_idx = std::cmp::min(child_idx, children.len() - 1);
            let child = children[child_idx];

            // If the child is an interior page, descend
            if self.parent_to_children.contains_key(&child) {
                current = child;
                continue;
            }

            // Child is a leaf page: return its group
            let gid = self.page_to_group.get(&child).copied()?;
            return Some((child, gid));
        }
    }

    /// Binary search key boundaries to find the child index for a search key.
    ///
    /// Returns the index into children[] where the search key belongs.
    /// - If search_key < boundaries[0], returns 0 (leftmost child)
    /// - If search_key >= boundaries[i], returns i+1
    /// - If search_key >= boundaries[last], returns children.len()-1 (rightmost)
    fn binary_search_child(
        &self,
        boundaries: &[KeyBoundary],
        search_key: &[SqliteValue],
        _is_table: bool,
    ) -> usize {
        if boundaries.is_empty() {
            return 0;
        }

        // Compare search_key against boundary keys (composite comparison).
        // For table interiors: single integer comparison.
        // For index interiors: column-by-column comparison.
        let mut lo = 0usize;
        let mut hi = boundaries.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let cmp = compare_keys(search_key, &boundaries[mid].values);
            if cmp == std::cmp::Ordering::Less {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        lo
    }
}

/// Compare two composite keys column-by-column using BINARY collation.
/// Shorter key is a prefix match (equal up to its length).
fn compare_keys(a: &[SqliteValue], b: &[SqliteValue]) -> std::cmp::Ordering {
    for (av, bv) in a.iter().zip(b.iter()) {
        let cmp = av.cmp_binary(bv);
        if cmp != std::cmp::Ordering::Equal {
            return cmp;
        }
    }
    // Prefix match: equal up to the shorter length
    a.len().cmp(&b.len())
}

/// Extract child page numbers from an interior page buffer.
/// Returns 1-based page numbers (SQLite convention).
/// Used only in tests; production uses parse_interior_cell for key extraction.
#[cfg(test)]
fn extract_children(buf: &[u8], hdr_off: usize) -> Vec<u32> {
    if buf.len() < hdr_off + 12 {
        return Vec::new();
    }

    let cell_count = u16::from_be_bytes([buf[hdr_off + 3], buf[hdr_off + 4]]) as usize;

    // Right-most child pointer (4 bytes at hdr_off + 8)
    let right_child = u32::from_be_bytes([
        buf[hdr_off + 8],
        buf[hdr_off + 9],
        buf[hdr_off + 10],
        buf[hdr_off + 11],
    ]);

    let mut children = Vec::with_capacity(cell_count + 1);

    // Cell pointer array starts at hdr_off + 12
    let cell_ptr_start = hdr_off + 12;
    for i in 0..cell_count {
        let ptr_off = cell_ptr_start + i * 2;
        if ptr_off + 2 > buf.len() {
            break;
        }
        let cell_off = u16::from_be_bytes([buf[ptr_off], buf[ptr_off + 1]]) as usize;
        // Each interior cell starts with a 4-byte left child page number
        if cell_off + 4 > buf.len() {
            continue;
        }
        let left_child = u32::from_be_bytes([
            buf[cell_off],
            buf[cell_off + 1],
            buf[cell_off + 2],
            buf[cell_off + 3],
        ]);
        children.push(left_child);
    }

    // Right child is the "after last key" child
    children.push(right_child);

    children
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a fake interior page buffer (type 0x05, table interior).
    fn make_interior_page(page_size: usize, children: &[u32], right_child: u32) -> Vec<u8> {
        let mut buf = vec![0u8; page_size];
        let hdr_off = 0;

        // Type byte
        buf[hdr_off] = 0x05;
        // Cell count
        let cell_count = children.len() as u16;
        buf[hdr_off + 3] = (cell_count >> 8) as u8;
        buf[hdr_off + 4] = cell_count as u8;
        // Right child
        let rc = right_child.to_be_bytes();
        buf[hdr_off + 8..hdr_off + 12].copy_from_slice(&rc);

        // Cell pointer array + cells
        // Each cell: 4-byte left child + minimal key payload
        let cell_ptr_start = hdr_off + 12;
        let mut cell_data_offset = cell_ptr_start + children.len() * 2 + 16; // some padding

        for (i, &child) in children.iter().enumerate() {
            // Write cell pointer
            let ptr_off = cell_ptr_start + i * 2;
            let cell_ptr = (cell_data_offset as u16).to_be_bytes();
            buf[ptr_off] = cell_ptr[0];
            buf[ptr_off + 1] = cell_ptr[1];

            // Write cell: 4-byte left child + 1-byte key
            let child_bytes = child.to_be_bytes();
            buf[cell_data_offset..cell_data_offset + 4].copy_from_slice(&child_bytes);
            buf[cell_data_offset + 4] = 0x01; // minimal key
            cell_data_offset += 8; // 4 bytes child + padding
        }

        buf
    }

    #[test]
    fn test_extract_children_basic() {
        let page = make_interior_page(4096, &[2, 3, 4], 5);
        let children = extract_children(&page, 0);
        // Should have left children [2, 3, 4] + right child [5]
        assert_eq!(children, vec![2, 3, 4, 5]);
    }

    #[test]
    fn test_extract_children_single() {
        let page = make_interior_page(4096, &[2], 3);
        let children = extract_children(&page, 0);
        assert_eq!(children, vec![2, 3]);
    }

    #[test]
    fn test_extract_children_empty_page() {
        let buf = vec![0u8; 100];
        let children = extract_children(&buf, 0);
        // Cell count = 0, right child = 0
        assert_eq!(children, vec![0]); // just the right child (page 0)
    }

    #[test]
    fn test_interior_map_siblings() {
        let mut map = InteriorMap::default();
        // Interior page 10 has children [20, 21, 22]
        map.parent_to_children.insert(10, vec![20, 21, 22]);
        map.child_to_parent.insert(20, 10);
        map.child_to_parent.insert(21, 10);
        map.child_to_parent.insert(22, 10);

        assert_eq!(map.siblings(20), vec![21, 22]);
        assert_eq!(map.siblings(21), vec![20, 22]);
        assert_eq!(map.siblings(22), vec![20, 21]);
        assert!(map.siblings(99).is_empty()); // unknown page
    }

    #[test]
    fn test_interior_map_sibling_groups() {
        let mut map = InteriorMap::default();
        map.parent_to_children.insert(10, vec![20, 21, 22, 23]);
        map.child_to_parent.insert(20, 10);
        map.child_to_parent.insert(21, 10);
        map.child_to_parent.insert(22, 10);
        map.child_to_parent.insert(23, 10);
        // Pages 20, 21 in group 0; pages 22, 23 in group 1
        map.page_to_group.insert(20, 0);
        map.page_to_group.insert(21, 0);
        map.page_to_group.insert(22, 1);
        map.page_to_group.insert(23, 1);

        // Siblings of page 20 (group 0): should return group 1
        let groups = map.sibling_groups(20);
        assert_eq!(groups, vec![1]);

        // Siblings of page 22 (group 1): should return group 0
        let groups = map.sibling_groups(22);
        assert_eq!(groups, vec![0]);
    }

    #[test]
    fn test_interior_map_subtree_groups() {
        let mut map = InteriorMap::default();
        // Two-level tree:
        // Root (page 5) -> interior pages [10, 11]
        // Interior 10 -> leaf pages [20, 21, 22]
        // Interior 11 -> leaf pages [23, 24, 25]
        map.parent_to_children.insert(5, vec![10, 11]);
        map.parent_to_children.insert(10, vec![20, 21, 22]);
        map.parent_to_children.insert(11, vec![23, 24, 25]);
        map.child_to_parent.insert(10, 5);
        map.child_to_parent.insert(11, 5);
        for p in 20..=25 {
            map.child_to_parent.insert(p, if p < 23 { 10 } else { 11 });
        }
        // Groups: 20-22 in group 2, 23-25 in group 3
        for p in 20..=22 { map.page_to_group.insert(p, 2); }
        for p in 23..=25 { map.page_to_group.insert(p, 3); }

        let groups = map.subtree_groups(5);
        assert_eq!(groups, vec![2, 3]);

        let groups = map.subtree_groups(10);
        assert_eq!(groups, vec![2]);

        let groups = map.subtree_groups(11);
        assert_eq!(groups, vec![3]);
    }

    #[test]
    fn test_predict_leaf_single_level() {
        // Table interior with 3 children and 2 key boundaries:
        // child[0]=page 10 | key=50 | child[1]=page 11 | key=100 | child[2]=page 12
        let mut map = InteriorMap::default();
        map.parent_to_children.insert(0, vec![10, 11, 12]);
        map.key_boundaries.insert(0, vec![
            KeyBoundary { values: vec![SqliteValue::Integer(50)] },
            KeyBoundary { values: vec![SqliteValue::Integer(100)] },
        ]);
        map.is_table_interior.insert(0, true);
        map.page_to_group.insert(10, 1);
        map.page_to_group.insert(11, 2);
        map.page_to_group.insert(12, 3);

        // Key 25 < 50 -> child[0] = page 10, group 1
        assert_eq!(map.predict_leaf(0, &[SqliteValue::Integer(25)]), Some((10, 1)));
        // Key 50 >= 50 -> child[1] = page 11, group 2
        assert_eq!(map.predict_leaf(0, &[SqliteValue::Integer(50)]), Some((11, 2)));
        // Key 75 >= 50, < 100 -> child[1] = page 11, group 2
        assert_eq!(map.predict_leaf(0, &[SqliteValue::Integer(75)]), Some((11, 2)));
        // Key 100 >= 100 -> child[2] = page 12, group 3
        assert_eq!(map.predict_leaf(0, &[SqliteValue::Integer(100)]), Some((12, 3)));
        // Key 999 >= 100 -> child[2] = page 12, group 3
        assert_eq!(map.predict_leaf(0, &[SqliteValue::Integer(999)]), Some((12, 3)));
    }

    #[test]
    fn test_predict_leaf_two_levels() {
        // Root (page 0) -> interior [5, 6]
        // Interior 5 -> leaf [10, 11] with boundary key=50
        // Interior 6 -> leaf [12, 13] with boundary key=150
        // Root boundary key=100 (between interior 5 and 6)
        let mut map = InteriorMap::default();
        map.parent_to_children.insert(0, vec![5, 6]);
        map.key_boundaries.insert(0, vec![
            KeyBoundary { values: vec![SqliteValue::Integer(100)] },
        ]);
        map.is_table_interior.insert(0, true);

        map.parent_to_children.insert(5, vec![10, 11]);
        map.key_boundaries.insert(5, vec![
            KeyBoundary { values: vec![SqliteValue::Integer(50)] },
        ]);
        map.is_table_interior.insert(5, true);
        map.child_to_parent.insert(5, 0);

        map.parent_to_children.insert(6, vec![12, 13]);
        map.key_boundaries.insert(6, vec![
            KeyBoundary { values: vec![SqliteValue::Integer(150)] },
        ]);
        map.is_table_interior.insert(6, true);
        map.child_to_parent.insert(6, 0);

        map.page_to_group.insert(10, 1);
        map.page_to_group.insert(11, 2);
        map.page_to_group.insert(12, 3);
        map.page_to_group.insert(13, 4);

        // Key 25: root -> 5 (< 100) -> 10 (< 50) -> group 1
        assert_eq!(map.predict_leaf(0, &[SqliteValue::Integer(25)]), Some((10, 1)));
        // Key 75: root -> 5 (< 100) -> 11 (>= 50) -> group 2
        assert_eq!(map.predict_leaf(0, &[SqliteValue::Integer(75)]), Some((11, 2)));
        // Key 125: root -> 6 (>= 100) -> 12 (< 150) -> group 3
        assert_eq!(map.predict_leaf(0, &[SqliteValue::Integer(125)]), Some((12, 3)));
        // Key 200: root -> 6 (>= 100) -> 13 (>= 150) -> group 4
        assert_eq!(map.predict_leaf(0, &[SqliteValue::Integer(200)]), Some((13, 4)));
    }

    #[test]
    fn test_predict_leaf_composite_key() {
        // Index interior with composite key (name, age)
        // child[0] | ("Alice", 30) | child[1] | ("Bob", 25) | child[2]
        let mut map = InteriorMap::default();
        map.parent_to_children.insert(0, vec![10, 11, 12]);
        map.key_boundaries.insert(0, vec![
            KeyBoundary { values: vec![SqliteValue::Text("Alice".into()), SqliteValue::Integer(30)] },
            KeyBoundary { values: vec![SqliteValue::Text("Bob".into()), SqliteValue::Integer(25)] },
        ]);
        map.is_table_interior.insert(0, false);
        map.page_to_group.insert(10, 1);
        map.page_to_group.insert(11, 2);
        map.page_to_group.insert(12, 3);

        // "Aaron" < "Alice" -> child[0]
        assert_eq!(
            map.predict_leaf(0, &[SqliteValue::Text("Aaron".into()), SqliteValue::Integer(0)]),
            Some((10, 1)),
        );
        // "Alice", 30 >= ("Alice", 30) -> child[1]
        assert_eq!(
            map.predict_leaf(0, &[SqliteValue::Text("Alice".into()), SqliteValue::Integer(30)]),
            Some((11, 2)),
        );
        // "Charlie" >= "Bob" -> child[2]
        assert_eq!(
            map.predict_leaf(0, &[SqliteValue::Text("Charlie".into()), SqliteValue::Integer(0)]),
            Some((12, 3)),
        );
    }
}
