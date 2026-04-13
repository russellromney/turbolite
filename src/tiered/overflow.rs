//! Phase Jena-e: Overflow chain prefetch.
//!
//! When a leaf page contains cells with overflow (payload > maxLocal),
//! the overflow data spills to linked pages via 4-byte next-page pointers.
//! Each link is a separate page read, causing sequential blocking faults
//! for multi-MB TEXT/BLOB values.
//!
//! This module detects overflow on leaf page read and prefetches the
//! overflow chain proactively. The chain is: first overflow page number
//! is embedded in the cell, each overflow page's first 4 bytes point to
//! the next page (0 = end of chain).
//!
//! SQLite overflow thresholds:
//! - Table leaf (0x0D): maxLocal = usableSize - 35 (for 64KB: 65501 bytes)
//! - Index leaf (0x0A): maxLocal = ((usableSize - 12) * 64 / 255) - 23

use super::*;
use super::record_parser;

/// Detect overflow pages in a leaf page and return their page numbers (0-based).
///
/// Scans all cells. For cells where payload > maxLocal, extracts the first
/// overflow page number from the cell. Does NOT follow the chain (that requires
/// reading the overflow pages, which is what prefetch will do).
///
/// Returns deduplicated 0-based page numbers of first overflow pages.
pub(crate) fn detect_overflow_pages(
    page: &[u8],
    page_num: u64,
    page_size: u32,
) -> Vec<u64> {
    let hdr_off = if page_num == 0 { 100 } else { 0 };
    let type_byte = page.get(hdr_off).copied().unwrap_or(0);

    let is_table_leaf = type_byte == 0x0D;
    let is_index_leaf = type_byte == 0x0A;
    if !is_table_leaf && !is_index_leaf {
        return Vec::new();
    }

    let usable_size = page_size as usize; // assuming no reserved bytes

    // SQLite overflow thresholds
    let max_local = if is_table_leaf {
        usable_size - 35
    } else {
        // Index leaf: ((U - 12) * 64 / 255) - 23
        ((usable_size - 12) * 64 / 255) - 23
    };

    let cell_offs = record_parser::cell_offsets(page, hdr_off);
    let mut overflow_pages = Vec::new();

    for &cell_off in &cell_offs {
        let cell_data = &page[cell_off..];

        // Read payload size varint
        let (payload_size, ps_len) = match record_parser::read_varint(cell_data) {
            Some(v) => v,
            None => continue,
        };

        // For table leaf: skip rowid varint after payload size
        let header_len = if is_table_leaf {
            let (_, rid_len) = match record_parser::read_varint(&cell_data[ps_len..]) {
                Some(v) => v,
                None => continue,
            };
            ps_len + rid_len
        } else {
            ps_len
        };

        let payload_size = payload_size as usize;
        if payload_size <= max_local {
            continue; // no overflow
        }

        // Calculate local payload size (how much fits on this page)
        let min_local = if is_table_leaf {
            // Table leaf: minLocal = (U - 12) * 32 / 255 - 23
            ((usable_size - 12) * 32 / 255) - 23
        } else {
            // Index leaf: minLocal = ((U - 12) * 32 / 255) - 23
            ((usable_size - 12) * 32 / 255) - 23
        };

        let local_size = if min_local + (payload_size - min_local) % (usable_size - 4) <= max_local {
            min_local + (payload_size - min_local) % (usable_size - 4)
        } else {
            min_local
        };

        // The first overflow page number is at cell_data[header_len + local_size .. +4]
        let ovfl_ptr_offset = header_len + local_size;
        if ovfl_ptr_offset + 4 > cell_data.len() {
            continue;
        }

        let first_overflow = u32::from_be_bytes([
            cell_data[ovfl_ptr_offset],
            cell_data[ovfl_ptr_offset + 1],
            cell_data[ovfl_ptr_offset + 2],
            cell_data[ovfl_ptr_offset + 3],
        ]);

        if first_overflow > 0 {
            overflow_pages.push((first_overflow - 1) as u64); // 0-based
        }
    }

    overflow_pages.sort_unstable();
    overflow_pages.dedup();
    overflow_pages
}

/// Follow an overflow chain from a page already in cache.
/// Returns all overflow page numbers in the chain (0-based), up to max_depth.
///
/// Each overflow page's first 4 bytes contain the next page number (1-based, 0 = end).
/// The rest is overflow payload data.
#[allow(dead_code)]
pub(crate) fn follow_overflow_chain(
    first_page: u64,
    cache: &DiskCache,
    page_size: u32,
    max_depth: usize,
) -> Vec<u64> {
    let mut chain = Vec::new();
    let mut current = first_page;
    let mut buf = vec![0u8; page_size as usize];

    for _ in 0..max_depth {
        if cache.read_page(current, &mut buf).is_err() {
            break; // page not in cache yet, stop here
        }
        if buf.len() < 4 {
            break;
        }
        let next = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if next == 0 {
            break; // end of chain
        }
        let next_0based = (next - 1) as u64;
        chain.push(next_0based);
        current = next_0based;
    }

    chain
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_overflow_small_payload() {
        // A leaf page with small cells (no overflow)
        let page_size = 4096u32;
        let mut page = vec![0u8; page_size as usize];
        page[0] = 0x0D; // table leaf
        // cell_count = 0 (no cells)
        let overflow = detect_overflow_pages(&page, 1, page_size);
        assert!(overflow.is_empty());
    }

    #[test]
    fn test_overflow_chain_not_in_cache() {
        // follow_overflow_chain returns empty when page not in cache
        let dir = tempfile::tempdir().unwrap();
        let cache = DiskCache::new(
            dir.path(), 3600, 256, 4, 4096, 0, None, Vec::new(),
        ).unwrap();
        let chain = follow_overflow_chain(42, &cache, 4096, 64);
        assert!(chain.is_empty());
    }
}
