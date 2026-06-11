//! Advisory index-leaf lookahead state and rowid resolution helpers.

use std::collections::HashMap;
use std::io;
use std::sync::atomic::Ordering;

use super::btree_peek::parse_table_interior_cells;
use super::DiskCache;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RowidResolution {
    pub(crate) rowid: i64,
    pub(crate) page_num: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RowidPathResolution {
    pub(crate) rowid: i64,
    pub(crate) page_num: Option<u64>,
    pub(crate) path: Vec<u64>,
}

pub(crate) fn resolve_rowids_to_pages(
    table_root_page: u64,
    rowids: &[i64],
    cache: &DiskCache,
) -> Vec<RowidResolution> {
    let mut memo = HashMap::new();
    rowids
        .iter()
        .copied()
        .map(|rowid| RowidResolution {
            rowid,
            page_num: resolve_rowid_to_page(table_root_page, rowid, cache, &mut memo)
                .ok()
                .flatten(),
        })
        .collect()
}

pub(crate) fn resolve_rowids_to_pages_with_paths(
    table_root_page: u64,
    rowids: &[i64],
    cache: &DiskCache,
) -> Vec<RowidPathResolution> {
    rowids
        .iter()
        .copied()
        .map(
            |rowid| match resolve_rowid_to_page_with_path(table_root_page, rowid, cache) {
                Ok((page_num, path)) => RowidPathResolution {
                    rowid,
                    page_num,
                    path,
                },
                Err(_) => RowidPathResolution {
                    rowid,
                    page_num: None,
                    path: Vec::new(),
                },
            },
        )
        .collect()
}

fn resolve_rowid_to_page(
    table_root_page: u64,
    rowid: i64,
    cache: &DiskCache,
    memo: &mut HashMap<(u64, i64), Option<u64>>,
) -> io::Result<Option<u64>> {
    let mut page_num = table_root_page;
    let page_size = cache.page_size.load(Ordering::Acquire) as usize;
    if page_size == 0 {
        return Ok(None);
    }
    let mut page = vec![0u8; page_size];

    for _depth in 0..64 {
        if let Some(resolved) = memo.get(&(page_num, rowid)).copied() {
            return Ok(resolved);
        }
        if !cache.is_present(page_num) {
            memo.insert((page_num, rowid), None);
            return Ok(None);
        }
        cache.read_page(page_num, &mut page)?;
        let hdr_off = if page_num == 0 { 100 } else { 0 };
        match page.get(hdr_off).copied() {
            Some(0x0d) => {
                memo.insert((page_num, rowid), Some(page_num));
                return Ok(Some(page_num));
            }
            Some(0x05) => {
                let interior = parse_table_interior_cells(&page, page_num == 0);
                let Some(child_1based) = choose_table_child(&interior, rowid) else {
                    memo.insert((page_num, rowid), None);
                    return Ok(None);
                };
                if child_1based == 0 {
                    memo.insert((page_num, rowid), None);
                    return Ok(None);
                }
                let child_page = u64::from(child_1based - 1);
                if !cache.is_present(child_page) {
                    let inferred = parent_has_cached_table_leaf_child(&interior, cache, page_size)
                        .then_some(child_page);
                    memo.insert((page_num, rowid), inferred);
                    return Ok(inferred);
                }
                page_num = child_page;
            }
            _ => {
                memo.insert((page_num, rowid), None);
                return Ok(None);
            }
        }
    }

    Ok(None)
}

fn resolve_rowid_to_page_with_path(
    table_root_page: u64,
    rowid: i64,
    cache: &DiskCache,
) -> io::Result<(Option<u64>, Vec<u64>)> {
    let mut page_num = table_root_page;
    let page_size = cache.page_size.load(Ordering::Acquire) as usize;
    if page_size == 0 {
        return Ok((None, Vec::new()));
    }
    let mut page = vec![0u8; page_size];
    let mut path = Vec::new();

    for _depth in 0..64 {
        path.push(page_num);
        if !cache.is_present(page_num) {
            return Ok((None, path));
        }
        cache.read_page(page_num, &mut page)?;
        let hdr_off = if page_num == 0 { 100 } else { 0 };
        match page.get(hdr_off).copied() {
            Some(0x0d) => return Ok((Some(page_num), path)),
            Some(0x05) => {
                let interior = parse_table_interior_cells(&page, page_num == 0);
                let Some(child_1based) = choose_table_child(&interior, rowid) else {
                    return Ok((None, path));
                };
                if child_1based == 0 {
                    return Ok((None, path));
                }
                let child_page = u64::from(child_1based - 1);
                if !cache.is_present(child_page) {
                    path.push(child_page);
                    let inferred = parent_has_cached_table_leaf_child(&interior, cache, page_size)
                        .then_some(child_page);
                    return Ok((inferred, path));
                }
                page_num = child_page;
            }
            _ => return Ok((None, path)),
        }
    }

    Ok((None, path))
}

fn choose_table_child(interior: &super::btree_peek::TableInteriorCells, rowid: i64) -> Option<u32> {
    for cell in &interior.cells {
        if rowid <= cell.rowid_key {
            return Some(cell.child_page);
        }
    }
    interior.rightmost_child_page
}

fn parent_has_cached_table_leaf_child(
    interior: &super::btree_peek::TableInteriorCells,
    cache: &DiskCache,
    page_size: usize,
) -> bool {
    let mut child_page_nums: Vec<u64> = interior
        .cells
        .iter()
        .filter_map(|cell| cell.child_page.checked_sub(1).map(u64::from))
        .collect();
    if let Some(rightmost) = interior.rightmost_child_page {
        if let Some(page_num) = rightmost.checked_sub(1).map(u64::from) {
            child_page_nums.push(page_num);
        }
    }

    let mut child_page = vec![0u8; page_size];
    for child_page_num in child_page_nums {
        if !cache.is_present(child_page_num) {
            continue;
        }
        if cache.read_page(child_page_num, &mut child_page).is_err() {
            continue;
        }
        let hdr_off = if child_page_num == 0 { 100 } else { 0 };
        if child_page.get(hdr_off).copied() == Some(0x0d) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn read_db_page(file: &[u8], page_size: usize, page_num: u64) -> Option<&[u8]> {
        let start = page_num as usize * page_size;
        file.get(start..start + page_size)
    }

    fn table_leaf_rowids(page: &[u8], page1: bool) -> Vec<i64> {
        let hdr = if page1 { 100 } else { 0 };
        if page.get(hdr).copied() != Some(0x0d) || page.len() < hdr + 8 {
            return Vec::new();
        }
        let cell_count = u16::from_be_bytes([page[hdr + 3], page[hdr + 4]]) as usize;
        let mut rowids = Vec::new();
        for i in 0..cell_count {
            let ptr = hdr + 8 + i * 2;
            if ptr + 2 > page.len() {
                break;
            }
            let cell = u16::from_be_bytes([page[ptr], page[ptr + 1]]) as usize;
            let Some((_, used)) = test_read_varint(page, cell) else {
                continue;
            };
            let Some((rowid, _)) = test_read_varint(page, cell + used) else {
                continue;
            };
            rowids.push(rowid as i64);
        }
        rowids
    }

    fn test_read_varint(buf: &[u8], off: usize) -> Option<(u64, usize)> {
        let tail = buf.get(off..)?;
        let mut val = 0u64;
        for i in 0..8 {
            let b = *tail.get(i)?;
            val = (val << 7) | u64::from(b & 0x7f);
            if b & 0x80 == 0 {
                return Some((val, i + 1));
            }
        }
        let b = *tail.get(8)?;
        val = (val << 8) | u64::from(b);
        Some((val, 9))
    }

    fn build_resolver_fixture() -> (TempDir, Vec<u8>, usize, crate::btree_walker::BTreeEntry) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("resolver.db");
        let mut conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "PRAGMA page_size=1024;
             CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT);",
        )
        .unwrap();
        let tx = conn.transaction().unwrap();
        for id in 1..=3000i64 {
            tx.execute(
                "INSERT INTO data (id, val) VALUES (?1, ?2)",
                rusqlite::params![id, format!("value-{id:05}-{}", "x".repeat(80))],
            )
            .unwrap();
        }
        tx.commit().unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        drop(conn);

        let file = std::fs::read(&db_path).unwrap();
        let page_size = u16::from_be_bytes([file[16], file[17]]) as usize;
        let page_count = file.len() / page_size;
        let walk =
            crate::btree_walker::walk_all_btrees(page_count as u64, page_size as u32, &|p| {
                read_db_page(&file, page_size, p).map(ToOwned::to_owned)
            });
        let table = walk
            .btrees
            .values()
            .find(|entry| entry.name == "data")
            .expect("data table")
            .clone();
        (dir, file, page_size, table)
    }

    #[test]
    fn resolver_maps_rowids_to_leaf_pages_using_cached_interiors() {
        let (_dir, file, page_size, table) = build_resolver_fixture();
        let cache_dir = tempfile::tempdir().unwrap();
        let cache = DiskCache::new(
            cache_dir.path(),
            3600,
            8,
            1,
            page_size as u32,
            4000,
            None,
            Vec::new(),
        )
        .unwrap();
        for &page_num in &table.pages {
            let page = read_db_page(&file, page_size, page_num).unwrap();
            cache.write_page(page_num, page).unwrap();
        }

        let wanted = [1, 777, 2999];
        let resolved = resolve_rowids_to_pages(table.root_page, &wanted, &cache);
        for item in resolved {
            let page_num = item.page_num.expect("resolved page");
            let page = read_db_page(&file, page_size, page_num).unwrap();
            assert!(
                table_leaf_rowids(page, page_num == 0).contains(&item.rowid),
                "rowid {} should be on resolved page {}",
                item.rowid,
                page_num
            );
        }
    }

    #[test]
    fn resolver_does_not_infer_child_from_unproven_parent_level() {
        let (_dir, file, page_size, table) = build_resolver_fixture();
        let cache_dir = tempfile::tempdir().unwrap();
        let cache = DiskCache::new(
            cache_dir.path(),
            3600,
            8,
            1,
            page_size as u32,
            4000,
            None,
            Vec::new(),
        )
        .unwrap();
        let root = read_db_page(&file, page_size, table.root_page).unwrap();
        cache.write_page(table.root_page, root).unwrap();

        let resolved = resolve_rowids_to_pages(table.root_page, &[1500], &cache);
        assert_eq!(resolved[0].rowid, 1500);
        assert_eq!(resolved[0].page_num, None);
    }

    #[test]
    fn resolver_infers_uncached_leaf_when_sibling_leaf_proves_parent_level() {
        let (_dir, file, page_size, table) = build_resolver_fixture();
        let cache_dir = tempfile::tempdir().unwrap();
        let cache = DiskCache::new(
            cache_dir.path(),
            3600,
            8,
            1,
            page_size as u32,
            4000,
            None,
            Vec::new(),
        )
        .unwrap();

        let mut parent_and_children = None;
        for &page_num in &table.pages {
            let page = read_db_page(&file, page_size, page_num).unwrap();
            let hdr = if page_num == 0 { 100 } else { 0 };
            if page.get(hdr).copied() != Some(0x05) {
                continue;
            }
            let interior = parse_table_interior_cells(page, page_num == 0);
            let mut leaf_children = Vec::new();
            for child_1based in interior
                .cells
                .iter()
                .map(|cell| cell.child_page)
                .chain(interior.rightmost_child_page)
            {
                if child_1based == 0 {
                    continue;
                }
                let child_page_num = u64::from(child_1based - 1);
                let child = read_db_page(&file, page_size, child_page_num).unwrap();
                let child_hdr = if child_page_num == 0 { 100 } else { 0 };
                if child.get(child_hdr).copied() == Some(0x0d) {
                    leaf_children.push(child_page_num);
                }
            }
            if leaf_children.len() >= 2 {
                parent_and_children = Some((page_num, leaf_children));
                break;
            }
        }
        let (parent_page, leaf_children) = parent_and_children.expect("leaf parent");
        let proof_leaf = leaf_children[0];
        let target_leaf = leaf_children[1];
        let target_page = read_db_page(&file, page_size, target_leaf).unwrap();
        let rowid = table_leaf_rowids(target_page, target_leaf == 0)
            .into_iter()
            .next()
            .expect("target leaf rowid");

        cache
            .write_page(
                parent_page,
                read_db_page(&file, page_size, parent_page).unwrap(),
            )
            .unwrap();
        cache
            .write_page(
                proof_leaf,
                read_db_page(&file, page_size, proof_leaf).unwrap(),
            )
            .unwrap();

        let resolved = resolve_rowids_to_pages(parent_page, &[rowid], &cache);
        assert_eq!(resolved[0].page_num, Some(target_leaf));
    }
}
