//! Small, panic-free SQLite b-tree parsers for advisory lookahead.
//!
//! These helpers never perform I/O and never produce bytes that SQLite reads.
//! A malformed page simply yields a partial result plus counters explaining
//! what was skipped.

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct IndexLeafRowids {
    pub(crate) rowids: Vec<i64>,
    pub(crate) first_int_keys: Vec<Option<i64>>,
    pub(crate) overflow_cells: usize,
    pub(crate) malformed_cells: usize,
    pub(crate) stopped_early: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TableInteriorCell {
    /// SQLite's on-page child pointer, 1-based.
    pub(crate) child_page: u32,
    pub(crate) rowid_key: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TableInteriorCells {
    /// SQLite's rightmost child pointer, 1-based.
    pub(crate) rightmost_child_page: Option<u32>,
    pub(crate) cells: Vec<TableInteriorCell>,
    pub(crate) malformed_cells: usize,
    pub(crate) stopped_early: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TableLeafRowids {
    pub(crate) rowids: Vec<i64>,
    pub(crate) malformed_cells: usize,
    pub(crate) stopped_early: bool,
}

pub(crate) fn parse_index_leaf_rowids(
    page: &[u8],
    page1: bool,
    cap: usize,
    usable_size: Option<usize>,
) -> IndexLeafRowids {
    let mut out = IndexLeafRowids::default();
    if cap == 0 {
        return out;
    }
    let Some(header) = btree_header(page, page1, 8) else {
        return out;
    };
    if page.get(header).copied() != Some(0x0a) {
        return out;
    }

    let cell_count = read_u16(page, header + 3).unwrap_or(0) as usize;
    let pointer_array = header + 8;
    let max_local = index_max_local_payload(index_usable_size(page, page1, usable_size));

    for i in 0..cell_count {
        if out.rowids.len() >= cap {
            out.stopped_early = true;
            break;
        }
        let ptr_off = pointer_array + i * 2;
        let Some(cell_off) = read_u16(page, ptr_off).map(usize::from) else {
            out.stopped_early = true;
            break;
        };
        let Some((rowid, first_int_key, overflowed)) =
            parse_index_leaf_cell_rowid(page, cell_off, max_local)
        else {
            out.malformed_cells += 1;
            continue;
        };
        if overflowed {
            out.overflow_cells += 1;
            continue;
        }
        out.rowids.push(rowid);
        out.first_int_keys.push(first_int_key);
    }

    out
}

pub(crate) fn parse_table_leaf_rowids(page: &[u8], page1: bool, cap: usize) -> TableLeafRowids {
    let mut out = TableLeafRowids::default();
    if cap == 0 {
        return out;
    }
    let Some(header) = btree_header(page, page1, 8) else {
        return out;
    };
    if page.get(header).copied() != Some(0x0d) {
        return out;
    }

    let cell_count = read_u16(page, header + 3).unwrap_or(0) as usize;
    let pointer_array = header + 8;
    for i in 0..cell_count {
        if out.rowids.len() >= cap {
            out.stopped_early = true;
            break;
        }
        let ptr_off = pointer_array + i * 2;
        let Some(cell_off) = read_u16(page, ptr_off).map(usize::from) else {
            out.stopped_early = true;
            break;
        };
        let Some((_payload_len, payload_len_bytes)) = read_varint(page, cell_off) else {
            out.malformed_cells += 1;
            continue;
        };
        let Some((rowid, _rowid_bytes)) = read_varint(page, cell_off + payload_len_bytes) else {
            out.malformed_cells += 1;
            continue;
        };
        let Ok(rowid) = i64::try_from(rowid) else {
            out.malformed_cells += 1;
            continue;
        };
        out.rowids.push(rowid);
    }

    out
}

pub(crate) fn parse_table_interior_cells(page: &[u8], page1: bool) -> TableInteriorCells {
    let mut out = TableInteriorCells::default();
    let Some(header) = btree_header(page, page1, 12) else {
        return out;
    };
    if page.get(header).copied() != Some(0x05) {
        return out;
    }

    out.rightmost_child_page = read_u32(page, header + 8);
    let cell_count = read_u16(page, header + 3).unwrap_or(0) as usize;
    let pointer_array = header + 12;

    for i in 0..cell_count {
        let ptr_off = pointer_array + i * 2;
        let Some(cell_off) = read_u16(page, ptr_off).map(usize::from) else {
            out.stopped_early = true;
            break;
        };
        if cell_off + 4 > page.len() {
            out.malformed_cells += 1;
            continue;
        }
        let child_page = read_u32(page, cell_off).expect("bounds checked");
        let Some((key, _used)) = read_varint(page, cell_off + 4) else {
            out.malformed_cells += 1;
            continue;
        };
        out.cells.push(TableInteriorCell {
            child_page,
            rowid_key: key as i64,
        });
    }

    out
}

fn parse_index_leaf_cell_rowid(
    page: &[u8],
    cell_off: usize,
    max_local: usize,
) -> Option<(i64, Option<i64>, bool)> {
    let (payload_len, payload_len_bytes) = read_varint(page, cell_off)?;
    let payload_len = usize::try_from(payload_len).ok()?;
    let payload_start = cell_off.checked_add(payload_len_bytes)?;
    let payload_end = payload_start.checked_add(payload_len)?;
    let overflowed = payload_len > max_local;
    if overflowed || payload_end > page.len() {
        return Some((0, None, overflowed));
    }

    let (header_len, header_len_bytes) = read_varint(page, payload_start)?;
    let header_len = usize::try_from(header_len).ok()?;
    if header_len < header_len_bytes {
        return None;
    }
    let header_end = payload_start.checked_add(header_len)?;
    if header_end > payload_end {
        return None;
    }

    let mut serials = Vec::new();
    let mut pos = payload_start + header_len_bytes;
    while pos < header_end {
        let (serial, used) = read_varint(page, pos)?;
        if used == 0 {
            return None;
        }
        pos += used;
        serials.push(serial);
    }
    let &rowid_serial = serials.last()?;

    let mut body_pos = header_end;
    let first_int_key = read_record_int_key(page, body_pos, serials[0])?;
    for serial in serials.iter().take(serials.len().saturating_sub(1)) {
        let size = serial_type_size(*serial)?;
        if body_pos.checked_add(size)? > payload_end {
            return None;
        }
        body_pos = body_pos.checked_add(size)?;
    }
    let rowid_size = serial_type_size(rowid_serial)?;
    if body_pos + rowid_size > payload_end {
        return None;
    }
    read_record_int(page, body_pos, rowid_serial).map(|rowid| (rowid, first_int_key, false))
}

fn btree_header(page: &[u8], page1: bool, header_len: usize) -> Option<usize> {
    let header = if page1 { 100 } else { 0 };
    if page.len() >= header + header_len {
        Some(header)
    } else {
        None
    }
}

fn index_max_local_payload(usable_size: usize) -> usize {
    if usable_size <= 12 {
        return 0;
    }
    ((usable_size - 12) * 64 / 255).saturating_sub(23)
}

fn index_usable_size(page: &[u8], page1: bool, usable_size: Option<usize>) -> usize {
    if let Some(usable_size) = usable_size {
        return usable_size.min(page.len());
    }
    if page1 {
        let reserved = page.get(20).copied().unwrap_or(0) as usize;
        return page.len().saturating_sub(reserved);
    }
    page.len()
}

fn read_u16(buf: &[u8], off: usize) -> Option<u16> {
    let bytes: [u8; 2] = buf.get(off..off + 2)?.try_into().ok()?;
    Some(u16::from_be_bytes(bytes))
}

fn read_u32(buf: &[u8], off: usize) -> Option<u32> {
    let bytes: [u8; 4] = buf.get(off..off + 4)?.try_into().ok()?;
    Some(u32::from_be_bytes(bytes))
}

fn read_varint(buf: &[u8], off: usize) -> Option<(u64, usize)> {
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

fn serial_type_size(serial_type: u64) -> Option<usize> {
    match serial_type {
        0 => Some(0),
        1 => Some(1),
        2 => Some(2),
        3 => Some(3),
        4 => Some(4),
        5 => Some(6),
        6 | 7 => Some(8),
        8 | 9 => Some(0),
        10 | 11 => None,
        n if n >= 12 => Some(((n - 12) / 2) as usize),
        _ => None,
    }
}

fn read_record_int(buf: &[u8], off: usize, serial_type: u64) -> Option<i64> {
    let bytes = match serial_type {
        0 => return Some(0),
        1 => 1,
        2 => 2,
        3 => 3,
        4 => 4,
        5 => 6,
        6 => 8,
        // Serial type 7 is an IEEE-754 REAL. Rowid-bearing index payloads end
        // with the integer rowid, so a REAL in the final slot is not a rowid;
        // bail the cell instead of manufacturing an integer.
        7 => return None,
        8 => return Some(0),
        9 => return Some(1),
        _ => return None,
    };
    let src = buf.get(off..off + bytes)?;
    let mut val = 0i64;
    for b in src {
        val = (val << 8) | i64::from(*b);
    }
    let shift = 64 - bytes * 8;
    Some((val << shift) >> shift)
}

fn read_record_int_key(buf: &[u8], off: usize, serial_type: u64) -> Option<Option<i64>> {
    match serial_type {
        0 | 7 => Some(None),
        1..=6 | 8 | 9 => read_record_int(buf, off, serial_type).map(Some),
        _ => Some(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn read_db_page(file: &[u8], page_size: usize, page_num: u64) -> Option<&[u8]> {
        let start = page_num as usize * page_size;
        file.get(start..start + page_size)
    }

    fn encode_varint(mut value: u64) -> Vec<u8> {
        if value <= 0x7f {
            return vec![value as u8];
        }
        let mut bytes = Vec::new();
        while value > 0 {
            bytes.push((value & 0x7f) as u8);
            value >>= 7;
        }
        bytes.reverse();
        let last = bytes.len() - 1;
        for b in &mut bytes[..last] {
            *b |= 0x80;
        }
        bytes
    }

    #[test]
    fn varint_decode_known_encodings() {
        assert_eq!(read_varint(&[0x00], 0), Some((0, 1)));
        assert_eq!(read_varint(&[0x7f], 0), Some((127, 1)));
        assert_eq!(read_varint(&[0x81, 0x00], 0), Some((128, 2)));
        assert_eq!(read_varint(&[0x81, 0x01], 0), Some((129, 2)));
        assert_eq!(read_varint(&[0xff; 8], 0), None);
        assert_eq!(
            read_varint(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f], 0),
            Some((0xffff_ffff_ffff_ff7f, 9))
        );
        assert_eq!(
            read_varint(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff], 0),
            Some((u64::MAX, 9))
        );
    }

    #[test]
    fn index_leaf_rowids_from_real_sqlite_pages() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("idx.db");
        let mut conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "PRAGMA page_size=4096;
             CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, created_at INTEGER);
             CREATE INDEX idx_posts_user_created ON posts(user_id, created_at DESC);",
        )
        .unwrap();
        let tx = conn.transaction().unwrap();
        for id in 1..=200i64 {
            tx.execute(
                "INSERT INTO posts (id, user_id, created_at) VALUES (?1, ?2, ?3)",
                rusqlite::params![id, id % 17, id * 10],
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
        let index = walk
            .btrees
            .values()
            .find(|entry| entry.name == "idx_posts_user_created")
            .expect("index btree");

        let mut rowids = Vec::new();
        let mut first_int_keys = Vec::new();
        for &page_num in &index.pages {
            let page = read_db_page(&file, page_size, page_num).unwrap();
            let header = if page_num == 0 { 100 } else { 0 };
            if page.get(header).copied() == Some(0x0a) {
                let parsed = parse_index_leaf_rowids(page, page_num == 0, 512, None);
                rowids.extend(parsed.rowids);
                first_int_keys.extend(parsed.first_int_keys);
            }
        }
        rowids.sort_unstable();
        assert_eq!(rowids, (1..=200).collect::<Vec<_>>());
        assert!(first_int_keys.contains(&Some(7)));
        assert!(first_int_keys.contains(&Some(16)));
    }

    #[test]
    fn table_interior_cells_from_real_multilevel_table() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("table.db");
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
            .expect("table btree");
        let table_pages: HashSet<u32> = table.pages.iter().map(|p| *p as u32 + 1).collect();
        let interior_page = table
            .pages
            .iter()
            .copied()
            .find(|&page_num| {
                let page = read_db_page(&file, page_size, page_num).unwrap();
                let header = if page_num == 0 { 100 } else { 0 };
                page.get(header).copied() == Some(0x05)
            })
            .expect("interior table page");
        let parsed = parse_table_interior_cells(
            read_db_page(&file, page_size, interior_page).unwrap(),
            false,
        );

        assert!(!parsed.cells.is_empty());
        assert!(parsed.rightmost_child_page.is_some());
        for cell in &parsed.cells {
            assert!(table_pages.contains(&cell.child_page));
        }
        assert!(table_pages.contains(&parsed.rightmost_child_page.unwrap()));
    }

    #[test]
    fn page1_header_offset_is_honored_for_table_interior() {
        let mut page = vec![0u8; 256];
        page[100] = 0x05;
        page[103..105].copy_from_slice(&1u16.to_be_bytes());
        page[108..112].copy_from_slice(&9u32.to_be_bytes());
        page[112..114].copy_from_slice(&120u16.to_be_bytes());
        page[120..124].copy_from_slice(&7u32.to_be_bytes());
        page[124] = 42;

        let parsed = parse_table_interior_cells(&page, true);
        assert_eq!(parsed.rightmost_child_page, Some(9));
        assert_eq!(
            parsed.cells,
            vec![TableInteriorCell {
                child_page: 7,
                rowid_key: 42,
            }]
        );
    }

    #[test]
    fn overflow_index_cells_are_skipped_not_panics() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("overflow.db");
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "PRAGMA page_size=1024;
             CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT);
             CREATE INDEX idx_docs_body ON docs(body);",
        )
        .unwrap();
        let big = "z".repeat(5000);
        for id in 1..=20i64 {
            conn.execute(
                "INSERT INTO docs (id, body) VALUES (?1, ?2)",
                rusqlite::params![id, big],
            )
            .unwrap();
        }
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
        let index = walk
            .btrees
            .values()
            .find(|entry| entry.name == "idx_docs_body")
            .expect("index btree");

        let mut overflow_cells = 0;
        for &page_num in &index.pages {
            let page = read_db_page(&file, page_size, page_num).unwrap();
            let header = if page_num == 0 { 100 } else { 0 };
            if page.get(header).copied() == Some(0x0a) {
                overflow_cells +=
                    parse_index_leaf_rowids(page, page_num == 0, 256, None).overflow_cells;
            }
        }
        assert!(overflow_cells > 0);
    }

    #[test]
    fn reserved_bytes_reduce_index_overflow_threshold() {
        let mut page = vec![0u8; 4096];
        page[20] = 255;
        page[100] = 0x0a;
        page[103..105].copy_from_slice(&1u16.to_be_bytes());
        page[108..110].copy_from_slice(&200u16.to_be_bytes());
        let payload_len = encode_varint(970);
        page[200..200 + payload_len.len()].copy_from_slice(&payload_len);

        let parsed = parse_index_leaf_rowids(&page, true, 64, None);
        assert_eq!(parsed.overflow_cells, 1);
        assert_eq!(parsed.malformed_cells, 0);
        assert!(parsed.rowids.is_empty());
    }

    #[test]
    fn real_serial_type_in_final_index_slot_bails_cell() {
        let mut page = vec![0u8; 256];
        page[0] = 0x0a;
        page[3..5].copy_from_slice(&1u16.to_be_bytes());
        page[8..10].copy_from_slice(&64u16.to_be_bytes());
        page[64] = 10; // payload bytes
        page[65] = 2; // record header bytes: header_len + one serial varint
        page[66] = 7; // REAL, not an integer rowid

        let parsed = parse_index_leaf_rowids(&page, false, 64, None);
        assert!(parsed.rowids.is_empty());
        assert_eq!(parsed.malformed_cells, 1);
    }

    #[test]
    fn text_keyed_multi_column_index_yields_rowids() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("text_index.db");
        let mut conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "PRAGMA page_size=4096;
             CREATE TABLE docs (id INTEGER PRIMARY KEY, a TEXT NOT NULL, b TEXT NOT NULL);
             CREATE INDEX idx_docs_a_b ON docs(a, b);",
        )
        .unwrap();
        let tx = conn.transaction().unwrap();
        for id in 1..=120i64 {
            tx.execute(
                "INSERT INTO docs (id, a, b) VALUES (?1, ?2, ?3)",
                rusqlite::params![
                    id,
                    format!("key-{id:03}"),
                    format!("suffix-{:03}", 120 - id)
                ],
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
        let index = walk
            .btrees
            .values()
            .find(|entry| entry.name == "idx_docs_a_b")
            .expect("text index btree");

        let mut rowids = Vec::new();
        for &page_num in &index.pages {
            let page = read_db_page(&file, page_size, page_num).unwrap();
            let header = if page_num == 0 { 100 } else { 0 };
            if page.get(header).copied() == Some(0x0a) {
                rowids.extend(parse_index_leaf_rowids(page, page_num == 0, 256, None).rowids);
            }
        }
        rowids.sort_unstable();
        assert_eq!(rowids, (1..=120).collect::<Vec<_>>());
    }

    #[test]
    fn without_rowid_table_leaf_bails_instead_of_inventing_rowids() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("without_rowid.db");
        let mut conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "PRAGMA page_size=4096;
             CREATE TABLE wr (
               a TEXT NOT NULL,
               b TEXT NOT NULL,
               payload TEXT,
               PRIMARY KEY (a, b)
             ) WITHOUT ROWID;",
        )
        .unwrap();
        let tx = conn.transaction().unwrap();
        for id in 1..=120i64 {
            tx.execute(
                "INSERT INTO wr (a, b, payload) VALUES (?1, ?2, ?3)",
                rusqlite::params![
                    format!("key-{id:03}"),
                    format!("suffix-{:03}", 120 - id),
                    format!("payload-{id}")
                ],
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
            .find(|entry| entry.name == "wr")
            .expect("without rowid table btree");

        let mut malformed = 0;
        let mut rowids = Vec::new();
        for &page_num in &table.pages {
            let page = read_db_page(&file, page_size, page_num).unwrap();
            let header = if page_num == 0 { 100 } else { 0 };
            if page.get(header).copied() == Some(0x0a) {
                let parsed = parse_index_leaf_rowids(page, page_num == 0, 256, None);
                malformed += parsed.malformed_cells;
                rowids.extend(parsed.rowids);
            }
        }
        assert!(rowids.is_empty());
        assert!(
            malformed > 0,
            "WITHOUT ROWID table leaves must bail instead of exposing fake rowids"
        );
    }

    #[test]
    fn parser_is_panic_free_on_adversarial_bytes() {
        let mut x = 0x1234_5678_9abc_def0u64;
        for len in 0..512usize {
            let mut page = vec![0u8; len];
            for b in &mut page {
                x ^= x << 7;
                x ^= x >> 9;
                x ^= x << 8;
                *b = x as u8;
            }
            let _ = parse_index_leaf_rowids(&page, false, 64, None);
            let _ = parse_index_leaf_rowids(&page, true, 64, None);
            let _ = parse_table_interior_cells(&page, false);
            let _ = parse_table_interior_cells(&page, true);
        }
    }
}
