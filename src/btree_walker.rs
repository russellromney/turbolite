//! B-tree walker: enumerates all pages belonging to each B-tree in a SQLite database.
//!
//! Parses sqlite_master (page 1) to discover root pages, then walks each B-tree
//! to collect all interior, leaf, and overflow pages. Used by the tiered VFS to
//! build B-tree-aware page groups (Phase Midway).
//!
//! SQLite page types:
//!   0x02 = interior index B-tree
//!   0x05 = interior table B-tree
//!   0x0A = leaf index B-tree
//!   0x0D = leaf table B-tree
//!
//! Interior pages have child pointers; leaf pages do not.
//! Overflow pages are chained via a 4-byte next-page pointer at offset 0.

use std::collections::{HashMap, HashSet, VecDeque};

/// Information about a single B-tree discovered from sqlite_master.
#[derive(Debug, Clone)]
pub struct BTreeEntry {
    /// "table" or "index"
    pub obj_type: String,
    /// Name of the table or index
    pub name: String,
    /// Root page number (1-based, as stored in sqlite_master)
    pub root_page: u64,
    /// All page numbers belonging to this B-tree (interior + leaf + overflow)
    pub pages: Vec<u64>,
}

/// Result of walking all B-trees in a database.
#[derive(Debug, Clone)]
pub struct BTreeWalkResult {
    /// One entry per B-tree (tables and indexes), keyed by root page number
    pub btrees: HashMap<u64, BTreeEntry>,
    /// Pages not belonging to any B-tree (freelist, pointer map, lock-byte, unallocated)
    pub unowned_pages: Vec<u64>,
}

/// Walk all B-trees in a SQLite database.
///
/// `read_page` is a closure that reads page N (0-based) and returns its raw bytes.
/// Page 0 is the first page of the database (contains the 100-byte DB header + sqlite_master root).
///
/// Returns a map of root_page -> BTreeEntry with all pages enumerated.
pub fn walk_all_btrees(
    page_count: u64,
    page_size: u32,
    read_page: &dyn Fn(u64) -> Option<Vec<u8>>,
) -> BTreeWalkResult {
    // Step 1: Parse sqlite_master to get root pages
    let master_entries = parse_sqlite_master(page_size, read_page);

    // Step 2: Walk each B-tree from its root page
    let mut btrees = HashMap::new();
    let mut all_owned: HashSet<u64> = HashSet::new();

    // sqlite_master itself is a B-tree rooted at page 1 (page 0 in 0-based)
    // Add it explicitly since it doesn't appear in its own records
    let master_pages = walk_btree(0, page_size, read_page);
    all_owned.extend(&master_pages);
    btrees.insert(0, BTreeEntry {
        obj_type: "table".to_string(),
        name: "sqlite_master".to_string(),
        root_page: 0,
        pages: master_pages,
    });

    for entry in &master_entries {
        if entry.root_page == 0 {
            continue; // root_page 0 means the object was dropped
        }
        // sqlite stores 1-based page numbers; we use 0-based internally
        let root_0based = entry.root_page - 1;
        let pages = walk_btree(root_0based, page_size, read_page);
        all_owned.extend(&pages);
        btrees.insert(root_0based, BTreeEntry {
            obj_type: entry.obj_type.clone(),
            name: entry.name.clone(),
            root_page: root_0based,
            pages,
        });
    }

    // Step 3: Find unowned pages
    let mut unowned_pages = Vec::new();
    for p in 0..page_count {
        if !all_owned.contains(&p) {
            unowned_pages.push(p);
        }
    }

    BTreeWalkResult { btrees, unowned_pages }
}

/// Walk a single B-tree from its root page, returning all page numbers (0-based).
fn walk_btree(
    root_page: u64,
    page_size: u32,
    read_page: &dyn Fn(u64) -> Option<Vec<u8>>,
) -> Vec<u64> {
    let mut pages = Vec::new();
    let mut queue: VecDeque<u64> = VecDeque::new();
    let mut visited: HashSet<u64> = HashSet::new();

    queue.push_back(root_page);
    visited.insert(root_page);

    while let Some(page_num) = queue.pop_front() {
        let buf = match read_page(page_num) {
            Some(b) => b,
            None => continue,
        };
        pages.push(page_num);

        let hdr_off = if page_num == 0 { 100 } else { 0 };
        if buf.len() < hdr_off + 12 {
            continue;
        }

        let type_byte = buf[hdr_off];
        match type_byte {
            // Interior pages: collect child page pointers
            0x02 | 0x05 => {
                let children = extract_interior_children(&buf, hdr_off, page_size);
                for child_1based in children {
                    if child_1based == 0 {
                        continue;
                    }
                    let child = child_1based as u64 - 1; // convert to 0-based
                    if visited.insert(child) {
                        queue.push_back(child);
                    }
                }
            }
            // Leaf pages: collect overflow pages from cells
            0x0A | 0x0D => {
                let overflow_pages = extract_leaf_overflow_pages(&buf, hdr_off, page_size, type_byte);
                for ovfl_1based in overflow_pages {
                    if ovfl_1based == 0 {
                        continue;
                    }
                    // Follow the overflow chain
                    let mut ovfl = ovfl_1based as u64 - 1;
                    while visited.insert(ovfl) {
                        pages.push(ovfl);
                        if let Some(ovfl_buf) = read_page(ovfl) {
                            if ovfl_buf.len() >= 4 {
                                let next = u32::from_be_bytes([
                                    ovfl_buf[0], ovfl_buf[1], ovfl_buf[2], ovfl_buf[3],
                                ]);
                                if next == 0 {
                                    break;
                                }
                                ovfl = next as u64 - 1;
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                }
            }
            _ => {
                // Not a recognized B-tree page type. Could be a corrupted page
                // or a page type we don't handle. Just include it as part of
                // this B-tree since we reached it via traversal.
            }
        }
    }

    pages.sort_unstable();
    pages
}

/// Extract child page numbers from an interior B-tree page.
/// Returns 1-based page numbers (SQLite convention).
fn extract_interior_children(buf: &[u8], hdr_off: usize, _page_size: u32) -> Vec<u32> {
    let mut children = Vec::new();

    if buf.len() < hdr_off + 12 {
        return children;
    }

    let cell_count = u16::from_be_bytes([buf[hdr_off + 3], buf[hdr_off + 4]]) as usize;

    // Right-most child pointer (4 bytes at hdr_off + 8)
    let right_child = u32::from_be_bytes([
        buf[hdr_off + 8], buf[hdr_off + 9], buf[hdr_off + 10], buf[hdr_off + 11],
    ]);
    children.push(right_child);

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
            buf[cell_off], buf[cell_off + 1], buf[cell_off + 2], buf[cell_off + 3],
        ]);
        children.push(left_child);
    }

    children
}

/// Extract first overflow page numbers from leaf cells that overflow.
/// Returns 1-based page numbers.
fn extract_leaf_overflow_pages(
    buf: &[u8],
    hdr_off: usize,
    page_size: u32,
    type_byte: u8,
) -> Vec<u32> {
    let mut overflow_pages = Vec::new();

    if buf.len() < hdr_off + 8 {
        return overflow_pages;
    }

    let cell_count = u16::from_be_bytes([buf[hdr_off + 3], buf[hdr_off + 4]]) as usize;
    let is_table = type_byte == 0x0D;

    // Compute overflow threshold per SQLite docs:
    // For table leaf: usable_size - 35
    // For index leaf: ((usable_size - 12) * 64 / 255) - 23
    let usable = page_size as usize; // assume no reserved bytes
    let max_local = if is_table {
        usable - 35
    } else {
        ((usable - 12) * 64 / 255) - 23
    };

    // Cell pointer array starts at hdr_off + 8 (leaf pages have no right-child pointer)
    let cell_ptr_start = hdr_off + 8;
    for i in 0..cell_count {
        let ptr_off = cell_ptr_start + i * 2;
        if ptr_off + 2 > buf.len() {
            break;
        }
        let cell_off = u16::from_be_bytes([buf[ptr_off], buf[ptr_off + 1]]) as usize;
        if cell_off >= buf.len() {
            continue;
        }

        // Parse cell to determine payload size
        let mut pos = cell_off;
        let (payload_size, bytes_read) = read_varint(&buf[pos..]);
        pos += bytes_read;

        if is_table {
            // Table leaf cells have rowid after payload size
            let (_rowid, rb) = read_varint(&buf[pos..]);
            pos += rb;
        }

        // Check if payload overflows
        if payload_size as usize > max_local {
            // The overflow page pointer is at the end of the local payload portion.
            // Local payload size for overflow:
            // min_local = ((usable - 12) * 32 / 255) - 23
            let min_local = ((usable - 12) * 32 / 255) - 23;
            let local_size = if payload_size as usize <= max_local {
                payload_size as usize
            } else {
                let surplus = min_local + (payload_size as usize - min_local) % (usable - 4);
                if surplus <= max_local {
                    surplus
                } else {
                    min_local
                }
            };

            let ovfl_ptr_off = pos + local_size;
            if ovfl_ptr_off + 4 <= buf.len() {
                let ovfl = u32::from_be_bytes([
                    buf[ovfl_ptr_off], buf[ovfl_ptr_off + 1],
                    buf[ovfl_ptr_off + 2], buf[ovfl_ptr_off + 3],
                ]);
                if ovfl != 0 {
                    overflow_pages.push(ovfl);
                }
            }
        }
    }

    overflow_pages
}

/// Parsed entry from sqlite_master.
#[derive(Debug)]
struct MasterEntry {
    obj_type: String,
    name: String,
    root_page: u64,
}

/// Parse sqlite_master (the table B-tree rooted at page 1 / page 0 in 0-based).
/// Returns a list of (type, name, root_page) for all tables and indexes.
fn parse_sqlite_master(
    page_size: u32,
    read_page: &dyn Fn(u64) -> Option<Vec<u8>>,
) -> Vec<MasterEntry> {
    let mut entries = Vec::new();
    let mut leaf_pages = Vec::new();

    // sqlite_master is a table B-tree rooted at page 0 (1 in SQLite's 1-based numbering).
    // Collect all leaf pages by walking the B-tree.
    collect_leaf_pages(0, page_size, read_page, &mut leaf_pages);

    for page_num in leaf_pages {
        let buf = match read_page(page_num) {
            Some(b) => b,
            None => continue,
        };

        let hdr_off = if page_num == 0 { 100 } else { 0 };
        if buf.len() < hdr_off + 8 {
            continue;
        }

        let type_byte = buf[hdr_off];
        if type_byte != 0x0D {
            continue; // Not a leaf table page
        }

        let cell_count = u16::from_be_bytes([buf[hdr_off + 3], buf[hdr_off + 4]]) as usize;
        let cell_ptr_start = hdr_off + 8;

        for i in 0..cell_count {
            let ptr_off = cell_ptr_start + i * 2;
            if ptr_off + 2 > buf.len() {
                break;
            }
            let cell_off = u16::from_be_bytes([buf[ptr_off], buf[ptr_off + 1]]) as usize;
            if cell_off >= buf.len() {
                continue;
            }

            if let Some(entry) = parse_master_cell(&buf, cell_off, page_size) {
                entries.push(entry);
            }
        }
    }

    entries
}

/// Collect all leaf page numbers of a table B-tree (for sqlite_master parsing).
fn collect_leaf_pages(
    root_page: u64,
    page_size: u32,
    read_page: &dyn Fn(u64) -> Option<Vec<u8>>,
    leaves: &mut Vec<u64>,
) {
    let buf = match read_page(root_page) {
        Some(b) => b,
        None => return,
    };

    let hdr_off = if root_page == 0 { 100 } else { 0 };
    if buf.len() < hdr_off + 1 {
        return;
    }

    match buf[hdr_off] {
        0x05 => {
            // Interior table page: recurse into children
            let children = extract_interior_children(&buf, hdr_off, page_size);
            for child_1based in children {
                if child_1based > 0 {
                    collect_leaf_pages(child_1based as u64 - 1, page_size, read_page, leaves);
                }
            }
        }
        0x0D => {
            // Leaf table page
            leaves.push(root_page);
        }
        _ => {}
    }
}

/// Parse a single cell from a sqlite_master leaf page.
/// sqlite_master schema: type TEXT, name TEXT, tbl_name TEXT, rootpage INTEGER, sql TEXT
fn parse_master_cell(buf: &[u8], cell_off: usize, _page_size: u32) -> Option<MasterEntry> {
    if cell_off >= buf.len() {
        return None;
    }

    let mut pos = cell_off;
    // Payload length
    let (_payload_len, vr) = read_varint(&buf[pos..]);
    pos += vr;
    // Rowid
    let (_rowid, vr) = read_varint(&buf[pos..]);
    pos += vr;

    // Now we're at the start of the record payload
    let payload_start = pos;
    if payload_start >= buf.len() {
        return None;
    }

    // Record header: starts with header length varint
    let (header_len, vr) = read_varint(&buf[pos..]);
    let header_end = payload_start + header_len as usize;
    pos += vr;

    // Read serial types for the 5 columns
    let mut serial_types = Vec::new();
    for _ in 0..5 {
        if pos >= header_end || pos >= buf.len() {
            break;
        }
        let (st, vr) = read_varint(&buf[pos..]);
        pos += vr;
        serial_types.push(st);
    }

    if serial_types.len() < 5 {
        return None;
    }

    // Now read column values starting at header_end
    pos = header_end;
    if pos >= buf.len() {
        return None;
    }

    // Column 0: type (TEXT)
    let (obj_type, sz) = read_column_text(buf, pos, serial_types[0])?;
    pos += sz;

    // Column 1: name (TEXT)
    let (name, sz) = read_column_text(buf, pos, serial_types[1])?;
    pos += sz;

    // Column 2: tbl_name (TEXT) — skip
    let sz = serial_type_size(serial_types[2]);
    pos += sz;

    // Column 3: rootpage (INTEGER)
    let root_page = read_column_int(buf, pos, serial_types[3])?;

    Some(MasterEntry {
        obj_type,
        name,
        root_page: root_page as u64,
    })
}

/// Read a SQLite varint (1-9 bytes, MSB encoding).
fn read_varint(buf: &[u8]) -> (u64, usize) {
    if buf.is_empty() {
        return (0, 0);
    }

    let mut val: u64 = 0;
    for i in 0..8 {
        if i >= buf.len() {
            return (val, i);
        }
        val = (val << 7) | (buf[i] as u64 & 0x7F);
        if buf[i] & 0x80 == 0 {
            return (val, i + 1);
        }
    }
    // 9th byte: all 8 bits are payload
    if buf.len() > 8 {
        val = (val << 8) | buf[8] as u64;
        (val, 9)
    } else {
        (val, buf.len())
    }
}

/// Get the storage size of a column value given its serial type.
fn serial_type_size(serial_type: u64) -> usize {
    match serial_type {
        0 => 0,            // NULL
        1 => 1,            // 8-bit int
        2 => 2,            // 16-bit int
        3 => 3,            // 24-bit int
        4 => 4,            // 32-bit int
        5 => 6,            // 48-bit int
        6 => 8,            // 64-bit int
        7 => 8,            // IEEE float
        8 | 9 => 0,        // 0 or 1 (in-type value)
        10 | 11 => 0,      // reserved
        n if n >= 12 && n % 2 == 0 => ((n - 12) / 2) as usize, // BLOB
        n if n >= 13 && n % 2 == 1 => ((n - 13) / 2) as usize, // TEXT
        _ => 0,
    }
}

/// Read a TEXT column value.
fn read_column_text(buf: &[u8], pos: usize, serial_type: u64) -> Option<(String, usize)> {
    if serial_type < 13 || serial_type % 2 != 1 {
        // Not a text type. Could be NULL (0) for dropped objects.
        let sz = serial_type_size(serial_type);
        return Some((String::new(), sz));
    }
    let len = ((serial_type - 13) / 2) as usize;
    if pos + len > buf.len() {
        return None;
    }
    let s = String::from_utf8_lossy(&buf[pos..pos + len]).to_string();
    Some((s, len))
}

/// Read an INTEGER column value.
fn read_column_int(buf: &[u8], pos: usize, serial_type: u64) -> Option<i64> {
    let bytes = match serial_type {
        0 => return Some(0), // NULL → rootpage 0 (dropped)
        1 => 1,
        2 => 2,
        3 => 3,
        4 => 4,
        5 => 6,
        6 => 8,
        8 => return Some(0), // constant 0
        9 => return Some(1), // constant 1
        _ => return None,
    };
    if pos + bytes > buf.len() {
        return None;
    }
    // Read as big-endian signed integer
    let mut val: i64 = 0;
    for i in 0..bytes {
        val = (val << 8) | buf[pos + i] as i64;
    }
    // Sign-extend
    let shift = 64 - (bytes * 8);
    val = (val << shift) >> shift;
    Some(val)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use std::path::Path;

    /// Create a test database and walk its B-trees.
    #[test]
    fn test_walk_simple_database() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Create a database with known schema
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch("
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);
            CREATE INDEX idx_users_name ON users(name);
            CREATE INDEX idx_users_email ON users(email);
            INSERT INTO users VALUES (1, 'alice', 'alice@example.com');
            INSERT INTO users VALUES (2, 'bob', 'bob@example.com');
            INSERT INTO users VALUES (3, 'carol', 'carol@example.com');
        ").unwrap();
        // Force WAL checkpoint to ensure all pages are in the main DB file
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
        drop(conn);

        let file = std::fs::read(&db_path).unwrap();
        let page_size = u16::from_be_bytes([file[16], file[17]]) as u32;
        let page_count = (file.len() as u32 / page_size) as u64;

        let result = walk_all_btrees(page_count, page_size, &|page_num| {
            let offset = page_num as usize * page_size as usize;
            if offset + page_size as usize > file.len() {
                return None;
            }
            Some(file[offset..offset + page_size as usize].to_vec())
        });

        // Should find sqlite_master + users table + 2 indexes = 4 B-trees
        // (plus potentially sqlite_sequence if autoincrement, but we're not using it)
        assert!(result.btrees.len() >= 4, "expected at least 4 btrees, got {}", result.btrees.len());

        // sqlite_master should be at root page 0
        assert!(result.btrees.contains_key(&0), "missing sqlite_master");

        // Find the users table and indexes by name
        let users_table = result.btrees.values().find(|e| e.name == "users");
        assert!(users_table.is_some(), "missing users table");
        assert_eq!(users_table.unwrap().obj_type, "table");

        let name_idx = result.btrees.values().find(|e| e.name == "idx_users_name");
        assert!(name_idx.is_some(), "missing idx_users_name");
        assert_eq!(name_idx.unwrap().obj_type, "index");

        let email_idx = result.btrees.values().find(|e| e.name == "idx_users_email");
        assert!(email_idx.is_some(), "missing idx_users_email");
        assert_eq!(email_idx.unwrap().obj_type, "index");

        // Every page should be owned by some B-tree or unowned (freelist)
        let total_owned: usize = result.btrees.values().map(|e| e.pages.len()).sum();
        assert_eq!(
            total_owned + result.unowned_pages.len(),
            page_count as usize,
            "owned ({}) + unowned ({}) should equal page_count ({})",
            total_owned, result.unowned_pages.len(), page_count,
        );

        // No page should appear in two different B-trees
        let mut all_pages: Vec<u64> = result.btrees.values().flat_map(|e| e.pages.iter().copied()).collect();
        all_pages.sort_unstable();
        for w in all_pages.windows(2) {
            assert_ne!(w[0], w[1], "page {} appears in multiple B-trees", w[0]);
        }
    }

    /// Test with a larger dataset that may have multi-level B-trees.
    #[test]
    fn test_walk_multilevel_btree() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test_large.db");

        let mut conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch("
            PRAGMA page_size = 4096;
            CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT);
            CREATE INDEX idx_data_val ON data(val);
        ").unwrap();
        // Insert enough rows to force multi-level B-trees with 4KB pages
        let tx = conn.transaction().unwrap();
        for i in 0..5000 {
            tx.execute(
                "INSERT INTO data VALUES (?1, ?2)",
                rusqlite::params![i, format!("value_{:06}", i)],
            ).unwrap();
        }
        tx.commit().unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
        drop(conn);

        let file = std::fs::read(&db_path).unwrap();
        let page_size = u16::from_be_bytes([file[16], file[17]]) as u32;
        let page_count = (file.len() as u32 / page_size) as u64;

        let result = walk_all_btrees(page_count, page_size, &|page_num| {
            let offset = page_num as usize * page_size as usize;
            if offset + page_size as usize > file.len() {
                return None;
            }
            Some(file[offset..offset + page_size as usize].to_vec())
        });

        // Should find at least: sqlite_master, data table, idx_data_val
        assert!(result.btrees.len() >= 3);

        // The data table should have many pages (5000 rows in 4KB pages)
        let data_table = result.btrees.values().find(|e| e.name == "data").unwrap();
        assert!(data_table.pages.len() > 10, "data table should have many pages, got {}", data_table.pages.len());

        // The index should also have multiple pages
        let idx = result.btrees.values().find(|e| e.name == "idx_data_val").unwrap();
        assert!(idx.pages.len() > 5, "index should have multiple pages, got {}", idx.pages.len());

        // Verify complete coverage
        let total_owned: usize = result.btrees.values().map(|e| e.pages.len()).sum();
        assert_eq!(total_owned + result.unowned_pages.len(), page_count as usize);

        // No duplicates
        let mut all_pages: Vec<u64> = result.btrees.values().flat_map(|e| e.pages.iter().copied()).collect();
        all_pages.sort_unstable();
        for w in all_pages.windows(2) {
            assert_ne!(w[0], w[1], "page {} appears in multiple B-trees", w[0]);
        }
    }

    /// Test with overflow pages (large text values).
    #[test]
    fn test_walk_with_overflow() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test_overflow.db");

        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch("
            PRAGMA page_size = 4096;
            CREATE TABLE big (id INTEGER PRIMARY KEY, data TEXT);
        ").unwrap();
        // Insert values large enough to trigger overflow pages (> ~4000 bytes at 4KB page size)
        let big_val = "x".repeat(10000);
        for i in 0..20 {
            conn.execute(
                "INSERT INTO big VALUES (?1, ?2)",
                rusqlite::params![i, big_val],
            ).unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
        drop(conn);

        let file = std::fs::read(&db_path).unwrap();
        let page_size = u16::from_be_bytes([file[16], file[17]]) as u32;
        let page_count = (file.len() as u32 / page_size) as u64;

        let result = walk_all_btrees(page_count, page_size, &|page_num| {
            let offset = page_num as usize * page_size as usize;
            if offset + page_size as usize > file.len() {
                return None;
            }
            Some(file[offset..offset + page_size as usize].to_vec())
        });

        // The big table should have many pages (overflow pages for 10KB values)
        let big_table = result.btrees.values().find(|e| e.name == "big").unwrap();
        // 20 rows * ~10KB each = ~200KB. At 4KB pages that's ~50 pages minimum.
        assert!(big_table.pages.len() > 30,
            "big table should have overflow pages, got {} pages", big_table.pages.len());

        // Complete coverage
        let total_owned: usize = result.btrees.values().map(|e| e.pages.len()).sum();
        assert_eq!(total_owned + result.unowned_pages.len(), page_count as usize);
    }

    #[test]
    fn test_read_varint() {
        // Single byte
        assert_eq!(read_varint(&[0x05]), (5, 1));
        assert_eq!(read_varint(&[0x7F]), (127, 1));

        // Two bytes
        assert_eq!(read_varint(&[0x81, 0x00]), (128, 2));
        assert_eq!(read_varint(&[0x81, 0x01]), (129, 2));

        // Larger values
        assert_eq!(read_varint(&[0x82, 0x00]), (256, 2));
    }

    #[test]
    fn test_serial_type_size() {
        assert_eq!(serial_type_size(0), 0);  // NULL
        assert_eq!(serial_type_size(1), 1);  // 8-bit int
        assert_eq!(serial_type_size(4), 4);  // 32-bit int
        assert_eq!(serial_type_size(6), 8);  // 64-bit int
        assert_eq!(serial_type_size(7), 8);  // float
        assert_eq!(serial_type_size(8), 0);  // constant 0
        assert_eq!(serial_type_size(9), 0);  // constant 1
        assert_eq!(serial_type_size(13), 0); // TEXT len 0
        assert_eq!(serial_type_size(15), 1); // TEXT len 1
        assert_eq!(serial_type_size(17), 2); // TEXT len 2
        assert_eq!(serial_type_size(12), 0); // BLOB len 0
        assert_eq!(serial_type_size(14), 1); // BLOB len 1
    }
}
