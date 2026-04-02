//! Phase Jena-c/d: SQLite record format parser.
//!
//! Parses SQLite's B-tree cell format to extract:
//! - Key boundaries from interior cells (for exact leaf prediction)
//! - Rowids and column values from leaf cells (for cross-tree leaf chasing)
//!
//! SQLite record format (https://www.sqlite.org/fileformat2.html#record_format):
//! - Header: varint header_size, then varint type codes per column
//! - Body: column values concatenated, sizes determined by type codes
//!
//! Interior cell format:
//! - 4-byte left child pointer
//! - varint payload size
//! - varint rowid (table interior only, type 0x05)
//! - record payload (key data for index interior, type 0x02)
//!
//! Leaf cell format:
//! - varint payload size
//! - varint rowid (table leaf 0x0D)
//! - record payload

/// A parsed value from a SQLite record.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SqliteValue {
    Null,
    Integer(i64),
    Float(f64),
    Blob(Vec<u8>),
    Text(String),
}

impl SqliteValue {
    /// Compare two values using BINARY collation (memcmp semantics).
    /// Returns Ordering. Null sorts before everything.
    pub fn cmp_binary(&self, other: &SqliteValue) -> std::cmp::Ordering {
        use std::cmp::Ordering::*;
        match (self, other) {
            (SqliteValue::Null, SqliteValue::Null) => Equal,
            (SqliteValue::Null, _) => Less,
            (_, SqliteValue::Null) => Greater,
            (SqliteValue::Integer(a), SqliteValue::Integer(b)) => a.cmp(b),
            (SqliteValue::Integer(a), SqliteValue::Float(b)) => (*a as f64).partial_cmp(b).unwrap_or(Equal),
            (SqliteValue::Float(a), SqliteValue::Integer(b)) => a.partial_cmp(&(*b as f64)).unwrap_or(Equal),
            (SqliteValue::Float(a), SqliteValue::Float(b)) => a.partial_cmp(b).unwrap_or(Equal),
            // SQLite sort order: NULL < INTEGER/FLOAT < TEXT < BLOB
            (SqliteValue::Integer(_) | SqliteValue::Float(_), SqliteValue::Text(_) | SqliteValue::Blob(_)) => Less,
            (SqliteValue::Text(_) | SqliteValue::Blob(_), SqliteValue::Integer(_) | SqliteValue::Float(_)) => Greater,
            (SqliteValue::Text(a), SqliteValue::Text(b)) => a.as_bytes().cmp(b.as_bytes()),
            (SqliteValue::Blob(a), SqliteValue::Blob(b)) => a.cmp(b),
            (SqliteValue::Text(_), SqliteValue::Blob(_)) => Less,
            (SqliteValue::Blob(_), SqliteValue::Text(_)) => Greater,
        }
    }
}

/// Read a SQLite varint from a byte buffer. Returns (value, bytes_consumed).
/// SQLite varints are 1-9 bytes, big-endian, with high bit as continuation flag.
pub(crate) fn read_varint(buf: &[u8]) -> Option<(u64, usize)> {
    if buf.is_empty() {
        return None;
    }

    let mut value: u64 = 0;
    for i in 0..std::cmp::min(buf.len(), 9) {
        if i == 8 {
            // 9th byte: use all 8 bits
            value = (value << 8) | buf[i] as u64;
            return Some((value, 9));
        }
        value = (value << 7) | (buf[i] & 0x7F) as u64;
        if buf[i] & 0x80 == 0 {
            return Some((value, i + 1));
        }
    }
    None
}

/// Decode the size of a column value given its serial type code.
/// Returns the number of bytes the value occupies in the record body.
pub(crate) fn serial_type_size(type_code: u64) -> usize {
    match type_code {
        0 => 0,           // NULL
        1 => 1,           // 8-bit integer
        2 => 2,           // 16-bit integer
        3 => 3,           // 24-bit integer
        4 => 4,           // 32-bit integer
        5 => 6,           // 48-bit integer
        6 => 8,           // 64-bit integer
        7 => 8,           // IEEE 754 float
        8 => 0,           // integer 0
        9 => 0,           // integer 1
        10 | 11 => 0,     // reserved
        n if n >= 12 && n % 2 == 0 => ((n - 12) / 2) as usize,  // BLOB
        n if n >= 13 && n % 2 == 1 => ((n - 13) / 2) as usize,  // TEXT
        _ => 0,
    }
}

/// Read a column value from the record body given its serial type code.
pub(crate) fn read_column_value(buf: &[u8], type_code: u64) -> SqliteValue {
    match type_code {
        0 => SqliteValue::Null,
        1 => {
            if buf.is_empty() { return SqliteValue::Null; }
            SqliteValue::Integer(buf[0] as i8 as i64)
        }
        2 => {
            if buf.len() < 2 { return SqliteValue::Null; }
            SqliteValue::Integer(i16::from_be_bytes([buf[0], buf[1]]) as i64)
        }
        3 => {
            if buf.len() < 3 { return SqliteValue::Null; }
            let v = ((buf[0] as i32) << 16) | ((buf[1] as i32) << 8) | buf[2] as i32;
            // Sign-extend from 24 bits
            let v = if v & 0x800000 != 0 { v | !0xFFFFFF } else { v };
            SqliteValue::Integer(v as i64)
        }
        4 => {
            if buf.len() < 4 { return SqliteValue::Null; }
            SqliteValue::Integer(i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as i64)
        }
        5 => {
            if buf.len() < 6 { return SqliteValue::Null; }
            let v = ((buf[0] as i64) << 40) | ((buf[1] as i64) << 32) | ((buf[2] as i64) << 24)
                | ((buf[3] as i64) << 16) | ((buf[4] as i64) << 8) | buf[5] as i64;
            // Sign-extend from 48 bits
            let v = if v & 0x800000000000 != 0 { v | !0xFFFFFFFFFFFF } else { v };
            SqliteValue::Integer(v)
        }
        6 => {
            if buf.len() < 8 { return SqliteValue::Null; }
            SqliteValue::Integer(i64::from_be_bytes([buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7]]))
        }
        7 => {
            if buf.len() < 8 { return SqliteValue::Null; }
            SqliteValue::Float(f64::from_be_bytes([buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7]]))
        }
        8 => SqliteValue::Integer(0),
        9 => SqliteValue::Integer(1),
        n if n >= 12 && n % 2 == 0 => {
            let size = ((n - 12) / 2) as usize;
            if buf.len() < size { return SqliteValue::Null; }
            SqliteValue::Blob(buf[..size].to_vec())
        }
        n if n >= 13 && n % 2 == 1 => {
            let size = ((n - 13) / 2) as usize;
            if buf.len() < size { return SqliteValue::Null; }
            SqliteValue::Text(String::from_utf8_lossy(&buf[..size]).into_owned())
        }
        _ => SqliteValue::Null,
    }
}

/// Parse a SQLite record payload into column values.
/// Returns a vector of values, one per column.
pub(crate) fn parse_record(payload: &[u8]) -> Vec<SqliteValue> {
    if payload.is_empty() {
        return Vec::new();
    }

    // Read header size
    let (header_size, hdr_varint_len) = match read_varint(payload) {
        Some(v) => v,
        None => return Vec::new(),
    };
    let header_size = header_size as usize;

    if header_size > payload.len() {
        return Vec::new();
    }

    // Read type codes from header
    let mut types = Vec::new();
    let mut offset = hdr_varint_len;
    while offset < header_size {
        match read_varint(&payload[offset..]) {
            Some((type_code, len)) => {
                types.push(type_code);
                offset += len;
            }
            None => break,
        }
    }

    // Read values from body
    let mut body_offset = header_size;
    let mut values = Vec::with_capacity(types.len());
    for type_code in &types {
        let size = serial_type_size(*type_code);
        if body_offset + size > payload.len() {
            values.push(SqliteValue::Null);
            continue;
        }
        values.push(read_column_value(&payload[body_offset..], *type_code));
        body_offset += size;
    }

    values
}

/// Parse an interior cell to extract the key values.
///
/// For table interior pages (0x05): the key is just the rowid.
/// For index interior pages (0x02): the key is a full record.
///
/// `cell_data` starts at the cell offset (after cell pointer dereference).
/// `is_table_interior`: true for type 0x05, false for type 0x02.
///
/// Returns (left_child_page_1based, key_values).
pub(crate) fn parse_interior_cell(cell_data: &[u8], is_table_interior: bool) -> Option<(u32, Vec<SqliteValue>)> {
    if cell_data.len() < 4 {
        return None;
    }

    // 4-byte left child pointer
    let left_child = u32::from_be_bytes([cell_data[0], cell_data[1], cell_data[2], cell_data[3]]);
    let rest = &cell_data[4..];

    if is_table_interior {
        // Table interior: payload is just a varint rowid (the integer key)
        let (rowid, _) = read_varint(rest)?;
        Some((left_child, vec![SqliteValue::Integer(rowid as i64)]))
    } else {
        // Index interior: varint payload_size, then record payload
        let (payload_size, ps_len) = read_varint(rest)?;
        let payload_start = ps_len;
        let payload_end = payload_start + payload_size as usize;
        if payload_end > rest.len() {
            return None;
        }
        let values = parse_record(&rest[payload_start..payload_end]);
        Some((left_child, values))
    }
}

/// Parse a leaf cell to extract rowid and column values.
///
/// For table leaf pages (0x0D): varint payload_size, varint rowid, record payload.
/// For index leaf pages (0x0A): varint payload_size, record payload (no separate rowid).
///
/// Returns (rowid_or_none, column_values).
pub(crate) fn parse_leaf_cell(cell_data: &[u8], is_table_leaf: bool) -> Option<(Option<i64>, Vec<SqliteValue>)> {
    if cell_data.is_empty() {
        return None;
    }

    let (payload_size, ps_len) = read_varint(cell_data)?;
    let mut offset = ps_len;

    let rowid = if is_table_leaf {
        let (rid, rid_len) = read_varint(&cell_data[offset..])?;
        offset += rid_len;
        Some(rid as i64)
    } else {
        None
    };

    let payload_end = offset + payload_size as usize;
    if payload_end > cell_data.len() {
        // Overflow: only parse what's on this page
        let available = &cell_data[offset..];
        let values = parse_record(available);
        return Some((rowid, values));
    }

    let values = parse_record(&cell_data[offset..payload_end]);
    Some((rowid, values))
}

/// Extract all cells from a B-tree page.
/// Returns cell offsets (pointing into the page buffer).
pub(crate) fn cell_offsets(page: &[u8], hdr_off: usize) -> Vec<usize> {
    if page.len() < hdr_off + 8 {
        return Vec::new();
    }
    let cell_count = u16::from_be_bytes([page[hdr_off + 3], page[hdr_off + 4]]) as usize;
    let is_interior = page[hdr_off] == 0x05 || page[hdr_off] == 0x02;
    let page_header_size = if is_interior { 12 } else { 8 };
    let ptr_start = hdr_off + page_header_size;

    let mut offsets = Vec::with_capacity(cell_count);
    for i in 0..cell_count {
        let ptr_off = ptr_start + i * 2;
        if ptr_off + 2 > page.len() { break; }
        let cell_off = u16::from_be_bytes([page[ptr_off], page[ptr_off + 1]]) as usize;
        if cell_off < page.len() {
            offsets.push(cell_off);
        }
    }
    offsets
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_single_byte() {
        assert_eq!(read_varint(&[0x00]), Some((0, 1)));
        assert_eq!(read_varint(&[0x7F]), Some((127, 1)));
        assert_eq!(read_varint(&[0x01]), Some((1, 1)));
    }

    #[test]
    fn test_varint_two_bytes() {
        // 0x81 0x00 = 128
        assert_eq!(read_varint(&[0x81, 0x00]), Some((128, 2)));
        // 0x81 0x01 = 129
        assert_eq!(read_varint(&[0x81, 0x01]), Some((129, 2)));
    }

    #[test]
    fn test_varint_large() {
        // 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF = max
        let max = read_varint(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
        assert!(max.is_some());
        assert_eq!(max.unwrap().1, 9);
    }

    #[test]
    fn test_serial_type_sizes() {
        assert_eq!(serial_type_size(0), 0);  // NULL
        assert_eq!(serial_type_size(1), 1);  // 8-bit int
        assert_eq!(serial_type_size(4), 4);  // 32-bit int
        assert_eq!(serial_type_size(6), 8);  // 64-bit int
        assert_eq!(serial_type_size(7), 8);  // float
        assert_eq!(serial_type_size(8), 0);  // integer 0
        assert_eq!(serial_type_size(9), 0);  // integer 1
        assert_eq!(serial_type_size(12), 0); // zero-length blob
        assert_eq!(serial_type_size(14), 1); // 1-byte blob
        assert_eq!(serial_type_size(13), 0); // zero-length text
        assert_eq!(serial_type_size(15), 1); // 1-byte text
    }

    #[test]
    fn test_parse_record_integer() {
        // Header: 2 bytes (header_size=2, type=4 for 32-bit int)
        // Body: 4 bytes (0x00000042 = 66)
        let payload = vec![
            0x02, // header_size = 2
            0x04, // type code 4 = 32-bit integer
            0x00, 0x00, 0x00, 0x42, // value = 66
        ];
        let values = parse_record(&payload);
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], SqliteValue::Integer(66));
    }

    #[test]
    fn test_parse_record_text() {
        // type code 15 = 1-byte text
        let payload = vec![
            0x02, // header_size = 2
            0x0F, // type code 15 = 1-byte text
            b'A', // value = "A"
        ];
        let values = parse_record(&payload);
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], SqliteValue::Text("A".to_string()));
    }

    #[test]
    fn test_parse_record_multi_column() {
        // Two columns: integer 42, text "hi"
        // type 1 (8-bit int), type 17 (2-byte text)
        let payload = vec![
            0x03, // header_size = 3
            0x01, // type 1 = 8-bit int
            0x11, // type 17 = 2-byte text
            0x2A, // integer value = 42
            b'h', b'i', // text value = "hi"
        ];
        let values = parse_record(&payload);
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], SqliteValue::Integer(42));
        assert_eq!(values[1], SqliteValue::Text("hi".to_string()));
    }

    #[test]
    fn test_parse_record_null_and_constants() {
        // NULL, integer 0, integer 1
        let payload = vec![
            0x04, // header_size = 4
            0x00, // type 0 = NULL
            0x08, // type 8 = integer 0
            0x09, // type 9 = integer 1
            // no body needed for these types
        ];
        let values = parse_record(&payload);
        assert_eq!(values.len(), 3);
        assert_eq!(values[0], SqliteValue::Null);
        assert_eq!(values[1], SqliteValue::Integer(0));
        assert_eq!(values[2], SqliteValue::Integer(1));
    }

    #[test]
    fn test_value_comparison() {
        use std::cmp::Ordering::*;
        assert_eq!(SqliteValue::Null.cmp_binary(&SqliteValue::Integer(1)), Less);
        assert_eq!(SqliteValue::Integer(1).cmp_binary(&SqliteValue::Integer(2)), Less);
        assert_eq!(SqliteValue::Integer(5).cmp_binary(&SqliteValue::Text("a".into())), Less);
        assert_eq!(SqliteValue::Text("a".into()).cmp_binary(&SqliteValue::Text("b".into())), Less);
        assert_eq!(SqliteValue::Text("a".into()).cmp_binary(&SqliteValue::Blob(vec![0])), Less);
    }
}
