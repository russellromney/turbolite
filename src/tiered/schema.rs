//! Phase Jena-d2: Schema discovery for leaf chasing.
//!
//! Parses CREATE TABLE/INDEX statements from sqlite_master to extract
//! column names and their positions. This info is needed by the leaf chaser
//! to know which column to extract from a source table's leaf record.
//!
//! Schema info is pushed from the extension's trace callback (which has
//! db access) and consumed by the VFS (which only sees page reads).

use std::collections::HashMap;
use std::sync::Mutex;

/// Global schema cache. Populated by the extension, consumed by the VFS.
static SCHEMA_CACHE: Mutex<Option<SchemaInfo>> = Mutex::new(None);

/// Parsed schema info for all tables and indexes.
#[derive(Debug, Clone, Default)]
pub(crate) struct SchemaInfo {
    /// table_name -> ordered column names.
    pub table_columns: HashMap<String, Vec<String>>,
    /// index_name -> ordered column names (from CREATE INDEX).
    pub index_columns: HashMap<String, Vec<String>>,
    /// tree_name (table or index) -> root page (0-based).
    pub tree_roots: HashMap<String, u64>,
}

/// Push schema info to the global cache (called from extension trace callback).
pub fn push_schema(info: SchemaInfo) {
    *SCHEMA_CACHE.lock().unwrap() = Some(info);
}

/// Take schema info from the global cache (called from VFS).
/// Returns None if no schema has been pushed yet.
pub(crate) fn take_schema() -> Option<SchemaInfo> {
    SCHEMA_CACHE.lock().unwrap().take()
}

/// Peek at schema info without consuming it.
pub(crate) fn peek_schema() -> Option<SchemaInfo> {
    SCHEMA_CACHE.lock().unwrap().clone()
}

/// Parse a CREATE TABLE statement to extract column names in order.
///
/// Handles basic syntax: `CREATE TABLE name (col1 TYPE, col2 TYPE, ...)`
/// Does NOT handle: virtual tables, generated columns, complex constraints.
pub(crate) fn parse_create_table(sql: &str) -> Option<(String, Vec<String>)> {
    let sql = sql.trim();
    // Case-insensitive prefix match
    let upper = sql.to_uppercase();
    if !upper.starts_with("CREATE TABLE") {
        return None;
    }

    // Skip "CREATE TABLE [IF NOT EXISTS] name"
    let after_create = &sql["CREATE TABLE".len()..].trim_start();
    let after_ifne = if after_create.to_uppercase().starts_with("IF NOT EXISTS") {
        after_create["IF NOT EXISTS".len()..].trim_start()
    } else {
        after_create
    };

    // Table name (possibly quoted)
    let (table_name, rest) = parse_identifier(after_ifne)?;

    // Find the opening parenthesis
    let rest = rest.trim_start();
    if !rest.starts_with('(') {
        return None;
    }
    let inside = &rest[1..];

    // Find matching closing parenthesis
    let close_paren = find_matching_paren(inside)?;
    let columns_str = &inside[..close_paren];

    // Split by commas (respecting parentheses for constraints)
    let parts = split_column_defs(columns_str);
    let mut columns = Vec::new();

    for part in &parts {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        // Skip table constraints: PRIMARY KEY(...), UNIQUE(...), CHECK(...), FOREIGN KEY(...)
        let upper_part = part.to_uppercase();
        if upper_part.starts_with("PRIMARY KEY")
            || upper_part.starts_with("UNIQUE")
            || upper_part.starts_with("CHECK")
            || upper_part.starts_with("FOREIGN KEY")
            || upper_part.starts_with("CONSTRAINT")
        {
            continue;
        }
        // Column name is the first token
        if let Some((col_name, _)) = parse_identifier(part) {
            columns.push(col_name);
        }
    }

    Some((table_name, columns))
}

/// Parse a CREATE INDEX statement to extract column names.
///
/// Handles: `CREATE [UNIQUE] INDEX name ON table (col1, col2, ...)`
pub(crate) fn parse_create_index(sql: &str) -> Option<(String, String, Vec<String>)> {
    let sql = sql.trim();
    let upper = sql.to_uppercase();
    if !upper.starts_with("CREATE") {
        return None;
    }

    // Skip "CREATE [UNIQUE] INDEX [IF NOT EXISTS] name ON table (...)"
    let mut rest: &str = sql["CREATE".len()..].trim_start();
    if rest.to_uppercase().starts_with("UNIQUE") {
        rest = rest["UNIQUE".len()..].trim_start();
    }
    if !rest.to_uppercase().starts_with("INDEX") {
        return None;
    }
    rest = rest["INDEX".len()..].trim_start();
    if rest.to_uppercase().starts_with("IF NOT EXISTS") {
        rest = rest["IF NOT EXISTS".len()..].trim_start();
    }

    // Index name
    let (index_name, rest) = parse_identifier(rest)?;
    let rest = rest.trim_start();

    // ON keyword
    if !rest.to_uppercase().starts_with("ON") {
        return None;
    }
    let rest = rest["ON".len()..].trim_start();

    // Table name
    let (_table_name, rest) = parse_identifier(rest)?;
    let rest = rest.trim_start();

    // Column list in parentheses
    if !rest.starts_with('(') {
        return None;
    }
    let inside = &rest[1..];
    let close = find_matching_paren(inside)?;
    let cols_str = &inside[..close];

    let mut columns = Vec::new();
    for part in cols_str.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        // Column name, possibly with ASC/DESC/COLLATE suffix
        if let Some((col_name, _)) = parse_identifier(part) {
            columns.push(col_name);
        }
    }

    Some((index_name, _table_name.to_string(), columns))
}

/// Parse a possibly-quoted identifier from the start of a string.
/// Returns (identifier, rest_of_string).
fn parse_identifier(s: &str) -> Option<(String, &str)> {
    let s = s.trim_start();
    if s.is_empty() {
        return None;
    }

    if s.starts_with('"') {
        // Double-quoted identifier
        let end = s[1..].find('"')? + 1;
        let name = s[1..end].to_string();
        Some((name, &s[end + 1..]))
    } else if s.starts_with('[') {
        // Bracket-quoted identifier
        let end = s[1..].find(']')? + 1;
        let name = s[1..end].to_string();
        Some((name, &s[end + 1..]))
    } else if s.starts_with('`') {
        // Backtick-quoted identifier
        let end = s[1..].find('`')? + 1;
        let name = s[1..end].to_string();
        Some((name, &s[end + 1..]))
    } else {
        // Unquoted: take chars until whitespace or special char
        let end = s.find(|c: char| c.is_whitespace() || c == '(' || c == ')' || c == ',' || c == ';')
            .unwrap_or(s.len());
        if end == 0 {
            return None;
        }
        Some((s[..end].to_string(), &s[end..]))
    }
}

/// Find the index of the matching closing parenthesis.
fn find_matching_paren(s: &str) -> Option<usize> {
    let mut depth = 0;
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                if depth == 0 {
                    return Some(i);
                }
                depth -= 1;
            }
            _ => {}
        }
    }
    None
}

/// Split column definitions by commas, respecting parenthesized expressions.
fn split_column_defs(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0;
    let mut start = 0;

    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < s.len() {
        parts.push(&s[start..]);
    }
    parts
}

/// Build SchemaInfo from sqlite_master rows.
///
/// `rows` is a list of (type, name, tbl_name, rootpage, sql) tuples,
/// matching sqlite_master's schema.
pub(crate) fn build_schema_info(
    rows: &[(String, String, String, i64, Option<String>)],
) -> SchemaInfo {
    let mut info = SchemaInfo::default();

    for (obj_type, name, _tbl_name, rootpage, sql) in rows {
        // Root page: sqlite_master uses 1-based, we use 0-based
        let root_0based = if *rootpage > 0 { (*rootpage - 1) as u64 } else { 0 };
        info.tree_roots.insert(name.clone(), root_0based);

        if let Some(sql) = sql {
            match obj_type.as_str() {
                "table" => {
                    if let Some((table_name, columns)) = parse_create_table(sql) {
                        info.table_columns.insert(table_name, columns);
                    }
                }
                "index" => {
                    if let Some((index_name, _table, columns)) = parse_create_index(sql) {
                        info.index_columns.insert(index_name, columns);
                    }
                }
                _ => {}
            }
        }
    }

    info
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_create_table_basic() {
        let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)";
        let (name, cols) = parse_create_table(sql).unwrap();
        assert_eq!(name, "users");
        assert_eq!(cols, vec!["id", "name", "email"]);
    }

    #[test]
    fn test_parse_create_table_if_not_exists() {
        let sql = "CREATE TABLE IF NOT EXISTS posts (id INTEGER PRIMARY KEY, user_id INTEGER, content TEXT)";
        let (name, cols) = parse_create_table(sql).unwrap();
        assert_eq!(name, "posts");
        assert_eq!(cols, vec!["id", "user_id", "content"]);
    }

    #[test]
    fn test_parse_create_table_with_constraints() {
        let sql = "CREATE TABLE orders (id INTEGER, user_id INTEGER, total REAL, PRIMARY KEY(id), FOREIGN KEY(user_id) REFERENCES users(id))";
        let (name, cols) = parse_create_table(sql).unwrap();
        assert_eq!(name, "orders");
        assert_eq!(cols, vec!["id", "user_id", "total"]);
    }

    #[test]
    fn test_parse_create_table_quoted() {
        let sql = r#"CREATE TABLE "my table" ("col 1" TEXT, "col 2" INTEGER)"#;
        let (name, cols) = parse_create_table(sql).unwrap();
        assert_eq!(name, "my table");
        assert_eq!(cols, vec!["col 1", "col 2"]);
    }

    #[test]
    fn test_parse_create_index() {
        let sql = "CREATE INDEX idx_posts_user ON posts (user_id)";
        let (idx, _tbl, cols) = parse_create_index(sql).unwrap();
        assert_eq!(idx, "idx_posts_user");
        assert_eq!(cols, vec!["user_id"]);
    }

    #[test]
    fn test_parse_create_index_composite() {
        let sql = "CREATE UNIQUE INDEX idx_orders_user_date ON orders (user_id, created_at DESC)";
        let (idx, _tbl, cols) = parse_create_index(sql).unwrap();
        assert_eq!(idx, "idx_orders_user_date");
        // DESC is a modifier, not a column name, but our parser takes the first token
        assert_eq!(cols, vec!["user_id", "created_at"]);
    }

    #[test]
    fn test_build_schema_info() {
        let rows = vec![
            ("table".into(), "users".into(), "users".into(), 2, Some("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)".into())),
            ("index".into(), "idx_users_name".into(), "users".into(), 3, Some("CREATE INDEX idx_users_name ON users (name)".into())),
            ("table".into(), "posts".into(), "posts".into(), 4, Some("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, content TEXT)".into())),
        ];
        let info = build_schema_info(&rows);

        assert_eq!(info.table_columns["users"], vec!["id", "name"]);
        assert_eq!(info.table_columns["posts"], vec!["id", "user_id", "content"]);
        assert_eq!(info.index_columns["idx_users_name"], vec!["name"]);
        // Root pages: 0-based
        assert_eq!(info.tree_roots["users"], 1);  // sqlite_master rootpage=2 -> 0-based=1
        assert_eq!(info.tree_roots["idx_users_name"], 2);
        assert_eq!(info.tree_roots["posts"], 3);
    }

    #[test]
    fn test_global_schema_cache() {
        // Clear any leftover
        let _ = take_schema();

        assert!(peek_schema().is_none());

        let info = SchemaInfo {
            table_columns: {
                let mut m = HashMap::new();
                m.insert("t".into(), vec!["a".into(), "b".into()]);
                m
            },
            ..Default::default()
        };
        push_schema(info);

        let peeked = peek_schema().unwrap();
        assert_eq!(peeked.table_columns["t"], vec!["a", "b"]);

        let taken = take_schema().unwrap();
        assert_eq!(taken.table_columns["t"], vec!["a", "b"]);

        // Consumed
        assert!(take_schema().is_none());
    }
}
