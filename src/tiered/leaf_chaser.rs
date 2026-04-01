//! Phase Jena-d: Cross-tree leaf chasing.
//!
//! When a leaf page arrives from table A, parse its cells and extract
//! join column values. Use those values to predict which leaf groups in
//! table B will be needed, and prefetch them before SQLite asks.
//!
//! This turns sequential cross-table blocking faults into pipelined
//! parallel prefetch. For a 5-table join, all tables' groups can be
//! fetching concurrently.

use std::collections::HashMap;

use super::record_parser::{self, SqliteValue};
use super::interior_map::InteriorMap;
use super::query_plan::{PlannedAccess, AccessType};

/// A chase rule: "when reading a leaf from source_tree, extract column at
/// position col_index, and use it to predict_leaf on target_root_page."
#[derive(Debug, Clone)]
pub(crate) struct ChaseRule {
    /// Tree name we're chasing FROM (the source leaf).
    pub source_tree: String,
    /// Column index in the source leaf's record to extract as the chase key.
    /// For table leaves (0x0D): column index in the row record.
    /// For index leaves (0x0A): column index in the index record.
    pub col_index: usize,
    /// Root page (0-based) of the target tree to predict_leaf on.
    pub target_root_page: u64,
    /// Target tree name (for logging).
    pub target_tree: String,
}

/// Build chase rules from a planned access list.
///
/// For each consecutive pair of SEARCH accesses in a join, if the second
/// access has constraint columns, try to find the matching column in the
/// first access's table. This requires schema info (column positions),
/// which we approximate from the index definition.
///
/// `tree_roots` maps tree_name -> root_page (0-based).
/// `index_columns` maps index_name -> ordered column names from the CREATE INDEX.
///
/// Returns chase rules, possibly empty if no join chasing is possible.
pub(crate) fn build_chase_rules(
    plan: &[PlannedAccess],
    tree_roots: &HashMap<String, u64>,
    table_columns: &HashMap<String, Vec<String>>,
) -> Vec<ChaseRule> {
    let mut rules = Vec::new();

    // For each consecutive pair of accesses in the plan:
    // If access[i] is a table scan/search producing rows,
    // and access[i+1] is a SEARCH with constraint columns,
    // then the constraint columns tell us what to extract from access[i]'s rows.
    for window in plan.windows(2) {
        let source = &window[0];
        let target = &window[1];

        // Target must be a SEARCH with constraints
        if target.access_type != AccessType::Search || target.constraint_columns.is_empty() {
            continue;
        }

        let target_tree = &target.tree_name;
        let target_root = match tree_roots.get(target_tree) {
            Some(&r) => r,
            None => continue,
        };

        // The constraint column on the target tells us what value we need.
        // We need to find this column in the source table's row.
        let source_table = match &source.table_name {
            Some(t) => t,
            None => continue,
        };

        // Get source table's column list
        let source_cols = match table_columns.get(source_table) {
            Some(cols) => cols,
            None => continue,
        };

        // For each constraint column on the target, find its position in
        // the source table. The constraint name from EQP is the column name
        // on the TARGET index, but for a join like `ON a.user_id = b.id`,
        // the source column might have a different name (user_id vs id).
        //
        // Heuristic: check if any source column matches any target constraint
        // column by name. This works for self-referential joins and common
        // naming patterns (id=id, user_id=user_id).
        //
        // For different names (a.user_id = b.id), we'd need the actual SQL
        // parse tree, which the VFS doesn't have. In that case, we skip.
        for target_col in &target.constraint_columns {
            // FK heuristic first (more specific): source "user_id" matches
            // target "id" on table "users".
            if let Some(found_idx) = source_cols.iter().position(|c| {
                fk_column_matches(c, target_col, target.table_name.as_deref())
            }) {
                rules.push(ChaseRule {
                    source_tree: source.tree_name.clone(),
                    col_index: found_idx,
                    target_root_page: target_root,
                    target_tree: target_tree.clone(),
                });
                break;
            }
            // Fallback: exact column name match (self-joins, same-name FKs)
            if let Some(col_idx) = source_cols.iter().position(|c| c == target_col) {
                rules.push(ChaseRule {
                    source_tree: source.tree_name.clone(),
                    col_index: col_idx,
                    target_root_page: target_root,
                    target_tree: target_tree.clone(),
                });
                break;
            }
        }
    }

    rules
}

/// FK column matching heuristic.
///
/// Checks if `source_col` (e.g., "user_id") could be a foreign key pointing
/// to `target_col` (e.g., "id") on `target_table` (e.g., "users").
///
/// Patterns matched:
/// - "user_id" -> strip "_id" -> "user" matches table "users" (singular/plural)
/// - "author_fk" -> strip "_fk" -> "author" matches table "authors"
/// - "parent_id" -> strip "_id" -> "parent" (if target_col is "id")
fn fk_column_matches(source_col: &str, target_col: &str, target_table: Option<&str>) -> bool {
    let source_lower = source_col.to_lowercase();
    let target_col_lower = target_col.to_lowercase();

    // Strip common FK suffixes
    let prefix = if let Some(p) = source_lower.strip_suffix("_id") {
        p
    } else if let Some(p) = source_lower.strip_suffix("_fk") {
        p
    } else {
        return false;
    };

    // If target constraint is "id" or "rowid", the FK prefix should match
    // the target table name (with basic singular/plural handling)
    if target_col_lower == "id" || target_col_lower == "rowid" {
        if let Some(table) = target_table {
            let table_lower = table.to_lowercase();
            // "user" matches "users", "users" matches "users", "user" matches "user"
            return table_lower == prefix
                || table_lower == format!("{}s", prefix)
                || table_lower.strip_suffix('s') == Some(prefix);
        }
        // No table info, can't match
        return false;
    }

    // If target constraint matches the prefix (e.g., source "category_id", target "category")
    prefix == target_col_lower

}

/// Extract chase keys from a leaf page buffer.
///
/// Parses all cells in the leaf page, extracts the column at `col_index`,
/// and returns unique values suitable for predict_leaf.
///
/// `is_table_leaf`: true for type 0x0D (table leaf), false for 0x0A (index leaf).
pub(crate) fn extract_chase_keys(
    page: &[u8],
    hdr_off: usize,
    col_index: usize,
    is_table_leaf: bool,
    max_keys: usize,
) -> Vec<SqliteValue> {
    let cell_offs = record_parser::cell_offsets(page, hdr_off);
    let mut keys = Vec::new();
    let mut seen_ints = std::collections::HashSet::new();

    for &cell_off in &cell_offs {
        if keys.len() >= max_keys {
            break;
        }
        let cell_data = &page[cell_off..];
        let parsed = if is_table_leaf {
            record_parser::parse_leaf_cell(cell_data, true)
        } else {
            record_parser::parse_leaf_cell(cell_data, false)
        };
        if let Some((_rowid, values)) = parsed {
            if col_index < values.len() {
                let val = &values[col_index];
                // Deduplicate integer keys (common for FK columns)
                match val {
                    SqliteValue::Integer(n) => {
                        if seen_ints.insert(*n) {
                            keys.push(val.clone());
                        }
                    }
                    _ => keys.push(val.clone()),
                }
            }
        }
    }

    keys
}

/// Execute chase: for each key, predict the leaf group in the target tree
/// and return group IDs to prefetch.
pub(crate) fn chase_predict_groups(
    keys: &[SqliteValue],
    target_root_page: u64,
    interior_map: &InteriorMap,
) -> Vec<u64> {
    let mut groups = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for key in keys {
        if let Some((_leaf_page, gid)) = interior_map.predict_leaf(target_root_page, &[key.clone()]) {
            if seen.insert(gid) {
                groups.push(gid);
            }
        }
    }

    groups
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_chase_rules_self_join_exact_match() {
        // Self-join: employees.manager_id = employees.id (same table, exact column name)
        let plan = vec![
            PlannedAccess {
                tree_name: "employees".into(),
                access_type: AccessType::Scan,
                table_name: Some("employees".into()),
                constraint_columns: vec![],
            },
            PlannedAccess {
                tree_name: "idx_emp_name".into(),
                access_type: AccessType::Search,
                table_name: Some("employees".into()),
                constraint_columns: vec!["name".into()],
            },
        ];

        let mut tree_roots = HashMap::new();
        tree_roots.insert("idx_emp_name".into(), 5);

        let mut table_columns = HashMap::new();
        table_columns.insert("employees".into(), vec!["id".into(), "name".into(), "manager_id".into()]);

        let rules = build_chase_rules(&plan, &tree_roots, &table_columns);
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].col_index, 1); // "name" is column 1 (exact match)
    }

    #[test]
    fn test_build_chase_rules_no_match() {
        let plan = vec![
            PlannedAccess {
                tree_name: "orders".into(),
                access_type: AccessType::Scan,
                table_name: Some("orders".into()),
                constraint_columns: vec![],
            },
            PlannedAccess {
                tree_name: "idx_products_sku".into(),
                access_type: AccessType::Search,
                table_name: Some("products".into()),
                constraint_columns: vec!["sku".into()],
            },
        ];

        let mut tree_roots = HashMap::new();
        tree_roots.insert("idx_products_sku".into(), 10);

        // orders has no "sku" column -> no chase rule
        let mut table_columns = HashMap::new();
        table_columns.insert("orders".into(), vec!["id".into(), "product_id".into()]);

        let rules = build_chase_rules(&plan, &tree_roots, &table_columns);
        assert!(rules.is_empty());
    }

    #[test]
    fn test_fk_column_matching() {
        // user_id -> id on users table
        assert!(fk_column_matches("user_id", "id", Some("users")));
        // author_id -> id on authors table
        assert!(fk_column_matches("author_id", "id", Some("authors")));
        // author_fk -> id on authors table
        assert!(fk_column_matches("author_fk", "id", Some("authors")));
        // user_id -> id on user table (singular)
        assert!(fk_column_matches("user_id", "id", Some("user")));
        // no match: post_id -> id on users table
        assert!(!fk_column_matches("post_id", "id", Some("users")));
        // no match: name -> id
        assert!(!fk_column_matches("name", "id", Some("users")));
        // category_id -> category (non-id target)
        assert!(fk_column_matches("category_id", "category", None));
    }

    #[test]
    fn test_build_chase_rules_fk_heuristic() {
        // posts.user_id = users.id (different column names)
        let plan = vec![
            PlannedAccess {
                tree_name: "posts".into(),
                access_type: AccessType::Scan,
                table_name: Some("posts".into()),
                constraint_columns: vec![],
            },
            PlannedAccess {
                tree_name: "idx_users_id".into(),
                access_type: AccessType::Search,
                table_name: Some("users".into()),
                constraint_columns: vec!["id".into()],
            },
        ];

        let mut tree_roots = HashMap::new();
        tree_roots.insert("idx_users_id".into(), 5);

        let mut table_columns = HashMap::new();
        table_columns.insert("posts".into(), vec!["id".into(), "user_id".into(), "content".into()]);

        let rules = build_chase_rules(&plan, &tree_roots, &table_columns);
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].source_tree, "posts");
        assert_eq!(rules[0].col_index, 1); // "user_id" is column 1 in posts
        assert_eq!(rules[0].target_tree, "idx_users_id");
    }

    #[test]
    fn test_chase_predict_groups() {
        let mut map = InteriorMap::default();
        // Simple tree: root 0 -> children [10, 11, 12]
        // key boundaries: 100, 200
        map.parent_to_children.insert(0, vec![10, 11, 12]);
        map.key_boundaries.insert(0, vec![
            super::super::interior_map::KeyBoundary { values: vec![SqliteValue::Integer(100)] },
            super::super::interior_map::KeyBoundary { values: vec![SqliteValue::Integer(200)] },
        ]);
        map.is_table_interior.insert(0, true);
        map.page_to_group.insert(10, 1);
        map.page_to_group.insert(11, 2);
        map.page_to_group.insert(12, 3);

        let keys = vec![
            SqliteValue::Integer(50),   // -> group 1
            SqliteValue::Integer(150),  // -> group 2
            SqliteValue::Integer(250),  // -> group 3
            SqliteValue::Integer(50),   // duplicate, ignored
        ];

        let groups = chase_predict_groups(&keys, 0, &map);
        assert_eq!(groups, vec![1, 2, 3]);
    }

    #[test]
    fn test_extract_constraint_columns() {
        use super::super::query_plan::parse_eqp_output;

        let eqp = "SEARCH TABLE users USING INDEX idx_users_email (email=?)";
        let accesses = parse_eqp_output(eqp);
        // Find the index access
        let idx_access = accesses.iter().find(|a| a.tree_name == "idx_users_email").unwrap();
        assert_eq!(idx_access.constraint_columns, vec!["email"]);
        assert_eq!(idx_access.table_name, Some("users".to_string()));
    }

    #[test]
    fn test_extract_constraint_columns_composite() {
        use super::super::query_plan::parse_eqp_output;

        let eqp = "SEARCH TABLE orders USING INDEX idx_orders_user_date (user_id=? AND date>?)";
        let accesses = parse_eqp_output(eqp);
        let idx_access = accesses.iter().find(|a| a.tree_name == "idx_orders_user_date").unwrap();
        assert_eq!(idx_access.constraint_columns, vec!["user_id", "date"]);
    }
}
