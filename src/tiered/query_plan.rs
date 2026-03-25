//! Phase Marne: Query-plan-aware prefetch.
//!
//! A global queue of planned B-tree accesses populated by the trace callback
//! (in ext_entry.c) and drained by the VFS on first cache miss.
//!
//! The trace callback runs EXPLAIN QUERY PLAN on each SQL statement at the
//! start of sqlite3_step(), extracts SCAN/SEARCH + table/index names, and
//! pushes PlannedAccess entries to the queue. The VFS drains the queue on
//! first cache miss and submits all planned groups to the prefetch pool.
//!
//! When the queue is empty (extension not loaded, or DDL/PRAGMA), the VFS
//! falls back to the hop schedule.

use std::sync::Mutex;

/// Access type from EXPLAIN QUERY PLAN output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum AccessType {
    /// SEARCH: index lookup. Prefetch index groups only.
    Search = 0,
    /// SCAN: full table scan. Prefetch all groups (index + data).
    Scan = 1,
}

/// A single planned B-tree access extracted from EQP output.
#[derive(Debug, Clone)]
pub struct PlannedAccess {
    /// Table or index name (matches manifest's tree_name_to_groups keys).
    pub tree_name: String,
    /// SCAN or SEARCH.
    pub access_type: AccessType,
}

/// Global queue of planned accesses. The trace callback pushes, VFS drains.
///
/// Using a simple Mutex<Vec> because:
/// - Push path (trace callback): runs once per step(), ~10us EQP cost dominates
/// - Drain path (VFS read): one drain per query on first cache miss
/// - No contention in practice: push completes before drain starts (synchronous trace)
static PLAN_QUEUE: Mutex<Vec<PlannedAccess>> = Mutex::new(Vec::new());

/// Push planned accesses to the global queue (called from trace callback).
pub fn push_planned_accesses(accesses: Vec<PlannedAccess>) {
    if accesses.is_empty() {
        return;
    }
    let mut queue = PLAN_QUEUE.lock().expect("plan queue poisoned");
    queue.extend(accesses);
}

/// Drain all planned accesses from the queue (called from VFS on first cache miss).
/// Returns an empty Vec if nothing is queued.
pub fn drain_planned_accesses() -> Vec<PlannedAccess> {
    let mut queue = PLAN_QUEUE.lock().expect("plan queue poisoned");
    if queue.is_empty() {
        return Vec::new();
    }
    std::mem::take(&mut *queue)
}

/// Parse EXPLAIN QUERY PLAN output text into PlannedAccess entries.
///
/// EQP output rows look like:
///   "SEARCH users USING INDEX idx_users_email (email=?)"
///   "SCAN posts"
///   "SEARCH posts USING INDEX idx_posts_user_id (user_id=?)"
///   "SEARCH posts USING COVERING INDEX idx_posts_user_id (user_id=?)"
///   "SCAN posts USING INDEX idx_posts_created"
///
/// We extract:
/// - SCAN vs SEARCH
/// - The table name (word after SCAN/SEARCH)
/// - The index name if "USING INDEX" or "USING COVERING INDEX" is present
///
/// For SEARCH: we emit the index name (that's the tree being traversed).
/// For SCAN: we emit the table name (all data groups needed).
///   If SCAN uses an index (SCAN ... USING INDEX), we also emit the index.
pub fn parse_eqp_output(eqp_text: &str) -> Vec<PlannedAccess> {
    let mut accesses = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for line in eqp_text.lines() {
        let trimmed = line.trim();

        // Find SCAN or SEARCH token
        let (access_type, rest) = if let Some(pos) = trimmed.find("SCAN") {
            (AccessType::Scan, &trimmed[pos + 4..])
        } else if let Some(pos) = trimmed.find("SEARCH") {
            (AccessType::Search, &trimmed[pos + 6..])
        } else {
            continue;
        };

        let rest = rest.trim();

        // Next word is the table name
        let table_name = match rest.split_whitespace().next() {
            Some(name) => name,
            None => continue,
        };

        // For SCAN: always emit the table (we need all data groups)
        if access_type == AccessType::Scan {
            if seen.insert((table_name.to_string(), AccessType::Scan)) {
                accesses.push(PlannedAccess {
                    tree_name: table_name.to_string(),
                    access_type: AccessType::Scan,
                });
            }
        }

        // Check for USING [COVERING] INDEX <index_name>
        if let Some(idx_pos) = rest.find("USING INDEX").or_else(|| rest.find("USING COVERING INDEX")) {
            let after_using = &rest[idx_pos..];
            // Skip "USING INDEX " or "USING COVERING INDEX "
            let idx_name_start = if after_using.starts_with("USING COVERING INDEX") {
                "USING COVERING INDEX ".len()
            } else {
                "USING INDEX ".len()
            };
            if let Some(idx_name) = after_using.get(idx_name_start..)
                .and_then(|s| s.split_whitespace().next())
            {
                let idx_access = match access_type {
                    // SEARCH via index: emit the index as Search
                    AccessType::Search => AccessType::Search,
                    // SCAN via index: emit the index as Scan (full index scan)
                    AccessType::Scan => AccessType::Scan,
                };
                if seen.insert((idx_name.to_string(), idx_access)) {
                    accesses.push(PlannedAccess {
                        tree_name: idx_name.to_string(),
                        access_type: idx_access,
                    });
                }
            }
        } else if access_type == AccessType::Search {
            // SEARCH without explicit index (e.g., rowid lookup): emit table as Search
            if seen.insert((table_name.to_string(), AccessType::Search)) {
                accesses.push(PlannedAccess {
                    tree_name: table_name.to_string(),
                    access_type: AccessType::Search,
                });
            }
        }
    }

    accesses
}

/// Run EXPLAIN QUERY PLAN on a SQL string and return planned accesses.
/// Called from the C trace callback via FFI.
///
/// Returns a Vec of PlannedAccess. Returns empty Vec on error (non-fatal:
/// the VFS falls back to the hop schedule).
///
/// # Safety
/// `db` must be a valid sqlite3 database handle.
pub unsafe fn run_eqp_and_parse(db: *mut std::ffi::c_void, sql: &str) -> Vec<PlannedAccess> {
    // Skip statements that won't benefit from prefetch
    let upper = sql.trim();
    if upper.is_empty() {
        return Vec::new();
    }
    // Fast check: skip non-SELECT and non-WITH (CTE) statements
    let first_word = upper.split_whitespace().next().unwrap_or("");
    match first_word.to_uppercase().as_str() {
        "SELECT" | "WITH" => {}
        _ => return Vec::new(),
    }

    let eqp_sql = format!("EXPLAIN QUERY PLAN {}", sql);
    let c_sql = match std::ffi::CString::new(eqp_sql) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };

    let mut stmt: *mut std::ffi::c_void = std::ptr::null_mut();
    let mut tail: *const std::ffi::c_char = std::ptr::null();

    let rc = sqlite3_prepare_v2(
        db,
        c_sql.as_ptr(),
        -1,
        &mut stmt as *mut *mut std::ffi::c_void as *mut *mut _,
        &mut tail as *mut *const std::ffi::c_char as *mut *const _,
    );
    if rc != 0 || stmt.is_null() {
        return Vec::new();
    }

    let mut output = String::new();
    loop {
        let step_rc = sqlite3_step(stmt);
        if step_rc == 100 {
            // SQLITE_ROW
            // EQP columns: id, parent, notused, detail
            // detail is column 3
            let detail_ptr = sqlite3_column_text(stmt, 3);
            if !detail_ptr.is_null() {
                if let Ok(detail) = std::ffi::CStr::from_ptr(detail_ptr as *const _).to_str() {
                    output.push_str(detail);
                    output.push('\n');
                }
            }
        } else {
            break;
        }
    }

    sqlite3_finalize(stmt);

    parse_eqp_output(&output)
}

// SQLite C API functions. In the loadable extension these resolve to shims
// in ext_entry.c that route through the API table. When linked directly
// (e.g., rusqlite bundled builds), these are the real sqlite3 symbols.
extern "C" {
    fn sqlite3_prepare_v2(
        db: *mut std::ffi::c_void,
        sql: *const std::ffi::c_char,
        nbyte: i32,
        stmt: *mut *mut std::ffi::c_void,
        tail: *mut *const std::ffi::c_char,
    ) -> i32;

    fn sqlite3_step(stmt: *mut std::ffi::c_void) -> i32;

    // Returns const unsigned char* in C. We treat it as c_char since we
    // immediately pass it to CStr::from_ptr which expects *const c_char.
    fn sqlite3_column_text(stmt: *mut std::ffi::c_void, col: i32) -> *const std::ffi::c_char;

    fn sqlite3_finalize(stmt: *mut std::ffi::c_void) -> i32;
}

/// FFI entry point called from C trace callback.
/// Runs EQP, parses, and pushes to global queue.
///
/// # Safety
/// `db` must be a valid sqlite3 handle. `sql` must be a valid C string.
#[no_mangle]
pub unsafe extern "C" fn turbolite_trace_push_plan(
    db: *mut std::ffi::c_void,
    sql: *const std::ffi::c_char,
) {
    if sql.is_null() {
        return;
    }
    let sql_str = match std::ffi::CStr::from_ptr(sql).to_str() {
        Ok(s) => s,
        Err(_) => return,
    };

    let accesses = run_eqp_and_parse(db, sql_str);
    push_planned_accesses(accesses);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_search_with_index() {
        let eqp = "SEARCH users USING INDEX idx_users_email (email=?)";
        let result = parse_eqp_output(eqp);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].tree_name, "idx_users_email");
        assert_eq!(result[0].access_type, AccessType::Search);
    }

    #[test]
    fn test_parse_scan() {
        let eqp = "SCAN posts";
        let result = parse_eqp_output(eqp);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].tree_name, "posts");
        assert_eq!(result[0].access_type, AccessType::Scan);
    }

    #[test]
    fn test_parse_scan_with_index() {
        let eqp = "SCAN posts USING INDEX idx_posts_created";
        let result = parse_eqp_output(eqp);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].tree_name, "posts");
        assert_eq!(result[0].access_type, AccessType::Scan);
        assert_eq!(result[1].tree_name, "idx_posts_created");
        assert_eq!(result[1].access_type, AccessType::Scan);
    }

    #[test]
    fn test_parse_search_rowid() {
        let eqp = "SEARCH users USING INTEGER PRIMARY KEY (rowid=?)";
        let result = parse_eqp_output(eqp);
        // No "USING INDEX", falls back to table name as Search
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].tree_name, "users");
        assert_eq!(result[0].access_type, AccessType::Search);
    }

    #[test]
    fn test_parse_join() {
        let eqp = "\
SEARCH users USING INDEX sqlite_autoindex_users_1 (id=?)
SEARCH posts USING INDEX idx_posts_user_id (user_id=?)
SCAN likes";
        let result = parse_eqp_output(eqp);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].tree_name, "sqlite_autoindex_users_1");
        assert_eq!(result[0].access_type, AccessType::Search);
        assert_eq!(result[1].tree_name, "idx_posts_user_id");
        assert_eq!(result[1].access_type, AccessType::Search);
        assert_eq!(result[2].tree_name, "likes");
        assert_eq!(result[2].access_type, AccessType::Scan);
    }

    #[test]
    fn test_parse_covering_index() {
        let eqp = "SEARCH users USING COVERING INDEX idx_users_email_name (email=?)";
        let result = parse_eqp_output(eqp);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].tree_name, "idx_users_email_name");
        assert_eq!(result[0].access_type, AccessType::Search);
    }

    #[test]
    fn test_parse_deduplicates() {
        let eqp = "\
SCAN users
SCAN users";
        let result = parse_eqp_output(eqp);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_parse_ignores_non_scan_search() {
        let eqp = "\
USE TEMP B-TREE FOR ORDER BY
COMPOUND SUBQUERY 1";
        let result = parse_eqp_output(eqp);
        assert_eq!(result.is_empty(), true);
    }

    #[test]
    fn test_queue_push_drain() {
        // Drain any leftover from other tests
        drain_planned_accesses();

        push_planned_accesses(vec![
            PlannedAccess { tree_name: "users".into(), access_type: AccessType::Scan },
            PlannedAccess { tree_name: "idx_posts_id".into(), access_type: AccessType::Search },
        ]);

        let drained = drain_planned_accesses();
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].tree_name, "users");
        assert_eq!(drained[1].tree_name, "idx_posts_id");

        // Queue is empty after drain
        let drained2 = drain_planned_accesses();
        assert!(drained2.is_empty());
    }

    #[test]
    fn test_queue_accumulates_multiple_pushes() {
        // Lock the queue for the duration to prevent other tests from draining
        let mut queue = super::PLAN_QUEUE.lock().expect("plan queue poisoned");
        queue.clear();

        // Simulate two pushes by extending directly
        queue.push(PlannedAccess { tree_name: "users".into(), access_type: AccessType::Scan });
        queue.push(PlannedAccess { tree_name: "posts".into(), access_type: AccessType::Search });

        assert_eq!(queue.len(), 2);
        queue.clear();
    }

    #[test]
    fn test_empty_push_is_noop() {
        drain_planned_accesses();
        push_planned_accesses(vec![]);
        let drained = drain_planned_accesses();
        assert!(drained.is_empty());
    }

    #[test]
    fn test_parse_subquery() {
        // Subqueries produce nested EQP lines with different indentation
        let eqp = "\
SEARCH orders USING INDEX idx_orders_user_id (user_id=?)
SCAN line_items
SEARCH products USING INDEX sqlite_autoindex_products_1 (id=?)";
        let result = parse_eqp_output(eqp);
        assert_eq!(result.len(), 3);
        // SEARCH orders -> index name
        assert_eq!(result[0].tree_name, "idx_orders_user_id");
        assert_eq!(result[0].access_type, AccessType::Search);
        // SCAN line_items -> table name
        assert_eq!(result[1].tree_name, "line_items");
        assert_eq!(result[1].access_type, AccessType::Scan);
        // SEARCH products -> index name
        assert_eq!(result[2].tree_name, "sqlite_autoindex_products_1");
        assert_eq!(result[2].access_type, AccessType::Search);
    }

    #[test]
    fn test_parse_eqp_with_indented_output() {
        // Real EQP output has indented detail lines
        let eqp = "\
   SEARCH users USING INDEX idx_users_email (email=?)
      SCAN posts";
        let result = parse_eqp_output(eqp);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].tree_name, "idx_users_email");
        assert_eq!(result[1].tree_name, "posts");
    }

    #[test]
    fn test_run_eqp_skips_non_select() {
        // run_eqp_and_parse should return empty for non-SELECT/WITH
        // We can't test with a real db handle, but we can test the
        // SQL filtering logic by checking the early returns
        let non_selects = &[
            "INSERT INTO users VALUES (1, 'alice')",
            "UPDATE users SET name = 'bob' WHERE id = 1",
            "DELETE FROM users WHERE id = 1",
            "CREATE TABLE foo (id INTEGER PRIMARY KEY)",
            "PRAGMA journal_mode=WAL",
            "BEGIN",
            "COMMIT",
            "",
            "   ",
        ];
        for sql in non_selects {
            // run_eqp_and_parse would return empty for these even with a valid db,
            // because the early check filters by first word.
            // We verify the filter logic directly:
            let trimmed = sql.trim();
            if trimmed.is_empty() {
                continue;
            }
            let first_word = trimmed.split_whitespace().next().unwrap_or("");
            let is_query = matches!(first_word.to_uppercase().as_str(), "SELECT" | "WITH");
            assert!(!is_query, "expected non-query for: {}", sql);
        }
    }

    #[test]
    fn test_parse_same_table_scan_and_search_both_emitted() {
        // A self-join could produce both SCAN and SEARCH on the same table
        let eqp = "\
SCAN users
SEARCH users USING INDEX idx_users_email (email=?)";
        let result = parse_eqp_output(eqp);
        // SCAN users -> table as Scan
        // SEARCH users -> index as Search
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].tree_name, "users");
        assert_eq!(result[0].access_type, AccessType::Scan);
        assert_eq!(result[1].tree_name, "idx_users_email");
        assert_eq!(result[1].access_type, AccessType::Search);
    }
}
