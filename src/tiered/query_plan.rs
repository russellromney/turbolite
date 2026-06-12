//! Query-plan-aware prefetch.
//!
//! A per-thread queue of planned B-tree accesses populated by the trace
//! callback (in ext_entry.c) and drained by the VFS on first cache miss.
//!
//! The trace callback runs EXPLAIN QUERY PLAN on each SQL statement at the
//! start of sqlite3_step(), extracts SCAN/SEARCH + table/index names, and
//! pushes PlannedAccess entries to the queue. The VFS drains the queue on
//! first cache miss and submits all planned groups to the prefetch pool.
//!
//! When the queue is empty (extension not loaded, or DDL/PRAGMA), the VFS
//! falls back to the hop schedule.

use std::cell::RefCell;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Mutex, OnceLock};

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
    /// Table name this access belongs to (for join chasing).
    pub table_name: Option<String>,
    /// Column constraint from EQP, e.g., "id=?" from "USING INDEX idx (id=?)".
    /// Used by leaf chasing to know which column links tables in a join.
    pub constraint_columns: Vec<String>,
}

/// Attach simple integer equality hints to planned SEARCH constraints.
///
/// The EQP text only says `col=?`; callers that still have the query's bound
/// values can annotate that as `col=123`. Consumers must treat this as an
/// advisory hint and fall back when it is absent.
pub fn attach_integer_constraint_hints(accesses: &mut [PlannedAccess], values: &[i64]) {
    if values.is_empty() {
        return;
    }
    let mut value_iter = values.iter();
    for access in accesses.iter_mut() {
        if access.access_type != AccessType::Search {
            continue;
        }
        for column in &mut access.constraint_columns {
            if column.contains('=') {
                continue;
            }
            let Some(value) = value_iter.next() else {
                return;
            };
            *column = format!("{}={}", column, value);
        }
    }
}

pub(crate) fn first_integer_constraint_hint(access: &PlannedAccess) -> Option<i64> {
    access.constraint_columns.iter().find_map(|column| {
        let (_name, value) = column.split_once('=')?;
        value.trim().parse::<i64>().ok()
    })
}

#[derive(Debug, Clone)]
struct PlannedAccessBatch {
    id: u64,
    accesses: Vec<PlannedAccess>,
}

static NEXT_PLAN_BATCH_ID: AtomicU64 = AtomicU64::new(1);
static GLOBAL_PLAN_QUEUE: OnceLock<Mutex<Vec<PlannedAccessBatch>>> = OnceLock::new();
static GLOBAL_CONSUMED_PLAN_BATCH_IDS: OnceLock<Mutex<HashSet<u64>>> = OnceLock::new();

fn global_plan_queue() -> &'static Mutex<Vec<PlannedAccessBatch>> {
    GLOBAL_PLAN_QUEUE.get_or_init(|| Mutex::new(Vec::new()))
}

fn global_consumed_plan_batch_ids() -> &'static Mutex<HashSet<u64>> {
    GLOBAL_CONSUMED_PLAN_BATCH_IDS.get_or_init(|| Mutex::new(HashSet::new()))
}

// Same-thread queue of planned accesses. SQLite normally invokes the trace
// callback and VFS reads on the connection's thread; keeping this queue
// thread-local prevents unrelated connections/tests from stealing another
// query's plan before that first cache miss. Each push is also mirrored into a
// process-global fallback queue so a cross-thread VFS read can still consume
// the advisory plan instead of silently disabling plan-aware prefetch.
thread_local! {
    static PLAN_QUEUE: RefCell<Vec<PlannedAccessBatch>> = const { RefCell::new(Vec::new()) };
}

/// Push planned accesses to the global queue (called from trace callback).
pub fn push_planned_accesses(accesses: Vec<PlannedAccess>) {
    if accesses.is_empty() {
        return;
    }
    let batch = PlannedAccessBatch {
        id: NEXT_PLAN_BATCH_ID.fetch_add(1, AtomicOrdering::Relaxed),
        accesses,
    };
    PLAN_QUEUE.with(|queue| queue.borrow_mut().push(batch.clone()));
    global_plan_queue()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(batch);
}

/// Drain all planned accesses from the queue (called from VFS on first cache miss).
/// Returns an empty Vec if nothing is queued.
pub fn drain_planned_accesses() -> Vec<PlannedAccess> {
    let local = PLAN_QUEUE.with(|queue| {
        let mut queue = queue.borrow_mut();
        if queue.is_empty() {
            None
        } else {
            Some(std::mem::take(&mut *queue))
        }
    });

    if let Some(batches) = local {
        let mut consumed = global_consumed_plan_batch_ids()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let batches: Vec<PlannedAccessBatch> = batches
            .into_iter()
            .filter(|batch| !consumed.remove(&batch.id))
            .collect();
        drop(consumed);
        if batches.is_empty() {
            return Vec::new();
        }
        let drained_ids: HashSet<u64> = batches.iter().map(|batch| batch.id).collect();
        global_plan_queue()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|batch| !drained_ids.contains(&batch.id));
        return batches
            .into_iter()
            .flat_map(|batch| batch.accesses)
            .collect();
    }

    let mut global = global_plan_queue()
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if global.is_empty() {
        return Vec::new();
    }
    let batches = std::mem::take(&mut *global);
    let consumed_ids: HashSet<u64> = batches.iter().map(|batch| batch.id).collect();
    drop(global);
    global_consumed_plan_batch_ids()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .extend(consumed_ids);
    batches
        .into_iter()
        .flat_map(|batch| batch.accesses)
        .collect()
}

/// End-of-query signal. Set by the SQLITE_TRACE_PROFILE callback when a
/// statement finishes. The VFS checks this on the next read to trigger
/// between-query eviction.
///
/// AtomicBool is sufficient: we only need "at least one query ended since
/// last check." Multiple concurrent completions collapse to one eviction
/// pass, which is correct (eviction is idempotent).
static END_QUERY_SIGNAL: AtomicBool = AtomicBool::new(false);

/// Signal that a query has completed. Called from the C trace profile callback.
pub fn signal_end_query() {
    END_QUERY_SIGNAL.store(true, Ordering::Release);
}

/// Check and clear the end-of-query signal. Returns true if at least one
/// query completed since the last check. The VFS calls this on every read
/// to decide whether to run between-query eviction.
pub fn check_and_clear_end_query() -> bool {
    END_QUERY_SIGNAL.swap(false, Ordering::AcqRel)
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

        // Next word is the table name (skip optional "TABLE" or "SUBQUERY" keyword)
        let mut words = rest.split_whitespace();
        let first = match words.next() {
            Some(w) => w,
            None => continue,
        };
        let table_name = if first == "TABLE" || first == "SUBQUERY" {
            match words.next() {
                Some(name) => name,
                None => continue,
            }
        } else {
            first
        };

        // Extract constraint columns from (col=? AND col2>?) if present
        let constraint_columns = extract_constraint_columns(rest);

        // For SCAN: always emit the table (we need all data groups)
        if access_type == AccessType::Scan
            && seen.insert((table_name.to_string(), AccessType::Scan))
        {
            accesses.push(PlannedAccess {
                tree_name: table_name.to_string(),
                access_type: AccessType::Scan,
                table_name: Some(table_name.to_string()),
                constraint_columns: constraint_columns.clone(),
            });
        }

        // Check for USING [COVERING] INDEX <index_name>
        if let Some(idx_pos) = rest
            .find("USING INDEX")
            .or_else(|| rest.find("USING COVERING INDEX"))
        {
            let after_using = &rest[idx_pos..];
            // Skip "USING INDEX " or "USING COVERING INDEX "
            let idx_name_start = if after_using.starts_with("USING COVERING INDEX") {
                "USING COVERING INDEX ".len()
            } else {
                "USING INDEX ".len()
            };
            if let Some(idx_name) = after_using
                .get(idx_name_start..)
                .and_then(|s| s.split_whitespace().next())
            {
                let idx_access = access_type;
                if seen.insert((idx_name.to_string(), idx_access)) {
                    accesses.push(PlannedAccess {
                        tree_name: idx_name.to_string(),
                        access_type: idx_access,
                        table_name: Some(table_name.to_string()),
                        constraint_columns: constraint_columns.clone(),
                    });
                }
            }
        }
        // SEARCH without USING INDEX (e.g., rowid lookup) emits nothing.
        // Rowid lookups are point queries; the hop schedule handles them.
    }

    accesses
}

/// Extract column names from EQP constraint expression like "(id=? AND name>?)".
/// Returns ["id", "name"]. Handles =, <, >, <=, >=, IS operators.
fn extract_constraint_columns(eqp_rest: &str) -> Vec<String> {
    // Look for (...) at end of line
    let paren_start = match eqp_rest.rfind('(') {
        Some(p) => p,
        None => return Vec::new(),
    };
    let paren_end = match eqp_rest[paren_start..].find(')') {
        Some(p) => paren_start + p,
        None => return Vec::new(),
    };
    let inside = &eqp_rest[paren_start + 1..paren_end];

    // Split by " AND " and extract column names (left of operator)
    let mut columns = Vec::new();
    for part in inside.split(" AND ") {
        let part = part.trim();
        // Find the operator: =, <, >, <=, >=, IS
        for op in &["<=", ">=", "=", "<", ">", " IS "] {
            if let Some(pos) = part.find(op) {
                let col = part[..pos].trim();
                if !col.is_empty() {
                    columns.push(col.to_string());
                }
                break;
            }
        }
    }
    columns
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
        &mut stmt as *mut *mut std::ffi::c_void,
        &mut tail as *mut *const std::ffi::c_char,
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

// C trace callback FFI wrappers (`turbolite_trace_push_plan` /
// `turbolite_trace_end_query`) live in the `turbolite-ffi` crate. They
// forward to `run_eqp_and_parse` / `push_planned_accesses` /
// `signal_end_query` above.

#[cfg(test)]
#[path = "test_query_plan.rs"]
mod tests;
