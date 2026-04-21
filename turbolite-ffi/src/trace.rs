//! FFI wrappers that the C shim's trace callback invokes. The shim runs
//! per-statement EQP and pushes planned accesses into turbolite's queue
//! so the VFS can drain them on the next xRead — see ext_entry.c's
//! `turbolite_trace_callback`.

use std::ffi::CStr;
use std::os::raw::c_char;

use turbolite::tiered::{push_planned_accesses, run_eqp_and_parse, signal_end_query};

/// Run EQP on `sql` against `db` and push the planned accesses onto the
/// global plan queue. Invoked from `SQLITE_TRACE_STMT`.
///
/// # Safety
/// `db` must be a valid sqlite3 handle. `sql` must be a valid C string.
#[no_mangle]
pub unsafe extern "C" fn turbolite_trace_push_plan(
    db: *mut std::ffi::c_void,
    sql: *const c_char,
) {
    if sql.is_null() {
        return;
    }
    let sql_str = match CStr::from_ptr(sql).to_str() {
        Ok(s) => s,
        Err(_) => return,
    };

    let accesses = run_eqp_and_parse(db, sql_str);
    push_planned_accesses(accesses);
}

/// Signal query completion so the VFS runs between-query eviction on the
/// next read. Invoked from `SQLITE_TRACE_PROFILE`.
#[no_mangle]
pub extern "C" fn turbolite_trace_end_query() {
    signal_end_query();
}
