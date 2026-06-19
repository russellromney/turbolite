//! FFI wrappers that the C shim's trace callback invokes. The shim runs
//! per-statement EQP and pushes planned accesses into turbolite's queue
//! so the VFS can drain them on the next xRead — see ext_entry.c's
//! `turbolite_trace_callback`.

use std::ffi::CStr;
use std::os::raw::c_char;

use turbolite::tiered::{push_planned_accesses, run_eqp_and_parse, signal_end_query};

/// Run an FFI body under `catch_unwind`, discarding the result on panic.
///
/// Trace callbacks are invoked directly from SQLite's trace machinery;
/// unwinding across that C boundary is undefined behavior, so every trace
/// entry point routes through this guard.
fn trace_guard<F>(body: F)
where
    F: FnOnce(),
{
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body));
}

/// Run EQP on `sql` against `db` and push the planned accesses onto the
/// global plan queue. Invoked from `SQLITE_TRACE_STMT`.
///
/// # Safety
/// `db` must be a valid sqlite3 handle. `sql` must be a valid C string.
#[no_mangle]
pub unsafe extern "C" fn turbolite_trace_push_plan(db: *mut std::ffi::c_void, sql: *const c_char) {
    trace_guard(|| {
        if sql.is_null() {
            return;
        }
        let sql_str = match unsafe { CStr::from_ptr(sql).to_str() } {
            Ok(s) => s,
            Err(_) => return,
        };

        let accesses = run_eqp_and_parse(db, sql_str);
        push_planned_accesses(accesses);
    });
}

/// Signal query completion so the VFS runs between-query eviction on the
/// next read. Invoked from `SQLITE_TRACE_PROFILE`.
#[no_mangle]
pub extern "C" fn turbolite_trace_end_query() {
    trace_guard(signal_end_query);
}

#[cfg(test)]
mod tests {
    use super::*;

    // G6 regression: trace callbacks must not unwind across the C boundary.
    #[test]
    fn trace_guard_catches_panic() {
        let mut ran = false;
        trace_guard(|| {
            ran = true;
            panic!("deliberate panic");
        });
        assert!(ran, "body should have run");
    }

    #[test]
    fn trace_push_plan_null_sql_is_safe() {
        // NULL sql must return without panicking; db is ignored on this path.
        unsafe { turbolite_trace_push_plan(std::ptr::null_mut(), std::ptr::null()) };
    }

    #[test]
    fn trace_end_query_does_not_panic() {
        turbolite_trace_end_query();
    }
}
