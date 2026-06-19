//! Per-connection install helper for `turbolite_config_set`.
//!
//! Phase Cirrus h2 — closes the multi-connection-per-thread routing
//! hole on the C side by capturing the calling connection's handle
//! queue via `sqlite3_create_function_v2`'s `pApp` pointer.
//!
//! Exported as a C-callable symbol (`turbolite_install_config_functions`)
//! so loadable-extension hosts (`SELECT load_extension(...)`), Python
//! ctypes, Go cgo, and Node koffi call the same routine. The body is in
//! Rust so it compiles in both cdylib flavors:
//!
//! - **bundled-sqlite** (standalone cdylib): sqlite3 comes in via
//!   `libsqlite3-sys`. The integration tests (which link turbolite-ffi
//!   as an rlib) exercise this path directly.
//! - **loadable-extension**: the `ext_entry.c` shim's macro-routed
//!   symbol shims (sqlite3_vfs_register, sqlite3_create_function_v2,
//!   etc.) resolve the extern "C" declarations below at link time so
//!   this code runs unchanged inside a host process.
//!
//! Call contract: invoke immediately after opening a turbolite-backed
//! connection, before opening any other turbolite connection on the
//! same thread. The helper runs `PRAGMA schema_version` internally to
//! force the VFS `xOpen`, then snapshots the top of the thread-local
//! active-handle stack — that's THIS connection's queue because install
//! runs synchronously on the opening thread.

use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_void};
use std::sync::Arc;

use crate::settings::{
    turbolite_current_queue_clone, turbolite_settings_queue_free_cb, turbolite_settings_queue_push,
};

/// Run an FFI body under `catch_unwind`, returning `fallback` on panic.
///
/// Unwinding across the `extern "C"` boundary (including a SQLite-invoked
/// scalar callback) is undefined behavior; wrap the body so a caught panic
/// becomes the documented error sentinel instead.
fn install_guard<F, R>(fallback: R, body: F) -> R
where
    F: FnOnce() -> R,
{
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)) {
        Ok(value) => value,
        Err(_) => fallback,
    }
}

// SQLite constants we need. Mirrors <sqlite3.h>.
const SQLITE_OK: c_int = 0;
const SQLITE_MISUSE: c_int = 21;
const SQLITE_UTF8: c_int = 1;
const SQLITE_DIRECTONLY: c_int = 0x0008_0000;

// Opaque handles.
#[allow(non_camel_case_types)]
pub type sqlite3 = c_void;
#[allow(non_camel_case_types)]
pub type sqlite3_context = c_void;
#[allow(non_camel_case_types)]
pub type sqlite3_value = c_void;

type CreateFnStep = unsafe extern "C" fn(*mut sqlite3_context, c_int, *mut *mut sqlite3_value);
type CreateFnFinal = unsafe extern "C" fn(*mut sqlite3_context);
type CreateFnDestroy = unsafe extern "C" fn(*mut c_void);

const SQLITE_FCNTL_VFS_POINTER: c_int = 27;

// Minimal shape of sqlite3_vfs — we only touch `zName`. The real struct
// has ~20 fields; we keep the prefix-compatible layout so &vfs->zName
// lands at the right offset. Defined here as a local opaque-ish struct
// rather than binding all of sqlite3_vfs via cbindgen or bindgen.
#[repr(C)]
struct sqlite3_vfs_prefix {
    i_version: c_int,
    sz_os_file: c_int,
    mx_pathname: c_int,
    p_next: *mut c_void,
    z_name: *const c_char,
    // ... rest of struct follows but we don't touch it.
}

extern "C" {
    fn sqlite3_exec(
        db: *mut sqlite3,
        sql: *const c_char,
        callback: *const c_void,
        arg: *const c_void,
        errmsg: *mut *mut c_char,
    ) -> c_int;

    fn sqlite3_free(ptr: *mut c_void);

    fn sqlite3_create_function_v2(
        db: *mut sqlite3,
        zFunctionName: *const c_char,
        nArg: c_int,
        eTextRep: c_int,
        pApp: *mut c_void,
        xFunc: Option<CreateFnStep>,
        xStep: Option<CreateFnStep>,
        xFinal: Option<CreateFnFinal>,
        xDestroy: Option<CreateFnDestroy>,
    ) -> c_int;

    fn sqlite3_user_data(ctx: *mut sqlite3_context) -> *mut c_void;
    fn sqlite3_value_text(value: *mut sqlite3_value) -> *const c_char;
    fn sqlite3_result_error(ctx: *mut sqlite3_context, msg: *const c_char, len: c_int);
    fn sqlite3_result_int(ctx: *mut sqlite3_context, val: c_int);

    fn sqlite3_file_control(
        db: *mut sqlite3,
        zDbName: *const c_char,
        op: c_int,
        arg: *mut c_void,
    ) -> c_int;
}

/// Scalar function body wired via `sqlite3_create_function_v2`. Reads
/// the captured queue pointer from `pApp` (set at install time) and
/// pushes the `(key, value)` into it via the queue-FFI helpers.
unsafe extern "C" fn config_set_scalar(
    ctx: *mut sqlite3_context,
    _argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    // SQLite invokes this across the C boundary; a panic must not unwind into
    // it. On a caught panic, report a SQLite error rather than leaving the
    // result unset.
    let panicked = install_guard(true, || {
        let queue = sqlite3_user_data(ctx);
        if queue.is_null() {
            let msg = b"turbolite_config_set: missing queue pointer (bug)\0";
            sqlite3_result_error(ctx, msg.as_ptr() as *const c_char, -1);
            return false;
        }

        let key = sqlite3_value_text(*argv.offset(0));
        let value = sqlite3_value_text(*argv.offset(1));
        if key.is_null() || value.is_null() {
            let msg = b"turbolite_config_set: key and value required\0";
            sqlite3_result_error(ctx, msg.as_ptr() as *const c_char, -1);
            return false;
        }

        let rc = turbolite_settings_queue_push(queue, key, value);
        if rc != 0 {
            let msg = b"turbolite_config_set: invalid key or value\0";
            sqlite3_result_error(ctx, msg.as_ptr() as *const c_char, -1);
            return false;
        }
        sqlite3_result_int(ctx, 0);
        false
    });
    if panicked {
        let msg = b"turbolite_config_set: internal panic caught\0";
        sqlite3_result_error(ctx, msg.as_ptr() as *const c_char, -1);
    }
}

/// Register the `turbolite_config_set(key, value)` SQL function on this
/// connection, capturing the calling connection's handle queue via
/// `sqlite3_create_function_v2`'s `pApp`.
///
/// Returns:
/// - `SQLITE_OK` (0) on success
/// - `SQLITE_MISUSE` if the connection's VFS isn't one of turbolite's
///   registered names (protects against cross-connection queue leaks
///   when this function is called on a non-turbolite connection while
///   a turbolite handle is alive on the same thread — the queue would
///   otherwise route pushes to the wrong connection)
/// - A SQLite error code if the `PRAGMA schema_version` probe fails
///   (connection isn't turbolite-backed)
/// - `SQLITE_MISUSE` if no turbolite handle is active on this thread
///   after the probe
///
/// # Safety
/// `db` must be a live `sqlite3*` handle.
#[no_mangle]
pub unsafe extern "C" fn turbolite_install_config_functions(db: *mut sqlite3) -> c_int {
    install_guard(SQLITE_MISUSE, || {
        // VFS-name guard. See the `install_hook.rs` equivalent comment:
        // without this, a non-turbolite connection opened on a thread with
        // an active turbolite handle would receive a scalar pointing at
        // the turbolite handle's queue.
        if !connection_uses_turbolite_vfs(db) {
            return SQLITE_MISUSE;
        }

        // Force xOpen on the main-db file so THIS connection's handle
        // queue is top-of-stack on the thread-local. `PRAGMA schema_version`
        // reads page 1 which is enough to trigger the VFS open.
        let pragma = c"PRAGMA schema_version";
        let mut err_msg: *mut c_char = std::ptr::null_mut();
        let rc = sqlite3_exec(
            db,
            pragma.as_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            &mut err_msg,
        );
        if !err_msg.is_null() {
            sqlite3_free(err_msg as *mut c_void);
        }
        if rc != SQLITE_OK {
            return rc;
        }

        let queue = turbolite_current_queue_clone();
        if queue.is_null() {
            return SQLITE_MISUSE;
        }

        let fn_name = c"turbolite_config_set";
        let rc = sqlite3_create_function_v2(
            db,
            fn_name.as_ptr(),
            2,
            SQLITE_UTF8 | SQLITE_DIRECTONLY,
            queue as *mut c_void,
            Some(config_set_scalar),
            None,
            None,
            Some(turbolite_settings_queue_free_cb),
        );
        if rc != SQLITE_OK {
            // sqlite3_create_function_v2 did not take ownership of pApp; if we
            // leave the raw pointer dangling the Arc is leaked. Reconstruct and
            // drop it so the queue is freed.
            let _ = unsafe {
                Arc::from_raw(
                    queue
                        as *const std::sync::Mutex<Vec<turbolite::tiered::settings::SettingUpdate>>,
                )
            };
        }
        rc
    })
}

/// Ask SQLite for the sqlite3_vfs pointer backing the "main" database
/// on this connection, read its zName, and check the name against
/// turbolite's registered-VFS-name set. Returns false for plain sqlite
/// connections; true only when the connection was opened with
/// `vfs=<some name passed to turbolite::tiered::register>`.
unsafe fn connection_uses_turbolite_vfs(db: *mut sqlite3) -> bool {
    let mut vfs_ptr: *mut sqlite3_vfs_prefix = std::ptr::null_mut();
    let main = c"main".as_ptr() as *const c_char;
    let rc = sqlite3_file_control(
        db,
        main,
        SQLITE_FCNTL_VFS_POINTER,
        &mut vfs_ptr as *mut _ as *mut c_void,
    );
    if rc != SQLITE_OK || vfs_ptr.is_null() {
        return false;
    }
    // Version-gate before trusting the hand-rolled prefix layout. SQLite has
    // only ever grown sqlite3_vfs by appending fields, so iVersion >= 1 and a
    // positive szOsFile guarantee the iVersion/szOsFile/mxPathname/pNext/zName
    // prefix we mirror is present and that `vfs_ptr` points at a real
    // sqlite3_vfs (not a stale/garbage pointer). Without this, a host with an
    // unexpected struct shape would have us dereference zName at the wrong
    // offset. iVersion is the first field, so reading it is always in bounds.
    let i_version = (*vfs_ptr).i_version;
    let sz_os_file = (*vfs_ptr).sz_os_file;
    if i_version < 1 || sz_os_file <= 0 {
        return false;
    }
    let name_ptr = (*vfs_ptr).z_name;
    if name_ptr.is_null() {
        return false;
    }
    let name_cstr = CStr::from_ptr(name_ptr);
    let name = match name_cstr.to_str() {
        Ok(s) => s,
        Err(_) => return false,
    };
    turbolite::tiered::is_registered_vfs_name(name)
}
