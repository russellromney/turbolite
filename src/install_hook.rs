//! Process-global auto-install for `turbolite_config_set`, mirror of the
//! `sqlite3_auto_extension` path in turbolite-ffi's loadable extension.
//!
//! Goal: any `rusqlite::Connection` opened in this process against a
//! turbolite-backed VFS gets `turbolite_config_set` installed with the
//! connection's own queue captured as `pApp` — same ergonomics as the
//! loadable-extension path, no explicit `install_config_functions(&conn)`
//! call required.
//!
//! Mechanism: on the first `TurboliteVfs::new_local` (or equivalent)
//! we register an `sqlite3_auto_extension` callback via libsqlite3-sys.
//! SQLite calls the registered callback for every newly-opened sqlite3
//! connection — turbolite's own, plus any unrelated ones in the same
//! process. The callback checks the thread-local active-handle stack;
//! if no turbolite handle is active (non-turbolite connection), it
//! returns `SQLITE_OK` and bails. Otherwise it binds the scalar with
//! `pApp = Arc::into_raw(queue)` and installs an `xDestroy` that drops
//! the Arc when the function is replaced or the connection closes.
//!
//! Why no `PRAGMA schema_version` probe here: auto-extension callbacks
//! fire inside `sqlite3_open_v2`, AFTER the VFS's `xOpen` for the
//! main-db file runs (SQLite reads the header during open, which forces
//! `xOpen`). By the time our callback fires, the connection's queue is
//! already on the thread-local stack. Avoiding the probe means unrelated
//! sqlite3 connections in the same process don't pay a per-open pragma
//! cost just because turbolite is linked.

use std::os::raw::{c_char, c_int};
use std::sync::{Arc, Mutex, Once};

use libsqlite3_sys as ffi;

use crate::tiered::settings::{self, validate, SettingUpdate};

// SQLITE_DIRECTONLY isn't re-exported by libsqlite3-sys. Value per
// sqlite3.h — gates the function from being invoked by trigger /
// generated column / CHECK expressions.
const SQLITE_DIRECTONLY: c_int = 0x0008_0000;

static REGISTERED: Once = Once::new();

/// Register the auto-extension hook. Idempotent across threads via
/// `std::sync::Once`. Safe to call from every VFS constructor; only the
/// first invocation reaches into `sqlite3_auto_extension`.
pub(crate) fn ensure_registered() {
    REGISTERED.call_once(|| unsafe {
        // sqlite3_auto_extension on libsqlite3-sys 0.33 takes a properly
        // typed `Option<unsafe extern "C" fn(db, pzErrMsg, pApi) -> c_int>`
        // so no transmute is needed.
        let rc = ffi::sqlite3_auto_extension(Some(auto_extension_entry));
        if rc != ffi::SQLITE_OK {
            // Registration failed is non-fatal — users can still call
            // `install_config_functions(&conn)` manually. Emit a
            // tracing warning so the failure isn't silent.
            tracing::warn!(
                target: "turbolite",
                "sqlite3_auto_extension(turbolite_install) returned {}",
                rc
            );
        }
    });
}

/// Auto-extension callback. SQLite invokes this for every new
/// `sqlite3_open_v2`. Returns SQLITE_OK in all cases so unrelated
/// sqlite3 opens in the same process aren't disturbed.
unsafe extern "C" fn auto_extension_entry(
    db: *mut ffi::sqlite3,
    _pz_err_msg: *mut *mut c_char,
    _p_api: *const ffi::sqlite3_api_routines,
) -> c_int {
    install(db);
    ffi::SQLITE_OK
}

/// Internal install: if `db` is backed by a turbolite VFS AND this
/// thread has an active turbolite handle, bind `turbolite_config_set`
/// to `db` with the handle's queue as `pApp`. The VFS-name guard is
/// load-bearing: without it, any plain sqlite3 connection opened on a
/// thread where a turbolite handle happens to be alive would get the
/// scalar installed pointing at the wrong queue (cross-connection
/// leak). See the `plain_connection_does_not_inherit_scalar_from_alive_turbolite_handle`
/// regression test.
///
/// Outcomes, each observable via `tracing` (target = `turbolite`):
/// - `trace!` on non-turbolite VFS (benign; normal for unrelated
///   sqlite3_open in the same process).
/// - `trace!` on "turbolite VFS but no queue on stack" (unexpected —
///   means xOpen didn't push; should not happen in practice).
/// - `error!` if `sqlite3_create_function_v2` fails after all other
///   guards pass (real bug).
/// - silent on the happy path.
unsafe fn install(db: *mut ffi::sqlite3) {
    // VFS guard: resolve the "main" db's VFS on this connection and
    // bail unless it's one of ours.
    if !connection_uses_turbolite_vfs(db) {
        tracing::trace!(
            target: "turbolite",
            db = ?db,
            "install_hook: connection's VFS is not turbolite-registered; \
             skipping turbolite_config_set binding"
        );
        return;
    }

    let queue = match settings::top_queue() {
        Some(q) => q,
        None => {
            tracing::trace!(
                target: "turbolite",
                db = ?db,
                "install_hook: connection uses a turbolite VFS but no \
                 handle queue is active on this thread; skipping bind. \
                 Call install_config_functions(&conn) manually if you \
                 need turbolite_config_set on this connection."
            );
            return;
        }
    };

    let queue_ptr = Arc::into_raw(queue) as *mut std::ffi::c_void;

    let name = b"turbolite_config_set\0".as_ptr() as *const c_char;
    let rc = ffi::sqlite3_create_function_v2(
        db,
        name,
        2,
        ffi::SQLITE_UTF8 | SQLITE_DIRECTONLY,
        queue_ptr,
        Some(scalar),
        None,
        None,
        Some(destroy),
    );
    if rc != ffi::SQLITE_OK {
        // Reclaim the Arc refcount since SQLite didn't take ownership.
        drop(Arc::from_raw(queue_ptr as *const Mutex<Vec<SettingUpdate>>));
        tracing::error!(
            target: "turbolite",
            db = ?db,
            rc,
            "install_hook: sqlite3_create_function_v2 for turbolite_config_set \
             failed; SELECT turbolite_config_set(...) will fail with 'no such \
             function' on this connection. Callers can recover by invoking \
             turbolite::install_config_functions(&conn) explicitly."
        );
    }
}

/// Scalar function body: read captured queue from `pApp`, validate
/// (key, value), push onto the queue.
unsafe extern "C" fn scalar(
    ctx: *mut ffi::sqlite3_context,
    _argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    let queue_ptr = ffi::sqlite3_user_data(ctx) as *const Mutex<Vec<SettingUpdate>>;
    if queue_ptr.is_null() {
        let err = b"turbolite_config_set: missing queue pointer (bug)\0";
        ffi::sqlite3_result_error(ctx, err.as_ptr() as *const c_char, -1);
        return;
    }

    let key_ptr = ffi::sqlite3_value_text(*argv.offset(0)) as *const c_char;
    let val_ptr = ffi::sqlite3_value_text(*argv.offset(1)) as *const c_char;
    if key_ptr.is_null() || val_ptr.is_null() {
        let err = b"turbolite_config_set: key and value required\0";
        ffi::sqlite3_result_error(ctx, err.as_ptr() as *const c_char, -1);
        return;
    }

    let key = match std::ffi::CStr::from_ptr(key_ptr).to_str() {
        Ok(s) => s,
        Err(_) => {
            let err = b"turbolite_config_set: key not UTF-8\0";
            ffi::sqlite3_result_error(ctx, err.as_ptr() as *const c_char, -1);
            return;
        }
    };
    let value = match std::ffi::CStr::from_ptr(val_ptr).to_str() {
        Ok(s) => s,
        Err(_) => {
            let err = b"turbolite_config_set: value not UTF-8\0";
            ffi::sqlite3_result_error(ctx, err.as_ptr() as *const c_char, -1);
            return;
        }
    };

    if let Err(msg) = validate(key, value) {
        let err = std::ffi::CString::new(format!("turbolite_config_set: {}", msg))
            .unwrap_or_else(|_| std::ffi::CString::new("turbolite_config_set: invalid").unwrap());
        ffi::sqlite3_result_error(ctx, err.as_ptr(), -1);
        return;
    }

    let queue: &Mutex<Vec<SettingUpdate>> = &*queue_ptr;
    queue
        .lock()
        .expect("settings queue poisoned")
        .push(SettingUpdate {
            key: key.to_string(),
            value: value.to_string(),
        });
    ffi::sqlite3_result_int(ctx, 0);
}

/// xDestroy callback: drops the Arc refcount owned by the registered
/// scalar function. Fires when the function is replaced (re-register)
/// or when the connection closes.
unsafe extern "C" fn destroy(ptr: *mut std::ffi::c_void) {
    if !ptr.is_null() {
        drop(Arc::from_raw(ptr as *const Mutex<Vec<SettingUpdate>>));
    }
}

/// Ask SQLite for the sqlite3_vfs pointer backing the "main" database
/// on this connection, read its zName, and check the name against
/// turbolite's registered-VFS-name set. Returns false for plain sqlite
/// connections (default / memory / other VFSes), true only when the
/// connection was opened with `vfs=<some name passed to turbolite::tiered::register>`.
unsafe fn connection_uses_turbolite_vfs(db: *mut ffi::sqlite3) -> bool {
    let mut vfs_ptr: *mut ffi::sqlite3_vfs = std::ptr::null_mut();
    let main = b"main\0".as_ptr() as *const c_char;
    let rc = ffi::sqlite3_file_control(
        db,
        main,
        ffi::SQLITE_FCNTL_VFS_POINTER,
        &mut vfs_ptr as *mut _ as *mut std::ffi::c_void,
    );
    if rc != ffi::SQLITE_OK || vfs_ptr.is_null() {
        return false;
    }
    let name_ptr = (*vfs_ptr).zName;
    if name_ptr.is_null() {
        return false;
    }
    let name_cstr = std::ffi::CStr::from_ptr(name_ptr);
    let name = match name_cstr.to_str() {
        Ok(s) => s,
        Err(_) => return false,
    };
    crate::tiered::is_registered_vfs_name(name)
}
