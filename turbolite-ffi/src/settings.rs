//! Top-of-stack settings FFI — the legacy `turbolite_config_set` entry
//! point used by `sqlite3_turbolite_init`'s auto-registration and by
//! the TypeScript / Go wrappers that still rely on thread-local routing.
//!
//! Phase Cirrus h2 introduces a per-connection install helper
//! (`turbolite_install_config_functions`) that captures the connection's
//! queue via `sqlite3_create_function_v2`'s `pApp`; the shim then calls
//! `turbolite_settings_queue_push` with the captured pointer rather than
//! going through this thread-local-based entry point. Keeping this
//! function during h1 preserves the old behavior verbatim — h2 removes
//! the auto-registration, at which point the only callers are bindings
//! that still use the top-of-stack path.

use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_void};
use std::sync::{Arc, Mutex};

use turbolite::tiered::settings::{self, push_to_current, validate, SettingUpdate, SettingsQueue};

/// FFI entry point: `turbolite_config_set(key, value)`.
///
/// Returns:
///   0 — pushed successfully
///   1 — validation failed (unknown key or bad value)
///   2 — no active turbolite handle on this thread
///
/// # Safety
/// `key` and `value` must be valid C strings.
#[no_mangle]
pub unsafe extern "C" fn turbolite_config_set(key: *const c_char, value: *const c_char) -> c_int {
    if key.is_null() || value.is_null() {
        return 1;
    }
    let key_str = match CStr::from_ptr(key).to_str() {
        Ok(s) => s,
        Err(_) => return 1,
    };
    let value_str = match CStr::from_ptr(value).to_str() {
        Ok(s) => s,
        Err(_) => return 1,
    };

    if validate(key_str, value_str).is_err() {
        return 1;
    }
    if !push_to_current(SettingUpdate {
        key: key_str.to_string(),
        value: value_str.to_string(),
    }) {
        return 2;
    }
    0
}

/// Clone an Arc to the current thread's top-of-stack handle queue, or
/// return NULL if no turbolite connection is active on this thread.
///
/// The returned pointer owns one refcount; the caller must eventually
/// release it via [`turbolite_settings_queue_free`] — typically by
/// handing it to `sqlite3_create_function_v2` as `pApp` with
/// [`turbolite_settings_queue_free_cb`] as `xDestroy`.
///
/// Used by `turbolite_install_config_functions` to snapshot the calling
/// connection's queue at install time.
#[no_mangle]
pub extern "C" fn turbolite_current_queue_clone() -> *const c_void {
    match settings::top_queue() {
        Some(q) => Arc::into_raw(q) as *const c_void,
        None => std::ptr::null(),
    }
}

/// Drop one refcount on a queue pointer previously returned by
/// [`turbolite_current_queue_clone`]. NULL is a no-op.
///
/// # Safety
/// `ptr` must be NULL or a pointer returned by
/// `turbolite_current_queue_clone` (not yet freed).
#[no_mangle]
pub unsafe extern "C" fn turbolite_settings_queue_free(ptr: *const c_void) {
    if !ptr.is_null() {
        // Round-trip back to Arc; drop releases one refcount.
        drop(Arc::from_raw(ptr as *const Mutex<Vec<SettingUpdate>>));
    }
}

/// `xDestroy` callback signature wrapper for
/// `sqlite3_create_function_v2`. `void *` instead of `const void *` to
/// match SQLite's callback ABI.
///
/// # Safety
/// `ptr` must be NULL or a pointer returned by
/// `turbolite_current_queue_clone` (not yet freed).
#[no_mangle]
pub unsafe extern "C" fn turbolite_settings_queue_free_cb(ptr: *mut c_void) {
    turbolite_settings_queue_free(ptr as *const c_void);
}

/// Push a `(key, value)` update into a specific queue pointer. Used by
/// the C shim's `turbolite_config_set_func` after pApp-capture.
///
/// Returns:
///   0 — pushed successfully
///   1 — validation failed (unknown key / bad value) or null pointer
///
/// # Safety
/// `queue_ptr` must be a live pointer returned by
/// `turbolite_current_queue_clone`. `key` / `value` must be valid C
/// strings.
#[no_mangle]
pub unsafe extern "C" fn turbolite_settings_queue_push(
    queue_ptr: *const c_void,
    key: *const c_char,
    value: *const c_char,
) -> c_int {
    if queue_ptr.is_null() || key.is_null() || value.is_null() {
        return 1;
    }
    let key_str = match CStr::from_ptr(key).to_str() {
        Ok(s) => s,
        Err(_) => return 1,
    };
    let value_str = match CStr::from_ptr(value).to_str() {
        Ok(s) => s,
        Err(_) => return 1,
    };
    if validate(key_str, value_str).is_err() {
        return 1;
    }

    // Borrow the Arc via ManuallyDrop so we don't consume the refcount;
    // xDestroy still owns one refcount for the function's lifetime.
    let queue_arc: &Mutex<Vec<SettingUpdate>> = &*(queue_ptr as *const Mutex<Vec<SettingUpdate>>);
    queue_arc
        .lock()
        .expect("settings queue poisoned")
        .push(SettingUpdate {
            key: key_str.to_string(),
            value: value_str.to_string(),
        });

    // Silence the "unused type" warning for the public alias so the docs
    // cross-link cleanly.
    let _: Option<SettingsQueue> = None;
    0
}
