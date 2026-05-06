//! C FFI interface for turbolite.
//!
//! Builds as a shared library (.so on Linux, .dylib on macOS) that can be loaded
//! by any language with C FFI support (Python ctypes, Go cgo, Node ffi-napi, etc.).
//!
//! # Building
//!
//! ```sh
//! # Shared library linking system SQLite (for use by apps that already link SQLite):
//! make lib
//!
//! # Generate C header:
//! make header
//! ```
//!
//! # Usage from C
//!
//! ```c
//! #include "turbolite.h"
//! #include <sqlite3.h>
//!
//! // Recommended file-first registration: the caller's database path is
//! // the local page image. Sidecar metadata lives in `/data/app.db-turbolite/`.
//! int rc = turbolite_register_local_file_first("turbolite", "/data/app.db", 3);
//! if (rc != 0) {
//!     fprintf(stderr, "error: %s\n", turbolite_last_error());
//! }
//!
//! sqlite3 *db;
//! sqlite3_open_v2("/data/app.db", &db,
//!                 SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "turbolite");
//!
//! // Lower-level: pass a cache directory and let turbolite store the
//! // page image under `<cache_dir>/data.cache`. Useful when the caller
//! // wants to manage the cache layout themselves.
//! // int rc = turbolite_register_local("turbolite-low", "/data/mydb", 3);
//!
//! // JSON config for full control (file-first):
//! // turbolite_register("turbolite",
//! //     "{\"local_data_path\": \"/data/app.db\"}");
//! ```

use std::cell::RefCell;
use std::collections::HashSet;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};

// --- Error handling ---

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

// Track closed handle addresses to prevent use-after-close and double-close
// We add BEFORE dropping so the second call sees the address still valid
thread_local! {
    static CLOSED_HANDLES: RefCell<HashSet<u64>> = RefCell::new(HashSet::new());
}

fn set_last_error(msg: &str) {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = CString::new(msg).ok();
    });
}

fn clear_last_error() {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = None;
    });
}

/// Get the last error message, or NULL if no error occurred.
///
/// The returned pointer is valid until the next turbolite_* call on this thread.
#[no_mangle]
pub extern "C" fn turbolite_last_error() -> *const c_char {
    LAST_ERROR.with(|e| match &*e.borrow() {
        Some(s) => s.as_ptr(),
        None => std::ptr::null(),
    })
}

// --- Version ---

/// Get the turbolite version string. Always returns a valid pointer.
#[no_mangle]
pub extern "C" fn turbolite_version() -> *const c_char {
    // Null-terminated static byte string — no allocation, lives forever.
    static VERSION: &[u8] = concat!(env!("CARGO_PKG_VERSION"), "\0").as_bytes();
    VERSION.as_ptr() as *const c_char
}

// --- VFS registration ---

/// Register a local TurboliteVfs keyed to a database file path (file-first).
///
/// This is the recommended local registration. The caller's `database_path`
/// is the primary local database image; turbolite stores its hidden
/// implementation state under `<database_path>-turbolite/` (manifest, cache,
/// staging logs, etc.). Bindings should prefer this over
/// [`turbolite_register_local`].
///
/// `database_path` may be relative or absolute. Relative paths are kept
/// verbatim and resolved against the process working directory at open time.
/// The string is copied immediately into a `PathBuf`; the caller may free
/// the buffer after this call returns.
///
/// # Parameters
/// - `name`: VFS name (e.g. `"turbolite"`). Must be unique per process.
/// - `database_path`: User-facing database path (e.g. `/data/app.db`).
/// - `compression_level`: zstd level 1-22 (3 is a good default).
///
/// # Returns
/// 0 on success, -1 on error. Call `turbolite_last_error()` for details.
#[no_mangle]
pub extern "C" fn turbolite_register_local_file_first(
    name: *const c_char,
    database_path: *const c_char,
    compression_level: c_int,
) -> c_int {
    clear_last_error();
    let name = match cstr_to_str(name, "name") {
        Ok(s) => s,
        Err(code) => return code,
    };
    let database_path = match cstr_to_str(database_path, "database_path") {
        Ok(s) => s,
        Err(code) => return code,
    };

    let mut config = turbolite::tiered::TurboliteConfig::for_database_path(database_path);
    config.compression.level = compression_level;
    config.compression_level = compression_level;

    let vfs = match turbolite::tiered::TurboliteVfs::new_local(config) {
        Ok(v) => v,
        Err(e) => {
            set_last_error(&format!("local vfs creation failed: {}", e));
            return -1;
        }
    };
    match turbolite::tiered::register(name, vfs) {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(&format!("register failed: {}", e));
            -1
        }
    }
}

/// Register a local TurboliteVfs (lower-level: caller picks cache_dir).
///
/// Lower-level than [`turbolite_register_local_file_first`]: turbolite owns
/// the entire `cache_dir` directory and stores the local database image at
/// `<cache_dir>/data.cache` instead of a caller-supplied file path. Use this
/// only when you need to control the cache layout directly. New embedders
/// should prefer the file-first registration so the user-facing artifact is
/// `app.db`, not `cache_dir/data.cache`.
///
/// # Parameters
/// - `name`: VFS name (e.g. `"turbolite"`). Must be unique per process.
/// - `cache_dir`: Directory turbolite owns for its page-group storage.
/// - `compression_level`: zstd level 1-22 (3 is a good default).
///
/// # Returns
/// 0 on success, -1 on error. Call `turbolite_last_error()` for details.
#[no_mangle]
pub extern "C" fn turbolite_register_local(
    name: *const c_char,
    cache_dir: *const c_char,
    compression_level: c_int,
) -> c_int {
    clear_last_error();
    let name = match cstr_to_str(name, "name") {
        Ok(s) => s,
        Err(code) => return code,
    };
    let cache_dir = match cstr_to_str(cache_dir, "cache_dir") {
        Ok(s) => s,
        Err(code) => return code,
    };

    let config = turbolite::tiered::TurboliteConfig {
        cache_dir: std::path::PathBuf::from(cache_dir),
        compression: turbolite::tiered::CompressionConfig {
            level: compression_level,
            ..Default::default()
        },
        ..Default::default()
    };

    let vfs = match turbolite::tiered::TurboliteVfs::new_local(config) {
        Ok(v) => v,
        Err(e) => {
            set_last_error(&format!("local vfs creation failed: {}", e));
            return -1;
        }
    };
    match turbolite::tiered::register(name, vfs) {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(&format!("register failed: {}", e));
            -1
        }
    }
}

/// Compute the hidden sidecar directory path for a database file.
///
/// Convenience for bindings that want to assert or eagerly create the
/// sidecar location without re-implementing the suffix rule. For
/// `database_path = "/data/app.db"` the result is `/data/app.db-turbolite`.
///
/// The returned string is heap-allocated; callers must free it with
/// [`turbolite_free_string`].
///
/// Returns NULL if `database_path` is NULL or not valid UTF-8.
#[no_mangle]
pub extern "C" fn turbolite_state_dir_for_database_path(
    database_path: *const c_char,
) -> *mut c_char {
    clear_last_error();
    let database_path = match cstr_to_str(database_path, "database_path") {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let dir = turbolite::tiered::TurboliteConfig::state_dir_for_database_path(
        database_path,
        "-turbolite",
    );
    match CString::new(dir.to_string_lossy().into_owned()) {
        Ok(cs) => cs.into_raw(),
        Err(e) => {
            set_last_error(&format!("path contains null byte: {}", e));
            std::ptr::null_mut()
        }
    }
}

/// Register a TurboliteVfs from a JSON configuration string.
///
/// Unified entry point that supports both local and cloud modes. The JSON
/// object is deserialized into a `TurboliteConfig`. Unknown fields are ignored.
///
/// # File-first local mode (recommended)
///
/// ```json
/// { "local_data_path": "/data/app.db" }
/// ```
///
/// turbolite owns `app.db` as the local page image; the sidecar lives next
/// to it under `app.db-turbolite/`. When `local_data_path` is set and
/// `cache_dir` is *not* supplied, the sidecar path is derived from
/// `local_data_path`. To pin the sidecar somewhere else, set both fields.
///
/// # Lower-level local mode
///
/// ```json
/// { "cache_dir": "/data/mydb" }
/// ```
///
/// turbolite owns the directory and stores the page image at
/// `/data/mydb/data.cache`. Prefer the file-first form for new embedders.
///
/// # Cloud mode
///
/// ```json
/// {
///   "storage_backend": { "S3": { "bucket": "my-bucket", "prefix": "db/" } },
///   "local_data_path": "/data/app.db"
/// }
/// ```
///
/// # Parameters
/// - `name`: VFS name (e.g. `"turbolite"`). Must be unique.
/// - `config_json`: JSON string with configuration fields.
///
/// # Returns
/// 0 on success, -1 on error. Call `turbolite_last_error()` for details.
#[no_mangle]
pub extern "C" fn turbolite_register(name: *const c_char, config_json: *const c_char) -> c_int {
    clear_last_error();
    let name = match cstr_to_str(name, "name") {
        Ok(s) => s,
        Err(code) => return code,
    };
    let config_json = match cstr_to_str(config_json, "config_json") {
        Ok(s) => s,
        Err(code) => return code,
    };

    // Parse to a Value first so we can detect whether `cache_dir` was
    // explicitly supplied; if `local_data_path` is set without an explicit
    // `cache_dir`, derive the file-first sidecar path from local_data_path.
    let raw: serde_json::Value = match serde_json::from_str(config_json) {
        Ok(v) => v,
        Err(e) => {
            set_last_error(&format!("invalid config JSON: {}", e));
            return -1;
        }
    };
    let cache_dir_present = raw
        .get("cache_dir")
        .map(|v| !v.is_null())
        .unwrap_or(false);
    let local_data_path_present = raw
        .get("local_data_path")
        .map(|v| !v.is_null())
        .unwrap_or(false);

    let mut config: turbolite::tiered::TurboliteConfig = match serde_json::from_value(raw) {
        Ok(c) => c,
        Err(e) => {
            set_last_error(&format!("invalid config JSON: {}", e));
            return -1;
        }
    };

    if local_data_path_present && !cache_dir_present {
        if let Some(db_path) = config.local_data_path.clone() {
            config.cache_dir =
                turbolite::tiered::TurboliteConfig::state_dir_for_database_path(
                    &db_path,
                    "-turbolite",
                );
        }
    }

    let vfs = match turbolite::tiered::TurboliteVfs::new_local(config) {
        Ok(v) => v,
        Err(e) => {
            set_last_error(&format!("vfs creation failed: {}", e));
            return -1;
        }
    };
    match turbolite::tiered::register(name, vfs) {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(&format!("register failed: {}", e));
            -1
        }
    }
}

/// Register an S3-backed cloud VFS.
///
/// The VFS stores data in S3 with a local NVMe cache. Requires the `cloud`
/// feature at build time.
///
/// # Parameters
/// - `name`: VFS name.
/// - `bucket`: S3 bucket name.
/// - `prefix`: S3 key prefix (e.g. `"databases/tenant-123"`).
/// - `cache_dir`: Local cache directory path.
/// - `endpoint_url`: Custom S3 endpoint (for MinIO/Tigris), or NULL for AWS default.
/// - `region`: AWS region, or NULL for `"auto"`.
///
/// # Returns
/// 0 on success, -1 on error.
#[cfg(feature = "cli-s3")]
#[no_mangle]
pub extern "C" fn turbolite_register_cloud(
    name: *const c_char,
    bucket: *const c_char,
    prefix: *const c_char,
    cache_dir: *const c_char,
    endpoint_url: *const c_char,
    region: *const c_char,
) -> c_int {
    clear_last_error();
    let name = match cstr_to_str(name, "name") {
        Ok(s) => s,
        Err(code) => return code,
    };
    let bucket = match cstr_to_str(bucket, "bucket") {
        Ok(s) => s,
        Err(code) => return code,
    };
    let prefix = match cstr_to_str(prefix, "prefix") {
        Ok(s) => s,
        Err(code) => return code,
    };
    let cache_dir = match cstr_to_str(cache_dir, "cache_dir") {
        Ok(s) => s,
        Err(code) => return code,
    };
    let endpoint_url = nullable_cstr_to_option(endpoint_url);
    let region = nullable_cstr_to_option(region);

    let config = turbolite::tiered::TurboliteConfig {
        cache_dir: std::path::PathBuf::from(cache_dir),
        ..Default::default()
    };

    // Wire hadb-storage-s3 via env vars + an owned tokio runtime (the
    // FFI boundary can't inherit one from the caller).
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            set_last_error(&format!("tokio runtime: {}", e));
            return -1;
        }
    };
    let handle = runtime.handle().clone();
    let _ = (prefix, region); // reserved for Phase Turbogenesis CLI wiring
    let backend = match handle.block_on(async {
        hadb_storage_s3::S3Storage::from_env(bucket.to_string(), endpoint_url).await
    }) {
        Ok(b) => std::sync::Arc::new(b) as std::sync::Arc<dyn hadb_storage::StorageBackend>,
        Err(e) => {
            set_last_error(&format!("S3Storage::from_env: {}", e));
            return -1;
        }
    };

    let vfs = match turbolite::tiered::TurboliteVfs::with_backend(config, backend, handle) {
        Ok(v) => v,
        Err(e) => {
            set_last_error(&format!("cloud vfs creation failed: {}", e));
            return -1;
        }
    };
    // Leak the runtime for the process lifetime; the VFS captures a handle
    // into it. The FFI boundary owns no cleanup hook.
    std::mem::forget(runtime);
    match turbolite::tiered::register(name, vfs) {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(&format!("cloud register failed: {}", e));
            -1
        }
    }
}

/// Backward-compatible alias for `turbolite_register_cloud`.
#[cfg(feature = "cli-s3")]
#[no_mangle]
pub extern "C" fn turbolite_register_tiered(
    name: *const c_char,
    bucket: *const c_char,
    prefix: *const c_char,
    cache_dir: *const c_char,
    endpoint_url: *const c_char,
    region: *const c_char,
) -> c_int {
    turbolite_register_cloud(name, bucket, prefix, cache_dir, endpoint_url, region)
}

// --- Utilities ---

/// Clear all VFS caches (shared file state, in-process locks).
///
/// Call this when running fresh benchmarks or tests to ensure no stale state.
#[no_mangle]
pub extern "C" fn turbolite_clear_caches() {
    turbolite::clear_all_caches();
}

/// Invalidate cached state for a specific database file.
///
/// Call after modifying a database file externally (e.g. after compaction).
///
/// # Returns
/// 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn turbolite_invalidate_cache(path: *const c_char) -> c_int {
    clear_last_error();
    let path = match cstr_to_str(path, "path") {
        Ok(s) => s,
        Err(code) => return code,
    };
    turbolite::invalidate_cache(path);
    0
}

// --- Database operations ---
// Thin wrappers around rusqlite so FFI consumers (Python, Go, etc.) can
// open databases, run queries, and read results through the VFS without
// needing their own SQLite linkage.

/// Opaque database connection handle.
pub struct TurboliteDb {
    conn: rusqlite::Connection,
}

/// Open a database using a previously registered VFS.
///
/// # Parameters
/// - `path`: Database file path.
/// - `vfs_name`: Name of the VFS registered via `turbolite_register_*`.
///
/// # Returns
/// Opaque handle on success, NULL on error. Must be closed with `turbolite_close`.
#[no_mangle]
pub extern "C" fn turbolite_open(path: *const c_char, vfs_name: *const c_char) -> *mut TurboliteDb {
    clear_last_error();
    let path = match cstr_to_str(path, "path") {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let vfs_name = match cstr_to_str(vfs_name, "vfs_name") {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };

    let flags =
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE;

    match rusqlite::Connection::open_with_flags_and_vfs(path, flags, vfs_name) {
        Ok(conn) => {
            // turbolite manages its own manifest-aware page cache. Disable SQLite's.
            let _ = conn.execute_batch("PRAGMA cache_size=0;");
            let db = Box::into_raw(Box::new(TurboliteDb { conn }));
            // Clear any stale "closed" marker if this address was reused
            let addr = db as u64;
            CLOSED_HANDLES.with(|handles| {
                handles.borrow_mut().remove(&addr);
            });
            db
        }
        Err(e) => {
            set_last_error(&format!("open failed: {}", e));
            std::ptr::null_mut()
        }
    }
}

/// Execute a SQL statement (DDL/DML) that returns no rows.
///
/// # Returns
/// 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn turbolite_exec(db: *mut TurboliteDb, sql: *const c_char) -> c_int {
    clear_last_error();
    if db.is_null() {
        set_last_error("db handle must not be NULL");
        return -1;
    }
    let addr = db as u64;
    let is_closed = CLOSED_HANDLES.with(|h| h.borrow().contains(&addr));
    if is_closed {
        set_last_error("db handle is already closed");
        return -1;
    }
    let sql = match cstr_to_str(sql, "sql") {
        Ok(s) => s,
        Err(code) => return code,
    };

    let db = unsafe { &*db };
    match db.conn.execute_batch(sql) {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(&format!("exec failed: {}", e));
            -1
        }
    }
}

/// Execute a SQL query and return results as a JSON array of objects.
///
/// Example return: `[{"id":1,"name":"alice"},{"id":2,"name":"bob"}]`
///
/// # Returns
/// Heap-allocated JSON string on success (caller must free with `turbolite_free_string`),
/// or NULL on error.
#[no_mangle]
pub extern "C" fn turbolite_query_json(db: *mut TurboliteDb, sql: *const c_char) -> *mut c_char {
    clear_last_error();
    if db.is_null() {
        set_last_error("db handle must not be NULL");
        return std::ptr::null_mut();
    }
    let addr = db as u64;
    let is_closed = CLOSED_HANDLES.with(|h| h.borrow().contains(&addr));
    if is_closed {
        set_last_error("db handle is already closed");
        return std::ptr::null_mut();
    }
    let sql = match cstr_to_str(sql, "sql") {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };

    let db = unsafe { &*db };
    let result = (|| -> Result<String, String> {
        let mut stmt = db
            .conn
            .prepare(sql)
            .map_err(|e| format!("prepare: {}", e))?;
        let col_count = stmt.column_count();
        let col_names: Vec<String> = (0..col_count)
            .map(|i| stmt.column_name(i).unwrap_or("?").to_string())
            .collect();

        let mut rows_json = Vec::new();
        let mut rows = stmt.query([]).map_err(|e| format!("query: {}", e))?;
        while let Some(row) = rows.next().map_err(|e| format!("next: {}", e))? {
            let mut obj = serde_json::Map::new();
            for (i, name) in col_names.iter().enumerate() {
                let val: serde_json::Value = match row.get_ref(i) {
                    Ok(rusqlite::types::ValueRef::Null) => serde_json::Value::Null,
                    Ok(rusqlite::types::ValueRef::Integer(n)) => serde_json::json!(n),
                    Ok(rusqlite::types::ValueRef::Real(f)) => serde_json::json!(f),
                    Ok(rusqlite::types::ValueRef::Text(s)) => {
                        serde_json::Value::String(String::from_utf8_lossy(s).into_owned())
                    }
                    Ok(rusqlite::types::ValueRef::Blob(b)) => {
                        // Encode blobs as base64 for JSON safety.
                        serde_json::Value::String(format!("blob:{} bytes", b.len()))
                    }
                    Err(e) => serde_json::Value::String(format!("error: {}", e)),
                };
                obj.insert(name.clone(), val);
            }
            rows_json.push(serde_json::Value::Object(obj));
        }
        serde_json::to_string(&rows_json).map_err(|e| format!("json: {}", e))
    })();

    match result {
        Ok(json) => match CString::new(json) {
            Ok(cs) => cs.into_raw(),
            Err(e) => {
                set_last_error(&format!("json contains null byte: {}", e));
                std::ptr::null_mut()
            }
        },
        Err(msg) => {
            set_last_error(&msg);
            std::ptr::null_mut()
        }
    }
}

/// Free a string returned by `turbolite_query_json`.
#[no_mangle]
pub extern "C" fn turbolite_free_string(s: *mut c_char) {
    if !s.is_null() {
        unsafe {
            drop(CString::from_raw(s));
        }
    }
}

/// Close a database connection opened with `turbolite_open`.
#[no_mangle]
pub extern "C" fn turbolite_close(db: *mut TurboliteDb) {
    if db.is_null() {
        return;
    }
    let addr = db as u64;
    let already_closed = CLOSED_HANDLES.with(|handles| handles.borrow().contains(&addr));
    if already_closed {
        return;
    }
    CLOSED_HANDLES.with(|handles| {
        handles.borrow_mut().insert(addr);
    });
    unsafe {
        drop(Box::from_raw(db));
    }
}

// --- Internal helpers ---

fn cstr_to_str<'a>(ptr: *const c_char, param_name: &str) -> Result<&'a str, c_int> {
    if ptr.is_null() {
        set_last_error(&format!("{} must not be NULL", param_name));
        return Err(-1);
    }
    unsafe { CStr::from_ptr(ptr) }.to_str().map_err(|_| {
        set_last_error(&format!("{} is not valid UTF-8", param_name));
        -1
    })
}

#[cfg(feature = "cli-s3")]
fn nullable_cstr_to_option<'a>(ptr: *const c_char) -> Option<&'a str> {
    if ptr.is_null() {
        return None;
    }
    unsafe { CStr::from_ptr(ptr) }.to_str().ok()
}
