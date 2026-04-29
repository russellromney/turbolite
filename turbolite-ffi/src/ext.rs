//! SQLite loadable extension support.
//!
//! Exports `turbolite_ext_register_vfs()` which is called from the C entry
//! point in `ext_entry.c` after `SQLITE_EXTENSION_INIT2` stores the API table.
//!
//! ## VFS registration
//!
//! Always registers **"turbolite"** — local TurboliteVfs (manifest + page groups).
//!
//! If `TURBOLITE_BUCKET` is set, also registers **"turbolite-s3"** — tiered
//! S3 VFS. Fails hard if bucket is set but configuration is invalid.
//!
//! ### Environment variables (tiered mode)
//!
//! | Variable | Required | Default | Description |
//! |---|---|---|---|
//! | `TURBOLITE_BUCKET` | yes | — | S3 bucket name (triggers S3 VFS registration) |
//! | `TURBOLITE_PREFIX` | no | `"turbolite"` | S3 key prefix |
//! | `TURBOLITE_CACHE_DIR` | no | `"/tmp/turbolite"` | Local cache directory |
//! | `TURBOLITE_ENDPOINT_URL` | no | — | Custom S3 endpoint (Tigris, MinIO) |
//! | `TURBOLITE_REGION` | no | — | AWS region |
//! | `TURBOLITE_PREFETCH_THREADS` | no | `num_cpus + 1` | Prefetch worker threads |
//! | `TURBOLITE_COMPRESSION_LEVEL` | no | `3` | Zstd level 1-22 |
//! | `TURBOLITE_READ_ONLY` | no | `false` | Open in read-only mode |
//!
//! Falls back to `AWS_ENDPOINT_URL` / `AWS_REGION` if the `TURBOLITE_` variants
//! are not set.

use std::sync::atomic::{AtomicBool, Ordering};

static LOCAL_VFS_REGISTERED: AtomicBool = AtomicBool::new(false);
static TIERED_VFS_REGISTERED: AtomicBool = AtomicBool::new(false);

/// Global bench handle for the tiered VFS (set during extension load).
/// Exposed to C via FFI functions for SQL-callable cache control and S3 counters.
#[cfg(feature = "cli-s3")]
static BENCH_HANDLE: std::sync::OnceLock<turbolite::tiered::TurboliteSharedState> =
    std::sync::OnceLock::new();

/// Called from C entry point (`sqlite3_turbolite_init` in ext_entry.c).
/// Returns 0 on success, 1 on error. Idempotent: second call is a no-op.
///
/// Always registers "turbolite" (local TurboliteVfs).
/// If TURBOLITE_BUCKET is set, also registers "turbolite-s3" (tiered VFS).
/// Panics if TURBOLITE_BUCKET is set but tiered VFS creation fails.
#[no_mangle]
pub extern "C" fn turbolite_ext_register_vfs() -> std::os::raw::c_int {
    // Register local VFS (always)
    if !LOCAL_VFS_REGISTERED.swap(true, Ordering::SeqCst) {
        if let Err(e) = register_local() {
            LOCAL_VFS_REGISTERED.store(false, Ordering::SeqCst);
            eprintln!("turbolite: failed to register local VFS: {e}");
            return 1;
        }
    }

    // Register tiered VFS if TURBOLITE_BUCKET is set
    if std::env::var("TURBOLITE_BUCKET").is_ok()
        && !TIERED_VFS_REGISTERED.swap(true, Ordering::SeqCst)
    {
        if let Err(e) = register_tiered() {
            TIERED_VFS_REGISTERED.store(false, Ordering::SeqCst);
            eprintln!("turbolite: TURBOLITE_BUCKET is set but tiered VFS failed: {e}");
            return 1;
        }
    }

    0
}

fn register_local() -> Result<(), std::io::Error> {
    use std::path::PathBuf;
    use turbolite::tiered::{TurboliteConfig, TurboliteVfs};

    // Loadable extensions preserve the historical "." cache_dir fallback when
    // TURBOLITE_CACHE_DIR is unset. Everything else comes from the env-driven
    // constructor.
    let config = TurboliteConfig {
        cache_dir: std::env::var("TURBOLITE_CACHE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(".")),
        ..TurboliteConfig::from_env()
    };
    let vfs = TurboliteVfs::new_local(config)?;
    turbolite::tiered::register("turbolite", vfs)
}

#[cfg(feature = "cli-s3")]
fn register_tiered() -> Result<(), std::io::Error> {
    // The loadable-extension's S3 wiring used the old bucket / prefix /
    // endpoint_url fields on TurboliteConfig. With the backend-agnostic
    // refactor those fields live on the hadb_storage_s3 construction path
    // (the Rust API exposes with_backend). Rewiring the extension
    // entrypoint is tracked under Phase Turbogenesis c5.
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "turbolite loadable-extension S3 mode is being rewired in Phase Turbogenesis c5",
    ))
}

// ── Bench SQL functions (FFI, called from ext_entry.c) ──────────────────

/// Clear cache. mode: 0 = all, 1 = data only, 2 = interior only (keeps interior + group 0).
/// Returns 0 on success, 1 if no tiered VFS.
#[cfg(feature = "cli-s3")]
#[no_mangle]
pub extern "C" fn turbolite_bench_clear_cache(mode: i32) -> i32 {
    match BENCH_HANDLE.get() {
        Some(h) => {
            match mode {
                0 => h.clear_cache_all(),
                1 => h.clear_cache_data_only(),
                2 => h.clear_cache_interior_only(),
                _ => return 1,
            }
            0
        }
        None => 1,
    }
}

/// Reset S3 counters. Always returns 0; counters are now backend-impl
/// specific and not exposed through the generic trait.
#[cfg(feature = "cli-s3")]
#[no_mangle]
pub extern "C" fn turbolite_bench_reset_s3() -> i32 {
    0
}

/// Get S3 GET count. Returns 0: not surfaced through the generic
/// `StorageBackend` trait. Embedders that need per-backend metrics
/// hold a concrete `hadb_storage_s3::S3Storage` directly.
#[cfg(feature = "cli-s3")]
#[no_mangle]
pub extern "C" fn turbolite_bench_s3_gets() -> i64 {
    0
}

/// Get S3 GET bytes. Always 0; see above.
#[cfg(feature = "cli-s3")]
#[no_mangle]
pub extern "C" fn turbolite_bench_s3_bytes() -> i64 {
    0
}

// Cache eviction + observability

/// Evict cached data for named trees. tree_names is a comma-separated C string.
/// Returns number of groups evicted, or -1 if no tiered VFS.
#[cfg(feature = "cli-s3")]
#[no_mangle]
pub unsafe extern "C" fn turbolite_evict_tree(tree_names: *const std::os::raw::c_char) -> i32 {
    if tree_names.is_null() {
        return -1;
    }
    let names = match std::ffi::CStr::from_ptr(tree_names).to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    match BENCH_HANDLE.get() {
        Some(h) => h.evict_tree(names) as i32,
        None => -1,
    }
}

#[cfg(not(feature = "cli-s3"))]
#[no_mangle]
pub unsafe extern "C" fn turbolite_evict_tree(_tree_names: *const std::os::raw::c_char) -> i32 {
    -1
}

/// Return cache info as a JSON C string. Caller must treat as SQLITE_TRANSIENT.
/// Returns null if no tiered VFS.
///
/// Uses a thread-local buffer to avoid allocation lifetime issues across FFI.
#[cfg(feature = "cli-s3")]
#[no_mangle]
pub extern "C" fn turbolite_cache_info() -> *const std::os::raw::c_char {
    thread_local! {
        static CACHE_INFO_BUF: std::cell::RefCell<std::ffi::CString> =
            std::cell::RefCell::new(std::ffi::CString::new("").unwrap());
    }
    match BENCH_HANDLE.get() {
        Some(h) => {
            let json = h.cache_info();
            match std::ffi::CString::new(json) {
                Ok(c) => CACHE_INFO_BUF.with(|buf| {
                    *buf.borrow_mut() = c;
                    buf.borrow().as_ptr()
                }),
                Err(_) => std::ptr::null(),
            }
        }
        None => std::ptr::null(),
    }
}

/// Warm cache for a planned query. Runs EQP to extract trees, submits groups to prefetch.
/// Returns JSON C string with trees warmed and groups submitted. Null if no tiered VFS.
/// db must be a valid sqlite3 handle, sql must be a valid C string.
#[cfg(feature = "cli-s3")]
#[no_mangle]
pub unsafe extern "C" fn turbolite_warm(
    db: *mut std::ffi::c_void,
    sql: *const std::os::raw::c_char,
) -> *const std::os::raw::c_char {
    thread_local! {
        static WARM_BUF: std::cell::RefCell<std::ffi::CString> =
            std::cell::RefCell::new(std::ffi::CString::new("").unwrap());
    }
    if sql.is_null() {
        return std::ptr::null();
    }
    let sql_str = match std::ffi::CStr::from_ptr(sql).to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null(),
    };
    match BENCH_HANDLE.get() {
        Some(h) => {
            let accesses = turbolite::tiered::run_eqp_and_parse(db, sql_str);
            let json = h.warm_from_plan(&accesses);
            match std::ffi::CString::new(json) {
                Ok(c) => WARM_BUF.with(|buf| {
                    *buf.borrow_mut() = c;
                    buf.borrow().as_ptr()
                }),
                Err(_) => std::ptr::null(),
            }
        }
        None => std::ptr::null(),
    }
}

#[cfg(not(feature = "cli-s3"))]
#[no_mangle]
pub unsafe extern "C" fn turbolite_warm(
    _db: *mut std::ffi::c_void,
    _sql: *const std::os::raw::c_char,
) -> *const std::os::raw::c_char {
    std::ptr::null()
}

/// Evict cached data for trees referenced by a SQL query. Runs EQP, extracts
/// tree names, evicts their groups. Returns groups evicted, or -1 if no VFS.
#[cfg(feature = "cli-s3")]
#[no_mangle]
pub unsafe extern "C" fn turbolite_evict_query(
    db: *mut std::ffi::c_void,
    sql: *const std::os::raw::c_char,
) -> i32 {
    if sql.is_null() {
        return -1;
    }
    let sql_str = match std::ffi::CStr::from_ptr(sql).to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    match BENCH_HANDLE.get() {
        Some(h) => {
            let accesses = turbolite::tiered::run_eqp_and_parse(db, sql_str);
            h.evict_query(&accesses) as i32
        }
        None => -1,
    }
}

#[cfg(not(feature = "cli-s3"))]
#[no_mangle]
pub unsafe extern "C" fn turbolite_evict_query(
    _db: *mut std::ffi::c_void,
    _sql: *const std::os::raw::c_char,
) -> i32 {
    -1
}

/// Evict cached sub-chunks by tier. Accepts "data", "index", or "all".
/// Returns number of sub-chunks evicted, or -1 if no tiered VFS.
#[cfg(feature = "cli-s3")]
#[no_mangle]
pub unsafe extern "C" fn turbolite_evict(tier: *const std::os::raw::c_char) -> i32 {
    if tier.is_null() {
        return -1;
    }
    let tier_str = match std::ffi::CStr::from_ptr(tier).to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    match BENCH_HANDLE.get() {
        Some(h) => h.evict_tier(tier_str) as i32,
        None => -1,
    }
}

#[cfg(not(feature = "cli-s3"))]
#[no_mangle]
pub unsafe extern "C" fn turbolite_evict(_tier: *const std::os::raw::c_char) -> i32 {
    -1
}

#[cfg(not(feature = "cli-s3"))]
#[no_mangle]
pub extern "C" fn turbolite_cache_info() -> *const std::os::raw::c_char {
    std::ptr::null()
}

/// Full GC: list all S3 objects under prefix, delete orphans not in manifest.
/// Returns number of objects deleted, or -1 if no tiered VFS.
#[cfg(feature = "cli-s3")]
#[no_mangle]
pub extern "C" fn turbolite_gc() -> i32 {
    match BENCH_HANDLE.get() {
        Some(h) => match h.gc() {
            Ok(count) => count as i32,
            Err(e) => {
                eprintln!("[gc] ERROR: {}", e);
                -1
            }
        },
        None => -1,
    }
}

/// Compact B-tree groups: re-walk B-trees, repack groups with >30% dead space.
/// Returns JSON report string (caller must free), or null on error.
#[cfg(feature = "cli-s3")]
#[no_mangle]
pub extern "C" fn turbolite_compact() -> *const std::os::raw::c_char {
    match BENCH_HANDLE.get() {
        Some(h) => match h.compact(0.3) {
            Ok(json) => {
                let c_str = std::ffi::CString::new(json).unwrap();
                c_str.into_raw()
            }
            Err(e) => {
                eprintln!("[compact] ERROR: {}", e);
                std::ptr::null()
            }
        },
        None => std::ptr::null(),
    }
}

#[cfg(not(feature = "cli-s3"))]
#[no_mangle]
pub extern "C" fn turbolite_compact() -> *const std::os::raw::c_char {
    std::ptr::null()
}

#[cfg(not(feature = "cli-s3"))]
#[no_mangle]
pub extern "C" fn turbolite_gc() -> i32 {
    -1
}

/// Create and register a new local VFS with a custom name and cache directory.
/// Called from SQL: SELECT turbolite_register_vfs('name', '/path/to/cache_dir').
/// Each registered VFS gets its own manifest, cache, and page group state,
/// enabling multiple independent databases in the same process.
/// Returns 0 on success, 1 on error.
#[no_mangle]
pub extern "C" fn turbolite_ext_register_named_vfs(
    name_ptr: *const std::os::raw::c_char,
    cache_dir_ptr: *const std::os::raw::c_char,
) -> std::os::raw::c_int {
    let name = unsafe {
        match std::ffi::CStr::from_ptr(name_ptr).to_str() {
            Ok(s) => s.to_string(),
            Err(_) => return 1,
        }
    };
    let cache_dir = unsafe {
        match std::ffi::CStr::from_ptr(cache_dir_ptr).to_str() {
            Ok(s) => std::path::PathBuf::from(s),
            Err(_) => return 1,
        }
    };

    let config = turbolite::tiered::TurboliteConfig {
        cache_dir,
        ..turbolite::tiered::TurboliteConfig::from_env()
    };
    match turbolite::tiered::TurboliteVfs::new_local(config) {
        Ok(vfs) => match turbolite::tiered::register(&name, vfs) {
            Ok(()) => 0,
            Err(e) => {
                eprintln!("turbolite: failed to register VFS '{}': {}", name, e);
                1
            }
        },
        Err(e) => {
            eprintln!("turbolite: failed to create VFS '{}': {}", name, e);
            1
        }
    }
}

#[cfg(not(feature = "cli-s3"))]
fn register_tiered() -> Result<(), std::io::Error> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "TURBOLITE_BUCKET is set but this extension was built without the 'cloud' feature. \
         Rebuild with: make ext  (includes cloud by default)",
    ))
}
