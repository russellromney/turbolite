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
#[cfg(feature = "cloud")]
static BENCH_HANDLE: std::sync::OnceLock<crate::tiered::TurboliteSharedState> = std::sync::OnceLock::new();

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
    use crate::tiered::{TurboliteConfig, TurboliteVfs, StorageBackend};

    let level = std::env::var("TURBOLITE_COMPRESSION_LEVEL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);
    let cache_dir = std::env::var("TURBOLITE_CACHE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("."));

    let config = TurboliteConfig {
        storage_backend: StorageBackend::Local,
        cache_dir,
        compression_level: level,
        ..Default::default()
    };
    let vfs = TurboliteVfs::new(config)?;
    crate::tiered::register("turbolite", vfs)
}

#[cfg(feature = "cloud")]
fn register_tiered() -> Result<(), std::io::Error> {
    use std::path::PathBuf;
    use crate::tiered::{TurboliteConfig, TurboliteVfs};

    let bucket = std::env::var("TURBOLITE_BUCKET")
        .expect("TURBOLITE_BUCKET must be set for tiered mode");
    let prefix = std::env::var("TURBOLITE_PREFIX")
        .unwrap_or_else(|_| "turbolite".into());
    let cache_dir = std::env::var("TURBOLITE_CACHE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp/turbolite"));
    let endpoint_url = std::env::var("TURBOLITE_ENDPOINT_URL")
        .or_else(|_| std::env::var("AWS_ENDPOINT_URL"))
        .ok();
    let region = std::env::var("TURBOLITE_REGION")
        .or_else(|_| std::env::var("AWS_REGION"))
        .ok();
    let prefetch_threads = std::env::var("TURBOLITE_PREFETCH_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let compression_level = std::env::var("TURBOLITE_COMPRESSION_LEVEL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);
    let read_only = std::env::var("TURBOLITE_READ_ONLY")
        .map(|s| s == "1" || s == "true")
        .unwrap_or(false);

    let mut config = TurboliteConfig {
        bucket,
        prefix,
        cache_dir,
        endpoint_url,
        region,
        compression_level,
        read_only,
        ..Default::default()
    };
    if prefetch_threads > 0 {
        config.prefetch_threads = prefetch_threads;
    }

    let vfs = TurboliteVfs::new(config)?;
    let _ = BENCH_HANDLE.set(vfs.shared_state());
    crate::tiered::register("turbolite-s3", vfs)
}

// ── Bench SQL functions (FFI, called from ext_entry.c) ──────────────────

/// Clear cache. mode: 0 = all, 1 = data only, 2 = interior only (keeps interior + group 0).
/// Returns 0 on success, 1 if no tiered VFS.
#[cfg(feature = "cloud")]
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

/// Reset S3 counters. Returns 0 on success.
#[cfg(feature = "cloud")]
#[no_mangle]
pub extern "C" fn turbolite_bench_reset_s3() -> i32 {
    match BENCH_HANDLE.get() {
        Some(h) => { h.reset_s3_counters(); 0 }
        None => 1,
    }
}

/// Get S3 GET count since last reset.
#[cfg(feature = "cloud")]
#[no_mangle]
pub extern "C" fn turbolite_bench_s3_gets() -> i64 {
    BENCH_HANDLE.get().map_or(0, |h| h.s3_counters().0 as i64)
}

/// Get S3 GET bytes since last reset.
#[cfg(feature = "cloud")]
#[no_mangle]
pub extern "C" fn turbolite_bench_s3_bytes() -> i64 {
    BENCH_HANDLE.get().map_or(0, |h| h.s3_counters().1 as i64)
}

// ── Phase Stalingrad: cache eviction + observability ──────────────────

/// Evict cached data for named trees. tree_names is a comma-separated C string.
/// Returns number of groups evicted, or -1 if no tiered VFS.
#[cfg(feature = "cloud")]
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

#[cfg(not(feature = "cloud"))]
#[no_mangle]
pub unsafe extern "C" fn turbolite_evict_tree(_tree_names: *const std::os::raw::c_char) -> i32 {
    -1
}

/// Return cache info as a JSON C string. Caller must treat as SQLITE_TRANSIENT.
/// Returns null if no tiered VFS.
///
/// Uses a thread-local buffer to avoid allocation lifetime issues across FFI.
#[cfg(feature = "cloud")]
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
                Ok(c) => {
                    CACHE_INFO_BUF.with(|buf| {
                        *buf.borrow_mut() = c;
                        buf.borrow().as_ptr()
                    })
                }
                Err(_) => std::ptr::null(),
            }
        }
        None => std::ptr::null(),
    }
}

/// Warm cache for a planned query. Runs EQP to extract trees, submits groups to prefetch.
/// Returns JSON C string with trees warmed and groups submitted. Null if no tiered VFS.
/// db must be a valid sqlite3 handle, sql must be a valid C string.
#[cfg(feature = "cloud")]
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
            let accesses = crate::tiered::run_eqp_and_parse(db, sql_str);
            let json = h.warm_from_plan(&accesses);
            match std::ffi::CString::new(json) {
                Ok(c) => {
                    WARM_BUF.with(|buf| {
                        *buf.borrow_mut() = c;
                        buf.borrow().as_ptr()
                    })
                }
                Err(_) => std::ptr::null(),
            }
        }
        None => std::ptr::null(),
    }
}

#[cfg(not(feature = "cloud"))]
#[no_mangle]
pub unsafe extern "C" fn turbolite_warm(
    _db: *mut std::ffi::c_void,
    _sql: *const std::os::raw::c_char,
) -> *const std::os::raw::c_char {
    std::ptr::null()
}

/// Evict cached data for trees referenced by a SQL query. Runs EQP, extracts
/// tree names, evicts their groups. Returns groups evicted, or -1 if no VFS.
#[cfg(feature = "cloud")]
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
            let accesses = crate::tiered::run_eqp_and_parse(db, sql_str);
            h.evict_query(&accesses) as i32
        }
        None => -1,
    }
}

#[cfg(not(feature = "cloud"))]
#[no_mangle]
pub unsafe extern "C" fn turbolite_evict_query(
    _db: *mut std::ffi::c_void,
    _sql: *const std::os::raw::c_char,
) -> i32 {
    -1
}

/// Evict cached sub-chunks by tier. Accepts "data", "index", or "all".
/// Returns number of sub-chunks evicted, or -1 if no tiered VFS.
#[cfg(feature = "cloud")]
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

#[cfg(not(feature = "cloud"))]
#[no_mangle]
pub unsafe extern "C" fn turbolite_evict(_tier: *const std::os::raw::c_char) -> i32 {
    -1
}

#[cfg(not(feature = "cloud"))]
#[no_mangle]
pub extern "C" fn turbolite_cache_info() -> *const std::os::raw::c_char {
    std::ptr::null()
}

/// Full GC: list all S3 objects under prefix, delete orphans not in manifest.
/// Returns number of objects deleted, or -1 if no tiered VFS.
#[cfg(feature = "cloud")]
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
#[cfg(feature = "cloud")]
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

#[cfg(not(feature = "cloud"))]
#[no_mangle]
pub extern "C" fn turbolite_compact() -> *const std::os::raw::c_char {
    std::ptr::null()
}

#[cfg(not(feature = "cloud"))]
#[no_mangle]
pub extern "C" fn turbolite_gc() -> i32 {
    -1
}

#[cfg(not(feature = "cloud"))]
fn register_tiered() -> Result<(), std::io::Error> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "TURBOLITE_BUCKET is set but this extension was built without the 'cloud' feature. \
         Rebuild with: make ext  (includes cloud by default)",
    ))
}
