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
//! Bindings can also call `SELECT turbolite_register_file_first_vfs(name,
//! db_path)` per database to register a VFS keyed to a specific file path.
//! That is the recommended user-facing entry point: turbolite owns
//! `db_path` as the local page image and stores its sidecar metadata under
//! `<db_path>-turbolite/`.
//!
//! ### Environment variables
//!
//! | Variable | Required | Default | Description |
//! |---|---|---|---|
//! | `TURBOLITE_DATABASE_PATH` | no | — | File-first local database image path. When set, the default `"turbolite"` VFS registers as file-first and stores sidecar state under `<path>-turbolite/`. |
//! | `TURBOLITE_BUCKET` | (s3) | — | S3 bucket name (triggers S3 VFS registration) |
//! | `TURBOLITE_PREFIX` | no | `"turbolite"` | S3 key prefix |
//! | `TURBOLITE_CACHE_DIR` | no | `"."` (local) / `"/tmp/turbolite"` (s3) | Lower-level cache directory; ignored when `TURBOLITE_DATABASE_PATH` is set. |
//! | `TURBOLITE_ENDPOINT_URL` | no | — | Custom S3 endpoint (Tigris, MinIO) |
//! | `TURBOLITE_REGION` | no | — | AWS region |
//! | `TURBOLITE_PREFETCH_THREADS` | no | `num_cpus + 1` | Prefetch worker threads |
//! | `TURBOLITE_COMPRESSION_LEVEL` | no | `3` | Zstd level 1-22 |
//! | `TURBOLITE_READ_ONLY` | no | `false` | Open in read-only mode |
//!
//! Falls back to `AWS_ENDPOINT_URL` / `AWS_REGION` if the `TURBOLITE_` variants
//! are not set.

use std::os::raw::{c_char, c_int, c_void};
use std::sync::atomic::{AtomicBool, Ordering};

extern "C" {
    fn turbolite_c_sqlite3_turbolite_init(
        db: *mut c_void,
        pz_err_msg: *mut *mut c_char,
        api: *const c_void,
    ) -> c_int;
}

/// SQLite loadable-extension entry point.
///
/// The implementation lives in the C shim so it can use SQLite's extension
/// API-table macros, but the exported symbol is Rust-owned. That keeps the
/// cdylib export behavior consistent across macOS and Linux.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_turbolite_init(
    db: *mut c_void,
    pz_err_msg: *mut *mut c_char,
    api: *const c_void,
) -> c_int {
    turbolite_c_sqlite3_turbolite_init(db, pz_err_msg, api)
}

static LOCAL_VFS_REGISTERED: AtomicBool = AtomicBool::new(false);
static TIERED_VFS_REGISTERED: AtomicBool = AtomicBool::new(false);
#[cfg(feature = "cli-s3")]
static FLUSH_THREAD_STARTED: AtomicBool = AtomicBool::new(false);

/// Global bench handle for the tiered VFS (set during extension load).
/// Exposed to C via FFI functions for SQL-callable cache control and S3 counters.
#[cfg(feature = "cli-s3")]
static BENCH_HANDLE: std::sync::OnceLock<turbolite::tiered::TurboliteSharedState> =
    std::sync::OnceLock::new();

#[cfg(feature = "cli-s3")]
static S3_RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();

#[cfg(feature = "cli-s3")]
fn s3_runtime() -> &'static tokio::runtime::Runtime {
    S3_RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .expect("turbolite: failed to build shared S3 registration runtime")
    })
}

#[cfg(feature = "cli-s3")]
#[derive(Default)]
struct StorageCounters {
    gets: std::sync::atomic::AtomicU64,
    get_bytes: std::sync::atomic::AtomicU64,
    puts: std::sync::atomic::AtomicU64,
    put_bytes: std::sync::atomic::AtomicU64,
}

#[cfg(feature = "cli-s3")]
struct CounterBaselines {
    counters: Option<std::sync::Arc<StorageCounters>>,
    base_gets: u64,
    base_get_bytes: u64,
    base_puts: u64,
    base_put_bytes: u64,
}

#[cfg(feature = "cli-s3")]
impl Default for CounterBaselines {
    fn default() -> Self {
        Self {
            counters: None,
            base_gets: 0,
            base_get_bytes: 0,
            base_puts: 0,
            base_put_bytes: 0,
        }
    }
}

#[cfg(feature = "cli-s3")]
struct CountingStorageBackend {
    inner: std::sync::Arc<dyn hadb_storage::StorageBackend>,
    counters: std::sync::Arc<StorageCounters>,
}

#[cfg(feature = "cli-s3")]
impl CountingStorageBackend {
    fn new(
        inner: std::sync::Arc<dyn hadb_storage::StorageBackend>,
    ) -> (Self, std::sync::Arc<StorageCounters>) {
        let counters = std::sync::Arc::new(StorageCounters::default());
        (
            Self {
                inner,
                counters: counters.clone(),
            },
            counters,
        )
    }
}

#[cfg(feature = "cli-s3")]
#[async_trait::async_trait]
impl hadb_storage::StorageBackend for CountingStorageBackend {
    async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        let result = self.inner.get(key).await?;
        if let Some(bytes) = &result {
            self.counters
                .gets
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.counters
                .get_bytes
                .fetch_add(bytes.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(result)
    }

    async fn put(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
        self.inner.put(key, data).await?;
        self.counters
            .puts
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.counters
            .put_bytes
            .fetch_add(data.len() as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.inner.delete(key).await
    }

    async fn list(&self, prefix: &str, after: Option<&str>) -> anyhow::Result<Vec<String>> {
        self.inner.list(prefix, after).await
    }

    async fn exists(&self, key: &str) -> anyhow::Result<bool> {
        self.inner.exists(key).await
    }

    async fn put_if_absent(
        &self,
        key: &str,
        data: &[u8],
    ) -> anyhow::Result<hadb_storage::CasResult> {
        let result = self.inner.put_if_absent(key, data).await?;
        if result.success {
            self.counters
                .puts
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.counters
                .put_bytes
                .fetch_add(data.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(result)
    }

    async fn put_if_match(
        &self,
        key: &str,
        data: &[u8],
        etag: &str,
    ) -> anyhow::Result<hadb_storage::CasResult> {
        let result = self.inner.put_if_match(key, data, etag).await?;
        if result.success {
            self.counters
                .puts
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.counters
                .put_bytes
                .fetch_add(data.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(result)
    }

    async fn range_get(&self, key: &str, start: u64, len: u32) -> anyhow::Result<Option<Vec<u8>>> {
        let result = self.inner.range_get(key, start, len).await?;
        if let Some(bytes) = &result {
            self.counters
                .gets
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.counters
                .get_bytes
                .fetch_add(bytes.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(result)
    }

    async fn delete_many(&self, keys: &[String]) -> anyhow::Result<usize> {
        self.inner.delete_many(keys).await
    }

    async fn put_many(&self, entries: &[(String, Vec<u8>)]) -> anyhow::Result<()> {
        self.inner.put_many(entries).await?;
        let bytes = entries
            .iter()
            .map(|(_, data)| data.len() as u64)
            .sum::<u64>();
        self.counters
            .puts
            .fetch_add(entries.len() as u64, std::sync::atomic::Ordering::Relaxed);
        self.counters
            .put_bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    fn backend_name(&self) -> &str {
        self.inner.backend_name()
    }
}

#[cfg(feature = "cli-s3")]
static S3_COUNTERS: std::sync::OnceLock<std::sync::Mutex<CounterBaselines>> =
    std::sync::OnceLock::new();

#[cfg(feature = "cli-s3")]
fn s3_counters() -> &'static std::sync::Mutex<CounterBaselines> {
    S3_COUNTERS.get_or_init(|| std::sync::Mutex::new(CounterBaselines::default()))
}

#[cfg(feature = "cli-s3")]
fn start_background_flush(interval_ms: u64) {
    if interval_ms == 0 || FLUSH_THREAD_STARTED.swap(true, Ordering::SeqCst) {
        return;
    }

    std::thread::spawn(move || {
        let interval = std::time::Duration::from_millis(interval_ms);
        loop {
            std::thread::sleep(interval);
            if let Some(handle) = BENCH_HANDLE.get() {
                if handle.has_pending_flush() {
                    if let Err(e) = handle.flush_to_storage() {
                        eprintln!("turbolite: background flush failed: {e}");
                    }
                }
            }
        }
    });
}

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

    // File-first when TURBOLITE_DATABASE_PATH is set: the env-supplied path
    // becomes the local page image and the sidecar lives at
    // `<path>-turbolite/`. Otherwise fall back to the historical "."
    // cache_dir behavior so existing test-extension callers keep working.
    //
    // Both branches start from `from_env()` so other TURBOLITE_* knobs
    // (read-only, compression, prefetch, cache) are honored consistently.
    let config = match std::env::var("TURBOLITE_DATABASE_PATH").map(PathBuf::from) {
        Ok(db_path) => TurboliteConfig::from_env().with_database_path(db_path),
        Err(_) => TurboliteConfig {
            cache_dir: std::env::var("TURBOLITE_CACHE_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from(".")),
            ..TurboliteConfig::from_env()
        },
    };
    let vfs = TurboliteVfs::new_local(config)?;
    turbolite::tiered::register("turbolite", vfs)
}

#[cfg(feature = "cli-s3")]
fn register_tiered() -> Result<(), std::io::Error> {
    use std::path::PathBuf;
    use std::sync::Arc;
    use turbolite::tiered::{TurboliteConfig, TurboliteVfs};

    let bucket =
        std::env::var("TURBOLITE_BUCKET").expect("TURBOLITE_BUCKET must be set for tiered mode");
    let prefix = std::env::var("TURBOLITE_PREFIX").unwrap_or_else(|_| "turbolite".into());
    let cache_dir = std::env::var("TURBOLITE_CACHE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp/turbolite"));
    let endpoint_url = std::env::var("TURBOLITE_ENDPOINT_URL")
        .or_else(|_| std::env::var("AWS_ENDPOINT_URL"))
        .ok();

    let runtime = tokio::runtime::Runtime::new()?;
    let handle = runtime.handle().clone();
    let backend = handle
        .block_on(async {
            hadb_storage_s3::S3Storage::from_env(bucket, endpoint_url.as_deref())
                .await
                .map(|storage| storage.with_prefix(prefix))
        })
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    let backend: Arc<dyn hadb_storage::StorageBackend> = Arc::new(backend);
    let (counting_backend, counters_handle) = CountingStorageBackend::new(backend);
    let backend: Arc<dyn hadb_storage::StorageBackend> = Arc::new(counting_backend);
    {
        let mut counters = s3_counters().lock().expect("s3 counter mutex poisoned");
        counters.counters = Some(counters_handle);
        counters.base_gets = 0;
        counters.base_get_bytes = 0;
        counters.base_puts = 0;
        counters.base_put_bytes = 0;
    }

    // Same shape as register_local: TURBOLITE_DATABASE_PATH selects file-first
    // and overrides cache_dir; otherwise the explicit cache_dir wins.
    let config = match std::env::var("TURBOLITE_DATABASE_PATH").map(PathBuf::from) {
        Ok(db_path) => TurboliteConfig::from_env().with_database_path(db_path),
        Err(_) => TurboliteConfig {
            cache_dir,
            ..TurboliteConfig::from_env()
        },
    };
    turbolite::tiered::set_local_checkpoint_only(
        std::env::var("TURBOLITE_LOCAL_THEN_FLUSH")
            .map(|v| !matches!(v.as_str(), "false" | "0"))
            .unwrap_or(true),
    );
    let vfs = TurboliteVfs::with_backend(config, backend, handle)?;
    let _ = BENCH_HANDLE.set(vfs.shared_state());
    let flush_interval_ms = std::env::var("TURBOLITE_FLUSH_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(15_000);
    start_background_flush(flush_interval_ms);
    // The VFS holds a runtime handle for async storage work. Keep the owned
    // runtime alive for the process lifetime, matching the standalone FFI path.
    std::mem::forget(runtime);
    turbolite::tiered::register("turbolite-s3", vfs)
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

#[cfg(feature = "cli-s3")]
#[no_mangle]
pub extern "C" fn turbolite_bench_flush_to_storage() -> i32 {
    match BENCH_HANDLE.get() {
        Some(h) => match h.flush_to_storage() {
            Ok(()) => 0,
            Err(e) => {
                eprintln!("turbolite: flush_to_storage failed: {e}");
                1
            }
        },
        None => 1,
    }
}

/// Reset S3 counters. Always returns 0; counters are now backend-impl
/// specific and not exposed through the generic trait.
#[cfg(feature = "cli-s3")]
#[no_mangle]
pub extern "C" fn turbolite_bench_reset_s3() -> i32 {
    let mut counters = s3_counters().lock().expect("s3 counter mutex poisoned");
    match counters.counters.clone() {
        Some(storage_counters) => {
            counters.base_gets = storage_counters
                .gets
                .load(std::sync::atomic::Ordering::Relaxed);
            counters.base_get_bytes = storage_counters
                .get_bytes
                .load(std::sync::atomic::Ordering::Relaxed);
            counters.base_puts = storage_counters
                .puts
                .load(std::sync::atomic::Ordering::Relaxed);
            counters.base_put_bytes = storage_counters
                .put_bytes
                .load(std::sync::atomic::Ordering::Relaxed);
            0
        }
        None => 1,
    }
}

/// Get S3 GET count. Returns 0: not surfaced through the generic
/// `StorageBackend` trait. Embedders that need per-backend metrics
/// hold a concrete `hadb_storage_s3::S3Storage` directly.
#[cfg(feature = "cli-s3")]
#[no_mangle]
pub extern "C" fn turbolite_bench_s3_gets() -> i64 {
    let counters = s3_counters().lock().expect("s3 counter mutex poisoned");
    counters.counters.as_ref().map_or(0, |storage_counters| {
        storage_counters
            .gets
            .load(std::sync::atomic::Ordering::Relaxed)
            .saturating_sub(counters.base_gets) as i64
    })
}

/// Get S3 GET bytes. Always 0; see above.
#[cfg(feature = "cli-s3")]
#[no_mangle]
pub extern "C" fn turbolite_bench_s3_bytes() -> i64 {
    let counters = s3_counters().lock().expect("s3 counter mutex poisoned");
    counters.counters.as_ref().map_or(0, |storage_counters| {
        storage_counters
            .get_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
            .saturating_sub(counters.base_get_bytes) as i64
    })
}

#[cfg(feature = "cli-s3")]
#[no_mangle]
pub extern "C" fn turbolite_bench_s3_puts() -> i64 {
    let counters = s3_counters().lock().expect("s3 counter mutex poisoned");
    counters.counters.as_ref().map_or(0, |storage_counters| {
        storage_counters
            .puts
            .load(std::sync::atomic::Ordering::Relaxed)
            .saturating_sub(counters.base_puts) as i64
    })
}

#[cfg(feature = "cli-s3")]
#[no_mangle]
pub extern "C" fn turbolite_bench_s3_put_bytes() -> i64 {
    let counters = s3_counters().lock().expect("s3 counter mutex poisoned");
    counters.counters.as_ref().map_or(0, |storage_counters| {
        storage_counters
            .put_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
            .saturating_sub(counters.base_put_bytes) as i64
    })
}

#[cfg(not(feature = "cli-s3"))]
#[no_mangle]
pub extern "C" fn turbolite_bench_flush_to_storage() -> i32 {
    1
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
///
/// This is the lower-level form: turbolite owns the entire `cache_dir` and
/// stores the local database image at `<cache_dir>/data.cache`. Bindings that
/// expose a user-facing `app.db` should prefer
/// [`turbolite_ext_register_file_first_vfs`] so the user's database path is
/// the local image.
///
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

/// Register a file-first local VFS keyed to a database path.
///
/// Called from SQL via:
///
/// ```sql
/// SELECT turbolite_register_file_first_vfs('app', '/data/app.db');
/// ```
///
/// The caller's `db_path` becomes the local database image and turbolite
/// stores its sidecar metadata at `<db_path>-turbolite/`. This is the
/// recommended user-facing entry point — bindings should expose this rather
/// than the bare `cache_dir`-driven [`turbolite_ext_register_named_vfs`].
///
/// Other `TURBOLITE_*` env vars (compression, cache, prefetch, read-only)
/// are honored via `TurboliteConfig::from_env()`; only the cache_dir is
/// overridden to match the database path.
///
/// Returns 0 on success, 1 on error.
#[no_mangle]
pub extern "C" fn turbolite_ext_register_file_first_vfs(
    name_ptr: *const std::os::raw::c_char,
    db_path_ptr: *const std::os::raw::c_char,
) -> std::os::raw::c_int {
    let name = unsafe {
        match std::ffi::CStr::from_ptr(name_ptr).to_str() {
            Ok(s) => s.to_string(),
            Err(_) => return 1,
        }
    };
    let db_path = unsafe {
        match std::ffi::CStr::from_ptr(db_path_ptr).to_str() {
            Ok(s) => std::path::PathBuf::from(s),
            Err(_) => return 1,
        }
    };

    let config = turbolite::tiered::TurboliteConfig::from_env().with_database_path(db_path);
    match turbolite::tiered::TurboliteVfs::new_local(config) {
        Ok(vfs) => match turbolite::tiered::register(&name, vfs) {
            Ok(()) => 0,
            Err(e) => {
                eprintln!(
                    "turbolite: failed to register file-first VFS '{}': {}",
                    name, e
                );
                1
            }
        },
        Err(e) => {
            eprintln!(
                "turbolite: failed to create file-first VFS '{}': {}",
                name, e
            );
            1
        }
    }
}

#[cfg(feature = "cli-s3")]
fn optional_cstr(ptr: *const std::os::raw::c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    unsafe { std::ffi::CStr::from_ptr(ptr).to_str().ok() }
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

/// Register a file-first S3 VFS keyed to one logical volume.
///
/// Unlike the process-global `turbolite-s3` convenience VFS, this creates a
/// VFS with caller-supplied name and caller-supplied bucket/prefix. Bindings
/// that open many volumes in one process should use this path so each volume
/// has a distinct VFS identity.
#[cfg(feature = "cli-s3")]
#[no_mangle]
pub extern "C" fn turbolite_ext_register_s3_file_first_vfs(
    name_ptr: *const std::os::raw::c_char,
    db_path_ptr: *const std::os::raw::c_char,
    bucket_ptr: *const std::os::raw::c_char,
    prefix_ptr: *const std::os::raw::c_char,
    endpoint_ptr: *const std::os::raw::c_char,
    region_ptr: *const std::os::raw::c_char,
) -> std::os::raw::c_int {
    use std::sync::Arc;
    use turbolite::tiered::{TurboliteConfig, TurboliteVfs};

    let name = unsafe {
        match std::ffi::CStr::from_ptr(name_ptr).to_str() {
            Ok(s) if !s.is_empty() => s.to_string(),
            _ => return 1,
        }
    };
    let db_path = unsafe {
        match std::ffi::CStr::from_ptr(db_path_ptr).to_str() {
            Ok(s) if !s.is_empty() => std::path::PathBuf::from(s),
            _ => return 1,
        }
    };
    let bucket = unsafe {
        match std::ffi::CStr::from_ptr(bucket_ptr).to_str() {
            Ok(s) if !s.is_empty() => s.to_string(),
            _ => return 1,
        }
    };
    let prefix = optional_cstr(prefix_ptr).unwrap_or_else(|| "turbolite".to_string());
    let endpoint_url = optional_cstr(endpoint_ptr);
    let region = optional_cstr(region_ptr);

    let handle = s3_runtime().handle().clone();
    let backend = match handle.block_on(async {
        hadb_storage_s3::S3Storage::from_env(bucket.clone(), endpoint_url.as_deref())
            .await
            .map(|storage| storage.with_prefix(prefix.clone()))
    }) {
        Ok(backend) => backend,
        Err(e) => {
            eprintln!(
                "turbolite: failed to create S3 backend for VFS '{}': {}",
                name, e
            );
            return 1;
        }
    };
    let backend: Arc<dyn hadb_storage::StorageBackend> = Arc::new(backend);
    let (counting_backend, counters_handle) = CountingStorageBackend::new(backend);
    let backend: Arc<dyn hadb_storage::StorageBackend> = Arc::new(counting_backend);
    {
        let mut counters = s3_counters().lock().expect("s3 counter mutex poisoned");
        counters.counters = Some(counters_handle);
        counters.base_gets = 0;
        counters.base_get_bytes = 0;
        counters.base_puts = 0;
        counters.base_put_bytes = 0;
    }

    let mut config = TurboliteConfig::from_env().with_database_path(db_path);
    config.bucket = bucket;
    config.prefix = prefix;
    config.endpoint_url = endpoint_url;
    config.region = region;

    let vfs = match TurboliteVfs::with_backend(config, backend, handle) {
        Ok(vfs) => vfs,
        Err(e) => {
            eprintln!(
                "turbolite: failed to create file-first S3 VFS '{}': {}",
                name, e
            );
            return 1;
        }
    };
    match turbolite::tiered::register(&name, vfs) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!(
                "turbolite: failed to register file-first S3 VFS '{}': {}",
                name, e
            );
            1
        }
    }
}

#[cfg(not(feature = "cli-s3"))]
#[no_mangle]
pub extern "C" fn turbolite_ext_register_s3_file_first_vfs(
    _name_ptr: *const std::os::raw::c_char,
    _db_path_ptr: *const std::os::raw::c_char,
    _bucket_ptr: *const std::os::raw::c_char,
    _prefix_ptr: *const std::os::raw::c_char,
    _endpoint_ptr: *const std::os::raw::c_char,
    _region_ptr: *const std::os::raw::c_char,
) -> std::os::raw::c_int {
    eprintln!("turbolite: file-first S3 VFS registration requires the 'cli-s3' feature");
    1
}

#[cfg(not(feature = "cli-s3"))]
fn register_tiered() -> Result<(), std::io::Error> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "TURBOLITE_BUCKET is set but this extension was built without the 'cloud' feature. \
         Rebuild with: make ext  (includes cloud by default)",
    ))
}
