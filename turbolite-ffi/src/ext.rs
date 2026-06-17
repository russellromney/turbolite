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

/// Run an FFI body under `catch_unwind`, returning `fallback` on panic.
///
/// Unwinding across the `extern "C"` boundary is undefined behavior, so every
/// exported function below routes its body through this guard and converts a
/// caught panic into the function's documented error sentinel. The closure is
/// wrapped in `AssertUnwindSafe` because these bodies legitimately touch raw
/// pointers and process-global state across the boundary; on the panic path
/// only a fixed sentinel is returned.
fn ext_guard<F, R>(fallback: R, body: F) -> R
where
    F: FnOnce() -> R,
{
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)) {
        Ok(value) => value,
        Err(_) => fallback,
    }
}

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
///
/// # Safety
/// Called by SQLite during extension load. `db`, `pz_err_msg`, and `api` must
/// be the valid pointers SQLite passes to an extension entry point.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_turbolite_init(
    db: *mut c_void,
    pz_err_msg: *mut *mut c_char,
    api: *const c_void,
) -> c_int {
    // SQLITE_ERROR == 1: a panic unwinding from the C init (which may call
    // back into Rust registration) across this boundary is UB.
    ext_guard(1, || {
        turbolite_c_sqlite3_turbolite_init(db, pz_err_msg, api)
    })
}

static LOCAL_VFS_REGISTERED: AtomicBool = AtomicBool::new(false);
static TIERED_VFS_REGISTERED: AtomicBool = AtomicBool::new(false);
#[cfg(feature = "cli-s3")]
static FLUSH_THREAD_STARTED: AtomicBool = AtomicBool::new(false);

/// Global bench handles keyed by VFS name. Replaces the single
/// `OnceLock<TurboliteSharedState>` so multiple tiered/S3 VFS registrations
/// in one process each keep their shared state addressable by name.
/// Stored as `Arc` so lookups can hand out an owned handle without requiring
/// `TurboliteSharedState` to be `Clone`.
#[cfg(feature = "cli-s3")]
static BENCH_HANDLES: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<String, std::sync::Arc<turbolite::tiered::TurboliteSharedState>>>,
> = std::sync::OnceLock::new();

/// Name of the most recently registered tiered VFS. Existing bench/cache
/// helpers that take no VFS name target this handle for backward
/// compatibility.
#[cfg(feature = "cli-s3")]
static MOST_RECENT_TIERED_VFS: std::sync::OnceLock<String> = std::sync::OnceLock::new();

#[cfg(feature = "cli-s3")]
fn bench_handles() -> &'static std::sync::Mutex<
    std::collections::HashMap<String, std::sync::Arc<turbolite::tiered::TurboliteSharedState>>,
> {
    BENCH_HANDLES.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

#[cfg(feature = "cli-s3")]
static S3_RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();



/// Store a tiered VFS's shared state keyed by name and mark it as the most
/// recently registered tiered VFS.
#[cfg(feature = "cli-s3")]
fn insert_bench_handle(name: &str, state: turbolite::tiered::TurboliteSharedState) {
    let mut handles = bench_handles().lock().unwrap_or_else(|e| e.into_inner());
    handles.insert(name.to_string(), std::sync::Arc::new(state));
    let _ = MOST_RECENT_TIERED_VFS.set(name.to_string());
}

/// Look up a bench/shared-state handle by VFS name, or fall back to the most
/// recently registered tiered VFS when `name` is `None`.
#[cfg(feature = "cli-s3")]
fn get_bench_handle(
    name: Option<&str>,
) -> Option<std::sync::Arc<turbolite::tiered::TurboliteSharedState>> {
    let handles = bench_handles().lock().unwrap_or_else(|e| e.into_inner());
    if let Some(n) = name {
        handles.get(n).cloned()
    } else {
        MOST_RECENT_TIERED_VFS
            .get()
            .and_then(|n| handles.get(n).cloned())
    }
}

/// Returns the shared S3 registration runtime, or `None` if it could not be
/// built. Built lazily once. Returning `None` (instead of `.expect`-panicking)
/// keeps the failure on the FFI error path rather than unwinding across C.
#[cfg(feature = "cli-s3")]
fn s3_runtime() -> Option<&'static tokio::runtime::Runtime> {
    // OnceLock has no fallible get_or_init on stable, so build first and only
    // store on success; a transient failure can be retried on the next call.
    if let Some(rt) = S3_RUNTIME.get() {
        return Some(rt);
    }
    match tokio::runtime::Runtime::new() {
        Ok(rt) => {
            let _ = S3_RUNTIME.set(rt);
            S3_RUNTIME.get()
        }
        Err(e) => {
            eprintln!("turbolite: failed to build shared S3 registration runtime: {e}");
            None
        }
    }
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
#[derive(Default)]
struct CounterBaselines {
    counters: Option<std::sync::Arc<StorageCounters>>,
    base_gets: u64,
    base_get_bytes: u64,
    base_puts: u64,
    base_put_bytes: u64,
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
            if let Some(handle) = get_bench_handle(None) {
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
/// Returns 1 if TURBOLITE_BUCKET is set but tiered VFS creation fails.
#[no_mangle]
pub extern "C" fn turbolite_ext_register_vfs() -> std::os::raw::c_int {
    ext_guard(1, || {
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
    })
}

/// Register a tiered VFS with SQLite through sqlite-plugin. In loadable-extension
/// mode the registration resolves its `sqlite3_*` symbols through the API-table
/// shims in ext_entry.c.
#[inline]
fn register_turbolite_vfs(
    name: &str,
    vfs: turbolite::tiered::TurboliteVfs,
) -> Result<(), std::io::Error> {
    turbolite::tiered::register(name, vfs)
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
    register_turbolite_vfs("turbolite", vfs)
}

#[cfg(feature = "cli-s3")]
fn register_tiered() -> Result<(), std::io::Error> {
    use std::path::PathBuf;
    use std::sync::Arc;
    use turbolite::tiered::{TurboliteConfig, TurboliteVfs};

    let bucket = std::env::var("TURBOLITE_BUCKET").map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "TURBOLITE_BUCKET must be set for tiered mode",
        )
    })?;
    let prefix = std::env::var("TURBOLITE_PREFIX").unwrap_or_else(|_| "turbolite".into());
    let cache_dir = std::env::var("TURBOLITE_CACHE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp/turbolite"));
    let endpoint_url = std::env::var("TURBOLITE_ENDPOINT_URL")
        .or_else(|_| std::env::var("AWS_ENDPOINT_URL"))
        .ok();

    let handle = match s3_runtime() {
        Some(rt) => rt.handle().clone(),
        None => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "S3 registration runtime unavailable",
            ));
        }
    };
    let backend = handle
        .block_on(async {
            hadb_storage_s3::S3Storage::from_env(bucket, endpoint_url.as_deref())
                .await
                .map(|storage| storage.with_prefix(prefix))
        })
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let backend: Arc<dyn hadb_storage::StorageBackend> = Arc::new(backend);
    let (counting_backend, counters_handle) = CountingStorageBackend::new(backend);
    let backend: Arc<dyn hadb_storage::StorageBackend> = Arc::new(counting_backend);
    {
        let mut counters = s3_counters().lock().unwrap_or_else(|e| e.into_inner());
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
    // Legacy process-global shim; new VFS instances derive checkpoint mode from
    // config.cache.checkpoint_mode and ignore this flag. Kept as-is to preserve
    // existing behavior; allow the deprecation rather than change semantics.
    #[allow(deprecated)]
    turbolite::tiered::set_local_checkpoint_only(
        std::env::var("TURBOLITE_LOCAL_THEN_FLUSH")
            .map(|v| !matches!(v.as_str(), "false" | "0"))
            .unwrap_or(true),
    );
    let vfs = TurboliteVfs::with_backend(config, backend, handle)?;
    insert_bench_handle("turbolite-s3", vfs.shared_state());
    let flush_interval_ms = std::env::var("TURBOLITE_FLUSH_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(15_000);
    start_background_flush(flush_interval_ms);
    register_turbolite_vfs("turbolite-s3", vfs)
}

// ── Bench SQL functions (FFI, called from ext_entry.c) ──────────────────

/// Clear cache. mode: 0 = all, 1 = data only, 2 = interior only (keeps interior + group 0).
/// Returns 0 on success, 1 if no tiered VFS.
#[cfg(feature = "cli-s3")]
#[no_mangle]
pub extern "C" fn turbolite_bench_clear_cache(mode: i32) -> i32 {
    ext_guard(1, || match get_bench_handle(None) {
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
    })
}

#[cfg(feature = "cli-s3")]
#[no_mangle]
pub extern "C" fn turbolite_bench_flush_to_storage() -> i32 {
    ext_guard(1, || match get_bench_handle(None) {
        Some(h) => match h.flush_to_storage() {
            Ok(()) => 0,
            Err(e) => {
                eprintln!("turbolite: flush_to_storage failed: {e}");
                1
            }
        },
        None => 1,
    })
}

/// Reset S3 counters. Always returns 0; counters are now backend-impl
/// specific and not exposed through the generic trait.
#[cfg(feature = "cli-s3")]
#[no_mangle]
pub extern "C" fn turbolite_bench_reset_s3() -> i32 {
    ext_guard(1, || {
        let mut counters = s3_counters().lock().unwrap_or_else(|e| e.into_inner());
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
    })
}

/// Get S3 GET count. Returns 0: not surfaced through the generic
/// `StorageBackend` trait. Embedders that need per-backend metrics
/// hold a concrete `hadb_storage_s3::S3Storage` directly.
#[cfg(feature = "cli-s3")]
#[no_mangle]
pub extern "C" fn turbolite_bench_s3_gets() -> i64 {
    ext_guard(0, || {
        let counters = s3_counters().lock().unwrap_or_else(|e| e.into_inner());
        counters.counters.as_ref().map_or(0, |storage_counters| {
            storage_counters
                .gets
                .load(std::sync::atomic::Ordering::Relaxed)
                .saturating_sub(counters.base_gets) as i64
        })
    })
}

/// Get S3 GET bytes. Always 0; see above.
#[cfg(feature = "cli-s3")]
#[no_mangle]
pub extern "C" fn turbolite_bench_s3_bytes() -> i64 {
    ext_guard(0, || {
        let counters = s3_counters().lock().unwrap_or_else(|e| e.into_inner());
        counters.counters.as_ref().map_or(0, |storage_counters| {
            storage_counters
                .get_bytes
                .load(std::sync::atomic::Ordering::Relaxed)
                .saturating_sub(counters.base_get_bytes) as i64
        })
    })
}

#[cfg(feature = "cli-s3")]
#[no_mangle]
pub extern "C" fn turbolite_bench_s3_puts() -> i64 {
    ext_guard(0, || {
        let counters = s3_counters().lock().unwrap_or_else(|e| e.into_inner());
        counters.counters.as_ref().map_or(0, |storage_counters| {
            storage_counters
                .puts
                .load(std::sync::atomic::Ordering::Relaxed)
                .saturating_sub(counters.base_puts) as i64
        })
    })
}

#[cfg(feature = "cli-s3")]
#[no_mangle]
pub extern "C" fn turbolite_bench_s3_put_bytes() -> i64 {
    ext_guard(0, || {
        let counters = s3_counters().lock().unwrap_or_else(|e| e.into_inner());
        counters.counters.as_ref().map_or(0, |storage_counters| {
            storage_counters
                .put_bytes
                .load(std::sync::atomic::Ordering::Relaxed)
                .saturating_sub(counters.base_put_bytes) as i64
        })
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
///
/// # Safety
/// `tree_names` must be NULL or a valid, NUL-terminated C string.
#[cfg(feature = "cli-s3")]
#[no_mangle]
pub unsafe extern "C" fn turbolite_evict_tree(tree_names: *const std::os::raw::c_char) -> i32 {
    ext_guard(-1, || {
        if tree_names.is_null() {
            return -1;
        }
        let names = match std::ffi::CStr::from_ptr(tree_names).to_str() {
            Ok(s) => s,
            Err(_) => return -1,
        };
        match get_bench_handle(None) {
            Some(h) => h.evict_tree(names) as i32,
            None => -1,
        }
    })
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
    ext_guard(std::ptr::null(), || {
        thread_local! {
            static CACHE_INFO_BUF: std::cell::RefCell<std::ffi::CString> =
                std::cell::RefCell::new(std::ffi::CString::default());
        }
        match get_bench_handle(None) {
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
    })
}

/// Warm cache for a planned query. Runs EQP to extract trees, submits groups to prefetch.
/// Returns JSON C string with trees warmed and groups submitted. Null if no tiered VFS.
/// db must be a valid sqlite3 handle, sql must be a valid C string.
///
/// # Safety
/// `db` must be a valid `sqlite3*` handle. `sql` must be NULL or a valid,
/// NUL-terminated C string.
#[cfg(feature = "cli-s3")]
#[no_mangle]
pub unsafe extern "C" fn turbolite_warm(
    db: *mut std::ffi::c_void,
    sql: *const std::os::raw::c_char,
) -> *const std::os::raw::c_char {
    ext_guard(std::ptr::null(), || {
        thread_local! {
            static WARM_BUF: std::cell::RefCell<std::ffi::CString> =
                std::cell::RefCell::new(std::ffi::CString::default());
        }
        if sql.is_null() {
            return std::ptr::null();
        }
        let sql_str = match std::ffi::CStr::from_ptr(sql).to_str() {
            Ok(s) => s,
            Err(_) => return std::ptr::null(),
        };
        match get_bench_handle(None) {
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
    })
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
///
/// # Safety
/// `db` must be a valid `sqlite3*` handle. `sql` must be NULL or a valid,
/// NUL-terminated C string.
#[cfg(feature = "cli-s3")]
#[no_mangle]
pub unsafe extern "C" fn turbolite_evict_query(
    db: *mut std::ffi::c_void,
    sql: *const std::os::raw::c_char,
) -> i32 {
    ext_guard(-1, || {
        if sql.is_null() {
            return -1;
        }
        let sql_str = match std::ffi::CStr::from_ptr(sql).to_str() {
            Ok(s) => s,
            Err(_) => return -1,
        };
        match get_bench_handle(None) {
            Some(h) => {
                let accesses = turbolite::tiered::run_eqp_and_parse(db, sql_str);
                h.evict_query(&accesses) as i32
            }
            None => -1,
        }
    })
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
///
/// # Safety
/// `tier` must be NULL or a valid, NUL-terminated C string.
#[cfg(feature = "cli-s3")]
#[no_mangle]
pub unsafe extern "C" fn turbolite_evict(tier: *const std::os::raw::c_char) -> i32 {
    ext_guard(-1, || {
        if tier.is_null() {
            return -1;
        }
        let tier_str = match std::ffi::CStr::from_ptr(tier).to_str() {
            Ok(s) => s,
            Err(_) => return -1,
        };
        match get_bench_handle(None) {
            Some(h) => h.evict_tier(tier_str) as i32,
            None => -1,
        }
    })
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
    ext_guard(-1, || match get_bench_handle(None) {
        Some(h) => match h.gc() {
            Ok(count) => count as i32,
            Err(e) => {
                eprintln!("[gc] ERROR: {}", e);
                -1
            }
        },
        None => -1,
    })
}

/// Compact B-tree groups: re-walk B-trees, repack groups with >30% dead space.
/// Returns a JSON report C string, or null on error.
///
/// The returned pointer is owned by a thread-local buffer and stays valid
/// until the next `turbolite_compact` call on the same thread (treat as
/// SQLITE_TRANSIENT). The caller must NOT free it. The previous version
/// returned `CString::into_raw`, which the C side never freed (a leak per
/// call), and `unwrap`'d on a JSON string containing a NUL byte (a panic
/// across the C boundary).
#[cfg(feature = "cli-s3")]
#[no_mangle]
pub extern "C" fn turbolite_compact() -> *const std::os::raw::c_char {
    ext_guard(std::ptr::null(), || {
        thread_local! {
            static COMPACT_BUF: std::cell::RefCell<std::ffi::CString> =
                std::cell::RefCell::new(std::ffi::CString::default());
        }
        match get_bench_handle(None) {
            Some(h) => match h.compact(0.3) {
                Ok(json) => match std::ffi::CString::new(json) {
                    Ok(c) => COMPACT_BUF.with(|buf| {
                        *buf.borrow_mut() = c;
                        buf.borrow().as_ptr()
                    }),
                    Err(_) => std::ptr::null(),
                },
                Err(e) => {
                    eprintln!("[compact] ERROR: {}", e);
                    std::ptr::null()
                }
            },
            None => std::ptr::null(),
        }
    })
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
///
/// # Safety
/// `name_ptr` and `cache_dir_ptr` must be valid, NUL-terminated C strings.
#[no_mangle]
pub unsafe extern "C" fn turbolite_ext_register_named_vfs(
    name_ptr: *const std::os::raw::c_char,
    cache_dir_ptr: *const std::os::raw::c_char,
) -> std::os::raw::c_int {
    ext_guard(1, || {
        let name = match required_cstr(name_ptr) {
            Some(s) => s,
            None => return 1,
        };
        let cache_dir = match required_cstr(cache_dir_ptr) {
            Some(s) => std::path::PathBuf::from(s),
            None => return 1,
        };

        let config = turbolite::tiered::TurboliteConfig {
            cache_dir,
            ..turbolite::tiered::TurboliteConfig::from_env()
        };
        match turbolite::tiered::TurboliteVfs::new_local(config) {
            Ok(vfs) => match register_turbolite_vfs(&name, vfs) {
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
    })
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
///
/// # Safety
/// `name_ptr` and `db_path_ptr` must be valid, NUL-terminated C strings.
#[no_mangle]
pub unsafe extern "C" fn turbolite_ext_register_file_first_vfs(
    name_ptr: *const std::os::raw::c_char,
    db_path_ptr: *const std::os::raw::c_char,
) -> std::os::raw::c_int {
    ext_guard(1, || {
        let name = match required_cstr(name_ptr) {
            Some(s) => s,
            None => return 1,
        };
        let db_path = match required_cstr(db_path_ptr) {
            Some(s) => std::path::PathBuf::from(s),
            None => return 1,
        };

        let config = turbolite::tiered::TurboliteConfig::from_env().with_database_path(db_path);
        match turbolite::tiered::TurboliteVfs::new_local(config) {
            Ok(vfs) => match register_turbolite_vfs(&name, vfs) {
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
    })
}

/// Decode a required NUL-terminated C string. Returns `None` when `ptr` is
/// NULL, empty, or not valid UTF-8. Used by registration entry points to
/// reject missing required parameters in one place.
fn required_cstr(ptr: *const std::os::raw::c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    unsafe { std::ffi::CStr::from_ptr(ptr).to_str().ok() }
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
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
///
/// # Safety
/// `name_ptr`, `db_path_ptr`, and `bucket_ptr` must be valid, NUL-terminated C
/// strings. `prefix_ptr`/`endpoint_ptr`/`region_ptr` must each be either NULL
/// or a valid NUL-terminated C string.
#[cfg(feature = "cli-s3")]
#[no_mangle]
pub unsafe extern "C" fn turbolite_ext_register_s3_file_first_vfs(
    name_ptr: *const std::os::raw::c_char,
    db_path_ptr: *const std::os::raw::c_char,
    bucket_ptr: *const std::os::raw::c_char,
    prefix_ptr: *const std::os::raw::c_char,
    endpoint_ptr: *const std::os::raw::c_char,
    region_ptr: *const std::os::raw::c_char,
) -> std::os::raw::c_int {
    use std::sync::Arc;
    use turbolite::tiered::{TurboliteConfig, TurboliteVfs};

    ext_guard(1, || {
        let name = match required_cstr(name_ptr) {
            Some(s) => s,
            None => return 1,
        };
        let db_path = match required_cstr(db_path_ptr) {
            Some(s) => std::path::PathBuf::from(s),
            None => return 1,
        };
        let bucket = match required_cstr(bucket_ptr) {
            Some(s) => s,
            None => return 1,
        };
        let prefix = optional_cstr(prefix_ptr).unwrap_or_else(|| "turbolite".to_string());
        let endpoint_url = optional_cstr(endpoint_ptr);
        let region = optional_cstr(region_ptr);

        let handle = match s3_runtime() {
            Some(rt) => rt.handle().clone(),
            None => {
                eprintln!("turbolite: S3 registration runtime unavailable for VFS '{name}'");
                return 1;
            }
        };
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
            let mut counters = s3_counters().lock().unwrap_or_else(|e| e.into_inner());
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
        insert_bench_handle(&name, vfs.shared_state());
        match register_turbolite_vfs(&name, vfs) {
            Ok(()) => 0,
            Err(e) => {
                eprintln!(
                    "turbolite: failed to register file-first S3 VFS '{}': {}",
                    name, e
                );
                1
            }
        }
    })
}

/// # Safety
/// See the `cli-s3` build of this function; this stub dereferences nothing.
#[cfg(not(feature = "cli-s3"))]
#[no_mangle]
pub unsafe extern "C" fn turbolite_ext_register_s3_file_first_vfs(
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

#[cfg(test)]
mod tests {
    use super::*;

    // G5 regression: required string parameters must reject NULL/empty.
    #[test]
    fn required_cstr_rejects_null_and_empty() {
        assert!(required_cstr(std::ptr::null()).is_none());
        let empty = c"";
        assert!(required_cstr(empty.as_ptr()).is_none());
        let valid = c"hello";
        assert_eq!(required_cstr(valid.as_ptr()), Some("hello".to_string()));
    }

    #[test]
    fn ext_register_named_vfs_rejects_null() {
        let rc = unsafe { turbolite_ext_register_named_vfs(std::ptr::null(), std::ptr::null()) };
        assert_eq!(rc, 1);
    }

    #[test]
    fn ext_register_file_first_vfs_rejects_null() {
        let rc =
            unsafe { turbolite_ext_register_file_first_vfs(std::ptr::null(), std::ptr::null()) };
        assert_eq!(rc, 1);
    }

    // G9 regression: bench handles are keyed by VFS name and fall back to the
    // most recently registered tiered VFS.
    #[cfg(feature = "cli-s3")]
    #[test]
    fn bench_handles_keyed_by_name_and_most_recent() {
        // Empty map: no handle.
        assert!(get_bench_handle(None).is_none());
        assert!(get_bench_handle(Some("missing")).is_none());

        // Build two independent local VFSes just to obtain real shared state
        // values; the test only verifies map lookup plumbing.
        let tmp_a = tempfile::tempdir().unwrap();
        let vfs_a = turbolite::tiered::TurboliteVfs::new_local(
            turbolite::tiered::TurboliteConfig::for_database_path(
                tmp_a.path().join("a.db").to_str().unwrap(),
            ),
        )
        .unwrap();
        insert_bench_handle("vfs-a", vfs_a.shared_state());
        assert!(get_bench_handle(Some("vfs-a")).is_some());
        assert!(get_bench_handle(None).is_some(), "most-recent fallback");

        let tmp_b = tempfile::tempdir().unwrap();
        let vfs_b = turbolite::tiered::TurboliteVfs::new_local(
            turbolite::tiered::TurboliteConfig::for_database_path(
                tmp_b.path().join("b.db").to_str().unwrap(),
            ),
        )
        .unwrap();
        insert_bench_handle("vfs-b", vfs_b.shared_state());
        assert!(get_bench_handle(Some("vfs-b")).is_some());
        assert!(get_bench_handle(Some("vfs-a")).is_some(), "earlier handle preserved");
    }
}
