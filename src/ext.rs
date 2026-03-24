//! SQLite loadable extension support.
//!
//! Exports `turbolite_ext_register_vfs()` which is called from the C entry
//! point in `ext_entry.c` after `SQLITE_EXTENSION_INIT2` stores the API table.
//!
//! The C shim provides symbol shims for `sqlite3_vfs_register` etc. that route
//! through the extension API table, so `sqlite_vfs::register()` works correctly
//! inside a loadable extension.
//!
//! ## VFS selection
//!
//! If `TURBOLITE_BUCKET` is set, registers a **tiered S3 VFS** (requires the
//! `tiered` feature). Otherwise, registers a **local compressed VFS**.
//!
//! ### Environment variables (tiered mode)
//!
//! | Variable | Required | Default | Description |
//! |---|---|---|---|
//! | `TURBOLITE_BUCKET` | yes | — | S3 bucket name |
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

static VFS_REGISTERED: AtomicBool = AtomicBool::new(false);

/// Called from C entry point (`sqlite3_turbolite_init` in ext_entry.c).
/// Returns 0 on success, 1 on error. Idempotent: second call is a no-op.
#[no_mangle]
pub extern "C" fn turbolite_ext_register_vfs() -> std::os::raw::c_int {
    if VFS_REGISTERED.swap(true, Ordering::SeqCst) {
        return 0;
    }

    let result = if std::env::var("TURBOLITE_BUCKET").is_ok() {
        register_tiered()
    } else {
        register_local()
    };

    match result {
        Ok(()) => 0,
        Err(_) => {
            VFS_REGISTERED.store(false, Ordering::SeqCst);
            1
        }
    }
}

fn register_local() -> Result<(), std::io::Error> {
    use std::path::PathBuf;
    let level = std::env::var("TURBOLITE_COMPRESSION_LEVEL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);
    let vfs = crate::CompressedVfs::new(PathBuf::from("."), level);
    crate::register("turbolite", vfs)
}

#[cfg(feature = "tiered")]
fn register_tiered() -> Result<(), std::io::Error> {
    use std::path::PathBuf;
    use crate::tiered::{TieredConfig, TieredVfs};

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
        .unwrap_or(0); // 0 = use default (num_cpus + 1)
    let compression_level = std::env::var("TURBOLITE_COMPRESSION_LEVEL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);
    let read_only = std::env::var("TURBOLITE_READ_ONLY")
        .map(|s| s == "1" || s == "true")
        .unwrap_or(false);

    let mut config = TieredConfig {
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

    let vfs = TieredVfs::new(config)?;
    crate::tiered::register("turbolite", vfs)
}

#[cfg(not(feature = "tiered"))]
fn register_tiered() -> Result<(), std::io::Error> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "TURBOLITE_BUCKET is set but this extension was built without the 'tiered' feature",
    ))
}
