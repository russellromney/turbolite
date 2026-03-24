//! SQLite loadable extension support.
//!
//! Exports `turbolite_ext_register_vfs()` which is called from the C entry
//! point in `ext_entry.c` after `SQLITE_EXTENSION_INIT2` stores the API table.
//!
//! The C shim provides symbol shims for `sqlite3_vfs_register` etc. that route
//! through the extension API table, so `sqlite_vfs::register()` works correctly
//! inside a loadable extension.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::CompressedVfs;

/// Track whether the VFS has already been registered (idempotent load).
static VFS_REGISTERED: AtomicBool = AtomicBool::new(false);

/// Called from C entry point (`sqlite3_turbolite_init` in ext_entry.c).
///
/// Registers a compressed VFS named "turbolite" with zstd level 3.
/// The base directory is "." — SQLite passes full paths to the VFS, so
/// relative base dir means paths are used as-given by the host application.
///
/// Returns 0 on success, 1 on error. Idempotent: second call is a no-op.
#[no_mangle]
pub extern "C" fn turbolite_ext_register_vfs() -> std::os::raw::c_int {
    if VFS_REGISTERED.swap(true, Ordering::SeqCst) {
        // Already registered — idempotent success.
        return 0;
    }

    let vfs = CompressedVfs::new(PathBuf::from("."), 3);
    match crate::register("turbolite", vfs) {
        Ok(()) => 0,
        Err(_) => {
            VFS_REGISTERED.store(false, Ordering::SeqCst);
            1
        }
    }
}
