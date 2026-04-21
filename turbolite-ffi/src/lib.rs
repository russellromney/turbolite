//! turbolite-ffi — C FFI + SQLite loadable-extension front-end for turbolite.
//!
//! All `#[no_mangle] extern "C"` symbols with the `turbolite_` prefix live
//! here. turbolite (the library crate) is pure Rust; it only exposes the
//! native `TurboliteVfs` / `TurboliteHandle` API. Consumers pick their
//! door:
//!
//! - Rust users: depend on `turbolite` directly.
//! - C / Python / Go / Node / loadable extension users: load the cdylib
//!   produced by this crate.
//!
//! # Build modes
//!
//! Two mutually exclusive cdylib flavors, selected by features:
//!
//! 1. **Standalone** (default, `bundled-sqlite`): self-contained .dylib
//!    / .so. Bundles sqlite3 via turbolite's `bundled-sqlite` feature
//!    and exposes `turbolite_open` / `turbolite_exec` / `turbolite_query_json`
//!    rusqlite wrappers. Used by Python ctypes, Go cgo, Node koffi, and
//!    the C FFI integration tests.
//!
//! 2. **Loadable extension** (`loadable-extension`, no `bundled-sqlite`):
//!    compiled with the ext_entry.c shim. dlopened by an existing sqlite3
//!    process via `sqlite3_load_extension`. Only the `sqlite3_turbolite_init`
//!    symbol is exported.

#[cfg(not(feature = "loadable-extension"))]
pub mod ffi;

#[cfg(feature = "loadable-extension")]
pub mod ext;

// FFI wrappers always compiled — the C shim references them under
// `loadable-extension`, and keeping them in the standalone cdylib costs
// nothing.
mod trace;
pub use trace::{turbolite_trace_end_query, turbolite_trace_push_plan};

pub mod settings;
pub use settings::{
    turbolite_config_set, turbolite_current_queue_clone, turbolite_settings_queue_free,
    turbolite_settings_queue_free_cb, turbolite_settings_queue_push,
};

pub mod install;
pub use install::turbolite_install_config_functions;
