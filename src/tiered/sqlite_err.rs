//! Error conversion between io::Error and SQLite error codes.
//!
//! All internal turbolite code uses io::Error. Conversion happens only at
//! the sqlite-plugin Vfs trait boundary.

use std::io;

/// Convert io::Error to SQLite error code for the Vfs trait boundary.
pub(crate) fn io_to_sqlite(e: io::Error) -> i32 {
    match e.kind() {
        io::ErrorKind::NotFound => sqlite_plugin::vars::SQLITE_CANTOPEN,
        io::ErrorKind::PermissionDenied => sqlite_plugin::vars::SQLITE_PERM,
        io::ErrorKind::WouldBlock => sqlite_plugin::vars::SQLITE_BUSY,
        io::ErrorKind::AlreadyExists => sqlite_plugin::vars::SQLITE_CANTOPEN,
        io::ErrorKind::InvalidInput => sqlite_plugin::vars::SQLITE_MISUSE,
        _ => sqlite_plugin::vars::SQLITE_IOERR,
    }
}

/// Convert SQLite error code to io::Error.
#[allow(dead_code)]
pub(crate) fn sqlite_to_io(code: i32) -> io::Error {
    let msg = match code {
        sqlite_plugin::vars::SQLITE_CANTOPEN => "cannot open",
        sqlite_plugin::vars::SQLITE_PERM => "permission denied",
        sqlite_plugin::vars::SQLITE_BUSY => "database busy",
        sqlite_plugin::vars::SQLITE_IOERR => "I/O error",
        sqlite_plugin::vars::SQLITE_MISUSE => "misuse",
        _ => "sqlite error",
    };
    io::Error::new(io::ErrorKind::Other, format!("{msg} (sqlite code {code})"))
}

/// Extension trait for converting Result<T, io::Error> to VfsResult<T>.
pub(crate) trait IntoVfsResult<T> {
    fn into_vfs(self) -> Result<T, i32>;
}

impl<T> IntoVfsResult<T> for Result<T, io::Error> {
    fn into_vfs(self) -> Result<T, i32> {
        self.map_err(io_to_sqlite)
    }
}
