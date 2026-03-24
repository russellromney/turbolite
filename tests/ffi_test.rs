//! Tests for the C FFI interface.
//!
//! These tests call the `extern "C"` functions directly to verify they work
//! correctly before exposing them through the shared library.

use std::ffi::CString;

use turbolite::ffi::*;

// --- Version ---

#[test]
fn test_version_returns_valid_string() {
    let ptr = turbolite_version();
    assert!(!ptr.is_null());
    let version = unsafe { std::ffi::CStr::from_ptr(ptr) }.to_str().unwrap();
    assert_eq!(version, env!("CARGO_PKG_VERSION"));
}

// --- Error handling ---

#[test]
fn test_last_error_initially_null() {
    // Clear any prior errors by calling a successful function.
    turbolite_clear_caches();
    let err = turbolite_last_error();
    assert!(err.is_null());
}

#[test]
fn test_null_name_sets_error() {
    let base_dir = CString::new("/tmp/ffi-test").unwrap();
    let rc = turbolite_register_compressed(std::ptr::null(), base_dir.as_ptr(), 3);
    assert_eq!(rc, -1);

    let err = turbolite_last_error();
    assert!(!err.is_null());
    let msg = unsafe { std::ffi::CStr::from_ptr(err) }.to_str().unwrap();
    assert!(msg.contains("name"), "error should mention 'name', got: {}", msg);
}

#[test]
fn test_null_base_dir_sets_error() {
    let name = CString::new("null-base-dir-test").unwrap();
    let rc = turbolite_register_compressed(name.as_ptr(), std::ptr::null(), 3);
    assert_eq!(rc, -1);

    let err = turbolite_last_error();
    assert!(!err.is_null());
    let msg = unsafe { std::ffi::CStr::from_ptr(err) }.to_str().unwrap();
    assert!(
        msg.contains("base_dir"),
        "error should mention 'base_dir', got: {}",
        msg
    );
}

#[test]
fn test_null_passthrough_name_sets_error() {
    let base_dir = CString::new("/tmp/ffi-test").unwrap();
    let rc = turbolite_register_passthrough(std::ptr::null(), base_dir.as_ptr());
    assert_eq!(rc, -1);

    let err = turbolite_last_error();
    assert!(!err.is_null());
}

// --- Registration ---

#[test]
fn test_register_compressed_success() {
    let dir = tempfile::tempdir().unwrap();
    let name = CString::new("ffi-compressed-test").unwrap();
    let base_dir = CString::new(dir.path().to_str().unwrap()).unwrap();

    let rc = turbolite_register_compressed(name.as_ptr(), base_dir.as_ptr(), 3);
    assert_eq!(rc, 0, "register_compressed should succeed");

    // Error should be cleared on success.
    let err = turbolite_last_error();
    assert!(err.is_null());
}

#[test]
fn test_register_passthrough_success() {
    let dir = tempfile::tempdir().unwrap();
    let name = CString::new("ffi-passthrough-test").unwrap();
    let base_dir = CString::new(dir.path().to_str().unwrap()).unwrap();

    let rc = turbolite_register_passthrough(name.as_ptr(), base_dir.as_ptr());
    assert_eq!(rc, 0, "register_passthrough should succeed");
}

#[test]
fn test_register_duplicate_name_succeeds() {
    // SQLite allows re-registering a VFS with the same name (replaces the old one).
    let dir = tempfile::tempdir().unwrap();
    let name = CString::new("ffi-dup-test").unwrap();
    let base_dir = CString::new(dir.path().to_str().unwrap()).unwrap();

    let rc1 = turbolite_register_compressed(name.as_ptr(), base_dir.as_ptr(), 3);
    assert_eq!(rc1, 0);

    let rc2 = turbolite_register_compressed(name.as_ptr(), base_dir.as_ptr(), 3);
    assert_eq!(rc2, 0, "re-registering same name should succeed");
}

// --- Database operations ---

#[test]
fn test_open_and_close() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new("ffi-open-close").unwrap();
    let base_dir = CString::new(dir.path().to_str().unwrap()).unwrap();
    turbolite_register_compressed(vfs_name.as_ptr(), base_dir.as_ptr(), 3);

    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null(), "open should return non-null handle");
    turbolite_close(db);
}

#[test]
fn test_open_null_path_returns_null() {
    let db = turbolite_open(std::ptr::null(), CString::new("any").unwrap().as_ptr());
    assert!(db.is_null());
    let err = turbolite_last_error();
    assert!(!err.is_null());
}

#[test]
fn test_close_null_is_safe() {
    turbolite_close(std::ptr::null_mut());
}

#[test]
fn test_exec_create_and_insert() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new("ffi-exec-test").unwrap();
    let base_dir = CString::new(dir.path().to_str().unwrap()).unwrap();
    turbolite_register_compressed(vfs_name.as_ptr(), base_dir.as_ptr(), 3);

    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());

    let sql = CString::new("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    assert_eq!(turbolite_exec(db, sql.as_ptr()), 0);

    let sql = CString::new("INSERT INTO t VALUES (1, 'hello')").unwrap();
    assert_eq!(turbolite_exec(db, sql.as_ptr()), 0);

    turbolite_close(db);
}

#[test]
fn test_exec_bad_sql_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new("ffi-badsql").unwrap();
    let base_dir = CString::new(dir.path().to_str().unwrap()).unwrap();
    turbolite_register_compressed(vfs_name.as_ptr(), base_dir.as_ptr(), 3);

    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());

    let sql = CString::new("NOT VALID SQL").unwrap();
    assert_eq!(turbolite_exec(db, sql.as_ptr()), -1);

    let err = turbolite_last_error();
    assert!(!err.is_null());

    turbolite_close(db);
}

#[test]
fn test_exec_null_db_returns_error() {
    let sql = CString::new("SELECT 1").unwrap();
    assert_eq!(turbolite_exec(std::ptr::null_mut(), sql.as_ptr()), -1);
}

#[test]
fn test_query_json_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new("ffi-query-json").unwrap();
    let base_dir = CString::new(dir.path().to_str().unwrap()).unwrap();
    turbolite_register_compressed(vfs_name.as_ptr(), base_dir.as_ptr(), 3);

    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());

    let sql = CString::new(
        "CREATE TABLE t (id INTEGER, name TEXT); \
         INSERT INTO t VALUES (1, 'alice'); \
         INSERT INTO t VALUES (2, 'bob');"
    ).unwrap();
    turbolite_exec(db, sql.as_ptr());

    let query = CString::new("SELECT * FROM t ORDER BY id").unwrap();
    let json_ptr = turbolite_query_json(db, query.as_ptr());
    assert!(!json_ptr.is_null(), "query_json should return non-null");

    let json_str = unsafe { std::ffi::CStr::from_ptr(json_ptr) }.to_str().unwrap();
    let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();
    let rows = parsed.as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"], 1);
    assert_eq!(rows[0]["name"], "alice");
    assert_eq!(rows[1]["id"], 2);
    assert_eq!(rows[1]["name"], "bob");

    turbolite_free_string(json_ptr);
    turbolite_close(db);
}

#[test]
fn test_query_json_null_db_returns_null() {
    let sql = CString::new("SELECT 1").unwrap();
    let ptr = turbolite_query_json(std::ptr::null_mut(), sql.as_ptr());
    assert!(ptr.is_null());
}

#[test]
fn test_free_string_null_is_safe() {
    turbolite_free_string(std::ptr::null_mut());
}

#[test]
fn test_persistence_across_connections() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new("ffi-persist").unwrap();
    let base_dir = CString::new(dir.path().to_str().unwrap()).unwrap();
    turbolite_register_compressed(vfs_name.as_ptr(), base_dir.as_ptr(), 3);

    let db_path = CString::new(dir.path().join("persist.db").to_str().unwrap()).unwrap();

    // Write
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());
    let sql = CString::new(
        "CREATE TABLE kv (k TEXT, v TEXT); INSERT INTO kv VALUES ('a', 'b');"
    ).unwrap();
    turbolite_exec(db, sql.as_ptr());
    turbolite_close(db);

    // Re-open and read
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());
    let query = CString::new("SELECT * FROM kv").unwrap();
    let json_ptr = turbolite_query_json(db, query.as_ptr());
    assert!(!json_ptr.is_null());

    let json_str = unsafe { std::ffi::CStr::from_ptr(json_ptr) }.to_str().unwrap();
    let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();
    let rows = parsed.as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["k"], "a");
    assert_eq!(rows[0]["v"], "b");

    turbolite_free_string(json_ptr);
    turbolite_close(db);
}

// --- Cache utilities ---

#[test]
fn test_clear_caches_does_not_panic() {
    turbolite_clear_caches();
}

#[test]
fn test_invalidate_cache_with_valid_path() {
    let dir = tempfile::tempdir().unwrap();
    let path = CString::new(dir.path().to_str().unwrap()).unwrap();
    let rc = turbolite_invalidate_cache(path.as_ptr());
    assert_eq!(rc, 0);
}

#[test]
fn test_invalidate_cache_null_path_fails() {
    let rc = turbolite_invalidate_cache(std::ptr::null());
    assert_eq!(rc, -1);

    let err = turbolite_last_error();
    assert!(!err.is_null());
    let msg = unsafe { std::ffi::CStr::from_ptr(err) }.to_str().unwrap();
    assert!(msg.contains("path"), "error should mention 'path', got: {}", msg);
}
