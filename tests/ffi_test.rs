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

// --- turbolite_register_local ---

#[test]
fn test_register_local_success() {
    let dir = tempfile::tempdir().unwrap();
    let name = CString::new("ffi-local-test").unwrap();
    let cache_dir = CString::new(dir.path().to_str().unwrap()).unwrap();

    let rc = turbolite_register_local(name.as_ptr(), cache_dir.as_ptr(), 3);
    assert_eq!(rc, 0, "register_local should succeed");

    let err = turbolite_last_error();
    assert!(err.is_null());
}

#[test]
fn test_register_local_null_name_fails() {
    let cache_dir = CString::new("/tmp/ffi-local-null-name").unwrap();
    let rc = turbolite_register_local(std::ptr::null(), cache_dir.as_ptr(), 3);
    assert_eq!(rc, -1);

    let err = turbolite_last_error();
    assert!(!err.is_null());
    let msg = unsafe { std::ffi::CStr::from_ptr(err) }.to_str().unwrap();
    assert!(msg.contains("name"), "error should mention 'name', got: {}", msg);
}

#[test]
fn test_register_local_null_cache_dir_fails() {
    let name = CString::new("ffi-local-null-dir").unwrap();
    let rc = turbolite_register_local(name.as_ptr(), std::ptr::null(), 3);
    assert_eq!(rc, -1);

    let err = turbolite_last_error();
    assert!(!err.is_null());
    let msg = unsafe { std::ffi::CStr::from_ptr(err) }.to_str().unwrap();
    assert!(msg.contains("cache_dir"), "error should mention 'cache_dir', got: {}", msg);
}

#[test]
fn test_register_local_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new("ffi-local-roundtrip").unwrap();
    let cache_dir = CString::new(dir.path().to_str().unwrap()).unwrap();

    let rc = turbolite_register_local(vfs_name.as_ptr(), cache_dir.as_ptr(), 3);
    assert_eq!(rc, 0);

    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null(), "open should return non-null handle");

    // Create table, insert, query
    let sql = CString::new(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT); \
         INSERT INTO items VALUES (1, 'alpha'); \
         INSERT INTO items VALUES (2, 'beta');"
    ).unwrap();
    assert_eq!(turbolite_exec(db, sql.as_ptr()), 0);

    let query = CString::new("SELECT * FROM items ORDER BY id").unwrap();
    let json_ptr = turbolite_query_json(db, query.as_ptr());
    assert!(!json_ptr.is_null());

    let json_str = unsafe { std::ffi::CStr::from_ptr(json_ptr) }.to_str().unwrap();
    let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();
    let rows = parsed.as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"], 1);
    assert_eq!(rows[0]["name"], "alpha");
    assert_eq!(rows[1]["id"], 2);
    assert_eq!(rows[1]["name"], "beta");

    turbolite_free_string(json_ptr);
    turbolite_close(db);
}

#[test]
fn test_register_local_persistence_across_connections() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new("ffi-local-persist").unwrap();
    let cache_dir = CString::new(dir.path().to_str().unwrap()).unwrap();
    turbolite_register_local(vfs_name.as_ptr(), cache_dir.as_ptr(), 3);

    let db_path = CString::new(dir.path().join("persist.db").to_str().unwrap()).unwrap();

    // Write
    {
        let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
        assert!(!db.is_null());
        let sql = CString::new(
            "CREATE TABLE kv (k TEXT, v TEXT); INSERT INTO kv VALUES ('x', 'y');"
        ).unwrap();
        turbolite_exec(db, sql.as_ptr());
        turbolite_close(db);
    }

    // Re-open and read
    {
        let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
        assert!(!db.is_null());
        let query = CString::new("SELECT * FROM kv").unwrap();
        let json_ptr = turbolite_query_json(db, query.as_ptr());
        assert!(!json_ptr.is_null());

        let json_str = unsafe { std::ffi::CStr::from_ptr(json_ptr) }.to_str().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();
        let rows = parsed.as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["k"], "x");
        assert_eq!(rows[0]["v"], "y");

        turbolite_free_string(json_ptr);
        turbolite_close(db);
    }
}

#[test]
fn test_register_local_duplicate_name_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let name = CString::new("ffi-local-dup").unwrap();
    let cache_dir = CString::new(dir.path().to_str().unwrap()).unwrap();

    let rc1 = turbolite_register_local(name.as_ptr(), cache_dir.as_ptr(), 3);
    assert_eq!(rc1, 0);

    let rc2 = turbolite_register_local(name.as_ptr(), cache_dir.as_ptr(), 3);
    assert_eq!(rc2, 0, "re-registering same name should succeed");
}

// --- turbolite_register (JSON config) ---

#[test]
fn test_register_json_local_mode() {
    let dir = tempfile::tempdir().unwrap();
    let name = CString::new("ffi-json-local").unwrap();
    let config = format!(r#"{{ "cache_dir": "{}" }}"#, dir.path().to_str().unwrap());
    let config_json = CString::new(config).unwrap();

    let rc = turbolite_register(name.as_ptr(), config_json.as_ptr());
    assert_eq!(rc, 0, "register with JSON should succeed");

    // Verify it works by opening a database
    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), name.as_ptr());
    assert!(!db.is_null());

    let sql = CString::new("CREATE TABLE t (id INTEGER); INSERT INTO t VALUES (42);").unwrap();
    assert_eq!(turbolite_exec(db, sql.as_ptr()), 0);

    let query = CString::new("SELECT id FROM t").unwrap();
    let json_ptr = turbolite_query_json(db, query.as_ptr());
    assert!(!json_ptr.is_null());

    let json_str = unsafe { std::ffi::CStr::from_ptr(json_ptr) }.to_str().unwrap();
    let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();
    assert_eq!(parsed[0]["id"], 42);

    turbolite_free_string(json_ptr);
    turbolite_close(db);
}

#[test]
fn test_register_json_with_compression_level() {
    let dir = tempfile::tempdir().unwrap();
    let name = CString::new("ffi-json-level").unwrap();
    let config = format!(
        r#"{{ "cache_dir": "{}", "compression_level": 9 }}"#,
        dir.path().to_str().unwrap()
    );
    let config_json = CString::new(config).unwrap();

    let rc = turbolite_register(name.as_ptr(), config_json.as_ptr());
    assert_eq!(rc, 0);
}

#[test]
fn test_register_json_invalid_json_fails() {
    let name = CString::new("ffi-json-bad").unwrap();
    let config_json = CString::new("not valid json {{{").unwrap();

    let rc = turbolite_register(name.as_ptr(), config_json.as_ptr());
    assert_eq!(rc, -1);

    let err = turbolite_last_error();
    assert!(!err.is_null());
    let msg = unsafe { std::ffi::CStr::from_ptr(err) }.to_str().unwrap();
    assert!(msg.contains("JSON") || msg.contains("json"), "error should mention JSON, got: {}", msg);
}

#[test]
fn test_register_json_null_name_fails() {
    let config_json = CString::new("{}").unwrap();
    let rc = turbolite_register(std::ptr::null(), config_json.as_ptr());
    assert_eq!(rc, -1);
}

#[test]
fn test_register_json_null_config_fails() {
    let name = CString::new("ffi-json-null-config").unwrap();
    let rc = turbolite_register(name.as_ptr(), std::ptr::null());
    assert_eq!(rc, -1);

    let err = turbolite_last_error();
    assert!(!err.is_null());
    let msg = unsafe { std::ffi::CStr::from_ptr(err) }.to_str().unwrap();
    assert!(msg.contains("config_json"), "error should mention 'config_json', got: {}", msg);
}

#[test]
fn test_register_json_empty_object_uses_defaults() {
    let name = CString::new("ffi-json-defaults").unwrap();
    let config_json = CString::new("{}").unwrap();

    // Should succeed with all defaults (Local backend, default cache_dir)
    let rc = turbolite_register(name.as_ptr(), config_json.as_ptr());
    assert_eq!(rc, 0);
}

#[test]
fn test_register_json_explicit_local_backend() {
    let dir = tempfile::tempdir().unwrap();
    let name = CString::new("ffi-json-explicit-local").unwrap();
    let config = format!(
        r#"{{ "storage_backend": "Local", "cache_dir": "{}" }}"#,
        dir.path().to_str().unwrap()
    );
    let config_json = CString::new(config).unwrap();

    let rc = turbolite_register(name.as_ptr(), config_json.as_ptr());
    assert_eq!(rc, 0);

    // Verify it works
    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), name.as_ptr());
    assert!(!db.is_null());

    let sql = CString::new("CREATE TABLE t (v TEXT); INSERT INTO t VALUES ('ok');").unwrap();
    assert_eq!(turbolite_exec(db, sql.as_ptr()), 0);

    let query = CString::new("SELECT v FROM t").unwrap();
    let json_ptr = turbolite_query_json(db, query.as_ptr());
    assert!(!json_ptr.is_null());
    let json_str = unsafe { std::ffi::CStr::from_ptr(json_ptr) }.to_str().unwrap();
    let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();
    assert_eq!(parsed[0]["v"], "ok");

    turbolite_free_string(json_ptr);
    turbolite_close(db);
}

#[test]
fn test_register_json_unknown_fields_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let name = CString::new("ffi-json-unknown").unwrap();
    let config = format!(
        r#"{{ "cache_dir": "{}", "some_future_field": true, "another_thing": 42 }}"#,
        dir.path().to_str().unwrap()
    );
    let config_json = CString::new(config).unwrap();

    let rc = turbolite_register(name.as_ptr(), config_json.as_ptr());
    assert_eq!(rc, 0, "unknown fields should be silently ignored");
}

// --- turbolite_register_tiered alias ---

#[cfg(feature = "cloud")]
#[test]
fn test_register_tiered_null_bucket_fails() {
    let name = CString::new("ffi-tiered-alias").unwrap();
    let prefix = CString::new("prefix").unwrap();
    let cache_dir = CString::new("/tmp/ffi-tiered-alias").unwrap();

    let rc = turbolite_register_tiered(
        name.as_ptr(),
        std::ptr::null(),
        prefix.as_ptr(),
        cache_dir.as_ptr(),
        std::ptr::null(),
        std::ptr::null(),
    );
    assert_eq!(rc, -1);

    let err = turbolite_last_error();
    assert!(!err.is_null());
    let msg = unsafe { std::ffi::CStr::from_ptr(err) }.to_str().unwrap();
    assert!(msg.contains("bucket"), "error should mention 'bucket', got: {}", msg);
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
