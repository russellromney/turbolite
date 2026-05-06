//! Tests for the C FFI interface.
//!
//! These tests call the `extern "C"` functions directly to verify they work
//! correctly before exposing them through the shared library.

use std::ffi::CString;

use turbolite_ffi::ffi::*;

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

// --- Database operations ---

#[test]
fn test_open_and_close() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new("ffi-open-close").unwrap();
    let cache_dir = CString::new(dir.path().to_str().unwrap()).unwrap();
    turbolite_register_local(vfs_name.as_ptr(), cache_dir.as_ptr(), 3);

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
    let cache_dir = CString::new(dir.path().to_str().unwrap()).unwrap();
    turbolite_register_local(vfs_name.as_ptr(), cache_dir.as_ptr(), 3);

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
    let cache_dir = CString::new(dir.path().to_str().unwrap()).unwrap();
    turbolite_register_local(vfs_name.as_ptr(), cache_dir.as_ptr(), 3);

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
    let cache_dir = CString::new(dir.path().to_str().unwrap()).unwrap();
    turbolite_register_local(vfs_name.as_ptr(), cache_dir.as_ptr(), 3);

    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());

    let sql = CString::new(
        "CREATE TABLE t (id INTEGER, name TEXT); \
         INSERT INTO t VALUES (1, 'alice'); \
         INSERT INTO t VALUES (2, 'bob');",
    )
    .unwrap();
    turbolite_exec(db, sql.as_ptr());

    let query = CString::new("SELECT * FROM t ORDER BY id").unwrap();
    let json_ptr = turbolite_query_json(db, query.as_ptr());
    assert!(!json_ptr.is_null(), "query_json should return non-null");

    let json_str = unsafe { std::ffi::CStr::from_ptr(json_ptr) }
        .to_str()
        .unwrap();
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
    let cache_dir = CString::new(dir.path().to_str().unwrap()).unwrap();
    turbolite_register_local(vfs_name.as_ptr(), cache_dir.as_ptr(), 3);

    let db_path = CString::new(dir.path().join("persist.db").to_str().unwrap()).unwrap();

    // Write
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());
    let sql = CString::new("CREATE TABLE kv (k TEXT, v TEXT); INSERT INTO kv VALUES ('a', 'b');")
        .unwrap();
    turbolite_exec(db, sql.as_ptr());
    turbolite_close(db);

    // Re-open and read
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());
    let query = CString::new("SELECT * FROM kv").unwrap();
    let json_ptr = turbolite_query_json(db, query.as_ptr());
    assert!(!json_ptr.is_null());

    let json_str = unsafe { std::ffi::CStr::from_ptr(json_ptr) }
        .to_str()
        .unwrap();
    let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();
    let rows = parsed.as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["k"], "a");
    assert_eq!(rows[0]["v"], "b");

    turbolite_free_string(json_ptr);
    turbolite_close(db);
}

// --- turbolite_register_local_file_first ---

#[test]
fn test_register_local_file_first_layout() {
    // File-first: db_path is the local page image; sidecar metadata lives at
    // `<db_path>-turbolite/` and the consolidated state file is
    // local_state.msgpack. Old split tracking files should not appear.
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("app.db");
    let vfs_name = CString::new("ffi-file-first").unwrap();
    let db_path_c = CString::new(db_path.to_str().unwrap()).unwrap();

    let rc = turbolite_register_local_file_first(vfs_name.as_ptr(), db_path_c.as_ptr(), 3);
    assert_eq!(rc, 0, "register_local_file_first should succeed");

    let db = turbolite_open(db_path_c.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null(), "open should return non-null handle");

    let sql = CString::new(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT); \
         INSERT INTO t VALUES (1, 'one'); \
         INSERT INTO t VALUES (2, 'two');",
    )
    .unwrap();
    assert_eq!(turbolite_exec(db, sql.as_ptr()), 0);
    turbolite_close(db);

    assert!(db_path.is_file(), "file-first: app.db must exist");
    let sidecar = dir.path().join("app.db-turbolite");
    assert!(sidecar.is_dir(), "file-first: sidecar dir must exist");

    let local_state = sidecar.join("local_state.msgpack");
    assert!(
        local_state.is_file(),
        "file-first: local_state.msgpack must exist"
    );

    // In pure-local mode the local backend still owns its own
    // `manifest.msgpack` under the sidecar (via the storage client); only the
    // DiskCache split tracking files are gone. The file-first contract is
    // about consolidating local cache tracking into local_state.msgpack.
    for legacy in [
        "page_bitmap",
        "sub_chunk_tracker",
        "cache_index.json",
        "dirty_groups.msgpack",
    ] {
        assert!(
            !sidecar.join(legacy).is_file(),
            "file-first: legacy split file {legacy} should not be produced"
        );
    }

    // Reopen and read.
    let db = turbolite_open(db_path_c.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());
    let query = CString::new("SELECT * FROM t ORDER BY id").unwrap();
    let json_ptr = turbolite_query_json(db, query.as_ptr());
    assert!(!json_ptr.is_null());
    let json_str = unsafe { std::ffi::CStr::from_ptr(json_ptr) }
        .to_str()
        .unwrap();
    let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();
    let rows = parsed.as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["val"], "one");
    assert_eq!(rows[1]["val"], "two");
    turbolite_free_string(json_ptr);
    turbolite_close(db);
}

#[test]
fn test_state_dir_for_database_path_helper() {
    let path = CString::new("/tmp/app.db").unwrap();
    let ptr = turbolite_state_dir_for_database_path(path.as_ptr());
    assert!(!ptr.is_null());
    let s = unsafe { std::ffi::CStr::from_ptr(ptr) }
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(s, "/tmp/app.db-turbolite");
    turbolite_free_string(ptr);
}

#[test]
fn test_state_dir_for_database_path_null_returns_null() {
    let ptr = turbolite_state_dir_for_database_path(std::ptr::null());
    assert!(ptr.is_null());
}

#[test]
fn test_register_json_local_data_path_derives_sidecar() {
    // When the JSON config sets `local_data_path` but not `cache_dir`,
    // the sidecar must end up at `<local_data_path>-turbolite/`, not at
    // the default `/tmp/turbolite-cache`.
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("derived.db");
    let name = CString::new("ffi-json-file-first-derive").unwrap();
    let config = format!(
        r#"{{ "local_data_path": "{}" }}"#,
        db_path.to_str().unwrap()
    );
    let config_json = CString::new(config).unwrap();
    let rc = turbolite_register(name.as_ptr(), config_json.as_ptr());
    assert_eq!(rc, 0);

    let db_path_c = CString::new(db_path.to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path_c.as_ptr(), name.as_ptr());
    assert!(!db.is_null());
    let sql = CString::new("CREATE TABLE t (id INTEGER); INSERT INTO t VALUES (1);").unwrap();
    assert_eq!(turbolite_exec(db, sql.as_ptr()), 0);
    turbolite_close(db);

    let sidecar = dir.path().join("derived.db-turbolite");
    assert!(
        sidecar.is_dir(),
        "JSON local_data_path-only must derive sidecar next to db",
    );
    assert!(sidecar.join("local_state.msgpack").is_file());
}

#[test]
fn test_register_json_local_data_path_with_explicit_cache_dir() {
    // When BOTH fields are set, `cache_dir` wins (no auto-derivation).
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("explicit.db");
    let custom_sidecar = dir.path().join("custom-sidecar");
    std::fs::create_dir_all(&custom_sidecar).unwrap();
    let name = CString::new("ffi-json-file-first-explicit").unwrap();
    let config = format!(
        r#"{{ "local_data_path": "{}", "cache_dir": "{}" }}"#,
        db_path.to_str().unwrap(),
        custom_sidecar.to_str().unwrap()
    );
    let config_json = CString::new(config).unwrap();
    assert_eq!(turbolite_register(name.as_ptr(), config_json.as_ptr()), 0);

    let db_path_c = CString::new(db_path.to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path_c.as_ptr(), name.as_ptr());
    assert!(!db.is_null());
    let sql = CString::new("CREATE TABLE t (id INTEGER); INSERT INTO t VALUES (1);").unwrap();
    assert_eq!(turbolite_exec(db, sql.as_ptr()), 0);
    turbolite_close(db);

    assert!(custom_sidecar.join("local_state.msgpack").is_file());
    // No auto-derived sidecar at the default location.
    assert!(!dir.path().join("explicit.db-turbolite").exists());
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
    assert!(
        msg.contains("name"),
        "error should mention 'name', got: {}",
        msg
    );
}

#[test]
fn test_register_local_null_cache_dir_fails() {
    let name = CString::new("ffi-local-null-dir").unwrap();
    let rc = turbolite_register_local(name.as_ptr(), std::ptr::null(), 3);
    assert_eq!(rc, -1);

    let err = turbolite_last_error();
    assert!(!err.is_null());
    let msg = unsafe { std::ffi::CStr::from_ptr(err) }.to_str().unwrap();
    assert!(
        msg.contains("cache_dir"),
        "error should mention 'cache_dir', got: {}",
        msg
    );
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
         INSERT INTO items VALUES (2, 'beta');",
    )
    .unwrap();
    assert_eq!(turbolite_exec(db, sql.as_ptr()), 0);

    let query = CString::new("SELECT * FROM items ORDER BY id").unwrap();
    let json_ptr = turbolite_query_json(db, query.as_ptr());
    assert!(!json_ptr.is_null());

    let json_str = unsafe { std::ffi::CStr::from_ptr(json_ptr) }
        .to_str()
        .unwrap();
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
        let sql =
            CString::new("CREATE TABLE kv (k TEXT, v TEXT); INSERT INTO kv VALUES ('x', 'y');")
                .unwrap();
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

        let json_str = unsafe { std::ffi::CStr::from_ptr(json_ptr) }
            .to_str()
            .unwrap();
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

    let json_str = unsafe { std::ffi::CStr::from_ptr(json_ptr) }
        .to_str()
        .unwrap();
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
    assert!(
        msg.contains("JSON") || msg.contains("json"),
        "error should mention JSON, got: {}",
        msg
    );
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
    assert!(
        msg.contains("config_json"),
        "error should mention 'config_json', got: {}",
        msg
    );
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
    let json_str = unsafe { std::ffi::CStr::from_ptr(json_ptr) }
        .to_str()
        .unwrap();
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

#[cfg(feature = "s3")]
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
    assert!(
        msg.contains("bucket"),
        "error should mention 'bucket', got: {}",
        msg
    );
}

// --- Query JSON: NULL, REAL, BLOB, empty ---

#[test]
fn test_query_json_null_values() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new("ffi-null-vals").unwrap();
    let cache_dir = CString::new(dir.path().to_str().unwrap()).unwrap();
    turbolite_register_local(vfs_name.as_ptr(), cache_dir.as_ptr(), 3);

    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());

    let sql = CString::new(
        "CREATE TABLE t (id INTEGER, val TEXT); \
         INSERT INTO t VALUES (1, NULL); \
         INSERT INTO t VALUES (NULL, 'present');",
    )
    .unwrap();
    assert_eq!(turbolite_exec(db, sql.as_ptr()), 0);

    let query = CString::new("SELECT * FROM t ORDER BY rowid").unwrap();
    let json_ptr = turbolite_query_json(db, query.as_ptr());
    assert!(!json_ptr.is_null());

    let json_str = unsafe { std::ffi::CStr::from_ptr(json_ptr) }
        .to_str()
        .unwrap();
    let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();
    let rows = parsed.as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"], 1);
    assert!(rows[0]["val"].is_null(), "val should be JSON null");
    assert!(rows[1]["id"].is_null(), "id should be JSON null");
    assert_eq!(rows[1]["val"], "present");

    turbolite_free_string(json_ptr);
    turbolite_close(db);
}

#[test]
fn test_query_json_real_values() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new("ffi-real-vals").unwrap();
    let cache_dir = CString::new(dir.path().to_str().unwrap()).unwrap();
    turbolite_register_local(vfs_name.as_ptr(), cache_dir.as_ptr(), 3);

    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());

    let sql = CString::new(
        "CREATE TABLE t (id INTEGER, price REAL); \
         INSERT INTO t VALUES (1, 3.14); \
         INSERT INTO t VALUES (2, -0.001); \
         INSERT INTO t VALUES (3, 99999.99);",
    )
    .unwrap();
    assert_eq!(turbolite_exec(db, sql.as_ptr()), 0);

    let query = CString::new("SELECT * FROM t ORDER BY id").unwrap();
    let json_ptr = turbolite_query_json(db, query.as_ptr());
    assert!(!json_ptr.is_null());

    let json_str = unsafe { std::ffi::CStr::from_ptr(json_ptr) }
        .to_str()
        .unwrap();
    let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();
    let rows = parsed.as_array().unwrap();
    assert_eq!(rows.len(), 3);
    assert!((rows[0]["price"].as_f64().unwrap() - 3.14).abs() < 1e-9);
    assert!((rows[1]["price"].as_f64().unwrap() - (-0.001)).abs() < 1e-9);
    assert!((rows[2]["price"].as_f64().unwrap() - 99999.99).abs() < 1e-9);

    turbolite_free_string(json_ptr);
    turbolite_close(db);
}

#[test]
fn test_query_json_blob_values() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new("ffi-blob-vals").unwrap();
    let cache_dir = CString::new(dir.path().to_str().unwrap()).unwrap();
    turbolite_register_local(vfs_name.as_ptr(), cache_dir.as_ptr(), 3);

    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());

    let sql = CString::new(
        "CREATE TABLE t (id INTEGER, data BLOB); \
         INSERT INTO t VALUES (1, X'DEADBEEF');",
    )
    .unwrap();
    assert_eq!(turbolite_exec(db, sql.as_ptr()), 0);

    let query = CString::new("SELECT * FROM t").unwrap();
    let json_ptr = turbolite_query_json(db, query.as_ptr());
    assert!(!json_ptr.is_null());

    let json_str = unsafe { std::ffi::CStr::from_ptr(json_ptr) }
        .to_str()
        .unwrap();
    let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();
    let rows = parsed.as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["data"], "blob:4 bytes");

    turbolite_free_string(json_ptr);
    turbolite_close(db);
}

#[test]
fn test_query_json_empty_result() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new("ffi-empty-result").unwrap();
    let cache_dir = CString::new(dir.path().to_str().unwrap()).unwrap();
    turbolite_register_local(vfs_name.as_ptr(), cache_dir.as_ptr(), 3);

    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());

    let sql = CString::new("CREATE TABLE t (id INTEGER)").unwrap();
    assert_eq!(turbolite_exec(db, sql.as_ptr()), 0);

    let query = CString::new("SELECT * FROM t WHERE id = 999").unwrap();
    let json_ptr = turbolite_query_json(db, query.as_ptr());
    assert!(
        !json_ptr.is_null(),
        "empty result should return non-null pointer"
    );

    let json_str = unsafe { std::ffi::CStr::from_ptr(json_ptr) }
        .to_str()
        .unwrap();
    assert_eq!(json_str, "[]");

    turbolite_free_string(json_ptr);
    turbolite_close(db);
}

// --- Open / exec / query edge cases ---

#[test]
fn test_open_invalid_vfs_fails() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let vfs_name = CString::new("ffi-never-registered-vfs").unwrap();

    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(
        db.is_null(),
        "opening with unregistered VFS should return null"
    );

    let err = turbolite_last_error();
    assert!(!err.is_null());
}

#[test]
fn test_exec_null_sql_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new("ffi-exec-null-sql").unwrap();
    let cache_dir = CString::new(dir.path().to_str().unwrap()).unwrap();
    turbolite_register_local(vfs_name.as_ptr(), cache_dir.as_ptr(), 3);

    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());

    let rc = turbolite_exec(db, std::ptr::null());
    assert_eq!(rc, -1);

    let err = turbolite_last_error();
    assert!(!err.is_null());

    turbolite_close(db);
}

#[test]
fn test_query_json_null_sql_returns_null() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new("ffi-query-null-sql").unwrap();
    let cache_dir = CString::new(dir.path().to_str().unwrap()).unwrap();
    turbolite_register_local(vfs_name.as_ptr(), cache_dir.as_ptr(), 3);

    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());

    let ptr = turbolite_query_json(db, std::ptr::null());
    assert!(ptr.is_null(), "query_json with null SQL should return null");

    let err = turbolite_last_error();
    assert!(!err.is_null());

    turbolite_close(db);
}

// --- WAL mode ---

#[test]
fn test_wal_mode_through_ffi() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new("ffi-wal-mode").unwrap();
    let cache_dir = CString::new(dir.path().to_str().unwrap()).unwrap();
    turbolite_register_local(vfs_name.as_ptr(), cache_dir.as_ptr(), 3);

    let db_path = CString::new(dir.path().join("wal.db").to_str().unwrap()).unwrap();

    // Open, enable WAL, insert data, checkpoint, close
    {
        let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
        assert!(!db.is_null());

        let wal_sql = CString::new("PRAGMA journal_mode=WAL").unwrap();
        // exec won't return the pragma result, but it should not error
        // Use query_json to verify WAL mode was set
        let json_ptr = turbolite_query_json(db, wal_sql.as_ptr());
        assert!(!json_ptr.is_null());
        let json_str = unsafe { std::ffi::CStr::from_ptr(json_ptr) }
            .to_str()
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();
        let mode = parsed[0]["journal_mode"].as_str().unwrap();
        assert_eq!(mode, "wal");
        turbolite_free_string(json_ptr);

        let sql = CString::new(
            "CREATE TABLE t (id INTEGER, val TEXT); \
             INSERT INTO t VALUES (1, 'wal-test');",
        )
        .unwrap();
        assert_eq!(turbolite_exec(db, sql.as_ptr()), 0);

        let checkpoint = CString::new("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        assert_eq!(turbolite_exec(db, checkpoint.as_ptr()), 0);

        turbolite_close(db);
    }

    // Reopen and verify data persisted
    {
        let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
        assert!(!db.is_null());

        let query = CString::new("SELECT * FROM t").unwrap();
        let json_ptr = turbolite_query_json(db, query.as_ptr());
        assert!(!json_ptr.is_null());

        let json_str = unsafe { std::ffi::CStr::from_ptr(json_ptr) }
            .to_str()
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();
        let rows = parsed.as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"], 1);
        assert_eq!(rows[0]["val"], "wal-test");

        turbolite_free_string(json_ptr);
        turbolite_close(db);
    }
}

// --- Multiple databases on one VFS ---

#[test]
fn test_register_local_then_open_multiple_dbs() {
    let dir = tempfile::tempdir().unwrap();

    // Each database needs its own VFS + cache directory to avoid manifest conflicts
    let vfs_a = CString::new("ffi-multi-db-a").unwrap();
    let cache_a = dir.path().join("cache_a");
    std::fs::create_dir_all(&cache_a).unwrap();
    let cache_a_str = CString::new(cache_a.to_str().unwrap()).unwrap();
    turbolite_register_local(vfs_a.as_ptr(), cache_a_str.as_ptr(), 3);

    let vfs_b = CString::new("ffi-multi-db-b").unwrap();
    let cache_b = dir.path().join("cache_b");
    std::fs::create_dir_all(&cache_b).unwrap();
    let cache_b_str = CString::new(cache_b.to_str().unwrap()).unwrap();
    turbolite_register_local(vfs_b.as_ptr(), cache_b_str.as_ptr(), 3);

    let db_path_a = CString::new(cache_a.join("a.db").to_str().unwrap()).unwrap();
    let db_path_b = CString::new(cache_b.join("b.db").to_str().unwrap()).unwrap();

    let db_a = turbolite_open(db_path_a.as_ptr(), vfs_a.as_ptr());
    let db_b = turbolite_open(db_path_b.as_ptr(), vfs_b.as_ptr());
    assert!(!db_a.is_null());
    assert!(!db_b.is_null());

    // Insert different data in each
    let sql_a =
        CString::new("CREATE TABLE t (val TEXT); INSERT INTO t VALUES ('from-a');").unwrap();
    let sql_b =
        CString::new("CREATE TABLE t (val TEXT); INSERT INTO t VALUES ('from-b');").unwrap();
    assert_eq!(turbolite_exec(db_a, sql_a.as_ptr()), 0);
    assert_eq!(turbolite_exec(db_b, sql_b.as_ptr()), 0);

    // Verify independence
    let query = CString::new("SELECT val FROM t").unwrap();

    let json_a = turbolite_query_json(db_a, query.as_ptr());
    assert!(!json_a.is_null());
    let str_a = unsafe { std::ffi::CStr::from_ptr(json_a) }
        .to_str()
        .unwrap();
    let parsed_a: serde_json::Value = serde_json::from_str(str_a).unwrap();
    assert_eq!(parsed_a[0]["val"], "from-a");

    let json_b = turbolite_query_json(db_b, query.as_ptr());
    assert!(!json_b.is_null());
    let str_b = unsafe { std::ffi::CStr::from_ptr(json_b) }
        .to_str()
        .unwrap();
    let parsed_b: serde_json::Value = serde_json::from_str(str_b).unwrap();
    assert_eq!(parsed_b[0]["val"], "from-b");

    turbolite_free_string(json_a);
    turbolite_free_string(json_b);
    turbolite_close(db_a);
    turbolite_close(db_b);
}

// --- Large query result ---

#[test]
fn test_large_query_result() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new("ffi-large-result").unwrap();
    let cache_dir = CString::new(dir.path().to_str().unwrap()).unwrap();
    turbolite_register_local(vfs_name.as_ptr(), cache_dir.as_ptr(), 3);

    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());

    let create = CString::new("CREATE TABLE t (id INTEGER, val TEXT)").unwrap();
    assert_eq!(turbolite_exec(db, create.as_ptr()), 0);

    // Insert 1000 rows using a recursive CTE for speed
    let insert = CString::new(
        "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 1000) \
         INSERT INTO t SELECT x, 'row-' || x FROM cnt;",
    )
    .unwrap();
    assert_eq!(turbolite_exec(db, insert.as_ptr()), 0);

    let query = CString::new("SELECT * FROM t ORDER BY id").unwrap();
    let json_ptr = turbolite_query_json(db, query.as_ptr());
    assert!(!json_ptr.is_null());

    let json_str = unsafe { std::ffi::CStr::from_ptr(json_ptr) }
        .to_str()
        .unwrap();
    let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();
    let rows = parsed.as_array().unwrap();
    assert_eq!(rows.len(), 1000);
    assert_eq!(rows[0]["id"], 1);
    assert_eq!(rows[0]["val"], "row-1");
    assert_eq!(rows[999]["id"], 1000);
    assert_eq!(rows[999]["val"], "row-1000");

    turbolite_free_string(json_ptr);
    turbolite_close(db);
}

// --- JSON config with all local fields ---

#[test]
fn test_register_json_with_all_local_fields() {
    let dir = tempfile::tempdir().unwrap();
    let name = CString::new("ffi-json-all-local").unwrap();
    let config = format!(
        r#"{{
            "cache_dir": "{}",
            "compression_level": 5,
            "read_only": false,
            "cache_compression": true,
            "cache_compression_level": 7
        }}"#,
        dir.path().to_str().unwrap()
    );
    let config_json = CString::new(config).unwrap();

    let rc = turbolite_register(name.as_ptr(), config_json.as_ptr());
    assert_eq!(rc, 0, "register with all local fields should succeed");

    // Verify it actually works by opening a db and doing a roundtrip
    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), name.as_ptr());
    assert!(!db.is_null());

    let sql = CString::new("CREATE TABLE t (id INTEGER); INSERT INTO t VALUES (77);").unwrap();
    assert_eq!(turbolite_exec(db, sql.as_ptr()), 0);

    let query = CString::new("SELECT id FROM t").unwrap();
    let json_ptr = turbolite_query_json(db, query.as_ptr());
    assert!(!json_ptr.is_null());

    let json_str = unsafe { std::ffi::CStr::from_ptr(json_ptr) }
        .to_str()
        .unwrap();
    let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();
    assert_eq!(parsed[0]["id"], 77);

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
    assert!(
        msg.contains("path"),
        "error should mention 'path', got: {}",
        msg
    );
}
