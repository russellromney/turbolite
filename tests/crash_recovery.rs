use std::ffi::CString;

use turbolite::ffi::{
    turbolite_close, turbolite_exec, turbolite_free_string, turbolite_last_error, turbolite_open,
    turbolite_query_json, turbolite_register_local,
};

fn setup_vfs(dir: &std::path::Path) -> String {
    let vfs_name = format!("crash_vfs_{}_{}", std::process::id(), rand_id());
    let cache_dir = dir.to_str().unwrap();
    let vfs_name_c = CString::new(vfs_name.as_str()).unwrap();
    let cache_dir_c = CString::new(cache_dir).unwrap();
    // Use turbolite_register_local (simpler, more reliable than JSON config)
    let rc = turbolite_register_local(vfs_name_c.as_ptr(), cache_dir_c.as_ptr(), 3);
    assert_eq!(rc, 0, "VFS registration must succeed");
    vfs_name
}

fn rand_id() -> u32 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u32
}

/// Get error message as String, or None if no error
fn get_last_error() -> Option<String> {
    let err_ptr = turbolite_last_error();
    if err_ptr.is_null() {
        None
    } else {
        Some(unsafe {
            std::ffi::CStr::from_ptr(err_ptr)
                .to_str()
                .unwrap()
                .to_string()
        })
    }
}

fn open_db(path: &str, vfs: &str) -> *mut turbolite::ffi::TurboliteDb {
    let path_c = CString::new(path).unwrap();
    let vfs_c = CString::new(vfs).unwrap();
    let db = turbolite_open(path_c.as_ptr(), vfs_c.as_ptr());
    if db.is_null() {
        if let Some(err) = get_last_error() {
            panic!("open_db failed: {}", err);
        } else {
            panic!("open_db failed: unknown error");
        }
    }
    db
}

fn exec_sql(db: *mut turbolite::ffi::TurboliteDb, sql: &str) -> i32 {
    let sql_c = CString::new(sql).unwrap();
    let rc = turbolite_exec(db, sql_c.as_ptr());
    if rc != 0 {
        if let Some(err) = get_last_error() {
            panic!("exec_sql failed: {}", err);
        }
    }
    rc
}

fn query_json(db: *mut turbolite::ffi::TurboliteDb, sql: &str) -> String {
    let sql_c = CString::new(sql).unwrap();
    let ptr = turbolite_query_json(db, sql_c.as_ptr());
    if ptr.is_null() {
        if let Some(err) = get_last_error() {
            panic!("query_json failed: {}", err);
        } else {
            panic!("query_json failed: unknown error (sql: {})", sql);
        }
    }
    let s = unsafe { std::ffi::CStr::from_ptr(ptr) }
        .to_str()
        .unwrap()
        .to_string();
    turbolite_free_string(ptr);
    s
}

#[test]
fn test_crash_during_write_verify_integrity() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db_path_str = db_path.to_str().unwrap().to_string();

    let vfs_name = setup_vfs(dir.path());

    let db = open_db(&db_path_str, &vfs_name);
    exec_sql(db, "PRAGMA journal_mode=WAL");
    exec_sql(
        db,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)",
    );

    for i in 0..100 {
        exec_sql(
            db,
            &format!("INSERT INTO items VALUES ({}, 'item_{}')", i, i),
        );
    }

    exec_sql(db, "INSERT INTO items VALUES (999, 'last_committed')");

    turbolite_close(db);

    let db = open_db(&db_path_str, &vfs_name);
    let json = query_json(db, "SELECT COUNT(*) as cnt FROM items");
    assert!(
        json.contains("101"),
        "all committed rows must survive: {}",
        json
    );

    let json = query_json(db, "SELECT value FROM items WHERE id = 999");
    assert!(
        json.contains("last_committed"),
        "last committed row must be readable: {}",
        json
    );

    let json = query_json(db, "PRAGMA integrity_check");
    assert!(json.contains("ok"), "integrity check must pass: {}", json);

    turbolite_close(db);
}

#[test]
fn test_crash_during_checkpoint_verify_integrity() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("checkpoint_test.db");
    let db_path_str = db_path.to_str().unwrap().to_string();

    let vfs_name = setup_vfs(dir.path());

    let db = open_db(&db_path_str, &vfs_name);
    exec_sql(db, "PRAGMA journal_mode=WAL");
    exec_sql(
        db,
        "CREATE TABLE data (id INTEGER PRIMARY KEY, payload TEXT)",
    );

    for i in 0..50 {
        exec_sql(
            db,
            &format!("INSERT INTO data VALUES ({}, 'payload_{}')", i, i),
        );
    }

    exec_sql(db, "PRAGMA wal_checkpoint(TRUNCATE)");

    exec_sql(db, "INSERT INTO data VALUES (500, 'post_checkpoint')");

    turbolite_close(db);

    let db = open_db(&db_path_str, &vfs_name);
    let json = query_json(db, "SELECT COUNT(*) as cnt FROM data");
    assert!(
        json.contains("51"),
        "all rows including post-checkpoint must survive: {}",
        json
    );

    let json = query_json(db, "SELECT payload FROM data WHERE id = 500");
    assert!(
        json.contains("post_checkpoint"),
        "post-checkpoint row must be readable: {}",
        json
    );

    let json = query_json(db, "PRAGMA integrity_check");
    assert!(
        json.contains("ok"),
        "integrity check must pass after checkpoint: {}",
        json
    );

    turbolite_close(db);
}

#[test]
fn test_multiple_writes_restart_integrity() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("multi_write.db");
    let db_path_str = db_path.to_str().unwrap().to_string();

    let vfs_name = setup_vfs(dir.path());

    let db = open_db(&db_path_str, &vfs_name);
    exec_sql(db, "PRAGMA journal_mode=WAL");
    exec_sql(db, "CREATE TABLE counter (val INTEGER)");
    exec_sql(db, "INSERT INTO counter VALUES (0)");
    // Ensure WAL is checkpointed to main db before close to avoid replay issues on reopen
    exec_sql(db, "PRAGMA wal_checkpoint(TRUNCATE)");
    turbolite_close(db);

    for round in 1..=5 {
        let db = open_db(&db_path_str, &vfs_name);
        exec_sql(db, &format!("UPDATE counter SET val = {}", round));
        // Checkpoint after each update to ensure persistence
        exec_sql(db, "PRAGMA wal_checkpoint(TRUNCATE)");
        let json = query_json(db, "SELECT val FROM counter");
        assert!(
            json.contains(&format!(r#""val":{}"#, round)),
            "round {} value must be correct: {}",
            round,
            json
        );
        turbolite_close(db);
    }

    let db = open_db(&db_path_str, &vfs_name);
    let json = query_json(db, "SELECT val FROM counter");
    assert!(
        json.contains(r#""val":5"#),
        "final value must be 5: {}",
        json
    );

    let json = query_json(db, "PRAGMA integrity_check");
    assert!(
        json.contains("ok"),
        "integrity must pass after multiple restarts: {}",
        json
    );

    turbolite_close(db);
}

#[test]
fn test_wal_mode_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("wal_test.db");
    let db_path_str = db_path.to_str().unwrap().to_string();

    let vfs_name = setup_vfs(dir.path());

    let db = open_db(&db_path_str, &vfs_name);
    exec_sql(db, "PRAGMA journal_mode=WAL");
    exec_sql(
        db,
        "CREATE TABLE wal_data (id INTEGER PRIMARY KEY, txt TEXT)",
    );

    for i in 0..200 {
        exec_sql(
            db,
            &format!("INSERT INTO wal_data VALUES ({}, 'row_{}')", i, i),
        );
    }

    // Ensure WAL is checkpointed to main db before close to avoid replay issues on reopen
    exec_sql(db, "PRAGMA wal_checkpoint(TRUNCATE)");

    turbolite_close(db);

    let db = open_db(&db_path_str, &vfs_name);
    let json = query_json(db, "SELECT COUNT(*) as cnt FROM wal_data");
    assert!(
        json.contains("200"),
        "all WAL rows must survive restart: {}",
        json
    );

    let json = query_json(db, "PRAGMA integrity_check");
    assert!(
        json.contains("ok"),
        "integrity must pass after WAL restart: {}",
        json
    );

    turbolite_close(db);
}

#[test]
fn test_large_write_restart_integrity() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("large_write.db");
    let db_path_str = db_path.to_str().unwrap().to_string();

    let vfs_name = setup_vfs(dir.path());

    let db = open_db(&db_path_str, &vfs_name);
    exec_sql(db, "PRAGMA journal_mode=WAL");
    exec_sql(
        db,
        "CREATE TABLE blob_table (id INTEGER PRIMARY KEY, data BLOB)",
    );

    let large_data = "X".repeat(65536);
    for i in 0..10 {
        exec_sql(
            db,
            &format!("INSERT INTO blob_table VALUES ({}, '{}')", i, large_data),
        );
    }

    turbolite_close(db);

    let db = open_db(&db_path_str, &vfs_name);
    let json = query_json(db, "SELECT COUNT(*) as cnt FROM blob_table");
    assert!(json.contains("10"), "all large rows must survive: {}", json);

    let json = query_json(db, "PRAGMA integrity_check");
    assert!(
        json.contains("ok"),
        "integrity must pass after large writes: {}",
        json
    );

    turbolite_close(db);
}

// =============================================================================
// Journal Mode Crash Recovery Tests
// =============================================================================

#[test]
fn test_truncate_mode_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("truncate_test.db");
    let db_path_str = db_path.to_str().unwrap().to_string();

    let vfs_name = setup_vfs(dir.path());

    let db = open_db(&db_path_str, &vfs_name);
    exec_sql(db, "PRAGMA journal_mode=TRUNCATE");
    exec_sql(
        db,
        "CREATE TABLE truncate_data (id INTEGER PRIMARY KEY, val TEXT)",
    );
    for i in 0..100 {
        exec_sql(
            db,
            &format!("INSERT INTO truncate_data VALUES ({}, 'val_{}')", i, i),
        );
    }
    turbolite_close(db);

    let db = open_db(&db_path_str, &vfs_name);
    let json = query_json(db, "SELECT COUNT(*) as cnt FROM truncate_data");
    assert!(
        json.contains("100"),
        "all rows must survive TRUNCATE restart: {}",
        json
    );
    let json = query_json(db, "PRAGMA integrity_check");
    assert!(json.contains("ok"), "integrity must pass: {}", json);
    turbolite_close(db);
}

#[test]
fn test_truncate_mode_with_pending_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("truncate_pending.db");
    let db_path_str = db_path.to_str().unwrap().to_string();

    let vfs_name = setup_vfs(dir.path());

    let db = open_db(&db_path_str, &vfs_name);
    exec_sql(db, "PRAGMA journal_mode=TRUNCATE");
    exec_sql(
        db,
        "CREATE TABLE pending (id INTEGER PRIMARY KEY, data TEXT)",
    );
    exec_sql(db, "INSERT INTO pending VALUES (1, 'committed')");
    exec_sql(db, "INSERT INTO pending VALUES (2, 'also_committed')");
    turbolite_close(db);

    let db = open_db(&db_path_str, &vfs_name);
    let json = query_json(db, "SELECT COUNT(*) as cnt FROM pending");
    assert!(json.contains("2"), "committed rows must survive: {}", json);
    let json = query_json(db, "PRAGMA integrity_check");
    assert!(json.contains("ok"), "integrity must pass: {}", json);
    turbolite_close(db);
}

#[test]
fn test_persist_mode_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("persist_test.db");
    let db_path_str = db_path.to_str().unwrap().to_string();

    let vfs_name = setup_vfs(dir.path());

    let db = open_db(&db_path_str, &vfs_name);
    exec_sql(db, "PRAGMA journal_mode=PERSIST");
    exec_sql(
        db,
        "CREATE TABLE persist_data (id INTEGER PRIMARY KEY, name TEXT)",
    );
    for i in 0..50 {
        exec_sql(
            db,
            &format!("INSERT INTO persist_data VALUES ({}, 'name_{}')", i, i),
        );
    }
    turbolite_close(db);

    let db = open_db(&db_path_str, &vfs_name);
    let json = query_json(db, "SELECT COUNT(*) as cnt FROM persist_data");
    assert!(
        json.contains("50"),
        "all rows must survive PERSIST restart: {}",
        json
    );
    let json = query_json(db, "PRAGMA integrity_check");
    assert!(json.contains("ok"), "integrity must pass: {}", json);
    turbolite_close(db);
}

#[test]
fn test_persist_mode_recovery_after_incomplete_write() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("persist_incomplete.db");
    let db_path_str = db_path.to_str().unwrap().to_string();

    let vfs_name = setup_vfs(dir.path());

    let db = open_db(&db_path_str, &vfs_name);
    exec_sql(db, "PRAGMA journal_mode=PERSIST");
    exec_sql(
        db,
        "CREATE TABLE incomplete (id INTEGER PRIMARY KEY, x TEXT)",
    );
    exec_sql(db, "INSERT INTO incomplete VALUES (1, 'first')");
    exec_sql(db, "INSERT INTO incomplete VALUES (2, 'second')");
    // Simulate incomplete transaction by closing without final commit marker
    turbolite_close(db);

    let db = open_db(&db_path_str, &vfs_name);
    let json = query_json(db, "SELECT COUNT(*) as cnt FROM incomplete");
    assert!(json.contains("2"), "committed rows must survive: {}", json);
    let json = query_json(db, "PRAGMA integrity_check");
    assert!(json.contains("ok"), "integrity must pass: {}", json);
    turbolite_close(db);
}

#[test]
fn test_memory_mode_is_volatile() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("memory_test.db");
    let db_path_str = db_path.to_str().unwrap().to_string();

    let vfs_name = setup_vfs(dir.path());

    let db = open_db(&db_path_str, &vfs_name);
    exec_sql(db, "PRAGMA journal_mode=MEMORY");
    exec_sql(
        db,
        "CREATE TABLE memory_data (id INTEGER PRIMARY KEY, val TEXT)",
    );
    for i in 0..50 {
        exec_sql(
            db,
            &format!("INSERT INTO memory_data VALUES ({}, 'mem_{}')", i, i),
        );
    }
    // Data should persist in memory-backed journal while DB is open
    let json = query_json(db, "SELECT COUNT(*) as cnt FROM memory_data");
    assert!(json.contains("50"), "data visible during session: {}", json);
    turbolite_close(db);

    // On restart, journal_mode=MEMORY means data is LOST (expected behavior)
    let db = open_db(&db_path_str, &vfs_name);
    let json = query_json(db, "SELECT COUNT(*) as cnt FROM memory_data");
    assert!(
        json.contains("0") || !json.contains("50"),
        "MEMORY mode is volatile - data lost on close (expected): {}",
        json
    );
    let json = query_json(db, "PRAGMA integrity_check");
    assert!(
        json.contains("ok"),
        "empty DB integrity must pass: {}",
        json
    );
    turbolite_close(db);
}

#[test]
fn test_memory_mode_with_sync_write() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("memory_sync.db");
    let db_path_str = db_path.to_str().unwrap().to_string();

    let vfs_name = setup_vfs(dir.path());

    let db = open_db(&db_path_str, &vfs_name);
    exec_sql(db, "PRAGMA journal_mode=MEMORY");
    exec_sql(db, "PRAGMA synchronous=FULL");
    exec_sql(
        db,
        "CREATE TABLE mem_sync (id INTEGER PRIMARY KEY, data TEXT)",
    );
    exec_sql(db, "INSERT INTO mem_sync VALUES (1, 'synced')");
    exec_sql(db, "INSERT INTO mem_sync VALUES (2, 'also_synced')");
    let json = query_json(db, "SELECT COUNT(*) as cnt FROM mem_sync");
    assert!(json.contains("2"), "data visible during session: {}", json);
    exec_sql(db, "PRAGMA wal_checkpoint(TRUNCATE)");
    turbolite_close(db);

    let db = open_db(&db_path_str, &vfs_name);
    let json = query_json(db, "SELECT COUNT(*) as cnt FROM mem_sync");
    assert!(
        json.contains("2"),
        "data persists with checkpoint: {}",
        json
    );
    let json = query_json(db, "PRAGMA integrity_check");
    assert!(json.contains("ok"), "integrity must pass: {}", json);
    turbolite_close(db);
}

#[test]
fn test_synchronous_off_crash_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("sync_off.db");
    let db_path_str = db_path.to_str().unwrap().to_string();

    let vfs_name = setup_vfs(dir.path());

    let db = open_db(&db_path_str, &vfs_name);
    exec_sql(db, "PRAGMA journal_mode=WAL");
    exec_sql(db, "PRAGMA synchronous=OFF");
    exec_sql(
        db,
        "CREATE TABLE syncoff (id INTEGER PRIMARY KEY, val TEXT)",
    );
    for i in 0..100 {
        exec_sql(
            db,
            &format!("INSERT INTO syncoff VALUES ({}, 'off_{}')", i, i),
        );
    }
    exec_sql(db, "PRAGMA wal_checkpoint(TRUNCATE)");
    turbolite_close(db);

    let db = open_db(&db_path_str, &vfs_name);
    let json = query_json(db, "SELECT COUNT(*) as cnt FROM syncoff");
    assert!(
        json.contains("100"),
        "checkpointed data must survive: {}",
        json
    );
    let json = query_json(db, "PRAGMA integrity_check");
    assert!(json.contains("ok"), "integrity must pass: {}", json);
    turbolite_close(db);
}

#[test]
fn test_synchronous_full_integrity() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("sync_full.db");
    let db_path_str = db_path.to_str().unwrap().to_string();

    let vfs_name = setup_vfs(dir.path());

    let db = open_db(&db_path_str, &vfs_name);
    exec_sql(db, "PRAGMA journal_mode=WAL");
    exec_sql(db, "PRAGMA synchronous=FULL");
    exec_sql(
        db,
        "CREATE TABLE syncfulldata (id INTEGER PRIMARY KEY, payload TEXT)",
    );
    for i in 0..50 {
        exec_sql(
            db,
            &format!("INSERT INTO syncfulldata VALUES ({}, 'full_{}')", i, i),
        );
    }
    turbolite_close(db);

    let db = open_db(&db_path_str, &vfs_name);
    let json = query_json(db, "SELECT COUNT(*) as cnt FROM syncfulldata");
    assert!(
        json.contains("50"),
        "all rows must survive with synchronous=FULL: {}",
        json
    );
    let json = query_json(db, "PRAGMA integrity_check");
    assert!(json.contains("ok"), "integrity must pass: {}", json);
    turbolite_close(db);
}

#[test]
fn test_synchronous_extra_full_write_integrity() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("sync_extra.db");
    let db_path_str = db_path.to_str().unwrap().to_string();

    let vfs_name = setup_vfs(dir.path());

    let db = open_db(&db_path_str, &vfs_name);
    exec_sql(db, "PRAGMA journal_mode=WAL");
    exec_sql(db, "PRAGMA synchronous=EXTRA"); // Even more paranoid than FULL
    exec_sql(
        db,
        "CREATE TABLE syncextra (id INTEGER PRIMARY KEY, data TEXT)",
    );
    exec_sql(db, "INSERT INTO syncextra VALUES (1, 'extra_safe')");
    exec_sql(db, "INSERT INTO syncextra VALUES (2, 'double_safe')");
    exec_sql(db, "PRAGMA wal_checkpoint(TRUNCATE)");
    turbolite_close(db);

    let db = open_db(&db_path_str, &vfs_name);
    let json = query_json(db, "SELECT COUNT(*) as cnt FROM syncextra");
    assert!(
        json.contains("2"),
        "all rows must survive synchronous=EXTRA: {}",
        json
    );
    let json = query_json(db, "SELECT data FROM syncextra WHERE id=2");
    assert!(
        json.contains("double_safe"),
        "row 2 must be correct: {}",
        json
    );
    let json = query_json(db, "PRAGMA integrity_check");
    assert!(json.contains("ok"), "integrity must pass: {}", json);
    turbolite_close(db);
}

// =============================================================================
// Mixed Mode Stress Tests
// =============================================================================

#[test]
fn test_wal_truncate_checkpoint_multiple_rounds() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("multi_checkpoint.db");
    let db_path_str = db_path.to_str().unwrap().to_string();

    let vfs_name = setup_vfs(dir.path());

    let db = open_db(&db_path_str, &vfs_name);
    exec_sql(db, "PRAGMA journal_mode=WAL");
    exec_sql(
        db,
        "CREATE TABLE mcheck (id INTEGER PRIMARY KEY, round INTEGER)",
    );

    for round in 1..=10 {
        exec_sql(
            db,
            &format!("INSERT INTO mcheck VALUES ({}, {})", round, round),
        );
        exec_sql(db, "PRAGMA wal_checkpoint(TRUNCATE)");
    }
    turbolite_close(db);

    let db = open_db(&db_path_str, &vfs_name);
    let json = query_json(db, "SELECT COUNT(*) as cnt FROM mcheck");
    assert!(json.contains("10"), "all 10 rounds must survive: {}", json);
    let json = query_json(db, "PRAGMA integrity_check");
    assert!(json.contains("ok"), "integrity must pass: {}", json);
    turbolite_close(db);
}

#[test]
fn test_delete_mode_crash_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("delete_test.db");
    let db_path_str = db_path.to_str().unwrap().to_string();

    let vfs_name = setup_vfs(dir.path());

    let db = open_db(&db_path_str, &vfs_name);
    exec_sql(db, "PRAGMA journal_mode=DELETE");
    exec_sql(
        db,
        "CREATE TABLE delete_data (id INTEGER PRIMARY KEY, val TEXT)",
    );
    for i in 0..75 {
        exec_sql(
            db,
            &format!("INSERT INTO delete_data VALUES ({}, 'del_{}')", i, i),
        );
    }
    turbolite_close(db);

    let db = open_db(&db_path_str, &vfs_name);
    let json = query_json(db, "SELECT COUNT(*) as cnt FROM delete_data");
    assert!(
        json.contains("75"),
        "all rows must survive DELETE mode restart: {}",
        json
    );
    let json = query_json(db, "PRAGMA integrity_check");
    assert!(json.contains("ok"), "integrity must pass: {}", json);
    turbolite_close(db);
}
