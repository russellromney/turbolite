use std::ffi::CString;
use std::os::raw::c_char;
use std::panic::{self, AssertUnwindSafe};
use std::ptr;

use turbolite_ffi::ffi::{
    turbolite_clear_caches, turbolite_close, turbolite_exec, turbolite_free_string,
    turbolite_invalidate_cache, turbolite_last_error, turbolite_open, turbolite_query_json,
    turbolite_register, turbolite_register_local, turbolite_version,
};

fn ffi_no_panic<F, R>(label: &str, f: F) -> Result<R, String>
where
    F: FnOnce() -> R,
{
    match panic::catch_unwind(AssertUnwindSafe(f)) {
        Ok(val) => Ok(val),
        Err(e) => {
            let msg = if let Some(s) = e.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = e.downcast_ref::<String>() {
                s.clone()
            } else {
                format!("{label} panicked with unknown payload")
            };
            Err(msg)
        }
    }
}

fn garbage_cstring(len: usize) -> *const c_char {
    let mut buf = vec![0u8; len];
    for (i, b) in buf.iter_mut().enumerate().take(len - 1) {
        *b = (i % 254) as u8 + 1;
    }
    buf[len - 1] = 0;
    Box::into_raw(buf.into_boxed_slice()) as *const c_char
}

unsafe fn free_garbage(p: *const c_char) {
    if !p.is_null() {
        let mut len = 0usize;
        while std::ptr::read(p.add(len)) != 0 {
            len += 1;
        }
        drop(Box::from_raw(std::slice::from_raw_parts_mut(
            p as *mut u8,
            len + 1,
        )));
    }
}

static mut VFS_COUNTER: u64 = 0;

fn next_vfs_name() -> String {
    unsafe {
        VFS_COUNTER += 1;
        format!("test_vfs_{}", VFS_COUNTER)
    }
}

#[test]
fn test_last_error_no_error_returns_null() {
    turbolite_clear_caches();
    let p = turbolite_last_error();
    assert!(p.is_null());
}

#[test]
fn test_version_never_null() {
    let p = turbolite_version();
    assert!(!p.is_null());
    let s = unsafe { std::ffi::CStr::from_ptr(p) }.to_str().unwrap();
    assert!(!s.is_empty());
}

#[test]
fn test_register_local_null_name() {
    let dir = tempfile::tempdir().unwrap();
    let cache = CString::new(dir.path().to_str().unwrap()).unwrap();
    let result = ffi_no_panic("register_local(null_name)", || {
        turbolite_register_local(ptr::null(), cache.as_ptr(), 3)
    });
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), -1);
}

#[test]
fn test_register_local_null_cache_dir() {
    let name = CString::new(next_vfs_name()).unwrap();
    let result = ffi_no_panic("register_local(null_cache)", || {
        turbolite_register_local(name.as_ptr(), ptr::null(), 3)
    });
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), -1);
}

#[test]
fn test_register_local_both_null() {
    let result = ffi_no_panic("register_local(both_null)", || {
        turbolite_register_local(ptr::null(), ptr::null(), 3)
    });
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), -1);
}

#[test]
fn test_register_local_garbage_name() {
    let dir = tempfile::tempdir().unwrap();
    let cache = CString::new(dir.path().to_str().unwrap()).unwrap();
    let garbage = garbage_cstring(256);
    let result = ffi_no_panic("register_local(garbage_name)", || {
        let r = turbolite_register_local(garbage, cache.as_ptr(), 3);
        unsafe { free_garbage(garbage) };
        r
    });
    assert!(result.is_ok());
}

#[test]
fn test_register_local_oversized_name() {
    let dir = tempfile::tempdir().unwrap();
    let cache = CString::new(dir.path().to_str().unwrap()).unwrap();
    let huge = "A".repeat(1_000_000);
    let name = CString::new(huge).unwrap();
    let result = ffi_no_panic("register_local(oversized_name)", || {
        turbolite_register_local(name.as_ptr(), cache.as_ptr(), 3)
    });
    assert!(result.is_ok());
}

#[test]
fn test_register_local_valid_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let name = CString::new(next_vfs_name()).unwrap();
    let cache = CString::new(dir.path().to_str().unwrap()).unwrap();
    let result = ffi_no_panic("register_local(valid)", || {
        turbolite_register_local(name.as_ptr(), cache.as_ptr(), 3)
    });
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0);
}

#[test]
fn test_register_null_name() {
    let json = CString::new(r#"{"cache_dir":"/tmp"}"#).unwrap();
    let result = ffi_no_panic("register(null_name)", || {
        turbolite_register(ptr::null(), json.as_ptr())
    });
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), -1);
}

#[test]
fn test_register_null_json() {
    let name = CString::new(next_vfs_name()).unwrap();
    let result = ffi_no_panic("register(null_json)", || {
        turbolite_register(name.as_ptr(), ptr::null())
    });
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), -1);
}

#[test]
fn test_register_both_null() {
    let result = ffi_no_panic("register(both_null)", || {
        turbolite_register(ptr::null(), ptr::null())
    });
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), -1);
}

#[test]
fn test_register_invalid_json() {
    let name = CString::new(next_vfs_name()).unwrap();
    let json = CString::new("not json at all {{{").unwrap();
    let result = ffi_no_panic("register(bad_json)", || {
        turbolite_register(name.as_ptr(), json.as_ptr())
    });
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), -1);
}

#[test]
fn test_register_garbage_json() {
    let name = CString::new(next_vfs_name()).unwrap();
    let garbage = garbage_cstring(512);
    let result = ffi_no_panic("register(garbage_json)", || {
        let r = turbolite_register(name.as_ptr(), garbage);
        unsafe { free_garbage(garbage) };
        r
    });
    assert!(result.is_ok());
}

#[test]
fn test_register_valid_json_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let name = CString::new(next_vfs_name()).unwrap();
    let json = CString::new(format!(
        r#"{{"cache_dir":"{}"}}"#,
        dir.path().to_str().unwrap()
    ))
    .unwrap();
    let result = ffi_no_panic("register(valid_json)", || {
        turbolite_register(name.as_ptr(), json.as_ptr())
    });
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0);
}

#[test]
fn test_invalidate_cache_null() {
    let result = ffi_no_panic("invalidate_cache(null)", || {
        turbolite_invalidate_cache(ptr::null())
    });
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), -1);
}

#[test]
fn test_invalidate_cache_garbage() {
    let garbage = garbage_cstring(128);
    let result = ffi_no_panic("invalidate_cache(garbage)", || {
        let r = turbolite_invalidate_cache(garbage);
        unsafe { free_garbage(garbage) };
        r
    });
    assert!(result.is_ok());
}

#[test]
fn test_invalidate_cache_valid_path() {
    let path = CString::new("/tmp/nonexistent_db_file").unwrap();
    let result = ffi_no_panic("invalidate_cache(valid)", || {
        turbolite_invalidate_cache(path.as_ptr())
    });
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0);
}

#[test]
fn test_clear_caches_never_panics() {
    let result = ffi_no_panic("clear_caches", || {
        turbolite_clear_caches();
    });
    assert!(result.is_ok());
}

#[test]
fn test_open_null_path() {
    let vfs = CString::new("turbolite").unwrap();
    let result = ffi_no_panic("open(null_path)", || {
        turbolite_open(ptr::null(), vfs.as_ptr())
    });
    assert!(result.is_ok());
    assert!(result.unwrap().is_null());
}

#[test]
fn test_open_null_vfs() {
    let path = CString::new("/tmp/test.db").unwrap();
    let result = ffi_no_panic("open(null_vfs)", || {
        turbolite_open(path.as_ptr(), ptr::null())
    });
    assert!(result.is_ok());
    assert!(result.unwrap().is_null());
}

#[test]
fn test_open_both_null() {
    let result = ffi_no_panic("open(both_null)", || {
        turbolite_open(ptr::null(), ptr::null())
    });
    assert!(result.is_ok());
    assert!(result.unwrap().is_null());
}

#[test]
fn test_open_garbage_path() {
    let vfs = CString::new("turbolite").unwrap();
    let garbage = garbage_cstring(256);
    let result = ffi_no_panic("open(garbage_path)", || {
        let r = turbolite_open(garbage, vfs.as_ptr());
        unsafe { free_garbage(garbage) };
        r
    });
    assert!(result.is_ok());
}

#[test]
fn test_open_nonexistent_vfs_returns_null() {
    let dir = tempfile::tempdir().unwrap();
    let path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let vfs = CString::new("nonexistent_vfs_xyz").unwrap();
    let result = ffi_no_panic("open(bad_vfs)", || {
        turbolite_open(path.as_ptr(), vfs.as_ptr())
    });
    assert!(result.is_ok());
    assert!(result.unwrap().is_null());
}

#[test]
fn test_exec_null_db() {
    let sql = CString::new("SELECT 1").unwrap();
    let result = ffi_no_panic("exec(null_db)", || {
        turbolite_exec(ptr::null_mut(), sql.as_ptr())
    });
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), -1);
}

#[test]
fn test_exec_null_sql() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new(next_vfs_name()).unwrap();
    let cache_dir = dir.path().to_str().unwrap();
    let json = CString::new(format!(r#"{{"cache_dir":"{}"}}"#, cache_dir)).unwrap();
    assert_eq!(turbolite_register(vfs_name.as_ptr(), json.as_ptr()), 0);

    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());

    let result = ffi_no_panic("exec(null_sql)", || turbolite_exec(db, ptr::null()));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), -1);

    turbolite_close(db);
}

#[test]
fn test_exec_both_null() {
    let result = ffi_no_panic("exec(both_null)", || {
        turbolite_exec(ptr::null_mut(), ptr::null())
    });
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), -1);
}

#[test]
fn test_exec_garbage_sql() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new(next_vfs_name()).unwrap();
    let cache_dir = dir.path().to_str().unwrap();
    let json = CString::new(format!(r#"{{"cache_dir":"{}"}}"#, cache_dir)).unwrap();
    assert_eq!(turbolite_register(vfs_name.as_ptr(), json.as_ptr()), 0);

    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());

    let garbage = garbage_cstring(256);
    let result = ffi_no_panic("exec(garbage_sql)", || {
        let r = turbolite_exec(db, garbage);
        unsafe { free_garbage(garbage) };
        r
    });
    assert!(result.is_ok());

    turbolite_close(db);
}

#[test]
fn test_exec_invalid_sql_syntax() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new(next_vfs_name()).unwrap();
    let json = CString::new(format!(
        r#"{{"cache_dir":"{}"}}"#,
        dir.path().to_str().unwrap()
    ))
    .unwrap();
    assert_eq!(turbolite_register(vfs_name.as_ptr(), json.as_ptr()), 0);

    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());

    let sql = CString::new("THIS IS NOT SQL @@@###$$$").unwrap();
    let result = ffi_no_panic("exec(bad_syntax)", || turbolite_exec(db, sql.as_ptr()));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), -1);

    turbolite_close(db);
}

#[test]
fn test_exec_valid_sql() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new(next_vfs_name()).unwrap();
    let json = CString::new(format!(
        r#"{{"cache_dir":"{}"}}"#,
        dir.path().to_str().unwrap()
    ))
    .unwrap();
    assert_eq!(turbolite_register(vfs_name.as_ptr(), json.as_ptr()), 0);

    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());

    let sql = CString::new("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
    let result = ffi_no_panic("exec(valid)", || turbolite_exec(db, sql.as_ptr()));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0);

    turbolite_close(db);
}

#[test]
fn test_query_json_null_db() {
    let sql = CString::new("SELECT 1").unwrap();
    let result = ffi_no_panic("query_json(null_db)", || {
        turbolite_query_json(ptr::null_mut(), sql.as_ptr())
    });
    assert!(result.is_ok());
    assert!(result.unwrap().is_null());
}

#[test]
fn test_query_json_null_sql() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new(next_vfs_name()).unwrap();
    let json = CString::new(format!(
        r#"{{"cache_dir":"{}"}}"#,
        dir.path().to_str().unwrap()
    ))
    .unwrap();
    assert_eq!(turbolite_register(vfs_name.as_ptr(), json.as_ptr()), 0);

    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());

    let result = ffi_no_panic("query_json(null_sql)", || {
        turbolite_query_json(db, ptr::null())
    });
    assert!(result.is_ok());
    assert!(result.unwrap().is_null());

    turbolite_close(db);
}

#[test]
fn test_query_json_both_null() {
    let result = ffi_no_panic("query_json(both_null)", || {
        turbolite_query_json(ptr::null_mut(), ptr::null())
    });
    assert!(result.is_ok());
    assert!(result.unwrap().is_null());
}

#[test]
fn test_query_json_garbage_sql() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new(next_vfs_name()).unwrap();
    let json = CString::new(format!(
        r#"{{"cache_dir":"{}"}}"#,
        dir.path().to_str().unwrap()
    ))
    .unwrap();
    assert_eq!(turbolite_register(vfs_name.as_ptr(), json.as_ptr()), 0);

    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());

    let garbage = garbage_cstring(256);
    let result = ffi_no_panic("query_json(garbage_sql)", || {
        let r = turbolite_query_json(db, garbage);
        unsafe { free_garbage(garbage) };
        r
    });
    assert!(result.is_ok());

    turbolite_close(db);
}

#[test]
fn test_query_json_valid_select() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new(next_vfs_name()).unwrap();
    let json = CString::new(format!(
        r#"{{"cache_dir":"{}"}}"#,
        dir.path().to_str().unwrap()
    ))
    .unwrap();
    assert_eq!(turbolite_register(vfs_name.as_ptr(), json.as_ptr()), 0);

    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());

    let create = CString::new("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    assert_eq!(turbolite_exec(db, create.as_ptr()), 0);
    let insert = CString::new("INSERT INTO users VALUES (1, 'alice')").unwrap();
    assert_eq!(turbolite_exec(db, insert.as_ptr()), 0);

    let sql = CString::new("SELECT * FROM users").unwrap();
    let result = ffi_no_panic("query_json(valid)", || {
        turbolite_query_json(db, sql.as_ptr())
    });
    assert!(result.is_ok());
    let json_ptr = result.unwrap();
    assert!(!json_ptr.is_null());
    let json_str = unsafe { std::ffi::CStr::from_ptr(json_ptr) }
        .to_str()
        .unwrap();
    assert!(json_str.contains("alice"));

    turbolite_free_string(json_ptr);
    turbolite_close(db);
}

#[test]
fn test_free_string_null_is_safe() {
    let result = ffi_no_panic("free_string(null)", || {
        turbolite_free_string(ptr::null_mut());
    });
    assert!(result.is_ok());
}

#[test]
fn test_close_null_is_safe() {
    let result = ffi_no_panic("close(null)", || {
        turbolite_close(ptr::null_mut());
    });
    assert!(result.is_ok());
}

#[test]
fn test_exec_oversized_sql() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new(next_vfs_name()).unwrap();
    let json = CString::new(format!(
        r#"{{"cache_dir":"{}"}}"#,
        dir.path().to_str().unwrap()
    ))
    .unwrap();
    assert_eq!(turbolite_register(vfs_name.as_ptr(), json.as_ptr()), 0);

    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());

    let huge_sql = "SELECT 1; ".repeat(100_000);
    let sql = CString::new(huge_sql).unwrap();
    let result = ffi_no_panic("exec(oversized)", || turbolite_exec(db, sql.as_ptr()));
    assert!(result.is_ok());

    turbolite_close(db);
}

#[test]
fn test_register_oversized_json() {
    let name = CString::new(next_vfs_name()).unwrap();
    let huge_json = format!(
        r#"{{"cache_dir":"/tmp", "padding":"{}"}}"#,
        "X".repeat(100_000)
    );
    let json = CString::new(huge_json).unwrap();
    let result = ffi_no_panic("register(oversized_json)", || {
        turbolite_register(name.as_ptr(), json.as_ptr())
    });
    assert!(result.is_ok());
}

#[test]
fn test_register_local_non_utf8_name() {
    let dir = tempfile::tempdir().unwrap();
    let cache = CString::new(dir.path().to_str().unwrap()).unwrap();
    let non_utf8: Vec<u8> = vec![0xFF, 0xFE, 0x00];
    let result = ffi_no_panic("register_local(non_utf8_name)", || {
        turbolite_register_local(non_utf8.as_ptr() as *const c_char, cache.as_ptr(), 3)
    });
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), -1);
}

#[test]
fn test_register_non_utf8_json() {
    let name = CString::new(next_vfs_name()).unwrap();
    let non_utf8: Vec<u8> = vec![0x80, 0x81, 0x82, 0x00];
    let result = ffi_no_panic("register(non_utf8_json)", || {
        turbolite_register(name.as_ptr(), non_utf8.as_ptr() as *const c_char)
    });
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), -1);
}

#[test]
fn test_exec_null_byte_in_sql() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new(next_vfs_name()).unwrap();
    let json = CString::new(format!(
        r#"{{"cache_dir":"{}"}}"#,
        dir.path().to_str().unwrap()
    ))
    .unwrap();
    assert_eq!(turbolite_register(vfs_name.as_ptr(), json.as_ptr()), 0);

    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());

    let sql_with_null: Vec<u8> = b"SELECT 1\x00SELECT 2".to_vec();
    let result = ffi_no_panic("exec(null_byte)", || {
        turbolite_exec(db, sql_with_null.as_ptr() as *const c_char)
    });
    assert!(result.is_ok());

    turbolite_close(db);
}

#[test]
fn test_query_json_emoji_in_results() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new(next_vfs_name()).unwrap();
    let json = CString::new(format!(
        r#"{{"cache_dir":"{}"}}"#,
        dir.path().to_str().unwrap()
    ))
    .unwrap();
    assert_eq!(turbolite_register(vfs_name.as_ptr(), json.as_ptr()), 0);

    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());

    let create = CString::new("CREATE TABLE emoji (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    assert_eq!(turbolite_exec(db, create.as_ptr()), 0);
    let insert = CString::new("INSERT INTO emoji VALUES (1, '\u{1F680}')").unwrap();
    assert_eq!(turbolite_exec(db, insert.as_ptr()), 0);

    let sql = CString::new("SELECT * FROM emoji").unwrap();
    let result = ffi_no_panic("query_json(emoji)", || {
        turbolite_query_json(db, sql.as_ptr())
    });
    assert!(result.is_ok());
    let json_ptr = result.unwrap();
    assert!(!json_ptr.is_null());
    let json_str = unsafe { std::ffi::CStr::from_ptr(json_ptr) }
        .to_str()
        .unwrap();
    assert!(json_str.contains("\u{1F680}"));

    turbolite_free_string(json_ptr);
    turbolite_close(db);
}

#[test]
fn test_use_db_after_close() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new(next_vfs_name()).unwrap();
    let json = CString::new(format!(
        r#"{{"cache_dir":"{}"}}"#,
        dir.path().to_str().unwrap()
    ))
    .unwrap();
    assert_eq!(turbolite_register(vfs_name.as_ptr(), json.as_ptr()), 0);

    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());

    let sql = CString::new("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
    assert_eq!(turbolite_exec(db, sql.as_ptr()), 0);
    turbolite_close(db);

    let result = ffi_no_panic("exec(after_close)", || turbolite_exec(db, sql.as_ptr()));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), -1);
}

#[test]
fn test_double_close_same_db() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new(next_vfs_name()).unwrap();
    let json = CString::new(format!(
        r#"{{"cache_dir":"{}"}}"#,
        dir.path().to_str().unwrap()
    ))
    .unwrap();
    assert_eq!(turbolite_register(vfs_name.as_ptr(), json.as_ptr()), 0);

    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());

    let sql = CString::new("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
    assert_eq!(turbolite_exec(db, sql.as_ptr()), 0);

    let result1 = ffi_no_panic("close(first)", || {
        turbolite_close(db);
    });
    assert!(result1.is_ok());

    let result2 = ffi_no_panic("close(second)", || {
        turbolite_close(db);
    });
    assert!(result2.is_ok());
}

#[test]
fn test_query_after_close() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new(next_vfs_name()).unwrap();
    let json = CString::new(format!(
        r#"{{"cache_dir":"{}"}}"#,
        dir.path().to_str().unwrap()
    ))
    .unwrap();
    assert_eq!(turbolite_register(vfs_name.as_ptr(), json.as_ptr()), 0);

    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());

    let create = CString::new("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
    assert_eq!(turbolite_exec(db, create.as_ptr()), 0);
    let insert = CString::new("INSERT INTO t VALUES (1)").unwrap();
    assert_eq!(turbolite_exec(db, insert.as_ptr()), 0);
    turbolite_close(db);

    let sql = CString::new("SELECT * FROM t").unwrap();
    let result = ffi_no_panic("query_json(after_close)", || {
        turbolite_query_json(db, sql.as_ptr())
    });
    assert!(result.is_ok());
    assert!(result.unwrap().is_null());
}

#[test]
fn test_free_string_after_close() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_name = CString::new(next_vfs_name()).unwrap();
    let json = CString::new(format!(
        r#"{{"cache_dir":"{}"}}"#,
        dir.path().to_str().unwrap()
    ))
    .unwrap();
    assert_eq!(turbolite_register(vfs_name.as_ptr(), json.as_ptr()), 0);

    let db_path = CString::new(dir.path().join("test.db").to_str().unwrap()).unwrap();
    let db = turbolite_open(db_path.as_ptr(), vfs_name.as_ptr());
    assert!(!db.is_null());

    let sql = CString::new("SELECT 1").unwrap();
    let json_ptr = turbolite_query_json(db, sql.as_ptr());
    assert!(!json_ptr.is_null());
    turbolite_close(db);

    let result = ffi_no_panic("free_string(after_close)", || {
        turbolite_free_string(json_ptr);
    });
    assert!(result.is_ok());
}
