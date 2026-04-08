#![no_main]

use libfuzzer_sys::fuzz_target;
use std::ffi::CString;

fuzz_target!(|data: &[u8]| {
    if data.len() < 10 {
        return;
    }

    let (vfs_name, db_path, sql) = split_data(data);

    if vfs_name.is_empty() || db_path.is_empty() {
        return;
    }

    let cache_dir = format!("/tmp/turbolite_fuzz_{}", rand_id());
    let vfs_name_c = CString::new(vfs_name.as_str()).unwrap();
    let cache_dir_c = CString::new(cache_dir.as_str()).unwrap();

    let rc = turbolite::ffi::turbolite_register_local(vfs_name_c.as_ptr(), cache_dir_c.as_ptr(), 3);
    if rc != 0 {
        std::hint::black_box(rc);
        return;
    }

    let db_path_c = CString::new(db_path.as_str()).unwrap();
    let vfs_name_c = CString::new(vfs_name.as_str()).unwrap();
    let db = turbolite::ffi::turbolite_open(db_path_c.as_ptr(), vfs_name_c.as_ptr());

    if !db.is_null() {
        if !sql.is_empty() {
            let sql_c = CString::new(sql.as_str()).unwrap();
            let _ = turbolite::ffi::turbolite_exec(db, sql_c.as_ptr());
        }

        if !sql.is_empty() && sql.to_lowercase().starts_with("select") {
            let sql_c = CString::new(sql.as_str()).unwrap();
            let ptr = turbolite::ffi::turbolite_query_json(db, sql_c.as_ptr());
            if !ptr.is_null() {
                turbolite::ffi::turbolite_free_string(ptr);
            }
        }

        turbolite::ffi::turbolite_close(db);
    }

    std::fs::remove_dir_all(&cache_dir).ok();
});

fn split_data(data: &[u8]) -> (String, String, String) {
    let len = data.len();
    if len < 10 {
        return (String::new(), String::new(), String::new());
    }

    let sep1 = find_separator(data, 0);
    let sep2 = find_separator(data, sep1 + 1);

    let vfs_name = String::from_utf8_lossy(&data[0..sep1.min(len)]).to_string();
    let rest = &data[sep1.min(len)..];
    let db_path = String::from_utf8_lossy(rest).to_string();

    let sql = if sep2 < len {
        String::from_utf8_lossy(&data[sep2..]).to_string()
    } else {
        String::new()
    };

    (vfs_name, db_path, sql)
}

fn find_separator(data: &[u8], after: usize) -> usize {
    let search = &data[after..];
    for (i, &b) in search.iter().enumerate() {
        if b == b'\n' || b == b'\0' {
            return after + i;
        }
    }
    data.len().saturating_sub(1)
}

fn rand_id() -> u32 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u32
}
