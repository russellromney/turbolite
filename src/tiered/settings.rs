//! Per-connection prefetch tuning via SQL functions.
//!
//! Users call `SELECT turbolite_config_set('key', 'value')` to adjust prefetch
//! schedules without reopening the connection. The SQL function pushes to
//! a global queue; the VFS drains it on the next read (same pattern as
//! the plan queue in query_plan.rs).
//!
//! Supported keys:
//!   - `prefetch`: convenience, sets both search and lookup schedules
//!   - `prefetch_search`: SEARCH query schedule (aggressive warmup)
//!   - `prefetch_lookup`: index lookup / point query schedule (conservative)
//!   - `prefetch_reset`: reset both to defaults (value ignored)
//!   - `plan_aware`: "true"/"false" to enable/disable plan-aware prefetch
//!
//! Example:
//! ```sql
//! SELECT turbolite_config_set('prefetch_search', '0.3,0.3,0.4');
//! SELECT turbolite_config_set('prefetch_lookup', '0,0.1,0.2');
//! SELECT turbolite_config_set('prefetch', '0.2,0.3,0.5');  -- sets both
//! SELECT turbolite_config_set('prefetch_reset', '');         -- reset to defaults
//! SELECT turbolite_config_set('plan_aware', 'true');
//! ```

use std::sync::Mutex;

/// A single setting update from a SQL function call.
#[derive(Debug, Clone)]
pub struct SettingUpdate {
    pub key: String,
    pub value: String,
}

/// Global queue of pending setting updates.
/// Push: SQL function (turbolite_config_set). Drain: VFS on next read.
/// Safe because SQLite is single-threaded per connection: the SQL function
/// completes before the next VFS read on the same connection.
static SETTINGS_QUEUE: Mutex<Vec<SettingUpdate>> = Mutex::new(Vec::new());

/// Push a setting update (called from SQL function via FFI).
pub fn push_setting(key: String, value: String) {
    let mut queue = SETTINGS_QUEUE.lock().expect("settings queue poisoned");
    queue.push(SettingUpdate { key, value });
}

/// Drain all pending setting updates (called from VFS on each read).
/// Returns empty Vec if nothing pending.
pub fn drain_settings() -> Vec<SettingUpdate> {
    let mut queue = SETTINGS_QUEUE.lock().expect("settings queue poisoned");
    if queue.is_empty() {
        return Vec::new();
    }
    std::mem::take(&mut *queue)
}

/// Parse a comma-separated hop schedule string into Vec<f32>.
/// Returns None if parsing fails.
pub fn parse_hops(value: &str) -> Option<Vec<f32>> {
    let hops: Result<Vec<f32>, _> = value.split(',')
        .map(|s| s.trim().parse::<f32>())
        .collect();
    hops.ok().filter(|v| !v.is_empty())
}

/// FFI entry point: `turbolite_config_set(key, value)` SQL function.
///
/// # Safety
/// `key` and `value` must be valid C strings.
#[no_mangle]
pub unsafe extern "C" fn turbolite_config_set(
    key: *const std::ffi::c_char,
    value: *const std::ffi::c_char,
) -> i32 {
    if key.is_null() || value.is_null() {
        return 1;
    }
    let key_str = match std::ffi::CStr::from_ptr(key).to_str() {
        Ok(s) => s,
        Err(_) => return 1,
    };
    let value_str = match std::ffi::CStr::from_ptr(value).to_str() {
        Ok(s) => s,
        Err(_) => return 1,
    };

    // Validate before pushing
    match key_str {
        "prefetch" | "prefetch_search" | "prefetch_lookup" => {
            if parse_hops(value_str).is_none() {
                return 1;
            }
        }
        "prefetch_reset" => {
            // Value ignored, any value accepted
        }
        "plan_aware" => {
            if !matches!(value_str, "true" | "false" | "1" | "0") {
                return 1;
            }
        }
        _ => return 1,
    }

    push_setting(key_str.to_string(), value_str.to_string());
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_hops_valid() {
        assert_eq!(parse_hops("0.3,0.3,0.4"), Some(vec![0.3, 0.3, 0.4]));
        assert_eq!(parse_hops("0,0,0.2,0.3,0.5"), Some(vec![0.0, 0.0, 0.2, 0.3, 0.5]));
        assert_eq!(parse_hops("1.0"), Some(vec![1.0]));
    }

    #[test]
    fn test_parse_hops_invalid() {
        assert_eq!(parse_hops(""), None);
        assert_eq!(parse_hops("abc"), None);
        assert_eq!(parse_hops("0.3,abc"), None);
    }

    #[test]
    fn test_push_and_drain() {
        drain_settings();

        push_setting("prefetch_search".to_string(), "0.3,0.3,0.4".to_string());
        push_setting("prefetch_lookup".to_string(), "0,0.1,0.2".to_string());

        let updates = drain_settings();
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].key, "prefetch_search");
        assert_eq!(updates[0].value, "0.3,0.3,0.4");
        assert_eq!(updates[1].key, "prefetch_lookup");
        assert_eq!(updates[1].value, "0,0.1,0.2");

        assert!(drain_settings().is_empty());
    }

    #[test]
    fn test_prefetch_convenience_sets_both() {
        drain_settings();

        push_setting("prefetch".to_string(), "0.5,0.5".to_string());
        let updates = drain_settings();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].key, "prefetch");
        assert_eq!(updates[0].value, "0.5,0.5");
    }

    #[test]
    fn test_prefetch_reset() {
        drain_settings();

        push_setting("prefetch_reset".to_string(), "".to_string());
        let updates = drain_settings();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].key, "prefetch_reset");
    }

    #[test]
    fn test_ffi_validation() {
        use std::ffi::CString;
        drain_settings();

        // Valid: prefetch_search with schedule
        let key = CString::new("prefetch_search").unwrap();
        let value = CString::new("0.3,0.3,0.4").unwrap();
        assert_eq!(unsafe { turbolite_config_set(key.as_ptr(), value.as_ptr()) }, 0);

        // Valid: prefetch convenience
        let key = CString::new("prefetch").unwrap();
        let value = CString::new("0.2,0.3,0.5").unwrap();
        assert_eq!(unsafe { turbolite_config_set(key.as_ptr(), value.as_ptr()) }, 0);

        // Valid: prefetch_reset (value ignored)
        let key = CString::new("prefetch_reset").unwrap();
        let value = CString::new("").unwrap();
        assert_eq!(unsafe { turbolite_config_set(key.as_ptr(), value.as_ptr()) }, 0);

        // Invalid: unknown key
        let bad_key = CString::new("unknown_key").unwrap();
        let value = CString::new("0.3").unwrap();
        assert_eq!(unsafe { turbolite_config_set(bad_key.as_ptr(), value.as_ptr()) }, 1);

        // Invalid: bad schedule value
        let key = CString::new("prefetch_search").unwrap();
        let bad_value = CString::new("not,numbers").unwrap();
        assert_eq!(unsafe { turbolite_config_set(key.as_ptr(), bad_value.as_ptr()) }, 1);

        // Valid: plan_aware
        let plan_key = CString::new("plan_aware").unwrap();
        let plan_val = CString::new("true").unwrap();
        assert_eq!(unsafe { turbolite_config_set(plan_key.as_ptr(), plan_val.as_ptr()) }, 0);

        // Invalid: plan_aware bad value
        let bad_plan = CString::new("maybe").unwrap();
        assert_eq!(unsafe { turbolite_config_set(plan_key.as_ptr(), bad_plan.as_ptr()) }, 1);

        drain_settings();
    }
}
