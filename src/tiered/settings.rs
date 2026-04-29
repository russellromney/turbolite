//! Per-connection prefetch / cache tuning via the `turbolite_config_set`
//! SQL function, plus small parsing helpers reused by config loaders.
//!
//! Users call `SELECT turbolite_config_set('key', 'value')` to adjust
//! prefetch schedules and related knobs without reopening the connection.
//! The function pushes a `SettingUpdate` into the calling handle's own
//! queue; the VFS drains it on the next read (same "push → drain on next
//! xRead" pattern as the EQP plan queue, but scoped per-handle).
//!
//! Scoping is per-`TurboliteHandle`, not process-global. Each handle owns
//! an `Arc<Mutex<Vec<SettingUpdate>>>` and registers it on a thread-local
//! stack in `TurboliteHandle::new_*` and un-registers on `Drop`. The FFI
//! function pushes to the top of the current thread's stack — the most
//! recently opened handle on this thread. SQLite runs a connection
//! single-threaded, so "most recently opened on this thread" is the
//! connection whose query is being executed when the SQL function fires.
//! Connection A setting a schedule cannot leak into connection B's VFS,
//! even when they share a VFS registration, because the state rides on
//! the handle, not the VFS.
//!
//! Supported keys:
//!   - `prefetch`        — convenience, sets both search and lookup schedules
//!   - `prefetch_search` — SEARCH query schedule (aggressive warmup)
//!   - `prefetch_lookup` — index lookup / point query schedule (conservative)
//!   - `prefetch_reset`  — reset both to defaults (value ignored)
//!   - `plan_aware`      — "true"/"false"/"1"/"0"
//!   - `cache_limit`     — byte-size string ("512MB", "2GB", "0" = unlimited)
//!   - `evict_on_checkpoint` — "true"/"false"/"1"/"0"
//!
//! Example:
//! ```sql
//! SELECT turbolite_config_set('prefetch_search', '0.3,0.3,0.4');
//! SELECT turbolite_config_set('prefetch_lookup', '0,0,0');
//! SELECT * FROM posts WHERE id = 42;  -- runs with the tuned schedule
//! ```

use std::cell::RefCell;
use std::sync::{Arc, Mutex};

/// A single setting update from a SQL function call.
#[derive(Debug, Clone)]
pub struct SettingUpdate {
    pub key: String,
    pub value: String,
}

/// Per-handle settings queue. Shared between the `TurboliteHandle` that
/// drains it on xRead and the FFI function that pushes into it.
pub type SettingsQueue = Arc<Mutex<Vec<SettingUpdate>>>;

thread_local! {
    /// Stack of active handle queues on this thread, most-recent at the
    /// back. Pushed by `TurboliteHandle::new_*`, popped by `Drop`.
    /// The FFI function pushes into the top of the stack.
    static QUEUE_STACK: RefCell<Vec<SettingsQueue>> = const { RefCell::new(Vec::new()) };
}

/// Construct a fresh settings queue for a new handle.
pub fn new_queue() -> SettingsQueue {
    Arc::new(Mutex::new(Vec::new()))
}

/// Register a handle's queue as the current active queue on this thread.
/// Called from `TurboliteHandle::new_*`.
pub fn enter_handle(q: SettingsQueue) {
    QUEUE_STACK.with(|s| s.borrow_mut().push(q));
}

/// Un-register a handle's queue from this thread's stack. Called from
/// `TurboliteHandle::Drop`. Matches by `Arc` identity (pointer equality)
/// rather than stack position so out-of-order drops don't corrupt the
/// stack.
pub fn leave_handle(q: &SettingsQueue) {
    QUEUE_STACK.with(|s| {
        let mut stack = s.borrow_mut();
        if let Some(pos) = stack.iter().rposition(|x| Arc::ptr_eq(x, q)) {
            stack.remove(pos);
        }
    });
}

/// Push a setting update into the current thread's top-of-stack queue.
/// Returns `true` if a handle was active on this thread, `false` if not.
pub fn push_to_current(update: SettingUpdate) -> bool {
    QUEUE_STACK.with(|s| {
        let stack = s.borrow();
        if let Some(q) = stack.last() {
            q.lock().expect("settings queue poisoned").push(update);
            true
        } else {
            false
        }
    })
}

/// Clone the Arc of the top-of-stack queue on the current thread, if
/// any. Used by [`crate::install_config_functions`] to capture the
/// calling connection's handle queue at install time so the scalar
/// function closure can push directly to it — correct routing without
/// consulting the thread-local stack at call time.
pub fn top_queue() -> Option<SettingsQueue> {
    QUEUE_STACK.with(|s| s.borrow().last().cloned())
}

/// Inspect the latest pending value for `key` on the current thread's
/// top-of-stack queue. Primarily for tests and diagnostics; does not
/// drain. Returns `None` if no handle is active on this thread or no
/// update for `key` is pending.
///
/// "Last write wins" matches the drain semantics: if multiple pushes for
/// the same key are queued, the most recent one is the one `xRead` will
/// eventually apply, so that's what `peek` surfaces.
pub fn peek_top_for_key(key: &str) -> Option<String> {
    QUEUE_STACK.with(|s| {
        let stack = s.borrow();
        let q = stack.last()?;
        let queue = q.lock().expect("settings queue poisoned");
        queue
            .iter()
            .rev()
            .find(|u| u.key == key)
            .map(|u| u.value.clone())
    })
}

/// Drain all pending updates from a specific handle's queue. Called from
/// the handle's xRead path; safe to call concurrently with `push_to_current`
/// (Mutex serializes), but in practice one handle's queue is only touched
/// by one thread.
pub fn drain_queue(q: &SettingsQueue) -> Vec<SettingUpdate> {
    let mut guard = q.lock().expect("settings queue poisoned");
    if guard.is_empty() {
        Vec::new()
    } else {
        std::mem::take(&mut *guard)
    }
}

/// Parse a comma-separated hop schedule string into `Vec<f32>`. Returns
/// `None` on parse failure or empty input.
pub fn parse_hops(value: &str) -> Option<Vec<f32>> {
    let hops: Result<Vec<f32>, _> = value.split(',').map(|s| s.trim().parse::<f32>()).collect();
    hops.ok().filter(|v| !v.is_empty())
}

/// Parse a human-readable byte size string into `u64` bytes.
/// Supports: "512MB", "2GB", "512M", "2G", "1073741824", "0" (unlimited).
/// Case-insensitive suffixes. Returns None on parse failure.
pub fn parse_byte_size(value: &str) -> Option<u64> {
    let s = value.trim();
    if s.is_empty() {
        return None;
    }
    if let Ok(n) = s.parse::<u64>() {
        return Some(n);
    }
    let s_upper = s.to_uppercase();
    let (num_part, multiplier) = if s_upper.ends_with("GB") {
        (&s[..s.len() - 2], 1024u64 * 1024 * 1024)
    } else if s_upper.ends_with("MB") {
        (&s[..s.len() - 2], 1024u64 * 1024)
    } else if s_upper.ends_with("KB") {
        (&s[..s.len() - 2], 1024u64)
    } else if s_upper.ends_with('G') {
        (&s[..s.len() - 1], 1024u64 * 1024 * 1024)
    } else if s_upper.ends_with('M') {
        (&s[..s.len() - 1], 1024u64 * 1024)
    } else if s_upper.ends_with('K') {
        (&s[..s.len() - 1], 1024u64)
    } else {
        return None;
    };
    let num: f64 = num_part.trim().parse().ok()?;
    if num < 0.0 {
        return None;
    }
    Some((num * multiplier as f64) as u64)
}

/// Push a setting update from a Rust caller. Same semantics as the FFI
/// function below — scoped to the most recently opened handle on the
/// current thread. Returns `Err` if no handle is active here (meaning
/// no turbolite connection has been opened on this thread) or if the
/// key/value fails validation.
pub fn set(key: &str, value: &str) -> Result<(), &'static str> {
    validate(key, value)?;
    if !push_to_current(SettingUpdate {
        key: key.to_string(),
        value: value.to_string(),
    }) {
        return Err("no active turbolite handle on this thread");
    }
    Ok(())
}

/// Validate a `(key, value)` pair. Returns `Err` with a static description
/// on failure, `Ok(())` on success.
pub fn validate(key: &str, value: &str) -> Result<(), &'static str> {
    match key {
        "prefetch" | "prefetch_search" | "prefetch_lookup" => {
            if parse_hops(value).is_none() {
                return Err("invalid hop schedule");
            }
        }
        "prefetch_reset" => {
            // Value ignored, any value accepted.
        }
        "plan_aware" | "evict_on_checkpoint" => {
            if !matches!(value, "true" | "false" | "1" | "0") {
                return Err("expected true/false/1/0");
            }
        }
        "cache_limit" => {
            if parse_byte_size(value).is_none() {
                return Err("invalid byte size");
            }
        }
        _ => return Err("unknown key"),
    }
    Ok(())
}

// The `turbolite_config_set(key, value)` FFI / SQL entry point lives in
// turbolite-ffi. This crate only exposes the Rust-level `set` /
// `push_to_current` / `top_queue` primitives it wraps.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_hops_valid() {
        assert_eq!(parse_hops("0.3,0.3,0.4"), Some(vec![0.3, 0.3, 0.4]));
        assert_eq!(
            parse_hops("0,0,0.2,0.3,0.5"),
            Some(vec![0.0, 0.0, 0.2, 0.3, 0.5])
        );
        assert_eq!(parse_hops("1.0"), Some(vec![1.0]));
    }

    #[test]
    fn parse_hops_invalid() {
        assert_eq!(parse_hops(""), None);
        assert_eq!(parse_hops("abc"), None);
        assert_eq!(parse_hops("0.3,abc"), None);
    }

    #[test]
    fn validate_prefetch_keys() {
        assert!(validate("prefetch", "0.5,0.5").is_ok());
        assert!(validate("prefetch_search", "0.3,0.3,0.4").is_ok());
        assert!(validate("prefetch_lookup", "0,0,0").is_ok());
        assert!(validate("prefetch_reset", "").is_ok());
        assert!(validate("prefetch_search", "not,numbers").is_err());
    }

    #[test]
    fn validate_bool_and_bytes() {
        assert!(validate("plan_aware", "true").is_ok());
        assert!(validate("plan_aware", "maybe").is_err());
        assert!(validate("evict_on_checkpoint", "0").is_ok());
        assert!(validate("cache_limit", "512MB").is_ok());
        assert!(validate("cache_limit", "banana").is_err());
    }

    #[test]
    fn validate_unknown_key() {
        assert!(validate("nope", "true").is_err());
    }

    #[test]
    fn set_without_active_handle_errors() {
        // No TurboliteHandle on this thread; `set()` must refuse.
        assert_eq!(
            set("prefetch", "0.5,0.5"),
            Err("no active turbolite handle on this thread")
        );
    }

    #[test]
    fn push_to_current_respects_stack() {
        let q1 = new_queue();
        let q2 = new_queue();

        enter_handle(q1.clone());
        assert!(push_to_current(SettingUpdate {
            key: "k1".into(),
            value: "v1".into()
        }));
        // Now q1 has one update.
        assert_eq!(q1.lock().unwrap().len(), 1);
        assert_eq!(q2.lock().unwrap().len(), 0);

        enter_handle(q2.clone());
        assert!(push_to_current(SettingUpdate {
            key: "k2".into(),
            value: "v2".into()
        }));
        // q2 is now top-of-stack; update went there.
        assert_eq!(q1.lock().unwrap().len(), 1);
        assert_eq!(q2.lock().unwrap().len(), 1);

        leave_handle(&q2);
        assert!(push_to_current(SettingUpdate {
            key: "k3".into(),
            value: "v3".into()
        }));
        // q1 is back on top.
        assert_eq!(q1.lock().unwrap().len(), 2);
        assert_eq!(q2.lock().unwrap().len(), 1);

        leave_handle(&q1);
        // Stack empty.
        assert!(!push_to_current(SettingUpdate {
            key: "k4".into(),
            value: "v4".into()
        }));
    }

    #[test]
    fn leave_handle_out_of_order() {
        // Drop order may not match enter order (nested handles that close
        // inner-first is the common case, but we guarantee correctness
        // for arbitrary drop orders).
        let q1 = new_queue();
        let q2 = new_queue();
        let q3 = new_queue();
        enter_handle(q1.clone());
        enter_handle(q2.clone());
        enter_handle(q3.clone());

        // Drop q2 first (middle of the stack).
        leave_handle(&q2);

        // Push: top of remaining stack is q3.
        assert!(push_to_current(SettingUpdate {
            key: "x".into(),
            value: "y".into()
        }));
        assert_eq!(q3.lock().unwrap().len(), 1);
        assert_eq!(q1.lock().unwrap().len(), 0);

        leave_handle(&q3);
        leave_handle(&q1);
    }

    #[test]
    fn drain_queue_transfers_ownership() {
        let q = new_queue();
        q.lock().unwrap().push(SettingUpdate {
            key: "a".into(),
            value: "b".into(),
        });
        q.lock().unwrap().push(SettingUpdate {
            key: "c".into(),
            value: "d".into(),
        });

        let drained = drain_queue(&q);
        assert_eq!(drained.len(), 2);
        assert_eq!(q.lock().unwrap().len(), 0);

        let drained_again = drain_queue(&q);
        assert!(drained_again.is_empty());
    }

    // FFI validation coverage (the `turbolite_config_set` C entry point)
    // now lives in turbolite-ffi's test suite. The Rust-level equivalents
    // below exercise `set`, `push_to_current`, and `validate` directly.

    // =============================================================
    // Existing parse_byte_size tests (unchanged)
    // =============================================================

    #[test]
    fn parse_byte_size_numeric() {
        assert_eq!(parse_byte_size("0"), Some(0));
        assert_eq!(parse_byte_size("1073741824"), Some(1073741824));
        assert_eq!(parse_byte_size("512"), Some(512));
    }

    #[test]
    fn parse_byte_size_suffixes() {
        assert_eq!(parse_byte_size("512MB"), Some(512 * 1024 * 1024));
        assert_eq!(parse_byte_size("512mb"), Some(512 * 1024 * 1024));
        assert_eq!(parse_byte_size("2GB"), Some(2 * 1024 * 1024 * 1024));
        assert_eq!(parse_byte_size("2G"), Some(2 * 1024 * 1024 * 1024));
        assert_eq!(parse_byte_size("512M"), Some(512 * 1024 * 1024));
        assert_eq!(parse_byte_size("64KB"), Some(64 * 1024));
        assert_eq!(parse_byte_size("64K"), Some(64 * 1024));
    }

    #[test]
    fn parse_byte_size_fractional() {
        assert_eq!(
            parse_byte_size("1.5GB"),
            Some((1.5 * 1024.0 * 1024.0 * 1024.0) as u64)
        );
        assert_eq!(
            parse_byte_size("0.5M"),
            Some((0.5 * 1024.0 * 1024.0) as u64)
        );
    }

    #[test]
    fn parse_byte_size_invalid() {
        assert_eq!(parse_byte_size(""), None);
        assert_eq!(parse_byte_size("abc"), None);
        assert_eq!(parse_byte_size("MB"), None);
        assert_eq!(parse_byte_size("-1GB"), None);
    }
}
