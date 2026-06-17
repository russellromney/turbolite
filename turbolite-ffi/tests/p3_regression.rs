//! P3 FFI regression tests for fixes that are testable from the public
//! Rust API in default standalone mode.

use std::panic::{self, AssertUnwindSafe};

/// G6 regression: trace callbacks must not unwind across the C boundary,
/// even with null / invalid inputs.
#[test]
fn trace_push_plan_null_sql_does_not_unwind() {
    let result = panic::catch_unwind(AssertUnwindSafe(|| unsafe {
        turbolite_ffi::turbolite_trace_push_plan(std::ptr::null_mut(), std::ptr::null());
    }));
    assert!(result.is_ok(), "null sql must not unwind");
}

#[test]
fn trace_end_query_does_not_unwind() {
    let result =
        panic::catch_unwind(AssertUnwindSafe(|| turbolite_ffi::turbolite_trace_end_query()));
    assert!(result.is_ok(), "end_query must not unwind");
}

/// G5 regression (standalone path): the high-level register entry points
/// reject null / empty required strings without panicking.
#[cfg(not(feature = "loadable-extension"))]
#[test]
fn register_local_nulls_are_safe() {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        turbolite_ffi::ffi::turbolite_register_local(std::ptr::null(), std::ptr::null(), 3)
    }));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), -1);
}

#[cfg(not(feature = "loadable-extension"))]
#[test]
fn register_local_file_first_nulls_are_safe() {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        turbolite_ffi::ffi::turbolite_register_local_file_first(
            std::ptr::null(),
            std::ptr::null(),
            3,
        )
    }));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), -1);
}

/// G5 regression (loadable-extension path): ext register entry points
/// reject null / empty required string parameters.
#[cfg(feature = "loadable-extension")]
#[test]
fn ext_register_named_vfs_rejects_null() {
    let result = panic::catch_unwind(AssertUnwindSafe(|| unsafe {
        turbolite_ffi::ext::turbolite_ext_register_named_vfs(
            std::ptr::null(),
            std::ptr::null(),
        )
    }));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1);
}

#[cfg(feature = "loadable-extension")]
#[test]
fn ext_register_file_first_vfs_rejects_null() {
    let result = panic::catch_unwind(AssertUnwindSafe(|| unsafe {
        turbolite_ffi::ext::turbolite_ext_register_file_first_vfs(
            std::ptr::null(),
            std::ptr::null(),
        )
    }));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1);
}

/// G7 regression (via the public validate path): NaN / inf / overflow are
/// rejected by `turbolite_config_set` validation.
#[test]
fn config_set_rejects_non_finite_and_overflow() {
    use turbolite::tiered::settings;

    assert!(settings::validate("cache_limit", "NaN GB").is_err());
    assert!(settings::validate("cache_limit", "inf GB").is_err());
    assert!(settings::validate("cache_limit", "1e309 GB").is_err());
    assert!(settings::validate("prefetch", "NaN").is_err());
    assert!(settings::validate("prefetch", "inf").is_err());
    assert!(settings::validate("prefetch", "1.5").is_err());
}
