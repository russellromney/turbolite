//! Integration tests for tiered S3-backed storage.
//!
//! These tests run against Tigris (S3-compatible). Requires S3 credentials.
//!
//! ```bash
//! # Source Tigris credentials, then:
//! TIERED_TEST_BUCKET=sqlces-test \
//!   AWS_ENDPOINT_URL=https://t3.storage.dev \
//!   cargo test --features tiered,zstd tiered
//! ```

#[cfg(feature = "tiered")]
mod tiered {
    pub mod helpers;
    mod basic;
    mod data_ops;
    mod indexes;
    mod gc;
    #[cfg(feature = "encryption")]
    mod encryption;
    mod advanced;
}
