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

#[cfg(feature = "cloud")]
mod tiered {
    pub mod helpers;
    mod basic;
    mod btree_grouping;
    mod compact;
    mod data_ops;
    mod indexes;
    mod gc;
    #[cfg(feature = "encryption")]
    mod encryption;
    mod advanced;
    mod eviction;
    mod manifest_persistence;
    mod materialize;
    mod staging;
    mod borodino;
    mod jena;
    #[cfg(feature = "wal")]
    mod wal_integration;
    mod zenith;
    mod drift;
    mod oracle_s3;
    mod crash_flush;
}
