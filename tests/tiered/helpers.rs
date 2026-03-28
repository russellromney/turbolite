//! Shared helpers for tiered integration tests.

use turbolite::tiered::{Manifest, TieredConfig};
use std::sync::atomic::{AtomicU32, Ordering};

/// Deserialize manifest from msgpack bytes into serde_json::Value for test assertions.
pub fn manifest_from_msgpack(bytes: &[u8]) -> serde_json::Value {
    let m: Manifest = rmp_serde::from_slice(bytes).expect("valid msgpack manifest");
    serde_json::to_value(&m).expect("manifest to json value")
}

/// Counter for unique VFS names across tests (SQLite requires unique names).
static VFS_COUNTER: AtomicU32 = AtomicU32::new(0);

pub fn unique_vfs_name(prefix: &str) -> String {
    let n = VFS_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{}_{}", prefix, n)
}

/// Get test bucket from env, or skip.
pub fn test_bucket() -> String {
    std::env::var("TIERED_TEST_BUCKET")
        .expect("TIERED_TEST_BUCKET env var required for tiered tests")
}

/// Get S3 endpoint URL (default: Tigris).
pub fn endpoint_url() -> String {
    std::env::var("AWS_ENDPOINT_URL")
        .unwrap_or_else(|_| "https://t3.storage.dev".to_string())
}

/// Create a TieredConfig with a unique prefix (so tests don't collide).
pub fn test_config(prefix: &str, cache_dir: &std::path::Path) -> TieredConfig {
    let unique_prefix = format!(
        "test/{}/{}",
        prefix,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    TieredConfig {
        bucket: test_bucket(),
        prefix: unique_prefix,
        cache_dir: cache_dir.to_path_buf(),
        compression_level: 3,
        endpoint_url: Some(endpoint_url()),
        region: Some("auto".to_string()),
        ..Default::default()
    }
}

/// Directly read the S3 manifest and verify it has the expected properties.
/// This proves data actually landed in Tigris, not just local cache.
pub fn verify_s3_manifest(
    bucket: &str,
    prefix: &str,
    endpoint: &Option<String>,
    expected_page_count_min: u64,
    expected_page_size: u64,
) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let manifest_data = rt.block_on(async {
        let aws_config = aws_config::from_env()
            .region(aws_sdk_s3::config::Region::new("auto"))
            .load()
            .await;
        let mut s3_config = aws_sdk_s3::config::Builder::from(&aws_config);
        if let Some(ep) = endpoint {
            s3_config = s3_config.endpoint_url(ep).force_path_style(true);
        }
        let client = aws_sdk_s3::Client::from_conf(s3_config.build());

        let resp = client
            .get_object()
            .bucket(bucket)
            .key(format!("{}/manifest.msgpack", prefix))
            .send()
            .await
            .expect("manifest should exist in S3 after checkpoint");

        resp.body
            .collect()
            .await
            .unwrap()
            .into_bytes()
            .to_vec()
    });

    let manifest: serde_json::Value = manifest_from_msgpack(&manifest_data);
    let version = manifest["version"].as_u64().unwrap();
    let page_count = manifest["page_count"].as_u64().unwrap();
    let page_size = manifest["page_size"].as_u64().unwrap();
    let pages_per_group = manifest["pages_per_group"].as_u64().unwrap_or(2048);

    assert!(version >= 1, "manifest version should be >= 1, got {}", version);
    assert!(
        page_count >= expected_page_count_min,
        "manifest page_count should be >= {}, got {}",
        expected_page_count_min, page_count
    );
    assert_eq!(
        page_size, expected_page_size,
        "manifest page_size mismatch"
    );
    assert!(
        pages_per_group > 0,
        "manifest pages_per_group should be > 0, got {}",
        pages_per_group
    );
}

/// Verify that S3 has page group objects under the prefix.
pub fn verify_s3_has_page_groups(
    bucket: &str,
    prefix: &str,
    endpoint: &Option<String>,
) -> usize {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let aws_config = aws_config::from_env()
            .region(aws_sdk_s3::config::Region::new("auto"))
            .load()
            .await;
        let mut s3_config = aws_sdk_s3::config::Builder::from(&aws_config);
        if let Some(ep) = endpoint {
            s3_config = s3_config.endpoint_url(ep).force_path_style(true);
        }
        let client = aws_sdk_s3::Client::from_conf(s3_config.build());

        let resp = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(format!("{}/pg/", prefix))
            .send()
            .await
            .expect("listing page groups should succeed");

        let count = resp.contents().len();
        assert!(count > 0, "should have at least 1 page group object in S3");
        count
    })
}

/// Helper: count all S3 objects under a prefix (pg/ + ibc/ + manifest).
pub fn count_s3_objects(bucket: &str, prefix: &str, endpoint: &Option<String>) -> usize {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let aws_config = aws_config::from_env()
            .region(aws_sdk_s3::config::Region::new("auto"))
            .load()
            .await;
        let mut s3_config = aws_sdk_s3::config::Builder::from(&aws_config);
        if let Some(ep) = endpoint {
            s3_config = s3_config.endpoint_url(ep).force_path_style(true);
        }
        let client = aws_sdk_s3::Client::from_conf(s3_config.build());

        let mut count = 0;
        let mut token: Option<String> = None;
        loop {
            let mut req = client
                .list_objects_v2()
                .bucket(bucket)
                .prefix(prefix);
            if let Some(t) = &token {
                req = req.continuation_token(t);
            }
            let resp = req.send().await.expect("S3 list should succeed");
            count += resp.contents().len();
            if resp.is_truncated() == Some(true) {
                token = resp.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }
        count
    })
}
