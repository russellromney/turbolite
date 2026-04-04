use super::*;
use crate::tiered::*;

#[test]
fn test_tiered_config_default() {
    let c = TurboliteConfig::default();
    assert_eq!(c.bucket, "");
    assert_eq!(c.prefix, "");
    assert_eq!(c.cache_dir, PathBuf::from("/tmp/turbolite-cache"));
    assert_eq!(c.compression_level, 1);
    assert_eq!(c.endpoint_url, None);
    assert!(!c.read_only);
    #[cfg(feature = "cloud")]
    assert!(c.runtime_handle.is_none());
    assert_eq!(c.pages_per_group, DEFAULT_PAGES_PER_GROUP);
    assert_eq!(c.region, None);
    assert_eq!(c.cache_ttl_secs, 3600);
    assert_eq!(c.prefetch_search, vec![0.3, 0.3, 0.4]);
    assert_eq!(c.prefetch_lookup, vec![0.0, 0.0, 0.0]);
    let expected_threads = std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(2) + 1;
    assert_eq!(c.prefetch_threads, expected_threads);
}

#[test]
fn test_tiered_config_default_pages_per_group() {
    assert_eq!(DEFAULT_PAGES_PER_GROUP, 256);
    assert_eq!(TurboliteConfig::default().pages_per_group, 256);
}

// ── Serde deserialization tests ────────────────────────────────────────

#[test]
fn test_deserialize_empty_object_uses_defaults() {
    let c: TurboliteConfig = serde_json::from_str("{}").expect("empty object");
    assert!(matches!(c.storage_backend, StorageBackend::Local));
    assert_eq!(c.compression_level, 1);
    assert_eq!(c.pages_per_group, DEFAULT_PAGES_PER_GROUP);
    assert!(!c.read_only);
}

#[test]
fn test_deserialize_local_backend_explicit() {
    let json = r#"{ "storage_backend": "Local", "cache_dir": "/data/test" }"#;
    let c: TurboliteConfig = serde_json::from_str(json).expect("local backend");
    assert!(matches!(c.storage_backend, StorageBackend::Local));
    assert_eq!(c.cache_dir, PathBuf::from("/data/test"));
}

#[cfg(feature = "cloud")]
#[test]
fn test_deserialize_s3_backend() {
    let json = r#"{
        "storage_backend": { "S3": { "bucket": "my-bucket", "prefix": "db/", "endpoint_url": null, "region": "us-west-2" } },
        "cache_dir": "/tmp/cache"
    }"#;
    let c: TurboliteConfig = serde_json::from_str(json).expect("s3 backend");
    match &c.storage_backend {
        StorageBackend::S3 { bucket, prefix, region, .. } => {
            assert_eq!(bucket, "my-bucket");
            assert_eq!(prefix, "db/");
            assert_eq!(region.as_deref(), Some("us-west-2"));
        }
        _ => panic!("expected S3 backend"),
    }
    assert_eq!(c.cache_dir, PathBuf::from("/tmp/cache"));
}

#[test]
fn test_deserialize_compression_level() {
    let json = r#"{ "compression_level": 9 }"#;
    let c: TurboliteConfig = serde_json::from_str(json).expect("compression level");
    assert_eq!(c.compression_level, 9);
}

#[test]
fn test_deserialize_read_only() {
    let json = r#"{ "read_only": true }"#;
    let c: TurboliteConfig = serde_json::from_str(json).expect("read only");
    assert!(c.read_only);
}

#[test]
fn test_deserialize_cache_ttl() {
    let json = r#"{ "cache_ttl_secs": 7200 }"#;
    let c: TurboliteConfig = serde_json::from_str(json).expect("cache ttl");
    assert_eq!(c.cache_ttl_secs, 7200);
}

#[test]
fn test_deserialize_pages_per_group() {
    let json = r#"{ "pages_per_group": 512 }"#;
    let c: TurboliteConfig = serde_json::from_str(json).expect("ppg");
    assert_eq!(c.pages_per_group, 512);
}

#[test]
fn test_deserialize_prefetch_schedules() {
    let json = r#"{ "prefetch_search": [0.5, 0.5], "prefetch_lookup": [0.1] }"#;
    let c: TurboliteConfig = serde_json::from_str(json).expect("prefetch");
    assert_eq!(c.prefetch_search, vec![0.5, 0.5]);
    assert_eq!(c.prefetch_lookup, vec![0.1]);
}

#[test]
fn test_deserialize_cache_compression() {
    let json = r#"{ "cache_compression": true, "cache_compression_level": 6 }"#;
    let c: TurboliteConfig = serde_json::from_str(json).expect("cache compression");
    assert!(c.cache_compression);
    assert_eq!(c.cache_compression_level, 6);
}

#[test]
fn test_deserialize_max_cache_bytes() {
    let json = r#"{ "max_cache_bytes": 536870912 }"#;
    let c: TurboliteConfig = serde_json::from_str(json).expect("max cache bytes");
    assert_eq!(c.max_cache_bytes, Some(536870912));
}

#[test]
fn test_deserialize_sync_mode_durable() {
    let json = r#"{ "sync_mode": "Durable" }"#;
    let c: TurboliteConfig = serde_json::from_str(json).expect("sync mode");
    assert_eq!(c.sync_mode, SyncMode::Durable);
}

#[test]
fn test_deserialize_sync_mode_local_then_flush() {
    let json = r#"{ "sync_mode": "LocalThenFlush" }"#;
    let c: TurboliteConfig = serde_json::from_str(json).expect("sync mode ltf");
    assert_eq!(c.sync_mode, SyncMode::LocalThenFlush);
}

#[test]
fn test_deserialize_manifest_source() {
    let json = r#"{ "manifest_source": "S3" }"#;
    let c: TurboliteConfig = serde_json::from_str(json).expect("manifest source");
    assert_eq!(c.manifest_source, ManifestSource::S3);
}

#[test]
fn test_deserialize_unknown_fields_ignored() {
    let json = r#"{ "cache_dir": "/tmp/x", "future_field": 42, "another": true }"#;
    let c: TurboliteConfig = serde_json::from_str(json).expect("unknown fields");
    assert_eq!(c.cache_dir, PathBuf::from("/tmp/x"));
}

#[test]
fn test_deserialize_invalid_json_fails() {
    let result = serde_json::from_str::<TurboliteConfig>("not json");
    assert!(result.is_err());
}

#[test]
fn test_deserialize_wrong_type_fails() {
    // compression_level should be i32, not string
    let result = serde_json::from_str::<TurboliteConfig>(r#"{ "compression_level": "high" }"#);
    assert!(result.is_err());
}

#[test]
fn test_serialize_roundtrip() {
    let original = TurboliteConfig {
        cache_dir: PathBuf::from("/data/mydb"),
        compression_level: 5,
        read_only: true,
        cache_ttl_secs: 1800,
        ..Default::default()
    };
    let json = serde_json::to_string(&original).expect("serialize");
    let deserialized: TurboliteConfig = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(deserialized.cache_dir, original.cache_dir);
    assert_eq!(deserialized.compression_level, original.compression_level);
    assert_eq!(deserialized.read_only, original.read_only);
    assert_eq!(deserialized.cache_ttl_secs, original.cache_ttl_secs);
}

#[test]
fn test_deserialize_encryption_key() {
    let key = [0xABu8; 32];
    let json = format!(r#"{{ "encryption_key": {:?} }}"#, key.to_vec());
    let c: TurboliteConfig = serde_json::from_str(&json).expect("encryption key");
    assert_eq!(c.encryption_key, Some(key));
}

#[test]
fn test_deserialize_encryption_key_null() {
    let json = r#"{ "encryption_key": null }"#;
    let c: TurboliteConfig = serde_json::from_str(json).expect("null encryption key");
    assert_eq!(c.encryption_key, None);
}

// ── effective_backend tests ────────────────────────────────────────────

#[test]
fn test_effective_backend_local_default() {
    let c = TurboliteConfig::default();
    assert!(matches!(c.effective_backend(), StorageBackend::Local));
    assert!(c.is_local());
}

#[cfg(feature = "cloud")]
#[test]
fn test_effective_backend_legacy_bucket_upgrades_to_s3() {
    let c = TurboliteConfig {
        bucket: "my-bucket".into(),
        prefix: "db/".into(),
        endpoint_url: Some("https://s3.example.com".into()),
        region: Some("us-east-1".into()),
        ..Default::default()
    };
    assert!(!c.is_local());
    match c.effective_backend() {
        StorageBackend::S3 { bucket, prefix, .. } => {
            assert_eq!(bucket, "my-bucket");
            assert_eq!(prefix, "db/");
        }
        _ => panic!("expected S3 from legacy bucket"),
    }
}

#[test]
fn test_effective_backend_empty_bucket_stays_local() {
    let c = TurboliteConfig {
        bucket: String::new(),
        ..Default::default()
    };
    assert!(c.is_local());
}

