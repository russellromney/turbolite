use super::*;
use crate::tiered::*;

#[test]
fn test_tiered_config_default() {
    let c = TurboliteConfig::default();
    assert_eq!(c.cache_dir, PathBuf::from("/tmp/turbolite-cache"));
    assert_eq!(c.compression.level, 3);
    assert!(!c.read_only);
    assert_eq!(c.cache.pages_per_group, DEFAULT_PAGES_PER_GROUP);
    assert_eq!(c.cache.ttl_secs, 0);
    assert_eq!(c.prefetch.search, vec![0.3, 0.3, 0.4]);
    assert_eq!(c.prefetch.lookup, vec![0.0, 0.0, 0.0]);
    let expected_threads = std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(2)
        + 1;
    assert_eq!(c.prefetch.threads, expected_threads);
}

#[test]
fn test_tiered_config_default_pages_per_group() {
    assert_eq!(DEFAULT_PAGES_PER_GROUP, 256);
    assert_eq!(TurboliteConfig::default().cache.pages_per_group, 256);
}

// ── Serde deserialization tests ────────────────────────────────────────

#[test]
fn test_deserialize_empty_object_uses_defaults() {
    let c: TurboliteConfig = serde_json::from_str("{}").expect("empty object");
    assert_eq!(c.compression.level, 3);
    assert_eq!(c.cache.pages_per_group, DEFAULT_PAGES_PER_GROUP);
    assert!(!c.read_only);
}

#[test]
fn test_deserialize_compression_level() {
    let json = r#"{ "compression": { "level": 9 } }"#;
    let c: TurboliteConfig = serde_json::from_str(json).expect("compression level");
    assert_eq!(c.compression.level, 9);
}

#[test]
fn test_deserialize_read_only() {
    let json = r#"{ "read_only": true }"#;
    let c: TurboliteConfig = serde_json::from_str(json).expect("read only");
    assert!(c.read_only);
}

#[test]
fn test_deserialize_cache_ttl() {
    let json = r#"{ "cache": { "ttl_secs": 7200 } }"#;
    let c: TurboliteConfig = serde_json::from_str(json).expect("cache ttl");
    assert_eq!(c.cache.ttl_secs, 7200);
}

#[test]
fn test_deserialize_pages_per_group() {
    let json = r#"{ "cache": { "pages_per_group": 512 } }"#;
    let c: TurboliteConfig = serde_json::from_str(json).expect("ppg");
    assert_eq!(c.cache.pages_per_group, 512);
}

#[test]
fn test_deserialize_prefetch_schedules() {
    let json = r#"{ "prefetch": { "search": [0.5, 0.5], "lookup": [0.1] } }"#;
    let c: TurboliteConfig = serde_json::from_str(json).expect("prefetch");
    assert_eq!(c.prefetch.search, vec![0.5, 0.5]);
    assert_eq!(c.prefetch.lookup, vec![0.1]);
}

#[test]
fn test_deserialize_cache_compression() {
    let json = r#"{ "cache": { "compression": true, "compression_level": 6 } }"#;
    let c: TurboliteConfig = serde_json::from_str(json).expect("cache compression");
    assert!(c.cache.compression);
    assert_eq!(c.cache.compression_level, 6);
}

#[test]
fn test_deserialize_max_cache_bytes() {
    let json = r#"{ "cache": { "max_bytes": 536870912 } }"#;
    let c: TurboliteConfig = serde_json::from_str(json).expect("max cache bytes");
    assert_eq!(c.cache.max_bytes, Some(536870912));
}

#[test]
fn test_deserialize_manifest_source_remote() {
    let json = r#"{ "prefetch": { "manifest_source": "Remote" } }"#;
    let c: TurboliteConfig = serde_json::from_str(json).expect("manifest source");
    assert_eq!(c.prefetch.manifest_source, ManifestSource::Remote);
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
    let result =
        serde_json::from_str::<TurboliteConfig>(r#"{ "compression": { "level": "high" } }"#);
    assert!(result.is_err());
}

#[test]
fn test_serialize_roundtrip() {
    let original = TurboliteConfig {
        cache_dir: PathBuf::from("/data/mydb"),
        read_only: true,
        compression: CompressionConfig {
            level: 5,
            ..Default::default()
        },
        cache: CacheConfig {
            ttl_secs: 1800,
            ..Default::default()
        },
        ..Default::default()
    };
    let json = serde_json::to_string(&original).expect("serialize");
    let deserialized: TurboliteConfig = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(deserialized.cache_dir, original.cache_dir);
    assert_eq!(deserialized.compression.level, original.compression.level);
    assert_eq!(deserialized.read_only, original.read_only);
    assert_eq!(deserialized.cache.ttl_secs, original.cache.ttl_secs);
}

#[test]
fn test_deserialize_encryption_key() {
    let key = [0xABu8; 32];
    let json = format!(r#"{{ "encryption": {{ "key": {:?} }} }}"#, key.to_vec());
    let c: TurboliteConfig = serde_json::from_str(&json).expect("encryption key");
    assert_eq!(c.encryption.key, Some(key));
}

#[test]
fn test_deserialize_encryption_key_null() {
    let json = r#"{ "encryption": { "key": null } }"#;
    let c: TurboliteConfig = serde_json::from_str(json).expect("null encryption key");
    assert_eq!(c.encryption.key, None);
}
