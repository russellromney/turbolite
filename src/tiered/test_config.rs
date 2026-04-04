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

