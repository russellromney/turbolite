use super::*;
use crate::tiered::*;

#[test]
fn test_tiered_config_default() {
    let c = TieredConfig::default();
    assert_eq!(c.bucket, "");
    assert_eq!(c.prefix, "");
    assert_eq!(c.cache_dir, PathBuf::from("/tmp/sqlces-cache"));
    assert_eq!(c.compression_level, 3);
    assert_eq!(c.endpoint_url, None);
    assert!(!c.read_only);
    assert!(c.runtime_handle.is_none());
    assert_eq!(c.pages_per_group, DEFAULT_PAGES_PER_GROUP);
    assert_eq!(c.region, None);
    assert_eq!(c.cache_ttl_secs, 3600);
    assert_eq!(c.prefetch_hops, vec![0.33, 0.33]);
    assert_eq!(c.btree_prefetch_hops, vec![0.0, 0.5, 0.5]);
    assert_eq!(c.max_range_gets_per_tree, 2);
    let expected_threads = std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(2) + 1;
    assert_eq!(c.prefetch_threads, expected_threads);
}

#[test]
fn test_tiered_config_default_pages_per_group() {
    assert_eq!(DEFAULT_PAGES_PER_GROUP, 256);
    assert_eq!(TieredConfig::default().pages_per_group, 256);
}

#[test]
fn test_max_range_gets_per_tree_config_default() {
    let c = TieredConfig::default();
    assert_eq!(c.max_range_gets_per_tree, 2);
}

#[test]
fn test_max_range_gets_per_tree_config_zero() {
    let c = TieredConfig {
        max_range_gets_per_tree: 0,
        ..Default::default()
    };
    assert_eq!(c.max_range_gets_per_tree, 0);
}

#[test]
fn test_max_range_gets_per_tree_config_max() {
    let c = TieredConfig {
        max_range_gets_per_tree: u8::MAX,
        ..Default::default()
    };
    assert_eq!(c.max_range_gets_per_tree, 255);
}
