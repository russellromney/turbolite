use super::*;
use tempfile::TempDir;

// =========================================================================
// StorageClient::Local tests
// =========================================================================

fn local_client(dir: &std::path::Path) -> StorageClient {
    StorageClient::local(dir.to_path_buf()).expect("local client creation failed")
}

// ── Page group operations ──

#[test]
fn test_local_put_get_page_group() {
    let dir = TempDir::new().unwrap();
    let client = local_client(dir.path());

    let key = StorageClient::page_group_key(0, 1);
    let data = vec![0xABu8; 1024];

    client.put_page_groups(&[(key.clone(), data.clone())]).unwrap();

    let result = client.get_page_group(&key).unwrap();
    assert_eq!(result, Some(data));
}

#[test]
fn test_local_get_missing_page_group() {
    let dir = TempDir::new().unwrap();
    let client = local_client(dir.path());

    let result = client.get_page_group("pg/99_v1").unwrap();
    assert_eq!(result, None);
}

#[test]
fn test_local_put_get_multiple_page_groups() {
    let dir = TempDir::new().unwrap();
    let client = local_client(dir.path());

    let groups: Vec<(String, Vec<u8>)> = (0..5)
        .map(|i| {
            let key = StorageClient::page_group_key(i, 1);
            let data = vec![i as u8; 512];
            (key, data)
        })
        .collect();

    client.put_page_groups(&groups).unwrap();

    let keys: Vec<String> = groups.iter().map(|(k, _)| k.clone()).collect();
    let result = client.get_page_groups_by_key(&keys).unwrap();
    assert_eq!(result.len(), 5);

    for (key, data) in &groups {
        assert_eq!(result.get(key).unwrap(), data);
    }
}

#[test]
fn test_local_get_page_groups_by_key_partial() {
    let dir = TempDir::new().unwrap();
    let client = local_client(dir.path());

    let key0 = StorageClient::page_group_key(0, 1);
    client.put_page_groups(&[(key0.clone(), vec![1, 2, 3])]).unwrap();

    let key1 = StorageClient::page_group_key(1, 1); // not stored
    let result = client.get_page_groups_by_key(&[key0.clone(), key1]).unwrap();
    assert_eq!(result.len(), 1);
    assert!(result.contains_key(&key0));
}

#[test]
fn test_local_delete_page_groups() {
    let dir = TempDir::new().unwrap();
    let client = local_client(dir.path());

    let key = StorageClient::page_group_key(0, 1);
    client.put_page_groups(&[(key.clone(), vec![1, 2, 3])]).unwrap();
    assert!(client.get_page_group(&key).unwrap().is_some());

    client.delete_page_groups(&[key.clone()]).unwrap();
    assert!(client.get_page_group(&key).unwrap().is_none());
}

#[test]
fn test_local_delete_missing_is_ok() {
    let dir = TempDir::new().unwrap();
    let client = local_client(dir.path());

    // Deleting non-existent key should not error
    client.delete_page_groups(&["pg/99_v1".to_string()]).unwrap();
}

#[test]
fn test_local_overwrite_page_group() {
    let dir = TempDir::new().unwrap();
    let client = local_client(dir.path());

    let key = StorageClient::page_group_key(0, 1);
    client.put_page_groups(&[(key.clone(), vec![1, 2, 3])]).unwrap();
    client.put_page_groups(&[(key.clone(), vec![4, 5, 6])]).unwrap();

    let result = client.get_page_group(&key).unwrap().unwrap();
    assert_eq!(result, vec![4, 5, 6]);
}

// ── Manifest operations ──

#[test]
fn test_local_manifest_roundtrip() {
    let dir = TempDir::new().unwrap();
    let client = local_client(dir.path());

    // No manifest initially
    assert!(client.get_manifest().unwrap().is_none());
    assert!(!client.exists().unwrap());

    // Create and store manifest
    let mut manifest = Manifest::empty();
    manifest.version = 1;
    manifest.page_count = 100;
    manifest.page_size = 4096;
    manifest.pages_per_group = 256;

    client.put_manifest(&manifest, &[]).unwrap();

    // Read it back
    let loaded = client.get_manifest().unwrap().expect("manifest should exist");
    assert_eq!(loaded.version, 1);
    assert_eq!(loaded.page_count, 100);
    assert_eq!(loaded.page_size, 4096);
    assert!(client.exists().unwrap());
}

#[test]
fn test_local_manifest_with_dirty_groups() {
    let dir = TempDir::new().unwrap();
    let client = local_client(dir.path());

    let manifest = Manifest::empty();
    client.put_manifest(&manifest, &[0, 1, 2]).unwrap();

    // Manifest loads fine (dirty_groups are metadata, not exposed via get_manifest)
    let loaded = client.get_manifest().unwrap().expect("manifest should exist");
    assert_eq!(loaded.version, 0);
}

#[test]
fn test_local_manifest_overwrite() {
    let dir = TempDir::new().unwrap();
    let client = local_client(dir.path());

    let mut m1 = Manifest::empty();
    m1.version = 1;
    client.put_manifest(&m1, &[]).unwrap();

    let mut m2 = Manifest::empty();
    m2.version = 2;
    m2.page_count = 50;
    client.put_manifest(&m2, &[]).unwrap();

    let loaded = client.get_manifest().unwrap().unwrap();
    assert_eq!(loaded.version, 2);
    assert_eq!(loaded.page_count, 50);
}

// ── Exists ──

#[test]
fn test_local_exists_empty() {
    let dir = TempDir::new().unwrap();
    let client = local_client(dir.path());
    assert!(!client.exists().unwrap());
}

#[test]
fn test_local_exists_after_manifest() {
    let dir = TempDir::new().unwrap();
    let client = local_client(dir.path());
    client.put_manifest(&Manifest::empty(), &[]).unwrap();
    assert!(client.exists().unwrap());
}

// ── Range GET ──

#[test]
fn test_local_range_get() {
    let dir = TempDir::new().unwrap();
    let client = local_client(dir.path());

    let key = StorageClient::page_group_key(0, 1);
    let data: Vec<u8> = (0..100).collect();
    client.put_page_groups(&[(key.clone(), data)]).unwrap();

    // Read bytes 10..20
    let result = client.range_get(&key, 10, 10).unwrap().unwrap();
    assert_eq!(result, (10u8..20).collect::<Vec<u8>>());
}

#[test]
fn test_local_range_get_missing() {
    let dir = TempDir::new().unwrap();
    let client = local_client(dir.path());

    let result = client.range_get("pg/99_v1", 0, 10).unwrap();
    assert_eq!(result, None);
}

// ── Key generation ──

#[test]
fn test_key_generation() {
    assert_eq!(StorageClient::page_group_key(0, 1), "pg/0_v1");
    assert_eq!(StorageClient::page_group_key(42, 7), "pg/42_v7");
    assert_eq!(StorageClient::interior_chunk_key(3, 5), "ibc/3_v5");
    assert_eq!(StorageClient::index_chunk_key(1, 2), "ixb/1_v2");
}

// ── Is local ──

#[test]
fn test_is_local() {
    let dir = TempDir::new().unwrap();
    let client = local_client(dir.path());
    assert!(client.is_local());
}

// ── Interior/index bundles use same put/get as page groups ──

#[test]
fn test_local_interior_bundle_roundtrip() {
    let dir = TempDir::new().unwrap();
    let client = local_client(dir.path());

    let key = StorageClient::interior_chunk_key(0, 1);
    let data = vec![0xFFu8; 256];
    client.put_page_groups(&[(key.clone(), data.clone())]).unwrap();

    let result = client.get_page_group(&key).unwrap().unwrap();
    assert_eq!(result, data);
}

#[test]
fn test_local_index_bundle_roundtrip() {
    let dir = TempDir::new().unwrap();
    let client = local_client(dir.path());

    let key = StorageClient::index_chunk_key(2, 3);
    let data = vec![0xCCu8; 128];
    client.put_page_groups(&[(key.clone(), data.clone())]).unwrap();

    let result = client.get_page_group(&key).unwrap().unwrap();
    assert_eq!(result, data);
}

// ── Concurrent put doesn't corrupt (atomic write) ──

#[test]
fn test_local_atomic_write() {
    let dir = TempDir::new().unwrap();
    let client = local_client(dir.path());

    let key = StorageClient::page_group_key(0, 1);

    // Write two versions; the second should win completely (no partial state)
    let data1 = vec![0xAAu8; 1000];
    let data2 = vec![0xBBu8; 2000];

    client.put_page_groups(&[(key.clone(), data1)]).unwrap();
    client.put_page_groups(&[(key.clone(), data2.clone())]).unwrap();

    let result = client.get_page_group(&key).unwrap().unwrap();
    assert_eq!(result, data2);
}

// ── StorageBackend config ──

#[test]
fn test_storage_backend_default_is_local() {
    let config = TieredConfig::default();
    assert!(matches!(config.storage_backend, StorageBackend::Local));
    assert!(config.is_local());
}

#[test]
fn test_effective_backend_local_with_empty_bucket() {
    let config = TieredConfig::default();
    assert!(matches!(config.effective_backend(), StorageBackend::Local));
}

#[cfg(feature = "cloud")]
#[test]
fn test_effective_backend_auto_upgrade_with_bucket() {
    let mut config = TieredConfig::default();
    config.bucket = "my-bucket".to_string();
    config.prefix = "my-prefix".to_string();

    match config.effective_backend() {
        StorageBackend::S3 { bucket, prefix, .. } => {
            assert_eq!(bucket, "my-bucket");
            assert_eq!(prefix, "my-prefix");
        }
        _ => panic!("expected S3 backend"),
    }
}

#[cfg(feature = "cloud")]
#[test]
fn test_effective_backend_explicit_s3() {
    let mut config = TieredConfig::default();
    config.storage_backend = StorageBackend::S3 {
        bucket: "explicit-bucket".to_string(),
        prefix: "explicit-prefix".to_string(),
        endpoint_url: None,
        region: None,
    };

    match config.effective_backend() {
        StorageBackend::S3 { bucket, .. } => {
            assert_eq!(bucket, "explicit-bucket");
        }
        _ => panic!("expected S3 backend"),
    }
}

// ── Diagnostics ──

#[test]
fn test_local_diagnostics_are_zero() {
    let dir = TempDir::new().unwrap();
    let client = local_client(dir.path());
    assert_eq!(client.fetch_count(), 0);
    assert_eq!(client.fetch_bytes(), 0);
}

// ── pg/ directory creation ──

#[test]
fn test_local_creates_pg_dir() {
    let dir = TempDir::new().unwrap();
    let _client = local_client(dir.path());
    assert!(dir.path().join("pg").is_dir());
}

#[test]
fn test_local_nested_dir_creation() {
    let dir = TempDir::new().unwrap();
    let nested = dir.path().join("deep").join("nested");
    let client = StorageClient::local(nested.clone()).unwrap();

    let key = StorageClient::page_group_key(0, 1);
    client.put_page_groups(&[(key.clone(), vec![1])]).unwrap();
    assert!(client.get_page_group(&key).unwrap().is_some());
}
