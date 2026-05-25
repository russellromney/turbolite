use super::*;
use arc_swap::ArcSwap;
use hadb_storage_mem::MemStorage;
use crate::DatabaseHandle;
use std::collections::HashSet;
#[cfg(feature = "encryption")]
use std::fs::OpenOptions as FsOpenOptions;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

#[cfg(feature = "encryption")]
fn test_key() -> [u8; 32] {
    let mut key = [0u8; 32];
    for (i, b) in key.iter_mut().enumerate() {
        *b = i as u8;
    }
    key
}

#[cfg(feature = "encryption")]
fn wrong_key() -> [u8; 32] {
    [0xFFu8; 32]
}

fn handle_with_manifest(
    dir: &TempDir,
    page_group_keys: Vec<String>,
) -> (TurboliteHandle, Arc<DiskCache>) {
    let page_size = 64;
    let pages_per_group = 4;
    let page_count = 4;
    let cache = Arc::new(
        DiskCache::new(
            dir.path(),
            3600,
            pages_per_group,
            2,
            page_size,
            page_count,
            None,
            Vec::new(),
        )
        .unwrap(),
    );
    let manifest = Manifest {
        page_count,
        page_size,
        pages_per_group,
        page_group_keys,
        ..Manifest::empty()
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    let handle = TurboliteHandle::new_tiered(
        Some(Arc::new(MemStorage::new())),
        Some(rt.handle().clone()),
        Arc::clone(&cache),
        Arc::new(ArcSwap::from_pointee(manifest)),
        Arc::new(Mutex::new(HashSet::new())),
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicU64::new(0)),
        dir.path().join("db.lock"),
        pages_per_group,
        0,
        true,
        vec![0.3, 0.3, 0.4],
        vec![0.0, 0.0, 0.0],
        None,
        false,
        #[cfg(feature = "zstd")]
        None,
        None,
        false,
        None,
        false,
        0,
        0,
        false,
        false,
        Arc::new(parking_lot::RwLock::new(())),
        Arc::new(AtomicU64::new(0)),
    )
    .unwrap();
    (handle, cache)
}

#[test]
fn tiered_read_releases_claim_when_page_group_key_missing() {
    let dir = TempDir::new().unwrap();
    let (mut handle, cache) = handle_with_manifest(&dir, Vec::new());

    let mut buf = vec![0u8; 64];
    handle.read_exact_at(&mut buf, 0).unwrap();

    assert_eq!(cache.group_state(0), GroupState::None);
    assert_eq!(buf, vec![0u8; 64]);
}

#[test]
fn tiered_read_releases_claim_when_page_group_key_empty() {
    let dir = TempDir::new().unwrap();
    let (mut handle, cache) = handle_with_manifest(&dir, vec![String::new()]);

    let mut buf = vec![0u8; 64];
    handle.read_exact_at(&mut buf, 0).unwrap();

    assert_eq!(cache.group_state(0), GroupState::None);
    assert_eq!(buf, vec![0u8; 64]);
}

#[test]
#[cfg(feature = "encryption")]
fn test_passthrough_write_read_roundtrip_encrypted() {
    // Passthrough files (WAL/journal) should CTR-encrypt on write and decrypt on read
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.wal");
    let file = FsOpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .unwrap();
    let key = test_key();
    let mut handle = TurboliteHandle::new_passthrough(file, path.clone(), Some(key));

    // Write data at various offsets
    let data1 = vec![0xAAu8; 128];
    let data2 = vec![0xBBu8; 256];
    handle.write_all_at(&data1, 0).unwrap();
    handle.write_all_at(&data2, 128).unwrap();

    // Read back — should get plaintext back through CTR decrypt
    let mut buf1 = vec![0u8; 128];
    let mut buf2 = vec![0u8; 256];
    handle.read_exact_at(&mut buf1, 0).unwrap();
    handle.read_exact_at(&mut buf2, 128).unwrap();

    assert_eq!(buf1, data1, "read at offset 0 must match written data");
    assert_eq!(buf2, data2, "read at offset 128 must match written data");
}

#[test]
#[cfg(feature = "encryption")]
fn test_passthrough_on_disk_not_plaintext() {
    // WAL file bytes on disk must NOT be plaintext when encryption enabled
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.wal");
    let file = FsOpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .unwrap();
    let key = test_key();
    let mut handle = TurboliteHandle::new_passthrough(file, path.clone(), Some(key));

    let plaintext = vec![0xCCu8; 512];
    handle.write_all_at(&plaintext, 0).unwrap();

    // Read raw bytes from disk (bypassing VFS)
    let raw = std::fs::read(&path).unwrap();
    assert_ne!(
        &raw[..512],
        &plaintext[..],
        "on-disk bytes must NOT be plaintext"
    );
}

#[test]
#[cfg(feature = "encryption")]
fn test_passthrough_wrong_key_returns_garbage() {
    // Reading with a different key must return garbage, not the original data
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.wal");

    // Write with correct key
    {
        let file = FsOpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        let key = test_key();
        let mut handle = TurboliteHandle::new_passthrough(file, path.clone(), Some(key));
        handle.write_all_at(&vec![0xDDu8; 256], 0).unwrap();
    }

    // Read with wrong key
    let file = FsOpenOptions::new().read(true).open(&path).unwrap();
    let mut handle = TurboliteHandle::new_passthrough(file, path.clone(), Some(wrong_key()));
    let mut buf = vec![0u8; 256];
    handle.read_exact_at(&mut buf, 0).unwrap(); // CTR doesn't fail, just produces wrong data
    assert_ne!(
        buf,
        vec![0xDDu8; 256],
        "wrong key must not produce original plaintext"
    );
}

#[test]
#[cfg(feature = "encryption")]
fn test_passthrough_no_encryption_is_plaintext() {
    // Without encryption key, passthrough should be plain read/write
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.wal");
    let file = FsOpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .unwrap();
    let mut handle = TurboliteHandle::new_passthrough(file, path.clone(), None);

    let plaintext = vec![0xEEu8; 128];
    handle.write_all_at(&plaintext, 0).unwrap();

    // On-disk should be plaintext
    let raw = std::fs::read(&path).unwrap();
    assert_eq!(
        &raw[..128],
        &plaintext[..],
        "without encryption, on-disk must be plaintext"
    );
}

#[test]
#[cfg(feature = "encryption")]
fn test_passthrough_different_offsets_different_ciphertext() {
    // Same data written at different offsets must produce different ciphertext
    // (each write picks a fresh random nonce stored in the sidecar).
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.wal");
    let file = FsOpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .unwrap();
    let key = test_key();
    let mut handle = TurboliteHandle::new_passthrough(file, path.clone(), Some(key));

    let data = vec![0xFFu8; 64];
    handle.write_all_at(&data, 0).unwrap();
    handle.write_all_at(&data, 64).unwrap();

    let raw = std::fs::read(&path).unwrap();
    assert_ne!(
        &raw[..64],
        &raw[64..128],
        "same data at different offsets must produce different ciphertext"
    );
}

#[test]
#[cfg(feature = "encryption")]
fn test_passthrough_same_offset_rewrite_decrypts_and_no_keystream_reuse() {
    // Rewriting the same WAL slot must (a) decrypt to the newest plaintext and
    // (b) use a fresh nonce so the two ciphertexts differ — the property the
    // append-only nonce sidecar exists to guarantee.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.wal");
    let open = || {
        FsOpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap()
    };
    let key = test_key();
    let mut handle = TurboliteHandle::new_passthrough(open(), path.clone(), Some(key));

    let v1 = vec![0x11u8; 128];
    handle.write_all_at(&v1, 0).unwrap();
    let raw1 = std::fs::read(&path).unwrap()[..128].to_vec();

    let v2 = vec![0x22u8; 128];
    handle.write_all_at(&v2, 0).unwrap();
    let raw2 = std::fs::read(&path).unwrap()[..128].to_vec();

    // Same offset, but XOR of the two ciphertexts must not equal XOR of the
    // two plaintexts (that equality is the two-time-pad signature).
    let ct_xor: Vec<u8> = raw1.iter().zip(&raw2).map(|(a, b)| a ^ b).collect();
    let pt_xor: Vec<u8> = v1.iter().zip(&v2).map(|(a, b)| a ^ b).collect();
    assert_ne!(ct_xor, pt_xor, "rewrite must not reuse the keystream");

    // Newest write decrypts.
    let mut buf = vec![0u8; 128];
    handle.read_exact_at(&mut buf, 0).unwrap();
    assert_eq!(buf, v2, "read after rewrite must return the newest value");
}

#[test]
#[cfg(feature = "encryption")]
fn test_passthrough_sidecar_reloads_and_compacts_across_restart() {
    // The append-only sidecar must survive a reopen (the load path replays the
    // log) and compact down to one record per live extent rather than growing
    // unbounded with rewrites.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.wal");
    let sidecar = {
        let mut s = path.clone().into_os_string();
        s.push(".tlnonce");
        std::path::PathBuf::from(s)
    };
    let open = || {
        FsOpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap()
    };
    let key = test_key();

    // Many rewrites of the same single extent: the append log grows by one
    // record each, but the live extent count stays at 1.
    {
        let mut handle = TurboliteHandle::new_passthrough(open(), path.clone(), Some(key));
        for i in 0..20u8 {
            handle.write_all_at(&vec![i; 128], 0).unwrap();
        }
        // Drop handle (simulates shutdown).
    }
    let grown = std::fs::metadata(&sidecar).unwrap().len();
    assert!(grown >= 24, "sidecar should hold at least one record");

    // Reopen: load() replays the log and compacts it to the single live extent
    // (24 bytes), and the newest plaintext still decrypts.
    let mut handle = TurboliteHandle::new_passthrough(open(), path.clone(), Some(key));
    let compacted = std::fs::metadata(&sidecar).unwrap().len();
    assert_eq!(
        compacted, 24,
        "load must compact the log to one record per live extent"
    );

    let mut buf = vec![0u8; 128];
    handle.read_exact_at(&mut buf, 0).unwrap();
    assert_eq!(
        buf,
        vec![19u8; 128],
        "newest value must survive the reload + compaction"
    );

    // A further write after reload still appends durably and reads back.
    handle.write_all_at(&vec![0x55u8; 128], 0).unwrap();
    let mut buf2 = vec![0u8; 128];
    handle.read_exact_at(&mut buf2, 0).unwrap();
    assert_eq!(buf2, vec![0x55u8; 128]);
}
