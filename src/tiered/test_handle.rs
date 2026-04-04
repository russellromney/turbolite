use super::*;
use tempfile::TempDir;
use std::fs::OpenOptions as FsOpenOptions;

fn test_key() -> [u8; 32] {
    let mut key = [0u8; 32];
    for (i, b) in key.iter_mut().enumerate() { *b = i as u8; }
    key
}

fn wrong_key() -> [u8; 32] {
    [0xFFu8; 32]
}

#[test]
#[cfg(feature = "encryption")]
fn test_passthrough_write_read_roundtrip_encrypted() {
    // Passthrough files (WAL/journal) should CTR-encrypt on write and decrypt on read
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.wal");
    let file = FsOpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
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
    let file = FsOpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
    let key = test_key();
    let mut handle = TurboliteHandle::new_passthrough(file, path.clone(), Some(key));

    let plaintext = vec![0xCCu8; 512];
    handle.write_all_at(&plaintext, 0).unwrap();

    // Read raw bytes from disk (bypassing VFS)
    let raw = std::fs::read(&path).unwrap();
    assert_ne!(&raw[..512], &plaintext[..], "on-disk bytes must NOT be plaintext");
}

#[test]
#[cfg(feature = "encryption")]
fn test_passthrough_wrong_key_returns_garbage() {
    // Reading with a different key must return garbage, not the original data
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.wal");

    // Write with correct key
    {
        let file = FsOpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
        let key = test_key();
        let mut handle = TurboliteHandle::new_passthrough(file, path.clone(), Some(key));
        handle.write_all_at(&vec![0xDDu8; 256], 0).unwrap();
    }

    // Read with wrong key
    let file = FsOpenOptions::new().read(true).open(&path).unwrap();
    let mut handle = TurboliteHandle::new_passthrough(file, path.clone(), Some(wrong_key()));
    let mut buf = vec![0u8; 256];
    handle.read_exact_at(&mut buf, 0).unwrap(); // CTR doesn't fail, just produces wrong data
    assert_ne!(buf, vec![0xDDu8; 256], "wrong key must not produce original plaintext");
}

#[test]
#[cfg(feature = "encryption")]
fn test_passthrough_no_encryption_is_plaintext() {
    // Without encryption key, passthrough should be plain read/write
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.wal");
    let file = FsOpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
    let mut handle = TurboliteHandle::new_passthrough(file, path.clone(), None);

    let plaintext = vec![0xEEu8; 128];
    handle.write_all_at(&plaintext, 0).unwrap();

    // On-disk should be plaintext
    let raw = std::fs::read(&path).unwrap();
    assert_eq!(&raw[..128], &plaintext[..], "without encryption, on-disk must be plaintext");
}

#[test]
#[cfg(feature = "encryption")]
fn test_passthrough_different_offsets_different_ciphertext() {
    // Same data written at different offsets must produce different ciphertext (nonce = offset)
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.wal");
    let file = FsOpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
    let key = test_key();
    let mut handle = TurboliteHandle::new_passthrough(file, path.clone(), Some(key));

    let data = vec![0xFFu8; 64];
    handle.write_all_at(&data, 0).unwrap();
    handle.write_all_at(&data, 64).unwrap();

    let raw = std::fs::read(&path).unwrap();
    assert_ne!(&raw[..64], &raw[64..128], "same data at different offsets must produce different ciphertext");
}
