//! Regression tests for adversarial review E5:
//! key rotation must clear the local cache before publishing the rotated manifest.

#[cfg(feature = "encryption")]
mod rotation_crash {
    use std::sync::Arc;

    use hadb_storage::StorageBackend;
    use hadb_storage_mem::MemStorage;
    use tempfile::TempDir;
    use turbolite::tiered::{
        register_shared, CacheConfig, CompressionConfig, SharedTurboliteVfs, TurboliteConfig,
        TurboliteVfs,
    };

    const OLD_KEY: [u8; 32] = [
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c,
        0x1d, 0x1e, 0x1f, 0x20,
    ];

    fn unique_vfs_name(label: &str) -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        format!("rotate-crash-{label}-{nanos}")
    }

    fn build_vfs(
        dir: &std::path::Path,
        label: &str,
        backend: Arc<dyn StorageBackend>,
        key: Option<[u8; 32]>,
        rt: tokio::runtime::Handle,
    ) -> (String, SharedTurboliteVfs) {
        let mut cfg = TurboliteConfig {
            cache_dir: dir.to_path_buf(),
            compression: CompressionConfig {
                level: 1,
                ..Default::default()
            },
            cache: CacheConfig {
                pages_per_group: 4,
                checkpoint_mode: turbolite::tiered::CheckpointMode::LocalThenFlush,
                ..Default::default()
            },
            ..Default::default()
        };
        #[cfg(feature = "encryption")]
        {
            cfg.encryption.key = key;
        }
        let vfs = TurboliteVfs::with_backend(cfg, backend, rt).expect("vfs");
        let shared = SharedTurboliteVfs::new(vfs);
        let vfs_name = unique_vfs_name(label);
        register_shared(&vfs_name, shared.clone()).expect("register");
        (vfs_name, shared)
    }

    fn write_rows(vfs_name: &str) {
        let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE;
        let conn = rusqlite::Connection::open_with_flags_and_vfs("rot.db", flags, vfs_name)
            .expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL;").expect("wal");
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);")
            .expect("schema");
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..100 {
            tx.execute("INSERT INTO t (id, v) VALUES (?1, ?2)", rusqlite::params![i, i * 10])
                .unwrap();
        }
        tx.commit().unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .expect("ckpt");
    }

    fn assert_rows(vfs_name: &str) {
        let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY;
        let conn =
            rusqlite::Connection::open_with_flags_and_vfs("rot.db", flags, vfs_name).expect("open");
        for i in 0..100 {
            let got: i64 = conn
                .query_row("SELECT v FROM t WHERE id = ?1", rusqlite::params![i], |r| {
                    r.get(0)
                })
                .expect(&format!("read row {i}"));
            assert_eq!(got, i * 10, "row {i} mismatch");
        }
    }

    #[test]
    fn rotation_clears_local_cache_before_manifest_commit() {
        let dir = TempDir::new().expect("tmpdir");
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("rt");
        let mem = Arc::new(MemStorage::new());

        let (vfs_name, shared) = build_vfs(dir.path(), "writer", mem.clone(), Some(OLD_KEY), rt.handle().clone());
        write_rows(&vfs_name);
        shared.flush_to_storage().expect("flush");

        let data_cache = dir.path().join("data.cache");
        let local_state = dir.path().join("local_state.msgpack");
        assert!(data_cache.exists(), "cache file should exist after flush");

        // Rotate to remove encryption.
        let mut cfg = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            compression: CompressionConfig {
                level: 1,
                ..Default::default()
            },
            cache: CacheConfig {
                pages_per_group: 4,
                checkpoint_mode: turbolite::tiered::CheckpointMode::LocalThenFlush,
                ..Default::default()
            },
            ..Default::default()
        };
        #[cfg(feature = "encryption")]
        {
            cfg.encryption.key = Some(OLD_KEY);
        }
        cfg.apply_legacy_flat_fields();
        turbolite::tiered::rotate_encryption_key(&cfg, mem.clone(), rt.handle().clone(), None)
            .expect("rotate");

        // Cache must be cleared BEFORE the function returns (and therefore before
        // any manifest commit that callers may observe).
        assert!(
            !data_cache.exists(),
            "data.cache must be cleared during rotation"
        );
        assert!(
            !local_state.exists(),
            "local_state.msgpack must be cleared during rotation"
        );

        // Reopen with no key and a fresh cache dir to prove the new manifest and
        // objects are decryptable without the old key.
        let cold_dir = TempDir::new().expect("cold tmpdir");
        let (cold_name, _) = build_vfs(cold_dir.path(), "cold", mem.clone(), None, rt.handle().clone());
        assert_rows(&cold_name);
    }
}
