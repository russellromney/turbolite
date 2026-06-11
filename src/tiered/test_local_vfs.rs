use super::*;
use anyhow::Result;
use async_trait::async_trait;
use hadb_storage::{CasResult, StorageBackend};
use hadb_storage_mem::MemStorage;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

struct DelayedReadStorageBackend {
    inner: Arc<dyn StorageBackend>,
    delay: Duration,
}

impl DelayedReadStorageBackend {
    fn new(inner: Arc<dyn StorageBackend>, delay: Duration) -> Self {
        Self { inner, delay }
    }
}

#[async_trait]
impl StorageBackend for DelayedReadStorageBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        tokio::time::sleep(self.delay).await;
        self.inner.get(key).await
    }

    async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.inner.put(key, data).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await
    }

    async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
        self.inner.list(prefix, after).await
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.inner.exists(key).await
    }

    async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
        self.inner.put_if_absent(key, data).await
    }

    async fn put_if_match(&self, key: &str, data: &[u8], etag: &str) -> Result<CasResult> {
        self.inner.put_if_match(key, data, etag).await
    }

    async fn range_get(&self, key: &str, start: u64, len: u32) -> Result<Option<Vec<u8>>> {
        tokio::time::sleep(self.delay).await;
        self.inner.range_get(key, start, len).await
    }

    async fn delete_many(&self, keys: &[String]) -> Result<usize> {
        self.inner.delete_many(keys).await
    }

    async fn put_many(&self, entries: &[(String, Vec<u8>)]) -> Result<()> {
        self.inner.put_many(entries).await
    }

    fn backend_name(&self) -> &str {
        self.inner.backend_name()
    }
}

fn unique_test_vfs_name(prefix: &str) -> String {
    format!(
        "{}_{}_{}",
        prefix,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn lookahead_profile_sql() -> &'static str {
    "SELECT posts.id, posts.content, posts.created_at
     FROM users
     JOIN posts ON posts.user_id = users.id
     WHERE users.id = ?1
     ORDER BY posts.created_at DESC
     LIMIT 10"
}

fn lookahead_profile_plan(reader: &rusqlite::Connection) -> Vec<query_plan::PlannedAccess> {
    let eqp_sql = format!("EXPLAIN QUERY PLAN {}", lookahead_profile_sql());
    let mut stmt = reader.prepare(&eqp_sql).expect("prepare profile eqp");
    let details: Vec<String> = stmt
        .query_map([7i64], |row| row.get::<_, String>(3))
        .expect("run profile eqp")
        .map(|row| row.expect("profile eqp detail"))
        .collect();
    let accesses = query_plan::parse_eqp_output(&details.join("\n"));
    assert!(
        accesses.iter().any(|access| {
            access.tree_name == "idx_posts_user_created"
                && access.access_type == query_plan::AccessType::Search
                && access.table_name.as_deref() == Some("posts")
        }),
        "profile EQP must map the searched index to its table for lookahead; details={details:?}, accesses={accesses:?}"
    );
    accesses
}

type LookaheadProfileReaderResult = (Vec<(i64, String, i64)>, u64, u64, u64, serde_json::Value);

fn run_lookahead_profile_reader(
    backend: Arc<CountingStorageBackend>,
    runtime: &tokio::runtime::Runtime,
    cache_dir: &std::path::Path,
    lookahead: bool,
    name_suffix: &str,
) -> LookaheadProfileReaderResult {
    query_plan::drain_planned_accesses();
    let mut reader_config = TurboliteConfig {
        cache_dir: cache_dir.to_path_buf(),
        read_only: true,
        ..Default::default()
    };
    reader_config.cache.pages_per_group = 8;
    reader_config.cache.sub_pages_per_frame = 1;
    reader_config.prefetch.threads = 8;
    reader_config.prefetch.queue_capacity = 64;
    reader_config.prefetch.io_permits = 16;
    reader_config.prefetch.foreground_reserved_permits = 1;
    reader_config.prefetch.query_plan = true;
    reader_config.prefetch.lookahead = lookahead;
    reader_config.prefetch.manifest_source = ManifestSource::Remote;

    let reader_vfs =
        TurboliteVfs::with_backend(reader_config, backend.clone(), runtime.handle().clone())
            .expect("reader VFS");
    let shared = reader_vfs.shared_state();
    let reader_vfs_name = unique_test_vfs_name(name_suffix);
    crate::tiered::register(&reader_vfs_name, reader_vfs).expect("register reader");
    let reader = rusqlite::Connection::open_with_flags_and_vfs(
        "lookahead_profile.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        &reader_vfs_name,
    )
    .expect("open reader");

    let accesses = lookahead_profile_plan(&reader);
    shared.clear_cache_all();
    backend.reset_stats();
    query_plan::push_planned_accesses(accesses);

    let mut stmt = reader
        .prepare(lookahead_profile_sql())
        .expect("prepare profile");
    let mut cursor = stmt.query([7i64]).expect("query profile");
    let mut rows = Vec::new();
    while let Some(row) = cursor.next().expect("step profile row") {
        rows.push((
            row.get::<_, i64>(0).unwrap(),
            row.get::<_, String>(1).unwrap(),
            row.get::<_, i64>(2).unwrap(),
        ));
    }
    assert_eq!(rows.len(), 10);

    let cache_info: serde_json::Value =
        serde_json::from_str(&shared.cache_info()).expect("cache info json");
    let foreground_ranges = cache_info["prefetch"]["traffic"]["foreground_range"]["ops"]
        .as_u64()
        .unwrap_or(0);
    let lookahead_fired = cache_info["prefetch"]["lookahead"]["fired"]
        .as_u64()
        .unwrap_or(0);
    let lookahead_hits = cache_info["prefetch"]["lookahead"]["hits"]
        .as_u64()
        .unwrap_or(0);
    (
        rows,
        foreground_ranges,
        lookahead_fired,
        lookahead_hits,
        cache_info,
    )
}

/// RED TEST: TurboliteVfs::new_local() with StorageBackend::Local should succeed
/// without any S3 credentials, tokio runtime, or cloud dependencies.
#[test]
fn test_local_vfs_construction() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };

    let vfs = TurboliteVfs::new_local(config).expect("local VFS construction should succeed");

    // Verify it's local
    assert!(vfs.is_local);
}

/// Phase Cirrus e: shared_state() is total for local VFSes. The old
/// "expect(shared_state)" panic for non-S3 backends is gone; the field
/// types carry the invariant.
#[test]
fn test_local_vfs_shared_state_is_total() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new_local(config).expect("local VFS");
    let state = vfs.shared_state();
    assert!(state.is_local);
}

/// RED TEST: Local VFS exists() returns false for new empty dir.
#[test]
fn test_local_vfs_exists_empty() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new_local(config).expect("local VFS");
    // Empty dir: no manifest in the backend.
    assert!(!vfs.exists_inner("main.db").unwrap());
}

/// RED TEST: Local VFS can register with SQLite, open a db, and do CRUD.
#[test]
fn test_local_vfs_sqlite_roundtrip() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };

    let vfs_name = format!("local_rt_{}", std::process::id());
    let vfs = TurboliteVfs::new_local(config).expect("local VFS");
    crate::tiered::register(&vfs_name, vfs).expect("register");

    let db_path = format!("file:test.db?vfs={}", vfs_name);
    let conn = rusqlite::Connection::open(&db_path).expect("open");
    conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'hello')", [])
        .unwrap();

    let val: String = conn
        .query_row("SELECT val FROM t WHERE id = 1", [], |row| row.get(0))
        .unwrap();
    assert_eq!(val, "hello");
}

/// Phase 008 fail-first: a true-cold profile-shaped index->table query should
/// stop paying one foreground range wait per returned row. Current main still
/// does roughly N serialized foreground ranges; lookahead should reduce that
/// shape to about b-tree depth plus the first anchored table miss.
#[test]
fn lookahead_profile_query_true_cold_foreground_rounds_are_depth_shaped() {
    query_plan::drain_planned_accesses();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let inner: Arc<dyn StorageBackend> = Arc::new(MemStorage::new());
    let delayed: Arc<dyn StorageBackend> = Arc::new(DelayedReadStorageBackend::new(
        inner,
        Duration::from_millis(2),
    ));
    let backend = Arc::new(CountingStorageBackend::new(delayed));
    let source_dir = TempDir::new().unwrap();
    let source_path = source_dir.path().join("lookahead_profile.db");
    let import_cache = TempDir::new().unwrap();
    let reader_cache_off = TempDir::new().unwrap();
    let reader_cache_on = TempDir::new().unwrap();

    let writer = rusqlite::Connection::open(&source_path).expect("open source sqlite");
    writer
        .execute_batch(
            "PRAGMA page_size=4096;
             CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
             CREATE TABLE posts (
               id INTEGER PRIMARY KEY,
               user_id INTEGER NOT NULL,
               content TEXT NOT NULL,
               created_at INTEGER NOT NULL
             );
             CREATE INDEX idx_posts_user_created
               ON posts(user_id, created_at DESC);",
        )
        .expect("schema");
    {
        let tx = writer.unchecked_transaction().expect("tx");
        for user_id in 0..20i64 {
            tx.execute(
                "INSERT INTO users (id, name) VALUES (?1, ?2)",
                rusqlite::params![user_id, format!("user-{user_id}")],
            )
            .expect("insert user");
        }
        let content = "x".repeat(2600);
        for post_id in 0..500i64 {
            tx.execute(
                "INSERT INTO posts (id, user_id, content, created_at)
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![post_id, post_id % 20, content, post_id],
            )
            .expect("insert post");
        }
        tx.commit().expect("commit");
    }
    writer
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("checkpoint writer");
    drop(writer);

    let mut import_config = TurboliteConfig {
        cache_dir: import_cache.path().to_path_buf(),
        ..Default::default()
    };
    import_config.cache.pages_per_group = 8;
    import_config.cache.sub_pages_per_frame = 1;
    import_config.prefetch.threads = 0;
    let import_vfs =
        TurboliteVfs::with_backend(import_config, backend.clone(), runtime.handle().clone())
            .expect("import VFS");
    let imported_manifest = import_vfs
        .import_sqlite_file(&source_path)
        .expect("import sqlite file");
    assert!(
        imported_manifest
            .tree_name_to_root_page
            .contains_key("idx_posts_user_created"),
        "fixture must be imported with a B-tree-aware index manifest"
    );

    let (rows_off, foreground_ranges_off, fired_off, hits_off, cache_info_off) =
        run_lookahead_profile_reader(
            backend.clone(),
            &runtime,
            reader_cache_off.path(),
            false,
            "lookahead_reader_off",
        );
    let (rows, foreground_ranges, lookahead_fired, lookahead_hits, cache_info) =
        run_lookahead_profile_reader(
            backend.clone(),
            &runtime,
            reader_cache_on.path(),
            true,
            "lookahead_reader_on",
        );
    assert_eq!(
        rows, rows_off,
        "lookahead must not change profile query results"
    );
    assert_eq!(fired_off, 0, "lookahead-off reader must not fire");
    assert_eq!(hits_off, 0, "lookahead-off reader must not count hits");
    assert!(
        foreground_ranges_off >= rows.len() as u64,
        "lookahead-off true-cold profile query should remain row-shaped; foreground_ranges_off={foreground_ranges_off}, cache_info_off={cache_info_off}"
    );
    assert!(
        lookahead_fired > 0,
        "lookahead should retain an index leaf and fire at the anchored table leaf; cache_info={cache_info}"
    );
    assert!(
        lookahead_hits > 0,
        "lookahead should satisfy at least one retained frame demand; cache_info={cache_info}"
    );
    assert!(
        foreground_ranges <= 8,
        "true-cold profile query should be depth-shaped, not row-shaped; foreground_ranges={foreground_ranges}, backend_stats={:?}",
        backend.stats()
    );
    assert!(
        foreground_ranges < foreground_ranges_off,
        "lookahead-on should beat the same true-cold query with lookahead off; off={foreground_ranges_off}, on={foreground_ranges}"
    );
}

#[test]
fn file_first_vfs_rejects_mismatched_main_db_name() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("app.db");
    let config = TurboliteConfig::for_database_path(&db_path);
    let vfs_name = format!(
        "file_first_reject_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );

    let vfs = TurboliteVfs::new_local(config).expect("local VFS");
    crate::tiered::register(&vfs_name, vfs).expect("register");

    let flags =
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE;
    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        db_path.to_str().expect("utf8 db path"),
        flags,
        &vfs_name,
    )
    .expect("configured file-first path opens");
    drop(conn);

    let other_path = dir.path().join("other.db");
    let err = rusqlite::Connection::open_with_flags_and_vfs(
        other_path.to_str().expect("utf8 other path"),
        flags,
        &vfs_name,
    )
    .expect_err("same file-first VFS must not alias another db name");
    assert!(matches!(err, rusqlite::Error::SqliteFailure(_, _)));

    let same_name_other_dir = dir.path().join("nested").join("app.db");
    std::fs::create_dir_all(same_name_other_dir.parent().unwrap()).unwrap();
    let err = rusqlite::Connection::open_with_flags_and_vfs(
        same_name_other_dir.to_str().expect("utf8 nested path"),
        flags,
        &vfs_name,
    )
    .expect_err("same file-first VFS must not alias the same filename in another directory");
    assert!(matches!(err, rusqlite::Error::SqliteFailure(_, _)));

    let err = rusqlite::Connection::open_with_flags_and_vfs("app.db", flags, &vfs_name)
        .expect_err("bare basename must not alias an absolute file-first path");
    assert!(matches!(err, rusqlite::Error::SqliteFailure(_, _)));
}

#[test]
fn file_first_vfs_refuses_to_delete_main_db_image() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("app.db");
    let config = TurboliteConfig::for_database_path(&db_path);
    let vfs = TurboliteVfs::new_local(config).expect("local VFS");

    std::fs::write(&db_path, b"turbolite-owned").unwrap();
    assert!(
        vfs.exists_inner(db_path.to_str().unwrap()).unwrap(),
        "main image should exist"
    );
    let err = vfs.delete_inner(db_path.to_str().unwrap()).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
    assert!(db_path.exists(), "main image must survive delete refusal");

    let err = vfs.delete_inner("app.db").unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
    assert!(
        db_path.exists(),
        "bare basename must not delete the bound main image"
    );

    assert!(
        !vfs.exists_inner("app.db").unwrap(),
        "bare basename must not alias the bound absolute main image"
    );
}

#[test]
fn file_first_with_backend_keeps_sidecar_to_local_state() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("app.db");
    let remote_dir = dir.path().join("backend");
    let config = TurboliteConfig::for_database_path(&db_path);
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let storage: std::sync::Arc<dyn hadb_storage::StorageBackend> =
        std::sync::Arc::new(hadb_storage_local::LocalStorage::new(&remote_dir));
    let vfs = TurboliteVfs::with_backend(config.clone(), storage, runtime.handle().clone())
        .expect("file-first backend VFS");
    let vfs_name = format!(
        "file_first_backend_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    crate::tiered::register(&vfs_name, vfs).expect("register");

    let flags =
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE;
    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        db_path.to_str().expect("utf8 db path"),
        flags,
        &vfs_name,
    )
    .expect("open file-first db");
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);
         INSERT INTO t VALUES (1, 'sidecar');",
    )
    .unwrap();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    drop(conn);

    assert!(db_path.exists(), "primary app.db artifact should exist");
    assert!(
        config.cache_dir.join("local_state.msgpack").exists(),
        "sidecar should carry unified local state"
    );
    assert!(
        !config.cache_dir.join("data.cache").exists(),
        "file-first mode must not create data.cache"
    );
    assert!(
        !config.cache_dir.join("manifest.msgpack").exists(),
        "caller-supplied backend keeps backend manifest out of the sidecar"
    );
    assert!(
        !config.cache_dir.join("p").exists(),
        "caller-supplied backend keeps page objects out of the sidecar"
    );
    assert!(
        remote_dir.join("manifest.msgpack").exists(),
        "backend owns the manifest object"
    );
    assert!(remote_dir.join("p").exists(), "backend owns page objects");
}

#[test]
fn set_manifest_adopts_same_version_higher_replay_cursor() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new_local(config).expect("local VFS");

    let mut base = Manifest::empty();
    base.version = 7;
    base.change_counter = 2;
    base.page_count = 4;
    vfs.set_manifest(base);
    assert_eq!(vfs.manifest().change_counter, 2);

    let mut advanced = vfs.manifest();
    advanced.change_counter = 9;
    vfs.set_manifest(advanced);
    assert_eq!(
        vfs.manifest().change_counter,
        9,
        "same-version manifest must be able to advance the replay floor"
    );

    let mut stale = vfs.manifest();
    stale.change_counter = 3;
    vfs.set_manifest(stale);
    assert_eq!(
        vfs.manifest().change_counter,
        9,
        "same-version stale cursor must still be rejected"
    );
}

#[test]
fn exact_replay_cursor_can_replace_sqlite_header_counter() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new_local(config).expect("local VFS");

    let mut base = Manifest::empty();
    base.version = 7;
    base.change_counter = 99;
    base.page_count = 4;
    vfs.set_manifest(base);

    let bytes = vfs
        .manifest_bytes_with_exact_replay_cursor(3)
        .expect("exact cursor manifest bytes");
    let decoded = TurboliteVfs::decode_manifest_bytes(&bytes).expect("decode exact cursor bytes");

    assert_eq!(decoded.change_counter, 3);
    assert_eq!(
        vfs.manifest().change_counter,
        3,
        "promotion publishes the external WAL replay cursor exactly, not max(sqlite header, replay seq)"
    );
}

#[test]
fn replay_cursor_contract_manifest_bytes_stamps_cursor_writer_and_anchor() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new_local(config).expect("local VFS");

    let mut base = Manifest::empty();
    base.version = 7;
    base.change_counter = 99;
    base.page_count = 4;
    vfs.set_manifest(base);

    let (bytes, anchor) = vfs
        .manifest_bytes_with_replay_cursor_anchor(3, 11, "writer-a")
        .expect("anchored replay cursor manifest bytes");
    let decoded = TurboliteVfs::decode_manifest_bytes(&bytes).expect("decode replay cursor bytes");

    assert_eq!(decoded.cursor.last_applied_seq, 3);
    assert_eq!(
        decoded.change_counter, 3,
        "legacy replay floor must track the exact anchored cursor until every consumer is cursor-native"
    );
    assert_eq!(decoded.cursor.epoch, 11);
    assert_eq!(decoded.writer_id, "writer-a");
    assert_eq!(decoded.cursor.base_object_checksum, anchor);
    assert_eq!(
        super::wire::base_anchor_checksum(&decoded)
            .expect("recompute base anchor")
            .to_vec(),
        anchor,
        "publisher and follower must agree on the first-delta chain anchor"
    );
    assert_eq!(
        vfs.manifest().cursor,
        decoded.cursor,
        "anchored replay cursor publication must update the installed manifest"
    );
    assert_eq!(
        vfs.manifest().change_counter,
        3,
        "installed compatibility cursor must match the published anchored cursor"
    );
}

#[test]
fn replay_cursor_contract_begin_replay_uses_installed_cursor_floor() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new_local(config).expect("local VFS");

    let mut base = Manifest::empty();
    base.version = 7;
    base.page_size = 4096;
    base.page_count = 4;
    vfs.set_manifest(base);
    vfs.manifest_bytes_with_replay_cursor_anchor(3, 11, "writer-a")
        .expect("seed anchored cursor");

    let mut replay = vfs.begin_replay().expect("begin replay after cursor");
    let err = replay
        .commit_changeset(3)
        .expect_err("begin_replay must not accept an already-folded sequence");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    replay
        .commit_changeset(4)
        .expect("begin_replay should resume after the installed replay cursor");
    replay.abort().expect("abort test replay");
}

#[test]
fn replay_cursor_contract_manifest_bytes_rejects_invalid_cursor_inputs() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new_local(config).expect("local VFS");

    let mut base = Manifest::empty();
    base.version = 7;
    base.page_count = 4;
    vfs.set_manifest(base);

    let err = vfs
        .manifest_bytes_with_replay_cursor_anchor(3, 0, "writer-a")
        .expect_err("zero cursor publish epoch must be rejected");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);

    let err = vfs
        .manifest_bytes_with_replay_cursor_anchor(3, 11, "   ")
        .expect_err("blank cursor writer must be rejected");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);

    vfs.manifest_bytes_with_replay_cursor_anchor(3, 11, "writer-a")
        .expect("seed valid anchored cursor");

    let err = vfs
        .manifest_bytes_with_replay_cursor_anchor(2, 12, "writer-b")
        .expect_err("published cursor must not move backward");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn replay_cursor_contract_update_replay_cursor_preserves_base_shape_and_writer() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new_local(config).expect("local VFS");

    let mut base = Manifest::empty();
    base.version = 9;
    base.page_count = 6;
    vfs.set_manifest(base);
    vfs.manifest_bytes_with_replay_cursor_anchor(3, 11, "writer-a")
        .expect("seed anchored cursor");

    let before = vfs.manifest();
    let next_cursor = ReplayCursor {
        last_applied_seq: 4,
        base_object_checksum: vec![0xAB; 32],
        epoch: 11,
    };
    vfs.update_replay_cursor(next_cursor.clone())
        .expect("advance replay cursor");
    let after = vfs.manifest();

    assert_eq!(after.cursor, next_cursor);
    assert_eq!(after.writer_id, "writer-a");
    assert_eq!(after.version, before.version);
    assert_eq!(after.page_count, before.page_count);
    assert_eq!(after.page_group_keys, before.page_group_keys);
}

#[test]
fn replay_cursor_contract_update_replay_cursor_rejects_malformed_or_backward_cursor() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new_local(config).expect("local VFS");

    let mut base = Manifest::empty();
    base.version = 9;
    base.page_count = 6;
    vfs.set_manifest(base);

    let err = vfs
        .update_replay_cursor(ReplayCursor {
            last_applied_seq: 1,
            base_object_checksum: vec![0xAB; 32],
            epoch: 11,
        })
        .expect_err("advanced cursor requires installed writer id");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);

    vfs.manifest_bytes_with_replay_cursor_anchor(3, 11, "writer-a")
        .expect("seed anchored cursor");

    let err = vfs
        .update_replay_cursor(ReplayCursor {
            last_applied_seq: 4,
            base_object_checksum: vec![0xAB; 31],
            epoch: 11,
        })
        .expect_err("advanced cursor requires a 32-byte anchor");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);

    let err = vfs
        .update_replay_cursor(ReplayCursor {
            last_applied_seq: 2,
            base_object_checksum: vec![0xAB; 32],
            epoch: 11,
        })
        .expect_err("follower cursor must not move backward");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn replay_cursor_contract_publish_replayed_base_stamps_final_cursor_anchor() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new_local(config).expect("local VFS");

    let mut base = Manifest::empty();
    base.version = 13;
    base.page_count = 8;
    base.page_group_keys = vec!["base-group-0".to_string(), "base-group-1".to_string()];
    vfs.set_manifest(base);

    vfs.manifest_bytes_with_replay_cursor_anchor(4, 10, "writer-a")
        .expect("seed working replay cursor");

    let before_publish = vfs.manifest();
    let (bytes, anchor) = vfs
        .publish_replayed_base_with_replay_cursor_anchor(5, 12, "writer-b")
        .expect("anchored replayed base");
    let decoded = TurboliteVfs::decode_manifest_bytes(&bytes).expect("decode promoted base");

    assert_eq!(decoded.cursor.last_applied_seq, 5);
    assert_eq!(decoded.cursor.epoch, 12);
    assert_eq!(decoded.writer_id, "writer-b");
    assert_eq!(decoded.cursor.base_object_checksum, anchor);
    assert_eq!(
        super::wire::base_anchor_checksum(&decoded)
            .expect("recompute promoted base anchor")
            .to_vec(),
        anchor,
        "promotion must anchor the final published base, not the stale working cursor"
    );
    assert_eq!(decoded.version, before_publish.version);
    assert_eq!(decoded.page_count, before_publish.page_count);
    assert_eq!(decoded.page_group_keys, before_publish.page_group_keys);
    assert_eq!(vfs.manifest().cursor, decoded.cursor);
}

/// Local VFS with compression enabled: write + checkpoint + reopen.
#[test]
#[cfg(feature = "zstd")]
fn test_local_vfs_with_compression() {
    let dir = TempDir::new().unwrap();

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                compression: true,
                compression_level: 3,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("local_cmp_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("local VFS with compression");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'compressed')", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
    }

    // Cold reopen with compression
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                compression: true,
                compression_level: 3,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("local_cmp2_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("reopen with compression");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 1", [], |row| row.get(0))
            .unwrap();
        assert_eq!(val, "compressed");
    }
}

/// Local VFS with multiple tables and schema changes.
#[test]
fn test_local_vfs_schema_changes() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs_name = format!("local_schema_{}", std::process::id());
    let vfs = TurboliteVfs::new_local(config).expect("local VFS");
    crate::tiered::register(&vfs_name, vfs).expect("register");

    let db_path = format!("file:test.db?vfs={}", vfs_name);
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();

    // Multiple tables
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])
        .unwrap();
    conn.execute(
        "CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT)",
        [],
    )
    .unwrap();
    conn.execute("CREATE INDEX idx_posts_user ON posts(user_id)", [])
        .unwrap();

    // Insert data
    conn.execute("INSERT INTO users VALUES (1, 'alice')", [])
        .unwrap();
    conn.execute("INSERT INTO users VALUES (2, 'bob')", [])
        .unwrap();
    conn.execute("INSERT INTO posts VALUES (1, 1, 'hello world')", [])
        .unwrap();
    conn.execute("INSERT INTO posts VALUES (2, 1, 'second post')", [])
        .unwrap();
    conn.execute("INSERT INTO posts VALUES (3, 2, 'bob post')", [])
        .unwrap();

    // Query with index
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM posts WHERE user_id = 1", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(count, 2);

    // Alter table
    conn.execute("ALTER TABLE users ADD COLUMN email TEXT", [])
        .unwrap();
    conn.execute(
        "UPDATE users SET email = 'alice@example.com' WHERE id = 1",
        [],
    )
    .unwrap();

    let email: String = conn
        .query_row("SELECT email FROM users WHERE id = 1", [], |row| row.get(0))
        .unwrap();
    assert_eq!(email, "alice@example.com");
}

/// Local VFS maintenance methods should work through the StorageBackend abstraction.
#[test]
fn test_local_vfs_cloud_methods_error() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs = TurboliteVfs::new_local(config).expect("local VFS");

    let gc_result = vfs.gc();
    assert!(
        gc_result.is_ok(),
        "gc should be valid for local backend storage"
    );

    let flush_result = vfs.flush_to_storage();
    assert!(
        flush_result.is_ok(),
        "flush_to_storage should be a no-op on local VFS"
    );
}

/// Local VFS can be constructed; I/O counters (previously exposed via
/// `s3_counters` / `reset_s3_counters`) now live on concrete backend
/// impls, not the generic VFS.
#[test]
fn test_local_vfs_smoke_construct() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let _vfs = TurboliteVfs::new_local(config).expect("local VFS");
}

/// RED TEST: Delete cache file after checkpoint, reopen, verify data recovered from local page groups.
#[test]
fn test_local_vfs_recover_from_page_groups() {
    let dir = TempDir::new().unwrap();

    // Phase 1: write data and checkpoint
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("local_pg1_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();
        for i in 0..100 {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("value_{}", i)],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        drop(conn);
    }

    // Verify page groups exist on disk
    let pg_dir = dir.path().join("p/d");
    assert!(pg_dir.is_dir(), "p/d/ directory should exist");
    let pg_files: Vec<_> = std::fs::read_dir(&pg_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|ext| ext != "tmp").unwrap_or(true))
        .collect();
    assert!(
        !pg_files.is_empty(),
        "page group files should exist in p/d/"
    );

    // Phase 2: delete cache file + bitmap (simulate cache loss), reopen
    let _ = std::fs::remove_file(dir.path().join("data.cache"));
    let _ = std::fs::remove_file(dir.path().join("page_bitmap"));
    let _ = std::fs::remove_file(dir.path().join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(dir.path().join("cache_index.json"));

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("local_pg2_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS after cache loss");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");

        // All data should be recoverable from local page groups
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            count, 100,
            "all 100 rows should be recovered from page groups"
        );

        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 42", [], |row| row.get(0))
            .unwrap();
        assert_eq!(val, "value_42");
    }
}

/// Multiple checkpoints produce distinct page group versions; all data recoverable.
#[test]
fn test_local_vfs_multi_checkpoint() {
    let dir = TempDir::new().unwrap();

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("local_mc_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();

        // Checkpoint 1: insert 50 rows
        for i in 0..50 {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("v1_{}", i)],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        // Checkpoint 2: insert 50 more rows + update some existing
        for i in 50..100 {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("v2_{}", i)],
            )
            .unwrap();
        }
        conn.execute("UPDATE t SET val = 'updated' WHERE id = 0", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        // Checkpoint 3: delete some rows
        conn.execute("DELETE FROM t WHERE id >= 80", []).unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        drop(conn);
    }

    // Delete cache, reopen, verify final state from page groups
    let _ = std::fs::remove_file(dir.path().join("data.cache"));
    let _ = std::fs::remove_file(dir.path().join("page_bitmap"));
    let _ = std::fs::remove_file(dir.path().join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(dir.path().join("cache_index.json"));

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("local_mc2_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).unwrap();

        // Should see 80 rows (100 inserted - 20 deleted)
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            count, 80,
            "should have 80 rows after multi-checkpoint recovery"
        );

        // Row 0 should be updated
        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 0", [], |row| row.get(0))
            .unwrap();
        assert_eq!(val, "updated");

        // Rows 80-99 should not exist
        let high: i64 = conn
            .query_row("SELECT COUNT(*) FROM t WHERE id >= 80", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(high, 0, "deleted rows should not exist");
    }
}

/// Local VFS data survives checkpoint + cold reopen.
#[test]
fn test_local_vfs_checkpoint_reopen() {
    let dir = TempDir::new().unwrap();

    // Write + checkpoint
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("local_ck1_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'persisted')", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
    }

    // Cold reopen
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("local_ck2_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");

        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 1", [], |row| row.get(0))
            .unwrap();
        assert_eq!(val, "persisted");
    }
}

// =========================================================================
// Override write + cold read tests
// =========================================================================

/// Write data with override_threshold=100 (high, so overrides are used),
/// cold reopen, verify data survives.
#[test]
fn test_local_vfs_override_write_cold_read() {
    use std::sync::atomic::Ordering;
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);

    let dir = TempDir::new().unwrap();

    // Write phase
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            // high threshold: everything goes to override path
            // disable auto-compact
            cache: CacheConfig {
                override_threshold: 100,
                compaction_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("local_ovr_w_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'override_test')", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
    }

    // Cold reopen
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                override_threshold: 100,
                compaction_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("local_ovr_r_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");
        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 1", [], |row| row.get(0))
            .unwrap();
        assert_eq!(val, "override_test");
    }
}

/// Write with override, then full rewrite via another checkpoint, cold reopen.
#[test]
fn test_local_vfs_override_then_full_rewrite() {
    use std::sync::atomic::Ordering;
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);

    let dir = TempDir::new().unwrap();

    // Write phase 1: create table and initial data
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                override_threshold: 100,
                compaction_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("local_ovr_fr1_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'first')", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
    }

    // Write phase 2: update to different value (full rewrite, threshold=0)
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            // back to default, full rewrite
            cache: CacheConfig {
                override_threshold: 0,
                compaction_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("local_ovr_fr2_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");
        conn.execute("UPDATE t SET val = 'final' WHERE id = 1", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
    }

    // Cold reopen: verify final value
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("local_ovr_fr3_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("final reopen");
        crate::tiered::register(&vfs_name, vfs).expect("register3");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("final open");
        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 1", [], |row| row.get(0))
            .unwrap();
        assert_eq!(val, "final");
    }
}

/// Accumulate overrides past compaction threshold, verify compaction fires, cold reopen.
#[test]
fn test_local_vfs_override_compaction() {
    use std::sync::atomic::Ordering;
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);

    let dir = TempDir::new().unwrap();

    // Initial write
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            // compact after 2 overrides
            cache: CacheConfig {
                override_threshold: 100,
                compaction_threshold: 2,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("local_cmpct1_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();
        for i in 1..=10 {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("val_{}", i)],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        // Update a few times to accumulate overrides, each followed by checkpoint
        for round in 1..=3 {
            conn.execute(
                "UPDATE t SET val = ?1 WHERE id = 1",
                rusqlite::params![format!("round_{}", round)],
            )
            .unwrap();
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
                .unwrap();
        }
    }

    // Cold reopen: data should be consistent after compaction
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("local_cmpct2_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");
        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 1", [], |row| row.get(0))
            .unwrap();
        assert_eq!(val, "round_3");

        // Other rows should be intact
        let val5: String = conn
            .query_row("SELECT val FROM t WHERE id = 5", [], |row| row.get(0))
            .unwrap();
        assert_eq!(val5, "val_5");
    }
}

// =========================================================================
// Cache validation on open
// =========================================================================

/// Reopen with same manifest version: cache warm, data correct.
#[test]
fn test_cache_validation_warm_reopen_same_version() {
    let dir = TempDir::new().unwrap();

    // Write data
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("cv_warm1_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let conn = rusqlite::Connection::open(format!("file:test.db?vfs={}", vfs_name)).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'warm')", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        drop(conn);
    }

    // Reopen: same manifest version, cache should be warm
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("cv_warm2_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let conn = rusqlite::Connection::open(format!("file:test.db?vfs={}", vfs_name)).unwrap();
        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "warm");
    }
}

/// Simulate external write: modify manifest version + page_group_keys on disk,
/// reopen, verify cache invalidation triggers and correct data is read.
#[test]
fn test_cache_validation_external_write_invalidates_stale_groups() {
    let dir = TempDir::new().unwrap();

    // Session 1: write data + checkpoint
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("cv_ext1_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let conn = rusqlite::Connection::open(format!("file:test.db?vfs={}", vfs_name)).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();
        for i in 1..=5 {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("v1_{}", i)],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        drop(conn);
    }

    // Session 2: update some rows (simulates another node writing)
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("cv_ext2_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let conn = rusqlite::Connection::open(format!("file:test.db?vfs={}", vfs_name)).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("UPDATE t SET val = 'updated' WHERE id = 3", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        drop(conn);
    }

    // Session 3: reopen (simulates original node reopening after external write)
    // The local manifest should now reflect session 2's version.
    // Cache validation should invalidate changed groups.
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("cv_ext3_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let conn = rusqlite::Connection::open(format!("file:test.db?vfs={}", vfs_name)).unwrap();

        // Should see the updated value from session 2
        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 3", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            val, "updated",
            "should see external write after cache validation"
        );

        // Unchanged rows should still be correct
        let val1: String = conn
            .query_row("SELECT val FROM t WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val1, "v1_1");
    }
}

/// Reopen after deletion of cache files: pages re-fetched from page groups.
#[test]
fn test_cache_validation_cold_start_after_cache_delete() {
    let dir = TempDir::new().unwrap();

    // Write data
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("cv_cold1_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let conn = rusqlite::Connection::open(format!("file:test.db?vfs={}", vfs_name)).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'persisted')", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        drop(conn);
    }

    // Delete cache files (simulates Lambda cold start with fresh disk)
    let _ = std::fs::remove_file(dir.path().join("data.cache"));
    let _ = std::fs::remove_file(dir.path().join("page_bitmap"));
    let _ = std::fs::remove_file(dir.path().join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(dir.path().join("cache_index.json"));

    // Reopen: cache empty, manifest still on disk, data from page groups
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("cv_cold2_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let conn = rusqlite::Connection::open(format!("file:test.db?vfs={}", vfs_name)).unwrap();
        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            val, "persisted",
            "should recover data from page groups after cache delete"
        );
    }
}

// ===== Transaction Rollback Handling =====

/// After a constraint violation (failed INSERT), subsequent reads must see
/// the correct data, not stale dirty pages from the rolled-back transaction.
#[test]
fn test_constraint_violation_does_not_corrupt_reads() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs_name = format!("zenith_c_constraint_{}", std::process::id());
    let vfs = TurboliteVfs::new_local(config).expect("VFS");
    crate::tiered::register(&vfs_name, vfs).expect("register");

    let conn = rusqlite::Connection::open(format!("file:test.db?vfs={}", vfs_name)).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT UNIQUE)",
        [],
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'original')", [])
        .unwrap();

    // Force a checkpoint so the data is committed to page groups
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();

    // Try to insert a duplicate value (should fail with UNIQUE constraint)
    let result = conn.execute("INSERT INTO t VALUES (2, 'original')", []);
    assert!(result.is_err(), "duplicate insert should fail");

    // Read should still see only the original row, not corrupted data
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1, "should have exactly 1 row after failed insert");

    let val: String = conn
        .query_row("SELECT val FROM t WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(val, "original");

    // Can still write after the failed transaction
    conn.execute("INSERT INTO t VALUES (2, 'second')", [])
        .unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 2);
}

/// Explicit BEGIN + rollback (via DROP or error) should not leave stale dirty pages.
#[test]
fn test_explicit_transaction_rollback() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs_name = format!("zenith_c_rollback_{}", std::process::id());
    let vfs = TurboliteVfs::new_local(config).expect("VFS");
    crate::tiered::register(&vfs_name, vfs).expect("register");

    let conn = rusqlite::Connection::open(format!("file:test.db?vfs={}", vfs_name)).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'committed')", [])
        .unwrap();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();

    // Begin a transaction, write something, then rollback
    conn.execute_batch("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'will_rollback')", [])
        .unwrap();
    conn.execute_batch("ROLLBACK").unwrap();

    // After rollback, we should see only the committed row
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1, "rolled-back row should not be visible");

    let val: String = conn
        .query_row("SELECT val FROM t WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(val, "committed");
}

/// Multiple constraint violations in a row should not accumulate stale pages.
#[test]
fn test_repeated_constraint_violations() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs_name = format!("zenith_c_repeated_{}", std::process::id());
    let vfs = TurboliteVfs::new_local(config).expect("VFS");
    crate::tiered::register(&vfs_name, vfs).expect("register");

    let conn = rusqlite::Connection::open(format!("file:test.db?vfs={}", vfs_name)).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT UNIQUE)",
        [],
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'one')", []).unwrap();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();

    // Several failing inserts
    for i in 0..5 {
        let result = conn.execute(&format!("INSERT INTO t VALUES ({}, 'one')", i + 10), []);
        assert!(result.is_err(), "duplicate should fail on iteration {}", i);
    }

    // Data should still be correct
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);

    // Successful write after repeated failures
    conn.execute("INSERT INTO t VALUES (2, 'two')", []).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 2);
}

// ===== WAL Migration Path =====

/// turbolite_migrate_to_s3_primary checkpoints WAL and prepares for journal_mode=OFF.
/// The turbolite VFS creates WAL stub files on open (unless S3Primary mode), so
/// the full migration from WAL to OFF requires:
/// 1. Call migrate (checkpoints WAL)
/// 2. Close the connection
/// 3. Reopen with a VFS configured for S3Primary or non-WAL mode
#[test]
fn test_migrate_to_s3_primary_from_wal() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs_name = format!("zenith_d_migrate_{}", std::process::id());
    let vfs = TurboliteVfs::new_local(config).expect("VFS");
    crate::tiered::register(&vfs_name, vfs).expect("register");

    let uri = format!("file:test.db?vfs={}", vfs_name);

    // Step 1: create data in WAL mode, then migrate (checkpoints WAL)
    {
        let conn = rusqlite::Connection::open(&uri).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'before_migrate')", [])
            .unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'second')", [])
            .unwrap();

        crate::tiered::turbolite_migrate_to_s3_primary(&conn).unwrap();
        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        assert!(mode == "off" || mode == "memory", "got: {}", mode);
    }

    // Step 2: verify data survived the checkpoint by reopening (still WAL mode
    // in local VFS, but all data is in the main database file, not the WAL)
    {
        let conn = rusqlite::Connection::open(&uri).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 2, "all rows should survive WAL checkpoint");

        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "before_migrate");
    }
}

/// Migrating a database that is already in DELETE mode switches to OFF directly.
#[test]
fn test_migrate_from_delete_to_off() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs_name = format!("zenith_d_delete_{}", std::process::id());
    let vfs = TurboliteVfs::new_local(config).expect("VFS");
    crate::tiered::register(&vfs_name, vfs).expect("register");

    let uri = format!("file:test.db?vfs={}", vfs_name);
    let conn = rusqlite::Connection::open(&uri).unwrap();
    // Start with DELETE mode (not WAL), then migrate to OFF
    conn.execute_batch("PRAGMA journal_mode=DELETE").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", [])
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1)", []).unwrap();

    // Migrate should succeed and switch to OFF
    crate::tiered::turbolite_migrate_to_s3_primary(&conn).unwrap();

    let mode: String = conn
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert!(mode == "off" || mode == "memory", "got: {}", mode);

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);
}

/// Migration is a no-op when already in journal_mode=OFF.
#[test]
fn test_migrate_already_off() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs_name = format!("zenith_d_already_{}", std::process::id());
    let vfs = TurboliteVfs::new_local(config).expect("VFS");
    crate::tiered::register(&vfs_name, vfs).expect("register");

    let uri = format!("file:test.db?vfs={}", vfs_name);
    let conn = rusqlite::Connection::open(&uri).unwrap();
    conn.execute_batch("PRAGMA journal_mode=DELETE").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", [])
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1)", []).unwrap();

    // First call: switches to OFF
    crate::tiered::turbolite_migrate_to_s3_primary(&conn).unwrap();
    let mode: String = conn
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert!(mode == "off" || mode == "memory", "got: {}", mode);

    // Second call: should be a no-op
    crate::tiered::turbolite_migrate_to_s3_primary(&conn).unwrap();
    let mode2: String = conn
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode, mode2);
}

/// Migration from WAL preserves data across many rows.
#[test]
fn test_migrate_preserves_large_dataset() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs_name = format!("zenith_d_large_{}", std::process::id());
    let vfs = TurboliteVfs::new_local(config).expect("VFS");
    crate::tiered::register(&vfs_name, vfs).expect("register");

    let uri = format!("file:test.db?vfs={}", vfs_name);
    let row_count = 500i64;

    // Create data in WAL mode and migrate
    {
        let conn = rusqlite::Connection::open(&uri).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();

        for i in 0..row_count {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("row_{}", i)],
            )
            .unwrap();
        }

        crate::tiered::turbolite_migrate_to_s3_primary(&conn).unwrap();
        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        assert!(mode == "off" || mode == "memory", "got: {}", mode);
    }

    // Verify all data survived
    {
        let conn = rusqlite::Connection::open(&uri).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, row_count);

        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 250", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "row_250");

        // Can still write after migration
        conn.execute(
            "INSERT INTO t VALUES (?1, 'new')",
            rusqlite::params![row_count],
        )
        .unwrap();
        let new_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(new_count, row_count + 1);
    }
}

// =========================================================================
// Stress tests
// =========================================================================

/// Large database: 10k rows across many page groups, checkpoint, cold reopen, verify all rows.
#[test]
fn test_stress_large_database_10k_rows() {
    let dir = TempDir::new().unwrap();
    let row_count = 10_000i64;

    // Write phase
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            // small groups to exercise many page groups
            cache: CacheConfig {
                pages_per_group: 4,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("stress_10k_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, grp INTEGER, val TEXT)",
            [],
        )
        .unwrap();

        // Insert in a single transaction for speed
        conn.execute_batch("BEGIN").unwrap();
        for i in 0..row_count {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2, ?3)",
                rusqlite::params![i, i % 100, format!("row_{}", i)],
            )
            .unwrap();
        }
        conn.execute_batch("COMMIT").unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        drop(conn);
    }

    // Cold reopen: delete cache files
    let _ = std::fs::remove_file(dir.path().join("data.cache"));
    let _ = std::fs::remove_file(dir.path().join("page_bitmap"));
    let _ = std::fs::remove_file(dir.path().join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(dir.path().join("cache_index.json"));

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                pages_per_group: 4,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("stress_10k_r_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, row_count, "all 10k rows should survive cold reopen");

        // Spot-check a few rows
        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 0", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "row_0");

        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 5000", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "row_5000");

        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 9999", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "row_9999");

        // Group query
        let grp_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t WHERE grp = 42", [], |r| r.get(0))
            .unwrap();
        assert_eq!(grp_count, 100, "each of 100 groups should have 100 rows");
    }
}

/// Many sequential overrides then compaction: verify compaction fires and data is correct.
#[test]
fn test_stress_many_overrides_compaction() {
    use std::sync::atomic::Ordering;
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);

    let dir = TempDir::new().unwrap();

    // Write phase: initial data + 10 sequential small updates with checkpoints
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            // compact after 5 overrides
            cache: CacheConfig {
                override_threshold: 100,
                compaction_threshold: 5,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("stress_ovr_cmpct_w_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();

        // Initial bulk insert
        for i in 1..=50 {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("initial_{}", i)],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        // 10 sequential small updates, each with a checkpoint (creates overrides)
        for round in 1..=10 {
            conn.execute(
                "UPDATE t SET val = ?1 WHERE id = ?2",
                rusqlite::params![format!("round_{}", round), round],
            )
            .unwrap();
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
                .unwrap();
        }

        drop(conn);
    }

    // Cold reopen: delete cache files
    let _ = std::fs::remove_file(dir.path().join("data.cache"));
    let _ = std::fs::remove_file(dir.path().join("page_bitmap"));
    let _ = std::fs::remove_file(dir.path().join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(dir.path().join("cache_index.json"));

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                override_threshold: 100,
                compaction_threshold: 5,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("stress_ovr_cmpct_r_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");

        // Rows 1-10 should have latest update values
        for i in 1..=10 {
            let val: String = conn
                .query_row(
                    "SELECT val FROM t WHERE id = ?1",
                    rusqlite::params![i],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                val,
                format!("round_{}", i),
                "row {} should have latest override value",
                i
            );
        }

        // Rows 11-50 should still have initial values
        for i in 11..=50 {
            let val: String = conn
                .query_row(
                    "SELECT val FROM t WHERE id = ?1",
                    rusqlite::params![i],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                val,
                format!("initial_{}", i),
                "row {} should retain initial value",
                i
            );
        }

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 50);
    }
}

/// Override + compression combined: both features active simultaneously.
#[test]
fn test_override_with_compression_roundtrip() {
    use std::sync::atomic::Ordering;
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);

    let dir = TempDir::new().unwrap();

    // Write phase: create base groups with compression + overrides
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            // disable compaction to keep overrides visible
            cache: CacheConfig {
                compression: true,
                compression_level: 3,
                override_threshold: 100,
                compaction_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("ovr_cmp_w_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();

        // Initial data - creates base groups
        for i in 1..=20 {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("base_{}", i)],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        // Update subset - creates overrides
        for i in 1..=5 {
            conn.execute(
                "UPDATE t SET val = ?1 WHERE id = ?2",
                rusqlite::params![format!("updated_{}", i), i],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        drop(conn);
    }

    // Cold reopen with cache deleted
    let _ = std::fs::remove_file(dir.path().join("data.cache"));
    let _ = std::fs::remove_file(dir.path().join("page_bitmap"));
    let _ = std::fs::remove_file(dir.path().join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(dir.path().join("cache_index.json"));

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                compression: true,
                compression_level: 3,
                override_threshold: 100,
                compaction_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("ovr_cmp_r_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");

        // Updated rows should have new values
        for i in 1..=5 {
            let val: String = conn
                .query_row(
                    "SELECT val FROM t WHERE id = ?1",
                    rusqlite::params![i],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(val, format!("updated_{}", i));
        }

        // Non-updated rows should have base values
        for i in 6..=20 {
            let val: String = conn
                .query_row(
                    "SELECT val FROM t WHERE id = ?1",
                    rusqlite::params![i],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(val, format!("base_{}", i));
        }

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 20);
    }
}

/// Rapid checkpoint cycles: 50 iterations of insert 1 row + checkpoint.
#[test]
fn test_stress_rapid_checkpoint_cycles() {
    let dir = TempDir::new().unwrap();
    let iterations = 50;

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                override_threshold: 100,
                compaction_threshold: 10,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("stress_rapid_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        for i in 1..=iterations {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("rapid_{}", i)],
            )
            .unwrap();
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
                .unwrap();
        }

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, iterations);

        drop(conn);
    }

    // Cold reopen
    let _ = std::fs::remove_file(dir.path().join("data.cache"));
    let _ = std::fs::remove_file(dir.path().join("page_bitmap"));
    let _ = std::fs::remove_file(dir.path().join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(dir.path().join("cache_index.json"));

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("stress_rapid_r_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            count, iterations,
            "all {} rows should survive rapid checkpoint cycles",
            iterations
        );

        // Spot check first and last
        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "rapid_1");

        let val: String = conn
            .query_row(
                "SELECT val FROM t WHERE id = ?1",
                rusqlite::params![iterations],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(val, format!("rapid_{}", iterations));
    }
}

// =========================================================================
// Edge case tests
// =========================================================================

/// Empty database: create schema, checkpoint, cold reopen, verify table exists with 0 rows.
#[test]
fn test_edge_empty_database_reopen() {
    let dir = TempDir::new().unwrap();

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("edge_empty_w_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        drop(conn);
    }

    // Cold reopen
    let _ = std::fs::remove_file(dir.path().join("data.cache"));
    let _ = std::fs::remove_file(dir.path().join("page_bitmap"));
    let _ = std::fs::remove_file(dir.path().join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(dir.path().join("cache_index.json"));

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("edge_empty_r_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");

        // Table should exist
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "empty table should have 0 rows after cold reopen");

        // Should be able to insert after cold reopen
        conn.execute("INSERT INTO t VALUES (1, 'after_reopen')", [])
            .unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }
}

/// Single row database: insert 1 row, checkpoint, cold reopen, verify.
#[test]
fn test_edge_single_row_database() {
    let dir = TempDir::new().unwrap();

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("edge_single_w_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'only_row')", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        drop(conn);
    }

    // Cold reopen
    let _ = std::fs::remove_file(dir.path().join("data.cache"));
    let _ = std::fs::remove_file(dir.path().join("page_bitmap"));
    let _ = std::fs::remove_file(dir.path().join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(dir.path().join("cache_index.json"));

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let vfs_name = format!("edge_single_r_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);

        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "only_row");
    }
}

/// Group boundary: with small pages_per_group, insert enough data to land exactly
/// on a group boundary. Verify data integrity across the boundary.
#[test]
fn test_edge_group_boundary_pages() {
    let dir = TempDir::new().unwrap();
    let pages_per_group = 4u32;

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                pages_per_group,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("edge_grpbnd_w_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT, padding TEXT)",
            [],
        )
        .unwrap();

        // Insert enough rows to span multiple groups with small pages_per_group=4.
        // Each 4KB page holds a limited number of rows; ~200 rows should span many groups.
        for i in 0..200 {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2, ?3)",
                rusqlite::params![i, format!("val_{}", i), "x".repeat(100)],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        // Update last page of one group and first page of next by updating scattered rows
        conn.execute("UPDATE t SET val = 'boundary_first' WHERE id = 0", [])
            .unwrap();
        conn.execute("UPDATE t SET val = 'boundary_last' WHERE id = 199", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        drop(conn);
    }

    // Cold reopen
    let _ = std::fs::remove_file(dir.path().join("data.cache"));
    let _ = std::fs::remove_file(dir.path().join("page_bitmap"));
    let _ = std::fs::remove_file(dir.path().join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(dir.path().join("cache_index.json"));

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                pages_per_group,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("edge_grpbnd_r_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 200);

        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 0", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "boundary_first");

        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 199", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "boundary_last");

        // Middle rows should be intact
        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 100", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "val_100");
    }
}

/// Override at frame boundary: with sub_pages_per_frame=4, update pages that span
/// two frames. Verify both override frames are created and readable.
#[test]
fn test_edge_override_at_frame_boundary() {
    use std::sync::atomic::Ordering;
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);

    let dir = TempDir::new().unwrap();

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            // 8 pages per group; 2 frames per group
            cache: CacheConfig {
                pages_per_group: 8,
                sub_pages_per_frame: 4,
                override_threshold: 100,
                compaction_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("edge_ovr_frame_w_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT, padding TEXT)",
            [],
        )
        .unwrap();

        // Insert enough data to span multiple groups/frames
        for i in 0..300 {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2, ?3)",
                rusqlite::params![i, format!("base_{}", i), "p".repeat(80)],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        // Update rows scattered across different pages to hit multiple frames
        for i in (0..300).step_by(25) {
            conn.execute(
                "UPDATE t SET val = ?1 WHERE id = ?2",
                rusqlite::params![format!("frame_ovr_{}", i), i],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        drop(conn);
    }

    // Cold reopen
    let _ = std::fs::remove_file(dir.path().join("data.cache"));
    let _ = std::fs::remove_file(dir.path().join("page_bitmap"));
    let _ = std::fs::remove_file(dir.path().join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(dir.path().join("cache_index.json"));

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                pages_per_group: 8,
                sub_pages_per_frame: 4,
                override_threshold: 100,
                compaction_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("edge_ovr_frame_r_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 300);

        // Verify overridden rows
        for i in (0..300).step_by(25) {
            let val: String = conn
                .query_row(
                    "SELECT val FROM t WHERE id = ?1",
                    rusqlite::params![i],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                val,
                format!("frame_ovr_{}", i),
                "override row {} mismatch",
                i
            );
        }

        // Verify non-overridden rows
        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "base_1");
    }
}

/// Rollback with dirty pages in multiple groups: begin transaction, update pages
/// across groups, ROLLBACK, verify all data reverts.
#[test]
fn test_rollback_multi_group_dirty_pages() {
    let dir = TempDir::new().unwrap();

    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        cache: CacheConfig {
            pages_per_group: 4,
            ..Default::default()
        },
        ..Default::default()
    };
    let vfs_name = format!("edge_rollback_mg_{}", std::process::id());
    let vfs = TurboliteVfs::new_local(config).expect("local VFS");
    crate::tiered::register(&vfs_name, vfs).expect("register");

    let db_path = format!("file:test.db?vfs={}", vfs_name);
    let conn = rusqlite::Connection::open(&db_path).expect("open");
    conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT, padding TEXT)",
        [],
    )
    .unwrap();

    // Insert enough data to span multiple groups (pages_per_group=4)
    for i in 0..200 {
        conn.execute(
            "INSERT INTO t VALUES (?1, ?2, ?3)",
            rusqlite::params![i, format!("committed_{}", i), "x".repeat(80)],
        )
        .unwrap();
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();

    // Begin transaction, update rows across many groups, then ROLLBACK
    conn.execute_batch("BEGIN").unwrap();
    for i in (0..200).step_by(10) {
        conn.execute(
            "UPDATE t SET val = ?1 WHERE id = ?2",
            rusqlite::params!["SHOULD_NOT_EXIST", i],
        )
        .unwrap();
    }
    conn.execute_batch("ROLLBACK").unwrap();

    // All rows should have original values
    for i in (0..200).step_by(10) {
        let val: String = conn
            .query_row(
                "SELECT val FROM t WHERE id = ?1",
                rusqlite::params![i],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            val,
            format!("committed_{}", i),
            "row {} should revert after rollback",
            i
        );
    }

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 200, "row count should be unchanged after rollback");

    // Verify we can still write after rollback
    conn.execute("INSERT INTO t VALUES (999, 'after_rollback', 'ok')", [])
        .unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 201);
}

/// Override then delete rows: insert, checkpoint (base), delete half, checkpoint (overrides),
/// cold reopen, verify only remaining rows exist.
#[test]
fn test_override_then_delete_rows() {
    use std::sync::atomic::Ordering;
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);

    let dir = TempDir::new().unwrap();

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                override_threshold: 100,
                compaction_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("edge_ovr_del_w_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();

        // Insert 100 rows, checkpoint (base groups)
        for i in 1..=100 {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("row_{}", i)],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        // Delete odd rows (50 rows), checkpoint (creates overrides for pages with deletions)
        conn.execute("DELETE FROM t WHERE id % 2 = 1", []).unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        drop(conn);
    }

    // Cold reopen
    let _ = std::fs::remove_file(dir.path().join("data.cache"));
    let _ = std::fs::remove_file(dir.path().join("page_bitmap"));
    let _ = std::fs::remove_file(dir.path().join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(dir.path().join("cache_index.json"));

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                override_threshold: 100,
                compaction_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("edge_ovr_del_r_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 50, "only even rows should remain");

        // Odd rows should not exist
        let odd_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t WHERE id % 2 = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(odd_count, 0, "all odd rows should be deleted");

        // Even rows should still exist
        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 2", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "row_2");

        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 100", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "row_100");
    }
}

/// Compaction with threshold=1: compact after every single override.
#[test]
fn test_compaction_threshold_one() {
    use std::sync::atomic::Ordering;
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);

    let dir = TempDir::new().unwrap();

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            // compact after every single override
            cache: CacheConfig {
                override_threshold: 100,
                compaction_threshold: 1,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("edge_cmpct1_w_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();

        // Initial data
        for i in 1..=10 {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("init_{}", i)],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        // Update + checkpoint (should trigger immediate compaction)
        conn.execute("UPDATE t SET val = 'compacted_1' WHERE id = 1", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        // Another update + checkpoint
        conn.execute("UPDATE t SET val = 'compacted_2' WHERE id = 2", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();

        drop(conn);
    }

    // Cold reopen
    let _ = std::fs::remove_file(dir.path().join("data.cache"));
    let _ = std::fs::remove_file(dir.path().join("page_bitmap"));
    let _ = std::fs::remove_file(dir.path().join("sub_chunk_tracker"));
    let _ = std::fs::remove_file(dir.path().join("cache_index.json"));

    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                override_threshold: 100,
                compaction_threshold: 1,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("edge_cmpct1_r_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("reopen");

        let val1: String = conn
            .query_row("SELECT val FROM t WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val1, "compacted_1");

        let val2: String = conn
            .query_row("SELECT val FROM t WHERE id = 2", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val2, "compacted_2");

        // Other rows untouched
        let val5: String = conn
            .query_row("SELECT val FROM t WHERE id = 5", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val5, "init_5");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 10);
    }
}

/// Cache validation with override changes between sessions:
/// Session 1 writes base, Session 2 updates with overrides, Session 3 reopens and
/// should see correct data from base + overrides.
#[test]
fn test_cache_validation_override_changes_between_sessions() {
    use std::sync::atomic::Ordering;
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);

    let dir = TempDir::new().unwrap();

    // Session 1: write base data + checkpoint
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                override_threshold: 100,
                compaction_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("cv_ovr_s1_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("local VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
            .unwrap();
        for i in 1..=10 {
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("session1_{}", i)],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        drop(conn);
    }

    // Session 2: update with overrides + checkpoint
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                override_threshold: 100,
                compaction_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("cv_ovr_s2_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS");
        crate::tiered::register(&vfs_name, vfs).expect("register2");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open session 2");
        conn.execute("UPDATE t SET val = 'session2_updated' WHERE id <= 3", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        drop(conn);
    }

    // Session 3: reopen (cache validation should detect override changes)
    {
        let config = TurboliteConfig {
            cache_dir: dir.path().to_path_buf(),
            cache: CacheConfig {
                override_threshold: 100,
                compaction_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let vfs_name = format!("cv_ovr_s3_{}", id);
        let vfs = TurboliteVfs::new_local(config).expect("reopen VFS session 3");
        crate::tiered::register(&vfs_name, vfs).expect("register3");

        let db_path = format!("file:test.db?vfs={}", vfs_name);
        let conn = rusqlite::Connection::open(&db_path).expect("open session 3");

        // Rows 1-3 should have session 2 updates
        for i in 1..=3 {
            let val: String = conn
                .query_row(
                    "SELECT val FROM t WHERE id = ?1",
                    rusqlite::params![i],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                val, "session2_updated",
                "row {} should have session 2 value",
                i
            );
        }

        // Rows 4-10 should still have session 1 values
        for i in 4..=10 {
            let val: String = conn
                .query_row(
                    "SELECT val FROM t WHERE id = ?1",
                    rusqlite::params![i],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                val,
                format!("session1_{}", i),
                "row {} should retain session 1 value",
                i
            );
        }

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 10);
    }
}

// =========================================================================
// Phase Cirrus c: per-handle `turbolite_config_set` integration tests
//
// Unit tests in `settings.rs` cover queue/stack routing mechanics. These
// exercise the full path: open a VFS-backed connection, push a setting
// via `settings::set()` (same code path as the C FFI), run a query,
// confirm the drain applies without crashing and without poisoning other
// handles on the thread.
// =========================================================================

#[test]
fn test_settings_set_round_trip() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };

    let vfs_name = format!("settings_rt_{}", std::process::id());
    let vfs = TurboliteVfs::new_local(config).expect("local VFS");
    crate::tiered::register(&vfs_name, vfs).expect("register");

    let db_path = format!("file:test.db?vfs={}", vfs_name);
    let conn = rusqlite::Connection::open(&db_path).expect("open");
    conn.execute_batch(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
         INSERT INTO t VALUES (1, 'hello'), (2, 'world');",
    )
    .unwrap();

    // Main-db handle is registered on the thread-local stack.
    crate::tiered::settings::set("prefetch_search", "0.5,0.5,0.0")
        .expect("set prefetch_search on active handle");
    crate::tiered::settings::set("prefetch_lookup", "0.0,0.0,0.0")
        .expect("set prefetch_lookup on active handle");
    crate::tiered::settings::set("plan_aware", "true").expect("set plan_aware on active handle");

    // Next read drains the queue. No observable field from outside for
    // local VFS, but the drain path must not crash and queries must
    // continue to work.
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 2);

    crate::tiered::settings::set("prefetch_reset", "").expect("reset");
    let v: String = conn
        .query_row("SELECT v FROM t WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(v, "hello");
}

#[test]
fn test_settings_set_without_handle_errors() {
    // No turbolite connection on this thread; push must fail loudly
    // rather than silently drop.
    let r = crate::tiered::settings::set("prefetch_search", "0.3,0.3,0.4");
    assert!(r.is_err(), "expected error with no active handle, got Ok");
}

#[test]
fn test_settings_set_per_connection_isolation() {
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let cfg_a = TurboliteConfig {
        cache_dir: dir_a.path().to_path_buf(),
        ..Default::default()
    };
    let cfg_b = TurboliteConfig {
        cache_dir: dir_b.path().to_path_buf(),
        ..Default::default()
    };

    let vfs_a_name = format!("settings_iso_a_{}", std::process::id());
    let vfs_b_name = format!("settings_iso_b_{}", std::process::id());
    crate::tiered::register(&vfs_a_name, TurboliteVfs::new_local(cfg_a).expect("vfs A"))
        .expect("reg A");
    crate::tiered::register(&vfs_b_name, TurboliteVfs::new_local(cfg_b).expect("vfs B"))
        .expect("reg B");

    let conn_a =
        rusqlite::Connection::open(format!("file:a.db?vfs={}", vfs_a_name)).expect("open A");
    conn_a
        .execute_batch("CREATE TABLE a (id INTEGER); INSERT INTO a VALUES (1);")
        .unwrap();

    let conn_b =
        rusqlite::Connection::open(format!("file:b.db?vfs={}", vfs_b_name)).expect("open B");
    conn_b
        .execute_batch("CREATE TABLE b (id INTEGER); INSERT INTO b VALUES (2);")
        .unwrap();

    // B is on top; push lands on B's queue.
    crate::tiered::settings::set("prefetch_search", "1.0").expect("set while B on top");

    // Close B → A back on top.
    drop(conn_b);

    // Push now lands on A, not the dropped B.
    crate::tiered::settings::set("prefetch_search", "0.5,0.5").expect("set while A on top");

    // A's next read drains A's queue, query still works.
    let n: i64 = conn_a
        .query_row("SELECT COUNT(*) FROM a", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 1);
}

/// Migration gate: drive a real SQLite database through the *sqlite-plugin*
/// backend of the tiered VFS end-to-end. journal_mode=MEMORY keeps the rollback
/// journal in RAM, so this needs no journal companion and no shared memory —
/// it isolates the file-op delegation (open → full lock ladder → write → read
/// → sync → file_size) from the WAL shared-memory work that lands next.
#[test]
fn plugin_vfs_crud_memory_journal() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs_name = format!("plugin_crud_mem_{}", std::process::id());
    let vfs = TurboliteVfs::new_local(config).expect("local VFS");
    crate::tiered::register_plugin(&vfs_name, vfs).expect("register plugin vfs");

    let db_path = format!("file:plugin_mem.db?vfs={}", vfs_name);
    let conn = rusqlite::Connection::open(&db_path).expect("open through plugin vfs");
    conn.execute_batch("PRAGMA journal_mode=MEMORY").unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'hello')", [])
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'world')", [])
        .unwrap();

    let n: i64 = conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 2, "rows visible through sqlite-plugin tiered VFS");
    let v: String = conn
        .query_row("SELECT val FROM t WHERE id = 2", [], |r| r.get(0))
        .unwrap();
    assert_eq!(v, "world", "row content round-trips through the plugin VFS");

    // A transaction that rolls back must leave no trace — exercises the
    // lock upgrade/downgrade path (Shared → Reserved → Exclusive → Shared).
    conn.execute_batch("BEGIN; INSERT INTO t VALUES (3, 'gone'); ROLLBACK;")
        .unwrap();
    let n: i64 = conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 2, "rolled-back insert is not visible");
}

/// DELETE-journal gate: SQLite's default mode keeps the rollback journal as an
/// on-disk `-journal` companion file. This proves that companion path works
/// through the plugin VFS — open/write/sync/delete of the journal file, and a
/// real ROLLBACK that reads it back — so WAL is not silently load-bearing.
#[test]
fn plugin_vfs_crud_delete_journal() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs_name = format!("plugin_crud_delete_{}", std::process::id());
    let vfs = TurboliteVfs::new_local(config).expect("local VFS");
    crate::tiered::register_plugin(&vfs_name, vfs).expect("register plugin vfs");

    let db_path = format!("file:plugin_delete.db?vfs={}", vfs_name);
    let conn = rusqlite::Connection::open(&db_path).expect("open through plugin vfs");
    let mode: String = conn
        .query_row("PRAGMA journal_mode=DELETE", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        mode, "delete",
        "DELETE journal mode engaged via the plugin VFS"
    );

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'hello')", [])
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'world')", [])
        .unwrap();

    let n: i64 = conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 2, "rows visible through DELETE-mode plugin VFS");

    // A rolled-back transaction must restore the prior image from the on-disk
    // rollback journal — the companion-file open/write/read/delete path.
    conn.execute_batch(
        "BEGIN; UPDATE t SET val='changed' WHERE id=1; INSERT INTO t VALUES (3, 'gone'); ROLLBACK;",
    )
    .unwrap();
    let n: i64 = conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 2, "rolled-back insert is not visible");
    let v: String = conn
        .query_row("SELECT val FROM t WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        v, "hello",
        "rolled-back update is reverted from the journal"
    );

    // Committed data survives reopening the connection.
    drop(conn);
    let conn = rusqlite::Connection::open(&db_path).expect("reopen through plugin vfs");
    let n: i64 = conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 2, "committed rows persist across reopen in DELETE mode");
}

/// WAL gate: same CRUD but in journal_mode=WAL, which drives the live-pointer
/// shared memory (`shm_map`/`shm_lock`/`shm_barrier`/`shm_unmap`) end-to-end.
/// A passing WAL checkpoint proves the WAL-index is wired correctly.
#[test]
fn plugin_vfs_crud_wal() {
    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs_name = format!("plugin_crud_wal_{}", std::process::id());
    let vfs = TurboliteVfs::new_local(config).expect("local VFS");
    crate::tiered::register_plugin(&vfs_name, vfs).expect("register plugin vfs");

    let db_path = format!("file:plugin_wal.db?vfs={}", vfs_name);
    let conn = rusqlite::Connection::open(&db_path).expect("open through plugin vfs");

    let mode: String = conn
        .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode, "wal", "WAL mode engaged through the plugin VFS");

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", [])
        .unwrap();
    for i in 0..50 {
        conn.execute("INSERT INTO t (val) VALUES (?1)", [format!("row-{i}")])
            .unwrap();
    }

    let n: i64 = conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 50, "rows visible through WAL-mode plugin VFS");

    // Force a checkpoint: drains the WAL back into the main db, exercising the
    // exclusive WAL-index locks + shared-memory reads.
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();

    let v: String = conn
        .query_row("SELECT val FROM t WHERE id = 50", [], |r| r.get(0))
        .unwrap();
    assert_eq!(v, "row-49", "content survives checkpoint");
}

/// The test the migration exists for: concurrent readers against a live writer
/// in WAL mode, checking an invariant that a torn WAL-index read would break.
///
/// The writer commits `INSERT INTO ta(k); INSERT INTO tb(k)` as one
/// transaction, so `SUM(ta) - SUM(tb)` is always 0 at any commit boundary. Each
/// reader evaluates that difference in a single statement; transaction
/// isolation must show it both inserts or neither. With the old copy-based
/// WAL-index a reader could pull a torn index header, compute the wrong frame
/// bound, and observe a half-applied commit (non-zero) or `DatabaseCorrupt`.
/// The live-pointer shm has no copy to tear, so every read must see 0.
#[test]
fn plugin_vfs_concurrent_wal_isolation() {
    use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
    use std::sync::Arc;

    let dir = TempDir::new().unwrap();
    let config = TurboliteConfig {
        cache_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    let vfs_name = format!("plugin_iso_{}", std::process::id());
    let vfs = TurboliteVfs::new_local(config).expect("local VFS");
    crate::tiered::register_plugin(&vfs_name, vfs).expect("register plugin vfs");
    let db_uri = format!("file:plugin_iso.db?vfs={}", vfs_name);

    // Seed: WAL mode + the two tables.
    {
        let conn = rusqlite::Connection::open(&db_uri).expect("seed open");
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let mode: String = conn
            .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
            .unwrap();
        assert_eq!(mode, "wal");
        conn.execute_batch("CREATE TABLE ta (k INTEGER); CREATE TABLE tb (k INTEGER);")
            .unwrap();
    }

    const READERS: usize = 16;
    const WRITES: i64 = 400;

    let stop = Arc::new(AtomicBool::new(false));
    let partial_reads = Arc::new(AtomicI64::new(0));
    let reads_done = Arc::new(AtomicI64::new(0));

    let mut handles = Vec::new();
    for _ in 0..READERS {
        let uri = db_uri.clone();
        let stop = Arc::clone(&stop);
        let partial = Arc::clone(&partial_reads);
        let done = Arc::clone(&reads_done);
        handles.push(std::thread::spawn(move || {
            let conn = rusqlite::Connection::open(&uri).expect("reader open");
            conn.busy_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            while !stop.load(Ordering::Relaxed) {
                let diff: i64 = conn
                    .query_row(
                        "SELECT (SELECT COALESCE(SUM(k),0) FROM ta) \
                              - (SELECT COALESCE(SUM(k),0) FROM tb)",
                        [],
                        |r| r.get(0),
                    )
                    .expect("reader query must not error (no DatabaseCorrupt)");
                if diff != 0 {
                    partial.fetch_add(1, Ordering::Relaxed);
                }
                done.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // Writer: one connection, WRITES atomic transactions, periodic checkpoints.
    let writer = {
        let uri = db_uri.clone();
        std::thread::spawn(move || {
            let conn = rusqlite::Connection::open(&uri).expect("writer open");
            conn.busy_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            for k in 1..=WRITES {
                conn.execute_batch(&format!(
                    "BEGIN; INSERT INTO ta(k) VALUES({k}); INSERT INTO tb(k) VALUES({k}); COMMIT;"
                ))
                .expect("writer txn");
                if k % 50 == 0 {
                    let _ = conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE);");
                }
            }
        })
    };

    writer.join().expect("writer thread");
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().expect("reader thread");
    }

    assert!(
        reads_done.load(Ordering::Relaxed) > 0,
        "readers must have actually run"
    );
    assert_eq!(
        partial_reads.load(Ordering::Relaxed),
        0,
        "no reader may observe a half-applied commit (torn WAL-index)"
    );

    // Final state is consistent and complete.
    let conn = rusqlite::Connection::open(&db_uri).expect("final open");
    let total_ta: i64 = conn
        .query_row("SELECT COALESCE(SUM(k),0) FROM ta", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        total_ta,
        WRITES * (WRITES + 1) / 2,
        "every committed write is durable"
    );
}

/// Cross-PROCESS WAL isolation: closes hostile-review finding #4 (the in-process
/// isolation test only exercises `IN_PROCESS_LOCKS`; this exercises the
/// `MAP_SHARED` `-shm` + fcntl file locks across real OS processes).
///
/// Uses the re-exec pattern: when `TURBOLITE_XPROC_ROLE` is set we're a spawned
/// worker — do the role's work and `process::exit`. The parent seeds a WAL db,
/// spawns one writer + several reader processes against the same file-first db,
/// and checks (a) no reader process ever observes a half-applied commit
/// (`SUM(ta)-SUM(tb) != 0`) or a corrupt read, and (b) a fresh process sees all
/// committed writes afterward (cross-process durability + final consistency).
#[test]
fn plugin_vfs_cross_process_wal_isolation() {
    use std::time::{Duration, Instant};

    const XWRITES: i64 = 300;
    let total_expected = XWRITES * (XWRITES + 1) / 2;

    // ── Worker mode (a spawned child) ────────────────────────────────────
    if let Ok(role) = std::env::var("TURBOLITE_XPROC_ROLE") {
        let dir = std::path::PathBuf::from(std::env::var("TURBOLITE_XPROC_DIR").unwrap());
        let db_path = dir.join("x.db");
        let vfs_name = format!("xproc_{}_{}", role, std::process::id());
        let config = TurboliteConfig::for_database_path(&db_path);
        let code: i32 = (|| {
            let vfs = TurboliteVfs::new_local(config).map_err(|_| 4)?;
            crate::tiered::register(&vfs_name, vfs).map_err(|_| 4)?;
            let uri = format!("file:{}?vfs={}", db_path.display(), vfs_name);
            let conn = rusqlite::Connection::open(&uri).map_err(|_| 4)?;
            conn.busy_timeout(Duration::from_secs(20)).map_err(|_| 4)?;
            // Ensure WAL is engaged on this connection.
            let mode: String = conn
                .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
                .map_err(|_| 4)?;
            if mode != "wal" {
                return Err(4);
            }
            let done = dir.join("writer_done");
            let invariant = |c: &rusqlite::Connection| -> Result<i64, i32> {
                c.query_row(
                    "SELECT (SELECT COALESCE(SUM(k),0) FROM ta) \
                          - (SELECT COALESCE(SUM(k),0) FROM tb)",
                    [],
                    |r| r.get::<_, i64>(0),
                )
                .map_err(|_| 3)
            };
            match role.as_str() {
                "writer" => {
                    for k in 1..=XWRITES {
                        conn.execute_batch(&format!(
                            "BEGIN IMMEDIATE; INSERT INTO ta(k) VALUES({k}); \
                             INSERT INTO tb(k) VALUES({k}); COMMIT;"
                        ))
                        .map_err(|_| 2)?;
                    }
                    std::fs::write(&done, b"1").map_err(|_| 2)?;
                    Ok(0)
                }
                "reader" => {
                    let deadline = Instant::now() + Duration::from_secs(30);
                    loop {
                        if invariant(&conn)? != 0 {
                            return Err(1); // torn read — a half-applied commit
                        }
                        if done.exists() {
                            // Drain a few more snapshots after the writer finished.
                            for _ in 0..100 {
                                if invariant(&conn)? != 0 {
                                    return Err(1);
                                }
                            }
                            return Ok(0);
                        }
                        if Instant::now() > deadline {
                            return Ok(0);
                        }
                    }
                }
                _ => Err(5),
            }
        })()
        .unwrap_or_else(|e| e);
        std::process::exit(code);
    }

    // ── Parent ───────────────────────────────────────────────────────────
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("x.db");
    {
        let vfs_name = format!("xproc_seed_{}", std::process::id());
        let vfs = TurboliteVfs::new_local(TurboliteConfig::for_database_path(&db_path))
            .expect("seed vfs");
        crate::tiered::register(&vfs_name, vfs).expect("seed register");
        let uri = format!("file:{}?vfs={}", db_path.display(), vfs_name);
        let conn = rusqlite::Connection::open(&uri).expect("seed open");
        let mode: String = conn
            .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            mode, "wal",
            "WAL must engage for cross-process coordination"
        );
        conn.execute_batch("CREATE TABLE ta (k INTEGER); CREATE TABLE tb (k INTEGER);")
            .unwrap();
    }

    let exe = std::env::current_exe().unwrap();
    let test_name = "tiered::vfs::local_vfs_tests::plugin_vfs_cross_process_wal_isolation";
    let spawn = |role: &str| {
        std::process::Command::new(&exe)
            .args([test_name, "--exact", "--nocapture", "--test-threads=1"])
            .env("TURBOLITE_XPROC_ROLE", role)
            .env("TURBOLITE_XPROC_DIR", dir.path())
            .spawn()
            .expect("spawn worker")
    };

    let writer = spawn("writer");
    let readers: Vec<_> = (0..3).map(|_| spawn("reader")).collect();

    let writer_code = writer
        .wait_with_output()
        .unwrap()
        .status
        .code()
        .unwrap_or(-1);
    assert_eq!(
        writer_code, 0,
        "writer process failed (code {writer_code}: 2=txn 4=setup)"
    );
    for (i, r) in readers.into_iter().enumerate() {
        let code = r.wait_with_output().unwrap().status.code().unwrap_or(-1);
        assert_eq!(
            code, 0,
            "reader {i} failed (code {code}: 1=TORN READ, 3=db error, 4=setup)"
        );
    }

    // Cross-process durability + final consistency: a fresh process/cache sees
    // every committed write and a balanced final state.
    let vfs_name = format!("xproc_final_{}", std::process::id());
    let vfs =
        TurboliteVfs::new_local(TurboliteConfig::for_database_path(&db_path)).expect("final vfs");
    crate::tiered::register(&vfs_name, vfs).expect("final register");
    let conn = rusqlite::Connection::open(format!("file:{}?vfs={}", db_path.display(), vfs_name))
        .expect("final open");
    let (sum_ta, diff): (i64, i64) = conn
        .query_row(
            "SELECT (SELECT COALESCE(SUM(k),0) FROM ta), \
                    (SELECT COALESCE(SUM(k),0) FROM ta) - (SELECT COALESCE(SUM(k),0) FROM tb)",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("final read");
    assert_eq!(diff, 0, "final state must be balanced");
    assert_eq!(
        sum_ta, total_expected,
        "a fresh process must see all cross-process-committed writes"
    );
}
