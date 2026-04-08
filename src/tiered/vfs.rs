use super::*;

// ===== TurboliteVfs =====

/// Turbolite SQLite VFS with compression, encryption, and optional S3 backing.
///
/// Works in two modes based on [`StorageBackend`]:
/// - **Local** (default): page groups stored on local disk. No cloud deps.
/// - **Cloud**: S3 is the source of truth, local disk is a cache. Requires `cloud` feature.
///
/// # Local mode
/// ```ignore
/// use turbolite::{TurboliteVfs, TurboliteConfig};
///
/// let config = TurboliteConfig {
///     cache_dir: "/data/mydb".into(),
///     ..Default::default()
/// };
/// let vfs = TurboliteVfs::new(config)?;
/// turbolite::tiered::register("mydb", vfs)?;
/// ```
pub struct TurboliteVfs {
    /// Unified storage client: Local (filesystem) or S3.
    pub(crate) storage: Arc<StorageClient>,
    /// S3 client (only set in cloud mode). Kept separate because PrefetchPool
    /// and flush_to_s3 need Arc<S3Client> directly.
    s3: Option<Arc<S3Client>>,
    cache: Arc<DiskCache>,
    prefetch_pool: Option<Arc<PrefetchPool>>,
    /// Shared page_count for prefetch workers (kept alive by PrefetchPool workers).
    #[allow(dead_code)]
    page_count: Arc<AtomicU64>,
    config: TurboliteConfig,
    /// Owned runtime (if we created one ourselves)
    #[cfg(feature = "cloud")]
    _runtime: Option<tokio::runtime::Runtime>,
    /// Shared manifest state. Written by TurboliteHandle during sync/checkpoint,
    /// read by flush_to_s3() for non-blocking S3 upload.
    shared_manifest: Arc<RwLock<Manifest>>,
    /// Shared pending S3 groups. Accumulated by TurboliteHandle during local-only
    /// checkpoints, drained by flush_to_s3(). Legacy path for global
    /// LOCAL_CHECKPOINT_ONLY flag; SyncMode::LocalThenFlush uses staging logs.
    shared_dirty_groups: Arc<Mutex<HashSet<u64>>>,
    /// Phase Kursk: pending staging log flushes. Populated by sync() in
    /// LocalThenFlush mode, drained by flush_to_s3().
    pending_flushes: Arc<Mutex<Vec<staging::PendingFlush>>>,
    /// Phase Kursk: monotonic counter for staging log filenames.
    /// Avoids version collisions when manifest version is updated by flush
    /// between two checkpoints.
    staging_seq: Arc<AtomicU64>,
    /// Serializes flush_to_s3() calls. Prevents two concurrent flushes from
    /// racing on version numbers and S3 keys. Also prevents a durable-mode
    /// checkpoint (which drains s3_dirty_groups in sync()) from interleaving
    /// with a flush in progress.
    flush_lock: Arc<Mutex<()>>,
    /// Phase Somme: WAL replication state (started lazily on first MainDb open).
    #[cfg(feature = "wal")]
    wal_state: std::sync::Mutex<wal_replication::WalReplicationState>,
    /// Tokio runtime handle for spawning WAL replication background task.
    /// None in local-only mode (no tokio dependency).
    #[cfg(feature = "cloud")]
    runtime_handle: Option<tokio::runtime::Handle>,
}

impl TurboliteVfs {
    /// Create a new VFS. Dispatches to local or cloud construction based on config.
    pub fn new(config: TurboliteConfig) -> io::Result<Self> {
        match config.effective_backend() {
            StorageBackend::Local => Self::new_local(config),
            #[cfg(feature = "cloud")]
            StorageBackend::S3 { .. } => Self::new_cloud(config),
        }
    }

    /// Construct a local-only VFS. No S3, no tokio, no async.
    /// Page groups and manifest stored at `{cache_dir}/`.
    fn new_local(mut config: TurboliteConfig) -> io::Result<Self> {
        // Local mode always uses LocalThenFlush (no S3 to upload to)
        config.sync_mode = SyncMode::LocalThenFlush;
        let storage = Arc::new(StorageClient::local(config.cache_dir.clone())?);

        // Load manifest + any dirty groups from crash recovery
        let (mut manifest, recovered_dirty_groups) = match storage.get_manifest_with_dirty_groups()? {
            (Some(m), dirty) => {
                if !dirty.is_empty() {
                    eprintln!(
                        "[local] loaded manifest (v{}, {} pages, {} dirty groups pending flush)",
                        m.version, m.page_count, dirty.len(),
                    );
                } else {
                    eprintln!(
                        "[local] loaded manifest (v{}, {} pages)",
                        m.version, m.page_count,
                    );
                }
                (m, dirty)
            }
            (None, _) => {
                eprintln!("[local] no manifest found, starting empty database");
                (Manifest::empty(), Vec::new())
            }
        };
        manifest.detect_and_normalize_strategy();

        let page_size = if manifest.page_size > 0 { manifest.page_size } else { 4096 };
        let ppg = if manifest.pages_per_group > 0 { manifest.pages_per_group } else { config.pages_per_group };

        let cache = DiskCache::new_with_compression(
            &config.cache_dir, config.cache_ttl_secs, ppg, config.sub_pages_per_frame,
            page_size, manifest.page_count, config.encryption_key,
            manifest.group_pages.clone(),
            config.cache_compression, config.cache_compression_level,
            #[cfg(feature = "zstd")]
            config.dictionary.clone(),
        )?;
        let manifest_groups = manifest.total_groups() as usize;
        cache.ensure_group_capacity(manifest_groups);
        let cache = Arc::new(cache);
        let page_count = Arc::new(AtomicU64::new(manifest.page_count));

        let shared_manifest = Arc::new(RwLock::new(manifest));
        let initial_dirty: HashSet<u64> = recovered_dirty_groups.into_iter().collect();
        let shared_dirty_groups = Arc::new(Mutex::new(initial_dirty));

        // Phase Kursk: recover staging logs
        let staging_dir = config.cache_dir.join("staging");
        let recovered_staging = staging::recover_staging_logs(&staging_dir, page_size)?;
        let max_recovered_version = recovered_staging.iter().map(|p| p.version).max().unwrap_or(0);
        let staging_seq = Arc::new(AtomicU64::new(max_recovered_version + 1));
        let pending_flushes = Arc::new(Mutex::new(recovered_staging));

        Ok(Self {
            storage,
            s3: None,
            cache,
            prefetch_pool: None,
            page_count,
            config,
            #[cfg(feature = "cloud")]
            _runtime: None,
            shared_manifest,
            shared_dirty_groups,
            pending_flushes,
            staging_seq,
            flush_lock: Arc::new(Mutex::new(())),
            #[cfg(feature = "wal")]
            wal_state: std::sync::Mutex::new(wal_replication::WalReplicationState::new()),
            #[cfg(feature = "cloud")]
            runtime_handle: None,
        })
    }

    /// Construct a cloud (S3-backed) VFS. Requires tokio runtime.
    #[cfg(feature = "cloud")]
    fn new_cloud(config: TurboliteConfig) -> io::Result<Self> {
        let (runtime_handle, owned_runtime) =
            if let Some(ref handle) = config.runtime_handle {
                (handle.clone(), None)
            } else if let Ok(handle) = TokioHandle::try_current() {
                (handle, None)
            } else {
                let rt = tokio::runtime::Runtime::new().map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!("Failed to create tokio runtime: {}", e),
                    )
                })?;
                let handle = rt.handle().clone();
                (handle, Some(rt))
            };

        eprintln!("[tiered] creating S3 client...");
        let s3 = S3Client::new_blocking(&config, &runtime_handle)?;
        eprintln!("[tiered] S3 client created, fetching manifest...");

        // Phase Gallipoli: try local manifest first for cache initialization.
        let (mut manifest, recovered_dirty_groups) = match manifest::LocalManifest::load(&config.cache_dir) {
            Ok(Some(local)) => {
                eprintln!(
                    "[tiered] loaded local manifest for cache init (v{}, {} pages, {} dirty groups)",
                    local.manifest.version, local.manifest.page_count, local.dirty_groups.len(),
                );
                (local.manifest, local.dirty_groups)
            }
            _ => {
                eprintln!("[tiered] no local manifest, fetching from S3 for cache init...");
                (s3.get_manifest()?.unwrap_or_else(Manifest::empty), Vec::new())
            }
        };
        manifest.detect_and_normalize_strategy();

        // S3Primary: patch journal_mode in db_header from OFF (0) to DELETE (1).
        // S3Primary relies on xSync to upload dirty pages to S3. But SQLite with
        // journal_mode=OFF never calls xSync on commit, silently losing all data.
        // DELETE mode ensures xSync fires for every commit while keeping S3 as
        // the durability layer (the rollback journal is just a local safety net).
        if config.sync_mode == SyncMode::S3Primary {
            if let Some(ref mut page0) = manifest.db_header {
                if page0.len() > 18 && page0[18] == 0 {
                    page0[18] = 1; // DELETE
                    eprintln!("[tiered] S3Primary: patched journal_mode OFF -> DELETE in db_header");
                }
            }
        }

        eprintln!("[tiered] manifest for cache init (page_size={}, ppg={}, strategy={:?})", manifest.page_size, manifest.pages_per_group, manifest.strategy);
        let page_size = if manifest.page_size > 0 { manifest.page_size } else { 4096 };
        let ppg = if manifest.pages_per_group > 0 { manifest.pages_per_group } else { config.pages_per_group };

        let cache = DiskCache::new_with_compression(
            &config.cache_dir, config.cache_ttl_secs, ppg, config.sub_pages_per_frame,
            page_size, manifest.page_count, config.encryption_key,
            manifest.group_pages.clone(),
            config.cache_compression, config.cache_compression_level,
            #[cfg(feature = "zstd")]
            config.dictionary.clone(),
        )?;
        let manifest_groups = manifest.total_groups() as usize;
        cache.ensure_group_capacity(manifest_groups);

        // Write patched page 0 to cache so the first connection sees DELETE mode.
        if config.sync_mode == SyncMode::S3Primary {
            if let Some(ref page0) = manifest.db_header {
                eprintln!("[tiered] S3Primary: writing patched page 0 to cache (len={}, byte18={})",
                    page0.len(), if page0.len() > 18 { page0[18] } else { 255 });
                let _ = cache.write_page(0, page0);
                cache.bitmap.lock().mark_present(0);
            } else {
                eprintln!("[tiered] S3Primary: no db_header in manifest, cannot patch page 0");
            }
        }

        let s3 = Arc::new(s3);
        let storage = Arc::new(StorageClient::s3(Arc::clone(&s3)));
        let cache = Arc::new(cache);
        let page_count = Arc::new(AtomicU64::new(manifest.page_count));

        let shared_manifest = Arc::new(RwLock::new(manifest));

        let prefetch_pool = Arc::new(PrefetchPool::new(
            config.prefetch_threads,
            Arc::clone(&s3),
            Arc::clone(&cache),
            ppg,
            Arc::clone(&page_count),
            #[cfg(feature = "zstd")]
            config.dictionary.clone(),
            config.encryption_key,
            Arc::clone(&shared_manifest),
        ));
        let initial_dirty: HashSet<u64> = recovered_dirty_groups.into_iter().collect();
        if !initial_dirty.is_empty() {
            eprintln!("[tiered] recovered {} dirty groups from local manifest (pending S3 flush)", initial_dirty.len());
        }
        let shared_dirty_groups = Arc::new(Mutex::new(initial_dirty));
        let flush_lock = Arc::new(Mutex::new(()));

        // Phase Kursk: recover staging logs
        let staging_dir = config.cache_dir.join("staging");
        let recovered_staging = staging::recover_staging_logs(&staging_dir, page_size)?;
        if !recovered_staging.is_empty() {
            eprintln!("[tiered] recovered {} staging logs from interrupted flush", recovered_staging.len());
        }
        let max_recovered_version = recovered_staging.iter().map(|p| p.version).max().unwrap_or(0);
        let staging_seq = Arc::new(AtomicU64::new(max_recovered_version + 1));
        let pending_flushes = Arc::new(Mutex::new(recovered_staging));

        let vfs = Self {
            storage,
            s3: Some(s3),
            cache,
            prefetch_pool: Some(prefetch_pool),
            page_count,
            config,
            _runtime: owned_runtime,
            shared_manifest,
            shared_dirty_groups,
            pending_flushes,
            staging_seq,
            flush_lock,
            #[cfg(feature = "wal")]
            wal_state: std::sync::Mutex::new(wal_replication::WalReplicationState::new()),
            runtime_handle: Some(runtime_handle),
        };

        // Phase Somme: WAL recovery on cold start
        #[cfg(feature = "wal")]
        if vfs.config.wal_replication {
            let manifest_cc = vfs.shared_manifest.read().change_counter;
            if manifest_cc > 0 {
                if let Some(ref rt) = vfs.runtime_handle {
                    let wal_prefix = format!("{}/wal/", vfs.config.prefix);
                    let shared = vfs.shared_state();
                    match wal_replication::recover_wal_from_shared_state(
                        &shared,
                        &vfs.cache,
                        manifest_cc,
                        vfs.shared_manifest.read().page_size,
                        &wal_prefix,
                        &vfs.config.bucket,
                        vfs.config.endpoint_url.as_deref(),
                        rt,
                        &vfs.config.cache_dir,
                    ) {
                        Ok(0) => eprintln!("[tiered] WAL recovery: no WAL segments to replay"),
                        Ok(n) => eprintln!("[tiered] WAL recovery: loaded {} pages from WAL", n),
                        Err(e) => eprintln!("[tiered] WARNING: WAL recovery failed: {}", e),
                    }
                }
            }
        }

        Ok(vfs)
    }

    /// Load manifest based on ManifestSource config.
    /// Returns (manifest, recovered_dirty_groups, was_warm_reconnect).
    fn load_manifest(&self) -> io::Result<(Manifest, Vec<u64>, bool)> {
        let existing = self.shared_manifest.read();
        let has_loaded = existing.page_count > 0 || existing.version > 0;
        drop(existing);

        match self.config.manifest_source {
            ManifestSource::Auto => {
                // If VFS already has a manifest (warm reconnect), use it
                if has_loaded {
                    let m = self.shared_manifest.read().clone();
                    eprintln!("[tiered] using in-memory manifest (warm reconnect, v{})", m.version);
                    return Ok((m, Vec::new(), true));
                }
                // Try local manifest first
                if let Some(local) = manifest::LocalManifest::load(&self.config.cache_dir)? {
                    let dirty = local.dirty_groups.clone();
                    eprintln!(
                        "[tiered] loaded local manifest (v{}, {} pages, {} dirty groups)",
                        local.manifest.version, local.manifest.page_count, dirty.len(),
                    );
                    let mut m = local.manifest;
                    m.build_page_index();
                    return Ok((m, dirty, false));
                }
                // Fall back to storage (S3 or local)
                eprintln!("[tiered] no local manifest, fetching from storage...");
                let m = self.storage.get_manifest()?.unwrap_or_else(Manifest::empty);
                eprintln!("[tiered] manifest fetched (v{}, {} pages)", m.version, m.page_count);
                Ok((m, Vec::new(), false))
            }
            ManifestSource::S3 => {
                eprintln!("[tiered] fetching manifest from storage (manifest_source=S3)...");
                let m = self.storage.get_manifest()?.unwrap_or_else(Manifest::empty);
                eprintln!("[tiered] manifest fetched (v{}, {} pages)", m.version, m.page_count);
                Ok((m, Vec::new(), false))
            }
        }
    }

    /// Get a shared state handle for cache control, S3 counters, flush_to_s3, and SQL functions.
    /// The handle shares the same cache, S3 client, manifest, and dirty groups as the VFS.
    /// Panics if called in local-only mode (no S3 client).
    #[cfg(feature = "cloud")]
    pub fn shared_state(&self) -> bench::TurboliteSharedState {
        TurboliteSharedState {
            s3: self.s3.as_ref().expect("shared_state requires S3 backend").clone(),
            cache: Arc::clone(&self.cache),
            prefetch_pool: self.prefetch_pool.as_ref().expect("shared_state requires prefetch pool").clone(),
            shared_manifest: Arc::clone(&self.shared_manifest),
            shared_dirty_groups: Arc::clone(&self.shared_dirty_groups),
            pending_flushes: Arc::clone(&self.pending_flushes),
            flush_lock: Arc::clone(&self.flush_lock),
            compression_level: self.config.compression_level,
            #[cfg(feature = "zstd")]
            dictionary: self.config.dictionary.clone(),
            encryption_key: self.config.encryption_key,
            gc_enabled: self.config.gc_enabled,
            override_threshold: self.config.override_threshold,
            compaction_threshold: self.config.compaction_threshold,
        }
    }

    /// Evict non-interior pages from disk cache. Interior pages and group 0
    /// (schema + root page) stay warm -- simulates production where structural
    /// pages are always hot after first access.
    ///
    /// Safe to call with pending flush: groups awaiting S3 upload are protected
    /// (their pages remain in the disk cache bitmap).
    pub fn clear_cache(&self) {
        if let Some(ref pool) = self.prefetch_pool {
            pool.wait_idle();
        }

        let pinned_pages = self.cache.interior_pages.lock().clone();
        let pending_groups = self.pending_group_pages();

        // Reset group states to None, except group 0 and pending groups stay Present
        let states = self.cache.group_states.lock();
        for (i, s) in states.iter().enumerate() {
            if i == 0 || pending_groups.contains_key(&(i as u64)) {
                s.store(GroupState::Present as u8, Ordering::Release);
            } else {
                s.store(GroupState::None as u8, Ordering::Release);
            }
        }
        drop(states);

        self.cache.group_access.lock().clear();

        // Clear bitmap except for interior pages, index pages, group 0, and pending groups
        // Uses B-tree-aware group_pages[0] for group 0 (not positional 0..ppg)
        {
            let index_pages = self.cache.index_pages.lock().clone();
            let gp = self.cache.group_pages.read();
            let mut bitmap = self.cache.bitmap.lock();
            bitmap.bits.fill(0);

            // Collect pages to keep
            let mut keep_pages: HashSet<u64> = HashSet::new();
            for &page in &pinned_pages {
                bitmap.mark_present(page);
                keep_pages.insert(page);
            }
            for &page in &index_pages {
                bitmap.mark_present(page);
                keep_pages.insert(page);
            }
            // Protect pending flush pages
            for pages in pending_groups.values() {
                for &p in pages {
                    bitmap.mark_present(p);
                    keep_pages.insert(p);
                }
            }
            if let Some(g0_pages) = gp.first() {
                // BTreeAware: explicit page list
                for &p in g0_pages {
                    bitmap.mark_present(p);
                    keep_pages.insert(p);
                }
            } else {
                // Positional: group 0 = pages 0..ppg
                let ppg = self.cache.pages_per_group as u64;
                for p in 0..ppg {
                    bitmap.mark_present(p);
                    keep_pages.insert(p);
                }
            }
            let _ = bitmap.persist();

            // Prune compressed cache index: remove entries for evicted pages
            drop(bitmap);
            self.cache.prune_cache_index(&keep_pages);
        }

        // Clear sub-chunk tracker: evict Data tier only, keep Pinned + Index
        {
            let mut tracker = self.cache.tracker.lock();
            tracker.clear_data_only();
        }
    }

    /// Upload locally-checkpointed dirty pages to S3 without holding any SQLite lock.
    ///
    /// # Two-phase checkpoint pattern
    ///
    /// ```ignore
    /// // Phase 1: fast (~1ms), holds SQLite EXCLUSIVE lock briefly
    /// turbolite::tiered::set_local_checkpoint_only(true);
    /// conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    /// turbolite::tiered::set_local_checkpoint_only(false);
    ///
    /// // Phase 2: slow (S3 uploads), NO SQLite lock held
    /// vfs.flush_to_s3().unwrap();
    /// ```
    ///
    /// # Durability model
    ///
    /// Between phase 1 and phase 2, data exists ONLY in the local disk cache.
    /// - Process crash: data survives (on local disk)
    /// - Machine loss: data lost (not yet on S3)
    /// - `clear_cache*` methods protect pending groups from eviction
    ///
    /// After flush_to_s3() completes, data is durable on S3.
    #[cfg(feature = "cloud")]
    pub fn flush_to_s3(&self) -> io::Result<()> {
        let s3 = self.s3.as_ref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Unsupported, "flush_to_s3 requires S3 backend")
        })?;
        let _guard = self.flush_lock.lock().unwrap();
        flush::flush_dirty_groups_to_s3(
            s3,
            &self.cache,
            &self.shared_manifest,
            &self.shared_dirty_groups,
            &self.pending_flushes,
            self.config.compression_level,
            #[cfg(feature = "zstd")]
            self.config.dictionary.as_deref(),
            self.config.encryption_key,
            self.config.gc_enabled,
            self.config.override_threshold,
            self.config.compaction_threshold,
        )
    }

    /// Returns true if there are dirty groups or staging logs pending S3 upload.
    pub fn has_pending_flush(&self) -> bool {
        !self.shared_dirty_groups.lock().unwrap().is_empty()
            || !self.pending_flushes.lock().unwrap().is_empty()
    }

    /// Get page numbers for all groups pending S3 upload.
    /// Used by clear_cache to protect unflushed pages from eviction.
    fn pending_group_pages(&self) -> HashMap<u64, Vec<u64>> {
        let pending = self.shared_dirty_groups.lock().unwrap();
        if pending.is_empty() {
            return HashMap::new();
        }
        let manifest = self.shared_manifest.read();
        let mut result = HashMap::new();
        for &gid in pending.iter() {
            let pages = manifest.group_page_nums(gid).into_owned();
            result.insert(gid, pages);
        }
        result
    }

    /// Reset S3 I/O counters. Returns (fetch_count, fetch_bytes) before reset.
    /// Returns (0, 0) in local-only mode or when cloud feature is disabled.
    pub fn reset_s3_counters(&self) -> (u64, u64) {
        #[cfg(feature = "cloud")]
        if let Some(s3) = &self.s3 {
            let count = s3.fetch_count.swap(0, Ordering::Relaxed);
            let bytes = s3.fetch_bytes.swap(0, Ordering::Relaxed);
            return (count, bytes);
        }
        (0, 0)
    }

    /// Read current S3 I/O counters without resetting.
    /// Returns (0, 0) in local-only mode or when cloud feature is disabled.
    pub fn s3_counters(&self) -> (u64, u64) {
        #[cfg(feature = "cloud")]
        if let Some(s3) = &self.s3 {
            return (
                s3.fetch_count.load(Ordering::Relaxed),
                s3.fetch_bytes.load(Ordering::Relaxed),
            );
        }
        (0, 0)
    }

    /// Garbage collect orphaned S3 objects not referenced by the current manifest.
    /// Lists all objects under the prefix, compares against manifest keys, and
    /// deletes unreferenced page groups and interior chunks.
    /// Returns the number of objects deleted.
    #[cfg(feature = "cloud")]
    pub fn gc(&self) -> io::Result<usize> {
        let s3 = self.s3.as_ref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Unsupported, "gc requires S3 backend")
        })?;
        let manifest = s3.get_manifest()?.unwrap_or_else(Manifest::empty);
        let all_keys = s3.list_all_keys()?;

        // Build set of live keys from manifest
        let mut live_keys: HashSet<String> = HashSet::new();
        // Phase Thermopylae: msgpack manifest is the live one.
        // Old manifest.json is an orphan and will be GC'd.
        live_keys.insert(s3.manifest_key_msgpack());
        for key in &manifest.page_group_keys {
            if !key.is_empty() {
                live_keys.insert(key.clone());
            }
        }
        for key in manifest.interior_chunk_keys.values() {
            live_keys.insert(key.clone());
        }
        for key in manifest.index_chunk_keys.values() {
            live_keys.insert(key.clone());
        }

        // Find orphans (keys in S3 but not in manifest)
        let orphans: Vec<String> = all_keys
            .into_iter()
            .filter(|k| !live_keys.contains(k))
            .collect();

        let count = orphans.len();
        if count > 0 {
            eprintln!("[gc] deleting {} orphaned S3 objects...", count);
            s3.delete_objects(&orphans)?;
            eprintln!("[gc] deleted {} orphaned objects", count);
        }
        Ok(count)
    }

    /// Helper to destroy all S3 data for a prefix.
    #[cfg(feature = "cloud")]
    pub fn destroy_s3(&self) -> io::Result<()> {
        let s3 = self.s3.as_ref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Unsupported, "destroy_s3 requires S3 backend")
        })?;
        S3Client::block_on(&s3.runtime, async {
            let mut continuation_token: Option<String> = None;
            loop {
                let mut req = s3
                    .client
                    .list_objects_v2()
                    .bucket(&s3.bucket)
                    .prefix(&s3.prefix);

                if let Some(token) = &continuation_token {
                    req = req.continuation_token(token);
                }

                let resp = req.send().await.map_err(|e| {
                    io::Error::new(io::ErrorKind::Other, format!("S3 list failed: {}", e))
                })?;

                let keys: Vec<String> = resp
                    .contents()
                    .iter()
                    .filter_map(|obj| obj.key().map(|k| k.to_string()))
                    .collect();

                for batch in keys.chunks(1000) {
                    let objects: Vec<aws_sdk_s3::types::ObjectIdentifier> = batch
                        .iter()
                        .map(|key| {
                            aws_sdk_s3::types::ObjectIdentifier::builder()
                                .key(key)
                                .build()
                                .expect("ObjectIdentifier requires key")
                        })
                        .collect();

                    let delete = aws_sdk_s3::types::Delete::builder()
                        .set_objects(Some(objects))
                        .quiet(true)
                        .build()
                        .map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::Other,
                                format!("Failed to build Delete request: {}", e),
                            )
                        })?;

                    s3.client
                        .delete_objects()
                        .bucket(&s3.bucket)
                        .delete(delete)
                        .send()
                        .await
                        .map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::Other,
                                format!("S3 batch delete failed: {}", e),
                            )
                        })?;
                }

                if resp.is_truncated() == Some(true) {
                    continuation_token =
                        resp.next_continuation_token().map(|s| s.to_string());
                } else {
                    break;
                }
            }
            Ok(())
        })
    }

    /// Return a clone of the current manifest state.
    pub fn manifest(&self) -> Manifest {
        self.shared_manifest.read().clone()
    }

    /// Fetch the latest manifest from S3 and apply it via set_manifest.
    /// Returns the new manifest version, or None if no manifest exists in S3.
    /// Used by HA followers to catch up from the leader's turbolite state.
    #[cfg(feature = "cloud")]
    pub fn fetch_and_apply_s3_manifest(&self) -> std::io::Result<Option<u64>> {
        let s3 = self.s3.as_ref().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::Other, "no S3 client (cloud feature required)")
        })?;
        match s3.get_manifest()? {
            Some(manifest) => {
                let version = manifest.version;
                self.set_manifest(manifest);
                Ok(Some(version))
            }
            None => Ok(None),
        }
    }

    /// Update the internal manifest from external state (e.g. haqlite catch-up).
    ///
    /// Rebuilds page_index and normalizes strategy, updates the disk cache's
    /// group_pages mapping, invalidates cache entries for groups whose
    /// page_group_keys changed, and atomically updates page_count.
    pub fn set_manifest(&self, mut manifest: Manifest) {
        manifest.detect_and_normalize_strategy();

        // Refuse to downgrade. The manifest version is monotonically increasing.
        // A stale S3 read (e.g., from a follower poll loop that raced with the
        // leader's xSync) must not revert the in-memory manifest.
        {
            let current = self.shared_manifest.read();
            if manifest.version > 0 && current.version > 0 && manifest.version <= current.version {
                if manifest.version < current.version {
                    eprintln!("[set_manifest] REJECTED: incoming v{} < current v{} (would downgrade)",
                        manifest.version, current.version);
                }
                return;
            }
        }

        // Snapshot old page_group_keys before swapping
        let old_keys: Vec<String> = {
            let old = self.shared_manifest.read();
            old.page_group_keys.clone()
        };

        // Update cache group_pages if present
        if !manifest.group_pages.is_empty() {
            self.cache.set_group_pages(manifest.group_pages.clone());
            self.cache.ensure_group_capacity(manifest.group_pages.len());
        }

        // Invalidate cache entries for groups whose page_group_keys changed.
        // Compare old vs new: any group whose key differs (or is new/removed)
        // gets evicted from local cache (bitmap cleared, group state reset)
        // so the VFS refetches from S3 on next read.
        let new_keys = &manifest.page_group_keys;
        let max_len = std::cmp::max(old_keys.len(), new_keys.len());
        // Collect changed groups first, then evict (avoids lock re-entry)
        let mut changed_groups: Vec<u64> = (0..max_len)
            .filter(|&gid| {
                let old_key = old_keys.get(gid).map(|s| s.as_str());
                let new_key = new_keys.get(gid).map(|s| s.as_str());
                old_key != new_key
            })
            .map(|gid| gid as u64)
            .collect();
        // Also check subframe_overrides: S3Primary uploads overrides rather than
        // new page groups. If overrides changed, those groups need eviction too.
        let old_version = {
            let old = self.shared_manifest.read();
            old.version
        };
        if manifest.version != old_version && manifest.version > 0 {
            // Version changed: evict ALL groups with overrides in the new manifest
            for (gid, ovs) in manifest.subframe_overrides.iter().enumerate() {
                if !ovs.is_empty() && !changed_groups.contains(&(gid as u64)) {
                    changed_groups.push(gid as u64);
                }
            }
        }

        if !changed_groups.is_empty() {
            eprintln!("[set_manifest] evicting {} changed groups (old_keys={}, new_keys={}): {:?}",
                changed_groups.len(), old_keys.len(), new_keys.len(), changed_groups);
        }
        for gid in &changed_groups {
            self.cache.evict_group(*gid);
        }
        // Verify eviction worked: no pages should be present after group eviction.
        if !changed_groups.is_empty() {
            let present: Vec<u64> = (0..std::cmp::max(manifest.page_count, 10))
                .filter(|&p| self.cache.is_present(p))
                .collect();
            if !present.is_empty() {
                eprintln!("[set_manifest] BUG: after evicting groups {:?}, {} pages still present: {:?}",
                    changed_groups, present.len(), &present[..std::cmp::min(20, present.len())]);
            }
        }

        // Write page 0 to local cache from manifest. This gives SQLite the
        // correct database header (page count, schema cookie) immediately,
        // without needing an S3 fetch or connection reopen.
        if let Some(ref page0) = manifest.db_header {
            let mut page0_patched = page0.clone();
            // If S3Primary sync mode and journal_mode is OFF (byte 18 = 0),
            // patch to DELETE (byte 18 = 1). S3Primary relies on xSync for
            // S3 uploads, but journal_mode=OFF prevents SQLite from calling
            // xSync on commit. DELETE mode ensures xSync fires.
            if self.config.sync_mode == SyncMode::S3Primary
                && page0_patched.len() > 18
                && page0_patched[18] == 0
            {
                page0_patched[18] = 1; // DELETE = 1
            }
            let _ = self.cache.write_page(0, &page0_patched);
            // Mark page 0 as present so the VFS serves it from cache
            // instead of re-fetching from S3 (which has the unpatched header).
            self.cache.bitmap.lock().mark_present(0);
        }

        // Update page_count atomic
        self.page_count.store(manifest.page_count, Ordering::Release);

        // Write manifest to shared state
        *self.shared_manifest.write() = manifest.clone();

        // Persist to local disk so the next Connection::open -> load_manifest()
        // picks up the new manifest instead of reading a stale local copy.
        let local = super::manifest::LocalManifest {
            manifest,
            dirty_groups: Vec::new(),
        };
        if let Err(e) = local.persist(&self.config.cache_dir) {
            eprintln!("[set_manifest] warning: failed to persist manifest locally: {}", e);
        }

        // Persist bitmap changes from eviction
        if let Err(e) = self.cache.persist_bitmap() {
            eprintln!("[set_manifest] warning: failed to persist bitmap: {}", e);
        }
    }

    /// Get the path to the raw cache file (data.cache). This is the file that
    /// stores SQLite pages at page_num * page_size offsets. External processes
    /// (walrust) can read/write this file directly for snapshot/restore.
    pub fn cache_file_path(&self) -> PathBuf {
        self.config.cache_dir.join("data.cache")
    }

    /// Sync VFS state after an external process (walrust restore) wrote pages
    /// directly to the cache file. Marks all pages as present in the bitmap
    /// and updates the page_count atomic.
    ///
    /// Call this after walrust restore writes to the raw cache file path,
    /// before reopening the SQLite connection through the VFS.
    pub fn sync_after_external_restore(&self, page_count: u64) {
        self.cache.mark_all_pages_present(page_count);
        self.page_count.store(page_count, Ordering::Release);
        if let Err(e) = self.cache.persist_bitmap() {
            eprintln!("[sync_after_external_restore] warning: failed to persist bitmap: {}", e);
        }
    }
}

impl Vfs for TurboliteVfs {
    type Handle = TurboliteHandle;

    fn open(&self, db: &str, opts: OpenOptions) -> Result<Self::Handle, io::Error> {
        let path = self.config.cache_dir.join(db);

        if matches!(opts.kind, OpenKind::MainDb) {
            // Phase Gallipoli: load manifest from local disk or S3 based on config.
            let (mut manifest, recovered_dirty_groups, warm_reconnect) = self.load_manifest()?;
            manifest.detect_and_normalize_strategy();

            let ppg = if manifest.pages_per_group > 0 {
                manifest.pages_per_group
            } else {
                self.config.pages_per_group
            };

            // Phase Zenith-b: validate cache against manifest.
            // Compare loaded manifest with previous session's cached manifest.
            // Invalidate groups whose page_group_keys changed (another node wrote).
            {
                let old_manifest = self.shared_manifest.read().clone();
                if old_manifest.version > 0 && old_manifest.version != manifest.version {
                    if old_manifest.version > manifest.version {
                        // Local ahead of S3 (crash recovery): full cache invalidation.
                        // Local writes were not published; S3 is authoritative.
                        eprintln!(
                            "[cache-validate] local v{} ahead of manifest v{}, full cache invalidation",
                            old_manifest.version, manifest.version,
                        );
                        let states = self.cache.group_states.lock();
                        for state in states.iter() {
                            state.store(GroupState::None as u8, Ordering::Release);
                        }
                        self.cache.group_condvar.notify_all();
                    } else {
                        // Manifest is newer: diff page_group_keys, invalidate changed groups.
                        let mut invalidated = 0usize;
                        let max_groups = std::cmp::max(
                            old_manifest.page_group_keys.len(),
                            manifest.page_group_keys.len(),
                        );
                        let states = self.cache.group_states.lock();
                        for gid in 0..max_groups {
                            let old_key = old_manifest.page_group_keys.get(gid);
                            let new_key = manifest.page_group_keys.get(gid);
                            if old_key != new_key {
                                if let Some(state) = states.get(gid) {
                                    state.store(GroupState::None as u8, Ordering::Release);
                                    invalidated += 1;
                                }
                            }
                            // Also invalidate groups with overrides that changed
                            let old_ovs = old_manifest.subframe_overrides.get(gid);
                            let new_ovs = manifest.subframe_overrides.get(gid);
                            if old_ovs != new_ovs {
                                if let Some(state) = states.get(gid) {
                                    if state.load(Ordering::Acquire) != GroupState::None as u8 {
                                        state.store(GroupState::None as u8, Ordering::Release);
                                        invalidated += 1;
                                    }
                                }
                            }
                        }
                        if invalidated > 0 {
                            self.cache.group_condvar.notify_all();
                            eprintln!(
                                "[cache-validate] manifest v{} -> v{}: invalidated {} groups",
                                old_manifest.version, manifest.version, invalidated,
                            );
                        } else {
                            eprintln!(
                                "[cache-validate] manifest v{} -> v{}: no groups changed",
                                old_manifest.version, manifest.version,
                            );
                        }
                    }
                }
            }

            // Update cache's group_pages from latest manifest (BTreeAware only)
            if !manifest.group_pages.is_empty() {
                self.cache.set_group_pages(manifest.group_pages.clone());
                self.cache.ensure_group_capacity(manifest.group_pages.len());
            }

            // Update shared manifest. Skip on warm reconnect: the manifest is
            // already in shared state, and writing our clone back would race
            // with set_manifest calls from HA follower catch-up threads.
            if !warm_reconnect {
                *self.shared_manifest.write() = manifest;
            }

            // Recover dirty groups from local manifest (LocalThenFlush crash recovery)
            if !recovered_dirty_groups.is_empty() {
                let mut pending = self.shared_dirty_groups.lock().unwrap();
                let count = recovered_dirty_groups.len();
                pending.extend(recovered_dirty_groups);
                eprintln!("[tiered] recovered {} dirty groups from local manifest (pending S3 flush)", count);
            }

            let lock_dir = self.config.cache_dir.join("locks");
            fs::create_dir_all(&lock_dir)?;
            let lock_path = lock_dir.join(db);
            FsOpenOptions::new()
                .create(true)
                .write(true)
                .open(&lock_path)?;

            // Create WAL stub file so SQLite can enter WAL mode.
            // Without this, SQLite checks xAccess("db-wal") on open, finds no
            // WAL file, and silently falls back to rollback journal mode even
            // if the header says WAL. The stub is empty (0 bytes); SQLite
            // treats an empty WAL as "cleanly shut down, start fresh".
            //
            // S3Primary mode: skip WAL stub. S3Primary requires journal_mode=OFF
            // or MEMORY (no WAL). Without the stub, SQLite stays in non-WAL mode.
            #[cfg(feature = "cloud")]
            let skip_wal_stub = self.config.sync_mode == SyncMode::S3Primary;
            #[cfg(not(feature = "cloud"))]
            let skip_wal_stub = false;

            if !self.config.read_only && !skip_wal_stub {
                let wal_path = self.config.cache_dir.join(format!("{}-wal", db));
                let _ = FsOpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(&wal_path);
            }

            // Phase Somme: start WAL replication on first MainDb open
            #[cfg(feature = "wal")]
            if self.config.wal_replication {
                let mut wal = self.wal_state.lock().unwrap();
                if !wal.is_started() {
                    let db_path = self.config.cache_dir.join(db);
                    let wal_prefix = format!("{}/wal/", self.config.prefix);
                    let manifest_cc = self.shared_manifest.read().change_counter;
                    if let Err(e) = wal.start(
                        db_path,
                        wal_prefix,
                        manifest_cc,
                        self.config.wal_sync_interval_ms,
                        self.config.bucket.clone(),
                        self.config.endpoint_url.clone(),
                        self.config.region.clone(),
                        self.runtime_handle.clone(),
                    ) {
                        eprintln!("[tiered] WARNING: failed to start WAL replication: {}", e);
                    }
                }
            }

            TurboliteHandle::new_tiered(
                self.s3.clone(),
                if self.storage.is_local() { Some(Arc::clone(&self.storage)) } else { None },
                Arc::clone(&self.cache),
                Arc::clone(&self.shared_manifest),
                Arc::clone(&self.shared_dirty_groups),
                Arc::clone(&self.pending_flushes),
                Arc::clone(&self.staging_seq),
                lock_path,
                ppg,
                self.config.compression_level,
                self.config.read_only,
                self.config.sync_mode,
                self.config.prefetch_search.clone(),
                self.config.prefetch_lookup.clone(),
                self.prefetch_pool.as_ref().map(Arc::clone),
                self.config.gc_enabled,
                self.config.eager_index_load,
                #[cfg(feature = "zstd")]
                self.config.dictionary.as_deref(),
                self.config.encryption_key,
                self.config.query_plan_prefetch,
                self.config.max_cache_bytes,
                self.config.evict_on_checkpoint,
                self.config.jena_enabled,
            )
        } else {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let file = FsOpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)?;
            Ok(TurboliteHandle::new_passthrough(file, path, self.config.encryption_key))
        }
    }

    fn delete(&self, db: &str) -> Result<(), io::Error> {
        let path = self.config.cache_dir.join(db);
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        Ok(())
    }

    fn exists(&self, db: &str) -> Result<bool, io::Error> {
        let path = self.config.cache_dir.join(db);
        if path.exists() {
            return Ok(true);
        }

        if db.ends_with("-wal") || db.ends_with("-journal") || db.ends_with("-shm") {
            return Ok(false);
        }

        Ok(self.storage.exists()?)
    }

    fn temporary_name(&self) -> String {
        format!("temp_{}", std::process::id())
    }

    fn random(&self, buffer: &mut [i8]) {
        use std::time::SystemTime;
        let mut seed = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        for b in buffer.iter_mut() {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            *b = (seed >> 33) as i8;
        }
    }

    fn sleep(&self, duration: Duration) -> Duration {
        std::thread::sleep(duration);
        duration
    }
}

#[cfg(test)]
#[path = "test_local_vfs.rs"]
mod local_vfs_tests;
