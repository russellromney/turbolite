use super::*;

use hadb_storage::StorageBackend;
use hadb_storage_local::LocalStorage;

use crate::tiered::storage as storage_helpers;

// ===== TurboliteVfs =====

/// Turbolite SQLite VFS with compression, encryption, and a pluggable
/// storage backend.
///
/// turbolite is byte-agnostic below the VFS; the actual storage (filesystem,
/// S3, fenced HTTP, etc.) is an `Arc<dyn hadb_storage::StorageBackend>`. Use
/// [`TurboliteVfs::new`] for the default local-filesystem backend rooted
/// under `config.cache_dir`, or [`TurboliteVfs::with_backend`] to
/// inject any other backend + the tokio runtime handle the VFS should
/// drive it on.
pub struct TurboliteVfs {
    /// Backing storage. For local mode this is
    /// `hadb_storage_local::LocalStorage::new(&config.cache_dir)`.
    pub(crate) storage: Arc<dyn StorageBackend>,
    /// Tokio runtime handle used to drive async backend calls from sync code.
    pub(crate) runtime: tokio::runtime::Handle,
    /// True when the storage is `LocalStorage` pointed at `config.cache_dir`.
    /// Some paths (WAL stub creation, cache validation) behave differently
    /// in local vs remote.
    pub(crate) is_local: bool,
    /// Optional owned runtime (for `new()` which spins up its own tokio).
    /// Kept alive for the VFS's lifetime.
    _owned_runtime: Option<tokio::runtime::Runtime>,
    pub(crate) cache: Arc<DiskCache>,
    prefetch_pool: Option<Arc<PrefetchPool>>,
    /// Shared page_count for prefetch workers (kept alive by PrefetchPool workers).
    #[allow(dead_code)]
    page_count: Arc<AtomicU64>,
    config: TurboliteConfig,
    /// Shared manifest state. Written by TurboliteHandle during sync/checkpoint,
    /// read by flush_to_storage() for non-blocking storage upload.
    shared_manifest: Arc<ArcSwap<Manifest>>,
    /// True when this VFS checkpoints into local staging and requires an
    /// explicit `flush_to_storage()` to publish to the backend.
    pub(crate) local_checkpoint_only: bool,
    /// Shared pending dirty groups. Accumulated by TurboliteHandle during
    /// local-only checkpoints, drained by flush_to_storage().
    shared_dirty_groups: Arc<Mutex<HashSet<u64>>>,
    /// Pending staging log flushes. Drained by flush_to_storage() for any
    /// staging logs recovered on open from older turbolite versions.
    pending_flushes: Arc<Mutex<Vec<staging::PendingFlush>>>,
    /// Monotonic counter for staging log filenames.
    /// Avoids version collisions when manifest version is updated by flush
    /// between two checkpoints.
    staging_seq: Arc<AtomicU64>,
    /// Serialises flush_to_storage() calls. Prevents two concurrent flushes
    /// from racing on version numbers and storage keys. Also prevents a
    /// durable-mode checkpoint from interleaving with a flush in progress.
    flush_lock: Arc<Mutex<()>>,
    /// Read/write gate for direct hybrid page replay.
    ///
    /// SQLite read transactions hold an Arc-owned read guard between
    /// `xLock(SHARED)` and `xUnlock(NONE)` on `TurboliteHandle`.
    /// `ReplayHandle::finalize` takes the write guard before installing
    /// staged pages, so a finalize and a SQLite read transaction are
    /// mutually exclusive: an in-flight read sees pre-replay state for
    /// its full duration, and a read started while finalize is running
    /// blocks until install completes and observes only the post-replay
    /// state. Mid-read torn snapshots are impossible.
    pub(crate) replay_gate: Arc<parking_lot::RwLock<()>>,
    /// Monotonic epoch incremented by `ReplayHandle::finalize` while
    /// holding `replay_gate.write()`. Background cache writers
    /// (PrefetchPool, eager-fetch on handle open) capture the current
    /// value at job submission and re-check it under `replay_gate.read()`
    /// immediately before their final cache write; if the value has
    /// advanced, they drop the work without writing. This stops a stale
    /// prefetch fetched against the pre-replay manifest from overwriting
    /// freshly-replayed bytes.
    pub(crate) replay_epoch: Arc<AtomicU64>,
    /// Test-only barrier that lets a deterministic test pause finalize
    /// after the write-gate is held but before any page write happens.
    /// Production builds compile this field out via #[cfg(test)].
    #[cfg(test)]
    pub(crate) finalize_pause: parking_lot::Mutex<Option<Arc<std::sync::Barrier>>>,
    /// WAL replication state (started lazily on first MainDb open).
    #[cfg(feature = "wal")]
    wal_state: std::sync::Mutex<wal_replication::WalReplicationState>,
}

impl TurboliteVfs {
    /// Local mode. Wraps `hadb_storage_local::LocalStorage` rooted at
    /// `config.cache_dir` and spins up a dedicated 2-thread tokio runtime.
    /// Use [`TurboliteVfs::with_backend`] if you want to supply a remote
    /// `StorageBackend` + reuse an existing runtime.
    pub fn new_local(mut config: TurboliteConfig) -> io::Result<Self> {
        let owned = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("build tokio rt: {e}")))?;
        let runtime = owned.handle().clone();
        config.apply_legacy_flat_fields();

        // Ensure the state dir exists before local_state/staging helpers
        // write under it. LocalStorage creates parents lazily for backend
        // objects, but local sidecar metadata is synchronous.
        fs::create_dir_all(&config.cache_dir)?;

        #[cfg(feature = "cli-s3")]
        if !config.bucket.is_empty() {
            let bucket = config.bucket.clone();
            let endpoint = config.endpoint_url.clone();
            let prefix = config.prefix.clone();
            let storage = owned
                .block_on(async {
                    hadb_storage_s3::S3Storage::from_env(bucket, endpoint.as_deref()).await
                })
                .map_err(|e| {
                    io::Error::new(io::ErrorKind::Other, format!("create S3 backend: {e}"))
                })?
                .with_prefix(prefix);
            let storage: Arc<dyn StorageBackend> = Arc::new(storage);
            #[cfg(feature = "bundled-sqlite")]
            crate::install_hook::ensure_registered();
            return Self::assemble(config, storage, runtime, Some(owned), false);
        }

        let storage: Arc<dyn StorageBackend> = Arc::new(LocalStorage::new(&config.cache_dir));
        #[cfg(feature = "bundled-sqlite")]
        crate::install_hook::ensure_registered();
        Self::assemble(config, storage, runtime, Some(owned), true)
    }

    /// Legacy constructor retained for the pre-hadb S3 test corpus.
    pub fn new(config: TurboliteConfig) -> io::Result<Self> {
        Self::new_local(config)
    }

    /// Caller-supplied backend. Checkpoints upload dirty pages to the backend
    /// synchronously (full remote durability on every checkpoint).
    pub fn with_backend(
        mut config: TurboliteConfig,
        backend: Arc<dyn StorageBackend>,
        runtime: tokio::runtime::Handle,
    ) -> io::Result<Self> {
        config.apply_legacy_flat_fields();
        fs::create_dir_all(&config.cache_dir)?;
        #[cfg(feature = "bundled-sqlite")]
        crate::install_hook::ensure_registered();
        Self::assemble(config, backend, runtime, None, false)
    }

    /// Private assembly path shared by [`Self::new_local`] and
    /// [`Self::with_backend`]. `owned_runtime` is Some only when the caller
    /// handed us a runtime to keep alive.
    fn assemble(
        config: TurboliteConfig,
        storage: Arc<dyn StorageBackend>,
        runtime: tokio::runtime::Handle,
        owned_runtime: Option<tokio::runtime::Runtime>,
        is_local: bool,
    ) -> io::Result<Self> {
        // 1. Decide which manifest to start from.
        //    Warm local_state wins unless missing, in which case we go to
        //    the backend. Dirty groups also live in local_state.
        let local_checkpoint_only = matches!(
            config.cache.checkpoint_mode,
            super::CheckpointMode::LocalThenFlush
        );
        let local_manifest = manifest::load_manifest_local(&config.cache_dir)?;
        let recovered_dirty_groups = manifest::load_dirty_groups(&config.cache_dir)?;

        let mut manifest = match local_manifest {
            Some(m) => {
                turbolite_debug!(
                    "[tiered] loaded local manifest (v{}, {} pages, {} dirty groups)",
                    m.version,
                    m.page_count,
                    recovered_dirty_groups.len(),
                );
                m
            }
            None => {
                turbolite_debug!("[tiered] no local manifest, fetching from backend...");
                let fetched = storage_helpers::get_manifest(storage.as_ref(), &runtime)?;
                match fetched {
                    Some(m) => {
                        turbolite_debug!(
                            "[tiered] manifest fetched from backend (v{}, {} pages)",
                            m.version,
                            m.page_count,
                        );
                        m
                    }
                    None => {
                        turbolite_debug!("[tiered] no manifest in backend, starting empty");
                        Manifest::empty()
                    }
                }
            }
        };
        manifest.detect_and_normalize_strategy();

        let page_size = if manifest.page_size > 0 {
            manifest.page_size
        } else {
            4096
        };
        let ppg = if manifest.pages_per_group > 0 {
            manifest.pages_per_group
        } else {
            config.cache.pages_per_group
        };

        let cache_file_path = config
            .local_data_path
            .clone()
            .unwrap_or_else(|| config.cache_dir.join("data.cache"));
        let cache = DiskCache::new_with_compression_at(
            &config.cache_dir,
            &cache_file_path,
            config.cache.ttl_secs,
            ppg,
            config.cache.sub_pages_per_frame,
            page_size,
            manifest.page_count,
            config.encryption.key,
            manifest.group_pages.clone(),
            config.cache.compression,
            config.cache.compression_level,
            #[cfg(feature = "zstd")]
            config.compression.dictionary.clone(),
            config.cache.mem_budget,
        )?;
        let manifest_groups = manifest.total_groups() as usize;
        cache.ensure_group_capacity(manifest_groups);

        // 2. Recover staging logs (interrupted flushes from a crash).
        let staging_dir = config.cache_dir.join("staging");
        let recovered_staging = staging::recover_staging_logs(&staging_dir, page_size)?;
        let max_recovered_version = recovered_staging
            .iter()
            .map(|p| p.version)
            .max()
            .unwrap_or(0);
        if !recovered_staging.is_empty() {
            turbolite_debug!(
                "[tiered] recovered {} staging logs (max v{})",
                recovered_staging.len(),
                max_recovered_version,
            );

            // Prefer the newest manifest embedded in staging log trailers
            // over the one we loaded above (the background flush may not
            // have run yet, leaving the on-disk / remote manifest stale).
            for flush_entry in recovered_staging.iter().rev() {
                if let Ok(Some(manifest_bytes)) =
                    staging::read_staging_manifest(&flush_entry.staging_path, flush_entry.page_size)
                {
                    if let Ok(m) = manifest::decode_manifest_bytes(&manifest_bytes) {
                        if m.version > manifest.version {
                            turbolite_debug!(
                                "[tiered] staging log v{} has newer manifest (v{}, {} pages)",
                                flush_entry.version,
                                m.version,
                                m.page_count,
                            );
                            manifest = m;
                            manifest.detect_and_normalize_strategy();
                            let new_groups = manifest.total_groups() as usize;
                            cache.ensure_group_capacity(new_groups);
                            if !manifest.group_pages.is_empty() {
                                cache.set_group_pages(manifest.group_pages.clone());
                            }
                            break;
                        }
                    }
                }
            }

            // Replay staging log pages into cache so cold reopens can
            // serve reads immediately without waiting for a background
            // flush to materialise them.
            for flush_entry in &recovered_staging {
                if flush_entry.page_size > 0 {
                    let current_ps = cache.page_size.load(Ordering::Relaxed);
                    if current_ps == 0 || (current_ps == 4096 && flush_entry.page_size != 4096) {
                        cache.set_page_size(flush_entry.page_size);
                        if manifest.page_size == 0 {
                            manifest.page_size = flush_entry.page_size;
                        }
                        if manifest.pages_per_group == 0 {
                            manifest.pages_per_group = ppg;
                        }
                    }
                }
                if let Ok(pages) =
                    staging::read_staging_log(&flush_entry.staging_path, flush_entry.page_size)
                {
                    for (page_num, data) in &pages {
                        let _ = cache.write_page(*page_num, data);
                        let new_count = *page_num + 1;
                        if new_count > manifest.page_count {
                            manifest.page_count = new_count;
                        }
                    }
                }
            }

            // For local mode, we clean staging logs after cache replay;
            // remote mode keeps them until the background flush runs.
            if is_local {
                for flush_entry in &recovered_staging {
                    staging::remove_staging_log(&flush_entry.staging_path);
                }
            }
        }

        let cache = Arc::new(cache);
        let page_count = Arc::new(AtomicU64::new(manifest.page_count));
        let shared_manifest = Arc::new(ArcSwap::from_pointee(manifest));
        let initial_dirty: HashSet<u64> = recovered_dirty_groups.into_iter().collect();
        if !initial_dirty.is_empty() {
            turbolite_debug!(
                "[tiered] recovered {} dirty groups pending flush",
                initial_dirty.len(),
            );
        }
        let shared_dirty_groups = Arc::new(Mutex::new(initial_dirty));

        let staging_seq = Arc::new(AtomicU64::new(max_recovered_version + 1));
        let pending_flushes = if is_local {
            // Staging logs already replayed into cache and deleted above.
            Arc::new(Mutex::new(Vec::new()))
        } else {
            Arc::new(Mutex::new(recovered_staging))
        };
        let flush_lock = Arc::new(Mutex::new(()));
        let replay_gate: Arc<parking_lot::RwLock<()>> = Arc::new(parking_lot::RwLock::new(()));
        let replay_epoch = Arc::new(AtomicU64::new(0));

        // Prefetch pool: backend-agnostic, runs for every non-local setup
        // (local has no remote I/O to parallelise, so skip the worker
        // threads entirely).
        let prefetch_pool = if is_local {
            None
        } else {
            Some(Arc::new(PrefetchPool::new(
                config.prefetch.threads,
                Arc::clone(&storage),
                runtime.clone(),
                Arc::clone(&cache),
                ppg,
                Arc::clone(&page_count),
                #[cfg(feature = "zstd")]
                config.compression.dictionary.clone(),
                config.encryption.key,
                Arc::clone(&shared_manifest),
                Arc::clone(&replay_gate),
                Arc::clone(&replay_epoch),
            )))
        };

        Ok(Self {
            storage,
            runtime,
            is_local,
            _owned_runtime: owned_runtime,
            cache,
            prefetch_pool,
            page_count,
            config,
            shared_manifest,
            local_checkpoint_only,
            shared_dirty_groups,
            pending_flushes,
            staging_seq,
            flush_lock,
            replay_gate,
            replay_epoch,
            #[cfg(test)]
            finalize_pause: parking_lot::Mutex::new(None),
            #[cfg(feature = "wal")]
            wal_state: std::sync::Mutex::new(wal_replication::WalReplicationState::new()),
        })
    }

    /// Load manifest based on ManifestSource config.
    /// Returns (manifest, recovered_dirty_groups, was_warm_reconnect).
    fn load_manifest(&self) -> io::Result<(Manifest, Vec<u64>, bool)> {
        let existing = self.shared_manifest.load();
        let has_loaded = existing.page_count > 0 || existing.version > 0;
        drop(existing);

        match self.config.prefetch.manifest_source {
            ManifestSource::Auto => {
                if has_loaded {
                    let m = (**self.shared_manifest.load()).clone();
                    turbolite_debug!(
                        "[tiered] using in-memory manifest (warm reconnect, v{})",
                        m.version,
                    );
                    return Ok((m, Vec::new(), true));
                }
                if let Some(mut m) = manifest::load_manifest_local(&self.config.cache_dir)? {
                    let dirty = manifest::load_dirty_groups(&self.config.cache_dir)?;
                    turbolite_debug!(
                        "[tiered] loaded local manifest (v{}, {} pages, {} dirty)",
                        m.version,
                        m.page_count,
                        dirty.len(),
                    );
                    m.build_page_index();
                    return Ok((m, dirty, false));
                }
                turbolite_debug!("[tiered] no local manifest, fetching from backend...");
                let m = storage_helpers::get_manifest(self.storage.as_ref(), &self.runtime)?
                    .unwrap_or_else(Manifest::empty);
                turbolite_debug!(
                    "[tiered] manifest fetched (v{}, {} pages)",
                    m.version,
                    m.page_count,
                );
                Ok((m, Vec::new(), false))
            }
            ManifestSource::Remote | ManifestSource::S3 => {
                turbolite_debug!(
                    "[tiered] fetching manifest from backend (manifest_source=Remote)",
                );
                let m = storage_helpers::get_manifest(self.storage.as_ref(), &self.runtime)?
                    .unwrap_or_else(Manifest::empty);
                Ok((m, Vec::new(), false))
            }
        }
    }

    /// Get a shared-state handle for cache control, `flush_to_storage`, and
    /// SQL functions.
    ///
    /// Total: every `TurboliteVfs` is constructed with a concrete backend
    /// (local mode wraps `LocalStorage`; remote mode takes an
    /// `Arc<dyn StorageBackend>` from the caller), so a
    /// [`TurboliteSharedState`] is always returnable. Phase Cirrus e removed
    /// the old panic path from pre-Anvil-g callers; the type system now
    /// carries the invariant — no `Option`, no `expect`.
    pub fn shared_state(&self) -> bench::TurboliteSharedState {
        TurboliteSharedState {
            storage: Arc::clone(&self.storage),
            runtime: self.runtime.clone(),
            is_local: self.is_local,
            cache: Arc::clone(&self.cache),
            prefetch_pool: self.prefetch_pool.as_ref().map(Arc::clone),
            shared_manifest: Arc::clone(&self.shared_manifest),
            shared_dirty_groups: Arc::clone(&self.shared_dirty_groups),
            pending_flushes: Arc::clone(&self.pending_flushes),
            flush_lock: Arc::clone(&self.flush_lock),
            compression_level: self.config.compression.level,
            #[cfg(feature = "zstd")]
            dictionary: self.config.compression.dictionary.clone(),
            encryption_key: self.config.encryption.key,
            gc_enabled: self.config.cache.gc_enabled,
            override_threshold: self.config.cache.override_threshold,
            compaction_threshold: self.config.cache.compaction_threshold,
        }
    }

    /// Evict non-interior pages from disk cache. Interior pages and group 0
    /// (schema + root page) stay warm.
    ///
    /// Safe to call with pending flush: groups awaiting upload are protected
    /// (their pages remain in the disk cache bitmap).
    pub fn clear_cache(&self) {
        if let Some(ref pool) = self.prefetch_pool {
            pool.wait_idle();
        }

        let pinned_pages = self.cache.interior_pages.lock().clone();
        let pending_groups = self.pending_group_pages();

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

        {
            let index_pages = self.cache.index_pages.lock().clone();
            let gp = self.cache.group_pages.read();
            let bitmap = self.cache.bitmap.read();
            for b in &bitmap.bits {
                b.store(0, std::sync::atomic::Ordering::Relaxed);
            }

            let mut keep_pages: HashSet<u64> = HashSet::new();
            for &page in &pinned_pages {
                bitmap.mark_present(page);
                keep_pages.insert(page);
            }
            for &page in &index_pages {
                bitmap.mark_present(page);
                keep_pages.insert(page);
            }
            for pages in pending_groups.values() {
                for &p in pages {
                    bitmap.mark_present(p);
                    keep_pages.insert(p);
                }
            }
            if let Some(g0_pages) = gp.first() {
                for &p in g0_pages {
                    bitmap.mark_present(p);
                    keep_pages.insert(p);
                }
            } else {
                let ppg = self.cache.pages_per_group as u64;
                for p in 0..ppg {
                    bitmap.mark_present(p);
                    keep_pages.insert(p);
                }
            }
            let _ = bitmap.persist();

            drop(bitmap);
            self.cache.prune_cache_index(&keep_pages);
        }

        {
            let mut tracker = self.cache.tracker.lock();
            tracker.clear_data_only();
        }
    }

    /// Upload locally-checkpointed dirty pages to the backend without holding
    /// any SQLite lock.
    pub fn flush_to_storage(&self) -> io::Result<()> {
        let _guard = self.flush_lock.lock().unwrap();
        flush::flush_dirty_groups(
            self.storage.as_ref(),
            &self.runtime,
            self.is_local,
            &self.cache,
            &self.shared_manifest,
            &self.shared_dirty_groups,
            &self.pending_flushes,
            self.config.compression.level,
            #[cfg(feature = "zstd")]
            self.config.compression.dictionary.as_deref(),
            self.config.encryption.key,
            self.config.cache.gc_enabled,
            self.config.cache.override_threshold,
            self.config.cache.compaction_threshold,
        )
    }

    /// Returns true if there are dirty groups or staging logs pending upload.
    pub fn has_pending_flush(&self) -> bool {
        !self.shared_dirty_groups.lock().unwrap().is_empty()
            || !self.pending_flushes.lock().unwrap().is_empty()
    }

    /// Get page numbers for all groups pending upload.
    fn pending_group_pages(&self) -> HashMap<u64, Vec<u64>> {
        let pending = self.shared_dirty_groups.lock().unwrap();
        if pending.is_empty() {
            return HashMap::new();
        }
        let manifest = self.shared_manifest.load();
        let mut result = HashMap::new();
        for &gid in pending.iter() {
            let pages = manifest.group_page_nums(gid).into_owned();
            result.insert(gid, pages);
        }
        result
    }

    /// Whether this VFS has remote storage (something other than the
    /// built-in local filesystem). Used to gate WAL stub creation, eager
    /// fetches, and other behaviour that only makes sense when there is a
    /// remote source of truth.
    pub fn has_remote_storage(&self) -> bool {
        !self.is_local
    }

    /// Garbage collect orphaned backend objects not referenced by the current
    /// manifest. Returns the number of objects deleted.
    pub fn gc(&self) -> io::Result<usize> {
        let manifest = storage_helpers::get_manifest(self.storage.as_ref(), &self.runtime)?
            .unwrap_or_else(Manifest::empty);
        let all_keys = storage_helpers::list_all_keys(self.storage.as_ref(), &self.runtime)?;

        let mut live_keys: HashSet<String> = HashSet::new();
        live_keys.insert(keys::MANIFEST_KEY.to_string());
        Self::add_manifest_keys_to_set(&manifest, &mut live_keys);

        let orphans: Vec<String> = all_keys
            .into_iter()
            .filter(|k| !live_keys.contains(k))
            .collect();

        let count = orphans.len();
        if count > 0 {
            turbolite_debug!("[gc] deleting {} orphaned objects...", count);
            storage_helpers::delete_objects(self.storage.as_ref(), &self.runtime, &orphans)?;
            turbolite_debug!("[gc] deleted {} orphaned objects", count);
        }
        Ok(count)
    }

    /// Add all page group, interior chunk, and index chunk keys from a
    /// manifest to a live key set.
    fn add_manifest_keys_to_set(manifest: &Manifest, live_keys: &mut HashSet<String>) {
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
        for overrides in &manifest.subframe_overrides {
            for ovr in overrides.values() {
                live_keys.insert(ovr.key.clone());
            }
        }
    }

    /// Validate the database against the backend.
    pub fn validate(&self) -> io::Result<super::ValidateResult> {
        let manifest = (**self.shared_manifest.load()).clone();
        let all_keys: HashSet<String> =
            storage_helpers::list_all_keys(self.storage.as_ref(), &self.runtime)?
                .into_iter()
                .collect();

        let mut live_keys: HashSet<String> = HashSet::new();

        let mut pg_missing = Vec::new();
        for key in &manifest.page_group_keys {
            live_keys.insert(key.clone());
            if !key.is_empty() && !all_keys.contains(key) {
                pg_missing.push(key.clone());
            }
        }
        let pg_present = manifest
            .page_group_keys
            .iter()
            .filter(|k| !k.is_empty())
            .count()
            - pg_missing.len();

        let mut int_missing = Vec::new();
        for key in manifest.interior_chunk_keys.values() {
            live_keys.insert(key.clone());
            if !all_keys.contains(key) {
                int_missing.push(key.clone());
            }
        }
        let int_present = manifest.interior_chunk_keys.len() - int_missing.len();

        let mut idx_missing = Vec::new();
        for key in manifest.index_chunk_keys.values() {
            live_keys.insert(key.clone());
            if !all_keys.contains(key) {
                idx_missing.push(key.clone());
            }
        }
        let idx_present = manifest.index_chunk_keys.len() - idx_missing.len();

        for overrides in &manifest.subframe_overrides {
            for ovr in overrides.values() {
                live_keys.insert(ovr.key.clone());
            }
        }
        live_keys.insert(keys::MANIFEST_KEY.to_string());

        let orphaned: Vec<String> = all_keys.difference(&live_keys).cloned().collect();

        // Data decode check
        let mut decode_errors = Vec::new();
        let page_size = manifest.page_size;
        let page_count = manifest.page_count;
        let sub_ppf = manifest.sub_pages_per_frame;
        let encryption_key = self.config.encryption.key.as_ref();

        let total_pg = manifest
            .page_group_keys
            .iter()
            .filter(|k| !k.is_empty())
            .count();
        let mut processed = 0usize;
        for (gid, key) in manifest.page_group_keys.iter().enumerate() {
            if key.is_empty() || pg_missing.contains(key) {
                continue;
            }
            match storage_helpers::get_page_group(self.storage.as_ref(), &self.runtime, key) {
                Ok(Some(data)) => {
                    let ft = manifest.frame_tables.get(gid);
                    let pages_in_group = manifest.group_page_nums(gid as u64).len() as u32;
                    if sub_ppf > 0 {
                        if let Some(ft) = ft {
                            if !ft.is_empty() {
                                if let Err(e) = decode_page_group_seekable_full(
                                    &data,
                                    ft,
                                    page_size,
                                    pages_in_group,
                                    page_count,
                                    0,
                                    #[cfg(feature = "zstd")]
                                    None,
                                    encryption_key,
                                ) {
                                    decode_errors.push((key.clone(), format!("{}", e)));
                                }
                            }
                        }
                    } else if let Err(e) = decode_page_group(
                        &data,
                        #[cfg(feature = "zstd")]
                        None,
                        encryption_key,
                    ) {
                        decode_errors.push((key.clone(), format!("{}", e)));
                    }
                }
                Ok(None) => {
                    decode_errors.push((key.clone(), "object returned empty".to_string()));
                }
                Err(e) => {
                    decode_errors.push((key.clone(), format!("fetch: {}", e)));
                }
            }
            processed += 1;
            if processed % 20 == 0 || processed == total_pg {
                eprintln!("  decoded {}/{} page groups", processed, total_pg);
            }
        }

        for (chunk_id, key) in &manifest.interior_chunk_keys {
            if int_missing.contains(key) {
                continue;
            }
            match storage_helpers::get_page_group(self.storage.as_ref(), &self.runtime, key) {
                Ok(Some(data)) => {
                    if let Err(e) = decode_interior_bundle(
                        &data,
                        #[cfg(feature = "zstd")]
                        None,
                        encryption_key,
                    ) {
                        decode_errors
                            .push((key.clone(), format!("interior chunk {}: {}", chunk_id, e)));
                    }
                }
                Ok(None) => {
                    decode_errors
                        .push((key.clone(), format!("interior chunk {}: empty", chunk_id)));
                }
                Err(e) => {
                    decode_errors.push((
                        key.clone(),
                        format!("interior chunk {} fetch: {}", chunk_id, e),
                    ));
                }
            }
        }

        for (chunk_id, key) in &manifest.index_chunk_keys {
            if idx_missing.contains(key) {
                continue;
            }
            match storage_helpers::get_page_group(self.storage.as_ref(), &self.runtime, key) {
                Ok(Some(data)) => {
                    if let Err(e) = decode_interior_bundle(
                        &data,
                        #[cfg(feature = "zstd")]
                        None,
                        encryption_key,
                    ) {
                        decode_errors
                            .push((key.clone(), format!("index chunk {}: {}", chunk_id, e)));
                    }
                }
                Ok(None) => {
                    decode_errors.push((key.clone(), format!("index chunk {}: empty", chunk_id)));
                }
                Err(e) => {
                    decode_errors.push((
                        key.clone(),
                        format!("index chunk {} fetch: {}", chunk_id, e),
                    ));
                }
            }
        }

        Ok(super::ValidateResult {
            manifest_version: manifest.version,
            page_groups_total: manifest
                .page_group_keys
                .iter()
                .filter(|k| !k.is_empty())
                .count(),
            page_groups_present: pg_present,
            page_groups_missing: pg_missing,
            interior_chunks_total: manifest.interior_chunk_keys.len(),
            interior_chunks_present: int_present,
            interior_chunks_missing: int_missing,
            index_chunks_total: manifest.index_chunk_keys.len(),
            index_chunks_present: idx_present,
            index_chunks_missing: idx_missing,
            orphaned_keys: orphaned,
            decode_errors,
        })
    }

    /// Seed this VFS's backend with a manifest from another source.
    pub fn seed_manifest(&self, manifest: &Manifest) -> io::Result<()> {
        storage_helpers::put_manifest(self.storage.as_ref(), &self.runtime, manifest)?;
        manifest::persist_manifest_local(&self.config.cache_dir, manifest)?;
        Ok(())
    }

    /// True if the backend already has a canonical turbolite manifest.
    pub fn remote_manifest_exists(&self) -> io::Result<bool> {
        storage_helpers::manifest_exists(self.storage.as_ref(), &self.runtime)
    }

    /// Import a local SQLite file into this VFS's backend as checkpointed base state.
    ///
    /// This is the fresh-bootstrap path for Turbolite + WAL-delta deployments:
    /// the caller already has a committed SQLite file on disk, and turbolite must
    /// publish that file as page-group base state before walrust starts shipping
    /// deltas against it.
    pub fn import_sqlite_file(&self, file_path: &Path) -> io::Result<Manifest> {
        let mut manifest = import::import_sqlite_file(
            &self.config,
            Arc::clone(&self.storage),
            self.runtime.clone(),
            file_path,
        )?;
        manifest.detect_and_normalize_strategy();
        let current_version = self.manifest().version;
        if manifest.version <= current_version {
            manifest.version = current_version + 1;
        }
        self.set_manifest(manifest.clone());
        manifest::persist_manifest_local(&self.config.cache_dir, &manifest)?;
        Ok(manifest)
    }

    /// Delete every object the backend knows about. Backend-agnostic
    /// wipe; previously `destroy_s3`.
    pub fn destroy_remote(&self) -> io::Result<()> {
        let all_keys = storage_helpers::list_all_keys(self.storage.as_ref(), &self.runtime)?;
        if all_keys.is_empty() {
            return Ok(());
        }
        storage_helpers::delete_objects(self.storage.as_ref(), &self.runtime, &all_keys)?;
        Ok(())
    }

    /// Legacy alias for the old S3-owned API.
    #[deprecated(since = "0.2.0", note = "use destroy_remote")]
    pub fn destroy_s3(&self) -> io::Result<()> {
        self.destroy_remote()
    }

    /// Return a clone of the current manifest state.
    pub fn manifest(&self) -> Manifest {
        (**self.shared_manifest.load()).clone()
    }

    /// Serialize the current manifest to the turbolite wire format, for
    /// publication through a `turbodb::ManifestStore` (as the envelope's
    /// opaque `payload`). See `src/tiered/wire.rs` for the tag byte
    /// layout. Consumers (haqlite, haqlite-turbolite) never interpret
    /// these bytes — they pipe them through as-is.
    pub fn manifest_bytes(&self) -> io::Result<Vec<u8>> {
        let m = self.manifest();
        super::wire::encode_pure(&m)
    }

    /// Decode wire bytes produced by `manifest_bytes` and apply the resulting
    /// page/base manifest via `set_manifest()`.
    pub fn set_manifest_bytes(&self, bytes: &[u8]) -> io::Result<()> {
        let manifest = super::wire::decode(bytes)?;
        self.set_manifest(manifest);
        Ok(())
    }

    /// Decode manifest wire bytes without mutating local VFS state.
    ///
    /// Useful for higher layers that need to inspect the currently published
    /// base manifest before deciding what to publish next.
    pub fn decode_manifest_bytes(bytes: &[u8]) -> io::Result<Manifest> {
        super::wire::decode(bytes)
    }

    /// Clone of the VFS-level replay gate. Higher layers (haqlite-turbolite
    /// follower apply) take the write half around out-of-band cache writes
    /// like `materialize_to_file` so in-flight VFS reads (which take the
    /// read half on `xLock(SHARED)`) cannot observe a torn cache file.
    pub fn replay_gate(&self) -> Arc<parking_lot::RwLock<()>> {
        Arc::clone(&self.replay_gate)
    }

    /// Fetch the latest manifest from the backend and apply it via `set_manifest`.
    /// Returns the new manifest version, or None if no manifest exists.
    /// Used by HA followers to catch up from the leader's turbolite state.
    pub fn fetch_and_apply_remote_manifest(&self) -> io::Result<Option<u64>> {
        match storage_helpers::get_manifest(self.storage.as_ref(), &self.runtime)? {
            Some(manifest) => {
                let version = manifest.version;
                self.set_manifest(manifest);
                Ok(Some(version))
            }
            None => Ok(None),
        }
    }

    /// Update the internal manifest from external state (e.g. haqlite catch-up).
    pub fn set_manifest(&self, mut manifest: Manifest) {
        manifest.detect_and_normalize_strategy();

        // Refuse to downgrade. The manifest version is monotonically increasing.
        {
            let current = self.shared_manifest.load();
            if manifest.version > 0 && current.version > 0 && manifest.version <= current.version {
                if manifest.version < current.version {
                    turbolite_debug!(
                        "[set_manifest] REJECTED: incoming v{} < current v{} (would downgrade)",
                        manifest.version,
                        current.version,
                    );
                }
                return;
            }
        }

        let old_keys: Vec<String> = {
            let old = self.shared_manifest.load();
            old.page_group_keys.clone()
        };

        if !manifest.group_pages.is_empty() {
            self.cache.set_group_pages(manifest.group_pages.clone());
            self.cache.ensure_group_capacity(manifest.group_pages.len());
        }

        let new_keys = &manifest.page_group_keys;
        let max_len = std::cmp::max(old_keys.len(), new_keys.len());
        let mut changed_groups: Vec<u64> = (0..max_len)
            .filter(|&gid| {
                let old_key = old_keys.get(gid).map(|s| s.as_str());
                let new_key = new_keys.get(gid).map(|s| s.as_str());
                old_key != new_key
            })
            .map(|gid| gid as u64)
            .collect();
        let old_version = {
            let old = self.shared_manifest.load();
            old.version
        };
        if manifest.version != old_version && manifest.version > 0 {
            for (gid, ovs) in manifest.subframe_overrides.iter().enumerate() {
                if !ovs.is_empty() && !changed_groups.contains(&(gid as u64)) {
                    changed_groups.push(gid as u64);
                }
            }
        }

        if !changed_groups.is_empty() {
            turbolite_debug!(
                "[set_manifest] evicting {} changed groups",
                changed_groups.len(),
            );
        }
        for gid in &changed_groups {
            self.cache.evict_group(*gid);
        }

        if let Some(ref page0) = manifest.db_header {
            let _ = self.cache.write_page(0, page0);
        }

        self.page_count
            .store(manifest.page_count, Ordering::Release);

        self.shared_manifest.store(Arc::new(manifest.clone()));

        if let Err(e) = super::manifest::persist_manifest_local(&self.config.cache_dir, &manifest) {
            eprintln!(
                "[set_manifest] warning: failed to persist manifest locally: {}",
                e,
            );
        }
        if let Err(e) = super::manifest::persist_dirty_groups(&self.config.cache_dir, &[]) {
            eprintln!(
                "[set_manifest] warning: failed to clear dirty_groups: {}",
                e,
            );
        }

        self.cache.bump_generation();

        if let Err(e) = self.cache.persist_bitmap() {
            eprintln!("[set_manifest] warning: failed to persist bitmap: {}", e);
        }
    }

    /// Get the path to the raw local database image.
    pub fn cache_file_path(&self) -> PathBuf {
        self.cache.cache_file_path.clone()
    }

    fn local_main_db_path(&self, db: &str) -> io::Result<PathBuf> {
        if let Some(data_path) = self.config.local_data_path.as_ref() {
            let requested = Path::new(db);
            let requested_has_parent = requested
                .parent()
                .is_some_and(|parent| parent != Path::new(""));
            let requested_matches = if requested_has_parent || requested.is_absolute() {
                requested == data_path
            } else {
                // A bare "app.db" is only unambiguous when the configured
                // data path is also bare. If the VFS is bound to
                // /data/app.db, callers must open that same path; basename-only
                // matching would alias unrelated cwd-local files onto it.
                data_path
                    .parent()
                    .is_none_or(|parent| parent == Path::new(""))
                    && requested.file_name() == data_path.file_name()
            };
            if !requested_matches {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "file-first Turbolite VFS is bound to {}; refusing open of {}",
                        data_path.display(),
                        db
                    ),
                ));
            }
            Ok(data_path.clone())
        } else {
            Ok(self.config.cache_dir.join(db))
        }
    }

    fn is_file_first_main_db_name(&self, db: &str) -> bool {
        self.config
            .local_data_path
            .as_ref()
            .is_some_and(|_| self.local_main_db_path(db).is_ok())
    }

    fn is_file_first_main_db_basename(&self, db: &str) -> bool {
        let Some(data_path) = self.config.local_data_path.as_ref() else {
            return false;
        };
        Path::new(db).file_name() == data_path.file_name()
    }

    fn local_sqlite_companion_path(&self, db: &str) -> PathBuf {
        if let Some(data_path) = self.config.local_data_path.as_ref() {
            let name = Path::new(db)
                .file_name()
                .map(|name| name.to_owned())
                .unwrap_or_else(|| db.into());
            data_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(name)
        } else {
            self.config.cache_dir.join(db)
        }
    }

    /// Replace the local cache contents from a materialized SQLite file.
    ///
    /// Used by external restore flows that rebuild a plain SQLite database file
    /// first, then need turbolite's own cache file descriptor and in-memory page
    /// state to adopt those bytes before the next VFS-backed connection opens.
    pub fn replace_cache_from_sqlite_file(&self, input: &Path) -> io::Result<()> {
        use std::os::unix::fs::FileExt;

        let manifest = self.manifest();
        if manifest.page_count == 0 || manifest.page_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "empty manifest, no data to restore into cache",
            ));
        }

        let page_size = manifest.page_size as usize;
        let mut page = vec![0u8; page_size];
        let input_file = std::fs::File::open(input)?;
        for page_num in 0..manifest.page_count {
            input_file.read_exact_at(&mut page, page_num * page_size as u64)?;
            self.cache.write_page(page_num, &page)?;
        }

        self.cache.mark_all_pages_present(manifest.page_count);
        self.page_count
            .store(manifest.page_count, Ordering::Release);
        self.cache.bump_generation();
        if let Err(e) = self.cache.persist_bitmap() {
            eprintln!(
                "[replace_cache_from_sqlite_file] warning: failed to persist bitmap: {}",
                e,
            );
        }
        Ok(())
    }

    /// Sync VFS state after an external process (walrust restore) wrote pages
    /// directly to the cache file.
    pub fn sync_after_external_restore(&self, page_count: u64) {
        // The external restore rewrote data.cache behind the VFS's back. Any
        // in-memory decoded pages from an earlier manifest version are now stale
        // and must be dropped so the next VFS open/read sees the restored bytes.
        let page_nums: Vec<u64> = (0..page_count).collect();
        self.cache.clear_pages_from_mem_cache(&page_nums);
        self.cache.bump_generation();
        self.cache.mark_all_pages_present(page_count);
        self.page_count.store(page_count, Ordering::Release);
        if let Err(e) = self.cache.persist_bitmap() {
            eprintln!(
                "[sync_after_external_restore] warning: failed to persist bitmap: {}",
                e,
            );
        }
    }

    /// Begin a direct hybrid page replay cycle.
    ///
    /// Returns a [`ReplayHandle`] that the caller drives with
    /// `apply_page` (per decoded HADBP page from walrust),
    /// `commit_changeset(seq)` (per S3 object), then exactly one of
    /// `finalize` (success) or `abort` (any failure). On finalize the
    /// staged pages are atomically installed into the live cache and
    /// emitted as a staging log under `pending_flushes` so the next
    /// `flush_to_storage` / `publish_replayed_base` turns them into
    /// uploaded page groups.
    ///
    /// See `crate::tiered::replay` for the full lifecycle contract.
    pub fn begin_replay(&self) -> io::Result<crate::tiered::replay::ReplayHandle> {
        let staging_dir = self.config.cache_dir.join("staging");
        std::fs::create_dir_all(&staging_dir)?;
        let ctx = crate::tiered::replay::ReplayContext {
            cache: self.cache.clone(),
            shared_manifest: self.shared_manifest.clone(),
            vfs_page_count: self.page_count.clone(),
            pending_flushes: self.pending_flushes.clone(),
            staging_seq: self.staging_seq.clone(),
            flush_lock: self.flush_lock.clone(),
            replay_gate: self.replay_gate.clone(),
            replay_epoch: self.replay_epoch.clone(),
            staging_dir,
            #[cfg(test)]
            finalize_pause: {
                let guard = self.finalize_pause.lock();
                guard.clone()
            },
        };
        crate::tiered::replay::ReplayHandle::new(ctx)
    }

    /// Test-only deterministic pause hook for `ReplayHandle::finalize`.
    ///
    /// When a barrier is installed, finalize will call `barrier.wait()`
    /// twice during the commit phase: once after acquiring
    /// `replay_gate.write()` but before any page writes (so the test
    /// can release/observe the pre-replay state under contention) and
    /// once at the end of the commit just before releasing the write
    /// guard (so the test can observe finalize-just-finished state).
    /// Setting `None` removes the pause for subsequent finalize calls.
    #[cfg(test)]
    pub fn install_finalize_pause_for_test(&self, barrier: Option<Arc<std::sync::Barrier>>) {
        *self.finalize_pause.lock() = barrier;
    }

    /// Promote accumulated replayed state to a remote manifest payload.
    ///
    /// Drives `flush_dirty_groups` over whatever staging logs and
    /// dirty groups have accumulated since the last flush, encodes
    /// the resulting manifest as pure page/base manifest bytes, and
    /// returns those bytes for the caller to CAS-publish under its
    /// manifest key. If no replay state is pending, the call still
    /// returns the current pure manifest payload — that covers the
    /// "follower already caught up before promotion" case.
    pub fn publish_replayed_base(&self) -> io::Result<Vec<u8>> {
        // Drive a flush over any pending replay state. flush_to_storage
        // drains pending_flushes + shared_dirty_groups together and
        // resolves dirty groups via manifest.page_location, which
        // works for both Positional and BTreeAware manifests because
        // ReplayHandle::finalize already extended page_count and
        // group_pages before emitting the staging log.
        let has_pending = self
            .pending_flushes
            .lock()
            .map(|g| !g.is_empty())
            .unwrap_or(false)
            || self
                .shared_dirty_groups
                .lock()
                .map(|g| !g.is_empty())
                .unwrap_or(false);
        if has_pending {
            self.flush_to_storage()?;
        }
        // Whether we flushed or not, the in-memory manifest is the
        // source of truth for the published payload. Delta replay is
        // discoverable from the integration layer's configured delta
        // storage, not encoded into the page/base manifest.
        self.manifest_bytes()
    }
}

impl Vfs for TurboliteVfs {
    type Handle = TurboliteHandle;

    fn open(&self, db: &str, opts: OpenOptions) -> Result<Self::Handle, io::Error> {
        let path = if matches!(opts.kind, OpenKind::MainDb) {
            self.local_main_db_path(db)?
        } else {
            self.local_sqlite_companion_path(db)
        };

        if matches!(opts.kind, OpenKind::MainDb) {
            let (mut manifest, recovered_dirty_groups, warm_reconnect) = self.load_manifest()?;
            manifest.detect_and_normalize_strategy();

            let ppg = if manifest.pages_per_group > 0 {
                manifest.pages_per_group
            } else {
                self.config.cache.pages_per_group
            };

            // Validate cache against manifest.
            {
                let old_manifest = (**self.shared_manifest.load()).clone();
                if old_manifest.version > 0 && old_manifest.version != manifest.version {
                    if old_manifest.version > manifest.version {
                        turbolite_debug!(
                            "[cache-validate] local v{} ahead of manifest v{}, full invalidation",
                            old_manifest.version,
                            manifest.version,
                        );
                        let states = self.cache.group_states.lock();
                        for state in states.iter() {
                            state.store(GroupState::None as u8, Ordering::Release);
                        }
                        self.cache.group_condvar.notify_all();
                    } else {
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
                            turbolite_debug!(
                                "[cache-validate] manifest v{} -> v{}: invalidated {} groups",
                                old_manifest.version,
                                manifest.version,
                                invalidated,
                            );
                        }
                    }
                }
            }

            if !manifest.group_pages.is_empty() {
                self.cache.set_group_pages(manifest.group_pages.clone());
                self.cache.ensure_group_capacity(manifest.group_pages.len());
            }

            if !warm_reconnect {
                self.shared_manifest.store(Arc::new(manifest));
            }

            if !recovered_dirty_groups.is_empty() {
                let mut pending = self.shared_dirty_groups.lock().unwrap();
                let count = recovered_dirty_groups.len();
                pending.extend(recovered_dirty_groups);
                turbolite_debug!(
                    "[tiered] recovered {} dirty groups from local manifest",
                    count,
                );
            }

            let lock_path = if let Some(data_path) = self.config.local_data_path.as_ref() {
                data_path.with_file_name(format!(
                    "{}-lock",
                    data_path
                        .file_name()
                        .map(|name| name.to_string_lossy())
                        .unwrap_or_else(|| db.into())
                ))
            } else {
                let lock_dir = self.config.cache_dir.join("locks");
                fs::create_dir_all(&lock_dir)?;
                lock_dir.join(db)
            };
            if let Some(parent) = lock_path.parent() {
                fs::create_dir_all(parent)?;
            }
            FsOpenOptions::new()
                .create(true)
                .write(true)
                .open(&lock_path)?;

            if !self.config.read_only {
                let wal_path = if let Some(data_path) = self.config.local_data_path.as_ref() {
                    data_path.with_file_name(format!(
                        "{}-wal",
                        data_path
                            .file_name()
                            .map(|name| name.to_string_lossy())
                            .unwrap_or_else(|| db.into())
                    ))
                } else {
                    self.config.cache_dir.join(format!("{}-wal", db))
                };
                if let Some(parent) = wal_path.parent() {
                    let _ = fs::create_dir_all(parent);
                }
                let _ = FsOpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(&wal_path);
            }

            // WAL replication (walrust) is wired elsewhere; the VFS just
            // kicks off the background task when `wal` feature is on.
            #[cfg(feature = "wal")]
            if self.config.wal.replication {
                let mut wal = self.wal_state.lock().unwrap();
                if !wal.is_started() {
                    let _ = (db, &mut *wal);
                    // The WAL loop used to be constructed here from
                    // config.bucket/prefix/endpoint_url/region, fields
                    // that no longer exist. Wiring WAL replication
                    // through the new StorageBackend injection happens
                    // in the WAL follow-up (see wal_replication.rs
                    // TODO). Keep the stub so WAL builds compile.
                    turbolite_debug!(
                        "[tiered] WAL replication requested but not wired in this build"
                    );
                }
            }

            TurboliteHandle::new_tiered(
                Some(Arc::clone(&self.storage)),
                Some(self.runtime.clone()),
                Arc::clone(&self.cache),
                Arc::clone(&self.shared_manifest),
                Arc::clone(&self.shared_dirty_groups),
                Arc::clone(&self.pending_flushes),
                Arc::clone(&self.staging_seq),
                lock_path,
                ppg,
                self.config.compression.level,
                self.config.read_only,
                self.config.prefetch.search.clone(),
                self.config.prefetch.lookup.clone(),
                self.prefetch_pool.as_ref().map(Arc::clone),
                self.config.cache.gc_enabled,
                #[cfg(feature = "zstd")]
                self.config.compression.dictionary.as_deref(),
                self.config.encryption.key,
                self.config.prefetch.query_plan,
                self.config.cache.max_bytes,
                self.config.cache.evict_on_checkpoint,
                self.config.cache.override_threshold,
                self.config.cache.compaction_threshold,
                self.is_local,
                self.local_checkpoint_only,
                Arc::clone(&self.replay_gate),
                Arc::clone(&self.replay_epoch),
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
            Ok(TurboliteHandle::new_passthrough(
                file,
                path,
                self.config.encryption.key,
            ))
        }
    }

    fn delete(&self, db: &str) -> Result<(), io::Error> {
        if self.is_file_first_main_db_name(db) || self.is_file_first_main_db_basename(db) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("refusing to delete Turbolite main database image {db}"),
            ));
        }
        let path = self.local_sqlite_companion_path(db);
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        Ok(())
    }

    fn exists(&self, db: &str) -> Result<bool, io::Error> {
        if self.config.local_data_path.is_some() {
            if let Ok(path) = self.local_main_db_path(db) {
                if path.exists() {
                    return Ok(true);
                }
                return storage_helpers::manifest_exists(self.storage.as_ref(), &self.runtime);
            }
            if self.is_file_first_main_db_basename(db) {
                return Ok(false);
            }
        }
        let path = self.local_sqlite_companion_path(db);
        if path.exists() {
            return Ok(true);
        }

        if db.ends_with("-wal") || db.ends_with("-journal") || db.ends_with("-shm") {
            return Ok(false);
        }

        // Fall back to checking whether the backend has a manifest.
        storage_helpers::manifest_exists(self.storage.as_ref(), &self.runtime)
    }

    fn temporary_name(&self) -> String {
        format!("temp_{}", std::process::id())
    }

    fn random(&self, buffer: &mut [i8]) {
        use std::time::SystemTime;
        let mut seed = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("SystemTime::now before UNIX_EPOCH")
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

impl Drop for TurboliteVfs {
    fn drop(&mut self) {
        // Drop prefetch pool first so worker threads join before the
        // tokio runtime is shut down. Then pause briefly to let reqwest's
        // background connection-pool tasks finish gracefully. Without the
        // sleep, aborted in-flight connections log noisy hyper
        // IncompleteMessage errors during teardown (harmless but distracting).
        self.prefetch_pool = None;
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

#[cfg(test)]
#[path = "test_local_vfs.rs"]
mod local_vfs_tests;
