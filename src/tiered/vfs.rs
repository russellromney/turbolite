use super::*;

use hadb_storage::StorageBackend;
use hadb_storage_local::LocalStorage;

use crate::tiered::storage as storage_helpers;

// ===== TurboliteVfs =====

/// Turbolite SQLite VFS with compression, encryption, and a pluggable
/// storage backend.
///
/// turbolite is byte-agnostic below the VFS; the actual storage (filesystem,
/// S3, Cinch HTTP, etc.) is an `Arc<dyn hadb_storage::StorageBackend>`. Use
/// [`TurboliteVfs::new`] for the default local-filesystem backend rooted
/// under `config.cache_dir`, or [`TurboliteVfs::new_with_storage`] to
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
    /// Shared pending dirty groups. Accumulated by TurboliteHandle during
    /// local-only checkpoints, drained by flush_to_storage(). Legacy path
    /// for the global LOCAL_CHECKPOINT_ONLY flag; SyncMode::LocalThenFlush
    /// uses staging logs.
    shared_dirty_groups: Arc<Mutex<HashSet<u64>>>,
    /// Pending staging log flushes. Populated by sync() in
    /// LocalThenFlush mode, drained by flush_to_storage().
    pending_flushes: Arc<Mutex<Vec<staging::PendingFlush>>>,
    /// Monotonic counter for staging log filenames.
    /// Avoids version collisions when manifest version is updated by flush
    /// between two checkpoints.
    staging_seq: Arc<AtomicU64>,
    /// Serialises flush_to_storage() calls. Prevents two concurrent flushes
    /// from racing on version numbers and storage keys. Also prevents a
    /// durable-mode checkpoint from interleaving with a flush in progress.
    flush_lock: Arc<Mutex<()>>,
    /// WAL replication state (started lazily on first MainDb open).
    #[cfg(feature = "wal")]
    wal_state: std::sync::Mutex<wal_replication::WalReplicationState>,
}

impl TurboliteVfs {
    /// Create a new VFS backed by `hadb_storage_local::LocalStorage`
    /// rooted at `config.cache_dir`. Spins up a dedicated 2-thread tokio
    /// runtime for backend calls.
    pub fn new(config: TurboliteConfig) -> io::Result<Self> {
        let owned = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("build tokio rt: {e}")))?;
        let runtime = owned.handle().clone();

        // Ensure the cache dir exists before LocalStorage tries to write
        // under it; LocalStorage creates parents lazily on put but we
        // also store manifest.msgpack / dirty_groups.msgpack directly
        // through synchronous helpers that assume the directory is there.
        fs::create_dir_all(&config.cache_dir)?;

        let storage: Arc<dyn StorageBackend> = Arc::new(LocalStorage::new(&config.cache_dir));
        Self::new_inner(config, storage, runtime, Some(owned), true)
    }

    /// Create a VFS backed by a caller-supplied `StorageBackend` + tokio
    /// runtime handle. The caller's `config.sync_mode` is honoured: pick
    /// `Durable` for full remote upload on every checkpoint,
    /// `LocalThenFlush` for staged uploads via `flush_to_storage()`, or
    /// `RemotePrimary` for per-commit manifest publish.
    pub fn new_with_storage(
        config: TurboliteConfig,
        backend: Arc<dyn StorageBackend>,
        runtime: tokio::runtime::Handle,
    ) -> io::Result<Self> {
        fs::create_dir_all(&config.cache_dir)?;
        Self::new_inner(config, backend, runtime, None, false)
    }

    /// Shared constructor path. `owned_runtime` is Some only when `new()`
    /// built its own runtime.
    fn new_inner(
        mut config: TurboliteConfig,
        storage: Arc<dyn StorageBackend>,
        runtime: tokio::runtime::Handle,
        owned_runtime: Option<tokio::runtime::Runtime>,
        is_local: bool,
    ) -> io::Result<Self> {
        if is_local {
            // Local mode has no remote to upload to; staging logs drain to
            // the same filesystem. LocalThenFlush is the only coherent
            // mode here.
            config.sync_mode = SyncMode::LocalThenFlush;
        }

        // 1. Decide which manifest to start from.
        //    Warm cache file wins unless it's missing, in which case we
        //    go to the backend. Dirty groups survive independently in
        //    `dirty_groups.msgpack`.
        let local_manifest = manifest::load_manifest_local(&config.cache_dir)?;
        let recovered_dirty_groups = manifest::load_dirty_groups(&config.cache_dir)?;

        let mut manifest = match local_manifest {
            Some(m) => {
                turbolite_debug!(
                    "[tiered] loaded local manifest (v{}, {} pages, {} dirty groups)",
                    m.version, m.page_count, recovered_dirty_groups.len(),
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
                            m.version, m.page_count,
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

        let page_size = if manifest.page_size > 0 { manifest.page_size } else { 4096 };
        let ppg = if manifest.pages_per_group > 0 {
            manifest.pages_per_group
        } else {
            config.cache.pages_per_group
        };

        let cache = DiskCache::new_with_compression(
            &config.cache_dir,
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
                if let Ok(Some(manifest_bytes)) = staging::read_staging_manifest(
                    &flush_entry.staging_path,
                    flush_entry.page_size,
                ) {
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
                if let Ok(pages) = staging::read_staging_log(
                    &flush_entry.staging_path,
                    flush_entry.page_size,
                ) {
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
            shared_dirty_groups,
            pending_flushes,
            staging_seq,
            flush_lock,
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
                        m.version, m.page_count, dirty.len(),
                    );
                    m.build_page_index();
                    return Ok((m, dirty, false));
                }
                turbolite_debug!("[tiered] no local manifest, fetching from backend...");
                let m = storage_helpers::get_manifest(self.storage.as_ref(), &self.runtime)?
                    .unwrap_or_else(Manifest::empty);
                turbolite_debug!(
                    "[tiered] manifest fetched (v{}, {} pages)",
                    m.version, m.page_count,
                );
                Ok((m, Vec::new(), false))
            }
            ManifestSource::Remote => {
                turbolite_debug!(
                    "[tiered] fetching manifest from backend (manifest_source=Remote)",
                );
                let m = storage_helpers::get_manifest(self.storage.as_ref(), &self.runtime)?
                    .unwrap_or_else(Manifest::empty);
                Ok((m, Vec::new(), false))
            }
        }
    }

    /// Get a shared state handle for cache control, flush_to_storage, and SQL functions.
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
    /// manifest or any snapshot manifests. Returns the number of objects
    /// deleted.
    pub fn gc(&self) -> io::Result<usize> {
        let manifest = storage_helpers::get_manifest(self.storage.as_ref(), &self.runtime)?
            .unwrap_or_else(Manifest::empty);
        let all_keys =
            storage_helpers::list_all_keys(self.storage.as_ref(), &self.runtime)?;

        let mut live_keys: HashSet<String> = HashSet::new();
        live_keys.insert(keys::MANIFEST_KEY.to_string());
        Self::add_manifest_keys_to_set(&manifest, &mut live_keys);

        let snap_prefix = "manifest-snap-";
        let snap_suffix = ".msgpack";
        for key in &all_keys {
            let filename = key.rsplit('/').next().unwrap_or(key);
            if filename.starts_with(snap_prefix) && filename.ends_with(snap_suffix) {
                live_keys.insert(key.clone());
                match storage_helpers::get_manifest_at_key(
                    self.storage.as_ref(),
                    &self.runtime,
                    key,
                ) {
                    Ok(Some(snap_manifest)) => {
                        Self::add_manifest_keys_to_set(&snap_manifest, &mut live_keys);
                    }
                    Ok(None) => {
                        turbolite_debug!(
                            "[gc] snapshot manifest {} disappeared during gc, skipping",
                            key,
                        );
                    }
                    Err(e) => {
                        eprintln!(
                            "[gc] WARNING: failed to load snapshot manifest {}: {}. Skipping.",
                            key, e,
                        );
                    }
                }
            }
        }

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
        let all_keys: HashSet<String> = storage_helpers::list_all_keys(
            self.storage.as_ref(),
            &self.runtime,
        )?
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
        let pg_present = manifest.page_group_keys.iter().filter(|k| !k.is_empty()).count() - pg_missing.len();

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

        for key in &all_keys {
            if key.contains("manifest-snap-") {
                live_keys.insert(key.clone());
            }
        }

        let orphaned: Vec<String> = all_keys.difference(&live_keys).cloned().collect();

        // Data decode check
        let mut decode_errors = Vec::new();
        let page_size = manifest.page_size;
        let page_count = manifest.page_count;
        let sub_ppf = manifest.sub_pages_per_frame;
        let encryption_key = self.config.encryption.key.as_ref();

        let total_pg = manifest.page_group_keys.iter().filter(|k| !k.is_empty()).count();
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
                                    &data, ft, page_size, pages_in_group, page_count, 0,
                                    #[cfg(feature = "zstd")] None,
                                    encryption_key,
                                ) {
                                    decode_errors.push((key.clone(), format!("{}", e)));
                                }
                            }
                        }
                    } else if let Err(e) = decode_page_group(
                        &data,
                        #[cfg(feature = "zstd")] None,
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
                        #[cfg(feature = "zstd")] None,
                        encryption_key,
                    ) {
                        decode_errors.push((key.clone(), format!("interior chunk {}: {}", chunk_id, e)));
                    }
                }
                Ok(None) => {
                    decode_errors.push((key.clone(), format!("interior chunk {}: empty", chunk_id)));
                }
                Err(e) => {
                    decode_errors.push((key.clone(), format!("interior chunk {} fetch: {}", chunk_id, e)));
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
                        #[cfg(feature = "zstd")] None,
                        encryption_key,
                    ) {
                        decode_errors.push((key.clone(), format!("index chunk {}: {}", chunk_id, e)));
                    }
                }
                Ok(None) => {
                    decode_errors.push((key.clone(), format!("index chunk {}: empty", chunk_id)));
                }
                Err(e) => {
                    decode_errors.push((key.clone(), format!("index chunk {} fetch: {}", chunk_id, e)));
                }
            }
        }

        Ok(super::ValidateResult {
            manifest_version: manifest.version,
            page_groups_total: manifest.page_group_keys.iter().filter(|k| !k.is_empty()).count(),
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

    /// Copy the current manifest to a snapshot key.
    pub fn copy_manifest_to_snapshot(&self, snap_id: &str) -> io::Result<String> {
        storage_helpers::copy_manifest_to_snapshot(
            self.storage.as_ref(),
            &self.runtime,
            snap_id,
        )
    }

    /// Delete a snapshot manifest copy.
    pub fn delete_snapshot_manifest(&self, snap_id: &str) -> io::Result<()> {
        storage_helpers::delete_snapshot_manifest(
            self.storage.as_ref(),
            &self.runtime,
            snap_id,
        )
    }

    /// Load a manifest from a snapshot key.
    pub fn get_snapshot_manifest(&self, snap_id: &str) -> io::Result<Option<Manifest>> {
        let key = keys::snapshot_manifest_key(snap_id);
        storage_helpers::get_manifest_at_key(self.storage.as_ref(), &self.runtime, &key)
    }

    /// Seed this VFS's backend with a manifest from another source (used by fork).
    pub fn seed_manifest(&self, manifest: &Manifest) -> io::Result<()> {
        storage_helpers::put_manifest(self.storage.as_ref(), &self.runtime, manifest)?;
        manifest::persist_manifest_local(&self.config.cache_dir, manifest)?;
        Ok(())
    }

    /// Delete every object the backend knows about. Backend-agnostic
    /// wipe; previously `destroy_s3`.
    pub fn destroy_remote(&self) -> io::Result<()> {
        let all_keys =
            storage_helpers::list_all_keys(self.storage.as_ref(), &self.runtime)?;
        if all_keys.is_empty() {
            return Ok(());
        }
        storage_helpers::delete_objects(self.storage.as_ref(), &self.runtime, &all_keys)?;
        Ok(())
    }

    /// Return a clone of the current manifest state.
    pub fn manifest(&self) -> Manifest {
        (**self.shared_manifest.load()).clone()
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
                        manifest.version, current.version,
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

        self.page_count.store(manifest.page_count, Ordering::Release);

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

    /// Get the path to the raw cache file (data.cache).
    pub fn cache_file_path(&self) -> PathBuf {
        self.config.cache_dir.join("data.cache")
    }

    /// Sync VFS state after an external process (walrust restore) wrote pages
    /// directly to the cache file.
    pub fn sync_after_external_restore(&self, page_count: u64) {
        self.cache.mark_all_pages_present(page_count);
        self.page_count.store(page_count, Ordering::Release);
        if let Err(e) = self.cache.persist_bitmap() {
            eprintln!(
                "[sync_after_external_restore] warning: failed to persist bitmap: {}",
                e,
            );
        }
    }
}

impl Vfs for TurboliteVfs {
    type Handle = TurboliteHandle;

    fn open(&self, db: &str, opts: OpenOptions) -> Result<Self::Handle, io::Error> {
        let path = self.config.cache_dir.join(db);

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
                            old_manifest.version, manifest.version,
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
                                old_manifest.version, manifest.version, invalidated,
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

            let lock_dir = self.config.cache_dir.join("locks");
            fs::create_dir_all(&lock_dir)?;
            let lock_path = lock_dir.join(db);
            FsOpenOptions::new()
                .create(true)
                .write(true)
                .open(&lock_path)?;

            // S3Primary -> RemotePrimary: no WAL stub.
            let skip_wal_stub = self.config.sync_mode == SyncMode::RemotePrimary;

            if !self.config.read_only && !skip_wal_stub {
                let wal_path = self.config.cache_dir.join(format!("{}-wal", db));
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
                self.config.sync_mode,
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
                self.is_local,
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
            Ok(TurboliteHandle::new_passthrough(file, path, self.config.encryption.key))
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

#[cfg(test)]
#[path = "test_local_vfs.rs"]
mod local_vfs_tests;
