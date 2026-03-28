use super::*;

// ===== TieredVfs =====

/// S3-backed tiered storage VFS.
///
/// # Usage
/// ```ignore
/// use turbolite::tiered::{TieredVfs, TieredConfig};
///
/// let config = TieredConfig {
///     bucket: "my-bucket".into(),
///     prefix: "databases/tenant-1".into(),
///     cache_dir: "/tmp/cache".into(),
///     ..Default::default()
/// };
/// let vfs = TieredVfs::new(config).expect("failed to create TieredVfs");
/// turbolite::tiered::register("tiered", vfs).unwrap();
/// ```
pub struct TieredVfs {
    s3: Arc<S3Client>,
    cache: Arc<DiskCache>,
    prefetch_pool: Arc<PrefetchPool>,
    /// Shared page_count for prefetch workers (kept alive by PrefetchPool workers).
    #[allow(dead_code)]
    page_count: Arc<AtomicU64>,
    config: TieredConfig,
    /// Owned runtime (if we created one ourselves)
    _runtime: Option<tokio::runtime::Runtime>,
    /// Phase Verdun: shared prediction table (None when prediction_enabled=false).
    prediction: Option<prediction::SharedPrediction>,
    /// Phase Verdun: shared access history (None when prediction_enabled=false).
    access_history: Option<prediction::SharedAccessHistory>,
    /// Shared manifest state. Written by TieredHandle during sync/checkpoint,
    /// read by flush_to_s3() for non-blocking S3 upload.
    shared_manifest: Arc<RwLock<Manifest>>,
    /// Shared pending S3 groups. Accumulated by TieredHandle during local-only
    /// checkpoints, drained by flush_to_s3().
    shared_dirty_groups: Arc<Mutex<HashSet<u64>>>,
    /// Serializes flush_to_s3() calls. Prevents two concurrent flushes from
    /// racing on version numbers and S3 keys. Also prevents a durable-mode
    /// checkpoint (which drains s3_dirty_groups in sync()) from interleaving
    /// with a flush in progress.
    flush_lock: Arc<Mutex<()>>,
}

impl TieredVfs {
    /// Create a new tiered VFS.
    pub fn new(config: TieredConfig) -> io::Result<Self> {
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

        // Fetch manifest to get page_size for cache initialization
        let mut manifest = s3.get_manifest()?.unwrap_or_else(Manifest::empty);
        manifest.detect_and_normalize_strategy();
        eprintln!("[tiered] manifest fetched (page_size={}, ppg={}, strategy={:?})", manifest.page_size, manifest.pages_per_group, manifest.strategy);
        let page_size = if manifest.page_size > 0 {
            manifest.page_size
        } else {
            4096 // default for new databases
        };
        let ppg = if manifest.pages_per_group > 0 {
            manifest.pages_per_group
        } else {
            config.pages_per_group
        };

        let cache = DiskCache::new(
            &config.cache_dir,
            config.cache_ttl_secs,
            ppg,
            config.sub_pages_per_frame,
            page_size,
            manifest.page_count,
            config.encryption_key,
            manifest.group_pages.clone(),
        )?;

        // B-tree-aware groups may exceed the positional group count formula.
        // Ensure group_states can track all manifest groups.
        let manifest_groups = manifest.total_groups() as usize;
        cache.ensure_group_capacity(manifest_groups);

        let s3 = Arc::new(s3);
        let cache = Arc::new(cache);
        let page_count = Arc::new(AtomicU64::new(manifest.page_count));

        let prefetch_pool = Arc::new(PrefetchPool::new(
            config.prefetch_threads,
            Arc::clone(&s3),
            Arc::clone(&cache),
            ppg,
            Arc::clone(&page_count),
            #[cfg(feature = "zstd")]
            config.dictionary.clone(),
            config.encryption_key,
        ));

        // Phase Verdun: initialize prediction table + access history from manifest (if enabled)
        let (prediction, access_history) = if config.prediction_enabled {
            let table = prediction::PredictionTable::from_persisted(&manifest.prediction_patterns);
            let mut history = prediction::AccessHistory::new();
            history.freq = manifest.btree_access_freq.clone();
            (
                Some(Arc::new(RwLock::new(table))),
                Some(Arc::new(RwLock::new(history))),
            )
        } else {
            (None, None)
        };

        // Shared state for two-phase checkpoint (flush_to_s3)
        let shared_manifest = Arc::new(RwLock::new(manifest));
        let shared_dirty_groups = Arc::new(Mutex::new(HashSet::new()));
        let flush_lock = Arc::new(Mutex::new(()));

        Ok(Self {
            s3,
            cache,
            prefetch_pool,
            page_count,
            config,
            _runtime: owned_runtime,
            prediction,
            access_history,
            shared_manifest,
            shared_dirty_groups,
            flush_lock,
        })
    }

    /// Get a lightweight handle for benchmarking (clear_cache, S3 counters, flush_to_s3).
    /// The handle shares the same cache, S3 client, manifest, and dirty groups as the VFS.
    pub fn bench_handle(&self) -> TieredBenchHandle {
        TieredBenchHandle {
            s3: Arc::clone(&self.s3),
            cache: Arc::clone(&self.cache),
            prefetch_pool: Arc::clone(&self.prefetch_pool),
            shared_manifest: Arc::clone(&self.shared_manifest),
            shared_dirty_groups: Arc::clone(&self.shared_dirty_groups),
            flush_lock: Arc::clone(&self.flush_lock),
            compression_level: self.config.compression_level,
            #[cfg(feature = "zstd")]
            dictionary: self.config.dictionary.clone(),
            encryption_key: self.config.encryption_key,
            gc_enabled: self.config.gc_enabled,
            prediction: self.prediction.clone(),
            access_history: self.access_history.clone(),
        }
    }

    /// Evict non-interior pages from disk cache. Interior pages and group 0
    /// (schema + root page) stay warm -- simulates production where structural
    /// pages are always hot after first access.
    ///
    /// Safe to call with pending flush: groups awaiting S3 upload are protected
    /// (their pages remain in the disk cache bitmap).
    pub fn clear_cache(&self) {
        self.prefetch_pool.wait_idle();

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
            for &page in &pinned_pages {
                bitmap.mark_present(page);
            }
            for &page in &index_pages {
                bitmap.mark_present(page);
            }
            // Protect pending flush pages
            for pages in pending_groups.values() {
                for &p in pages {
                    bitmap.mark_present(p);
                }
            }
            if let Some(g0_pages) = gp.first() {
                // BTreeAware: explicit page list
                for &p in g0_pages {
                    bitmap.mark_present(p);
                }
            } else {
                // Positional: group 0 = pages 0..ppg
                let ppg = self.cache.pages_per_group as u64;
                for p in 0..ppg {
                    bitmap.mark_present(p);
                }
            }
            let _ = bitmap.persist();
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
    pub fn flush_to_s3(&self) -> io::Result<()> {
        let _guard = self.flush_lock.lock().unwrap();
        flush::flush_dirty_groups_to_s3(
            &self.s3,
            &self.cache,
            &self.shared_manifest,
            &self.shared_dirty_groups,
            self.config.compression_level,
            #[cfg(feature = "zstd")]
            self.config.dictionary.as_deref(),
            self.config.encryption_key,
            self.config.gc_enabled,
            self.access_history.as_ref(),
            self.prediction.as_ref(),
        )
    }

    /// Returns true if there are dirty groups pending S3 upload.
    pub fn has_pending_flush(&self) -> bool {
        !self.shared_dirty_groups.lock().unwrap().is_empty()
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
    pub fn reset_s3_counters(&self) -> (u64, u64) {
        let count = self.s3.fetch_count.swap(0, Ordering::Relaxed);
        let bytes = self.s3.fetch_bytes.swap(0, Ordering::Relaxed);
        (count, bytes)
    }

    /// Read current S3 I/O counters without resetting.
    pub fn s3_counters(&self) -> (u64, u64) {
        (
            self.s3.fetch_count.load(Ordering::Relaxed),
            self.s3.fetch_bytes.load(Ordering::Relaxed),
        )
    }

    /// Garbage collect orphaned S3 objects not referenced by the current manifest.
    /// Lists all objects under the prefix, compares against manifest keys, and
    /// deletes unreferenced page groups and interior chunks.
    /// Returns the number of objects deleted.
    pub fn gc(&self) -> io::Result<usize> {
        let manifest = self.s3.get_manifest()?.unwrap_or_else(Manifest::empty);
        let all_keys = self.s3.list_all_keys()?;

        // Build set of live keys from manifest
        let mut live_keys: HashSet<String> = HashSet::new();
        // Phase Thermopylae: msgpack manifest is the live one.
        // Old manifest.json is an orphan and will be GC'd.
        live_keys.insert(self.s3.manifest_key_msgpack());
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
            self.s3.delete_objects(&orphans)?;
            eprintln!("[gc] deleted {} orphaned objects", count);
        }
        Ok(count)
    }

    /// Helper to destroy all S3 data for a prefix.
    pub fn destroy_s3(&self) -> io::Result<()> {
        let s3 = &self.s3;
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
}

impl Vfs for TieredVfs {
    type Handle = TieredHandle;

    fn open(&self, db: &str, opts: OpenOptions) -> Result<Self::Handle, io::Error> {
        let path = self.config.cache_dir.join(db);

        if matches!(opts.kind, OpenKind::MainDb) {
            let mut manifest = self.s3.get_manifest()?.unwrap_or_else(Manifest::empty);
            manifest.detect_and_normalize_strategy();

            let ppg = if manifest.pages_per_group > 0 {
                manifest.pages_per_group
            } else {
                self.config.pages_per_group
            };

            // Update cache's group_pages from latest manifest (BTreeAware only)
            if !manifest.group_pages.is_empty() {
                self.cache.set_group_pages(manifest.group_pages.clone());
                self.cache.ensure_group_capacity(manifest.group_pages.len());
            }

            // Update shared manifest with latest from S3
            *self.shared_manifest.write() = manifest;

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
            if !self.config.read_only {
                let wal_path = self.config.cache_dir.join(format!("{}-wal", db));
                let _ = FsOpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(&wal_path);
            }

            Ok(TieredHandle::new_tiered(
                Arc::clone(&self.s3),
                Arc::clone(&self.cache),
                Arc::clone(&self.shared_manifest),
                Arc::clone(&self.shared_dirty_groups),
                lock_path,
                ppg,
                self.config.compression_level,
                self.config.read_only,
                self.config.sync_mode,
                self.config.prefetch_hops.clone(),
                self.config.prefetch_search.clone(),
                self.config.prefetch_lookup.clone(),
                Some(Arc::clone(&self.prefetch_pool)),
                self.config.gc_enabled,
                self.config.eager_index_load,
                #[cfg(feature = "zstd")]
                self.config.dictionary.as_deref(),
                self.config.encryption_key,
                self.prediction.clone(),
                self.access_history.clone(),
                self.config.query_plan_prefetch,
                self.config.max_cache_bytes,
            ))
        } else {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let file = FsOpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)?;
            Ok(TieredHandle::new_passthrough(file, path, self.config.encryption_key))
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

        Ok(self.s3.get_manifest()?.is_some())
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
