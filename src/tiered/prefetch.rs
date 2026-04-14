// When cloud feature is disabled, PrefetchPool is a zero-size stub.
#[cfg(not(feature = "cloud"))]
pub(crate) struct PrefetchJob;

#[cfg(not(feature = "cloud"))]
pub(crate) struct PrefetchPool;

#[cfg(not(feature = "cloud"))]
impl PrefetchPool {
    pub(crate) fn wait_idle(&self) {}
    pub(crate) fn submit(&self, _gid: u64, _key: String, _ft: Vec<super::FrameEntry>, _ps: u32, _spf: u32, _gp: Vec<u64>, _overrides: std::collections::HashMap<usize, super::manifest::SubframeOverride>, _manifest_version: u64) -> bool { false }
}

#[cfg(feature = "cloud")]
use super::*;

// ===== PrefetchPool =====

#[cfg(feature = "cloud")]
/// A job for the prefetch thread pool.
pub(crate) struct PrefetchJob {
    pub(crate) gid: u64,
    pub(crate) key: String,
    /// Frame table for seekable format (empty = legacy single-frame format).
    pub(crate) frame_table: Vec<FrameEntry>,
    /// Page size (needed for seekable decode).
    pub(crate) page_size: u32,
    /// Sub-chunk size (needed for seekable decode).
    pub(crate) sub_pages_per_frame: u32,
    /// Page numbers in this group (Phase Midway: B-tree groups). Empty = legacy positional.
    pub(crate) group_page_nums: Vec<u64>,
    /// Phase Drift-c: override frames for this group.
    pub(crate) overrides: HashMap<usize, super::manifest::SubframeOverride>,
    /// Manifest version when this job was submitted. If the manifest changed
    /// (set_manifest eviction), the fetched data is stale and should be discarded.
    pub(crate) manifest_version: u64,
}

#[cfg(feature = "cloud")]
/// Fixed thread pool for background page group prefetching.
/// Workers loop on a shared flume receiver, fetching page groups from S3
/// and writing them to the local cache. Default thread count: num_cpus + 1
/// (keeps pipeline full when threads block on S3 I/O).
///
/// Uses flume channels instead of std::sync::mpsc (Phase Flume):
/// 1. flume::Receiver is Clone + Sync, so workers recv directly (no Mutex wrapper)
/// 2. Completion channel replaces AtomicU64 spin-polling in wait_idle()
/// 3. Bounded job channel provides backpressure when workers are saturated
pub(crate) struct PrefetchPool {
    /// Bounded job channel. Capacity = 2x workers to keep the pipeline full
    /// without unbounded growth.
    job_tx: flume::Sender<PrefetchJob>,
    /// Completion channel. Workers send gid on finish (success or skip).
    /// wait_idle() drains this instead of spin-polling an atomic counter.
    done_rx: flume::Receiver<u64>,
    /// Number of jobs submitted but not yet completed. Used by wait_idle()
    /// to know how many completions to drain.
    in_flight: AtomicU64,
    workers: parking_lot::Mutex<Vec<std::thread::JoinHandle<()>>>,
}

#[cfg(feature = "cloud")]
impl PrefetchPool {
    pub(crate) fn new(
        num_workers: u32,
        s3: Arc<S3Client>,
        cache: Arc<DiskCache>,
        pages_per_group: u32,
        page_count: Arc<AtomicU64>,
        #[cfg(feature = "zstd")] dictionary: Option<Vec<u8>>,
        encryption_key: Option<[u8; 32]>,
        shared_manifest: Arc<ArcSwap<super::manifest::Manifest>>,
    ) -> Self {
        // Bounded: 2x workers keeps pipeline full without unbounded growth
        let (job_tx, job_rx) = flume::bounded::<PrefetchJob>(num_workers as usize * 2);
        let (done_tx, done_rx) = flume::unbounded::<u64>();
        let mut workers = Vec::with_capacity(num_workers as usize);

        #[cfg(feature = "zstd")]
        let dictionary = dictionary.map(Arc::new);

        let encryption_key = Arc::new(encryption_key);

        for _ in 0..num_workers {
            let job_rx = job_rx.clone();
            let done_tx = done_tx.clone();
            let s3 = Arc::clone(&s3);
            let cache = Arc::clone(&cache);
            let page_count = Arc::clone(&page_count);
            let _ppg = pages_per_group;
            #[cfg(feature = "zstd")]
            let dictionary = dictionary.clone();
            let encryption_key = Arc::clone(&encryption_key);
            let shared_manifest = Arc::clone(&shared_manifest);

            workers.push(std::thread::spawn(move || {
                // flume::Receiver is Clone+Sync: no Mutex needed, workers steal directly
                while let Ok(job) = job_rx.recv() {
                    let gid = job.gid;

                    // Group may have been pre-claimed by the read path (Fetching)
                    // or submitted by trigger_prefetch (still None). Try to claim
                    // if not already Fetching; skip if already Done/Ready.
                    let current = cache.group_state(gid);
                    if current == GroupState::None {
                        if !cache.try_claim_group(gid) {
                            let _ = done_tx.send(gid);
                            continue;
                        }
                    } else if current != GroupState::Fetching {
                        if std::env::var("BENCH_VERBOSE").is_ok() {
                            turbolite_debug!("  [prefetch-skip] gid={} state={:?}", gid, current);
                        }
                        let _ = done_tx.send(gid);
                        continue;
                    }

                    let worker_start = Instant::now();

                    // Blocking S3 GET
                    let fetch_start = Instant::now();
                    let pg_data = match S3Client::block_on(&s3.runtime, s3.get_object_async(&job.key)) {
                        Ok(data) => data,
                        Err(e) => {
                            eprintln!("[prefetch] gid={} fetch error: {}", gid, e);
                            cache.unclaim_group(gid);
                            let _ = done_tx.send(gid);
                            continue;
                        }
                    };
                    let fetch_ms = fetch_start.elapsed().as_millis();

                    let Some(pg_data) = pg_data else {
                        // Key not found, reset to None
                        cache.unclaim_group(gid);
                        let _ = done_tx.send(gid);
                        continue;
                    };

                    // Decompress
                    #[cfg(feature = "zstd")]
                    let decoder_dict = dictionary
                        .as_deref()
                        .map(|d| zstd::dict::DecoderDictionary::copy(d));

                    let decompress_start = Instant::now();
                    let decode_result = if !job.frame_table.is_empty() {
                        let pc = page_count.load(Ordering::Relaxed);
                        decode_page_group_seekable_full(
                            &pg_data,
                            &job.frame_table,
                            job.page_size,
                            job.group_page_nums.len() as u32,
                            pc,
                            0, // B-tree groups: group size from group_page_nums, not positional
                            #[cfg(feature = "zstd")]
                            decoder_dict.as_ref(),
                            encryption_key.as_ref().as_ref(),
                        )
                    } else {
                        decode_page_group_bulk(
                            &pg_data,
                            #[cfg(feature = "zstd")]
                            decoder_dict.as_ref(),
                            encryption_key.as_ref().as_ref(),
                        )
                    };
                    let (pg_count, _pg_size, page_data) = match decode_result {
                        Ok(v) => v,
                        Err(e) => {
                            eprintln!("[prefetch] gid={} decode error: {}", gid, e);
                            cache.unclaim_group(gid);
                            let _ = done_tx.send(gid);
                            continue;
                        }
                    };
                    let decompress_ms = decompress_start.elapsed().as_millis();

                    // Check if manifest changed (set_manifest eviction) since this job
                    // was submitted. If so, our page group data is stale (wrong overrides,
                    // old page group keys). Discard to avoid overwriting correct data
                    // written by the foreground read path with the newer manifest.
                    let current_version = shared_manifest.load().version;
                    if current_version != job.manifest_version {
                        turbolite_debug!("[prefetch] gid={} manifest changed (v{} -> v{}), discarding stale fetch",
                            gid, job.manifest_version, current_version);
                        cache.unclaim_group(gid);
                        let _ = done_tx.send(gid);
                        continue;
                    }

                    // Write decoded pages to cache (Phase Midway: B-tree-aware scattered writes)
                    turbolite_debug!("[prefetch] gid={} writing {} pages (job_v={}, current_v={})",
                        gid, pg_count, job.manifest_version, current_version);
                    let write_start = Instant::now();
                    let actual_pages = std::cmp::min(pg_count as usize, job.group_page_nums.len());
                    let write_result = if actual_pages > 0 {
                        let data_len = actual_pages * _pg_size as usize;
                        cache.write_pages_scattered(
                            &job.group_page_nums[..actual_pages],
                            &page_data[..data_len],
                            job.gid,
                            0,
                        )
                    } else {
                        Ok(())
                    };
                    if let Err(e) = write_result {
                        eprintln!("[prefetch] gid={} write error: {}", gid, e);
                        cache.unclaim_group(gid);
                        let _ = done_tx.send(gid);
                        continue;
                    }
                    let write_ms = write_start.elapsed().as_millis();

                    // Scan pages for page types and set sub-chunk tier
                    {
                        let ps = _pg_size as usize;
                        for (i, &pnum) in job.group_page_nums.iter().take(actual_pages).enumerate() {
                            let hdr_off = if pnum == 0 { 100 } else { 0 };
                            let page_start = i * ps;
                            let type_byte = page_data.get(page_start + hdr_off).copied();
                            if let Some(b) = type_byte {
                                if b == 0x05 || b == 0x02 {
                                    cache.mark_interior_group(gid, pnum, i as u32);
                                } else if b == 0x0A {
                                    if let Some(page_slice) = page_data.get(page_start..page_start + ps) {
                                        if is_valid_btree_page(page_slice, hdr_off) {
                                            cache.mark_index_page(pnum, gid, i as u32);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Phase Drift-c: apply override frames
                    if !job.overrides.is_empty() && job.sub_pages_per_frame > 0 {
                        let spf = job.sub_pages_per_frame as usize;
                        for (&frame_idx, ovr) in &job.overrides {
                            let ovr_data = match S3Client::block_on(&s3.runtime, s3.get_object_async(&ovr.key)) {
                                Ok(Some(data)) => data,
                                Ok(None) => {
                                    turbolite_debug!("[prefetch] gid={} override frame {} key '{}' not found in S3",
                                        gid, frame_idx, ovr.key);
                                    continue;
                                }
                                Err(e) => {
                                    eprintln!("[prefetch] gid={} override frame {} fetch error: {}",
                                        gid, frame_idx, e);
                                    continue;
                                }
                            };
                            #[cfg(feature = "zstd")]
                            let ovr_decoder = dictionary.as_deref().map(|d| zstd::dict::DecoderDictionary::copy(d));
                            if let Ok(decompressed) = decode_seekable_subchunk(
                                &ovr_data,
                                #[cfg(feature = "zstd")]
                                ovr_decoder.as_ref(),
                                encryption_key.as_ref().as_ref(),
                            ) {
                                let frame_start = frame_idx * spf;
                                let frame_end = std::cmp::min(frame_start + spf, job.group_page_nums.len());
                                let frame_page_nums = &job.group_page_nums[frame_start..frame_end];
                                let data_len = frame_page_nums.len() * _pg_size as usize;
                                if data_len <= decompressed.len() {
                                    let _ = cache.write_pages_scattered(
                                        frame_page_nums, &decompressed[..data_len],
                                        job.gid, frame_start as u32,
                                    );
                                }
                            }
                        }
                    }

                    cache.mark_group_present(gid);
                    cache.touch_group(gid);
                    if std::env::var("BENCH_VERBOSE").is_ok() {
                        turbolite_debug!(
                            "  [prefetch-done] gid={} ({:.1}KB) fetch={}ms decompress={}ms write={}ms total={}ms",
                            gid,
                            pg_data.len() as f64 / 1024.0,
                            fetch_ms, decompress_ms, write_ms,
                            worker_start.elapsed().as_millis(),
                        );
                    }
                    let _ = done_tx.send(gid);
                }
            }));
        }

        Self {
            job_tx,
            done_rx,
            in_flight: AtomicU64::new(0),
            workers: parking_lot::Mutex::new(workers),
        }
    }

    /// Submit a prefetch job. Returns false if channel is closed.
    /// With bounded channel, this blocks if workers are saturated (backpressure).
    pub(crate) fn submit(&self, gid: u64, key: String, frame_table: Vec<FrameEntry>, page_size: u32, sub_ppf: u32, group_page_nums: Vec<u64>, overrides: HashMap<usize, super::manifest::SubframeOverride>, manifest_version: u64) -> bool {
        self.in_flight.fetch_add(1, Ordering::Acquire);
        match self.job_tx.send(PrefetchJob {
            gid,
            key,
            frame_table,
            manifest_version,
            page_size,
            sub_pages_per_frame: sub_ppf,
            group_page_nums,
            overrides,
        }) {
            Ok(()) => true,
            Err(_) => {
                self.in_flight.fetch_sub(1, Ordering::Release);
                false
            }
        }
    }

    /// Wait until all in-flight prefetch jobs complete.
    /// Drains the completion channel instead of spin-polling an atomic counter.
    pub(crate) fn wait_idle(&self) {
        loop {
            let remaining = self.in_flight.load(Ordering::Acquire);
            if remaining == 0 {
                break;
            }
            // Block on completion signal from a worker
            match self.done_rx.recv() {
                Ok(_gid) => {
                    self.in_flight.fetch_sub(1, Ordering::Release);
                }
                Err(_) => break, // all workers gone
            }
        }
    }
}

#[cfg(feature = "cloud")]
impl Drop for PrefetchPool {
    fn drop(&mut self) {
        // Drop the job sender to close the channel, causing workers to exit
        // their recv() loop. Then join all worker threads.
        let (dead_tx, _) = flume::bounded(0);
        drop(std::mem::replace(&mut self.job_tx, dead_tx));

        // Drain any remaining completions so workers aren't blocked on send
        while self.done_rx.try_recv().is_ok() {}

        let mut workers = self.workers.lock();
        for handle in workers.drain(..) {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
#[path = "test_prefetch.rs"]
mod tests;
