use super::*;

// ===== PrefetchPool =====

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
}

/// Fixed thread pool for background page group prefetching.
/// Workers loop on a shared mpsc receiver, fetching page groups from S3
/// and writing them to the local cache. Default thread count: num_cpus + 1
/// (keeps pipeline full when threads block on S3 I/O).
pub(crate) struct PrefetchPool {
    pub(crate) sender: std::sync::mpsc::Sender<PrefetchJob>,
    pub(crate) in_flight: Arc<AtomicU64>,
    pub(crate) workers: parking_lot::Mutex<Vec<std::thread::JoinHandle<()>>>,
}

impl PrefetchPool {
    pub(crate) fn new(
        num_workers: u32,
        s3: Arc<S3Client>,
        cache: Arc<DiskCache>,
        pages_per_group: u32,
        page_count: Arc<AtomicU64>,
        #[cfg(feature = "zstd")] dictionary: Option<Vec<u8>>,
        encryption_key: Option<[u8; 32]>,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<PrefetchJob>();
        let rx = Arc::new(Mutex::new(rx));
        let in_flight = Arc::new(AtomicU64::new(0));
        let mut workers = Vec::with_capacity(num_workers as usize);

        #[cfg(feature = "zstd")]
        let dictionary = dictionary.map(Arc::new);

        let encryption_key = Arc::new(encryption_key);

        for _ in 0..num_workers {
            let rx = Arc::clone(&rx);
            let s3 = Arc::clone(&s3);
            let cache = Arc::clone(&cache);
            let page_count = Arc::clone(&page_count);
            let in_flight = Arc::clone(&in_flight);
            let ppg = pages_per_group;
            #[cfg(feature = "zstd")]
            let dictionary = dictionary.clone();
            let encryption_key = Arc::clone(&encryption_key);

            workers.push(std::thread::spawn(move || {
                loop {
                    let job = {
                        let lock = rx.lock().expect("prefetch rx poisoned");
                        match lock.recv() {
                            Ok(job) => job,
                            Err(_) => break, // channel closed
                        }
                    };

                    // Group may have been pre-claimed by the read path (Fetching)
                    // or submitted by trigger_prefetch (still None). Try to claim
                    // if not already Fetching; skip if already Done/Ready.
                    let current = cache.group_state(job.gid);
                    if current == GroupState::None {
                        if !cache.try_claim_group(job.gid) {
                            in_flight.fetch_sub(1, Ordering::Release);
                            continue;
                        }
                    } else if current != GroupState::Fetching {
                        if std::env::var("BENCH_VERBOSE").is_ok() {
                            eprintln!("  [prefetch-skip] gid={} state={:?}", job.gid, current);
                        }
                        in_flight.fetch_sub(1, Ordering::Release);
                        continue;
                    }

                    let worker_start = Instant::now();

                    // Blocking S3 GET
                    let fetch_start = Instant::now();
                    let pg_data = match S3Client::block_on(&s3.runtime, s3.get_object_async(&job.key)) {
                        Ok(data) => data,
                        Err(e) => {
                            eprintln!("[prefetch] gid={} fetch error: {}", job.gid, e);
                            let states = cache.group_states.lock();
                            if let Some(s) = states.get(job.gid as usize) {
                                s.store(GroupState::None as u8, Ordering::Release);
                            }
                            cache.group_condvar.notify_all();
                            in_flight.fetch_sub(1, Ordering::Release);
                            continue;
                        }
                    };
                    let fetch_ms = fetch_start.elapsed().as_millis();

                    let Some(pg_data) = pg_data else {
                        // Key not found — reset to None
                        let states = cache.group_states.lock();
                        if let Some(s) = states.get(job.gid as usize) {
                            s.store(GroupState::None as u8, Ordering::Release);
                        }
                        cache.group_condvar.notify_all();
                        in_flight.fetch_sub(1, Ordering::Release);
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
                            eprintln!("[prefetch] gid={} decode error: {}", job.gid, e);
                            let states = cache.group_states.lock();
                            if let Some(s) = states.get(job.gid as usize) {
                                s.store(GroupState::None as u8, Ordering::Release);
                            }
                            cache.group_condvar.notify_all();
                            in_flight.fetch_sub(1, Ordering::Release);
                            continue;
                        }
                    };
                    let decompress_ms = decompress_start.elapsed().as_millis();

                    // Write decoded pages to cache (Phase Midway: B-tree-aware scattered writes)
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
                        eprintln!("[prefetch] gid={} write error: {}", job.gid, e);
                        let states = cache.group_states.lock();
                        if let Some(s) = states.get(job.gid as usize) {
                            s.store(GroupState::None as u8, Ordering::Release);
                        }
                        cache.group_condvar.notify_all();
                        in_flight.fetch_sub(1, Ordering::Release);
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
                                    cache.mark_interior_group(job.gid, pnum, i as u32);
                                } else if b == 0x0A {
                                    if let Some(page_slice) = page_data.get(page_start..page_start + ps) {
                                        if is_valid_btree_page(page_slice, hdr_off) {
                                            cache.mark_index_page(pnum, job.gid, i as u32);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    cache.mark_group_present(job.gid);
                    cache.touch_group(job.gid);
                    if std::env::var("BENCH_VERBOSE").is_ok() {
                        eprintln!(
                            "  [prefetch-done] gid={} ({:.1}KB) fetch={}ms decompress={}ms write={}ms total={}ms",
                            job.gid,
                            pg_data.len() as f64 / 1024.0,
                            fetch_ms, decompress_ms, write_ms,
                            worker_start.elapsed().as_millis(),
                        );
                    }
                    in_flight.fetch_sub(1, Ordering::Release);
                }
            }));
        }

        Self {
            sender: tx,
            in_flight,
            workers: parking_lot::Mutex::new(workers),
        }
    }

    /// Submit a prefetch job (non-blocking). Returns false if channel is closed.
    pub(crate) fn submit(&self, gid: u64, key: String, frame_table: Vec<FrameEntry>, page_size: u32, sub_ppf: u32, group_page_nums: Vec<u64>) -> bool {
        self.in_flight.fetch_add(1, Ordering::Acquire);
        match self.sender.send(PrefetchJob {
            gid,
            key,
            frame_table,
            page_size,
            sub_pages_per_frame: sub_ppf,
            group_page_nums,
        }) {
            Ok(()) => true,
            Err(_) => {
                self.in_flight.fetch_sub(1, Ordering::Release);
                false
            }
        }
    }

    /// Wait until all in-flight prefetch jobs complete.
    pub(crate) fn wait_idle(&self) {
        while self.in_flight.load(Ordering::Acquire) > 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

impl Drop for PrefetchPool {
    fn drop(&mut self) {
        // Drop sender to close the channel, then join all workers
        drop(std::mem::replace(&mut self.sender, mpsc::channel().0));
        let mut workers = self.workers.lock();
        for handle in workers.drain(..) {
            let _ = handle.join();
        }
    }
}

