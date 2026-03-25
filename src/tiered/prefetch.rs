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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tiered::*;
    use std::collections::HashMap;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use tempfile::TempDir;

    // =========================================================================
    // Demand-Driven Prefetch: btree_groups data structure tests
    // =========================================================================

    #[test]
    fn test_btree_groups_single_btree_multiple_groups() {
        // A B-tree spanning 3 groups: each group should map to all siblings
        let mut m = Manifest {
            page_count: 12,
            page_size: 4096,
            pages_per_group: 4,
            page_group_keys: vec!["a".into(), "b".into(), "c".into()],
            group_pages: vec![
                vec![0, 1, 2, 3],
                vec![4, 5, 6, 7],
                vec![8, 9, 10, 11],
            ],
            btrees: {
                let mut h = HashMap::new();
                h.insert(0, BTreeManifestEntry {
                    name: "users".into(),
                    obj_type: "table".into(),
                    group_ids: vec![0, 1, 2],
                });
                h
            },
            ..Manifest::empty()
        };
        m.build_page_index();

        // All three groups should map to the same sibling list [0, 1, 2]
        assert_eq!(m.btree_groups.get(&0).unwrap(), &vec![0u64, 1, 2]);
        assert_eq!(m.btree_groups.get(&1).unwrap(), &vec![0u64, 1, 2]);
        assert_eq!(m.btree_groups.get(&2).unwrap(), &vec![0u64, 1, 2]);
    }

    #[test]
    fn test_btree_groups_multiple_btrees_disjoint() {
        // Two B-trees, each with their own groups, no overlap
        let mut m = Manifest {
            page_count: 20,
            page_size: 4096,
            pages_per_group: 4,
            page_group_keys: vec!["a".into(), "b".into(), "c".into(), "d".into()],
            group_pages: vec![
                vec![0, 1, 2, 3],     // B-tree A
                vec![4, 5, 6, 7],     // B-tree A
                vec![8, 9, 10, 11],   // B-tree B
                vec![12, 13, 14, 15], // B-tree B
            ],
            btrees: {
                let mut h = HashMap::new();
                h.insert(0, BTreeManifestEntry {
                    name: "table_a".into(),
                    obj_type: "table".into(),
                    group_ids: vec![0, 1],
                });
                h.insert(8, BTreeManifestEntry {
                    name: "table_b".into(),
                    obj_type: "table".into(),
                    group_ids: vec![2, 3],
                });
                h
            },
            ..Manifest::empty()
        };
        m.build_page_index();

        // B-tree A groups map to [0, 1]
        assert_eq!(m.btree_groups.get(&0).unwrap(), &vec![0u64, 1]);
        assert_eq!(m.btree_groups.get(&1).unwrap(), &vec![0u64, 1]);
        // B-tree B groups map to [2, 3]
        assert_eq!(m.btree_groups.get(&2).unwrap(), &vec![2u64, 3]);
        assert_eq!(m.btree_groups.get(&3).unwrap(), &vec![2u64, 3]);
        // No cross-contamination
        assert!(!m.btree_groups[&0].contains(&2));
        assert!(!m.btree_groups[&2].contains(&0));
    }

    #[test]
    fn test_btree_groups_single_group_btree_has_self_only() {
        // A B-tree that fits in a single group: btree_groups maps it to [self]
        let mut m = Manifest {
            page_count: 4,
            page_size: 4096,
            pages_per_group: 4,
            page_group_keys: vec!["a".into()],
            group_pages: vec![vec![0, 1, 2, 3]],
            btrees: {
                let mut h = HashMap::new();
                h.insert(0, BTreeManifestEntry {
                    name: "small_table".into(),
                    obj_type: "table".into(),
                    group_ids: vec![0],
                });
                h
            },
            ..Manifest::empty()
        };
        m.build_page_index();

        // Single group: sibling list is [0] (only self)
        assert_eq!(m.btree_groups.get(&0).unwrap(), &vec![0u64]);
        // trigger_prefetch skips self, so no siblings to prefetch (correct)
    }

    #[test]
    fn test_btree_groups_empty_when_no_btrees() {
        // Manifest with group_pages but no btrees: btree_groups is empty
        let mut m = Manifest {
            page_count: 8,
            page_size: 4096,
            pages_per_group: 4,
            page_group_keys: vec!["a".into(), "b".into()],
            group_pages: vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7]],
            btrees: HashMap::new(),
            ..Manifest::empty()
        };
        m.build_page_index();

        assert!(m.btree_groups.is_empty());
    }

    #[test]
    fn test_btree_groups_group_not_in_any_btree() {
        // Group 2 exists in group_pages but isn't claimed by any btree
        let mut m = Manifest {
            page_count: 12,
            page_size: 4096,
            pages_per_group: 4,
            page_group_keys: vec!["a".into(), "b".into(), "c".into()],
            group_pages: vec![
                vec![0, 1, 2, 3],
                vec![4, 5, 6, 7],
                vec![8, 9, 10, 11], // orphan group
            ],
            btrees: {
                let mut h = HashMap::new();
                h.insert(0, BTreeManifestEntry {
                    name: "users".into(),
                    obj_type: "table".into(),
                    group_ids: vec![0, 1], // only groups 0 and 1
                });
                h
            },
            ..Manifest::empty()
        };
        m.build_page_index();

        assert!(m.btree_groups.get(&0).is_some());
        assert!(m.btree_groups.get(&1).is_some());
        assert!(m.btree_groups.get(&2).is_none(), "orphan group should not be in btree_groups");
    }

    #[test]
    fn test_btree_groups_rebuild_clears_stale() {
        // Calling build_page_index twice with different btrees replaces stale mappings
        let mut m = Manifest {
            page_count: 8,
            page_size: 4096,
            pages_per_group: 4,
            page_group_keys: vec!["a".into(), "b".into()],
            group_pages: vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7]],
            btrees: {
                let mut h = HashMap::new();
                h.insert(0, BTreeManifestEntry {
                    name: "v1".into(),
                    obj_type: "table".into(),
                    group_ids: vec![0, 1],
                });
                h
            },
            ..Manifest::empty()
        };
        m.build_page_index();
        assert_eq!(m.btree_groups.len(), 2);
        assert_eq!(m.btree_groups[&0], vec![0u64, 1]);

        // Change btrees: group 1 is now a separate B-tree
        m.btrees.clear();
        m.btrees.insert(0, BTreeManifestEntry {
            name: "v2_a".into(),
            obj_type: "table".into(),
            group_ids: vec![0],
        });
        m.btrees.insert(4, BTreeManifestEntry {
            name: "v2_b".into(),
            obj_type: "table".into(),
            group_ids: vec![1],
        });
        m.build_page_index();

        // Now group 0 and group 1 are in separate B-trees
        assert_eq!(m.btree_groups[&0], vec![0u64]);
        assert_eq!(m.btree_groups[&1], vec![1u64]);
    }

    #[test]
    fn test_btree_groups_overwrite_when_group_in_multiple_btrees() {
        // Edge case: a group appears in multiple B-tree entries.
        // Last-writer-wins due to HashMap iteration order.
        // This shouldn't happen in practice (B-tree-aware grouping assigns
        // each group to exactly one B-tree), but verify no panic.
        let mut m = Manifest {
            page_count: 8,
            page_size: 4096,
            pages_per_group: 4,
            page_group_keys: vec!["a".into(), "b".into()],
            group_pages: vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7]],
            btrees: {
                let mut h = HashMap::new();
                // Both B-trees claim group 0
                h.insert(0, BTreeManifestEntry {
                    name: "tree_a".into(),
                    obj_type: "table".into(),
                    group_ids: vec![0],
                });
                h.insert(4, BTreeManifestEntry {
                    name: "tree_b".into(),
                    obj_type: "index".into(),
                    group_ids: vec![0, 1], // group 0 also here
                });
                h
            },
            ..Manifest::empty()
        };
        m.build_page_index();

        // Group 0 ends up in btree_groups (no panic, last write wins)
        assert!(m.btree_groups.contains_key(&0));
        // Group 1 should be present too
        assert!(m.btree_groups.contains_key(&1));
    }

    // =========================================================================
    // Fraction-based prefetch escalation
    // =========================================================================

    #[test]
    fn test_fraction_prefetch_first_miss_33_percent() {
        // With hops=[0.33, 0.33] and consecutive_misses=1, prefetch 33% of siblings
        let hops = vec![0.33f32, 0.33];
        let hop_idx = 0usize; // consecutive_misses=1, so hop_idx=0
        let fraction = hops[hop_idx];
        // 10 eligible siblings -> ceil(10 * 0.33) = 4
        let eligible = 10;
        let max_submit = ((eligible as f32) * fraction).ceil() as usize;
        assert_eq!(max_submit, 4);
    }

    #[test]
    fn test_fraction_prefetch_second_miss_66_percent() {
        let hops = vec![0.33f32, 0.33];
        let hop_idx = 1usize; // consecutive_misses=2
        let fraction = hops[hop_idx];
        let eligible = 10;
        let max_submit = ((eligible as f32) * fraction).ceil() as usize;
        assert_eq!(max_submit, 4);
        // Total after 2 misses: 4 + 4 = 8 out of 10
    }

    #[test]
    fn test_fraction_prefetch_third_miss_all() {
        let hops = vec![0.33f32, 0.33];
        let hop_idx = 2usize; // consecutive_misses=3, beyond hops.len()
        let fraction = if hop_idx < hops.len() { hops[hop_idx] } else { 1.0 };
        assert_eq!(fraction, 1.0);
        let eligible = 10;
        let max_submit = ((eligible as f32) * fraction).ceil() as usize;
        assert_eq!(max_submit, 10); // all remaining
    }

    #[test]
    fn test_fraction_prefetch_single_sibling() {
        // With only 1 eligible sibling, even 33% rounds up to 1
        let hops = vec![0.33f32, 0.33];
        let fraction = hops[0];
        let eligible = 1;
        let max_submit = ((eligible as f32) * fraction).ceil() as usize;
        assert_eq!(max_submit, 1);
    }

    #[test]
    fn test_fraction_prefetch_zero_eligible() {
        // All siblings already fetching/present -> 0 eligible -> 0 to submit
        let hops = vec![0.33f32, 0.33];
        let fraction = hops[0];
        let eligible = 0;
        let max_submit = ((eligible as f32) * fraction).ceil() as usize;
        assert_eq!(max_submit, 0);
    }

    #[test]
    fn test_fraction_prefetch_empty_hops_fetches_all() {
        // Empty hops vec -> fraction = 1.0 -> fetch all on first miss
        let hops: Vec<f32> = vec![];
        let hop_idx = 0usize;
        let fraction = if hop_idx < hops.len() { hops[hop_idx] } else { 1.0 };
        assert_eq!(fraction, 1.0);
        let eligible = 10;
        let max_submit = ((eligible as f32) * fraction).ceil() as usize;
        assert_eq!(max_submit, 10);
    }

    #[test]
    fn test_prefetch_dedup_claim_prevents_double_download() {
        // Simulates trigger_prefetch deduplication: claiming before submitting
        // ensures at most one download per group
        let dir = TempDir::new().unwrap();
        let cache = Arc::new(DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap());

        // First call claims group 1
        assert!(cache.try_claim_group(1));
        assert_eq!(cache.group_state(1), GroupState::Fetching);

        // Second call (from another trigger_prefetch) fails to claim
        assert!(!cache.try_claim_group(1));

        // After first finishes, marks Present
        cache.mark_group_present(1);

        // Third call (from yet another trigger_prefetch) also can't claim
        assert!(!cache.try_claim_group(1));
    }

    #[test]
    fn test_prefetch_claim_reset_on_failure() {
        // If pool.submit fails, state must be reset to None so the group
        // can be retried by another miss
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap();

        assert!(cache.try_claim_group(1));
        assert_eq!(cache.group_state(1), GroupState::Fetching);

        // Simulate submit failure: reset to None
        {
            let states = cache.group_states.lock();
            if let Some(s) = states.get(1) {
                s.store(GroupState::None as u8, Ordering::Release);
            }
        }
        cache.group_condvar.notify_all();

        // Now another path can claim it
        assert!(cache.try_claim_group(1));
        assert_eq!(cache.group_state(1), GroupState::Fetching);
    }

    #[test]
    fn test_read_path_range_get_before_prefetch() {
        // Verify the logical ordering: range GET should complete before
        // background prefetch is submitted. We test this by checking that
        // after a cache miss, the page is served immediately while the
        // group state transitions happen after.
        //
        // This is a structural test of the invariant, not an integration test.
        let dir = TempDir::new().unwrap();
        let cache = DiskCache::new(dir.path(), 3600, 4, 2, 64, 8, None, Vec::new()).unwrap();

        // Before range GET: group state should be None
        assert_eq!(cache.group_state(0), GroupState::None);

        // After range GET writes sub-chunk to cache, page is present
        // but group state is still None (prefetch not yet submitted)
        let page_data = vec![42u8; 64];
        cache.write_pages_scattered(&[0], &page_data, 0, 0).unwrap();
        assert!(cache.is_present(0));
        assert_eq!(cache.group_state(0), GroupState::None,
            "group state should remain None until prefetch is submitted");

        // Now the read path would claim and submit to pool
        assert!(cache.try_claim_group(0));
        assert_eq!(cache.group_state(0), GroupState::Fetching);
    }
}

