//! Background prefetch thread pool.
//!
//! Workers drive `StorageBackend::get` calls through a shared tokio
//! runtime handle, decode the page group, and write pages into the
//! disk cache. Backend-agnostic: works for S3, local-remote HTTP, or
//! any other `StorageBackend`. Not instantiated in local-filesystem
//! mode (there's no remote I/O to parallelise).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use hadb_storage::StorageBackend;

use super::async_rt::block_on;
use super::manifest::{FrameEntry, Manifest, SubframeOverride};
use super::{
    decode_page_group_bulk, decode_page_group_seekable_full, decode_seekable_subchunk,
    is_valid_btree_page, DiskCache, GroupState,
};

/// A job for the prefetch thread pool.
pub(crate) struct PrefetchJob {
    pub(crate) gid: u64,
    pub(crate) key: String,
    /// Frame table for seekable format (empty = legacy single-frame format).
    pub(crate) frame_table: Vec<FrameEntry>,
    pub(crate) page_size: u32,
    pub(crate) sub_pages_per_frame: u32,
    /// Page numbers in this group (B-tree groups). Empty = legacy positional.
    pub(crate) group_page_nums: Vec<u64>,
    /// Override frames for this group.
    pub(crate) overrides: HashMap<usize, SubframeOverride>,
    /// Manifest version when this job was submitted.
    pub(crate) manifest_version: u64,
    /// Replay epoch captured at submission. The worker re-reads the
    /// shared epoch under `replay_gate.read()` immediately before its
    /// final cache write; if the value has advanced, replay finalize
    /// has installed new bytes for these pages and the prefetch's
    /// version of the bytes is stale. The job is dropped without
    /// writing.
    pub(crate) replay_epoch_at_submit: u64,
}

/// Fixed thread pool for background page group prefetching.
pub(crate) struct PrefetchPool {
    job_tx: flume::Sender<PrefetchJob>,
    /// Wake-up signal for `wait_idle`. Worker may drop sends when full —
    /// `in_flight` is the source of truth, the channel is just a doorbell.
    done_rx: flume::Receiver<u64>,
    /// Outstanding jobs: incremented by `submit`, decremented by the worker
    /// at the end of each job (on every code path). `wait_idle` polls this.
    in_flight: Arc<AtomicU64>,
    workers: parking_lot::Mutex<Vec<std::thread::JoinHandle<()>>>,
    /// Set to true when the pool is shutting down; suppresses noisy
    /// fetch/decode/write error logs during teardown.
    shutdown: Arc<AtomicBool>,
    /// Replay epoch reference held by the pool so `submit` can
    /// snapshot it into each `PrefetchJob`. Workers reload from the
    /// shared atomic under `replay_gate.read()` later.
    replay_epoch_at_pool: Arc<AtomicU64>,
}

impl PrefetchPool {
    pub(crate) fn new(
        num_workers: u32,
        storage: Arc<dyn StorageBackend>,
        runtime: tokio::runtime::Handle,
        cache: Arc<DiskCache>,
        pages_per_group: u32,
        page_count: Arc<AtomicU64>,
        #[cfg(feature = "zstd")] dictionary: Option<Vec<u8>>,
        encryption_key: Option<[u8; 32]>,
        shared_manifest: Arc<ArcSwap<Manifest>>,
        replay_gate: Arc<parking_lot::RwLock<()>>,
        replay_epoch: Arc<AtomicU64>,
    ) -> Self {
        let (job_tx, job_rx) = flume::bounded::<PrefetchJob>(num_workers as usize * 2);
        // Bounded so the completion signal can't grow without limit if no one
        // is calling `wait_idle`. Sized to comfortably absorb a wake-up backlog
        // (`num_workers * 4`); workers `try_send` and drop on full because
        // `in_flight` (atomic) is the authoritative outstanding-job count.
        let (done_tx, done_rx) = flume::bounded::<u64>(num_workers as usize * 4);
        let in_flight = Arc::new(AtomicU64::new(0));
        let mut workers = Vec::with_capacity(num_workers as usize);
        let shutdown = Arc::new(AtomicBool::new(false));

        #[cfg(feature = "zstd")]
        let dictionary = dictionary.map(Arc::new);
        let encryption_key = Arc::new(encryption_key);

        for _ in 0..num_workers {
            let job_rx = job_rx.clone();
            let done_tx = done_tx.clone();
            let storage = Arc::clone(&storage);
            let runtime = runtime.clone();
            let cache = Arc::clone(&cache);
            let page_count = Arc::clone(&page_count);
            let _ppg = pages_per_group;
            #[cfg(feature = "zstd")]
            let dictionary = dictionary.clone();
            let encryption_key = Arc::clone(&encryption_key);
            let shared_manifest = Arc::clone(&shared_manifest);
            let shutdown = Arc::clone(&shutdown);
            let in_flight = Arc::clone(&in_flight);
            let replay_gate = Arc::clone(&replay_gate);
            let replay_epoch = Arc::clone(&replay_epoch);

            workers.push(std::thread::spawn(move || {
                // Mark a job as finished: drop the in-flight counter (so
                // wait_idle can observe completion) and best-effort wake any
                // waiter via the bounded done channel. Dropped wake-ups are
                // fine — the next call to wait_idle reads the atomic directly.
                let finish = |gid: u64| {
                    in_flight.fetch_sub(1, Ordering::Release);
                    let _ = done_tx.try_send(gid);
                };

                // Worker error / stale paths use `unclaim_if_fetching`
                // (CAS Fetching → None) rather than the unconditional
                // `unclaim_group`. Replay finalize calls
                // `mark_pages_present` on the group's pages, which
                // also flips the group state to Present. If a worker
                // is still in flight when that happens (e.g., it
                // captured an old manifest version, or its
                // captured replay_epoch was bumped), it must not
                // reset Present → None: that would force a re-fetch
                // against a stale manifest key and could re-install
                // pre-replay bytes over the freshly-replayed ones.
                while let Ok(job) = job_rx.recv() {
                    let gid = job.gid;

                    let current = cache.group_state(gid);
                    if current == GroupState::None {
                        if !cache.try_claim_group(gid) {
                            finish(gid);
                            continue;
                        }
                    } else if current != GroupState::Fetching {
                        if ::tracing::enabled!(target: "turbolite", ::tracing::Level::DEBUG) {
                            turbolite_debug!("  [prefetch-skip] gid={} state={:?}", gid, current);
                        }
                        finish(gid);
                        continue;
                    }

                    let worker_start = Instant::now();

                    let fetch_start = Instant::now();
                    let pg_data = match block_on(&runtime, storage.get(&job.key)) {
                        Ok(data) => data,
                        Err(e) => {
                            if !shutdown.load(Ordering::Acquire) {
                                eprintln!("[prefetch] gid={} fetch error: {}", gid, e);
                            }
                            cache.unclaim_if_fetching(gid);
                            finish(gid);
                            continue;
                        }
                    };
                    let fetch_ms = fetch_start.elapsed().as_millis();

                    let Some(pg_data) = pg_data else {
                        cache.unclaim_group(gid);
                        finish(gid);
                        continue;
                    };

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
                            0,
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
                            if !shutdown.load(Ordering::Acquire) {
                                eprintln!("[prefetch] gid={} decode error: {}", gid, e);
                            }
                            cache.unclaim_if_fetching(gid);
                            finish(gid);
                            continue;
                        }
                    };
                    let decompress_ms = decompress_start.elapsed().as_millis();

                    let current_version = shared_manifest.load().version;
                    if current_version != job.manifest_version {
                        turbolite_debug!(
                            "[prefetch] gid={} manifest changed (v{} -> v{}), discarding stale fetch",
                            gid, job.manifest_version, current_version,
                        );
                        cache.unclaim_group(gid);
                        finish(gid);
                        continue;
                    }

                    // Hold the replay-gate read lock across both the
                    // epoch re-check and the cache write. This blocks
                    // concurrent finalize, which takes the write
                    // guard, and means we cannot race between
                    // "epoch matches" and "we wrote bytes". If
                    // finalize ran between submit and here, the
                    // captured epoch will not match the current value
                    // and we drop without writing.
                    let _replay_read_guard = replay_gate.read();
                    let current_epoch = replay_epoch.load(Ordering::Acquire);
                    if current_epoch != job.replay_epoch_at_submit {
                        turbolite_debug!(
                            "[prefetch] gid={} replay epoch advanced ({} -> {}), discarding stale fetch",
                            gid, job.replay_epoch_at_submit, current_epoch,
                        );
                        cache.unclaim_group(gid);
                        finish(gid);
                        continue;
                    }

                    turbolite_debug!(
                        "[prefetch] gid={} writing {} pages (job_v={}, current_v={})",
                        gid, pg_count, job.manifest_version, current_version,
                    );
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
                        if !shutdown.load(Ordering::Acquire) {
                            eprintln!("[prefetch] gid={} write error: {}", gid, e);
                        }
                        cache.unclaim_group(gid);
                        finish(gid);
                        continue;
                    }
                    let write_ms = write_start.elapsed().as_millis();

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

                    if !job.overrides.is_empty() && job.sub_pages_per_frame > 0 {
                        let spf = job.sub_pages_per_frame as usize;
                        for (&frame_idx, ovr) in &job.overrides {
                            let ovr_data = match block_on(&runtime, storage.get(&ovr.key)) {
                                Ok(Some(data)) => data,
                                Ok(None) => {
                                    turbolite_debug!(
                                        "[prefetch] gid={} override frame {} key '{}' not found",
                                        gid, frame_idx, ovr.key,
                                    );
                                    continue;
                                }
                                Err(e) => {
                                    if !shutdown.load(Ordering::Acquire) {
                                        eprintln!(
                                            "[prefetch] gid={} override frame {} fetch error: {}",
                                            gid, frame_idx, e,
                                        );
                                    }
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
                                        frame_page_nums,
                                        &decompressed[..data_len],
                                        job.gid,
                                        frame_start as u32,
                                    );
                                }
                            }
                        }
                    }

                    cache.mark_group_present(gid);
                    cache.touch_group(gid);
                    if ::tracing::enabled!(target: "turbolite", ::tracing::Level::DEBUG) {
                        turbolite_debug!(
                            "  [prefetch-done] gid={} ({:.1}KB) fetch={}ms decompress={}ms write={}ms total={}ms",
                            gid,
                            pg_data.len() as f64 / 1024.0,
                            fetch_ms, decompress_ms, write_ms,
                            worker_start.elapsed().as_millis(),
                        );
                    }
                    finish(gid);
                }
            }));
        }

        Self {
            job_tx,
            done_rx,
            in_flight,
            workers: parking_lot::Mutex::new(workers),
            shutdown,
            replay_epoch_at_pool: replay_epoch,
        }
    }

    /// Submit a prefetch job. Returns false if channel is closed.
    pub(crate) fn submit(
        &self,
        gid: u64,
        key: String,
        frame_table: Vec<FrameEntry>,
        page_size: u32,
        sub_ppf: u32,
        group_page_nums: Vec<u64>,
        overrides: HashMap<usize, SubframeOverride>,
        manifest_version: u64,
    ) -> bool {
        self.in_flight.fetch_add(1, Ordering::Acquire);
        // Snapshot the replay epoch at submission. The worker re-reads
        // it under the replay_gate read lock before its final cache
        // write; if it has advanced, replay finalize ran in between
        // and the prefetch's bytes are stale.
        let replay_epoch_at_submit = self.replay_epoch_at_pool.load(Ordering::Acquire);
        match self.job_tx.send(PrefetchJob {
            gid,
            key,
            frame_table,
            manifest_version,
            page_size,
            sub_pages_per_frame: sub_ppf,
            group_page_nums,
            overrides,
            replay_epoch_at_submit,
        }) {
            Ok(()) => true,
            Err(_) => {
                self.in_flight.fetch_sub(1, Ordering::Release);
                false
            }
        }
    }

    /// Wait until all in-flight prefetch jobs complete.
    ///
    /// Workers decrement `in_flight` when they finish a job, so the atomic
    /// is the source of truth. The done channel is best-effort wake-up: if
    /// a wake-up was dropped because the channel was full, we still make
    /// progress via the short timeout.
    pub(crate) fn wait_idle(&self) {
        loop {
            let remaining = self.in_flight.load(Ordering::Acquire);
            if remaining == 0 {
                break;
            }
            // Sleep until a worker rings the bell, or 50ms — whichever first.
            // Either way we re-check in_flight on the next iteration.
            match self
                .done_rx
                .recv_timeout(std::time::Duration::from_millis(50))
            {
                Ok(_gid) => {}
                Err(flume::RecvTimeoutError::Timeout) => {}
                Err(flume::RecvTimeoutError::Disconnected) => break,
            }
        }
    }
}

impl Drop for PrefetchPool {
    fn drop(&mut self) {
        // Signal shutdown so workers suppress spurious fetch/decode/write
        // error logs while the runtime/storage is torn down.
        self.shutdown.store(true, Ordering::Release);
        // Drain all in-flight work before shutting down the channel.
        self.wait_idle();

        let (dead_tx, _) = flume::bounded(0);
        drop(std::mem::replace(&mut self.job_tx, dead_tx));
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
