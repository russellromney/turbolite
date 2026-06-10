//! Background prefetch thread pool.
//!
//! Workers drive `StorageBackend::get` calls through a shared tokio
//! runtime handle, decode the page group, and write pages into the
//! disk cache. Backend-agnostic: works for S3, local-remote HTTP, or
//! any other `StorageBackend`. Not instantiated in local-filesystem
//! mode (there's no remote I/O to parallelise).

use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use hadb_storage::StorageBackend;

use super::async_rt::block_on;
use super::manifest::{FrameEntry, Manifest, SubframeOverride};
use super::{
    decode_page_group_bulk, decode_page_group_seekable_full, decode_seekable_subchunk,
    is_valid_btree_page, keys, storage as storage_helpers, DiskCache, GroupState,
};

/// A job for the prefetch thread pool.
pub(crate) struct PrefetchJob {
    /// Logical tree that requested this prefetch. None for demand-driven
    /// sibling prefetches where the trigger is a page miss rather than a
    /// planned query tree.
    pub(crate) tree_name: Option<String>,
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
    /// Time this optional job entered the queue.
    queued_at: Instant,
}

#[derive(Default, Clone)]
struct PrefetchTreeCounters {
    submitted: u64,
    completed: u64,
    skipped_state: u64,
    missing_objects: u64,
    fetch_errors: u64,
    decode_errors: u64,
    write_errors: u64,
    stale_manifest: u64,
    stale_replay: u64,
    bytes_fetched: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PrefetchSubmitOutcome {
    Accepted,
    Full,
    Closed,
    SkippedState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PrefetchTrafficClass {
    ForegroundRange,
    ForegroundGroup,
    PrefetchGroup,
    PrefetchOverride,
}

impl PrefetchTrafficClass {
    fn index(self) -> usize {
        match self {
            Self::ForegroundRange => 0,
            Self::ForegroundGroup => 1,
            Self::PrefetchGroup => 2,
            Self::PrefetchOverride => 3,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::ForegroundRange => "foreground_range",
            Self::ForegroundGroup => "foreground_group",
            Self::PrefetchGroup => "prefetch_group",
            Self::PrefetchOverride => "prefetch_override",
        }
    }

    const ALL: [Self; 4] = [
        Self::ForegroundRange,
        Self::ForegroundGroup,
        Self::PrefetchGroup,
        Self::PrefetchOverride,
    ];
}

const LATENCY_BUCKETS: usize = 40;

/// Lock-free log2-bucketed latency histogram in microseconds. Bucket `i`
/// counts samples in `[2^(i-1), 2^i)`, so reported quantiles are bucket
/// upper bounds — at most 2x the true value, which is enough resolution to
/// tell a 1ms GET from a 100ms one.
struct LatencyHistogram {
    buckets: [AtomicU64; LATENCY_BUCKETS],
    count: AtomicU64,
    max_us: AtomicU64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            max_us: AtomicU64::new(0),
        }
    }
}

impl LatencyHistogram {
    fn record(&self, elapsed: std::time::Duration) {
        let us = elapsed.as_micros() as u64;
        let idx = (64 - us.leading_zeros() as usize).min(LATENCY_BUCKETS - 1);
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.max_us.fetch_max(us, Ordering::Relaxed);
    }

    /// Upper bound (us) of the bucket holding the q-th quantile sample.
    fn quantile_us(&self, q: f64) -> u64 {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            return 0;
        }
        let target = ((q * count as f64).ceil() as u64).clamp(1, count);
        let mut seen = 0u64;
        for (idx, bucket) in self.buckets.iter().enumerate() {
            seen += bucket.load(Ordering::Relaxed);
            if seen >= target {
                return 1u64 << idx;
            }
        }
        self.max_us.load(Ordering::Relaxed)
    }

    fn snapshot_json(&self) -> serde_json::Value {
        serde_json::json!({
            "count": self.count.load(Ordering::Relaxed),
            "p50_us": self.quantile_us(0.50),
            "p95_us": self.quantile_us(0.95),
            "p99_us": self.quantile_us(0.99),
            "p999_us": self.quantile_us(0.999),
            "max_us": self.max_us.load(Ordering::Relaxed),
        })
    }
}

#[derive(Default)]
struct TrafficCounters {
    ops: AtomicU64,
    bytes: AtomicU64,
    errors: AtomicU64,
    latency_us: AtomicU64,
    latency: LatencyHistogram,
}

impl TrafficCounters {
    fn record(&self, bytes: u64, elapsed: std::time::Duration) {
        self.ops.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        self.latency_us
            .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
        self.latency.record(elapsed);
    }

    fn error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot_json(&self) -> serde_json::Value {
        serde_json::json!({
            "ops": self.ops.load(Ordering::Relaxed),
            "bytes": self.bytes.load(Ordering::Relaxed),
            "errors": self.errors.load(Ordering::Relaxed),
            "latency_us": self.latency_us.load(Ordering::Relaxed),
            "latency": self.latency.snapshot_json(),
        })
    }
}

#[derive(Default)]
pub(crate) struct PrefetchMetrics {
    submitted: AtomicU64,
    accepted: AtomicU64,
    full: AtomicU64,
    closed: AtomicU64,
    cancelled: AtomicU64,
    permit_unavailable: AtomicU64,
    completed: AtomicU64,
    skipped_state: AtomicU64,
    missing_objects: AtomicU64,
    fetch_errors: AtomicU64,
    decode_errors: AtomicU64,
    write_errors: AtomicU64,
    stale_manifest: AtomicU64,
    stale_replay: AtomicU64,
    bytes_fetched: AtomicU64,
    wait_before_fetch_us: AtomicU64,
    wait_before_fetch: LatencyHistogram,
    foreground_waits: AtomicU64,
    foreground_wait_us: AtomicU64,
    foreground_wait: LatencyHistogram,
    traffic: [TrafficCounters; 4],
    trees: parking_lot::Mutex<HashMap<String, PrefetchTreeCounters>>,
}

impl PrefetchMetrics {
    fn with_tree(&self, tree_name: &Option<String>, f: impl FnOnce(&mut PrefetchTreeCounters)) {
        if let Some(tree_name) = tree_name {
            let mut trees = self.trees.lock();
            f(trees.entry(tree_name.clone()).or_default());
        }
    }

    fn submitted(&self, tree_name: &Option<String>) {
        self.submitted.fetch_add(1, Ordering::Relaxed);
        self.with_tree(tree_name, |tree| tree.submitted += 1);
    }

    fn accepted(&self) {
        self.accepted.fetch_add(1, Ordering::Relaxed);
    }

    fn full(&self) {
        self.full.fetch_add(1, Ordering::Relaxed);
    }

    fn closed(&self) {
        self.closed.fetch_add(1, Ordering::Relaxed);
    }

    fn cancelled(&self, tree_name: &Option<String>) {
        self.cancelled.fetch_add(1, Ordering::Relaxed);
        self.with_tree(tree_name, |tree| tree.skipped_state += 1);
    }

    fn permit_unavailable(&self, tree_name: &Option<String>) {
        self.permit_unavailable.fetch_add(1, Ordering::Relaxed);
        self.with_tree(tree_name, |tree| tree.skipped_state += 1);
    }

    fn completed(&self, tree_name: &Option<String>, bytes: u64) {
        self.completed.fetch_add(1, Ordering::Relaxed);
        self.bytes_fetched.fetch_add(bytes, Ordering::Relaxed);
        self.with_tree(tree_name, |tree| {
            tree.completed += 1;
            tree.bytes_fetched += bytes;
        });
    }

    fn skipped_state(&self, tree_name: &Option<String>) {
        self.skipped_state.fetch_add(1, Ordering::Relaxed);
        self.with_tree(tree_name, |tree| tree.skipped_state += 1);
    }

    fn missing_object(&self, tree_name: &Option<String>) {
        self.missing_objects.fetch_add(1, Ordering::Relaxed);
        self.with_tree(tree_name, |tree| tree.missing_objects += 1);
    }

    fn fetch_error(&self, tree_name: &Option<String>) {
        self.fetch_errors.fetch_add(1, Ordering::Relaxed);
        self.with_tree(tree_name, |tree| tree.fetch_errors += 1);
    }

    fn decode_error(&self, tree_name: &Option<String>) {
        self.decode_errors.fetch_add(1, Ordering::Relaxed);
        self.with_tree(tree_name, |tree| tree.decode_errors += 1);
    }

    fn write_error(&self, tree_name: &Option<String>) {
        self.write_errors.fetch_add(1, Ordering::Relaxed);
        self.with_tree(tree_name, |tree| tree.write_errors += 1);
    }

    fn stale_manifest(&self, tree_name: &Option<String>) {
        self.stale_manifest.fetch_add(1, Ordering::Relaxed);
        self.with_tree(tree_name, |tree| tree.stale_manifest += 1);
    }

    fn stale_replay(&self, tree_name: &Option<String>) {
        self.stale_replay.fetch_add(1, Ordering::Relaxed);
        self.with_tree(tree_name, |tree| tree.stale_replay += 1);
    }

    fn wait_before_fetch(&self, elapsed: std::time::Duration) {
        self.wait_before_fetch_us
            .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
        self.wait_before_fetch.record(elapsed);
    }

    fn foreground_wait(&self, elapsed: std::time::Duration) {
        self.foreground_waits.fetch_add(1, Ordering::Relaxed);
        self.foreground_wait_us
            .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
        self.foreground_wait.record(elapsed);
    }

    fn traffic_success(
        &self,
        class: PrefetchTrafficClass,
        bytes: u64,
        elapsed: std::time::Duration,
    ) {
        self.traffic[class.index()].record(bytes, elapsed);
    }

    fn traffic_error(&self, class: PrefetchTrafficClass) {
        self.traffic[class.index()].error();
    }

    fn snapshot_json(
        &self,
        in_flight: u64,
        queued: u64,
        max_queued: u64,
        max_remote: u64,
    ) -> serde_json::Value {
        let trees = self.trees.lock();
        let tree_json = trees
            .iter()
            .map(|(name, counters)| {
                (
                    name.clone(),
                    serde_json::json!({
                        "submitted": counters.submitted,
                        "completed": counters.completed,
                        "skipped_state": counters.skipped_state,
                        "missing_objects": counters.missing_objects,
                        "fetch_errors": counters.fetch_errors,
                        "decode_errors": counters.decode_errors,
                        "write_errors": counters.write_errors,
                        "stale_manifest": counters.stale_manifest,
                        "stale_replay": counters.stale_replay,
                        "bytes_fetched": counters.bytes_fetched,
                    }),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        let traffic_json = PrefetchTrafficClass::ALL
            .iter()
            .map(|class| {
                (
                    class.as_str().to_string(),
                    self.traffic[class.index()].snapshot_json(),
                )
            })
            .collect::<serde_json::Map<_, _>>();

        serde_json::json!({
            "in_flight": in_flight,
            "queued": queued,
            "max_queued": max_queued,
            "max_remote_in_flight": max_remote,
            "submitted": self.submitted.load(Ordering::Relaxed),
            "accepted": self.accepted.load(Ordering::Relaxed),
            "full": self.full.load(Ordering::Relaxed),
            "closed": self.closed.load(Ordering::Relaxed),
            "cancelled": self.cancelled.load(Ordering::Relaxed),
            "permit_unavailable": self.permit_unavailable.load(Ordering::Relaxed),
            "completed": self.completed.load(Ordering::Relaxed),
            "skipped_state": self.skipped_state.load(Ordering::Relaxed),
            "missing_objects": self.missing_objects.load(Ordering::Relaxed),
            "fetch_errors": self.fetch_errors.load(Ordering::Relaxed),
            "decode_errors": self.decode_errors.load(Ordering::Relaxed),
            "write_errors": self.write_errors.load(Ordering::Relaxed),
            "stale_manifest": self.stale_manifest.load(Ordering::Relaxed),
            "stale_replay": self.stale_replay.load(Ordering::Relaxed),
            "bytes_fetched": self.bytes_fetched.load(Ordering::Relaxed),
            "wait_before_fetch_us": self.wait_before_fetch_us.load(Ordering::Relaxed),
            "wait_before_fetch": self.wait_before_fetch.snapshot_json(),
            "foreground_waits": self.foreground_waits.load(Ordering::Relaxed),
            "foreground_wait_us": self.foreground_wait_us.load(Ordering::Relaxed),
            "foreground_wait": self.foreground_wait.snapshot_json(),
            "traffic": traffic_json,
            "trees": tree_json,
        })
    }
}

#[derive(Debug)]
pub(crate) struct RemoteIoBudget {
    total: u64,
    foreground_reserved: u64,
    in_flight: AtomicU64,
    prefetch_in_flight: AtomicU64,
    max_in_flight: AtomicU64,
    condvar: parking_lot::Condvar,
    mutex: parking_lot::Mutex<()>,
}

pub(crate) struct RemoteIoPermit {
    budget: Arc<RemoteIoBudget>,
    prefetch: bool,
}

impl RemoteIoBudget {
    pub(crate) fn new(total: u32, foreground_reserved: u32) -> Arc<Self> {
        let total = u64::from(total.max(1));
        let foreground_reserved = u64::from(foreground_reserved).min(total);
        Arc::new(Self {
            total,
            foreground_reserved,
            in_flight: AtomicU64::new(0),
            prefetch_in_flight: AtomicU64::new(0),
            max_in_flight: AtomicU64::new(0),
            condvar: parking_lot::Condvar::new(),
            mutex: parking_lot::Mutex::new(()),
        })
    }

    fn note_max(&self, value: u64) {
        self.max_in_flight.fetch_max(value, Ordering::Relaxed);
    }

    pub(crate) fn try_acquire_prefetch(self: &Arc<Self>) -> Option<RemoteIoPermit> {
        let prefetch_limit = self.total.saturating_sub(self.foreground_reserved);
        if prefetch_limit == 0 {
            return None;
        }
        loop {
            let current_prefetch = self.prefetch_in_flight.load(Ordering::Acquire);
            if current_prefetch >= prefetch_limit {
                return None;
            }
            if self
                .prefetch_in_flight
                .compare_exchange(
                    current_prefetch,
                    current_prefetch + 1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                continue;
            }
            loop {
                let current = self.in_flight.load(Ordering::Acquire);
                if current >= self.total {
                    self.prefetch_in_flight.fetch_sub(1, Ordering::Release);
                    return None;
                }
                if self
                    .in_flight
                    .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    self.note_max(current + 1);
                    return Some(RemoteIoPermit {
                        budget: Arc::clone(self),
                        prefetch: true,
                    });
                }
            }
        }
    }

    pub(crate) fn acquire_foreground(self: &Arc<Self>) -> RemoteIoPermit {
        let mut guard = self.mutex.lock();
        loop {
            let current = self.in_flight.load(Ordering::Acquire);
            if current < self.total
                && self
                    .in_flight
                    .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                self.note_max(current + 1);
                return RemoteIoPermit {
                    budget: Arc::clone(self),
                    prefetch: false,
                };
            }
            self.condvar.wait(&mut guard);
        }
    }

    pub(crate) fn max_in_flight(&self) -> u64 {
        self.max_in_flight.load(Ordering::Relaxed)
    }
}

impl Drop for RemoteIoPermit {
    fn drop(&mut self) {
        self.budget.in_flight.fetch_sub(1, Ordering::Release);
        if self.prefetch {
            self.budget
                .prefetch_in_flight
                .fetch_sub(1, Ordering::Release);
        }
        self.budget.condvar.notify_one();
    }
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
    metrics: Arc<PrefetchMetrics>,
    queued_gids: Arc<parking_lot::Mutex<HashSet<u64>>>,
    active_gids: Arc<parking_lot::Mutex<HashSet<u64>>>,
    cancelled_gids: Arc<parking_lot::Mutex<HashSet<u64>>>,
    max_queued: Arc<AtomicU64>,
    io_budget: Arc<RemoteIoBudget>,
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
        queue_capacity: u32,
        io_budget: Arc<RemoteIoBudget>,
    ) -> Self {
        let (job_tx, job_rx) = flume::bounded::<PrefetchJob>(queue_capacity.max(1) as usize);
        // Bounded so the completion signal can't grow without limit if no one
        // is calling `wait_idle`. Sized to comfortably absorb a wake-up backlog
        // (`num_workers * 4`); workers `try_send` and drop on full because
        // `in_flight` (atomic) is the authoritative outstanding-job count.
        let (done_tx, done_rx) = flume::bounded::<u64>(num_workers as usize * 4);
        let in_flight = Arc::new(AtomicU64::new(0));
        let mut workers = Vec::with_capacity(num_workers as usize);
        let shutdown = Arc::new(AtomicBool::new(false));
        let metrics = Arc::new(PrefetchMetrics::default());
        let queued_gids = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let active_gids = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let cancelled_gids = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let max_queued = Arc::new(AtomicU64::new(0));

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
            let metrics = Arc::clone(&metrics);
            let queued_gids = Arc::clone(&queued_gids);
            let active_gids = Arc::clone(&active_gids);
            let cancelled_gids = Arc::clone(&cancelled_gids);
            let io_budget = Arc::clone(&io_budget);

            workers.push(std::thread::spawn(move || {
                // Mark a job as finished: drop the in-flight counter (so
                // wait_idle can observe completion) and best-effort wake any
                // waiter via the bounded done channel. Dropped wake-ups are
                // fine — the next call to wait_idle reads the atomic directly.
                let finish = |gid: u64| {
                    queued_gids.lock().remove(&gid);
                    active_gids.lock().remove(&gid);
                    cancelled_gids.lock().remove(&gid);
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
                    metrics.wait_before_fetch(job.queued_at.elapsed());

                    let current = cache.group_state(gid);
                    if current != GroupState::None {
                        if ::tracing::enabled!(target: "turbolite", ::tracing::Level::DEBUG) {
                            turbolite_debug!("  [prefetch-skip] gid={} state={:?}", gid, current);
                        }
                        metrics.skipped_state(&job.tree_name);
                        finish(gid);
                        continue;
                    }
                    let Some(_io_permit) = io_budget.try_acquire_prefetch() else {
                        metrics.permit_unavailable(&job.tree_name);
                        finish(gid);
                        continue;
                    };
                    if !cache.try_claim_group(gid) {
                        metrics.skipped_state(&job.tree_name);
                        finish(gid);
                        continue;
                    }
                    active_gids.lock().insert(gid);

                    let worker_start = Instant::now();

                    let fetch_start = Instant::now();
                    let pg_data = match block_on(&runtime, storage.get(&job.key)) {
                        Ok(data) => data,
                        Err(e) => {
                            if !shutdown.load(Ordering::Acquire) {
                                eprintln!("[prefetch] gid={} fetch error: {}", gid, e);
                            }
                            cache.unclaim_if_fetching(gid);
                            metrics.fetch_error(&job.tree_name);
                            metrics.traffic_error(PrefetchTrafficClass::PrefetchGroup);
                            finish(gid);
                            continue;
                        }
                    };
                    drop(_io_permit);
                    let fetch_ms = fetch_start.elapsed().as_millis();

                    let Some(pg_data) = pg_data else {
                        cache.unclaim_if_fetching(gid);
                        metrics.missing_object(&job.tree_name);
                        metrics.traffic_error(PrefetchTrafficClass::PrefetchGroup);
                        finish(gid);
                        continue;
                    };
                    if cancelled_gids.lock().contains(&gid) {
                        metrics.cancelled(&job.tree_name);
                        finish(gid);
                        continue;
                    };
                    metrics.traffic_success(
                        PrefetchTrafficClass::PrefetchGroup,
                        pg_data.len() as u64,
                        fetch_start.elapsed(),
                    );

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
                            &keys::aad_page_group(gid),
                            encryption_key.as_ref().as_ref(),
                        )
                    } else {
                        decode_page_group_bulk(
                            &pg_data,
                            #[cfg(feature = "zstd")]
                            decoder_dict.as_ref(),
                            &keys::aad_page_group(gid),
                            encryption_key.as_ref().as_ref(),
                        )
                    };
                    let (pg_count, _pg_size, mut page_data) = match decode_result {
                        Ok(v) => v,
                        Err(e) => {
                            if !shutdown.load(Ordering::Acquire) {
                                eprintln!("[prefetch] gid={} decode error: {}", gid, e);
                            }
                            cache.unclaim_if_fetching(gid);
                            metrics.decode_error(&job.tree_name);
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
                        cache.unclaim_if_fetching(gid);
                        metrics.stale_manifest(&job.tree_name);
                        finish(gid);
                        continue;
                    }

                    if !job.overrides.is_empty() && job.sub_pages_per_frame > 0 {
                        let spf = job.sub_pages_per_frame as usize;
                        let mut override_failed = false;
                        for (&frame_idx, ovr) in &job.overrides {
                            let override_fetch_start = Instant::now();
                            let Some(override_permit) = io_budget.try_acquire_prefetch() else {
                                cache.unclaim_if_fetching(gid);
                                metrics.permit_unavailable(&job.tree_name);
                                metrics.traffic_error(PrefetchTrafficClass::PrefetchOverride);
                                override_failed = true;
                                break;
                            };
                            let ovr_data = match block_on(&runtime, storage.get(&ovr.key)) {
                                Ok(Some(data)) => data,
                                Ok(None) => {
                                    turbolite_debug!(
                                        "[prefetch] gid={} override frame {} key '{}' not found",
                                        gid, frame_idx, ovr.key,
                                    );
                                    cache.unclaim_if_fetching(gid);
                                    metrics.missing_object(&job.tree_name);
                                    metrics.traffic_error(PrefetchTrafficClass::PrefetchOverride);
                                    override_failed = true;
                                    break;
                                }
                                Err(e) => {
                                    if !shutdown.load(Ordering::Acquire) {
                                        eprintln!(
                                            "[prefetch] gid={} override frame {} fetch error: {}",
                                            gid, frame_idx, e,
                                        );
                                    }
                                    cache.unclaim_if_fetching(gid);
                                    metrics.fetch_error(&job.tree_name);
                                    metrics.traffic_error(PrefetchTrafficClass::PrefetchOverride);
                                    override_failed = true;
                                    break;
                                }
                            };
                            drop(override_permit);
                            if cancelled_gids.lock().contains(&gid) {
                                metrics.cancelled(&job.tree_name);
                                override_failed = true;
                                break;
                            }
                            metrics.traffic_success(
                                PrefetchTrafficClass::PrefetchOverride,
                                ovr_data.len() as u64,
                                override_fetch_start.elapsed(),
                            );
                            #[cfg(feature = "zstd")]
                            let ovr_decoder = dictionary
                                .as_deref()
                                .map(|d| zstd::dict::DecoderDictionary::copy(d));
                            let decompressed = match decode_seekable_subchunk(
                                &ovr_data,
                                #[cfg(feature = "zstd")]
                                ovr_decoder.as_ref(),
                                &keys::aad_override_frame(gid, frame_idx),
                                encryption_key.as_ref().as_ref(),
                            ) {
                                Ok(bytes) => bytes,
                                Err(e) => {
                                    if !shutdown.load(Ordering::Acquire) {
                                        eprintln!(
                                            "[prefetch] gid={} override frame {} decode error: {}",
                                            gid, frame_idx, e,
                                        );
                                    }
                                    cache.unclaim_if_fetching(gid);
                                    metrics.decode_error(&job.tree_name);
                                    metrics.traffic_error(PrefetchTrafficClass::PrefetchOverride);
                                    override_failed = true;
                                    break;
                                }
                            };
                            let frame_start = frame_idx * spf;
                            let frame_end =
                                std::cmp::min(frame_start + spf, job.group_page_nums.len());
                            let data_len = (frame_end - frame_start) * _pg_size as usize;
                            let dest_start = frame_start * _pg_size as usize;
                            let dest_end = dest_start + data_len;
                            if data_len > decompressed.len() || dest_end > page_data.len() {
                                cache.unclaim_if_fetching(gid);
                                metrics.decode_error(&job.tree_name);
                                metrics.traffic_error(PrefetchTrafficClass::PrefetchOverride);
                                override_failed = true;
                                break;
                            }
                            page_data[dest_start..dest_end]
                                .copy_from_slice(&decompressed[..data_len]);
                        }
                        if override_failed {
                            finish(gid);
                            continue;
                        }
                    }

                    // Hold the replay-gate read lock across both the final
                    // epoch/cancel re-check and the cache write. Foreground
                    // cancellation takes the write side before clearing the
                    // claim, so a late optional worker cannot install bytes
                    // after foreground has taken over ownership.
                    let _replay_read_guard = replay_gate.read();
                    if cancelled_gids.lock().contains(&gid) {
                        metrics.cancelled(&job.tree_name);
                        finish(gid);
                        continue;
                    }
                    let current_version = shared_manifest.load().version;
                    if current_version != job.manifest_version {
                        turbolite_debug!(
                            "[prefetch] gid={} manifest changed (v{} -> v{}), discarding stale fetch",
                            gid,
                            job.manifest_version,
                            current_version,
                        );
                        cache.unclaim_if_fetching(gid);
                        metrics.stale_manifest(&job.tree_name);
                        finish(gid);
                        continue;
                    }
                    let current_epoch = replay_epoch.load(Ordering::Acquire);
                    if current_epoch != job.replay_epoch_at_submit {
                        turbolite_debug!(
                            "[prefetch] gid={} replay epoch advanced ({} -> {}), discarding stale fetch",
                            gid, job.replay_epoch_at_submit, current_epoch,
                        );
                        cache.unclaim_if_fetching(gid);
                        metrics.stale_replay(&job.tree_name);
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
                        cache.unclaim_if_fetching(gid);
                        metrics.write_error(&job.tree_name);
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

                    cache.mark_group_present(gid);
                    cache.touch_group(gid);
                    metrics.completed(&job.tree_name, pg_data.len() as u64);
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
            metrics,
            queued_gids,
            active_gids,
            cancelled_gids,
            max_queued,
            io_budget,
        }
    }

    /// Submit optional prefetch work without blocking or claiming a group.
    ///
    /// Group state remains `None` while work is merely queued. A worker must
    /// acquire a prefetch I/O permit and then claim `Fetching` immediately
    /// before starting remote I/O. This keeps foreground reads from waiting
    /// behind optional work that has not started.
    pub(crate) fn submit_optional(
        &self,
        tree_name: Option<String>,
        gid: u64,
        key: String,
        frame_table: Vec<FrameEntry>,
        page_size: u32,
        sub_ppf: u32,
        group_page_nums: Vec<u64>,
        overrides: HashMap<usize, SubframeOverride>,
        manifest_version: u64,
        cache: &DiskCache,
    ) -> PrefetchSubmitOutcome {
        if self.workers.lock().is_empty() {
            self.metrics.closed();
            return PrefetchSubmitOutcome::Closed;
        }
        if cache.group_state(gid) != GroupState::None {
            self.metrics.skipped_state(&tree_name);
            return PrefetchSubmitOutcome::SkippedState;
        }
        {
            let mut queued = self.queued_gids.lock();
            if !queued.insert(gid) {
                self.metrics.skipped_state(&tree_name);
                return PrefetchSubmitOutcome::SkippedState;
            }
            self.max_queued
                .fetch_max(queued.len() as u64, Ordering::Relaxed);
        }
        // Snapshot the replay epoch at submission. The worker re-reads
        // it under the replay_gate read lock before its final cache
        // write; if it has advanced, replay finalize ran in between
        // and the prefetch's bytes are stale.
        let replay_epoch_at_submit = self.replay_epoch_at_pool.load(Ordering::Acquire);
        let submit_tree_name = tree_name.clone();
        self.in_flight.fetch_add(1, Ordering::Release);
        let job = PrefetchJob {
            tree_name,
            gid,
            key,
            frame_table,
            manifest_version,
            page_size,
            sub_pages_per_frame: sub_ppf,
            group_page_nums,
            overrides,
            replay_epoch_at_submit,
            queued_at: Instant::now(),
        };
        match self.job_tx.try_send(job) {
            Ok(()) => {
                self.metrics.accepted();
                self.metrics.submitted(&submit_tree_name);
                PrefetchSubmitOutcome::Accepted
            }
            Err(flume::TrySendError::Full(job)) => {
                self.in_flight.fetch_sub(1, Ordering::Release);
                self.queued_gids.lock().remove(&job.gid);
                self.metrics.full();
                PrefetchSubmitOutcome::Full
            }
            Err(flume::TrySendError::Disconnected(job)) => {
                self.in_flight.fetch_sub(1, Ordering::Release);
                self.queued_gids.lock().remove(&job.gid);
                self.metrics.closed();
                PrefetchSubmitOutcome::Closed
            }
        }
    }

    pub(crate) fn is_optional_pending(&self, gid: u64) -> bool {
        self.queued_gids.lock().contains(&gid)
    }

    pub(crate) fn is_optional_fetching(&self, gid: u64) -> bool {
        self.active_gids.lock().contains(&gid)
    }

    pub(crate) fn cancel_optional_fetching(&self, gid: u64) -> bool {
        let active = self.active_gids.lock();
        if active.contains(&gid) {
            self.cancelled_gids.lock().insert(gid);
            true
        } else {
            false
        }
    }

    pub(crate) fn record_foreground_wait(&self, elapsed: std::time::Duration) {
        self.metrics.foreground_wait(elapsed);
    }

    pub(crate) fn get_foreground(
        &self,
        storage: &dyn StorageBackend,
        runtime: &tokio::runtime::Handle,
        key: &str,
    ) -> io::Result<Option<Vec<u8>>> {
        let _permit = self.io_budget.acquire_foreground();
        let start = Instant::now();
        match storage_helpers::get_page_group(storage, runtime, key) {
            Ok(result) => {
                if let Some(bytes) = &result {
                    self.metrics.traffic_success(
                        PrefetchTrafficClass::ForegroundGroup,
                        bytes.len() as u64,
                        start.elapsed(),
                    );
                }
                Ok(result)
            }
            Err(e) => {
                self.metrics
                    .traffic_error(PrefetchTrafficClass::ForegroundGroup);
                Err(e)
            }
        }
    }

    pub(crate) fn range_get_foreground(
        &self,
        storage: &dyn StorageBackend,
        runtime: &tokio::runtime::Handle,
        key: &str,
        start_byte: u64,
        len: u32,
    ) -> io::Result<Option<Vec<u8>>> {
        let _permit = self.io_budget.acquire_foreground();
        let start = Instant::now();
        match storage_helpers::range_get(storage, runtime, key, start_byte, len) {
            Ok(result) => {
                if let Some(bytes) = &result {
                    self.metrics.traffic_success(
                        PrefetchTrafficClass::ForegroundRange,
                        bytes.len() as u64,
                        start.elapsed(),
                    );
                }
                Ok(result)
            }
            Err(e) => {
                self.metrics
                    .traffic_error(PrefetchTrafficClass::ForegroundRange);
                Err(e)
            }
        }
    }

    pub(crate) fn stats_json(&self) -> serde_json::Value {
        self.metrics.snapshot_json(
            self.in_flight.load(Ordering::Relaxed),
            self.queued_gids.lock().len() as u64,
            self.max_queued.load(Ordering::Relaxed),
            self.io_budget.max_in_flight(),
        )
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
mod metrics_tests {
    use super::PrefetchMetrics;

    #[test]
    fn prefetch_metrics_snapshot_includes_per_tree_counters() {
        let metrics = PrefetchMetrics::default();
        let tree = Some("users".to_string());

        metrics.submitted(&tree);
        metrics.completed(&tree, 4096);
        metrics.fetch_error(&tree);

        let stats = metrics.snapshot_json(1, 0, 0, 1);
        assert_eq!(stats["in_flight"], 1);
        assert_eq!(stats["submitted"], 1);
        assert_eq!(stats["completed"], 1);
        assert_eq!(stats["fetch_errors"], 1);
        assert_eq!(stats["bytes_fetched"], 4096);
        assert_eq!(stats["trees"]["users"]["submitted"], 1);
        assert_eq!(stats["trees"]["users"]["completed"], 1);
        assert_eq!(stats["trees"]["users"]["fetch_errors"], 1);
        assert_eq!(stats["trees"]["users"]["bytes_fetched"], 4096);
    }

    #[test]
    fn latency_histogram_quantiles_are_bucket_upper_bounds() {
        use super::LatencyHistogram;
        use std::time::Duration;

        let hist = LatencyHistogram::default();
        assert_eq!(hist.quantile_us(0.99), 0, "empty histogram reports 0");

        // 99 fast samples (~100us) and 1 slow one (~50ms).
        for _ in 0..99 {
            hist.record(Duration::from_micros(100));
        }
        hist.record(Duration::from_millis(50));

        // 100us lands in [64, 128); 50_000us lands in [32768, 65536).
        assert_eq!(hist.quantile_us(0.50), 128);
        assert_eq!(hist.quantile_us(0.99), 128);
        assert_eq!(hist.quantile_us(0.999), 65536);

        let snap = hist.snapshot_json();
        assert_eq!(snap["count"], 100);
        assert_eq!(snap["p50_us"], 128);
        assert_eq!(snap["p999_us"], 65536);
        assert_eq!(snap["max_us"], 50_000);
    }

    #[test]
    fn traffic_counters_expose_latency_histogram() {
        let metrics = PrefetchMetrics::default();
        metrics.traffic_success(
            super::PrefetchTrafficClass::ForegroundRange,
            512,
            std::time::Duration::from_millis(3),
        );
        metrics.foreground_wait(std::time::Duration::from_millis(7));

        let stats = metrics.snapshot_json(0, 0, 0, 0);
        let range = &stats["traffic"]["foreground_range"];
        assert_eq!(range["latency"]["count"], 1);
        // 3000us lands in [2048, 4096).
        assert_eq!(range["latency"]["p99_us"], 4096);
        assert_eq!(stats["foreground_wait"]["count"], 1);
        // 7000us lands in [4096, 8192).
        assert_eq!(stats["foreground_wait"]["p99_us"], 8192);
    }
}

#[cfg(test)]
#[path = "test_prefetch.rs"]
mod tests;
