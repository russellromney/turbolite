//! Phase Verdun: Predictive cross-tree prefetch + access history.
//!
//! Learns which B-trees appear together in lock sessions (transactions/queries)
//! and prefetches the full set in parallel when a partial match is detected.
//! Also tracks B-tree access frequency for prediction confidence and decay.
//!
//! Phase Verdun-i: keys are table/index names (Strings), not root page IDs.
//! This survives VACUUM (root pages renumber, names stay the same).

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ===== Lock session tracking (per-connection) =====

/// Tracks which B-tree names are touched during a single lock session
/// (NONE -> SHARED/EXCLUSIVE -> NONE). Per-connection, not shared.
pub(crate) struct LockSession {
    /// B-tree names touched in the current lock session.
    pub btrees: HashSet<String>,
    /// Whether a lock session is active (between lock acquire and release).
    pub active: bool,
    /// Whether a prediction has already fired in this session (prevent re-firing).
    pub prediction_fired: bool,
    /// The pattern key that fired the prediction (for reinforcement on flush).
    /// Set by prediction firing (subphase d), consumed by flush.
    pub fired_pattern: Option<BTreeSet<String>>,
}

impl LockSession {
    pub fn new() -> Self {
        Self {
            btrees: HashSet::new(),
            active: false,
            prediction_fired: false,
            fired_pattern: None,
        }
    }

    /// Start a new lock session. Clears previous state.
    pub fn start(&mut self) {
        self.btrees.clear();
        self.active = true;
        self.prediction_fired = false;
        self.fired_pattern = None;
    }

    /// Record a B-tree name touch. Returns true if this is a new name
    /// (not previously seen in this session). Only clones the string
    /// when inserting a new entry (common case: re-touch is a no-op).
    pub fn touch(&mut self, tree_name: &str) -> bool {
        if !self.active {
            return false;
        }
        if self.btrees.contains(tree_name) {
            return false;
        }
        self.btrees.insert(tree_name.to_string());
        true
    }

    /// End the session and return the set of touched B-tree names.
    /// Returns None if the session was inactive or touched < 2 trees.
    pub fn flush(&mut self) -> Option<BTreeSet<String>> {
        if !self.active {
            return None;
        }
        self.active = false;
        self.prediction_fired = false;
        self.fired_pattern = None;

        if self.btrees.len() >= 2 {
            let pattern: BTreeSet<String> = self.btrees.drain().collect();
            Some(pattern)
        } else {
            self.btrees.clear();
            None
        }
    }

    /// Number of distinct B-tree names touched so far.
    pub fn tree_count(&self) -> usize {
        self.btrees.len()
    }
}

// ===== Prediction table (shared across connections) =====

/// A learned pattern: a set of B-tree names that appear together.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredictionEntry {
    /// The full set of B-tree names in this pattern.
    pub names: BTreeSet<String>,
    /// Confidence score (0.0 - 1.0). Fires when >= CONFIDENCE_THRESHOLD.
    pub confidence: f32,
    /// Number of times this prediction fired correctly.
    pub hit_count: u32,
}

/// Shared prediction state. Lives on TieredVfs, accessed by all connections.
pub struct PredictionTable {
    /// Learned patterns: pattern (sorted name set) -> entry.
    pub patterns: HashMap<BTreeSet<String>, PredictionEntry>,
    /// Pair index: (name_a, name_b) where a < b -> pattern keys.
    /// For O(1) lookup when the 2nd B-tree is touched.
    pair_index: HashMap<(String, String), Vec<BTreeSet<String>>>,
}

/// Confidence assigned to a newly observed pattern.
const INITIAL_CONFIDENCE: f32 = 0.3;
/// Reinforcement per correct prediction.
const REINFORCEMENT: f32 = 0.25;
/// Time decay per lock session flush (applied to all patterns).
const TIME_DECAY: f32 = 0.95;
/// Write decay per B-tree with dirty pages.
const WRITE_DECAY: f32 = 0.7;
/// Small confidence boost on re-observation (prevents 2-root patterns from decaying).
const OBSERVATION_BOOST: f32 = 0.1;
/// Minimum confidence to fire a prediction.
pub const CONFIDENCE_THRESHOLD: f32 = 0.5;
/// Prune patterns below this confidence on checkpoint.
const PRUNE_THRESHOLD: f32 = 0.1;

impl PredictionTable {
    pub fn new() -> Self {
        Self {
            patterns: HashMap::new(),
            pair_index: HashMap::new(),
        }
    }

    /// Restore from manifest-persisted patterns.
    pub fn from_persisted(patterns: &[(BTreeSet<String>, f32)]) -> Self {
        let mut table = Self::new();
        for (names, confidence) in patterns {
            let entry = PredictionEntry {
                names: names.clone(),
                confidence: *confidence,
                hit_count: 0,
            };
            table.patterns.insert(names.clone(), entry);
        }
        table.rebuild_pair_index();
        table
    }

    /// Serialize for manifest persistence.
    pub fn to_persisted(&self) -> Vec<(BTreeSet<String>, f32)> {
        self.patterns
            .iter()
            .filter(|(_, e)| e.confidence >= PRUNE_THRESHOLD)
            .map(|(k, e)| (k.clone(), e.confidence))
            .collect()
    }

    /// Observe a completed lock session pattern. Upserts the pattern with
    /// initial confidence or applies time decay + re-observation boost.
    pub fn observe(&mut self, pattern: &BTreeSet<String>) {
        if pattern.len() < 2 {
            return;
        }

        if let Some(entry) = self.patterns.get_mut(pattern) {
            // Existing pattern: time decay then small re-observation boost.
            // The boost prevents patterns that keep appearing (but can't fire predictions
            // because seen == pattern, e.g. 2-name patterns) from decaying to zero.
            // Steady state: c * 0.95 + 0.1 converges to 2.0 for any starting confidence.
            // Capped at 1.0, so effective steady state is 1.0 for active patterns.
            entry.confidence = (entry.confidence * TIME_DECAY + OBSERVATION_BOOST).min(1.0);
        } else {
            // New pattern
            let entry = PredictionEntry {
                names: pattern.clone(),
                confidence: INITIAL_CONFIDENCE,
                hit_count: 0,
            };
            self.patterns.insert(pattern.clone(), entry);
            self.add_to_pair_index(pattern);
        }
    }

    /// Look up predictions matching a partial set of B-tree names.
    /// Returns the predicted names NOT already in `seen` if confidence >= threshold.
    pub fn predict(&self, seen: &HashSet<String>) -> Option<Vec<String>> {
        if seen.len() < 2 {
            return None;
        }

        // Generate all pairs from seen names and check the pair index
        let seen_vec: Vec<&String> = seen.iter().collect();
        let mut best_match: Option<&PredictionEntry> = None;

        for i in 0..seen_vec.len() {
            for j in (i + 1)..seen_vec.len() {
                let pair = normalize_pair(seen_vec[i], seen_vec[j]);
                if let Some(pattern_keys) = self.pair_index.get(&pair) {
                    for key in pattern_keys {
                        if let Some(entry) = self.patterns.get(key) {
                            if entry.confidence >= CONFIDENCE_THRESHOLD {
                                // Check that seen is a subset of this pattern
                                if seen.iter().all(|r| entry.names.contains(r)) {
                                    match best_match {
                                        None => best_match = Some(entry),
                                        Some(current) if entry.confidence > current.confidence => {
                                            best_match = Some(entry);
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        best_match.and_then(|entry| {
            let extras: Vec<String> = entry
                .names
                .iter()
                .filter(|r| !seen.contains(r.as_str()))
                .cloned()
                .collect();
            if extras.is_empty() { None } else { Some(extras) }
        })
    }

    /// Reinforce a prediction that fired correctly (all predicted names were touched).
    pub fn reinforce(&mut self, pattern: &BTreeSet<String>) {
        if let Some(entry) = self.patterns.get_mut(pattern) {
            entry.confidence = (entry.confidence + REINFORCEMENT).min(1.0);
            entry.hit_count += 1;
        }
    }

    /// Apply write decay to all patterns containing the given B-tree name.
    /// Called once per dirty B-tree per lock session (not per dirty page).
    pub fn write_decay(&mut self, tree_name: &str) {
        for (_, entry) in self.patterns.iter_mut() {
            if entry.names.contains(tree_name) {
                entry.confidence *= WRITE_DECAY;
            }
        }
    }

    /// Apply global time decay and prune patterns below threshold. Call during checkpoint.
    /// Global decay ensures patterns that stop appearing (e.g. renamed tables) eventually
    /// fall below threshold and get pruned, even if they're never re-observed.
    pub fn prune(&mut self) {
        for entry in self.patterns.values_mut() {
            entry.confidence *= TIME_DECAY;
        }
        self.patterns.retain(|_, e| e.confidence >= PRUNE_THRESHOLD);
        self.rebuild_pair_index();
    }

    fn add_to_pair_index(&mut self, pattern: &BTreeSet<String>) {
        let names: Vec<&String> = pattern.iter().collect();
        for i in 0..names.len() {
            for j in (i + 1)..names.len() {
                let pair = normalize_pair(names[i], names[j]);
                self.pair_index
                    .entry(pair)
                    .or_default()
                    .push(pattern.clone());
            }
        }
    }

    fn rebuild_pair_index(&mut self) {
        self.pair_index.clear();
        let keys: Vec<BTreeSet<String>> = self.patterns.keys().cloned().collect();
        for pattern in &keys {
            self.add_to_pair_index(pattern);
        }
    }
}

fn normalize_pair(a: &str, b: &str) -> (String, String) {
    if a < b {
        (a.to_string(), b.to_string())
    } else {
        (b.to_string(), a.to_string())
    }
}

// ===== Access history (B-tree frequency tracking) =====

/// Tracks which B-trees are accessed most frequently.
/// Persisted in the manifest. Keyed by tree name (survives VACUUM).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AccessHistory {
    /// tree_name -> cumulative frequency score
    pub freq: HashMap<String, f32>,
}

/// Decay factor applied to all entries on checkpoint.
const ACCESS_DECAY: f32 = 0.9;
/// Prune entries below this value.
const ACCESS_PRUNE: f32 = 0.1;

impl AccessHistory {
    pub fn new() -> Self {
        Self {
            freq: HashMap::new(),
        }
    }

    /// Record B-tree names touched in a lock session.
    pub fn record(&mut self, names: &HashSet<String>) {
        for name in names {
            *self.freq.entry(name.clone()).or_insert(0.0) += 1.0;
        }
    }

    /// Apply decay and prune. Call during checkpoint.
    pub fn decay_and_prune(&mut self) {
        for v in self.freq.values_mut() {
            *v *= ACCESS_DECAY;
        }
        self.freq.retain(|_, v| *v >= ACCESS_PRUNE);
    }

    /// Return the top N B-tree names by frequency.
    pub fn top_trees(&self, n: usize) -> Vec<String> {
        let mut entries: Vec<(&String, &f32)> = self.freq.iter().collect();
        entries.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap_or(std::cmp::Ordering::Equal));
        entries.into_iter().take(n).map(|(k, _)| k.clone()).collect()
    }
}

/// Shared prediction state passed from TieredVfs to each TieredHandle.
pub type SharedPrediction = Arc<RwLock<PredictionTable>>;

/// Shared access history passed from TieredVfs to each TieredHandle.
pub type SharedAccessHistory = Arc<RwLock<AccessHistory>>;

#[cfg(test)]
#[path = "test_prediction.rs"]
mod tests;
