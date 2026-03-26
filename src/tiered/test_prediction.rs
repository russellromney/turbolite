use super::*;

#[test]
fn lock_session_start_and_flush() {
    let mut session = LockSession::new();
    assert!(!session.active);

    session.start();
    assert!(session.active);

    // Touch only 1 tree: flush returns None (need >= 2)
    session.touch("posts");
    assert_eq!(session.tree_count(), 1);
    let result = session.flush();
    assert!(result.is_none());
    assert!(!session.active);
}

#[test]
fn lock_session_two_trees_returns_pattern() {
    let mut session = LockSession::new();
    session.start();
    session.touch("posts");
    session.touch("users");
    assert_eq!(session.tree_count(), 2);

    let pattern = session.flush().expect("should return pattern with 2+ trees");
    assert!(pattern.contains("posts"));
    assert!(pattern.contains("users"));
    assert_eq!(pattern.len(), 2);
}

#[test]
fn lock_session_dedup_touches() {
    let mut session = LockSession::new();
    session.start();
    assert!(session.touch("posts")); // new
    assert!(!session.touch("posts")); // duplicate
    assert!(session.touch("users")); // new
    assert_eq!(session.tree_count(), 2);
}

#[test]
fn lock_session_inactive_ignores_touches() {
    let mut session = LockSession::new();
    // Not started
    assert!(!session.touch("posts"));
    assert_eq!(session.tree_count(), 0);
}

#[test]
fn prediction_table_observe_and_predict() {
    let mut table = PredictionTable::new();

    let pattern: BTreeSet<String> = ["idx_posts_user", "posts", "users"]
        .into_iter().map(String::from).collect();

    // First observation: confidence = 0.3 (below threshold 0.5)
    table.observe(&pattern);
    assert_eq!(table.patterns.len(), 1);
    let entry = table.patterns.get(&pattern).unwrap();
    assert!((entry.confidence - INITIAL_CONFIDENCE).abs() < f32::EPSILON);

    // Should not fire yet (0.3 < 0.5)
    let seen: HashSet<String> = ["idx_posts_user", "posts"]
        .into_iter().map(String::from).collect();
    assert!(table.predict(&seen).is_none());

    // Reinforce: 0.3 + 0.25 = 0.55 > 0.5
    table.reinforce(&pattern);
    let entry = table.patterns.get(&pattern).unwrap();
    assert!((entry.confidence - 0.55).abs() < f32::EPSILON);

    // Now it should fire
    let predicted = table.predict(&seen).expect("should predict");
    assert_eq!(predicted, vec!["users".to_string()]);
}

#[test]
fn prediction_table_time_decay_with_boost() {
    let mut table = PredictionTable::new();
    let pattern: BTreeSet<String> = ["posts", "users"]
        .into_iter().map(String::from).collect();

    table.observe(&pattern);
    table.reinforce(&pattern); // 0.55

    // Observe again: 0.55 * 0.95 + 0.1 = 0.6225
    table.observe(&pattern);
    let conf = table.patterns.get(&pattern).unwrap().confidence;
    let expected = 0.55 * TIME_DECAY + OBSERVATION_BOOST;
    assert!((conf - expected).abs() < 0.001);
    // Re-observation keeps pattern above threshold
    assert!(conf >= CONFIDENCE_THRESHOLD);
}

#[test]
fn prediction_table_write_decay() {
    let mut table = PredictionTable::new();
    let pattern: BTreeSet<String> = ["posts", "users"]
        .into_iter().map(String::from).collect();

    table.observe(&pattern);
    table.reinforce(&pattern); // 0.55

    table.write_decay("posts"); // 0.55 * 0.7 = 0.385
    let conf = table.patterns.get(&pattern).unwrap().confidence;
    assert!((conf - 0.55 * WRITE_DECAY).abs() < 0.001);
    assert!(conf < CONFIDENCE_THRESHOLD); // Should no longer fire
}

#[test]
fn prediction_table_prune() {
    let mut table = PredictionTable::new();
    let pattern: BTreeSet<String> = ["posts", "users"]
        .into_iter().map(String::from).collect();

    table.observe(&pattern); // 0.3

    // Decay below prune threshold
    for _ in 0..20 {
        table.write_decay("posts");
    }

    table.prune();
    assert!(table.patterns.is_empty());
    assert!(table.pair_index.is_empty());
}

#[test]
fn prediction_table_persist_roundtrip() {
    let mut table = PredictionTable::new();
    let p1: BTreeSet<String> = ["idx_posts_user", "posts", "users"]
        .into_iter().map(String::from).collect();
    let p2: BTreeSet<String> = ["comments", "likes"]
        .into_iter().map(String::from).collect();

    table.observe(&p1);
    table.reinforce(&p1); // 0.55
    table.observe(&p2);
    table.reinforce(&p2); // 0.55

    let persisted = table.to_persisted();
    let restored = PredictionTable::from_persisted(&persisted);

    assert_eq!(restored.patterns.len(), 2);
    assert!((restored.patterns.get(&p1).unwrap().confidence - 0.55).abs() < 0.001);
}

#[test]
fn prediction_pair_index_lookup() {
    let mut table = PredictionTable::new();
    let pattern: BTreeSet<String> = ["idx_posts_user", "posts", "users"]
        .into_iter().map(String::from).collect();
    table.observe(&pattern);
    table.reinforce(&pattern); // 0.55, fires

    // Pair (idx_posts_user, posts) should find the pattern
    let seen: HashSet<String> = ["idx_posts_user", "posts"]
        .into_iter().map(String::from).collect();
    let predicted = table.predict(&seen).expect("should predict");
    assert_eq!(predicted, vec!["users".to_string()]);

    // Pair (idx_posts_user, users) should also find it
    let seen2: HashSet<String> = ["idx_posts_user", "users"]
        .into_iter().map(String::from).collect();
    let predicted2 = table.predict(&seen2).expect("should predict");
    assert_eq!(predicted2, vec!["posts".to_string()]);

    // Pair (idx_posts_user, comments) should NOT match
    let seen3: HashSet<String> = ["idx_posts_user", "comments"]
        .into_iter().map(String::from).collect();
    assert!(table.predict(&seen3).is_none());
}

#[test]
fn access_history_record_and_top() {
    let mut history = AccessHistory::new();

    let names1: HashSet<String> = ["posts", "users", "idx_posts_user"]
        .into_iter().map(String::from).collect();
    let names2: HashSet<String> = ["posts", "users"]
        .into_iter().map(String::from).collect();

    history.record(&names1);
    history.record(&names2);

    // posts and users have freq 2.0, idx_posts_user has freq 1.0
    let top = history.top_trees(2);
    assert_eq!(top.len(), 2);
    assert!(top.contains(&"posts".to_string()));
    assert!(top.contains(&"users".to_string()));
}

#[test]
fn access_history_decay_and_prune() {
    let mut history = AccessHistory::new();

    let names: HashSet<String> = ["posts"].into_iter().map(String::from).collect();
    history.record(&names); // freq = 1.0

    // After ~25 decay cycles: 1.0 * 0.9^25 = ~0.072 < 0.1
    for _ in 0..25 {
        history.decay_and_prune();
    }

    assert!(history.freq.is_empty());
}

#[test]
fn prediction_needs_two_observations_to_fire() {
    let mut table = PredictionTable::new();
    let pattern: BTreeSet<String> = ["posts", "users"]
        .into_iter().map(String::from).collect();

    // First observation: 0.3 < 0.5
    table.observe(&pattern);
    let seen: HashSet<String> = ["posts", "users"]
        .into_iter().map(String::from).collect();
    assert!(table.predict(&seen).is_none());

    // Reinforce after first correct session: 0.3 + 0.25 = 0.55 >= 0.5
    table.reinforce(&pattern);
    // For a 2-name pattern where seen == pattern, no extras to predict -> None
    assert!(table.predict(&seen).is_none());
}

#[test]
fn prediction_three_writes_kill_confidence() {
    let mut table = PredictionTable::new();
    let pattern: BTreeSet<String> = ["idx_posts_user", "posts", "users"]
        .into_iter().map(String::from).collect();

    table.observe(&pattern);
    table.reinforce(&pattern); // 0.55
    table.reinforce(&pattern); // 0.80

    // 3 writes: 0.80 * 0.7^3 = 0.2744
    table.write_decay("posts");
    table.write_decay("posts");
    table.write_decay("posts");

    let conf = table.patterns.get(&pattern).unwrap().confidence;
    assert!(conf < CONFIDENCE_THRESHOLD);
}

#[test]
fn predict_returns_none_when_all_names_seen() {
    let mut table = PredictionTable::new();
    let pattern: BTreeSet<String> = ["idx_posts_user", "posts", "users"]
        .into_iter().map(String::from).collect();

    table.observe(&pattern);
    table.reinforce(&pattern); // 0.55

    // Seeing all 3 names means no extras to predict
    let seen: HashSet<String> = ["idx_posts_user", "posts", "users"]
        .into_iter().map(String::from).collect();
    assert!(table.predict(&seen).is_none());

    // Seeing 2 of 3 should still predict the 3rd
    let partial: HashSet<String> = ["idx_posts_user", "posts"]
        .into_iter().map(String::from).collect();
    let predicted = table.predict(&partial).expect("should predict");
    assert_eq!(predicted, vec!["users".to_string()]);
}

#[test]
fn lock_session_fired_pattern_cleared_on_start() {
    let mut session = LockSession::new();
    session.start();
    session.fired_pattern = Some(["posts", "users"].into_iter().map(String::from).collect());
    session.start(); // re-start clears everything
    assert!(session.fired_pattern.is_none());
    assert!(session.btrees.is_empty());
}

// ===== Subphase (b) tests: SharedAccessHistory wiring =====

#[test]
fn shared_access_history_concurrent_record() {
    let history: SharedAccessHistory = Arc::new(RwLock::new(AccessHistory::new()));

    // Simulate two connections recording concurrently
    let h1 = Arc::clone(&history);
    let h2 = Arc::clone(&history);

    let t1 = std::thread::spawn(move || {
        for _ in 0..100 {
            let names: HashSet<String> = ["posts", "users"]
                .into_iter().map(String::from).collect();
            h1.write().record(&names);
        }
    });
    let t2 = std::thread::spawn(move || {
        for _ in 0..100 {
            let names: HashSet<String> = ["users", "likes"]
                .into_iter().map(String::from).collect();
            h2.write().record(&names);
        }
    });

    t1.join().unwrap();
    t2.join().unwrap();

    let h = history.read();
    assert_eq!(*h.freq.get("posts").unwrap(), 100.0);
    assert_eq!(*h.freq.get("users").unwrap(), 200.0); // recorded by both threads
    assert_eq!(*h.freq.get("likes").unwrap(), 100.0);
}

#[test]
fn access_history_from_manifest_freq() {
    let mut freq: HashMap<String, f32> = HashMap::new();
    freq.insert("posts".into(), 5.0);
    freq.insert("users".into(), 3.0);
    freq.insert("likes".into(), 1.0);

    let mut history = AccessHistory::new();
    history.freq = freq;

    // top_trees should return in descending frequency order
    let top = history.top_trees(2);
    assert_eq!(top.len(), 2);
    assert_eq!(top[0], "posts");
    assert_eq!(top[1], "users");
}

// ===== Subphase (c) tests: checkpoint serialization =====

#[test]
fn prediction_table_prune_on_checkpoint() {
    let mut table = PredictionTable::new();
    let p1: BTreeSet<String> = ["posts", "users"]
        .into_iter().map(String::from).collect();
    let p2: BTreeSet<String> = ["comments", "likes"]
        .into_iter().map(String::from).collect();

    table.observe(&p1); // 0.3
    table.observe(&p2); // 0.3
    table.reinforce(&p1); // p1 -> 0.55
    // p2 stays at 0.3

    // Apply heavy write decay to p2 to push below prune threshold
    for _ in 0..10 {
        table.write_decay("comments");
    }

    table.prune();
    let persisted = table.to_persisted();
    // Only p1 should survive (0.55 > 0.1)
    assert_eq!(persisted.len(), 1);
    assert!(persisted[0].0.contains("posts"));
}

#[test]
fn access_history_decay_preserves_hot_btrees() {
    let mut history = AccessHistory::new();

    // Record "posts" many times, "temp" only once
    for _ in 0..20 {
        history.record(&["posts"].into_iter().map(String::from).collect());
    }
    history.record(&["temp"].into_iter().map(String::from).collect());

    // Multiple decay cycles
    for _ in 0..10 {
        history.decay_and_prune();
    }

    // "posts" should survive (20 * 0.9^10 = ~6.97)
    assert!(history.freq.contains_key("posts"));
    // "temp" still above 0.1 after 10 cycles (0.9^10 = 0.349)
    // After 25 total cycles it drops below
    for _ in 0..15 {
        history.decay_and_prune();
    }
    assert!(!history.freq.contains_key("temp"));
    assert!(history.freq.contains_key("posts")); // 20 * 0.9^25 = ~1.42
}

// ===== Subphase (d) tests: prediction firing =====

#[test]
fn prediction_firing_end_to_end() {
    // Simulate: observe pattern {A, B, C} twice (observe + reinforce) to build confidence.
    // Then on a new session touching A and B, predict C.
    let prediction: SharedPrediction = Arc::new(RwLock::new(PredictionTable::new()));
    let pattern: BTreeSet<String> = ["idx_posts_user", "posts", "users"]
        .into_iter().map(String::from).collect();

    // First session: observe
    prediction.write().observe(&pattern);
    // Reinforce (simulating a correct prediction from previous run)
    prediction.write().reinforce(&pattern); // 0.55

    // New session: touch idx_posts_user, then posts
    let mut session = LockSession::new();
    session.start();
    session.touch("idx_posts_user");
    assert!(session.tree_count() == 1);

    // Touch posts -> 2nd tree -> should fire prediction
    session.touch("posts");
    assert!(session.tree_count() == 2);

    // Check prediction
    let table = prediction.read();
    let extras = table.predict(&session.btrees).expect("should predict");
    assert_eq!(extras, vec!["users".to_string()]);
}

#[test]
fn prediction_reinforcement_on_correct_fire() {
    let mut table = PredictionTable::new();
    let pattern: BTreeSet<String> = ["idx_posts_user", "posts", "users"]
        .into_iter().map(String::from).collect();

    table.observe(&pattern); // 0.3
    table.reinforce(&pattern); // 0.55

    // Simulate: prediction fired for pattern, session touches all 3 names
    let mut session = LockSession::new();
    session.start();
    session.touch("idx_posts_user");
    session.touch("posts");
    session.touch("users");
    session.prediction_fired = true;
    session.fired_pattern = Some(pattern.clone());

    // Reinforce because all names were touched
    assert!(session.fired_pattern.as_ref().unwrap().iter()
        .all(|r| session.btrees.contains(r.as_str())));
    table.reinforce(&pattern); // 0.55 + 0.25 = 0.80

    let conf = table.patterns.get(&pattern).unwrap().confidence;
    assert!((conf - 0.80).abs() < 0.001);
}

#[test]
fn prediction_no_reinforcement_on_partial_touch() {
    let mut table = PredictionTable::new();
    let pattern: BTreeSet<String> = ["idx_posts_user", "posts", "users"]
        .into_iter().map(String::from).collect();

    table.observe(&pattern); // 0.3
    table.reinforce(&pattern); // 0.55

    // Session only touched 2 of 3 predicted names
    let mut session = LockSession::new();
    session.start();
    session.touch("idx_posts_user");
    session.touch("posts");
    // Did NOT touch "users"
    session.prediction_fired = true;
    session.fired_pattern = Some(pattern.clone());

    // Should NOT reinforce because "users" was not touched
    assert!(!session.fired_pattern.as_ref().unwrap().iter()
        .all(|r| session.btrees.contains(r.as_str())));

    // Confidence stays at 0.55
    let conf = table.patterns.get(&pattern).unwrap().confidence;
    assert!((conf - 0.55).abs() < 0.001);
}

#[test]
fn two_name_pattern_does_not_decay_below_threshold() {
    // Regression: 2-name patterns can't fire predictions (seen == pattern),
    // so they never get reinforced. Without the observation boost, they would
    // decay below threshold after ~3 observations.
    let mut table = PredictionTable::new();
    let pattern: BTreeSet<String> = ["posts", "users"]
        .into_iter().map(String::from).collect();

    table.observe(&pattern); // 0.3
    table.reinforce(&pattern); // 0.55

    // Simulate 20 more sessions where this pattern appears but never fires
    for _ in 0..20 {
        table.observe(&pattern);
    }

    let conf = table.patterns.get(&pattern).unwrap().confidence;
    // Should converge to a stable value above threshold, not decay to zero
    assert!(conf > 0.9, "confidence should converge near 1.0 but was {}", conf);
}

#[test]
fn shared_prediction_concurrent_access() {
    let prediction: SharedPrediction = Arc::new(RwLock::new(PredictionTable::new()));
    let pattern: BTreeSet<String> = ["idx_posts_user", "posts", "users"]
        .into_iter().map(String::from).collect();

    // Writer thread
    let p1 = Arc::clone(&prediction);
    let pat1 = pattern.clone();
    let writer = std::thread::spawn(move || {
        for _ in 0..50 {
            let mut table = p1.write();
            table.observe(&pat1);
            table.reinforce(&pat1);
        }
    });

    // Reader thread
    let p2 = Arc::clone(&prediction);
    let reader = std::thread::spawn(move || {
        let seen: HashSet<String> = ["idx_posts_user", "posts"]
            .into_iter().map(String::from).collect();
        for _ in 0..50 {
            let table = p2.read();
            let _ = table.predict(&seen);
        }
    });

    writer.join().unwrap();
    reader.join().unwrap();

    // Pattern should be at high confidence after 50 reinforce cycles
    let table = prediction.read();
    let conf = table.patterns.get(&pattern).unwrap().confidence;
    assert!(conf > 0.9);
}

// ===== Subphase (i) tests: name-based keys =====

#[test]
fn name_based_pattern_survives_conceptual_vacuum() {
    // Patterns keyed by name are immune to root page ID changes.
    // Simulate: same tree names, different "root IDs" (not tracked anymore).
    let mut table = PredictionTable::new();
    let pattern: BTreeSet<String> = ["posts", "users", "idx_posts_user"]
        .into_iter().map(String::from).collect();

    table.observe(&pattern);
    table.reinforce(&pattern); // 0.55

    // After conceptual VACUUM: pattern still exists because names are stable
    let seen: HashSet<String> = ["posts", "users"]
        .into_iter().map(String::from).collect();
    let predicted = table.predict(&seen).expect("pattern survives name-based keying");
    assert_eq!(predicted, vec!["idx_posts_user".to_string()]);
}

#[test]
fn rename_table_old_patterns_become_dead() {
    let mut table = PredictionTable::new();
    let pattern: BTreeSet<String> = ["old_posts", "users"]
        .into_iter().map(String::from).collect();

    table.observe(&pattern);
    table.reinforce(&pattern); // 0.55

    // After RENAME TABLE old_posts -> new_posts:
    // "old_posts" never appears in sessions again. The old pattern won't fire
    // (name mismatch), and prune() applies global time decay each checkpoint.
    // After enough checkpoints (prune calls), it decays below PRUNE_THRESHOLD.
    let seen: HashSet<String> = ["new_posts", "users"]
        .into_iter().map(String::from).collect();
    // Should NOT match old pattern (old_posts != new_posts)
    assert!(table.predict(&seen).is_none());

    // Simulate ~50 checkpoints: each prune() applies *0.95 global decay
    // 0.55 * 0.95^50 = 0.55 * 0.0769 = 0.042 < 0.1 (pruned)
    for _ in 0..50 {
        table.prune();
    }
    assert!(table.patterns.get(&pattern).is_none(), "stale pattern should be pruned after enough checkpoints");
}

#[test]
fn drop_create_same_name_carries_over() {
    let mut table = PredictionTable::new();
    let pattern: BTreeSet<String> = ["posts", "users", "idx_posts_user"]
        .into_iter().map(String::from).collect();

    table.observe(&pattern);
    table.reinforce(&pattern); // 0.55

    // DROP TABLE posts; CREATE TABLE posts(...);
    // Same name, different root page ID. But we key by name, so pattern persists.
    let seen: HashSet<String> = ["posts", "users"]
        .into_iter().map(String::from).collect();
    let predicted = table.predict(&seen).expect("same-name table carries over");
    assert_eq!(predicted, vec!["idx_posts_user".to_string()]);
}
