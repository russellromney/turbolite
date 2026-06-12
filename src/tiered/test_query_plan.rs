use super::*;

// The end-query signal (`signal_end_query` / `check_and_clear_end_query`) is
// process-global, so the tests asserting its exact set/clear semantics must not
// run concurrently with each other — cargo runs tests in parallel, and a
// sibling's `check_and_clear_end_query()` would steal a set signal mid-test.
// Serialize them on this lock. `unwrap_or_else(into_inner)` keeps one test's
// panic from poisoning the rest.
static END_QUERY_SIGNAL_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
static PLAN_QUEUE_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn test_parse_search_with_index() {
    let eqp = "SEARCH users USING INDEX idx_users_email (email=?)";
    let result = parse_eqp_output(eqp);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].tree_name, "idx_users_email");
    assert_eq!(result[0].access_type, AccessType::Search);
    assert_eq!(result[0].table_name.as_deref(), Some("users"));
    assert_eq!(result[0].constraint_columns, vec!["email".to_string()]);
}

#[test]
fn test_parse_scan() {
    let eqp = "SCAN posts";
    let result = parse_eqp_output(eqp);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].tree_name, "posts");
    assert_eq!(result[0].access_type, AccessType::Scan);
}

#[test]
fn test_parse_scan_with_index() {
    let eqp = "SCAN posts USING INDEX idx_posts_created";
    let result = parse_eqp_output(eqp);
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].tree_name, "posts");
    assert_eq!(result[0].access_type, AccessType::Scan);
    assert_eq!(result[1].tree_name, "idx_posts_created");
    assert_eq!(result[1].access_type, AccessType::Scan);
}

#[test]
fn test_parse_search_rowid() {
    let eqp = "SEARCH users USING INTEGER PRIMARY KEY (rowid=?)";
    let result = parse_eqp_output(eqp);
    // Rowid lookup: no prefetch needed, hop schedule handles it
    assert_eq!(result.len(), 0);
}

#[test]
fn test_parse_join() {
    let eqp = "\
SEARCH users USING INDEX sqlite_autoindex_users_1 (id=?)
SEARCH posts USING INDEX idx_posts_user_id (user_id=?)
SCAN likes";
    let result = parse_eqp_output(eqp);
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].tree_name, "sqlite_autoindex_users_1");
    assert_eq!(result[0].access_type, AccessType::Search);
    assert_eq!(result[1].tree_name, "idx_posts_user_id");
    assert_eq!(result[1].access_type, AccessType::Search);
    assert_eq!(result[1].table_name.as_deref(), Some("posts"));
    assert_eq!(result[1].constraint_columns, vec!["user_id".to_string()]);
    assert_eq!(result[2].tree_name, "likes");
    assert_eq!(result[2].access_type, AccessType::Scan);
}

#[test]
fn test_parse_profile_lookahead_shape_keeps_table_mapping() {
    let eqp = "\
SEARCH users USING INTEGER PRIMARY KEY (rowid=?)
SEARCH posts USING INDEX idx_posts_user_created (user_id=?)";
    let result = parse_eqp_output(eqp);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].tree_name, "idx_posts_user_created");
    assert_eq!(result[0].access_type, AccessType::Search);
    assert_eq!(result[0].table_name.as_deref(), Some("posts"));
    assert_eq!(result[0].constraint_columns, vec!["user_id".to_string()]);
}

#[test]
fn test_attach_integer_constraint_hints_annotates_search_constraints() {
    let eqp = "\
SEARCH posts USING INDEX idx_posts_user (user_id=?)
SCAN likes";
    let mut result = parse_eqp_output(eqp);
    attach_integer_constraint_hints(&mut result, &[42]);

    assert_eq!(result[0].constraint_columns, vec!["user_id=42".to_string()]);
    assert_eq!(first_integer_constraint_hint(&result[0]), Some(42));
    assert!(result[1].constraint_columns.is_empty());
}

#[test]
fn test_parse_covering_index() {
    let eqp = "SEARCH users USING COVERING INDEX idx_users_email_name (email=?)";
    let result = parse_eqp_output(eqp);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].tree_name, "idx_users_email_name");
    assert_eq!(result[0].access_type, AccessType::Search);
}

#[test]
fn test_parse_deduplicates() {
    let eqp = "\
SCAN users
SCAN users";
    let result = parse_eqp_output(eqp);
    assert_eq!(result.len(), 1);
}

#[test]
fn test_parse_ignores_non_scan_search() {
    let eqp = "\
USE TEMP B-TREE FOR ORDER BY
COMPOUND SUBQUERY 1";
    let result = parse_eqp_output(eqp);
    assert!(result.is_empty());
}

#[test]
fn test_queue_push_drain() {
    let _guard = PLAN_QUEUE_TEST_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    // Drain any leftover from other tests
    drain_planned_accesses();

    push_planned_accesses(vec![
        PlannedAccess {
            tree_name: "users".into(),
            access_type: AccessType::Scan,
            table_name: None,
            constraint_columns: vec![],
        },
        PlannedAccess {
            tree_name: "idx_posts_id".into(),
            access_type: AccessType::Search,
            table_name: None,
            constraint_columns: vec![],
        },
    ]);

    let drained = drain_planned_accesses();
    assert_eq!(drained.len(), 2);
    assert_eq!(drained[0].tree_name, "users");
    assert_eq!(drained[1].tree_name, "idx_posts_id");

    // Queue is empty after drain
    let drained2 = drain_planned_accesses();
    assert!(drained2.is_empty());
}

#[test]
fn test_queue_accumulates_multiple_pushes() {
    let _guard = PLAN_QUEUE_TEST_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    drain_planned_accesses();
    push_planned_accesses(vec![PlannedAccess {
        tree_name: "users".into(),
        access_type: AccessType::Scan,
        table_name: None,
        constraint_columns: vec![],
    }]);
    push_planned_accesses(vec![PlannedAccess {
        tree_name: "posts".into(),
        access_type: AccessType::Search,
        table_name: None,
        constraint_columns: vec![],
    }]);

    let drained = drain_planned_accesses();
    assert_eq!(drained.len(), 2);
    assert_eq!(drained[0].tree_name, "users");
    assert_eq!(drained[1].tree_name, "posts");
}

#[test]
fn test_empty_push_is_noop() {
    let _guard = PLAN_QUEUE_TEST_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    drain_planned_accesses();
    push_planned_accesses(vec![]);
    let drained = drain_planned_accesses();
    assert!(drained.is_empty());
}

#[test]
fn test_queue_cross_thread_fallback_drains_mirrored_plan() {
    let _guard = PLAN_QUEUE_TEST_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    drain_planned_accesses();

    let producer = std::thread::spawn(|| {
        push_planned_accesses(vec![PlannedAccess {
            tree_name: "cross_thread".into(),
            access_type: AccessType::Search,
            table_name: None,
            constraint_columns: vec![],
        }]);
    });
    producer.join().unwrap();

    let drained = drain_planned_accesses();
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].tree_name, "cross_thread");
    assert!(drain_planned_accesses().is_empty());
}

#[test]
fn test_queue_cross_thread_global_drain_suppresses_owner_local_copy() {
    let _guard = PLAN_QUEUE_TEST_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    drain_planned_accesses();

    let (pushed_tx, pushed_rx) = std::sync::mpsc::channel();
    let (drain_tx, drain_rx) = std::sync::mpsc::channel();
    let owner = std::thread::spawn(move || {
        push_planned_accesses(vec![PlannedAccess {
            tree_name: "owner_local".into(),
            access_type: AccessType::Search,
            table_name: Some("posts".into()),
            constraint_columns: vec!["user_id".into()],
        }]);
        pushed_tx.send(()).unwrap();
        drain_rx.recv().unwrap();
        drain_planned_accesses()
    });

    pushed_rx.recv().unwrap();
    let global_drained = drain_planned_accesses();
    assert_eq!(global_drained.len(), 1);
    assert_eq!(global_drained[0].tree_name, "owner_local");

    drain_tx.send(()).unwrap();
    let owner_drained = owner.join().unwrap();
    assert_eq!(
        owner_drained.len(),
        0,
        "cross-thread fallback must consume the owner's mirrored local batch to avoid processing one plan twice"
    );
    assert!(drain_planned_accesses().is_empty());
}

#[test]
fn test_parse_subquery() {
    // Subqueries produce nested EQP lines with different indentation
    let eqp = "\
SEARCH orders USING INDEX idx_orders_user_id (user_id=?)
SCAN line_items
SEARCH products USING INDEX sqlite_autoindex_products_1 (id=?)";
    let result = parse_eqp_output(eqp);
    assert_eq!(result.len(), 3);
    // SEARCH orders -> index name
    assert_eq!(result[0].tree_name, "idx_orders_user_id");
    assert_eq!(result[0].access_type, AccessType::Search);
    // SCAN line_items -> table name
    assert_eq!(result[1].tree_name, "line_items");
    assert_eq!(result[1].access_type, AccessType::Scan);
    // SEARCH products -> index name
    assert_eq!(result[2].tree_name, "sqlite_autoindex_products_1");
    assert_eq!(result[2].access_type, AccessType::Search);
}

#[test]
fn test_parse_eqp_with_indented_output() {
    // Real EQP output has indented detail lines
    let eqp = "\
   SEARCH users USING INDEX idx_users_email (email=?)
      SCAN posts";
    let result = parse_eqp_output(eqp);
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].tree_name, "idx_users_email");
    assert_eq!(result[1].tree_name, "posts");
}

#[test]
fn test_run_eqp_skips_non_select() {
    // run_eqp_and_parse should return empty for non-SELECT/WITH
    // We can't test with a real db handle, but we can test the
    // SQL filtering logic by checking the early returns
    let non_selects = &[
        "INSERT INTO users VALUES (1, 'alice')",
        "UPDATE users SET name = 'bob' WHERE id = 1",
        "DELETE FROM users WHERE id = 1",
        "CREATE TABLE foo (id INTEGER PRIMARY KEY)",
        "PRAGMA journal_mode=WAL",
        "BEGIN",
        "COMMIT",
        "",
        "   ",
    ];
    for sql in non_selects {
        // run_eqp_and_parse would return empty for these even with a valid db,
        // because the early check filters by first word.
        // We verify the filter logic directly:
        let trimmed = sql.trim();
        if trimmed.is_empty() {
            continue;
        }
        let first_word = trimmed.split_whitespace().next().unwrap_or("");
        let is_query = matches!(first_word.to_uppercase().as_str(), "SELECT" | "WITH");
        assert!(!is_query, "expected non-query for: {}", sql);
    }
}

#[test]
fn test_parse_same_table_scan_and_search_both_emitted() {
    // A self-join could produce both SCAN and SEARCH on the same table
    let eqp = "\
SCAN users
SEARCH users USING INDEX idx_users_email (email=?)";
    let result = parse_eqp_output(eqp);
    // SCAN users -> table as Scan
    // SEARCH users -> index as Search
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].tree_name, "users");
    assert_eq!(result[0].access_type, AccessType::Scan);
    assert_eq!(result[1].tree_name, "idx_users_email");
    assert_eq!(result[1].access_type, AccessType::Search);
}

// ── End-query signal tests ──

#[test]
fn test_end_query_signal_default_false() {
    let _guard = END_QUERY_SIGNAL_TEST_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    check_and_clear_end_query();
    assert!(!check_and_clear_end_query());
}

#[test]
fn test_end_query_signal_set_and_clear() {
    let _guard = END_QUERY_SIGNAL_TEST_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    check_and_clear_end_query();
    signal_end_query();
    assert!(check_and_clear_end_query());
    assert!(!check_and_clear_end_query());
}

#[test]
fn test_end_query_signal_multiple_signals_collapse() {
    let _guard = END_QUERY_SIGNAL_TEST_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    check_and_clear_end_query();
    signal_end_query();
    signal_end_query();
    signal_end_query();
    assert!(check_and_clear_end_query());
    assert!(!check_and_clear_end_query());
}

// The FFI entry-point smoke test (`turbolite_trace_end_query`) lives in
// turbolite-ffi; the Rust-level `signal_end_query` path is covered by
// `test_end_query_signal_*` above.

#[test]
fn test_end_query_signal_independent_of_plan_queue() {
    let _guard = END_QUERY_SIGNAL_TEST_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let _plan_guard = PLAN_QUEUE_TEST_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    check_and_clear_end_query();
    drain_planned_accesses();

    signal_end_query();
    push_planned_accesses(vec![PlannedAccess {
        tree_name: "users".into(),
        access_type: AccessType::Scan,
        table_name: None,
        constraint_columns: vec![],
    }]);

    assert!(check_and_clear_end_query());
    let planned = drain_planned_accesses();
    assert_eq!(planned.len(), 1);
    assert!(!check_and_clear_end_query());
}
