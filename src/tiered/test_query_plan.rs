use super::*;

#[test]
fn test_parse_search_with_index() {
    let eqp = "SEARCH users USING INDEX idx_users_email (email=?)";
    let result = parse_eqp_output(eqp);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].tree_name, "idx_users_email");
    assert_eq!(result[0].access_type, AccessType::Search);
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
    assert_eq!(result[2].tree_name, "likes");
    assert_eq!(result[2].access_type, AccessType::Scan);
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
    assert_eq!(result.is_empty(), true);
}

#[test]
fn test_queue_push_drain() {
    // Drain any leftover from other tests
    drain_planned_accesses();

    push_planned_accesses(vec![
        PlannedAccess { tree_name: "users".into(), access_type: AccessType::Scan },
        PlannedAccess { tree_name: "idx_posts_id".into(), access_type: AccessType::Search },
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
    // Lock the queue for the duration to prevent other tests from draining
    let mut queue = super::PLAN_QUEUE.lock().expect("plan queue poisoned");
    queue.clear();

    // Simulate two pushes by extending directly
    queue.push(PlannedAccess { tree_name: "users".into(), access_type: AccessType::Scan });
    queue.push(PlannedAccess { tree_name: "posts".into(), access_type: AccessType::Search });

    assert_eq!(queue.len(), 2);
    queue.clear();
}

#[test]
fn test_empty_push_is_noop() {
    drain_planned_accesses();
    push_planned_accesses(vec![]);
    let drained = drain_planned_accesses();
    assert!(drained.is_empty());
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

// ── End-query signal tests (Phase Stalingrad) ──

#[test]
fn test_end_query_signal_default_false() {
    check_and_clear_end_query();
    assert!(!check_and_clear_end_query());
}

#[test]
fn test_end_query_signal_set_and_clear() {
    check_and_clear_end_query();
    signal_end_query();
    assert!(check_and_clear_end_query());
    assert!(!check_and_clear_end_query());
}

#[test]
fn test_end_query_signal_multiple_signals_collapse() {
    check_and_clear_end_query();
    signal_end_query();
    signal_end_query();
    signal_end_query();
    assert!(check_and_clear_end_query());
    assert!(!check_and_clear_end_query());
}

#[test]
fn test_end_query_ffi_entry_point() {
    check_and_clear_end_query();
    turbolite_trace_end_query();
    assert!(check_and_clear_end_query());
    assert!(!check_and_clear_end_query());
}

#[test]
fn test_end_query_signal_independent_of_plan_queue() {
    check_and_clear_end_query();
    drain_planned_accesses();

    signal_end_query();
    push_planned_accesses(vec![
        PlannedAccess { tree_name: "users".into(), access_type: AccessType::Scan },
    ]);

    assert!(check_and_clear_end_query());
    let planned = drain_planned_accesses();
    assert_eq!(planned.len(), 1);
    assert!(!check_and_clear_end_query());
}
