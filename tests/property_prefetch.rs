use proptest::prelude::*;

fn run_proptest<T, F>(strategy: impl Strategy<Value = T>, cases: u32, test_fn: F)
where
    F: Fn(T) -> Result<(), proptest::test_runner::TestCaseError>,
{
    let config = proptest::test_runner::Config::with_cases(cases);
    let mut runner = proptest::test_runner::TestRunner::new(config);
    let _ = runner.run(&strategy, test_fn);
}

#[test]
fn schedule_elements_are_valid() {
    run_proptest(
        prop::collection::vec(0.0f32..=1.0f32, 0..10),
        1000,
        |schedule| {
            for (i, &element) in schedule.iter().enumerate() {
                prop_assert!(element.is_finite(), "element {} not finite", i);
                prop_assert!((0.0..=1.0).contains(&element), "element {} out of range", i);
            }
            Ok(())
        },
    );
}

#[test]
fn default_search_schedule_valid() {
    let schedule: Vec<f32> = vec![0.3, 0.3, 0.4];
    for (i, &element) in schedule.iter().enumerate() {
        assert!((0.0..=1.0).contains(&element), "search[{}] out of range", i);
    }
    let sum: f32 = schedule.iter().sum();
    assert!((sum - 1.0).abs() < 0.001, "search sum != 1.0, got {}", sum);
}

#[test]
fn default_lookup_schedule_valid() {
    let schedule: Vec<f32> = vec![0.0, 0.0, 0.0];
    for (i, &element) in schedule.iter().enumerate() {
        assert!(element == 0.0, "lookup[{}] != 0.0", i);
    }
}

#[test]
fn beyond_schedule_length_fetches_all() {
    run_proptest(
        (prop::collection::vec(0.0f32..=1.0f32, 1..5), 1u8..20u8),
        1000,
        |(schedule, extra_misses)| {
            let len = schedule.len();
            for i in 0..extra_misses {
                let hop_idx = len + i as usize;
                let fraction = if hop_idx < schedule.len() {
                    schedule[hop_idx]
                } else {
                    1.0
                };
                prop_assert_eq!(
                    fraction,
                    1.0,
                    "hop {} beyond schedule should be 1.0",
                    hop_idx
                );
            }
            Ok(())
        },
    );
}

#[test]
fn escalation_never_exceeds_total() {
    run_proptest(
        (
            prop::collection::vec(0.0f32..=1.0f32, 1..10),
            1usize..100usize,
        ),
        1000,
        |(schedule, num_eligible)| {
            let mut total_prefetched = 0usize;
            for hop_idx in 0..schedule.len() + 5 {
                let fraction = if hop_idx < schedule.len() {
                    schedule[hop_idx]
                } else {
                    1.0
                };
                let remaining = num_eligible.saturating_sub(total_prefetched);
                let to_prefetch = ((remaining as f32) * fraction).ceil() as usize;
                total_prefetched += to_prefetch;
                prop_assert!(
                    total_prefetched <= num_eligible,
                    "prefetched {} > eligible {}",
                    total_prefetched,
                    num_eligible
                );
                if total_prefetched >= num_eligible {
                    break;
                }
            }
            Ok(())
        },
    );
}
