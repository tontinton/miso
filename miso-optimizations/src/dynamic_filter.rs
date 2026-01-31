use miso_common::watch::Watch;
use miso_workflow::{WorkflowStep, scan::Scan};
use miso_workflow_types::join::JoinType;

use crate::pattern;

use super::{Group, Optimization, OptimizationResult, Pattern};

pub struct DynamicFilter {
    max_distinct_values: u64,
}

impl DynamicFilter {
    pub fn new(max_distinct_values: u64) -> Self {
        Self {
            max_distinct_values,
        }
    }
}

impl Optimization for DynamicFilter {
    fn pattern(&self) -> Pattern {
        pattern!(Scan [Count Limit TopN Summarize Sort Filter]*? Join)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> OptimizationResult {
        let WorkflowStep::Scan(left_scan) = &steps[0] else {
            return OptimizationResult::Unchanged;
        };
        let WorkflowStep::Join(join, workflow) = &steps[steps.len() - 1] else {
            return OptimizationResult::Unchanged;
        };
        let WorkflowStep::Scan(right_scan) = &workflow.steps[0] else {
            return OptimizationResult::Unchanged;
        };

        let (left_join_field, right_join_field) = &join.on;

        let left_steps_after_scan = if steps.len() > 2 {
            &steps[1..steps.len() - 1]
        } else {
            &[]
        };
        let right_steps_after_scan = if workflow.steps.len() > 1 {
            &workflow.steps[1..workflow.steps.len()]
        } else {
            &[]
        };

        let left_dcount = calculate_max_distinct_count(
            left_join_field.to_string(),
            left_scan,
            left_steps_after_scan,
        )
        .unwrap_or(self.max_distinct_values);
        let right_dcount = calculate_max_distinct_count(
            right_join_field.to_string(),
            right_scan,
            right_steps_after_scan,
        )
        .unwrap_or(self.max_distinct_values);

        if left_dcount >= self.max_distinct_values && right_dcount >= self.max_distinct_values {
            return OptimizationResult::Unchanged;
        }

        let mut left_scan = left_scan.clone();
        let mut workflow = workflow.clone();
        let WorkflowStep::Scan(right_scan) = &mut workflow.steps[0] else {
            return OptimizationResult::Unchanged;
        };

        let watch = Watch::default();
        right_scan.dynamic_filter_tx = Some(watch.clone());

        let producer_is_left = match join.type_ {
            JoinType::Inner => {
                if left_dcount < self.max_distinct_values && right_dcount < self.max_distinct_values
                {
                    left_dcount <= right_dcount
                } else if left_dcount < self.max_distinct_values {
                    true
                } else if right_dcount < self.max_distinct_values {
                    false
                } else {
                    return OptimizationResult::Unchanged;
                }
            }

            JoinType::Left if left_dcount < self.max_distinct_values => true,

            JoinType::Right if right_dcount < self.max_distinct_values => false,

            // Neither side strictly allowed, but at least one is small.
            _ => {
                if left_dcount >= self.max_distinct_values
                    && right_dcount >= self.max_distinct_values
                {
                    return OptimizationResult::Unchanged;
                }

                right_scan.add_not_to_dynamic_filter = true;
                left_dcount <= right_dcount
            }
        };

        if producer_is_left {
            right_scan.dynamic_filter_rx = Some(watch);
        } else {
            left_scan.dynamic_filter_rx = Some(watch);
        }

        let mut steps = steps.to_vec();
        *steps.first_mut().unwrap() = WorkflowStep::Scan(left_scan);
        *steps.last_mut().unwrap() = WorkflowStep::Join(join.clone(), workflow);

        OptimizationResult::Changed(steps)
    }
}

fn calculate_max_distinct_count(
    join_field: String,
    scan: &Scan,
    steps_after_scan: &[WorkflowStep],
) -> Option<u64> {
    let mut dcount: Option<u64> = None;
    let mut prev_dcount: Option<u64> = None;
    let mut fields = vec![join_field];

    for step in steps_after_scan.iter().rev() {
        match step {
            WorkflowStep::Count => dcount = Some(1),

            WorkflowStep::Limit(limit) | WorkflowStep::TopN(.., limit) => {
                dcount = Some(dcount.map_or(*limit, |d: u64| d.min(*limit)));
            }

            WorkflowStep::Summarize(summarize) => {
                if prev_dcount.is_some() {
                    // Seems a bit more complicated, don't optimize this for now.
                    return None;
                }
                prev_dcount = dcount.take();
                fields = summarize.by.iter().map(|bf| bf.name.to_string()).collect();
            }

            WorkflowStep::Sort(..) | WorkflowStep::Filter(..) => {}

            // Unsupported (need to think about project & extend):
            _ => return None,
        }
    }

    let dcounts: Vec<_> = fields
        .iter()
        .flat_map(|f| scan.get_field_stats(f)?.distinct_count)
        .collect();

    if dcounts.len() == fields.len() {
        dcount = dcounts
            .into_iter()
            .try_fold(1u64, |acc, x| acc.checked_mul(x))
            .map(|summarize_dc| dcount.map_or(summarize_dc, |limit_dc| summarize_dc.min(limit_dc)))
            .or(dcount);
    }

    prev_dcount.map_or(dcount, |pdc| Some(dcount.map_or(pdc, |dc| dc.min(pdc))))
}

#[cfg(test)]
mod tests {
    use std::{any::Any, fmt, sync::Arc};

    use async_trait::async_trait;
    use color_eyre::Result;
    use hashbrown::HashMap;
    use miso_connectors::{
        Collection, Connector, QueryHandle, QueryResponse, Split,
        stats::{CollectionStats, ConnectorStats, FieldStats},
    };
    use miso_workflow::{Workflow, WorkflowStep, scan::Scan};
    use miso_workflow_types::{expr::Expr, join::Join, summarize::Summarize};
    use parking_lot::Mutex;
    use serde::{Deserialize, Serialize};
    use test_case::test_case;

    use super::{
        DynamicFilter, JoinType, Optimization, OptimizationResult, calculate_max_distinct_count,
    };
    use crate::test_utils::{field, summarize_by};

    #[derive(Debug, Clone, Serialize, Deserialize, Default)]
    struct TestHandle;

    #[typetag::serde]
    impl QueryHandle for TestHandle {
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    impl fmt::Display for TestHandle {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "TestHandle")
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct TestConnector;

    #[async_trait]
    #[typetag::serde]
    impl Connector for TestConnector {
        fn get_collection(&self, _: &str) -> Option<Collection> {
            Some(Collection::default())
        }

        fn get_handle(&self, _: &str) -> Result<Box<dyn QueryHandle>> {
            Ok(Box::new(TestHandle))
        }

        fn get_splits(&self) -> Vec<Box<dyn Split>> {
            vec![]
        }

        async fn query(
            &self,
            _: &str,
            _: &dyn QueryHandle,
            _: Option<&dyn Split>,
        ) -> Result<QueryResponse> {
            unimplemented!()
        }

        async fn close(&self) {}
    }

    fn scan(field_stats: Vec<(&str, u64)>) -> Scan {
        let mut collection_stats = CollectionStats::new();
        for (field, distinct_count) in field_stats {
            collection_stats.insert(
                field.to_string(),
                FieldStats {
                    distinct_count: Some(distinct_count),
                },
            );
        }
        let mut connector_stats = ConnectorStats::new();
        connector_stats.insert("c".to_string(), collection_stats);

        Scan {
            connector_name: "test".to_string(),
            collection: "c".to_string(),
            static_fields: HashMap::new(),
            connector: Arc::new(TestConnector),
            handle: Arc::new(TestHandle),
            split: None,
            stats: Arc::new(Mutex::new(connector_stats)),
            dynamic_filter_tx: None,
            dynamic_filter_rx: None,
            add_not_to_dynamic_filter: false,
        }
    }

    fn calc(scan: &Scan, steps: &[WorkflowStep]) -> Option<u64> {
        calculate_max_distinct_count("id".to_string(), scan, steps)
    }

    #[test]
    fn limit_takes_minimum_of_limit_and_stats() {
        assert_eq!(
            calc(&scan(vec![("id", 100)]), &[WorkflowStep::Limit(10)]),
            Some(10)
        );
        assert_eq!(
            calc(&scan(vec![("id", 5)]), &[WorkflowStep::Limit(100)]),
            Some(5)
        );
        assert_eq!(calc(&scan(vec![]), &[WorkflowStep::Limit(25)]), Some(25));
    }

    #[test]
    fn summarize_multiplies_group_by_distinct_counts() {
        assert_eq!(
            calc(
                &scan(vec![("a", 5), ("b", 7)]),
                &[summarize_by(&["a", "b"])]
            ),
            Some(35)
        );
    }

    #[test]
    fn summarize_with_empty_group_by_returns_one() {
        assert_eq!(
            calc(&scan(vec![("id", 100)]), &[summarize_by(&[])]),
            Some(1)
        );
    }

    #[test]
    fn summarize_with_limit_takes_minimum() {
        let s = scan(vec![("category", 100)]);
        assert_eq!(
            calc(&s, &[summarize_by(&["category"]), WorkflowStep::Limit(10)]),
            Some(10)
        );
        assert_eq!(
            calc(&s, &[WorkflowStep::Limit(10), summarize_by(&["category"])]),
            Some(10)
        );
    }

    #[test]
    fn summarize_falls_back_to_limit_when_stats_incomplete() {
        let s = scan(vec![("a", 10)]);
        assert_eq!(
            calc(&s, &[summarize_by(&["a", "b"]), WorkflowStep::Limit(50)]),
            Some(50)
        );
    }

    #[test]
    fn two_summarizes_without_limit_uses_outer() {
        let s = scan(vec![("a", 10), ("b", 20)]);
        assert_eq!(
            calc(&s, &[summarize_by(&["a"]), summarize_by(&["b"])]),
            Some(10)
        );
    }

    #[test]
    fn two_summarizes_with_limit_after_bails_out() {
        let s = scan(vec![("a", 10), ("b", 20)]);
        assert_eq!(
            calc(
                &s,
                &[
                    summarize_by(&["a"]),
                    summarize_by(&["b"]),
                    WorkflowStep::Limit(5)
                ]
            ),
            None
        );
    }

    #[test]
    fn summarize_limit_summarize_takes_minimum() {
        let s = scan(vec![("a", 10), ("b", 20)]);
        assert_eq!(
            calc(
                &s,
                &[
                    summarize_by(&["a"]),
                    WorkflowStep::Limit(5),
                    summarize_by(&["b"])
                ]
            ),
            Some(5)
        );
    }

    #[test]
    fn count_after_summarize_returns_one() {
        let s = scan(vec![("category", 100)]);
        assert_eq!(
            calc(&s, &[summarize_by(&["category"]), WorkflowStep::Count]),
            Some(1)
        );
    }

    #[test]
    fn overflow_in_group_by_multiplication() {
        let s = scan(vec![("a", u64::MAX), ("b", 2)]);
        assert_eq!(calc(&s, &[summarize_by(&["a", "b"])]), None);
        assert_eq!(
            calc(&s, &[summarize_by(&["a", "b"]), WorkflowStep::Limit(100)]),
            Some(100)
        );
    }

    #[test]
    fn unsupported_steps_return_none() {
        let s = scan(vec![("id", 100)]);
        assert_eq!(calc(&s, &[WorkflowStep::Project(vec![])]), None);
        assert_eq!(calc(&s, &[WorkflowStep::Extend(vec![])]), None);
        assert_eq!(calc(&s, &[WorkflowStep::MuxLimit(10)]), None);
        assert_eq!(
            calc(
                &s,
                &[WorkflowStep::MuxSummarize(Summarize {
                    aggs: HashMap::new(),
                    by: vec![]
                })]
            ),
            None
        );
    }

    #[test]
    fn unsupported_step_in_middle_returns_none() {
        let s = scan(vec![("id", 100)]);
        assert_eq!(
            calc(
                &s,
                &[
                    WorkflowStep::Filter(Expr::Literal(true.into())),
                    WorkflowStep::Project(vec![]),
                    WorkflowStep::Limit(10),
                ]
            ),
            None
        );
    }

    #[test]
    fn zero_distinct_count_results_in_zero() {
        assert_eq!(
            calc(
                &scan(vec![("a", 0), ("b", 100)]),
                &[summarize_by(&["a", "b"])]
            ),
            Some(0)
        );
    }

    fn apply_opt(left: u64, right: u64, join_type: JoinType) -> (bool, bool) {
        let join_step = WorkflowStep::Join(
            Join {
                on: (field("id"), field("id")),
                type_: join_type,
                partitions: 1,
            },
            Workflow::new(vec![WorkflowStep::Scan(scan(vec![("id", right)]))]),
        );
        let steps = vec![WorkflowStep::Scan(scan(vec![("id", left)])), join_step];

        match DynamicFilter::new(100).apply(&steps, &[]) {
            OptimizationResult::Changed(s) => {
                let WorkflowStep::Join(_, w) = &s[1] else {
                    unreachable!()
                };
                let WorkflowStep::Scan(s) = &w.steps[0] else {
                    unreachable!()
                };
                (true, s.add_not_to_dynamic_filter)
            }
            _ => (false, false),
        }
    }

    #[test_case(10, 20, JoinType::Inner => (true, false); "inner_both_small")]
    #[test_case(10, 200, JoinType::Inner => (true, false); "inner_left_small")]
    #[test_case(200, 10, JoinType::Inner => (true, false); "inner_right_small")]
    #[test_case(200, 200, JoinType::Inner => (false, false); "inner_both_large")]
    #[test_case(10, 200, JoinType::Left => (true, false); "left_preferred_small")]
    #[test_case(200, 10, JoinType::Left => (true, true); "left_opposite_small_adds_not")]
    #[test_case(200, 200, JoinType::Left => (false, false); "left_both_large")]
    #[test_case(200, 10, JoinType::Right => (true, false); "right_preferred_small")]
    #[test_case(10, 200, JoinType::Right => (true, true); "right_opposite_small_adds_not")]
    #[test_case(200, 200, JoinType::Right => (false, false); "right_both_large")]
    fn dynamic_filter_join_types(left: u64, right: u64, jt: JoinType) -> (bool, bool) {
        apply_opt(left, right, jt)
    }
}
