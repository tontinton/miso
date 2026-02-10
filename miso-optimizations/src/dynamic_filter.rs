//! Speeds up joins by filtering early when one side is small.
//!
//! Joins can be expensive when both sides return lots of data. But often one side
//! has few distinct values on the join key - think joining a small lookup table
//! to a huge event stream. If we know those values upfront, we can filter the
//! big side *before* reading all that data.
//!
//! This optimization estimates distinct counts on each join side using connector
//! stats (field cardinality) combined with pipeline hints (limits, summarize groups).
//! When one side is small enough, we set up a `Watch` channel: the small side
//! broadcasts its join key values, the big side filters incoming data to only
//! matching keys.
//!
//! For inner joins, either side can be the producer (we pick the smaller one).
//! For left/right joins, semantics matter - we prefer the side that preserves
//! all rows. When we have to use the "wrong" side, we add NOT to the filter
//! (e.g., for a left join with small right side, we filter out non-matching
//! rows from the left side, which is safe because they'd produce no output anyway).

use miso_common::watch::Watch;
use miso_workflow::{WorkflowStep, scan::Scan};
use miso_workflow_types::expr::Expr;
use miso_workflow_types::field::Field;
use miso_workflow_types::join::JoinType;
use miso_workflow_types::project::ProjectField;

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
        pattern!(Scan [Count Limit TopN Summarize Sort Filter Project Extend Rename]*? Join)
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

        let left_dcount =
            calculate_max_distinct_count(left_join_field.clone(), left_scan, left_steps_after_scan)
                .unwrap_or(self.max_distinct_values);
        let right_dcount = calculate_max_distinct_count(
            right_join_field.clone(),
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

fn resolve_fields(
    fields: &mut [Field],
    project_fields: &[ProjectField],
    is_extend: bool,
) -> Option<()> {
    for f in fields.iter_mut() {
        if let Some(pf) = project_fields.iter().find(|pf| pf.to == *f) {
            match &pf.from {
                Expr::Field(source) => *f = source.clone(),
                _ => return None,
            }
        } else if !is_extend {
            return None;
        }
    }
    Some(())
}

fn calculate_max_distinct_count(
    join_field: Field,
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
                fields = summarize.by.iter().map(|bf| bf.name.clone()).collect();
            }

            WorkflowStep::Project(pf) => {
                resolve_fields(&mut fields, pf, false)?;
            }
            WorkflowStep::Extend(pf) => {
                resolve_fields(&mut fields, pf, true)?;
            }

            WorkflowStep::Rename(renames) => {
                for f in fields.iter_mut() {
                    if let Some((from, _)) = renames.iter().find(|(_, to)| to == f) {
                        *f = from.clone();
                    }
                }
            }

            WorkflowStep::Sort(..) | WorkflowStep::Filter(..) => {}

            _ => return None,
        }
    }

    let dcounts: Vec<_> = fields
        .iter()
        .flat_map(|f| scan.get_field_stats(&f.to_string())?.distinct_count)
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
    use miso_workflow_types::{join::Join, value::Value};
    use parking_lot::Mutex;
    use serde::{Deserialize, Serialize};
    use test_case::test_case;

    use super::{
        DynamicFilter, JoinType, Optimization, OptimizationResult, calculate_max_distinct_count,
    };
    use crate::test_utils::{field, literal_project, rename_project, sort_asc, summarize_by};

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
        calculate_max_distinct_count(field("id"), scan, steps)
    }

    #[test_case(vec![("id", 100)], &[] => Some(100); "field_stats_only")]
    #[test_case(vec![], &[] => None; "no_stats_returns_none")]
    #[test_case(vec![("other", 50)], &[] => None; "unrelated_field_stats")]
    fn calc_stats_only(stats: Vec<(&str, u64)>, steps: &[WorkflowStep]) -> Option<u64> {
        calc(&scan(stats), steps)
    }

    #[test_case(vec![("id", 100)], 10 => Some(10); "limit_below_stats")]
    #[test_case(vec![("id", 5)], 100 => Some(5); "stats_below_limit")]
    #[test_case(vec![], 25 => Some(25); "limit_without_stats")]
    fn calc_limit(stats: Vec<(&str, u64)>, limit: u64) -> Option<u64> {
        calc(&scan(stats), &[WorkflowStep::Limit(limit)])
    }

    #[test_case(vec![("id", 100)], 10 => Some(10); "topn_below_stats")]
    #[test_case(vec![("id", 5)], 100 => Some(5); "stats_below_topn")]
    #[test_case(vec![], 25 => Some(25); "topn_without_stats")]
    fn calc_topn(stats: Vec<(&str, u64)>, limit: u64) -> Option<u64> {
        calc(
            &scan(stats),
            &[WorkflowStep::TopN(vec![sort_asc(field("id"))], limit)],
        )
    }

    #[test_case(vec![("id", 100)] => Some(1); "count_returns_one")]
    #[test_case(vec![] => Some(1); "count_without_stats")]
    fn calc_count(stats: Vec<(&str, u64)>) -> Option<u64> {
        calc(&scan(stats), &[WorkflowStep::Count])
    }

    #[test_case(&["a", "b"], vec![("a", 5), ("b", 7)] => Some(35); "multiplies_group_by_dcounts")]
    #[test_case(&[], vec![("id", 100)] => Some(1); "empty_group_by_returns_one")]
    #[test_case(&["a", "b"], vec![("a", 0), ("b", 100)] => Some(0); "zero_dcount_propagates")]
    #[test_case(&["a", "b"], vec![("a", u64::MAX), ("b", 2)] => None; "overflow_returns_none")]
    #[test_case(&["a", "b"], vec![("a", 10)] => None; "incomplete_stats_returns_none")]
    fn calc_summarize(by: &[&str], stats: Vec<(&str, u64)>) -> Option<u64> {
        calc(&scan(stats), &[summarize_by(by)])
    }

    #[test]
    fn summarize_with_limit_takes_minimum() {
        let s = scan(vec![("cat", 100)]);
        assert_eq!(
            calc(&s, &[summarize_by(&["cat"]), WorkflowStep::Limit(10)]),
            Some(10)
        );
        assert_eq!(
            calc(&s, &[WorkflowStep::Limit(10), summarize_by(&["cat"])]),
            Some(10)
        );
    }

    #[test]
    fn summarize_falls_back_to_limit_when_stats_incomplete() {
        assert_eq!(
            calc(
                &scan(vec![("a", 10)]),
                &[summarize_by(&["a", "b"]), WorkflowStep::Limit(50)]
            ),
            Some(50)
        );
    }

    #[test]
    fn overflow_with_limit_falls_back_to_limit() {
        assert_eq!(
            calc(
                &scan(vec![("a", u64::MAX), ("b", 2)]),
                &[summarize_by(&["a", "b"]), WorkflowStep::Limit(100)]
            ),
            Some(100)
        );
    }

    #[test]
    fn count_after_summarize_returns_one() {
        assert_eq!(
            calc(
                &scan(vec![("cat", 100)]),
                &[summarize_by(&["cat"]), WorkflowStep::Count]
            ),
            Some(1)
        );
    }

    #[test]
    fn two_summarizes_uses_outer() {
        let s = scan(vec![("a", 10), ("b", 20)]);
        assert_eq!(
            calc(&s, &[summarize_by(&["a"]), summarize_by(&["b"])]),
            Some(10)
        );
    }

    #[test]
    fn two_summarizes_with_limit_after_second_bails_out() {
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

    #[test_case(
        vec![("original_id", 50)],
        vec![WorkflowStep::Project(vec![rename_project("id", "original_id")])]
        => Some(50); "project_rename_resolves_to_source_stats"
    )]
    #[test_case(
        vec![("id", 100)],
        vec![WorkflowStep::Project(vec![literal_project("id", Value::Int(1))])]
        => None; "project_with_non_field_expr_bails"
    )]
    #[test_case(
        vec![("id", 100)],
        vec![WorkflowStep::Project(vec![rename_project("other", "id")])]
        => None; "project_drops_join_field"
    )]
    #[test_case(
        vec![("id", 100)],
        vec![WorkflowStep::Project(vec![])]
        => None; "empty_project_drops_join_field"
    )]
    #[test_case(
        vec![("id", 100)],
        vec![WorkflowStep::Extend(vec![literal_project("extra", Value::Int(42))])]
        => Some(100); "extend_passthrough_when_field_not_in_extend_list"
    )]
    #[test_case(
        vec![("raw", 30)],
        vec![WorkflowStep::Extend(vec![rename_project("id", "raw")])]
        => Some(30); "extend_rename_resolves_to_source_stats"
    )]
    #[test_case(
        vec![("id", 100)],
        vec![WorkflowStep::Extend(vec![literal_project("id", Value::Int(1))])]
        => None; "extend_overwrites_join_field_with_literal_bails"
    )]
    fn calc_project_extend(stats: Vec<(&str, u64)>, steps: Vec<WorkflowStep>) -> Option<u64> {
        calc(&scan(stats), &steps)
    }

    #[test_case(
        vec![("original_id", 50)],
        vec![WorkflowStep::Rename(vec![(field("original_id"), field("id"))])]
        => Some(50); "rename_resolves_to_source_stats"
    )]
    #[test_case(
        vec![("id", 100)],
        vec![WorkflowStep::Rename(vec![(field("other"), field("renamed"))])]
        => Some(100); "rename_passthrough_when_field_not_renamed"
    )]
    #[test_case(
        vec![("raw", 30)],
        vec![
            WorkflowStep::Rename(vec![(field("raw"), field("mid"))]),
            WorkflowStep::Rename(vec![(field("mid"), field("id"))]),
        ]
        => Some(30); "chained_renames_resolve_through"
    )]
    fn calc_rename(stats: Vec<(&str, u64)>, steps: Vec<WorkflowStep>) -> Option<u64> {
        calc(&scan(stats), &steps)
    }

    #[test]
    fn rename_then_summarize_resolves_through() {
        assert_eq!(
            calc(
                &scan(vec![("raw_key", 25)]),
                &[
                    WorkflowStep::Rename(vec![(field("raw_key"), field("mapped"))]),
                    summarize_by(&["mapped"]),
                ]
            ),
            Some(25)
        );
    }

    #[test]
    fn extend_then_summarize_resolves_through() {
        assert_eq!(
            calc(
                &scan(vec![("raw_key", 25)]),
                &[
                    WorkflowStep::Extend(vec![rename_project("mapped", "raw_key")]),
                    summarize_by(&["mapped"]),
                ]
            ),
            Some(25)
        );
    }

    #[test_case(
        vec![WorkflowStep::Sort(vec![sort_asc(field("id"))])]
        => Some(50); "sort_is_passthrough"
    )]
    #[test_case(
        vec![WorkflowStep::Filter(miso_workflow_types::expr::Expr::Literal(true.into()))]
        => Some(50); "filter_is_passthrough"
    )]
    #[test_case(
        vec![
            WorkflowStep::Filter(miso_workflow_types::expr::Expr::Literal(true.into())),
            WorkflowStep::Sort(vec![sort_asc(field("id"))]),
            WorkflowStep::Limit(10),
        ]
        => Some(10); "mixed_passthrough_steps_with_limit"
    )]
    fn calc_passthrough(steps: Vec<WorkflowStep>) -> Option<u64> {
        calc(&scan(vec![("id", 50)]), &steps)
    }

    #[test]
    fn unsupported_step_returns_none() {
        assert_eq!(
            calc(&scan(vec![("id", 100)]), &[WorkflowStep::MuxLimit(10)]),
            None
        );
    }

    struct ApplyResult {
        changed: bool,
        add_not: bool,
        left_has_rx: bool,
        right_has_rx: bool,
    }

    fn apply_opt(left: u64, right: u64, join_type: JoinType) -> ApplyResult {
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
                let WorkflowStep::Scan(left_scan) = &s[0] else {
                    unreachable!()
                };
                let WorkflowStep::Join(_, w) = &s[1] else {
                    unreachable!()
                };
                let WorkflowStep::Scan(right_scan) = &w.steps[0] else {
                    unreachable!()
                };
                ApplyResult {
                    changed: true,
                    add_not: right_scan.add_not_to_dynamic_filter,
                    left_has_rx: left_scan.dynamic_filter_rx.is_some(),
                    right_has_rx: right_scan.dynamic_filter_rx.is_some(),
                }
            }
            _ => ApplyResult {
                changed: false,
                add_not: false,
                left_has_rx: false,
                right_has_rx: false,
            },
        }
    }

    #[test_case(10, 20, JoinType::Inner; "inner_both_small_picks_left_as_producer")]
    #[test_case(10, 200, JoinType::Inner; "inner_left_small")]
    fn inner_left_produces(left: u64, right: u64, jt: JoinType) {
        let r = apply_opt(left, right, jt);
        assert!(r.changed);
        assert!(!r.add_not);
        assert!(!r.left_has_rx, "left is producer, should not have rx");
        assert!(r.right_has_rx, "right is consumer, should have rx");
    }

    #[test]
    fn inner_right_small_produces_from_right() {
        let r = apply_opt(200, 10, JoinType::Inner);
        assert!(r.changed);
        assert!(!r.add_not);
        assert!(r.left_has_rx, "left is consumer, should have rx");
        assert!(!r.right_has_rx, "right is producer, should not have rx");
    }

    #[test_case(200, 200, JoinType::Inner; "inner_both_large")]
    #[test_case(200, 200, JoinType::Left; "left_both_large")]
    #[test_case(200, 200, JoinType::Right; "right_both_large")]
    fn both_large_unchanged(left: u64, right: u64, jt: JoinType) {
        let r = apply_opt(left, right, jt);
        assert!(!r.changed);
    }

    #[test]
    fn left_join_preferred_side_small() {
        let r = apply_opt(10, 200, JoinType::Left);
        assert!(r.changed);
        assert!(!r.add_not);
        assert!(!r.left_has_rx, "left is producer");
        assert!(r.right_has_rx, "right is consumer");
    }

    #[test]
    fn left_join_opposite_side_small_adds_not() {
        let r = apply_opt(200, 10, JoinType::Left);
        assert!(r.changed);
        assert!(r.add_not);
    }

    #[test]
    fn right_join_preferred_side_small() {
        let r = apply_opt(200, 10, JoinType::Right);
        assert!(r.changed);
        assert!(!r.add_not);
        assert!(r.left_has_rx, "left is consumer");
        assert!(!r.right_has_rx, "right is producer");
    }

    #[test]
    fn right_join_opposite_side_small_adds_not() {
        let r = apply_opt(10, 200, JoinType::Right);
        assert!(r.changed);
        assert!(r.add_not);
    }

    #[test]
    fn outer_join_small_side_adds_not() {
        let r = apply_opt(10, 200, JoinType::Outer);
        assert!(r.changed);
        assert!(r.add_not);
    }

    #[test]
    fn apply_with_intermediate_steps_between_scan_and_join() {
        let right_workflow = Workflow::new(vec![WorkflowStep::Scan(scan(vec![("id", 200)]))]);
        let steps = vec![
            WorkflowStep::Scan(scan(vec![("id", 10)])),
            WorkflowStep::Limit(5),
            WorkflowStep::Join(
                Join {
                    on: (field("id"), field("id")),
                    type_: JoinType::Inner,
                    partitions: 1,
                },
                right_workflow,
            ),
        ];

        let result = DynamicFilter::new(100).apply(&steps, &[]);
        match result {
            OptimizationResult::Changed(s) => {
                assert_eq!(s.len(), 3);
                assert!(matches!(&s[1], WorkflowStep::Limit(5)));
            }
            _ => panic!("expected Changed"),
        }
    }
}
