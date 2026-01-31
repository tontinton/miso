use std::collections::BTreeSet;

use miso_workflow::{WorkflowStep, WorkflowStepKind};
use miso_workflow_types::join::JoinType;

use const_folding::ConstFolding;
use convert_sort_limit_to_topn::ConvertSortLimitToTopN;
use dynamic_filter::DynamicFilter;
use eliminate_unused_fields::EliminateUnusedFields;
use filter_propagation::FilterPropagation;
use invert_branch_filter::InvertBranchFilter;
use merge_filters_into_and_filter::MergeFiltersIntoAndFilter;
use merge_topns::MergeTopNs;
use mux_into_union::MuxIntoUnion;
use pattern::{Group, Pattern};
use project_propagation::{ProjectPropagationWithEnd, ProjectPropagationWithoutEnd};
use push_into_scan::PushIntoScan;
use push_join_into_scan::PushJoinIntoScan;
use push_limit_into_limit::PushLimitIntoLimit;
use push_limit_into_topn::PushLimitIntoTopN;
use push_steps_into_union::PushStepsIntoUnion;
use push_union_into_scan::PushUnionIntoScan;
use remove_no_op_filter::RemoveNoOpFilter;
use remove_redundant_empty_steps::RemoveRedundantEmptySteps;
use remove_redundant_sort_before_topn::RemoveRedundantSortBeforeTopN;
use remove_redundant_sorts_before_aggregation::RemoveRedundantSortsBeforeAggregation;
use remove_redundant_steps_before_aggregation::RemoveRedundantStepsBeforeAggregation;
use reorder_filter_before_sort::ReorderFilterBeforeSort;
use short_circuit_false_filter::ShortCircuitFalseFilter;
use split_scan_to_union::SplitScanIntoUnion;
use summarize_const_to_project::SummarizeConstToProject;

mod const_folding;
mod convert_sort_limit_to_topn;
mod dynamic_filter;
mod eliminate_unused_fields;
mod field_replacer;
mod filter_propagation;
mod invert_branch_filter;
mod merge_filters_into_and_filter;
mod merge_topns;
mod mux_into_union;
mod pattern;
mod project_propagation;
mod push_into_scan;
mod push_join_into_scan;
mod push_limit_into_limit;
mod push_limit_into_topn;
mod push_steps_into_union;
mod push_union_into_scan;
mod remove_no_op_filter;
mod remove_redundant_empty_steps;
mod remove_redundant_sort_before_topn;
mod remove_redundant_sorts_before_aggregation;
mod remove_redundant_steps_before_aggregation;
mod reorder_filter_before_sort;
mod short_circuit_false_filter;
mod split_scan_to_union;
mod summarize_const_to_project;

#[cfg(test)]
mod test_utils;
#[cfg(test)]
mod tests;

macro_rules! opt {
    ($optimization:expr) => {
        OptimizationStep {
            optimization: Box::new($optimization) as Box<dyn Optimization>,
            run_once: false,
        }
    };
}

macro_rules! opt_once {
    ($optimization:expr) => {
        OptimizationStep {
            optimization: Box::new($optimization) as Box<dyn Optimization>,
            run_once: true,
        }
    };
}

/// Like dynamic-filtering.small.max-distinct-values-per-driver in trino.
const DEFAULT_MAX_DISTINCT_COUNT_FOR_DYNAMIC_FILTER: u64 = 10000;

#[derive(Debug, PartialEq)]
pub enum OptimizationResult {
    Unchanged,
    Changed(Vec<WorkflowStep>),
    ShortCircuit,
}

pub trait Optimization: Send + Sync {
    fn pattern(&self) -> Pattern;
    fn apply(&self, steps: &[WorkflowStep], groups: &[Group]) -> OptimizationResult;
}

struct OptimizationStep {
    optimization: Box<dyn Optimization>,

    /// Runs the optimization just once in the currently running optimization pass.
    run_once: bool,
}

pub struct Optimizer {
    optimizations: Vec<Vec<OptimizationStep>>,
    patterns: Vec<Vec<Pattern>>,
}

fn to_kind(steps: &[WorkflowStep]) -> Vec<WorkflowStepKind> {
    steps.iter().map(|x| x.kind()).collect()
}

fn chain(groups: impl IntoIterator<Item = Vec<OptimizationStep>>) -> Vec<OptimizationStep> {
    groups.into_iter().flatten().collect()
}

impl Optimizer {
    pub fn empty() -> Self {
        Self {
            optimizations: vec![],
            patterns: vec![],
        }
    }

    fn setup(max_distinct_values: u64) -> Vec<OptimizationStep> {
        vec![
            opt_once!(DynamicFilter::new(max_distinct_values)),
            opt_once!(SplitScanIntoUnion), // Must come after DynamicFilter
            opt_once!(EliminateUnusedFields),
        ]
    }

    fn expr_simplification() -> Vec<OptimizationStep> {
        vec![
            opt!(InvertBranchFilter),
            opt!(ConstFolding),
            opt!(RemoveNoOpFilter),
            opt!(ShortCircuitFalseFilter),
            opt!(RemoveRedundantEmptySteps),
        ]
    }

    fn predicate_pushdown() -> Vec<OptimizationStep> {
        chain([
            // Limit/TopN - merge/optimize BEFORE pushing.
            vec![
                opt!(PushLimitIntoLimit),
                opt!(ConvertSortLimitToTopN),
                opt!(PushLimitIntoTopN),
                opt!(MergeTopNs),
                opt!(RemoveRedundantSortBeforeTopN),
            ],
            // Aggregation - simplify BEFORE pushing.
            vec![
                opt!(RemoveRedundantSortsBeforeAggregation),
                opt!(RemoveRedundantStepsBeforeAggregation),
                opt!(SummarizeConstToProject),
            ],
            // Pushdowns.
            vec![
                opt!(PushIntoScan),
                opt!(PushUnionIntoScan),
                opt!(PushJoinIntoScan),
            ],
            // Union - AFTER pushing, to have better chances of union pushdown.
            vec![opt!(PushStepsIntoUnion), opt!(MuxIntoUnion)],
            // Predicate pushdown removes steps and might allow for more simplifications.
            Self::expr_simplification(),
            // Move steps around to enable pushdowns.
            vec![
                opt!(ReorderFilterBeforeSort),
                opt!(ProjectPropagationWithEnd),
                opt!(ProjectPropagationWithoutEnd),
                opt!(FilterPropagation),
            ],
        ])
    }

    pub fn with_dynamic_filtering(max_distinct_values: u64) -> Self {
        Self::new(vec![
            Self::setup(max_distinct_values),
            // Simplify expressions before any pushdowns.
            Self::expr_simplification(),
            Self::predicate_pushdown(),
            // Merge filters into AND only after no more filters to pushdown, as this
            // optimization is only good for in-process filtering.
            vec![opt!(MergeFiltersIntoAndFilter)],
        ])
    }

    fn new(optimizations: Vec<Vec<OptimizationStep>>) -> Self {
        let patterns = optimizations
            .iter()
            .map(|opts| opts.iter().map(|x| x.optimization.pattern()).collect())
            .collect();

        Self {
            optimizations,
            patterns,
        }
    }
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::with_dynamic_filtering(DEFAULT_MAX_DISTINCT_COUNT_FOR_DYNAMIC_FILTER)
    }
}

#[derive(Default)]
struct AlreadyRan {
    root: BTreeSet<usize>,
    subquery: SubQueryAlreadyRan,
}

#[derive(Default)]
struct SubQueryAlreadyRan {
    union: BTreeSet<usize>,
    join: BTreeSet<usize>,
}

enum PassResult {
    NotOptimized,
    Optimized,
    ShortCircuit,
}

fn run_optimization_pass(
    optimizations: &[OptimizationStep],
    patterns: &[Pattern],
    steps: &mut Vec<WorkflowStep>,
    kinded_steps: &mut Vec<WorkflowStepKind>,
    already_ran: &mut BTreeSet<usize>,
) -> PassResult {
    let mut optimized = false;
    let mut optimized_in_loop = true;

    while optimized_in_loop {
        optimized_in_loop = false;

        for (i, (optimization_step, pattern)) in optimizations.iter().zip(patterns).enumerate() {
            if optimization_step.run_once {
                if already_ran.contains(&i) {
                    continue;
                }
                already_ran.insert(i);
            }

            let mut groups = Vec::new();

            let mut current_start = 0;
            let mut last_start = None;
            while let Some((start, end)) = Pattern::search_first_with_groups(
                pattern,
                &kinded_steps[current_start..],
                &mut groups,
            ) {
                if last_start == Some(current_start) {
                    // Prevent infinite loop when an optimization returns the same amount of steps.
                    current_start += 1;
                    continue;
                }
                last_start = Some(current_start);

                let matched_groups = std::mem::take(&mut groups);
                let new_steps = match optimization_step.optimization.apply(
                    &steps[current_start + start..current_start + end],
                    &matched_groups,
                ) {
                    OptimizationResult::Unchanged => {
                        current_start += 1;
                        continue;
                    }
                    OptimizationResult::Changed(new_steps) => new_steps,
                    OptimizationResult::ShortCircuit => return PassResult::ShortCircuit,
                };

                current_start += end.saturating_sub(new_steps.len() + start);

                steps.splice(
                    last_start.unwrap() + start..last_start.unwrap() + end,
                    new_steps,
                );
                *kinded_steps = to_kind(steps);

                optimized_in_loop = true;
                optimized = true;
            }
        }
    }

    if optimized {
        PassResult::Optimized
    } else {
        PassResult::NotOptimized
    }
}

impl Optimizer {
    pub fn optimize(&self, mut steps: Vec<WorkflowStep>) -> Vec<WorkflowStep> {
        if self.optimizations.is_empty() {
            return steps;
        }

        for (optimizations, patterns) in self.optimizations.iter().zip(&self.patterns) {
            let mut already_ran = AlreadyRan::default();

            if self.optimize_sub_query_steps(
                optimizations,
                patterns,
                &mut steps,
                &mut already_ran.subquery,
            ) {
                return vec![];
            }
            let mut kinded_steps = to_kind(&steps);

            loop {
                match run_optimization_pass(
                    optimizations,
                    patterns,
                    &mut steps,
                    &mut kinded_steps,
                    &mut already_ran.root,
                ) {
                    PassResult::NotOptimized => break,
                    PassResult::Optimized => {}
                    PassResult::ShortCircuit => return vec![],
                }
            }

            if self.optimize_sub_query_steps(
                optimizations,
                patterns,
                &mut steps,
                &mut already_ran.subquery,
            ) {
                return vec![];
            }
        }

        steps
    }

    /// Returns whether to short-circuit.
    fn optimize_sub_query_steps(
        &self,
        optimizations: &[OptimizationStep],
        patterns: &[Pattern],
        steps: &mut Vec<WorkflowStep>,
        already_ran: &mut SubQueryAlreadyRan,
    ) -> bool {
        let run = |steps: &mut Vec<WorkflowStep>, already_ran: &mut BTreeSet<usize>| {
            let mut kinded_steps = to_kind(steps);
            loop {
                match run_optimization_pass(
                    optimizations,
                    patterns,
                    steps,
                    &mut kinded_steps,
                    already_ran,
                ) {
                    PassResult::NotOptimized => return false,
                    PassResult::Optimized => {}
                    PassResult::ShortCircuit => return true,
                }
            }
        };

        let mut i = 0;
        while i < steps.len() {
            match &mut steps[i] {
                WorkflowStep::Union(workflow) => {
                    if run(&mut workflow.steps, &mut already_ran.union) {
                        steps.remove(i);
                        continue;
                    }
                }
                WorkflowStep::Join(join, workflow) => {
                    if run(&mut workflow.steps, &mut already_ran.join) {
                        match join.type_ {
                            JoinType::Inner | JoinType::Right => return true,
                            JoinType::Left | JoinType::Outer => {
                                if let WorkflowStep::Scan(scan) = &mut steps[0] {
                                    scan.dynamic_filter_tx = None;
                                    scan.dynamic_filter_rx = None;
                                }
                                steps.remove(i);
                                continue;
                            }
                        }
                    }
                }
                _ => {}
            }
            i += 1;
        }
        false
    }
}
