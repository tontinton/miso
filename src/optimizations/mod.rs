use std::collections::BTreeSet;

use async_recursion::async_recursion;
use convert_sort_limit_to_topn::ConvertSortLimitToTopN;
use merge_filters_into_and_filter::MergeFiltersIntoAndFilter;
use pattern::{Group, Pattern};
use push_count_into_scan::PushCountIntoScan;
use push_filter_into_scan::PushFilterIntoScan;
use push_filter_into_union::PushFilterIntoUnion;
use push_limit_into_limit::PushLimitIntoLimit;
use push_limit_into_scan::PushLimitIntoScan;
use push_limit_into_topn::PushLimitIntoTopN;
use push_limit_into_union::PushLimitIntoUnion;
use push_summarize_into_scan::PushSummarizeIntoScan;
use push_topn_into_scan::PushTopNIntoScan;
use push_union_into_scan::PushUnionIntoScan;
use remove_redundant_sorts_before_count::RemoveRedundantSortsBeforeCount;
use reorder_filter_before_sort::ReorderFilterBeforeSort;
use tokio::task::yield_now;

use crate::workflow::{WorkflowStep, WorkflowStepKind};

mod convert_sort_limit_to_topn;
mod merge_filters_into_and_filter;
mod pattern;
mod push_count_into_scan;
mod push_filter_into_scan;
mod push_filter_into_union;
mod push_limit_into_limit;
mod push_limit_into_scan;
mod push_limit_into_topn;
mod push_limit_into_union;
mod push_summarize_into_scan;
mod push_topn_into_scan;
mod push_union_into_scan;
mod remove_redundant_sorts_before_count;
mod reorder_filter_before_sort;

#[cfg(test)]
mod tests;

#[macro_export]
macro_rules! opt {
    ($optimization:ident) => {
        OptimizationStep {
            optimization: Box::new($optimization) as Box<dyn Optimization>,
            run_once: false,
        }
    };
}

#[macro_export]
macro_rules! opt_once {
    ($optimization:ident) => {
        OptimizationStep {
            optimization: Box::new($optimization) as Box<dyn Optimization>,
            run_once: true,
        }
    };
}

pub trait Optimization: Send + Sync {
    fn pattern(&self) -> Pattern;
    fn apply(&self, steps: &[WorkflowStep], groups: &[Group]) -> Option<Vec<WorkflowStep>>;
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

impl Optimizer {
    pub fn empty() -> Self {
        Self {
            optimizations: vec![],
            patterns: vec![],
        }
    }

    /// empty() would also suffice for tests, but using this will make the tests faster!
    pub fn no_predicate_pushdowns() -> Self {
        Self::new(vec![vec![
            opt!(ReorderFilterBeforeSort),
            opt!(MergeFiltersIntoAndFilter),
            opt!(PushLimitIntoLimit),
            opt!(ConvertSortLimitToTopN),
            opt!(PushLimitIntoTopN),
            opt!(RemoveRedundantSortsBeforeCount),
        ]])
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
        Self::new(vec![
            vec![
                // Filter.
                opt!(ReorderFilterBeforeSort),
                opt!(MergeFiltersIntoAndFilter),
                opt!(PushFilterIntoScan),
                // Limit.
                opt!(PushLimitIntoLimit),
                opt!(ConvertSortLimitToTopN),
                opt!(PushLimitIntoTopN),
                opt!(PushLimitIntoScan),
                opt!(PushTopNIntoScan),
                // Count.
                opt!(PushCountIntoScan),
                opt!(RemoveRedundantSortsBeforeCount),
                // Summarize.
                opt!(PushSummarizeIntoScan),
                // Union.
                opt!(PushUnionIntoScan),
                opt!(PushFilterIntoUnion),
            ],
            vec![
                // Union.
                opt_once!(PushLimitIntoUnion),
            ],
        ])
    }
}

fn run_optimization_pass(
    optimizations: &[OptimizationStep],
    patterns: &[Pattern],
    steps: &mut Vec<WorkflowStep>,
    kinded_steps: &mut Vec<WorkflowStepKind>,
    already_ran: &mut BTreeSet<usize>,
) -> bool {
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

            let mut last_found_end = 0;
            while let Some((start, end)) = Pattern::search_first_with_groups(
                pattern,
                &kinded_steps[last_found_end..],
                &mut groups,
            ) {
                last_found_end += end - start;

                let matched_groups = std::mem::take(&mut groups);
                let Some(new_steps) = optimization_step
                    .optimization
                    .apply(&steps[start..end], &matched_groups)
                else {
                    continue;
                };

                if new_steps.len() > (end - start) {
                    last_found_end += new_steps.len() - (end - start);
                } else {
                    last_found_end -= (end - start) - new_steps.len();
                }

                steps.splice(start..end, new_steps);
                *kinded_steps = to_kind(steps);

                optimized_in_loop = true;
                optimized = true;
            }
        }
    }

    optimized
}

impl Optimizer {
    #[async_recursion]
    pub async fn optimize(&self, mut steps: Vec<WorkflowStep>) -> Vec<WorkflowStep> {
        if self.optimizations.is_empty() {
            return steps;
        }

        let mut kinded_steps = to_kind(&steps);

        for (optimizations, patterns) in self.optimizations.iter().zip(&self.patterns) {
            let mut already_ran = BTreeSet::new();

            while run_optimization_pass(
                optimizations,
                patterns,
                &mut steps,
                &mut kinded_steps,
                &mut already_ran,
            ) {
                // Let's be good neighbours and allow other tasks run for a bit.
                yield_now().await;
            }
        }

        // Don't forget to optimize union & join steps too!
        let mut optimized_inner_steps = Vec::with_capacity(steps.len());
        for step in steps {
            let optimized_inner_step = match step {
                WorkflowStep::Union(mut workflow) => {
                    workflow.steps = self.optimize(workflow.steps).await;
                    WorkflowStep::Union(workflow)
                }
                WorkflowStep::Join(config, mut workflow) => {
                    workflow.steps = self.optimize(workflow.steps).await;
                    WorkflowStep::Join(config, workflow)
                }
                _ => step,
            };

            optimized_inner_steps.push(optimized_inner_step);
        }
        optimized_inner_steps
    }
}
