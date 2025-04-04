use std::collections::BTreeSet;

use async_recursion::async_recursion;
use convert_sort_limit_to_topn::ConvertSortLimitToTopN;
use pattern::{Group, Pattern};
use push_count_into_scan::PushCountIntoScan;
use push_filter_and_limit_into_union::PushFilterAndLimitIntoUnion;
use push_filter_into_scan::PushFilterIntoScan;
use push_limit_into_limit::PushLimitIntoLimit;
use push_limit_into_scan::PushLimitIntoScan;
use push_limit_into_topn::PushLimitIntoTopN;
use push_summarize_into_scan::PushSummarizeIntoScan;
use push_topn_into_scan::PushTopNIntoScan;
use remove_redundant_sorts_before_count::RemoveRedundantSortsBeforeCount;
use reorder_filter_before_sort::ReorderFilterBeforeSort;
use tokio::task::yield_now;

use crate::workflow::{WorkflowStep, WorkflowStepKind};

mod convert_sort_limit_to_topn;
mod pattern;
mod push_count_into_scan;
mod push_filter_and_limit_into_union;
mod push_filter_into_scan;
mod push_limit_into_limit;
mod push_limit_into_scan;
mod push_limit_into_topn;
mod push_summarize_into_scan;
mod push_topn_into_scan;
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
}

impl Default for Optimizer {
    fn default() -> Self {
        let optimizations = vec![
            // First pass.
            vec![
                // Filter.
                opt!(ReorderFilterBeforeSort),
                opt!(PushFilterIntoScan),
                // Limit.
                opt!(PushLimitIntoLimit),
                opt!(PushLimitIntoScan),
                opt!(ConvertSortLimitToTopN),
                opt!(PushLimitIntoTopN),
                opt!(PushTopNIntoScan),
                // Count.
                opt!(PushCountIntoScan),
                opt!(RemoveRedundantSortsBeforeCount),
                // Summarize.
                opt!(PushSummarizeIntoScan),
            ],
            // Second pass - runs only after nothing to do on first pass.
            vec![
                // Union.
                opt_once!(PushFilterAndLimitIntoUnion),
            ],
        ];

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
        let mut already_ran = BTreeSet::new();

        'passes: loop {
            for (optimizations, patterns) in self.optimizations.iter().zip(&self.patterns) {
                let something_was_optimized = run_optimization_pass(
                    optimizations,
                    patterns,
                    &mut steps,
                    &mut kinded_steps,
                    &mut already_ran,
                );

                if something_was_optimized {
                    // Let's be good neighbours and allow other tasks to run for a bit.
                    yield_now().await;

                    // Restart from beginning.
                    continue 'passes;
                }
            }

            // Nothing left to optimize.
            break;
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
