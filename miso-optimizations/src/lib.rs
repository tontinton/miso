use std::collections::BTreeSet;

use miso_workflow::{WorkflowStep, WorkflowStepKind};

use const_folding::ConstFolding;
use convert_sort_limit_to_topn::ConvertSortLimitToTopN;
use dynamic_filter::DynamicFilter;
use merge_filters_into_and_filter::MergeFiltersIntoAndFilter;
use mux_into_union::MuxIntoUnion;
use pattern::{Group, Pattern};
use project_propagation::{ProjectPropagationWithEnd, ProjectPropagationWithoutEnd};
use push_count_into_scan::PushCountIntoScan;
use push_either_project_extend_rename_into_scan::PushEitherProjectExtendRenameIntoScan;
use push_filter_into_scan::PushFilterIntoScan;
use push_limit_into_limit::PushLimitIntoLimit;
use push_limit_into_scan::PushLimitIntoScan;
use push_limit_into_topn::PushLimitIntoTopN;
use push_steps_into_union::PushStepsIntoUnion;
use push_summarize_into_scan::PushSummarizeIntoScan;
use push_topn_into_scan::PushTopNIntoScan;
use push_union_into_scan::PushUnionIntoScan;
use remove_redundant_empty_steps::RemoveRedundantEmptySteps;
use remove_redundant_sorts_before_count::RemoveRedundantSortsBeforeCount;
use remove_redundant_steps_before_count::RemoveRedundantStepsBeforeCount;
use reorder_filter_before_sort::ReorderFilterBeforeSort;
use reorder_steps_before_mux::ReorderStepsBeforeMux;
use split_scan_to_union::SplitScanIntoUnion;

mod const_folding;
mod convert_sort_limit_to_topn;
mod dynamic_filter;
mod field_replacer;
mod merge_filters_into_and_filter;
mod mux_into_union;
mod pattern;
mod project_propagation;
mod push_count_into_scan;
mod push_either_project_extend_rename_into_scan;
mod push_filter_into_scan;
mod push_limit_into_limit;
mod push_limit_into_scan;
mod push_limit_into_topn;
mod push_steps_into_union;
mod push_summarize_into_scan;
mod push_topn_into_scan;
mod push_union_into_scan;
mod remove_redundant_empty_steps;
mod remove_redundant_sorts_before_count;
mod remove_redundant_steps_before_count;
mod reorder_filter_before_sort;
mod reorder_steps_before_mux;
mod split_scan_to_union;

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
const DEFAULT_MAX_DISTINCT_COUNT_FOR_DYNAMIC_FILTER: u32 = 10000;

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

    pub fn with_dynamic_filtering(max_distinct_values: u32) -> Self {
        Self::new(vec![
            // Pre predicate pushdowns.
            vec![
                opt_once!(DynamicFilter::new(max_distinct_values)),
                // Must come after dynamic filtering, so the split scan nodes will also receive
                // the dynamic filter.
                opt_once!(SplitScanIntoUnion),
                // Always returns something, even if the step is unmodified, so run only once.
                opt_once!(ConstFolding),
            ],
            // Predicate pushdowns + optimizations that help predicate pushdowns.
            vec![
                // Project & Extend & Rename.
                opt!(PushEitherProjectExtendRenameIntoScan),
                opt!(ProjectPropagationWithEnd),
                opt!(ProjectPropagationWithoutEnd),
                opt!(RemoveRedundantEmptySteps),
                // Filter.
                opt!(ReorderFilterBeforeSort),
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
                opt!(RemoveRedundantStepsBeforeCount),
                // Summarize.
                opt!(PushSummarizeIntoScan),
                // Union.
                opt!(PushUnionIntoScan),
                opt!(PushStepsIntoUnion),
                // Mux.
                opt!(MuxIntoUnion),
                opt!(ReorderStepsBeforeMux),
            ],
            // Post predicate pushdowns.
            vec![
                // Merge filters into AND only after no more filters to pushdown, as this
                // optimization is only good for in-process filtering.
                opt!(MergeFiltersIntoAndFilter),
            ],
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
                let Some(new_steps) = optimization_step.optimization.apply(
                    &steps[current_start + start..current_start + end],
                    &matched_groups,
                ) else {
                    current_start += end - start;
                    continue;
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

    optimized
}

impl Optimizer {
    pub fn optimize(&self, mut steps: Vec<WorkflowStep>) -> Vec<WorkflowStep> {
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
            ) {}
        }

        self.optimize_sub_query_steps(steps)
    }

    /// Optimize the query steps inside union and join.
    fn optimize_sub_query_steps(&self, steps: Vec<WorkflowStep>) -> Vec<WorkflowStep> {
        let mut optimized_inner_steps = Vec::with_capacity(steps.len());
        for step in steps {
            let optimized_inner_step = match step {
                WorkflowStep::Union(mut workflow) => {
                    workflow.steps = self.optimize(workflow.steps);
                    WorkflowStep::Union(workflow)
                }
                WorkflowStep::Join(config, mut workflow) => {
                    workflow.steps = self.optimize(workflow.steps);
                    WorkflowStep::Join(config, workflow)
                }
                _ => step,
            };

            optimized_inner_steps.push(optimized_inner_step);
        }
        optimized_inner_steps
    }
}
