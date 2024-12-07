use std::sync::atomic::{self, AtomicBool};

use convert_sort_limit_to_topn::ConvertSortLimitToTopN;
use pattern::{Group, Pattern};
use push_count_into_scan::PushCountIntoScan;
use push_filter_into_scan::PushFilterIntoScan;
use push_limit_into_limit::PushLimitIntoLimit;
use push_limit_into_scan::PushLimitIntoScan;
use push_limit_into_topn::PushLimitIntoTopN;
use push_limit_or_topn_into_union::PushLimitOrTopNIntoUnion;
use push_summarize_into_scan::PushSummarizeIntoScan;
use push_topn_into_scan::PushTopNIntoScan;
use remove_redundant_sorts_before_count::RemoveRedundantSortsBeforeCount;

use crate::workflow::{WorkflowStep, WorkflowStepKind};

mod convert_sort_limit_to_topn;
mod pattern;
mod push_count_into_scan;
mod push_filter_into_scan;
mod push_limit_into_limit;
mod push_limit_into_scan;
mod push_limit_into_topn;
mod push_limit_or_topn_into_union;
mod push_summarize_into_scan;
mod push_topn_into_scan;
mod remove_redundant_sorts_before_count;

#[macro_export]
macro_rules! opt {
    ($optimization:ident) => {
        Box::new($optimization) as Box<dyn Optimization>
    };
}

#[macro_export]
macro_rules! opt_once {
    ($optimization:ident) => {
        Box::new(OptimizeOnce {
            optimization: Box::new($optimization),
            ran: AtomicBool::new(false),
        })
    };
}

pub trait Optimization: Send + Sync {
    fn pattern(&self) -> Pattern;
    fn apply(&self, steps: &[WorkflowStep], groups: &[Group]) -> Option<Vec<WorkflowStep>>;
}

struct OptimizeOnce {
    optimization: Box<dyn Optimization>,
    ran: AtomicBool,
}

impl Optimization for OptimizeOnce {
    fn pattern(&self) -> Pattern {
        self.optimization.pattern()
    }

    fn apply(&self, steps: &[WorkflowStep], groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        let already_ran = self.ran.fetch_or(true, atomic::Ordering::Relaxed);
        if already_ran {
            return None;
        }
        self.optimization.apply(steps, groups)
    }
}

pub struct Optimizer {
    optimizations: Vec<Vec<Box<dyn Optimization>>>,
    patterns: Vec<Vec<Pattern>>,
}

fn to_kind(steps: &[WorkflowStep]) -> Vec<WorkflowStepKind> {
    steps.iter().map(|x| x.kind()).collect()
}

impl Default for Optimizer {
    fn default() -> Self {
        let optimizations = vec![
            // First pass.
            vec![
                // Filter.
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
                opt_once!(PushLimitOrTopNIntoUnion),
            ],
        ];

        let patterns = optimizations
            .iter()
            .map(|opts| opts.iter().map(|o| o.pattern()).collect())
            .collect();

        Self {
            optimizations,
            patterns,
        }
    }
}

pub fn run_optimization_pass(
    optimizations: &[Box<dyn Optimization>],
    patterns: &[Pattern],
    steps: &mut Vec<WorkflowStep>,
    kinded_steps: &mut Vec<WorkflowStepKind>,
) -> bool {
    let mut optimized = false;
    let mut optimized_in_loop = true;

    while optimized_in_loop {
        optimized_in_loop = false;

        for (optimization, pattern) in optimizations.iter().zip(patterns) {
            let mut groups = Vec::new();

            let mut last_found_end = 0;
            while let Some((start, end)) = Pattern::search_first_with_groups(
                pattern,
                &kinded_steps[last_found_end..],
                &mut groups,
            ) {
                last_found_end = end;

                let matched_groups = std::mem::take(&mut groups);
                let Some(new_steps) = optimization.apply(&steps[start..end], &matched_groups)
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
    pub fn optimize(&self, mut steps: Vec<WorkflowStep>) -> Vec<WorkflowStep> {
        let mut kinded_steps = to_kind(&steps);

        let mut optimized_in_loop = true;
        while optimized_in_loop {
            optimized_in_loop = false;

            for (optimizations, patterns) in self.optimizations.iter().zip(&self.patterns) {
                optimized_in_loop |=
                    run_optimization_pass(optimizations, patterns, &mut steps, &mut kinded_steps);
            }
        }

        // Don't forget to optimize union steps too!
        steps
            .into_iter()
            .map(|step| {
                if let WorkflowStep::Union(mut workflow) = step {
                    workflow.steps = self.optimize(workflow.steps);
                    WorkflowStep::Union(workflow)
                } else {
                    step
                }
            })
            .collect()
    }
}
