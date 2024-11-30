use pattern::{Group, Pattern};
use push_topn_into_limit::PushTopNIntoLimit;
use push_topn_into_scan::PushTopNIntoScan;

use crate::workflow::{WorkflowStep, WorkflowStepKind};
use convert_sort_limit_to_topn::ConvertSortLimitToTopN;
use push_count_into_scan::PushCountIntoScan;
use push_filter_into_scan::PushFilterIntoScan;
use push_limit_into_limit::PushLimitIntoLimit;
use push_limit_into_scan::PushLimitIntoScan;
use push_summarize_into_scan::PushSummarizeIntoScan;
use remove_redundant_sorts_before_count::RemoveRedundantSortsBeforeCount;

mod convert_sort_limit_to_topn;
mod pattern;
mod push_count_into_scan;
mod push_filter_into_scan;
mod push_limit_into_limit;
mod push_limit_into_scan;
mod push_summarize_into_scan;
mod push_topn_into_limit;
mod push_topn_into_scan;
mod remove_redundant_sorts_before_count;

#[macro_export]
macro_rules! opt {
    ($optimization:ident) => {
        Box::new($optimization) as Box<dyn Optimization>
    };
}

pub trait Optimization: Send + Sync {
    fn pattern(&self) -> Pattern;
    fn apply(&self, steps: &[WorkflowStep], groups: &[Group]) -> Option<Vec<WorkflowStep>>;
}

pub struct Optimizer {
    optimizations: Vec<Box<dyn Optimization>>,
    patterns: Vec<Pattern>,
}

fn to_kind(steps: &[WorkflowStep]) -> Vec<WorkflowStepKind> {
    steps.iter().map(|x| x.kind()).collect()
}

impl Default for Optimizer {
    fn default() -> Self {
        let optimizations = vec![
            // Filter.
            opt!(PushFilterIntoScan),
            // Limit.
            opt!(PushLimitIntoLimit),
            opt!(PushLimitIntoScan),
            opt!(ConvertSortLimitToTopN),
            opt!(PushTopNIntoLimit),
            opt!(PushTopNIntoScan),
            // Count.
            opt!(PushCountIntoScan),
            opt!(RemoveRedundantSortsBeforeCount),
            // Summarize.
            opt!(PushSummarizeIntoScan),
        ];
        let patterns = optimizations.iter().map(|o| o.pattern()).collect();
        Self {
            optimizations,
            patterns,
        }
    }
}

impl Optimizer {
    pub fn optimize(&self, mut steps: Vec<WorkflowStep>) -> Vec<WorkflowStep> {
        let mut kinded_steps = to_kind(&steps);
        let mut optimized_in_loop = true;
        let mut groups = Vec::new();

        while optimized_in_loop {
            optimized_in_loop = false;

            for (optimization, pattern) in self.optimizations.iter().zip(&self.patterns) {
                groups.clear();
                let Some((start, end)) =
                    Pattern::search_first_with_groups(pattern, &kinded_steps, &mut groups)
                else {
                    continue;
                };

                let Some(new_steps) = optimization.apply(&steps[start..end], &groups) else {
                    continue;
                };

                steps.splice(start..end, new_steps);
                kinded_steps = to_kind(&steps);
                optimized_in_loop = true;
            }
        }

        steps
    }
}
