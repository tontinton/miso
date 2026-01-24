use miso_workflow::WorkflowStep;

use crate::pattern;

use super::{Group, Optimization, OptimizationResult, Pattern};

pub struct RemoveRedundantSortBeforeTopN;

impl Optimization for RemoveRedundantSortBeforeTopN {
    fn pattern(&self) -> Pattern {
        pattern!(Sort [TopN MuxTopN])
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> OptimizationResult {
        let WorkflowStep::Sort(sort) = &steps[0] else {
            return OptimizationResult::Unchanged;
        };
        let (topn_sorts, limit, is_mux) = match &steps[1] {
            WorkflowStep::TopN(sorts, limit) => (sorts, *limit, false),
            WorkflowStep::MuxTopN(sorts, limit) => (sorts, *limit, true),
            _ => return OptimizationResult::Unchanged,
        };

        let is_prefix = sort.sorts.len() <= topn_sorts.len()
            && sort
                .sorts
                .iter()
                .zip(topn_sorts.iter())
                .all(|(a, b)| a == b);

        if !is_prefix {
            return OptimizationResult::Unchanged;
        }

        OptimizationResult::Changed(vec![if is_mux {
            WorkflowStep::MuxTopN(topn_sorts.clone(), limit)
        } else {
            WorkflowStep::TopN(topn_sorts.clone(), limit)
        }])
    }
}
