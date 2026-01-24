use miso_workflow::WorkflowStep;

use crate::pattern;

use super::{Group, Optimization, OptimizationResult, Pattern};

pub struct ConvertSortLimitToTopN;

impl Optimization for ConvertSortLimitToTopN {
    fn pattern(&self) -> Pattern {
        pattern!(Sort Limit)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> OptimizationResult {
        let WorkflowStep::Sort(sort) = steps[0].clone() else {
            return OptimizationResult::Unchanged;
        };
        let WorkflowStep::Limit(limit) = steps[1].clone() else {
            return OptimizationResult::Unchanged;
        };
        OptimizationResult::Changed(vec![WorkflowStep::TopN(sort.sorts, limit)])
    }
}
