use miso_workflow::WorkflowStep;

use crate::pattern;

use super::{Group, Optimization, Pattern};

pub struct ConvertSortLimitToTopN;

impl Optimization for ConvertSortLimitToTopN {
    fn pattern(&self) -> Pattern {
        pattern!(Sort Limit)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        let WorkflowStep::Sort(sorts) = steps[0].clone() else {
            return None;
        };
        let WorkflowStep::Limit(limit) = steps[1].clone() else {
            return None;
        };
        Some(vec![WorkflowStep::TopN(sorts, limit)])
    }
}
