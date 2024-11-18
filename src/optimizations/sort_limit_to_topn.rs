use crate::{pattern, workflow::WorkflowStep};

use super::{Optimization, Pattern};

pub struct SortLimitToTopN;

impl Optimization for SortLimitToTopN {
    fn pattern(&self) -> Pattern {
        pattern!(Sort -> Limit)
    }

    fn apply(&self, steps: &[WorkflowStep]) -> Option<Vec<WorkflowStep>> {
        let WorkflowStep::Sort(sort) = steps[0].clone() else {
            return None;
        };
        let WorkflowStep::Limit(limit) = steps[1].clone() else {
            return None;
        };
        Some(vec![WorkflowStep::TopN(sort, limit)])
    }
}
