use crate::{pattern, workflow::WorkflowStep};

use super::{Group, Optimization, Pattern};

pub struct PushTopNIntoLimit;

impl Optimization for PushTopNIntoLimit {
    fn pattern(&self) -> Pattern {
        pattern!(TopN Limit)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        let WorkflowStep::TopN(sorts, b) = &steps[0] else {
            return None;
        };
        let WorkflowStep::Limit(a) = &steps[1] else {
            return None;
        };
        Some(vec![WorkflowStep::TopN(
            sorts.clone(),
            std::cmp::min(*a, *b),
        )])
    }
}
