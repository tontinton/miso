use crate::{pattern, workflow::WorkflowStep};

use super::{Group, Optimization, Pattern};

pub struct PushLimitIntoTopN;

impl Optimization for PushLimitIntoTopN {
    fn pattern(&self) -> Pattern {
        pattern!(Limit TopN)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        let WorkflowStep::Limit(a) = &steps[0] else {
            return None;
        };
        let WorkflowStep::TopN(sorts, b) = &steps[1] else {
            return None;
        };
        Some(vec![WorkflowStep::TopN(
            sorts.clone(),
            std::cmp::min(*a, *b),
        )])
    }
}
