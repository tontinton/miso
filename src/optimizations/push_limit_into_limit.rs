use crate::{pattern, workflow::WorkflowStep};

use super::{Group, Optimization, Pattern};

pub struct PushLimitIntoLimit;

impl Optimization for PushLimitIntoLimit {
    fn pattern(&self) -> Pattern {
        pattern!(Limit Limit)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        let WorkflowStep::Limit(a) = &steps[0] else {
            return None;
        };
        let WorkflowStep::Limit(b) = &steps[1] else {
            return None;
        };
        Some(vec![WorkflowStep::Limit(std::cmp::min(*a, *b))])
    }
}
