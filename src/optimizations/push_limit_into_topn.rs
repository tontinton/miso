use crate::{pattern, workflow::WorkflowStep};

use super::{Optimization, Pattern};

pub struct PushLimitIntoTopN;

impl Optimization for PushLimitIntoTopN {
    fn pattern(&self) -> Pattern {
        pattern!(Limit -> TopN)
    }

    fn apply(&self, steps: &[WorkflowStep]) -> Option<Vec<WorkflowStep>> {
        let WorkflowStep::Limit(a) = &steps[0] else {
            return None;
        };
        let WorkflowStep::TopN(sort, b) = &steps[1] else {
            return None;
        };
        Some(vec![WorkflowStep::TopN(
            sort.clone(),
            std::cmp::min(*a, *b),
        )])
    }
}
