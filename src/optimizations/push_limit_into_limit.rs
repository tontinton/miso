use crate::{pattern, workflow::WorkflowStep};

use super::{Group, Optimization, Pattern};

pub struct PushLimitIntoLimit;

impl Optimization for PushLimitIntoLimit {
    fn pattern(&self) -> Pattern {
        pattern!([Limit MuxLimit] Limit)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        let (a, is_mux) = match &steps[0] {
            WorkflowStep::Limit(a) => (a, false),
            WorkflowStep::MuxLimit(a) => (a, true),
            _ => return None,
        };
        let WorkflowStep::Limit(b) = &steps[1] else {
            return None;
        };
        Some(vec![if is_mux {
            WorkflowStep::MuxLimit(std::cmp::min(*a, *b))
        } else {
            WorkflowStep::Limit(std::cmp::min(*a, *b))
        }])
    }
}
