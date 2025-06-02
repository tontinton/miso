use crate::{pattern, workflow::WorkflowStep};

use super::{Group, Optimization, Pattern};

pub struct PushLimitIntoTopN;

impl Optimization for PushLimitIntoTopN {
    fn pattern(&self) -> Pattern {
        pattern!([TopN MuxTopN] Limit)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        let (sorts, b, is_mux) = match &steps[0] {
            WorkflowStep::TopN(sorts, b) => (sorts, b, false),
            WorkflowStep::MuxTopN(sorts, b) => (sorts, b, true),
            _ => return None,
        };
        let WorkflowStep::Limit(a) = &steps[1] else {
            return None;
        };
        Some(vec![if is_mux {
            WorkflowStep::MuxTopN(sorts.clone(), std::cmp::min(*a, *b))
        } else {
            WorkflowStep::TopN(sorts.clone(), std::cmp::min(*a, *b))
        }])
    }
}
