use miso_workflow::WorkflowStep;

use crate::pattern;

use super::{Group, Optimization, OptimizationResult, Pattern};

pub struct PushLimitIntoTopN;

impl Optimization for PushLimitIntoTopN {
    fn pattern(&self) -> Pattern {
        pattern!([TopN MuxTopN] Limit)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> OptimizationResult {
        let (sorts, b, is_mux) = match &steps[0] {
            WorkflowStep::TopN(sorts, b) => (sorts, b, false),
            WorkflowStep::MuxTopN(sorts, b) => (sorts, b, true),
            _ => return OptimizationResult::Unchanged,
        };
        let WorkflowStep::Limit(a) = &steps[1] else {
            return OptimizationResult::Unchanged;
        };

        let sorts = sorts.clone();
        let min = std::cmp::min(*a, *b);

        OptimizationResult::Changed(vec![if is_mux {
            WorkflowStep::MuxTopN(sorts, min)
        } else {
            WorkflowStep::TopN(sorts, min)
        }])
    }
}
