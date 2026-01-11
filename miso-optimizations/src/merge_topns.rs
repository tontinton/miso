use miso_workflow::WorkflowStep;

use crate::pattern;

use super::{Group, Optimization, OptimizationResult, Pattern};

pub struct MergeTopNs;

impl Optimization for MergeTopNs {
    fn pattern(&self) -> Pattern {
        pattern!([TopN MuxTopN] [TopN MuxTopN])
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> OptimizationResult {
        let (sorts_a, limit_a, is_mux_a) = match &steps[0] {
            WorkflowStep::TopN(sorts, limit) => (sorts, *limit, false),
            WorkflowStep::MuxTopN(sorts, limit) => (sorts, *limit, true),
            _ => return OptimizationResult::Unchanged,
        };
        let (sorts_b, limit_b, is_mux_b) = match &steps[1] {
            WorkflowStep::TopN(sorts, limit) => (sorts, *limit, false),
            WorkflowStep::MuxTopN(sorts, limit) => (sorts, *limit, true),
            _ => return OptimizationResult::Unchanged,
        };

        if sorts_a != sorts_b {
            return OptimizationResult::Unchanged;
        }

        let min_limit = std::cmp::min(limit_a, limit_b);
        let is_mux = is_mux_a || is_mux_b;

        OptimizationResult::Changed(vec![if is_mux {
            WorkflowStep::MuxTopN(sorts_a.clone(), min_limit)
        } else {
            WorkflowStep::TopN(sorts_a.clone(), min_limit)
        }])
    }
}
