use miso_workflow::WorkflowStep;

use crate::pattern;

use super::{Group, Optimization, OptimizationResult, Pattern};

pub struct RemoveRedundantEmptySteps;

impl Optimization for RemoveRedundantEmptySteps {
    fn pattern(&self) -> Pattern {
        pattern!([Rename Extend Sort Expand])
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> OptimizationResult {
        match &steps[0] {
            WorkflowStep::Rename(v) if v.is_empty() => {}
            WorkflowStep::Extend(v) if v.is_empty() => {}
            WorkflowStep::Sort(v) if v.is_empty() => {}
            WorkflowStep::Expand(v) if v.fields.is_empty() => {}
            _ => return OptimizationResult::Unchanged,
        }
        OptimizationResult::Changed(vec![])
    }
}
