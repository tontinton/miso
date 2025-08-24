use miso_workflow::WorkflowStep;

use crate::pattern;

use super::{Group, Optimization, Pattern};

pub struct RemoveRedundantEmptySteps;

impl Optimization for RemoveRedundantEmptySteps {
    fn pattern(&self) -> Pattern {
        pattern!([Rename Extend Sort])
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        match &steps[0] {
            WorkflowStep::Rename(v) if v.is_empty() => {}
            WorkflowStep::Extend(v) if v.is_empty() => {}
            WorkflowStep::Sort(v) if v.is_empty() => {}
            _ => return None,
        }
        Some(vec![])
    }
}
