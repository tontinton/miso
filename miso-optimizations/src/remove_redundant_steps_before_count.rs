use miso_workflow::WorkflowStep;

use crate::pattern;

use super::{Group, Optimization, Pattern};

pub struct RemoveRedundantStepsBeforeCount;

impl Optimization for RemoveRedundantStepsBeforeCount {
    fn pattern(&self) -> Pattern {
        pattern!([Project Extend Rename] Count)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        Some(vec![steps[1].clone()])
    }
}
