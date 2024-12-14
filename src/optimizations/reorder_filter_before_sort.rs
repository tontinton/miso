use crate::{pattern, workflow::WorkflowStep};

use super::{Group, Optimization, Pattern};

pub struct ReorderFilterBeforeSort;

impl Optimization for ReorderFilterBeforeSort {
    fn pattern(&self) -> Pattern {
        pattern!(Sort Filter)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        Some(vec![steps[1].clone(), steps[0].clone()])
    }
}
