use miso_workflow::WorkflowStep;

use crate::pattern;

use super::{Group, Optimization, OptimizationResult, Pattern};

/// Allows for:
///  * Sorting less items.
///  * Filter predicate pushdown.
///  * Sort to be converted to top-n.
pub struct ReorderFilterBeforeSort;

impl Optimization for ReorderFilterBeforeSort {
    fn pattern(&self) -> Pattern {
        pattern!(Sort Filter)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> OptimizationResult {
        OptimizationResult::Changed(vec![steps[1].clone(), steps[0].clone()])
    }
}
