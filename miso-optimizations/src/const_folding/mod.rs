mod partial_evaluator;

use miso_workflow::WorkflowStep;

use crate::{const_folding::partial_evaluator::partial_eval, pattern};

use super::{Group, Optimization, Pattern};

/// Turns 'where x > 50 * 10 - 2' into 'where x > 498'.
pub struct ConstFolding;

impl Optimization for ConstFolding {
    fn pattern(&self) -> Pattern {
        pattern!(Filter)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        let WorkflowStep::Filter(expr) = &steps[0] else {
            return None;
        };

        if let Ok(expr) = partial_eval(expr) {
            return Some(vec![WorkflowStep::Filter(expr)]);
        }

        None
    }
}
