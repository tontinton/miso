use miso_workflow::WorkflowStep;

use crate::pattern;

use super::{Group, Optimization, Pattern};

/// Some steps after unions, when inserted as a step into the union subquery, can allow for
/// predicate pushdowns.
/// Also insert these steps right before the union, for the same reasons, just for
/// the outer query before the union step.
pub struct PushStepsIntoUnion;

impl Optimization for PushStepsIntoUnion {
    fn pattern(&self) -> Pattern {
        pattern!(Union + [Filter Project Extend])
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        let filter_step = &steps[steps.len() - 1];

        let mut new_steps = Vec::with_capacity(steps.len());
        new_steps.push(filter_step.clone());
        new_steps.extend(steps[..steps.len() - 1].to_vec());

        for step in &mut new_steps[1..steps.len()] {
            if let WorkflowStep::Union(workflow) = step {
                workflow.steps.push(filter_step.clone());
            }
        }

        Some(new_steps)
    }
}
