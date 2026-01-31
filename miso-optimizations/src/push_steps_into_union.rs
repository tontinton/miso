//! Pushes filter/project/extend into union branches for earlier reduction.
//!
//! A filter after a union applies to all branches, but runs *after* all data
//! is merged. By copying the filter into each branch, we filter earlier and
//! reduce data before merging. This also enables further pushdown optimizations
//! (like pushing filters into the connector).
//!
//! Example:
//!   scan | union (scan) | where x > 0
//! becomes:
//!   where x > 0 | scan | where x > 0 | union (scan | where x > 0)
//!
//! Same logic for project and extend - running them in each branch means less
//! data flowing through the union. The step also gets added before the first
//! union so the outer pipeline benefits too.

use miso_workflow::WorkflowStep;

use crate::pattern;

use super::{Group, Optimization, OptimizationResult, Pattern};

pub struct PushStepsIntoUnion;

impl Optimization for PushStepsIntoUnion {
    fn pattern(&self) -> Pattern {
        pattern!(Union + [Filter Project Extend])
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> OptimizationResult {
        let filter_step = &steps[steps.len() - 1];

        let mut new_steps = Vec::with_capacity(steps.len());
        new_steps.push(filter_step.clone());
        new_steps.extend(steps[..steps.len() - 1].to_vec());

        for step in &mut new_steps[1..steps.len()] {
            if let WorkflowStep::Union(workflow) = step {
                workflow.steps.push(filter_step.clone());
            }
        }

        OptimizationResult::Changed(new_steps)
    }
}
