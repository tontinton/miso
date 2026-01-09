use miso_workflow::WorkflowStep;

use crate::pattern;

use super::{Group, Optimization, OptimizationResult, Pattern};

pub struct PushUnionIntoScan;

impl Optimization for PushUnionIntoScan {
    fn pattern(&self) -> Pattern {
        pattern!(Scan Union)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> OptimizationResult {
        let WorkflowStep::Union(workflow) = &steps[1] else {
            return OptimizationResult::Unchanged;
        };
        if workflow.steps.len() != 1 {
            return OptimizationResult::Unchanged;
        }
        let WorkflowStep::Scan(union_scan) = workflow.steps[0].clone() else {
            return OptimizationResult::Unchanged;
        };
        let WorkflowStep::Scan(mut scan) = steps[0].clone() else {
            return OptimizationResult::Unchanged;
        };

        let Some(handle) = scan.connector.apply_union(
            &scan.collection,
            &union_scan.collection,
            scan.handle.as_ref(),
            union_scan.handle.as_ref(),
        ) else {
            return OptimizationResult::Unchanged;
        };
        scan.handle = handle.into();

        OptimizationResult::Changed(vec![WorkflowStep::Scan(scan)])
    }
}
