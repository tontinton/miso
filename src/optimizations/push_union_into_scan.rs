use crate::{pattern, workflow::WorkflowStep};

use super::{Group, Optimization, Pattern};

pub struct PushUnionIntoScan;

impl Optimization for PushUnionIntoScan {
    fn pattern(&self) -> Pattern {
        pattern!(Scan Union)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        let WorkflowStep::Union(workflow) = &steps[1] else {
            return None;
        };
        if workflow.steps.len() != 1 {
            return None;
        }
        let WorkflowStep::Scan(union_scan) = workflow.steps[0].clone() else {
            return None;
        };
        let WorkflowStep::Scan(mut scan) = steps[0].clone() else {
            return None;
        };

        scan.handle = scan
            .connector
            .apply_union(&scan.collection, &union_scan, scan.handle.as_ref())?
            .into();

        Some(vec![WorkflowStep::Scan(scan)])
    }
}
