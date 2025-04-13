use crate::{pattern, workflow::WorkflowStep};

use super::{Group, Optimization, Pattern};

pub struct PushUnionIntoScan;

impl Optimization for PushUnionIntoScan {
    fn pattern(&self) -> Pattern {
        pattern!(Scan Union)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        let WorkflowStep::Scan(mut scan) = steps[0].clone() else {
            return None;
        };
        let WorkflowStep::Union(workflow) = &steps[1] else {
            return None;
        };

        scan.handle = scan
            .connector
            .apply_union(&scan.collection, workflow, scan.handle.as_ref())?
            .into();

        Some(vec![WorkflowStep::Scan(scan)])
    }
}
