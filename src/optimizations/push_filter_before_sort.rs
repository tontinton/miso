use crate::{pattern, workflow::WorkflowStep};

use super::{Group, Optimization, Pattern};

pub struct PushFilterBeforeSort;

impl Optimization for PushFilterBeforeSort {
    fn pattern(&self) -> Pattern {
        pattern!(Scan Sort Filter)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        let WorkflowStep::Scan(mut scan) = steps[0].clone() else {
            return None;
        };
        let WorkflowStep::Filter(ast) = &steps[2] else {
            return None;
        };

        scan.handle = scan
            .connector
            .apply_filter(ast, scan.handle.as_ref())?
            .into();

        Some(vec![WorkflowStep::Scan(scan)])
    }
}
